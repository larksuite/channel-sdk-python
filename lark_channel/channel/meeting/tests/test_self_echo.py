"""Marking the bot's own contributions when they come back around."""

from . import fixtures as fx


def _chat_from(open_id):
    return fx.push_activity(
        [
            fx.push_item(
                "chat_received",
                [
                    fx.chat_item(
                        shape="push",
                        operator=fx.actor(open_id, shape="push", name="Someone"),
                    )
                ],
            )
        ]
    )


async def test_own_message_is_flagged_and_still_delivered(vc, tat_channel):
    """Dropping it would break transcript-style consumers that need the bot's
    own turns; the flag lets each consumer decide."""
    channel = tat_channel()
    session = await channel.join_meeting(fx.MEETING_NO)
    got = []
    session.on("chat", lambda event: got.append(event))

    fx.deliver(channel, _chat_from(fx.BOT_OPEN_ID))

    await fx.wait_for(lambda: got, what="the echoed chat message")
    assert got[0].self_echo is True


async def test_follow_mode_never_flags_an_echo(vc, uat_channel):
    """In follow mode the bot is not in the meeting at all, so nothing in the
    stream can have come from it."""
    channel, _store, _flow = uat_channel()
    got = []
    body = fx.poll_events(
        [
            fx.poll_item(
                "chat_received",
                [
                    fx.chat_item(
                        shape="poll",
                        operator=fx.actor(fx.BOT_OPEN_ID, shape="poll", name="Helper"),
                    )
                ],
            )
        ]
    )
    with fx.fast_sleep():
        session = await channel.follow_my_meeting(user_open_id=fx.USER_OPEN_ID)
        session.on("chat", lambda event: got.append(event))
        vc.sequence(fx.URI_EVENTS, [body, fx.poll_events([])])
        await fx.wait_for(lambda: got, what="the polled chat message")
        session.dispose()

    assert got[0].self_echo is False


async def test_unresolved_bot_identity_flags_the_event_as_possibly_our_own(
    vc, tat_channel, make_ch
):
    """``False`` means "definitely not me" and lets the reply loop close. Until
    the bot's own id is known, the honest answer is "maybe"."""
    channel = make_ch(meeting=fx.meeting_config())
    fx.mark_connected(channel, bot_open_id=None)
    assert channel.bot_identity is None

    session = await channel.join_meeting(fx.MEETING_NO)
    got = []
    session.on("chat", lambda event: got.append(event))

    fx.deliver(channel, _chat_from("ou_someone_else"))

    await fx.wait_for(lambda: got, what="the chat message")
    assert got[0].self_echo is True
