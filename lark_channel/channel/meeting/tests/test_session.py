"""Session lifecycle: entering, leaving, and being torn down."""

import asyncio
import gc
import logging
import time

import pytest

from lark_channel.channel.errors import FeishuChannelError, FeishuChannelErrorCode

from . import fixtures as fx


async def test_joining_requires_a_connection_while_following_does_not(
    vc, make_ch, uat_channel
):
    """Joining depends on pushed activity, so it needs the socket. Following is
    REST plus a user ticket, so opening a socket for it is pure overhead."""
    unconnected = make_ch(meeting=fx.meeting_config())
    with pytest.raises(FeishuChannelError) as excinfo:
        await unconnected.join_meeting(fx.MEETING_NO)
    assert excinfo.value.code is FeishuChannelErrorCode.NOT_CONNECTED
    assert vc.count(fx.URI_JOIN) == 0

    channel, _store, _flow = uat_channel()
    with fx.fast_sleep(max_sleeps=3):
        session = await channel.follow_my_meeting(user_open_id=fx.USER_OPEN_ID)
        assert session.mode == "uat"
        session.dispose()


async def test_join_sends_the_nine_digit_number_and_keeps_the_long_id(vc, tat_channel):
    channel = tat_channel()
    session = await channel.join_meeting(fx.MEETING_NO)

    call = vc.last(fx.URI_JOIN)
    assert call.body["join_type"] == 1
    assert call.body["join_identify"]["meeting_no"] == fx.MEETING_NO
    assert session.meeting_no == fx.MEETING_NO
    assert session.meeting_id == fx.MEETING_ID_STR
    assert session.mode == "tat"


async def test_leave_and_dispose_are_both_idempotent(vc, tat_channel):
    channel = tat_channel()
    session = await channel.join_meeting(fx.MEETING_NO)

    await session.leave()
    await session.leave()
    session.dispose()
    session.dispose()

    assert vc.count(fx.URI_LEAVE) == 1


async def test_leave_still_works_after_dispose(vc, tat_channel):
    """This is what makes "dispose does not leave the meeting, so leave before
    exiting the process" a usable instruction rather than a trap."""
    channel = tat_channel()
    session = await channel.join_meeting(fx.MEETING_NO)

    session.dispose()
    await session.leave()

    assert vc.count(fx.URI_LEAVE) == 1


async def test_dispose_does_not_leave_the_meeting(vc, tat_channel):
    channel = tat_channel()
    session = await channel.join_meeting(fx.MEETING_NO)

    session.dispose()
    await fx.settle()

    assert vc.count(fx.URI_LEAVE) == 0


async def test_meeting_ended_event_ends_the_session_and_leaves_once(vc, tat_channel):
    channel = tat_channel()
    session = await channel.join_meeting(fx.MEETING_NO)
    ended = []
    session.on("end", lambda event: ended.append(event))

    fx.deliver(channel, fx.push_meeting_ended())

    await fx.wait_for(lambda: ended, what="the end event")
    assert ended[0].reason == "meeting_ended"
    await fx.wait_for(lambda: vc.count(fx.URI_LEAVE) == 1, what="the leave call")
    await fx.settle()
    assert vc.count(fx.URI_LEAVE) == 1


async def test_channel_disconnect_disposes_sessions_without_leaving_meetings(
    vc, tat_channel
):
    """A reconnect must not make the bot vanish from every meeting it is in."""
    channel = tat_channel()
    session = await channel.join_meeting(fx.MEETING_NO)
    ended = []
    session.on("end", lambda event: ended.append(event))

    await channel.disconnect()
    await fx.settle()

    assert vc.count(fx.URI_LEAVE) == 0
    assert [event.reason for event in ended] == ["disposed"]


async def test_dispose_converges_every_task_the_session_started(
    vc, tat_channel, caplog
):
    channel = tat_channel(
        liveness_probe_interval_seconds=0.02, active_meeting_check_interval_seconds=0.02
    )
    session = await channel.join_meeting(fx.MEETING_NO)
    await fx.wait_for(
        lambda: vc.count(fx.URI_EVENTS) >= 1, what="the first liveness probe"
    )

    with caplog.at_level(logging.WARNING):
        session.dispose()
        await asyncio.sleep(0.1)
        probes_after_dispose = vc.count(fx.URI_EVENTS)
        await asyncio.sleep(0.1)
        assert vc.count(fx.URI_EVENTS) == probes_after_dispose
        gc.collect()
        await fx.settle()

    assert "Task was destroyed" not in caplog.text


async def test_failed_leave_still_tears_the_session_down(vc, tat_channel):
    channel = tat_channel()
    session = await channel.join_meeting(fx.MEETING_NO)
    errors = []
    session.on("error", lambda err: errors.append(err))
    vc.route(fx.URI_LEAVE, lambda call: (500, fx.error_body(500, "upstream exploded")))

    await session.leave()

    await fx.wait_for(lambda: errors, what="the leave failure on the error channel")
    assert isinstance(errors[0], FeishuChannelError)
    # The meeting is gone from the channel's routing table, so a further push
    # for it reaches nobody rather than resurrecting a half-torn-down session.
    got = []
    session.on("transcript", lambda event: got.append(event))
    fx.deliver(channel, fx.push_activity([fx.push_item("transcript_received")]))
    await fx.settle()
    assert got == []


async def test_leave_returns_while_a_handler_is_still_parked(vc, tat_channel):
    """The handler here yields control and never comes back — a plausible bug
    in someone else's code, and the only kind of stall this layer can defend
    against. Waiting for it unconditionally would hang teardown, and would
    hang the very timeout meant to rescue a stalled session."""
    channel = tat_channel(
        max_concurrent_sessions=1, dispose_drain_timeout_seconds=0.2
    )
    vc.sequence(
        fx.URI_JOIN,
        [fx.join_body(fx.MEETING_ID_STR), fx.join_body(fx.OTHER_MEETING_ID_STR)],
    )
    session = await channel.join_meeting(fx.MEETING_NO)
    entered = []

    async def parked(event):
        entered.append(event)
        await asyncio.Event().wait()

    session.on("transcript", parked)
    fx.deliver(channel, fx.push_activity([fx.push_item("transcript_received")]))
    await fx.wait_for(lambda: entered, what="the handler to be entered")

    started = time.monotonic()
    await asyncio.wait_for(session.leave(), timeout=3.0)
    assert time.monotonic() - started < 2.0

    # The seat has to come back even though the queue never drained, otherwise
    # one parked handler burns a slot for the life of the process.
    second = await channel.join_meeting(fx.OTHER_MEETING_NO)
    assert second.meeting_id == fx.OTHER_MEETING_ID_STR


async def test_leave_warns_when_the_delivery_queue_could_not_be_drained(
    vc, tat_channel, caplog
):
    channel = tat_channel(dispose_drain_timeout_seconds=0.2)
    session = await channel.join_meeting(fx.MEETING_NO)
    entered = []

    async def parked(event):
        entered.append(event)
        await asyncio.Event().wait()

    session.on("transcript", parked)
    fx.deliver(channel, fx.push_activity([fx.push_item("transcript_received")]))
    await fx.wait_for(lambda: entered, what="the handler to be entered")

    with caplog.at_level(logging.WARNING, logger="Lark"):
        await asyncio.wait_for(session.leave(), timeout=3.0)

    assert any(
        record.levelno >= logging.WARNING and "drain" in record.getMessage().lower()
        for record in caplog.records
    ), caplog.text


async def test_disconnect_returns_while_a_handler_is_still_parked(vc, tat_channel):
    channel = tat_channel(dispose_drain_timeout_seconds=0.2)
    session = await channel.join_meeting(fx.MEETING_NO)
    entered = []

    async def parked(event):
        entered.append(event)
        await asyncio.Event().wait()

    session.on("transcript", parked)
    fx.deliver(channel, fx.push_activity([fx.push_item("transcript_received")]))
    await fx.wait_for(lambda: entered, what="the handler to be entered")

    started = time.monotonic()
    await asyncio.wait_for(channel.disconnect(), timeout=8.0)
    assert time.monotonic() - started < 6.0


async def test_an_unknown_session_event_name_is_reported(vc, tat_channel, caplog):
    """A typo here produces a handler that is simply never called, which looks
    exactly like the platform not sending anything."""
    channel = tat_channel()
    session = await channel.join_meeting(fx.MEETING_NO)

    with caplog.at_level(logging.WARNING, logger="Lark"):
        session.on("transcripts", lambda event: None)   # the name is "transcript"

    assert any(
        record.levelno >= logging.WARNING and "transcripts" in record.getMessage()
        for record in caplog.records
    ), caplog.text


async def test_a_replaced_session_for_one_meeting_is_disposed_not_orphaned(
    vc, tat_channel, uat_channel
):
    """Two sessions for one meeting would leave the first out of the routing
    table but still running — and for a follow session that means it keeps
    polling the whole meeting with the user's ticket after the application
    believes it is gone."""
    channel, _store, _flow = uat_channel(active_meeting_check_interval_seconds=300.0)
    fx.mark_connected(channel)

    followed = await channel.follow_my_meeting(user_open_id=fx.USER_OPEN_ID)
    assert followed.meeting_id == fx.MEETING_ID_STR

    # The bot then gets pulled into the very same meeting.
    joined = await channel.join_meeting(fx.MEETING_NO)
    assert joined.meeting_id == fx.MEETING_ID_STR
    assert joined is not followed

    # Asserted as "stops growing" rather than "never grew": a round already in
    # flight when the takeover happens is not a leak.
    await asyncio.sleep(0.1)
    settled = vc.count(fx.URI_EVENTS)
    await asyncio.sleep(0.2)
    assert vc.count(fx.URI_EVENTS) == settled


async def test_a_superseded_handle_cannot_eject_the_replacement(vc, uat_channel):
    """Holding on to the old handle is easy, and calling ``leave()`` on it would
    remove the bot from a meeting the replacement is actively serving."""
    channel, _store, _flow = uat_channel(active_meeting_check_interval_seconds=300.0)
    fx.mark_connected(channel)

    followed = await channel.follow_my_meeting(user_open_id=fx.USER_OPEN_ID)
    joined = await channel.join_meeting(fx.MEETING_NO)
    assert joined is not followed

    await followed.leave()
    await fx.settle()

    assert vc.count(fx.URI_LEAVE) == 0
    # And the replacement can still depart on its own behalf.
    await joined.leave()
    assert vc.count(fx.URI_LEAVE) == 1
