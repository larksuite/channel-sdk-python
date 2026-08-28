"""Suppressing repeats without suppressing distinct content."""

from . import fixtures as fx


async def test_repeated_event_id_is_delivered_once(vc, uat_channel):
    channel, _store, _flow = uat_channel()
    got = []
    body = fx.poll_events([fx.poll_item("transcript_received", event_id="evt-1")])
    with fx.fast_sleep():
        session = await channel.follow_my_meeting(user_open_id=fx.USER_OPEN_ID)
        session.on("transcript", lambda event: got.append(event))
        vc.sequence(fx.URI_EVENTS, [body, body, fx.poll_events([])])
        await fx.wait_for(
            lambda: vc.count(fx.URI_EVENTS) >= 3, what="three polling rounds"
        )
        await fx.settle()
        session.dispose()

    assert len(got) == 1


async def test_identical_pushed_item_without_event_id_is_delivered_once(
    vc, tat_channel
):
    """Pushed activity items carry no id of their own, so the only thing that
    can catch a redelivery is a key synthesized from the content."""
    channel = tat_channel()
    session = await channel.join_meeting(fx.MEETING_NO)
    got = []
    session.on("transcript", lambda event: got.append(event))

    for envelope_id in ("env-a", "env-b"):
        fx.deliver(
            channel,
            fx.push_activity(
                [fx.push_item("transcript_received")], envelope_event_id=envelope_id
            ),
        )
    await fx.wait_for(lambda: got, what="the first transcript")
    await fx.settle()

    assert len(got) == 1


async def test_growing_sentence_is_delivered_every_time_and_keeps_its_sentence_id(
    vc, tat_channel
):
    """A sentence id is an upsert handle, not a dedup key: the platform resends
    the same sentence as the speaker keeps talking and the text grows."""
    channel = tat_channel(stabilize_seconds=0.0)
    session = await channel.join_meeting(fx.MEETING_NO)
    got = []
    session.on("transcript", lambda event: got.append(event))

    for text in ("今天", "今天来讨论"):
        fx.deliver(
            channel,
            fx.push_activity(
                [
                    fx.push_item(
                        "transcript_received",
                        [fx.transcript_item(shape="push", text=text, sentence_id="s-1")],
                    )
                ]
            ),
        )
    await fx.wait_for(lambda: len(got) >= 2, what="both revisions of the sentence")

    assert [event.text for event in got] == ["今天", "今天来讨论"]
    assert all(event.sentence_id == "s-1" for event in got)


async def test_same_sentence_in_two_parallel_meetings_is_not_cross_suppressed(
    vc, tat_channel
):
    """Two meetings running at once will produce byte-identical greetings from
    the same person seconds apart. A dedup key without the meeting in it makes
    one meeting's transcript vanish from the other."""
    channel = tat_channel()
    vc.sequence(
        fx.URI_JOIN,
        [fx.join_body(fx.MEETING_ID_STR), fx.join_body(fx.OTHER_MEETING_ID_STR)],
    )
    first = await channel.join_meeting(fx.MEETING_NO)
    second = await channel.join_meeting(fx.OTHER_MEETING_NO)

    got = []
    first.on("transcript", lambda event: got.append(("first", event)))
    second.on("transcript", lambda event: got.append(("second", event)))

    identical = [fx.transcript_item(shape="push", text="能听见吗", sentence_id="s-1")]
    for meeting_id in (fx.MEETING_ID_INT, fx.OTHER_MEETING_ID_INT):
        fx.deliver(
            channel,
            fx.push_activity(
                [fx.push_item("transcript_received", identical)],
                meeting_id=meeting_id,
                envelope_event_id="env-%s" % meeting_id,
            ),
        )

    await fx.wait_for(lambda: len(got) >= 2, what="one transcript per meeting")
    assert sorted(label for label, _ in got) == ["first", "second"]


async def test_message_layer_dedup_does_not_suppress_a_meeting_event(
    vc, uat_channel
):
    """The two dedup layers share an implementation but must not share a key
    space: platform event ids are global, so one layer marking an id would
    silently swallow the other layer's event."""
    channel, _store, _flow = uat_channel()
    channel._ensure_bg_loop()
    channel.safety.seen.add_sync("evt-collide")

    got = []
    body = fx.poll_events([fx.poll_item("transcript_received", event_id="evt-collide")])
    with fx.fast_sleep():
        session = await channel.follow_my_meeting(user_open_id=fx.USER_OPEN_ID)
        session.on("transcript", lambda event: got.append(event))
        vc.sequence(fx.URI_EVENTS, [body, fx.poll_events([])])
        await fx.wait_for(lambda: got, what="the transcript despite the marked id")
        session.dispose()

    assert channel.safety.seen.has_sync("evt-collide") is True
    assert channel.safety.seen.has_sync("evt-meeting-only") is False
