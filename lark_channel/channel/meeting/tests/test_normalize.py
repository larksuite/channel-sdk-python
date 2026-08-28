"""Unpacking one wire event into a stream of session events.

Every case here drives a real session — the push cases through the channel's
dispatcher, the poll cases through the polling transport — because the two
transports nest the same data differently and only an end-to-end path proves
both nestings are read.
"""

import asyncio
from dataclasses import asdict

from . import fixtures as fx


async def _tat_session(channel):
    return await channel.join_meeting(fx.MEETING_NO)


async def _poll_delivery(vc, uat_channel, body, event_name, *, expected=1, **meeting_kw):
    """Start a follow session, then make the poll transport serve ``body``.

    The response is swapped in *after* the handler is registered so the very
    first poll round cannot deliver before anybody is listening.
    """
    channel, _store, _flow = uat_channel(**meeting_kw)
    got = []
    with fx.fast_sleep():
        session = await channel.follow_my_meeting(user_open_id=fx.USER_OPEN_ID)
        session.on(event_name, lambda event: got.append(event))
        vc.sequence(fx.URI_EVENTS, [body, fx.poll_events([])])
        await fx.wait_for(
            lambda: len(got) >= expected, what="%s events from polling" % expected
        )
        session.dispose()
    return session, got


async def test_every_item_of_every_activity_is_delivered_in_array_order(
    vc, tat_channel
):
    channel = tat_channel()
    session = await _tat_session(channel)
    got = []
    session.on("transcript", lambda event: got.append(event))

    activities = []
    for outer in range(2):
        items = [
            fx.transcript_item(
                shape="push",
                text="line-%d-%d" % (outer, inner),
                sentence_id="sent-%d-%d" % (outer, inner),
            )
            for inner in range(3)
        ]
        activities.append(fx.push_item("transcript_received", items))
    fx.deliver(channel, fx.push_activity(activities))

    await fx.wait_for(lambda: len(got) >= 6, what="six transcripts")
    assert [event.text for event in got] == [
        "line-0-0",
        "line-0-1",
        "line-0-2",
        "line-1-0",
        "line-1-1",
        "line-1-2",
    ]


async def test_push_and_poll_shapes_produce_identical_session_events(
    vc, tat_channel, uat_channel
):
    """The same logical transcript, sent over both transports, must come out
    the other side field-for-field identical."""
    channel = tat_channel()
    session = await _tat_session(channel)
    pushed = []
    session.on("transcript", lambda event: pushed.append(event))
    fx.deliver(channel, fx.push_activity([fx.push_item("transcript_received")]))
    await fx.wait_for(lambda: pushed, what="the pushed transcript")

    _polled_session, polled = await _poll_delivery(
        vc, uat_channel, fx.poll_events([fx.poll_item("transcript_received")]), "transcript"
    )

    assert asdict(pushed[0]) == asdict(polled[0])


async def test_share_ended_reaches_the_handler_before_share_started(vc, tat_channel):
    """Order is the whole meaning of these two: a document swap arrives as
    ended-then-started, and reordering makes the business reconstruct the
    wrong shared document."""
    channel = tat_channel()
    session = await _tat_session(channel)
    order = []

    async def slow_handler(event):
        if event.action == "ended":
            await asyncio.sleep(0.01)
        order.append(event.action)

    session.on("share", slow_handler)
    fx.deliver(
        channel,
        fx.push_activity(
            [
                fx.push_item("magic_share_ended"),
                fx.push_item("magic_share_started"),
            ]
        ),
    )

    await fx.wait_for(lambda: len(order) >= 2, what="both share events")
    assert order == ["ended", "started"]


async def test_each_activity_type_maps_to_its_event_and_its_originator_field(
    vc, tat_channel
):
    channel = tat_channel()
    session = await _tat_session(channel)
    names = ("transcript", "chat", "participant", "share", "document_context")
    got = dict((name, []) for name in names)
    for name in names:
        session.on(name, lambda event, name=name: got[name].append(event))

    fx.deliver(
        channel,
        fx.push_activity([fx.push_item(t) for t in fx.ALL_ACTIVITY_TYPES]),
    )
    await fx.wait_for(
        lambda: sum(len(v) for v in got.values()) >= 7, what="all seven activities"
    )

    assert got["transcript"][0].actor.id == "ou_speaker"
    assert got["chat"][0].actor.id == "ou_chatter"
    assert [e.action for e in got["participant"]] == ["joined", "left"]
    assert [e.actor.id for e in got["participant"]] == ["ou_joiner", "ou_leaver"]
    assert [e.action for e in got["share"]] == ["started", "ended"]
    assert all(e.actor.id == "ou_sharer" for e in got["share"])
    assert got["document_context"][0].actor.id == "ou_editor"


async def test_unknown_activity_type_is_counted_as_empty_without_raising(
    vc, tat_channel
):
    channel = tat_channel()
    session = await _tat_session(channel)
    fx.deliver(channel, fx.push_activity([fx.push_item("brand_new_type")]))

    await fx.wait_for(
        lambda: "brand_new_type" in session.get_stats(),
        what="the unknown type to be accounted for",
    )
    stats = session.get_stats()["brand_new_type"]
    assert stats.received == 1
    assert stats.empty == 1


async def test_document_context_with_no_known_sub_object_is_skipped_not_counted_empty(
    vc, tat_channel
):
    """A fourth kind of document context is forward compatibility, not a
    parse failure — counting it as empty would make the health readout claim
    a field-shape regression that never happened."""
    channel = tat_channel()
    session = await _tat_session(channel)
    got = []
    session.on("document_context", lambda event: got.append(event))
    fx.deliver(
        channel,
        fx.push_activity(
            [
                fx.push_item(
                    "document_context_changed",
                    [fx.document_context_item(shape="push", kind="none")],
                )
            ]
        ),
    )

    await fx.wait_for(
        lambda: "document_context_changed" in session.get_stats(),
        what="the document context item to be accounted for",
    )
    await fx.settle()
    stats = session.get_stats()["document_context_changed"]
    assert stats.received == 1
    assert stats.empty == 0
    assert got == []


async def test_document_context_type_is_derived_from_the_present_sub_object(
    vc, tat_channel
):
    channel = tat_channel()
    session = await _tat_session(channel)
    got = []
    session.on("document_context", lambda event: got.append(event))
    fx.deliver(
        channel,
        fx.push_activity(
            [
                fx.push_item(
                    "document_context_changed",
                    [fx.document_context_item(shape="push", kind=kind)],
                )
                for kind in ("comment_focus", "section_location", "element_preview")
            ]
        ),
    )

    await fx.wait_for(lambda: len(got) >= 3, what="three document context events")
    assert [event.context_type for event in got] == [
        "comment_focus",
        "section_location",
        "element_preview",
    ]


async def test_explicit_context_type_wins_over_the_sub_object_it_disagrees_with(
    vc, tat_channel
):
    """The generated model has no ``context_type`` field at all, so a payload
    that carries one is the platform having moved ahead of it — believe the
    payload."""
    channel = tat_channel()
    session = await _tat_session(channel)
    got = []
    session.on("document_context", lambda event: got.append(event))
    fx.deliver(
        channel,
        fx.push_activity(
            [
                fx.push_item(
                    "document_context_changed",
                    [
                        fx.document_context_item(
                            shape="push",
                            kind="comment_focus",
                            context_type="section_location",
                        )
                    ],
                )
            ]
        ),
    )

    await fx.wait_for(lambda: got, what="the document context event")
    assert got[0].context_type == "section_location"


async def test_string_timestamps_become_ints_and_unparsable_ones_become_none(
    vc, tat_channel
):
    channel = tat_channel()
    session = await _tat_session(channel)
    got = []
    session.on("transcript", lambda event: got.append(event))
    fx.deliver(
        channel,
        fx.push_activity(
            [
                fx.push_item(
                    "transcript_received",
                    [
                        fx.transcript_item(
                            shape="push",
                            sentence_id="parsable",
                            start_time_ms="1730000000123",
                        ),
                        fx.transcript_item(
                            shape="push", sentence_id="garbage", start_time_ms="abc"
                        ),
                        fx.transcript_item(
                            shape="push", sentence_id="absent", start_time_ms=None
                        ),
                    ],
                )
            ]
        ),
    )

    await fx.wait_for(lambda: len(got) >= 3, what="three transcripts")
    assert [event.start_ms for event in got] == [1730000000123, None, None]


async def test_shared_document_is_read_from_share_doc(vc, tat_channel):
    channel = tat_channel()
    session = await _tat_session(channel)
    got = []
    session.on("share", lambda event: got.append(event))
    fx.deliver(channel, fx.push_activity([fx.push_item("magic_share_started")]))

    await fx.wait_for(lambda: got, what="the share event")
    assert got[0].doc.url == "https://example.test/docx/doc_1"
    assert got[0].doc.title == "Design doc"
