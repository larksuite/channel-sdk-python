"""Transcript settling: the debounce window, and what happens to the backlog."""

import asyncio

from lark_channel.channel.meeting.stabilizer import MAX_PENDING_TRANSCRIPTS

from . import fixtures as fx


def _transcript(text, sentence_id):
    return fx.push_activity(
        [
            fx.push_item(
                "transcript_received",
                [fx.transcript_item(shape="push", text=text, sentence_id=sentence_id)],
            )
        ],
        envelope_event_id="env-%s-%s" % (sentence_id, len(text)),
    )


async def test_zero_window_delivers_every_revision(vc, tat_channel):
    channel = tat_channel(stabilize_seconds=0.0)
    session = await channel.join_meeting(fx.MEETING_NO)
    got = []
    session.on("transcript", lambda event: got.append(event.text))

    for text in ("今", "今天", "今天来"):
        fx.deliver(channel, _transcript(text, "s-1"))

    await fx.wait_for(lambda: len(got) >= 3, what="three revisions")
    assert got == ["今", "今天", "今天来"]


async def test_positive_window_delivers_only_the_last_revision(vc, tat_channel):
    channel = tat_channel(stabilize_seconds=0.05)
    session = await channel.join_meeting(fx.MEETING_NO)
    got = []
    session.on("transcript", lambda event: got.append(event.text))

    fx.deliver(channel, _transcript("今", "s-1"))
    fx.deliver(channel, _transcript("今天来讨论", "s-1"))

    await fx.wait_for(lambda: got, what="the settled sentence")
    await asyncio.sleep(0.15)
    assert got == ["今天来讨论"]


async def test_pending_sentence_is_flushed_on_dispose_not_dropped(vc, tat_channel):
    """The debounce timer dies with the session; if the buffered sentence dies
    with it, the last thing anybody said in the meeting is lost."""
    channel = tat_channel(stabilize_seconds=5.0)
    session = await channel.join_meeting(fx.MEETING_NO)
    got = []
    session.on("transcript", lambda event: got.append(event.text))

    fx.deliver(channel, _transcript("最后一句", "s-1"))
    await fx.settle()
    assert got == []

    session.dispose()
    await fx.wait_for(lambda: got, what="the buffered sentence to be flushed")
    assert got == ["最后一句"]


async def test_overflowing_buffer_flushes_the_oldest_rather_than_dropping_it(
    vc, tat_channel
):
    channel = tat_channel(stabilize_seconds=5.0)
    session = await channel.join_meeting(fx.MEETING_NO)
    got = []
    session.on("transcript", lambda event: got.append(event.sentence_id))

    items = [
        fx.transcript_item(
            shape="push", text="line-%d" % i, sentence_id="s-%d" % i
        )
        for i in range(MAX_PENDING_TRANSCRIPTS + 1)
    ]
    fx.deliver(channel, fx.push_activity([fx.push_item("transcript_received", items)]))

    await fx.wait_for(
        lambda: got, what="the oldest buffered sentence to be pushed out", timeout=10.0
    )
    assert got == ["s-0"]
