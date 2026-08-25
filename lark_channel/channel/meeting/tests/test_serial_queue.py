"""The per-session delivery queue's ceiling.

Serial, awaited delivery means a slow handler makes work pile up at whatever
rate the meeting produces activity. Bounding that pile is not just a memory
question: *which* end is dropped decides whether the application rebuilds the
meeting correctly or rebuilds something plausible and wrong.
"""

import pytest

from lark_channel.channel.meeting.health_view import MeetingHealthView
from lark_channel.channel.meeting.serial_queue import (
    REPORT_RESERVE,
    SerialDelivery,
)

from . import fixtures as fx


def _delivery(**kwargs):
    return SerialDelivery(on_handler_error=lambda exc: None, **kwargs)


def test_a_full_queue_refuses_more_activity():
    """Unbounded growth is the failure this ceiling exists to prevent: nothing
    consumes while a handler is parked, and the meeting keeps producing."""
    queue = _delivery(max_queued=3)
    noop = lambda payload: None

    assert [queue.submit([noop], i) for i in range(3)] == [True, True, True]
    assert queue.submit([noop], 3) is False
    assert queue.dropped == 1


async def test_overflow_drops_the_newest_so_what_is_queued_stays_in_order():
    """Order is the guarantee this queue exists for. A document swap arrives as
    `magic_share_ended` then `magic_share_started`, so evicting from the front
    to make room splits that pair and hands the application a queue that still
    looks complete. An implementation that evicts the oldest must fail here.
    """
    seen = []
    queue = _delivery(max_queued=2)
    record = lambda payload: seen.append(payload)

    queue.submit([record], "magic_share_ended")
    queue.submit([record], "magic_share_started")
    queue.submit([record], "arrived_after_the_ceiling")

    queue.start()
    try:
        await fx.wait_for(lambda: len(seen) == 2, what="the two queued deliveries")
        assert seen == ["magic_share_ended", "magic_share_started"]
    finally:
        # The worker outlives the test otherwise, and its coroutine is dropped
        # when the loop closes — the same leak this suite errors on.
        queue.cancel()


def test_an_error_report_gets_through_a_queue_full_of_activity():
    """Teardown submits here — the `end` event, and any error raised by the
    departure call. Those are what explain why a session went away, so a
    ceiling filled by transcripts must not be able to drop the explanation and
    keep the noise."""
    queue = _delivery(max_queued=1)
    noop = lambda payload: None

    queue.submit([noop], "activity")
    assert queue.submit([noop], "more activity") is False
    assert queue.submit([noop], "why the session ended", reserved=True) is True


def test_the_report_headroom_is_itself_bounded():
    """Reserved is headroom, not an exemption — otherwise a handler that raises
    on every delivery would grow the queue through the reports about it."""
    queue = _delivery(max_queued=0)
    noop = lambda payload: None

    accepted = sum(
        1 for _ in range(REPORT_RESERVE + 5) if queue.submit([noop], "r", reserved=True)
    )
    assert accepted == REPORT_RESERVE


def test_drops_reach_the_channel_readout_from_live_and_retired_sessions():
    """A drop only this object knows about is exactly the silent loss the health
    readout exists to make diagnosable — and it has to survive the session
    ending, which is when somebody goes looking."""

    class _Session:
        def __init__(self, dropped):
            self.dropped_deliveries = dropped

        def get_stats(self):
            return {}

    live = _Session(2)
    view = MeetingHealthView(lambda: [live])
    view.retire(_Session(3))

    assert view.snapshot().dropped == 5
