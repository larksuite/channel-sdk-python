"""Joined as the bot: activity arrives on the socket, the probe fills the gaps.

Nothing to poll — the channel's dispatcher routes activity here by meeting id.
What this source owns is the two watchdogs a joined session needs: the liveness
probe (which also backfills) and the optional idle deadline.
"""

import asyncio
from typing import Any, Callable, List, Optional

from lark_channel.core.log import logger

from ..liveness import IN_MEETING, NOT_IN_MEETING


class PushSource:
    """Watchdogs for a joined session. Activity itself is pushed in."""

    mode = "tat"

    def __init__(
        self,
        *,
        meeting_id: str,
        probe: Any,
        probe_interval_seconds: float,
        idle_timeout_seconds: float,
        deliver: Callable[[List[dict]], Any],
        on_absent: Callable[[], Any],
        on_idle: Callable[[], Any],
        confirm_membership: Callable[[], None],
    ) -> None:
        self._meeting_id = meeting_id
        self._probe = probe
        self._probe_interval = probe_interval_seconds
        self._idle_timeout = idle_timeout_seconds
        self._deliver = deliver
        self._on_absent = on_absent
        self._on_idle = on_idle
        self._confirm_membership = confirm_membership
        self._tasks: List[asyncio.Task] = []
        self._closing: List[asyncio.Task] = []
        self._page_token: Optional[str] = None
        self._last_activity = 0.0
        self._running = False

    def start(self) -> None:
        self._running = True
        self.touch()
        if self._probe_interval > 0:
            self._tasks.append(asyncio.ensure_future(self._probe_loop()))
        if self._idle_timeout > 0:
            self._tasks.append(asyncio.ensure_future(self._idle_loop()))

    def touch(self) -> None:
        """Note that something happened, for the idle deadline."""
        self._last_activity = asyncio.get_event_loop().time()

    def stop(self) -> None:
        """Stop the loops. Never cancels the caller's own task.

        Teardown is often triggered *from* one of these loops — an idle
        deadline expiring, a probe proving the bot is gone. Cancelling the
        running task there would raise at its next ``await``, which is inside
        the teardown itself, so the departure call would never be made and the
        seat would never come back. The clear flag lets that task finish on its
        own instead.
        """
        self._running = False
        try:
            current = asyncio.current_task()
        except RuntimeError:  # pragma: no cover - no running loop
            current = None
        for task in self._tasks:
            if task is not current and not task.done():
                task.cancel()
        # Kept so teardown can wait for the cancellations to land. A cancelled
        # task still needs one turn of the loop to unwind, and a loop that stops
        # before that turn reports it as destroyed-while-pending — which is both
        # noise and a real leak in a long-lived process.
        self._closing = [t for t in self._tasks if t is not current]
        self._tasks = []

    async def wait_closed(self) -> None:
        """Wait for the cancelled loops to unwind. Safe to call repeatedly."""
        pending = [t for t in self._closing if not t.done()]
        self._closing = []
        if pending:
            await asyncio.gather(*pending, return_exceptions=True)

    async def _probe_loop(self) -> None:
        try:
            while self._running:
                await asyncio.sleep(self._probe_interval)
                if not self._running:
                    return
                verdict, events, next_token = await self._probe.probe(
                    meeting_id=self._meeting_id, page_token=self._page_token
                )
                self._page_token = next_token
                if verdict == IN_MEETING:
                    # Two different clocks. `touch()` is the idle deadline;
                    # `confirm_membership()` is the accounting deadline, which
                    # is a backstop for mis-accounting and must be refreshed by
                    # anything that proves the meeting is still ours. A long
                    # meeting with no pushed activity would otherwise have its
                    # seat released while the bot is demonstrably still in it.
                    self._confirm_membership()
                if events:
                    # Backfill counts as activity: the push path being quiet
                    # while the probe keeps finding events is exactly the case
                    # the idle deadline must not reclaim.
                    self.touch()
                    self._confirm_membership()
                    await self._deliver(events)
                if verdict == NOT_IN_MEETING:
                    await self._on_absent()
                    return
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # pragma: no cover - defensive
            logger.warning("meeting: liveness loop stopped: %s", type(exc).__name__)

    async def _idle_loop(self) -> None:
        try:
            while self._running:
                await asyncio.sleep(max(0.01, self._idle_timeout / 4.0))
                if not self._running:
                    return
                idle_for = asyncio.get_event_loop().time() - self._last_activity
                if idle_for >= self._idle_timeout:
                    await self._on_idle()
                    return
        except asyncio.CancelledError:
            raise


__all__ = ["PushSource"]
