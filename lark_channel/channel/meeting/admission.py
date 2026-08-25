"""The gate in front of both entry points.

Sessions are created from **outside** the process: joining starts at an
invitation from anybody who can add the bot to a meeting, following typically at
an inbound instruction. So the ceiling is not a tuning knob, it is the only
thing standing between a hostile-or-clumsy caller and unbounded growth of
sessions, timers, settle buffers and dedup entries.

Both entry points share it, and the reading covers **live local sessions plus
server-side participation**. Counting only live sessions makes it drop to zero
after a ``dispose()`` while the bot is still sitting in the meeting; counting
only participation misses follow sessions, which take no seat server-side and
whose reclamation depends on nothing but the gate.

Reclamation runs here rather than on a timer: this is the moment a seat is
actually wanted, and it needs no resource that outlives ``disconnect()``.
"""

import asyncio
from typing import Any, Callable, Dict, Optional, Set

from ..errors import FeishuChannelError, FeishuChannelErrorCode


class AdmissionGate:
    """Decides whether another meeting session may be created."""

    def __init__(
        self,
        *,
        max_concurrent_sessions: int,
        live_meetings: Callable[[], Set[str]],
        held_meetings: Callable[[], Set[str]],
        reconcile: Callable[[], Any],
    ) -> None:
        self._ceiling = max_concurrent_sessions
        self._live_meetings = live_meetings
        self._held_meetings = held_meetings
        self._reconcile = reconcile
        self._joining: Dict[str, "asyncio.Future"] = {}

    async def admit(self) -> None:
        """Reclaim what can be reclaimed, then compare against the ceiling."""
        await self._reconcile()
        occupied = self._live_meetings() | self._held_meetings()
        if len(occupied) >= self._ceiling:
            raise FeishuChannelError(
                FeishuChannelErrorCode.TOO_MANY_SESSIONS,
                "the meeting session ceiling (%d) is reached" % self._ceiling,
            )

    def joining(self, meeting_no: str) -> Optional["asyncio.Future"]:
        """The in-flight join for ``meeting_no``, if there is one."""
        return self._joining.get(meeting_no)

    def claim(self, meeting_no: str) -> "asyncio.Future":
        """Register an in-flight join. **Call before the first await.**

        Claiming after the admission check leaves a window where two concurrent
        calls both pass it, both call the platform, and the loser's session is
        evicted from the routing table with its probe loop still running.
        """
        future: "asyncio.Future" = asyncio.get_event_loop().create_future()
        self._joining[meeting_no] = future
        return future

    def release(self, meeting_no: str) -> None:
        self._joining.pop(meeting_no, None)


__all__ = ["AdmissionGate"]
