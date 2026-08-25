"""Assembling the channel-wide health readout.

Per-activity-type counters live on the sessions that saw the traffic, but the
readout is channel-level and has to survive the sessions ending — otherwise the
numbers reset every time a meeting finishes, which is exactly when somebody
goes looking at them.
"""

from typing import Any, Callable, Dict, Iterable, Optional

from .normalize import MAX_STAT_KEYS, OTHER_STAT_KEY
from .types import ActivityTypeStats, MeetingEventHealth


class MeetingHealthView:
    """Owns the channel-level health object and folds session stats into it."""

    def __init__(self, sessions: Callable[[], Iterable[Any]]) -> None:
        self._sessions = sessions
        self._health = MeetingEventHealth()
        self._retired: Dict[str, ActivityTypeStats] = {}
        self._retired_dropped = 0

    @property
    def health(self) -> MeetingEventHealth:
        """The mutable health object; callers may bump counters on it."""
        return self._health

    def mark_registration(self, *, ok: bool, reason: Optional[str]) -> None:
        self._health.registered = ok
        self._health.reason = reason

    def retire(self, session: Any) -> None:
        """Fold a departing session's counters in before it disappears.

        Bounded the same way the per-session map is: these keys come from the
        server and this map lives for the whole process, so the per-session
        ceiling alone would not hold it.
        """
        for key, stats in session.get_stats().items():
            self._merge(self._retired, key, stats)
        self._retired_dropped += session.dropped_deliveries

    def snapshot(self) -> MeetingEventHealth:
        aggregate: Dict[str, ActivityTypeStats] = {}
        dropped = self._retired_dropped
        for session in self._sessions():
            dropped += session.dropped_deliveries
            for key, stats in session.get_stats().items():
                self._merge(aggregate, key, stats)
        for key, stats in self._retired.items():
            self._merge(aggregate, key, stats)
        self._health.stats = aggregate
        self._health.dropped = dropped
        return self._health

    @staticmethod
    def _merge(
        target: Dict[str, ActivityTypeStats], key: str, stats: ActivityTypeStats
    ) -> None:
        if key not in target and len(target) >= MAX_STAT_KEYS:
            key = OTHER_STAT_KEY
        merged = target.setdefault(key, ActivityTypeStats())
        merged.received += stats.received
        merged.empty += stats.empty


__all__ = ["MeetingHealthView"]
