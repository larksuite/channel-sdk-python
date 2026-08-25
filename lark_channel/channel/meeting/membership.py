"""Server-side participation accounting — what the concurrency gate reads.

The gate cannot count live sessions. ``dispose()`` stops local work but leaves
the bot a participant, and a failed departure removes the session while the
seat is still taken. So a seat is released on **evidence that the bot is no
longer a participant**, which is a different thing from "the departure call
returned 200".

Both directions have to hold, and each failure is severe in its own way:

* release only on a clean departure, and every normally-ended meeting leaks a
  seat — ending is exactly when that call is most likely to 404. After
  ``max_concurrent_sessions`` such meetings both entry points are dead for the
  life of the process, and one tenant member looping "invite the bot, end the
  meeting" is enough to get there.
* release on any failure and the gate is off.

The awkward case is a departure whose outcome is unknown (5xx, timeout). The
seat is kept — but three rules stack into a dead end if nothing else is done:
the seat is kept, the session is still removed, and routing drops events for
meetings with no session. So accounting keeps listening after delivery stops
(``release`` is reachable from the router even with no session), and admission
reconciles the leftovers.

Reconciliation is **lazy**: it runs on admission, when evidence arrives, and on
connect. Not on a timer. A timer that survived ``disconnect()`` would need its
own thread, and a thread still running after the call whose entire job is
releasing resources is a dangling resource. A seat only needs reclaiming when
somebody wants one, and that is exactly when admission runs.
"""

import asyncio
import time
from typing import Any, Awaitable, Callable, Dict, List, Optional, Set

from lark_channel.core.log import logger

from .errors import sanitize_for_log
from .types import MembershipHealth

#: Evidence keys, as they appear in ``MembershipHealth.released_by_evidence``.
EVIDENCE_OK = "ok"
EVIDENCE_MEETING_ENDED = "meeting_ended"
EVIDENCE_TTL = "ttl"

#: Departure failures that prove the seat is already gone server-side.
#: ``121105`` is "meeting not exist" — but it also fires when *we* have been
#: sending the wrong meeting id all along, so releasing on it is reported.
ABSENCE_FEISHU_CODES = frozenset({121105, 120004})
ABSENCE_HTTP_STATUSES = frozenset({404})
#: Absence codes that are *also* what a systematically wrong meeting id looks
#: like, so releasing on them is reported rather than absorbed.
AMBIGUOUS_ABSENCE_CODES = frozenset({"121105"})


class _Entry:
    __slots__ = (
        "meeting_id",
        "added_at",
        "confirmed_at",
        "last_attempt_at",
        "departure_unresolved",
    )


    def __init__(self, meeting_id: str) -> None:
        self.meeting_id = meeting_id
        self.added_at = time.monotonic()
        #: Last time something proved this meeting is still ours.
        self.confirmed_at = self.added_at
        # `None`, not 0.0: `time.monotonic()` has an arbitrary origin — on some
        # platforms it starts near zero at process start — so a zero sentinel
        # reads as "attempted just now" for the first minute of the process and
        # silently suppresses the first reconciliation.
        self.last_attempt_at = None
        # Set only once a departure has actually been attempted and come back
        # inconclusive. Reconciliation keys on this rather than on "the session
        # is gone", because `dispose()` deliberately does *not* depart: a
        # reconnect must not walk the bot out of its meetings, so reconciling
        # every session-less entry would turn dispose into leave.
        self.departure_unresolved = False


class MembershipLedger:
    """Which meetings the bot is a participant of, as far as we can tell."""

    def __init__(
        self,
        *,
        health: MembershipHealth,
        max_age_seconds: float,
        reconcile_interval_seconds: float,
        reconcile_max_concurrency: int,
        reconcile_attempt_timeout_seconds: float,
        depart: Callable[[str], Awaitable[Any]],
        live_meetings: Callable[[], Set[str]],
    ) -> None:
        self._health = health
        self._max_age = max_age_seconds
        self._interval = reconcile_interval_seconds
        self._max_concurrency = max(1, reconcile_max_concurrency)
        self._attempt_timeout = reconcile_attempt_timeout_seconds
        self._depart = depart
        # Derived, never stored. "Does this meeting have a live session" used to
        # be a flag on the entry, and keeping it correct depended on the order
        # of `add()` and `dispose()` — which the supersede path gets backwards:
        # the new session is accounted for first, then the old one's teardown
        # clears the flag, then the new session is registered. Reading the
        # routing table instead removes the ordering as a correctness concern.
        self._live_meetings = live_meetings
        self._entries: Dict[str, _Entry] = {}

    # -- accounting ------------------------------------------------------
    def add(self, meeting_id: str) -> None:
        """Record that the bot is a participant of ``meeting_id``.

        Re-joining a meeting we still have an entry for resets it. Carrying the
        old ``departure_unresolved`` over would count a meeting with a live
        session as one whose departure is unresolved, and send reconciliation
        after it every interval — each attempt correctly refused by the
        requester guard, each one logging a warning that says the opposite of
        what is true.
        """
        if not meeting_id:
            return
        existing = self._entries.get(meeting_id)
        if existing is None:
            self._entries[meeting_id] = _Entry(meeting_id)
        else:
            existing.departure_unresolved = False
            existing.confirmed_at = time.monotonic()
        self._sync_health()

    def touch(self, meeting_id: str) -> None:
        """Note that this meeting is demonstrably still ours.

        The deadline below is a backstop for *mis-accounting*, so it has to be
        measured from the last evidence rather than from when the entry was
        created. Without this, a meeting that genuinely runs longer than
        ``membership_max_age_seconds`` has its seat released while the session
        is alive and the bot is still in the room — and the release logs a
        warning claiming our accounting is wrong.
        """
        entry = self._entries.get(meeting_id)
        if entry is not None:
            entry.confirmed_at = time.monotonic()

    def release(self, meeting_id: str, *, evidence: str) -> bool:
        """Give the seat back. ``True`` if it was held."""
        if self._entries.pop(meeting_id, None) is None:
            return False
        counts = self._health.released_by_evidence
        counts[evidence] = counts.get(evidence, 0) + 1
        if evidence in AMBIGUOUS_ABSENCE_CODES:
            # Also what a systematically wrong meeting id looks like. Absorbing
            # it silently would turn the gate off and keep every test green.
            logger.warning(
                "meeting: released a seat on %s (meeting not exist) for meeting %s; "
                "if this repeats, the meeting id being sent is likely wrong",
                evidence,
                sanitize_for_log(meeting_id),
            )
        self._sync_health()
        return True

    def held(self) -> Set[str]:
        """The seats currently taken.

        Expiry runs here because this is read on every admission and every
        health check, and it is purely local. Leaving it only inside
        ``reconcile`` would make the deadline depend on somebody attempting a
        new join — so a process that stops joining never releases anything.
        """
        self._expire_overdue()
        return set(self._entries)

    def note_departure_outcome(self, meeting_id: str, result: Any) -> None:
        """Apply whatever a departure attempt proved about the seat."""
        if result is None:
            return
        if getattr(result, "ok", False):
            self.release(meeting_id, evidence=EVIDENCE_OK)
            return
        code = getattr(result, "feishu_code", None)
        status = getattr(result, "status", None)
        if code in ABSENCE_FEISHU_CODES:
            self.release(meeting_id, evidence=str(code))
            return
        if status in ABSENCE_HTTP_STATUSES:
            self.release(meeting_id, evidence=str(status))
            return
        # Unknown outcome: keep the seat and mark it for reconciliation.
        entry = self._entries.get(meeting_id)
        if entry is not None:
            entry.departure_unresolved = True
            self._sync_health()

    # -- lazy reconciliation --------------------------------------------
    async def reconcile(self) -> None:
        """Work through seats whose session is gone. Cheap when there are none.

        Called from admission (before the ceiling is compared), when evidence
        arrives, and from ``connect()``. Deliberately not scheduled.
        """
        self._expire_overdue()
        if self._interval <= 0:
            return
        now = time.monotonic()
        candidates = [
            entry
            for entry in list(self._entries.values())
            if entry.departure_unresolved
            and (
                entry.last_attempt_at is None
                or (now - entry.last_attempt_at) >= self._interval
            )
        ]
        if not candidates:
            return
        for batch_start in range(0, len(candidates), self._max_concurrency):
            batch = candidates[batch_start : batch_start + self._max_concurrency]
            await asyncio.gather(
                *[self._attempt(entry) for entry in batch], return_exceptions=True
            )

    async def _attempt(self, entry: _Entry) -> None:
        entry.last_attempt_at = time.monotonic()
        self._health.reconcile_attempts += 1
        try:
            result = await asyncio.wait_for(
                self._depart(entry.meeting_id), timeout=self._attempt_timeout
            )
        except Exception:
            # Still unknown; the deadline is the backstop.
            return
        self.note_departure_outcome(entry.meeting_id, result)

    def _expire_overdue(self) -> None:
        if self._max_age <= 0:
            return
        now = time.monotonic()
        live = self._live_meetings()
        for meeting_id, entry in list(self._entries.items()):
            if meeting_id in live:
                # A seat with a live session is not mis-accounted, and this
                # deadline exists only to catch mis-accounting. Expiring it
                # would release the seat of a meeting the bot is demonstrably
                # still in — a two-hour meeting where nobody happens to speak
                # produces no activity and no conclusive probe, so the clock
                # would run out on a perfectly healthy session.
                continue
            if (now - entry.confirmed_at) < self._max_age:
                continue
            logger.warning(
                "meeting: releasing seat for meeting %s after %.0fs without a "
                "conclusive departure; the accounting for it is wrong",
                sanitize_for_log(meeting_id),
                now - entry.confirmed_at,
            )
            self.release(meeting_id, evidence=EVIDENCE_TTL)

    # -- health ----------------------------------------------------------
    def _sync_health(self) -> None:
        self._health.held = len(self._entries)
        # Entries whose departure was attempted and left unresolved — the ones
        # reconciliation works through. Not merely "has no session": a disposed
        # session leaves the bot in the meeting on purpose.
        self._health.retained_without_session = sum(
            1 for entry in self._entries.values() if entry.departure_unresolved
        )


__all__ = [
    "ABSENCE_FEISHU_CODES",
    "AMBIGUOUS_ABSENCE_CODES",
    "ABSENCE_HTTP_STATUSES",
    "EVIDENCE_MEETING_ENDED",
    "EVIDENCE_OK",
    "EVIDENCE_TTL",
    "MembershipLedger",
]
