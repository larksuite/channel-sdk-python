"""Confirming the bot is still a participant.

Being removed by a host, or a meeting being transferred, produces **no**
meeting-ended event at all. Without a probe those sessions live until the idle
timeout — which is off by default — so this is the mechanism that makes
reclamation work in the ordinary case.

The probe reuses ``bots/events`` with the app's own credential. That endpoint
takes either identity, and under the app identity it needs exactly the scope
``bots/join`` already needs: being able to join implies being able to probe, so
this costs no new credential and no new authorization. The same call also
backfills whatever the push transport missed, so one request does two jobs.

**Fail open.** Only a request that succeeded *and* said the bot is absent ends
a session. Everything else is "unknown". Probes for every session run on the
same schedule, so their failures are correlated — one network blip or one
missing scope would otherwise end every live session in a single tick.
"""

import time
from typing import Any, Dict, List, Optional, Tuple

from lark_channel.api.vc.bot import build_bot_events_request_as_app

from .api import MeetingApi
from .types import LivenessHealth

#: ``120004`` is a statement about the *bot*: it is not in this meeting.
NOT_IN_MEETING_CODES = frozenset({120004})
#: The evidence key recorded when a probe establishes absence. Pinned rather
#: than derived from the set above: it is part of the health readout's public
#: shape, and the set is explicitly open to gaining more codes.
ABSENCE_EVIDENCE = "120004"
#: ``120003`` is the same HTTP status about a *user*. Treating it as the bot's
#: departure ends live sessions — in follow mode it is not even about the bot.
USER_NOT_IN_MEETING_CODES = frozenset({120003})

#: The endpoint rejects anything below twenty during field validation, so a
#: probe asking for a single item never learns anything about anything.
MIN_PAGE_SIZE = 20

IN_MEETING = "in_meeting"
NOT_IN_MEETING = "not_in_meeting"
UNKNOWN = "unknown"


class LivenessProbe:
    """One place where "is the bot still in this meeting" is decided.

    Kept as its own object so the verdict logic has a single home: what the
    endpoint returns when its documented precondition is violated was only
    established by running it, and the next surprise should be a change to one
    class.
    """

    def __init__(self, api: MeetingApi, health: LivenessHealth) -> None:
        self._api = api
        self._health = health

    async def probe(
        self, *, meeting_id: str, page_token: Optional[str]
    ) -> Tuple[str, List[Dict[str, Any]], Optional[str]]:
        """``(verdict, backfilled events, next page token)``."""
        request = build_bot_events_request_as_app(
            meeting_id=meeting_id,
            page_token=page_token,
            page_size=MIN_PAGE_SIZE,
        )
        result = await self._api.call(request, what="meeting liveness probe")
        if result.ok:
            events = result.data.get("events")
            events = [e for e in events if isinstance(e, dict)] if isinstance(events, list) else []
            next_token = result.data.get("page_token") or page_token
            # An empty list is indistinguishable from a quiet meeting, so it
            # says nothing about participation either way.
            verdict = IN_MEETING if events else UNKNOWN
            self._record(verdict)
            return verdict, events, next_token
        if result.feishu_code in NOT_IN_MEETING_CODES:
            self._record(NOT_IN_MEETING)
            return NOT_IN_MEETING, [], page_token
        self._record(UNKNOWN)
        return UNKNOWN, [], page_token

    def _record(self, verdict: str) -> None:
        self._health.last_probe_at = time.time()
        self._health.last_verdict = verdict
        if verdict == UNKNOWN:
            self._health.consecutive_unknown += 1
        else:
            self._health.consecutive_unknown = 0


__all__ = [
    "ABSENCE_EVIDENCE",
    "IN_MEETING",
    "LivenessProbe",
    "MIN_PAGE_SIZE",
    "NOT_IN_MEETING",
    "NOT_IN_MEETING_CODES",
    "UNKNOWN",
    "USER_NOT_IN_MEETING_CODES",
]
