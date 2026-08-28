"""The meeting channel: routing, the concurrency gate, and both entry points.

Pushed activity is **application-wide** — every meeting this app is in arrives
on one stream, distinguished by ``meeting.id``. Business code faces one meeting
at a time, so this is where that one routing step happens; each
:class:`MeetingSession` then only ever sees its own meeting.

Session creation is triggered from **outside** the process: joining starts at
an invitation from anybody who can add the bot to a meeting, following typically
at an inbound instruction. So both entry points share one ceiling, and both get
an identity filter — a count gate and an identity gate answer different
questions and neither substitutes for the other.
"""

import asyncio
import time
from typing import Any, Callable, Dict, List, Optional, Set, Tuple

from lark_channel.api.vc.bot import (
    build_bot_join_request,
    build_bot_leave_request,
    build_user_active_meeting_request,
)
from lark_channel.core.log import logger

from ..errors import FeishuChannelError, FeishuChannelErrorCode
from .admission import AdmissionGate
from .api import MeetingApi
from .coerce import meeting_id_str
from .errors import sanitize_for_log
from .health_view import MeetingHealthView
from .liveness import ABSENCE_EVIDENCE, LivenessProbe
from .membership import EVIDENCE_MEETING_ENDED, MembershipLedger
from .normalize import meeting_id_of
from .session import MeetingSession
from .sources import PollSource, PushSource
from .types import (
    MeetingActor,
    MeetingEventHealth,
    MeetingInvitedEvent,
    MeetingOptions,
)

ACTIVITY_EVENT = "vc.bot.meeting_activity_v1"
INVITED_EVENT = "vc.bot.meeting_invited_v1"
ENDED_EVENT = "vc.bot.meeting_ended_v1"
INTERNAL_EVENT_TYPES = (ACTIVITY_EVENT, INVITED_EVENT, ENDED_EVENT)

#: Warned about once per process, on the first ``follow_my_meeting`` call.
_COMPLIANCE_NOTICE = (
    "meeting: follow_my_meeting reads every participant's speech for the whole "
    "meeting under a user's own authorization, and the bot is not visible in "
    "the meeting. Telling participants and obtaining their consent is the "
    "integrating application's responsibility; this SDK does not do it."
)


class MeetingChannel:
    """Owns every meeting session on one channel."""

    def __init__(
        self,
        *,
        client: Any,
        config: Any,
        seen: Any,
        bot_open_id_getter: Callable[[], Optional[str]],
        schedule: Callable[[Any], Any],
        resolve_ticket_interactive: Callable[..., Any],
        resolve_ticket_quiet: Callable[[str], Any],
        emit_invited: Callable[[MeetingInvitedEvent], Any],
        timeout_seconds: float = 30.0,
    ) -> None:
        self._config = config
        self._meeting_config = config.meeting
        self._seen = seen
        self._bot_open_id_getter = bot_open_id_getter
        self._schedule = schedule
        self._resolve_ticket_interactive = resolve_ticket_interactive
        self._resolve_ticket_quiet = resolve_ticket_quiet
        self._emit_invited = emit_invited

        self._api = MeetingApi(client, timeout_seconds=timeout_seconds)
        self._sessions: Dict[str, MeetingSession] = {}
        self._health_view = MeetingHealthView(lambda: list(self._sessions.values()))
        self._health = self._health_view.health
        self._probe = LivenessProbe(self._api, self._health.liveness)
        self._membership = MembershipLedger(
            health=self._health.membership,
            max_age_seconds=self._meeting_config.membership_max_age_seconds,
            reconcile_interval_seconds=(
                self._meeting_config.membership_reconcile_interval_seconds
            ),
            reconcile_max_concurrency=(
                self._meeting_config.membership_reconcile_max_concurrency
            ),
            reconcile_attempt_timeout_seconds=(
                self._meeting_config.membership_reconcile_attempt_timeout_seconds
            ),
            depart=self._depart,
            live_meetings=self._live_meeting_ids,
        )
        self._follow_by_user: Dict[Tuple[str, str], MeetingSession] = {}
        self._gate = AdmissionGate(
            max_concurrent_sessions=self._meeting_config.max_concurrent_sessions,
            live_meetings=self._live_meeting_ids,
            held_meetings=self._membership.held,
            reconcile=self._membership.reconcile,
        )
        self._compliance_warned = False

    def _live_meeting_ids(self) -> Set[str]:
        """The meetings this process currently has a session for.

        One method, two readers — the accounting ledger and the admission gate.
        The value of not storing this is that both read the same source; two
        copies of the same lambda is the one edit that could let them diverge
        again.
        """
        return set(self._sessions)

    # -- health ----------------------------------------------------------
    def health(self) -> MeetingEventHealth:
        # Reading `held()` is what expires overdue entries, so a caller that
        # only ever reads health still sees a current seat count rather than
        # seats that passed their deadline hours ago.
        self._membership.held()
        return self._health_view.snapshot()

    def mark_registration(self, *, ok: bool, reason: Optional[str]) -> None:
        self._health_view.mark_registration(ok=ok, reason=reason)

    def on_connected(self) -> None:
        """The channel just became ready. One of the three reconciliation points.

        Reconnecting is a good moment for it: a seat stranded by an
        inconclusive departure before the connection dropped is exactly what a
        fresh connection wants back, and this needs no timer.
        """
        self._schedule(self._membership.reconcile())

    # -- dispatcher entry points ----------------------------------------
    def on_activity(self, payload: Dict[str, Any]) -> None:
        self._schedule(self._handle_activity(payload))

    def on_invited(self, payload: Dict[str, Any]) -> None:
        self._schedule(self._handle_invited(payload))

    def on_ended(self, payload: Dict[str, Any]) -> None:
        self._schedule(self._handle_ended(payload))

    async def _handle_activity(self, payload: Dict[str, Any]) -> None:
        event = payload.get("event") or {}
        envelope_meeting = meeting_id_str((event.get("meeting") or {}).get("id"))
        activities = event.get("meeting_activity_items")
        if not isinstance(activities, list):
            return
        self._health.received += 1
        self._health.last_at = time.time()
        grouped: Dict[Optional[str], List[Dict[str, Any]]] = {}
        for activity in activities:
            if not isinstance(activity, dict):
                continue
            meeting_id = meeting_id_of(activity, envelope_meeting)
            grouped.setdefault(meeting_id, []).append(activity)
        for meeting_id, batch in grouped.items():
            session = self._sessions.get(meeting_id) if meeting_id else None
            if session is None:
                # Not a meeting this process runs, so there is nobody to
                # deliver to. Accounting is not skipped by this: the paths that
                # release a seat — `_handle_ended`, the liveness probe — do not
                # go through here.
                continue
            # Activity is evidence that this meeting is still ours, which is
            # what keeps a long meeting from hitting the accounting deadline.
            self._membership.touch(meeting_id)
            await session.ingest(batch)

    async def _handle_invited(self, payload: Dict[str, Any]) -> None:
        event = payload.get("event") or {}
        meeting = event.get("meeting") or {}
        inviter = _actor_from(event.get("inviter"))
        allowlist = self._meeting_config.invite_allowlist
        if allowlist is not None and (inviter.id or "") not in allowlist:
            logger.warning(
                "meeting: ignoring an invitation from an inviter outside "
                "invite_allowlist (meeting_no=%s)",
                sanitize_for_log(meeting.get("meeting_no")),
            )
            return
        invited = MeetingInvitedEvent(
            meeting_no=meeting.get("meeting_no") or "",
            meeting_id=meeting_id_str(meeting.get("id")),
            topic=meeting.get("topic"),
            inviter=inviter,
            bot=_actor_from(event.get("bot")),
            call_id=event.get("call_id"),
            invite_time=event.get("invite_time"),
        )
        await self._emit_invited(invited)

    async def _handle_ended(self, payload: Dict[str, Any]) -> None:
        event = payload.get("event") or {}
        meeting_id = meeting_id_str((event.get("meeting") or {}).get("id"))
        if not meeting_id:
            return
        # Accounting first, and unconditionally: the meeting is over
        # server-side, so the seat is gone whether or not a departure call ever
        # succeeded — and whether or not a session still exists to deliver to.
        # This is the rule that keeps a 5xx departure from stranding a seat.
        self._membership.release(meeting_id, evidence=EVIDENCE_MEETING_ENDED)
        # Evidence arriving is one of the three moments reconciliation runs:
        # a meeting ending is often when a *previous* inconclusive departure
        # becomes resolvable.
        await self._membership.reconcile()
        session = self._sessions.get(meeting_id)
        if session is None:
            return
        await self._retire(session, reason="meeting_ended", depart=True)

    # -- entry points ----------------------------------------------------
    async def join_meeting(
        self,
        meeting_no: str,
        *,
        connected: bool,
        password: Optional[str] = None,
        call_id: Optional[str] = None,
        options: Optional[MeetingOptions] = None,
    ) -> MeetingSession:
        if not connected:
            raise FeishuChannelError(
                FeishuChannelErrorCode.NOT_CONNECTED,
                "join_meeting needs the event socket: in-meeting activity is "
                "pushed, so without connect() the session would receive nothing",
            )
        inflight = self._gate.joining(meeting_no)
        if inflight is not None:
            return await asyncio.shield(inflight)
        # Claimed **before** the first await. Registering it after the admission
        # check leaves a window where two concurrent calls both pass the check,
        # both call the platform, and the loser's session is silently evicted
        # from the routing table with its probe loop still running.
        future = self._gate.claim(meeting_no)
        try:
            await self._gate.admit()
            session = await self._do_join(
                meeting_no, password=password, call_id=call_id, options=options
            )
        except BaseException as exc:
            future.set_exception(exc)
            # Nobody may be awaiting this future; retrieving it here keeps the
            # loop from reporting it as never-retrieved.
            future.exception()
            raise
        else:
            future.set_result(session)
            return session
        finally:
            self._gate.release(meeting_no)
            # Dropped from this frame's locals. On the failing path this frame
            # is part of the raised error's `__traceback__`, and a crash
            # reporter that reads frame locals reads the password with it.
            password = None

    async def _do_join(
        self,
        meeting_no: str,
        *,
        password: Optional[str],
        call_id: Optional[str],
        options: Optional[MeetingOptions],
    ) -> MeetingSession:
        request = build_bot_join_request(
            meeting_no=meeting_no, password=password, call_id=call_id
        )
        result = await self._api.call(request, what="meeting join")
        password = None  # see `join_meeting`: this frame unwinds on failure
        if not result.ok:
            if result.transport_error:
                # The platform may already have admitted the bot while the
                # answer was lost. There is nothing useful to send: departure
                # rejects a nine-digit number, and the long id was in the reply
                # that never arrived.
                logger.warning(
                    "meeting: join for %s failed without an answer; the bot may "
                    "already be a participant and cannot be removed automatically",
                    sanitize_for_log(meeting_no),
                )
            raise self._api.error_for(result, what="meeting join")
        meeting = result.data.get("meeting") or {}
        meeting_id = meeting_id_str(meeting.get("id"))
        if not meeting_id:
            # The response echoes the password back, so the same reasoning as
            # the request side applies in the other direction: this frame is
            # part of the raised error's `__traceback__`, and its locals still
            # reach that echo. `docs/security.md` promises passwords stay out
            # of error objects in *both* directions.
            meeting = None
            result = None
            raise FeishuChannelError(
                FeishuChannelErrorCode.UNKNOWN, "meeting join returned no meeting id"
            )
        self._membership.add(meeting_id)
        # Only these three fields are kept. Some meeting responses carry a
        # plaintext password, and anything kept here lives as long as the
        # session does.
        session = self._new_session(
            meeting_id=meeting_id,
            meeting_no=meeting.get("meeting_no") or meeting_no,
            topic=meeting.get("topic"),
            mode="tat",
            options=options,
        )
        source = PushSource(
            meeting_id=meeting_id,
            probe=self._probe,
            probe_interval_seconds=self._meeting_config.liveness_probe_interval_seconds,
            idle_timeout_seconds=self._meeting_config.idle_timeout_seconds,
            deliver=lambda events: session.ingest(events),
            on_absent=lambda: self._probe_said_absent(session),
            on_idle=lambda: self._retire(session, reason="idle_timeout", depart=True),
            confirm_membership=lambda: self._membership.touch(meeting_id),
        )
        session.attach(source)
        return session

    async def follow_my_meeting(
        self,
        *,
        user_open_id: str,
        prompt_context: Any = None,
        meeting_no: Optional[str] = None,
        options: Optional[MeetingOptions] = None,
    ) -> MeetingSession:
        # Identity gate first: before the ticket store is touched, before the
        # network, and before session reuse — reuse is keyed on the meeting, so
        # checking afterwards would let an unlisted caller inherit somebody
        # else's live session and with it that person's transcript.
        allowlist = self._meeting_config.follow_allowlist
        if allowlist is not None and user_open_id not in allowlist:
            raise FeishuChannelError(
                FeishuChannelErrorCode.PERMISSION_DENIED,
                "this open_id is not in meeting.follow_allowlist",
            )
        if not self._compliance_warned:
            self._compliance_warned = True
            logger.warning(_COMPLIANCE_NOTICE)
        # Reuse is keyed on the resolved meeting, not on the requested
        # `meeting_no` — which is optional and usually `None`, so keying on it
        # degrades to "one session per user" and hands back a stale session
        # after the user has moved to a different meeting.
        await self._gate.admit()
        token = await self._resolve_ticket_interactive(
            user_open_id=user_open_id, prompt_context=prompt_context
        )
        try:
            selected = await self._select_active_meeting(
                user_open_id=user_open_id, token=token, meeting_no=meeting_no
            )
        finally:
            token = None
        meeting_id, resolved_no, topic = selected
        reuse_key = (user_open_id, meeting_id)
        existing = self._follow_by_user.get(reuse_key)
        if existing is not None:
            return existing
        session = self._sessions.get(meeting_id)
        if session is not None:
            self._follow_by_user[reuse_key] = session
            return session
        session = self._new_session(
            meeting_id=meeting_id,
            meeting_no=resolved_no,
            topic=topic,
            mode="uat",
            options=options,
        )
        self._follow_by_user[reuse_key] = session
        source = PollSource(
            meeting_id=meeting_id,
            api=self._api,
            config=self._meeting_config,
            resolve_ticket=lambda: self._resolve_ticket_quiet(user_open_id),
            deliver=lambda events: session.ingest(events),
            on_ended=lambda reason: self._retire(session, reason=reason, depart=False),
            on_terminated=lambda error: self._terminate(session, error),
            on_error=session.report,
        )
        session.attach(source)
        return session

    async def _select_active_meeting(
        self, *, user_open_id: str, token: str, meeting_no: Optional[str]
    ) -> Tuple[str, str, Optional[str]]:
        result = await self._api.call(
            build_user_active_meeting_request(),
            user_access_token=token,
            what="active meeting lookup",
        )
        # "no active meeting" is the most common failure on this path, and this
        # frame is in the traceback when it raises.
        token = None
        if not result.ok:
            raise self._api.error_for(result, what="active meeting lookup")
        meetings = [m for m in (result.data.get("meetings") or []) if isinstance(m, dict)]
        if meeting_no is not None:
            meetings = [m for m in meetings if m.get("meeting_no") == meeting_no]
        if not meetings:
            raise FeishuChannelError(
                FeishuChannelErrorCode.MEETING_NOT_FOUND,
                "no active meeting found for this user",
            )
        chosen = meetings[0]
        if len(meetings) > 1:
            # Meeting titles are written by their creators, so they travel as
            # lazy arguments and get their control characters escaped — a
            # newline here would forge a log line.
            logger.warning(
                "meeting: following the first of %d active meetings (%s); "
                "pass meeting_no to choose. others: %s",
                len(meetings),
                sanitize_for_log(chosen.get("meeting_no")),
                sanitize_for_log(
                    ", ".join(str(m.get("meeting_no")) for m in meetings[1:])
                ),
            )
        meeting_id = meeting_id_str(chosen.get("meeting_id"))
        if not meeting_id:
            raise FeishuChannelError(
                FeishuChannelErrorCode.MEETING_NOT_FOUND,
                "the active meeting carried no meeting id",
            )
        return meeting_id, chosen.get("meeting_no") or "", chosen.get("meeting_title") or chosen.get("topic")

    # -- teardown --------------------------------------------------------
    def _new_session(
        self,
        *,
        meeting_id: str,
        meeting_no: str,
        topic: Optional[str],
        mode: str,
        options: Optional[MeetingOptions],
    ) -> MeetingSession:
        if options is None:
            options = MeetingOptions(
                stabilize_seconds=self._meeting_config.stabilize_seconds
            )
        superseded = self._sessions.get(meeting_id)
        if superseded is not None:
            # A second session for one meeting would leave the first one out of
            # the routing table but still running — for a follow session that
            # means it keeps polling the whole meeting with the user's ticket
            # after the application believes it is gone, and `dispose_all()`
            # can no longer reach it.
            logger.warning(
                "meeting: replacing an existing %s session for meeting %s; "
                "the previous one is being disposed",
                superseded.mode,
                sanitize_for_log(meeting_id),
            )
            superseded.dispose(reason="disposed")
        session = MeetingSession(
            meeting_id=meeting_id,
            meeting_no=meeting_no,
            mode=mode,
            topic=topic,
            options=options,
            api=self._api,
            config=self._meeting_config,
            seen=self._seen,
            bot_open_id_getter=self._bot_open_id_getter,
            on_teardown=self._forget,
            depart=self._depart,
        )
        self._sessions[meeting_id] = session
        return session

    def _forget(self, session: MeetingSession, reason: str) -> None:
        """Called from ``dispose()``: unregister and hand the seat back.

        Identity-checked, not keyed: a superseded session tearing down must not
        evict the one that replaced it.
        """
        if self._sessions.get(session.meeting_id) is session:
            self._sessions.pop(session.meeting_id, None)
        for key, tracked in list(self._follow_by_user.items()):
            if tracked is session:
                self._follow_by_user.pop(key, None)
        self._health_view.retire(session)
        # Draining belongs to whoever initiated the teardown — see
        # `MeetingSession._dispose_on_owner_loop`. Keeping a list of drained
        # sessions here would grow for the life of a process that never
        # disconnects, holding on to every torn-down session's closures,
        # counters and queue.


    async def _probe_said_absent(self, session: MeetingSession) -> None:
        """A probe established the bot is no longer a participant.

        The seat has to be released here. This is the one path with no
        ``meeting_ended_v1`` behind it — a host removing the bot, a meeting
        being transferred — so nothing else will ever produce the evidence, and
        lazy reconciliation only looks at entries whose *departure* came back
        inconclusive. Without this the seat waits out the accounting deadline,
        and repeating the removal exhausts the ceiling for both entry points.

        No departure call: the bot is already out, and this endpoint rejects a
        departure for a meeting it is not in.
        """
        self._membership.release(session.meeting_id, evidence=ABSENCE_EVIDENCE)
        await self._retire(session, reason="no_longer_active", depart=False)

    async def _retire(
        self, session: MeetingSession, *, reason: str, depart: bool
    ) -> None:
        await session.retire(reason=reason, depart=depart)

    async def _terminate(self, session: MeetingSession, error: Any) -> None:
        """Full recovery for a source that cannot continue.

        Stopping the loop is not enough: no idle deadline and no liveness probe
        applies to a follow session, so a half-terminated one would never be
        collected by anything.
        """
        await session.report(error)
        await session.retire(reason="error", depart=False)

    async def _depart(self, meeting_id: str, *, requester: Any = None) -> Any:
        """Leave ``meeting_id``, unless another session has taken it over.

        A stale handle is easy to hold on to: after a session is superseded the
        application may still call ``leave()`` on the old one, and that call
        would eject the bot from a meeting the *replacement* is actively
        serving.

        The condition is deliberately "another session is registered for this
        meeting", not "the caller is absent from the routing table" — the
        documented shutdown order is ``disconnect()`` then ``leave()``, and by
        then no session is registered at all, yet that departure is exactly the
        one that has to go through.
        """
        current = self._sessions.get(meeting_id)
        if current is not None and current is not requester:
            logger.warning(
                "meeting: ignoring a departure from a superseded session for "
                "meeting %s; another session is serving it",
                sanitize_for_log(meeting_id),
            )
            return None
        result = await self._api.call(
            build_bot_leave_request(meeting_id=meeting_id), what="meeting departure"
        )
        self._membership.note_departure_outcome(meeting_id, result)
        return result

    def dispose_all(self) -> List[MeetingSession]:
        """Dispose every live session without departing any meeting.

        A reconnect must not make the bot vanish from the meetings it is in, so
        this is what ``disconnect()`` does. The flip side is that the bot stays
        a participant, which is why the seats stay counted and why the process
        should ``leave()`` before exiting.
        """
        sessions = list(self._sessions.values())
        for session in sessions:
            session.dispose(reason="disposed")
        remaining = self._membership.held()
        if remaining:
            logger.warning(
                "channel: %d meeting(s) still have this bot as a participant "
                "after disconnect; call leave() on those sessions before exit "
                "or the bot stays in them",
                len(remaining),
            )
        return sessions

    async def drain_sessions(self, sessions: List[MeetingSession]) -> None:
        """Wait out each session's bounded drain. Used by channel teardown.

        ``drain()`` is single-flight, so this is safe even though disposal
        already scheduled one.
        """
        if not sessions:
            return
        # Concurrently, so the caller's budget is one session's drain rather
        # than the sum over every session.
        await asyncio.gather(
            *[self._drain_one(session) for session in sessions],
            return_exceptions=True,
        )

    @staticmethod
    async def _drain_one(session: MeetingSession) -> None:
        try:
            await session.drain()
        except Exception:  # pragma: no cover - teardown must not raise
            pass



def _actor_from(raw: Any) -> MeetingActor:
    from .coerce import actor_id

    if not isinstance(raw, dict):
        return MeetingActor()
    return MeetingActor(
        id=actor_id(raw),
        name=raw.get("name") or raw.get("user_name"),
        user_type=raw.get("user_type"),
        user_role=raw.get("user_role"),
    )


__all__ = [
    "ACTIVITY_EVENT",
    "ENDED_EVENT",
    "INTERNAL_EVENT_TYPES",
    "INVITED_EVENT",
    "MeetingChannel",
]
