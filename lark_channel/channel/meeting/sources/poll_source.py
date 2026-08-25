"""Following as the user: two loops over REST, under the user's own token.

The activity loop reads ``bots/events``; the end-detection loop re-checks
``user_active_meeting``, because this path gets no meeting-ended event.

Three things here are load-bearing:

**Ticket lookup is non-interactive.** The loop runs every few seconds for the
whole meeting, so anything it does per round it does hundreds of times. The
interactive resolver starts a device flow whenever the stored scopes do not
contain the requested one verbatim — an ordinary state, since ticket scopes are
whatever the platform granted the app — which in a loop means hundreds of
authorization cards or a silent stall inside ``poll``.

**Failure backoff is counted separately from empty-poll backoff.** A failing
credential must not keep retrying on the three-second cadence: that turns one
leak risk into thousands, and puts sustained invalid-auth traffic in front of
the platform's risk controls, where it can take the application's message path
down with it.

**The two loops fail asymmetrically, on purpose.** A credential failure stops
both — the ticket is shared, so neither can work. A retryable failure in
end-detection does *not* end the session: those failures are correlated across
every follow session (same endpoint, same cadence, often the same user), so
ending on them would end all of them at once while their transcripts were
flowing fine. The cost is losing meeting-end detection in that window, which is
lighter than killing healthy sessions.
"""

import asyncio
from typing import Any, Awaitable, Callable, List, Optional

from lark_channel.api.vc.bot import (
    build_bot_events_request_as_user,
    build_user_active_meeting_request,
)
from lark_channel.core.log import logger

from ..errors import sanitize_for_log

#: Feishu codes that mean the ticket will not start working again by itself.
CREDENTIAL_FEISHU_CODES = frozenset(
    {99991400, 99991401, 99991663, 99991664, 99991665, 99991666, 99991668, 99991672}
)
CREDENTIAL_HTTP_STATUSES = frozenset({401, 403})


def _is_credential_failure(result: Any) -> bool:
    if getattr(result, "feishu_code", None) in CREDENTIAL_FEISHU_CODES:
        return True
    return getattr(result, "status", None) in CREDENTIAL_HTTP_STATUSES


class PollSource:
    """Polls in-meeting events as the user, and watches for the meeting ending."""

    mode = "uat"

    def __init__(
        self,
        *,
        meeting_id: str,
        api: Any,
        config: Any,
        resolve_ticket: Callable[[], Awaitable[str]],
        deliver: Callable[[List[dict]], Any],
        on_ended: Callable[[str], Awaitable[Any]],
        on_terminated: Callable[[Any], Awaitable[Any]],
        on_error: Callable[[Any], Awaitable[Any]],
    ) -> None:
        self._meeting_id = meeting_id
        self._api = api
        self._config = config
        self._resolve_ticket = resolve_ticket
        self._deliver = deliver
        self._on_ended = on_ended
        self._on_terminated = on_terminated
        self._on_error = on_error
        self._tasks: List[asyncio.Task] = []
        self._closing: List[asyncio.Task] = []
        self._page_token: Optional[str] = None
        self._running = False
        self._terminating = False
        self._last_error: Any = None

    def start(self) -> None:
        self._running = True
        self._tasks.append(asyncio.ensure_future(self._activity_loop()))
        if self._config.active_meeting_check_interval_seconds > 0:
            self._tasks.append(asyncio.ensure_future(self._end_detection_loop()))

    def stop(self) -> None:
        """Stop the loops. Never cancels the caller's own task.

        Same reason as :meth:`PushSource.stop`: teardown is reached *from* one
        of these loops, and cancelling the running task would raise inside the
        teardown it triggered.
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

    def touch(self) -> None:
        """No idle deadline applies to this mode; kept for source parity."""

    # -- loops -----------------------------------------------------------
    async def _activity_loop(self) -> None:
        idle_rounds = 0
        failures = 0
        try:
            while self._running:
                outcome = await self._poll_once()
                if outcome is None:
                    return
                got_events, failed = outcome
                if failed:
                    failures += 1
                    if failures >= self._config.poll_max_consecutive_failures:
                        await self._terminate(self._last_error)
                        return
                    await asyncio.sleep(self._failure_delay(failures))
                    continue
                failures = 0
                if got_events:
                    idle_rounds = 0
                    delay = self._empty_delay(0)
                else:
                    # The ladder is read before the counter moves, so the first
                    # quiet round waits the floor rather than double it.
                    delay = self._empty_delay(idle_rounds)
                    idle_rounds += 1
                await asyncio.sleep(delay)
        except asyncio.CancelledError:
            raise

    async def _poll_once(self):
        """``(got_events, failed)``, or ``None`` when the source terminated."""
        try:
            token = await self._resolve_ticket()
        except Exception as exc:
            await self._terminate(exc)
            return None
        request = build_bot_events_request_as_user(
            meeting_id=self._meeting_id, page_token=self._page_token
        )
        result = await self._api.call(
            request, user_access_token=token, what="meeting event poll"
        )
        if result.ok:
            self._page_token = result.data.get("page_token") or self._page_token
            events = result.data.get("events")
            events = [e for e in events if isinstance(e, dict)] if isinstance(events, list) else []
            if events:
                await self._deliver(events)
            return bool(events), False
        error = self._api.error_for(result, what="meeting event poll")
        if _is_credential_failure(result):
            await self._terminate(error)
            return None
        # Reported even though it will be retried: a caller watching `error`
        # is watching for exactly this, and staying quiet until the source
        # gives up entirely hides a meeting whose transcript has stalled.
        self._last_error = error
        await self._on_error(error)
        return False, True

    async def _end_detection_loop(self) -> None:
        interval = self._config.active_meeting_check_interval_seconds
        failures = 0
        try:
            while self._running:
                await asyncio.sleep(interval)
                if not self._running:
                    return
                try:
                    token = await self._resolve_ticket()
                except Exception as exc:
                    await self._terminate(exc)
                    return
                result = await self._api.call(
                    build_user_active_meeting_request(),
                    user_access_token=token,
                    what="active meeting lookup",
                )
                if result.ok:
                    failures = 0
                    if not self._still_listed(result):
                        await self._on_ended("no_longer_active")
                        return
                    continue
                if _is_credential_failure(result):
                    # Shared ticket: neither loop can work, so both stop.
                    await self._terminate(
                        self._api.error_for(result, what="active meeting lookup")
                    )
                    return
                # Retryable: keep checking, never end the session on it.
                failures += 1
                await asyncio.sleep(self._failure_delay(failures))
        except asyncio.CancelledError:
            raise

    def _still_listed(self, result: Any) -> bool:
        from ..coerce import meeting_id_str

        meetings = result.data.get("meetings")
        if not isinstance(meetings, list):
            return False
        for meeting in meetings:
            if not isinstance(meeting, dict):
                continue
            if meeting_id_str(meeting.get("meeting_id")) == self._meeting_id:
                return True
        return False

    # -- helpers ---------------------------------------------------------
    def _empty_delay(self, idle_rounds: int) -> float:
        floor = self._config.poll_min_interval_seconds
        ceiling = self._config.poll_max_interval_seconds
        return min(floor * (2 ** idle_rounds), ceiling)

    def _failure_delay(self, failures: int) -> float:
        floor = self._config.poll_min_interval_seconds
        ceiling = self._config.poll_failure_max_interval_seconds
        return min(floor * (2 ** failures), ceiling)

    async def _terminate(self, error: Any) -> None:
        if self._terminating:
            return
        self._terminating = True
        self._running = False
        logger.debug(
            "meeting: follow source for %s terminating",
            sanitize_for_log(self._meeting_id),
        )
        await self._on_terminated(error)


__all__ = ["CREDENTIAL_FEISHU_CODES", "CREDENTIAL_HTTP_STATUSES", "PollSource"]
