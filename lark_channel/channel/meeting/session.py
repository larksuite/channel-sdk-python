"""One meeting, as the application sees it.

The same class serves both entry points; what differs is the injected source and
the ``mode`` string. That is the point of the design: moving from following a
meeting to joining one changes the entry-point line and nothing else.

``dispose()`` versus ``leave()`` is the one distinction callers must internalize:

* ``leave()`` departs the meeting, then tears down.
* ``dispose()`` tears down **without** departing — because a reconnect must not
  make the bot vanish from every meeting it is in.

Which means: before the process exits, ``leave()`` every live session, or the
bot sits in those meetings until the server ends them. ``leave()`` therefore
keeps working after ``dispose()``, so that instruction is usable rather than a
trap.
"""

import asyncio
import inspect
import json
import uuid as _uuid
from typing import Any, Callable, Dict, List, Optional

from lark_channel.api.vc.bot import build_bot_message_request
from lark_channel.core.log import logger

from ..errors import FeishuChannelError, FeishuChannelErrorCode
from .dedup import activity_key
from .errors import log_error_fields, sanitize_for_log
from .loop_affinity import await_on, run_on
from .normalize import activity_type_of, items_of, unpack
from .rate_limit import SendRateLimiter
from .serial_queue import SerialDelivery
from .stabilizer import TranscriptStabilizer
from .types import (
    ActivityTypeStats,
    MEETING_EVENT_NAMES,
    MeetingEndEvent,
    MeetingOptions,
)

Unsubscribe = Callable[[], None]


class MeetingSession:
    """A live meeting: an event stream, a way to speak, and a way to leave."""

    def __init__(
        self,
        *,
        meeting_id: str,
        meeting_no: str,
        mode: str,
        topic: Optional[str] = None,
        options: MeetingOptions,
        api: Any,
        config: Any,
        seen: Any,
        bot_open_id_getter: Callable[[], Optional[str]],
        on_teardown: Callable[["MeetingSession", str], Any],
        depart: Callable[..., Any],
    ) -> None:
        self.meeting_id = meeting_id
        self.meeting_no = meeting_no
        self.mode = mode
        #: untrusted — whatever the meeting's creator typed.
        self.topic = topic
        self._options = options
        self._api = api
        self._config = config
        self._seen = seen
        self._bot_open_id_getter = bot_open_id_getter
        self._on_teardown = on_teardown
        self._depart_call = depart

        self._handlers: Dict[str, List[Callable]] = {}
        self._stats: Dict[str, ActivityTypeStats] = {}
        self._source: Any = None
        self._loop: Optional[Any] = None
        self._closed = False
        self._ended = False
        self._left = False
        self._drain_task: Optional[Any] = None
        self._limiter = SendRateLimiter(config.send_rate_limit_per_minute)
        self._delivery = SerialDelivery(
            on_handler_error=self._report_from_worker
        )
        window = options.stabilize_seconds
        self._stabilizer = TranscriptStabilizer(
            window_seconds=window, emit=self._enqueue_transcript
        )

    def __repr__(self) -> str:
        # Field-by-field would print whatever else ends up on the instance; a
        # meeting session is one hop away from a user ticket.
        return "MeetingSession(meeting_id=%r, meeting_no=%r, mode=%r)" % (
            self.meeting_id,
            self.meeting_no,
            self.mode,
        )

    # -- wiring ----------------------------------------------------------
    def attach(self, source: Any) -> None:
        """Bind the source and remember the loop that now owns this session.

        Everything the session owns — the delivery queue, the source's tasks,
        the debounce timers — belongs to this loop. ``dispose()`` and
        ``leave()`` are public and can be called from anywhere, so they route
        the loop-bound parts back here: cancelling a task or waiting on a queue
        from a different loop either raises or silently never completes.
        """
        self._source = source
        self._loop = asyncio.get_event_loop()
        self._delivery.start()
        source.start()


    # -- subscriptions ---------------------------------------------------
    def on(self, name: str, handler: Callable) -> Unsubscribe:
        """Subscribe to a session event. Multicast; returns an unsubscribe.

        An unknown name is a warning rather than an error, matching
        ``FeishuChannel.on`` — but it is worth surfacing, because the failure it
        produces otherwise is a handler that is simply never called.
        """
        if name not in MEETING_EVENT_NAMES:
            logger.warning(
                "meeting: unknown session event %r; known events are %s",
                name,
                ", ".join(MEETING_EVENT_NAMES),
            )
        self._handlers.setdefault(name, []).append(handler)

        def unsubscribe() -> None:
            handlers = self._handlers.get(name)
            if not handlers:
                return
            try:
                handlers.remove(handler)
            except ValueError:
                return
            if not handlers:
                self._handlers.pop(name, None)

        return unsubscribe

    @property
    def dropped_deliveries(self) -> int:
        """Events refused by this session's delivery queue."""
        return self._delivery.dropped

    def get_stats(self) -> Dict[str, ActivityTypeStats]:
        """Per activity type parse accounting for this session."""
        return dict(self._stats)

    # -- ingestion -------------------------------------------------------
    async def ingest(self, activities: List[Dict[str, Any]]) -> None:
        """Unpack and deliver activity objects, in the order given.

        Order is the meaning of some of these: a document swap arrives as
        ended-then-started. Both nesting levels are walked in array order and
        delivered serially.
        """
        if self._closed:
            return
        if self._source is not None:
            self._source.touch()
        for activity in activities:
            await self._ingest_one(activity)

    async def _ingest_one(self, activity: Dict[str, Any]) -> None:
        activity_event_type = activity_type_of(activity)
        if activity_event_type is None:
            unpack(
                activity,
                meeting_id=self.meeting_id,
                include_raw=self._options.include_raw,
                stats=self._stats,
            )
            return
        items = items_of(activity, activity_event_type)
        key = activity_key(
            activity,
            items,
            meeting_id=self.meeting_id,
            activity_event_type=activity_event_type,
            event_id=activity.get("event_id"),
        )
        if self._seen.has_sync(key):
            return
        self._seen.add_sync(key)
        events = unpack(
            activity,
            meeting_id=self.meeting_id,
            include_raw=self._options.include_raw,
            stats=self._stats,
        )
        for name, event in events:
            event.self_echo = self._is_own(event)
            if name == "transcript":
                self._stabilizer.offer(event)
            else:
                self._emit(name, event)

    def _is_own(self, event: Any) -> bool:
        """Whether this item came from our own bot.

        ``uat`` mode cannot produce an echo: the bot is not in the meeting.

        In ``tat`` mode, before the bot's own id is resolved the honest answer
        is "maybe", and "maybe" has to read as ``True`` — ``False`` means
        "definitely not me" and lets a reply loop close.
        """
        if self.mode != "tat":
            return False
        own = self._bot_open_id_getter()
        if not own:
            return True
        actor = getattr(event, "actor", None)
        return bool(actor is not None and actor.id and actor.id == own)

    def _enqueue_transcript(self, event: Any) -> None:
        self._emit("transcript", event)

    def _emit(self, name: str, payload: Any) -> None:
        handlers = self._handlers.get(name)
        if handlers:
            self._delivery.submit(handlers, payload)

    # -- outbound --------------------------------------------------------
    async def send_message(self, text: str) -> None:
        """Send a message into the meeting. ``tat`` only."""
        if self.mode != "tat":
            raise FeishuChannelError(
                FeishuChannelErrorCode.NOT_SUPPORTED,
                "send_message needs the bot to be a participant; this session "
                "follows a meeting without joining it",
            )
        if not self._limiter.try_acquire():
            raise FeishuChannelError(
                FeishuChannelErrorCode.RATE_LIMITED,
                "in-meeting send budget exhausted for this session",
            )
        request = build_bot_message_request(
            meeting_id=self.meeting_id,
            msg_type="text",
            content=json.dumps({"text": text}, ensure_ascii=False),
            uuid=str(_uuid.uuid4()),
        )
        result = await self._api.call(request, what="in-meeting message")
        if not result.ok:
            raise self._api.error_for(result, what="in-meeting message")

    # -- teardown --------------------------------------------------------
    def dispose(self, *, reason: str = "disposed") -> None:
        """Stop local work. Does **not** depart the meeting. Idempotent.

        The delivery queue is drained afterwards, on the owning loop, because
        there is no caller here to await it.

        Synchronous and unconditional: cancelling timers, flushing settled
        transcripts, unregistering, and handing the seat back must all happen
        whether or not a handler is currently parked. Waiting on the delivery
        queue here would let one wedged handler hold a seat forever — and would
        hang the timeout meant to rescue that very situation.
        """
        if self._closed:
            return
        # Set here, synchronously, so this is idempotent and stops accepting
        # new activity the moment the caller asks — even if the rest has to
        # hop to another loop.
        self._closed = True
        run_on(self._loop, lambda: self._dispose_on_owner_loop(reason, drain=True))

    def _dispose_on_owner_loop(self, reason: str, *, drain: bool) -> None:
        if self._source is not None:
            self._source.stop()
        # Flushed before the queue stops accepting work, or the last thing
        # anybody said in the meeting dies with the debounce timer.
        self._stabilizer.flush_all()
        self._end(reason)
        self._on_teardown(self, reason)
        if drain:
            # Nobody is awaiting this teardown, so the drain is scheduled.
            # A teardown that *is* awaited drains inline instead — draining
            # from both places races the departure call, and the loser is the
            # error report explaining why the session went away.
            asyncio.ensure_future(self.drain())

    async def leave(self) -> None:
        """Depart the meeting, then tear down. Idempotent, and still valid
        after ``dispose()``.

        A failed departure does not abort teardown: the moment a meeting ends
        is exactly when this call is most likely to 404, and aborting there
        would leak a session for every normally-ended meeting. The failure goes
        to the ``error`` event instead, and accounting decides separately
        whether the seat can be released.
        """
        return await await_on(self._loop, self._leave_on_owner_loop)

    async def retire(self, *, reason: str, depart: bool) -> None:
        """Internal teardown with an explicit reason. Awaited by its caller.

        The one path that both names a reason other than ``left`` and needs the
        meeting departed — the meeting ending, an idle deadline expiring.
        """
        return await await_on(
            self._loop, lambda: self._retire_on_owner_loop(reason, depart)
        )

    async def _retire_on_owner_loop(self, reason: str, depart: bool) -> None:
        if not self._closed:
            self._closed = True
            self._dispose_on_owner_loop(reason, drain=False)
        if depart and not self._left and self.mode == "tat":
            self._left = True
            await self._depart()
        await self.drain()

    async def _leave_on_owner_loop(self) -> None:
        if not self._closed:
            self._closed = True
            self._dispose_on_owner_loop("left", drain=False)
        if not self._left and self.mode == "tat":
            self._left = True
            await self._depart()
        # Explicit teardown waits out the bounded drain, so a caller that
        # awaited leave() knows the handlers are finished or have been given up
        # on. dispose() cannot: it is synchronous by design.
        await self.drain()

    async def _depart(self) -> Any:
        # Identifies the caller so a superseded handle cannot eject the bot
        # from a meeting another session is now serving.
        result = await self._depart_call(self.meeting_id, requester=self)
        if result is not None and not getattr(result, "ok", False):
            await self.report(self._api.error_for(result, what="meeting departure"))
        return result

    async def drain(self) -> None:
        """Give queued handlers a bounded chance to finish, then stop them.

        Single-flight rather than merely idempotent. Teardown is reachable from
        a ``leave()`` the caller awaits *and* from the disposal path at the same
        time; a plain "already done" guard would let ``leave()`` return before
        the drain it is supposed to have waited for. Both callers await the same
        operation, and the timeout is reported once.
        """
        if self._drain_task is None:
            self._drain_task = asyncio.ensure_future(self._drain_once())
        try:
            await asyncio.shield(self._drain_task)
        except RuntimeError:
            # The drain was started on a loop that has since stopped, so
            # awaiting its future from here is not possible. Teardown has
            # already done everything that matters — the timers are cancelled,
            # the seat is back, the session is unregistered — and this method
            # promises not to raise, so the unfinished wait is dropped.
            logger.debug(
                "meeting: could not await the drain for meeting %s across loops",
                sanitize_for_log(self.meeting_id),
            )
        except asyncio.CancelledError:
            raise

    async def _drain_once(self) -> None:
        timeout = self._config.dispose_drain_timeout_seconds
        if self._source is not None:
            # Bounded: one of these loops may be parked in a call that cannot be
            # cancelled promptly, and teardown must not wait on it indefinitely.
            try:
                await asyncio.wait_for(
                    self._source.wait_closed(), timeout=max(0.05, timeout)
                )
            except (asyncio.TimeoutError, asyncio.CancelledError):
                pass
        drained = await self._delivery.drain(timeout)
        if not drained:
            logger.warning(
                "meeting: could not drain the delivery queue for meeting %s "
                "within %ss; a handler is still running and is being cancelled",
                sanitize_for_log(self.meeting_id),
                timeout,
            )
        self._delivery.cancel()

    def _end(self, reason: str) -> None:
        if self._ended:
            return
        self._ended = True
        self._emit("end", MeetingEndEvent(meeting_id=self.meeting_id, reason=reason))

    async def _report_from_worker(self, error: BaseException) -> None:
        """Report a failure raised *inside* the delivery worker.

        Deliberately does not go through the queue. ``_report`` submits to the
        error handlers, and this is called when a handler raised — so
        submitting would hand the same handlers the same kind of work, they
        would raise again, and the queue would feed itself forever while every
        real event queued behind it.
        """
        resolved = self._as_channel_error(error)
        handlers = list(self._handlers.get("error") or ())
        if not handlers:
            self._log_error(resolved)
            return
        await self._invoke_error_handlers(handlers, resolved)

    async def _invoke_error_handlers(
        self, handlers: List[Callable], error: FeishuChannelError
    ) -> None:
        for handler in handlers:
            try:
                result = handler(error)
                if inspect.isawaitable(result):
                    await result
            except Exception:
                # An error handler that itself fails is logged, never
                # re-dispatched: that is the loop this method exists to avoid.
                logger.exception("meeting: an error handler raised")

    async def report(self, error: BaseException) -> None:
        """Route a failure to this session's ``error`` subscribers.

        Awaitable on purpose. The fallback path below has to call handlers
        directly, and a version of this that *returned* that work left every
        caller silently dropping a coroutine — which is how "a failed departure
        after a dispose is reported" turned back into "it is swallowed", with
        only a never-awaited warning to show for it.
        """
        resolved = self._as_channel_error(error)
        handlers = list(self._handlers.get("error") or ())
        if not handlers:
            self._log_error(resolved)
            return
        if self._delivery.submit(handlers, resolved, reserved=True):
            return
        # The queue is already shut down. That is the ordinary case for a
        # `leave()` after a `dispose()`: disposal drained and cancelled the
        # queue, and the departure call then failed. Dropping the report here
        # would silence exactly the failure that explains the state.
        await self._invoke_error_handlers(handlers, resolved)


    @staticmethod
    def _as_channel_error(error: BaseException) -> FeishuChannelError:
        if isinstance(error, FeishuChannelError):
            return error
        return FeishuChannelError(
            FeishuChannelErrorCode.UNKNOWN,
            "%s in a meeting handler" % type(error).__name__,
        )

    def _log_error(self, error: FeishuChannelError) -> None:
        """The fallback when nobody is subscribed.

        ``context`` is left out on purpose: it can hold a console link, which is
        a signed one-click grant.
        """
        fields = log_error_fields(self.meeting_id, error)
        logger.error(
            "meeting: %s (meeting_id=%s code=%s feishu_code=%s)",
            fields["message"],
            fields["meeting_id"],
            fields["code"],
            fields["feishu_code"],
        )


__all__ = ["MeetingSession"]
