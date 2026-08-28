"""Per-session serial delivery with a bounded wait on teardown.

Delivery is serial and awaited because order is meaning: a document swap
arrives as ``magic_share_ended`` followed by ``magic_share_started``, and
reordering them makes the application reconstruct the wrong shared document.
Awaiting each handler before the next event is what makes that guarantee hold
end to end rather than only up to the queue.

The cost, which the README states: a handler that yields and takes a long time
holds up **this meeting's** stream. A handler that blocks *without* yielding
holds up the whole event loop — every meeting, the message path, the socket's
heartbeat — because there is one thread. That is not defensible from here;
blocking work belongs in an executor.
"""

import asyncio
import inspect
from typing import Any, Awaitable, Callable, List, Optional, Tuple

#: Ceiling on queued deliveries per session. A handler that yields but takes a
#: long time makes this queue grow at whatever rate the meeting produces
#: activity, so without a ceiling one parked handler costs unbounded memory for
#: the life of the meeting.
#:
#: Overflow rejects the *newest* delivery rather than evicting the oldest or
#: blocking the producer, and neither alternative is available here:
#:
#: * Evicting the oldest would break the guarantee this class exists for. Order
#:   is meaning — a document swap arrives as ``magic_share_ended`` then
#:   ``magic_share_started`` — and dropping from the front splits such pairs, so
#:   the application would rebuild the wrong state from a queue that still looks
#:   complete. Rejecting at the tail leaves what is queued contiguous.
#: * Blocking the producer would park the socket's message handler or the poll
#:   loop, and there is one thread: that stalls every meeting, the message path
#:   and the heartbeat with it.
MAX_QUEUED_DELIVERIES = 1000

#: Headroom above the ceiling, reserved for error reports. Teardown submits
#: here — the ``end`` event and any error raised by the departure call — and
#: those are the reports that explain why a session went away. Subjecting them
#: to a ceiling filled by transcripts would drop the explanation and keep the
#: noise. The reserve is small because these submissions are bounded per
#: session, unlike activity.
REPORT_RESERVE = 32


class SerialDelivery:
    """A single-consumer queue that awaits each handler in turn."""

    def __init__(
        self,
        *,
        on_handler_error: Callable[[BaseException], Any],
        max_queued: int = MAX_QUEUED_DELIVERIES,
    ) -> None:
        self._queue: "asyncio.Queue[Optional[Tuple[List[Callable], Any]]]" = (
            asyncio.Queue()
        )
        self._on_handler_error = on_handler_error
        self._max_queued = max_queued
        self._worker: Optional[asyncio.Task] = None
        self._closed = False
        self._dropped = 0

    def start(self) -> None:
        if self._worker is None:
            self._worker = asyncio.ensure_future(self._run())

    def submit(
        self, handlers: List[Callable], payload: Any, *, reserved: bool = False
    ) -> bool:
        """Queue one delivery. ``False`` if it was refused.

        Refused means the worker has been cancelled, or the queue is at its
        ceiling. Either way the return value matters: a caller reporting a
        failure needs to know the report was accepted, or it has to fall back
        to something else rather than let the failure disappear.

        ``reserved`` marks a delivery that may use the report headroom above
        the ceiling. Pass it for diagnostics, never for activity.
        """
        if self._closed:
            return False
        ceiling = self._max_queued + (REPORT_RESERVE if reserved else 0)
        if self._queue.qsize() >= ceiling:
            self._dropped += 1
            return False
        self._queue.put_nowait((list(handlers), payload))
        return True

    @property
    def idle(self) -> bool:
        return self._queue.empty()

    @property
    def dropped(self) -> int:
        """Deliveries refused because the queue was full.

        Surfaced through health rather than kept here: a drop that only this
        object knows about is the silent data loss the whole readout exists to
        make diagnosable.
        """
        return self._dropped

    async def _run(self) -> None:
        while True:
            entry = await self._queue.get()
            if entry is None:
                self._queue.task_done()
                return
            handlers, payload = entry
            for handler in handlers:
                try:
                    result = handler(payload)
                    if inspect.isawaitable(result):
                        await result
                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    try:
                        outcome = self._on_handler_error(exc)
                        if inspect.isawaitable(outcome):
                            await outcome
                    except Exception:  # pragma: no cover - reporting must not throw
                        pass
            self._queue.task_done()

    async def drain(self, timeout: float) -> bool:
        """Wait up to ``timeout`` for the queue to empty. ``True`` if it did.

        Bounded on purpose. The caller's own teardown runs whether or not this
        succeeds — otherwise one parked handler would hold a seat for the life
        of the process, and would also hold up the very timeout meant to
        rescue a stalled session.

        Draining does **not** stop accepting work; only :meth:`cancel` does.
        Teardown itself submits here — the ``end`` event, and any error raised
        by the departure call — and those arrive while the drain is in flight.
        Refusing them here would silently drop exactly the reports that explain
        why the session went away. The submissions during teardown are bounded,
        and ingestion has already stopped at the session level, so the queue
        still empties.
        """
        if timeout <= 0:
            return self._queue.empty()
        try:
            await asyncio.wait_for(self._queue.join(), timeout=timeout)
            return True
        except (asyncio.TimeoutError, asyncio.CancelledError):
            return False
        except Exception:  # pragma: no cover
            return False

    def cancel(self) -> None:
        self._closed = True
        worker = self._worker
        self._worker = None
        if worker is not None and not worker.done():
            worker.cancel()


__all__ = ["MAX_QUEUED_DELIVERIES", "REPORT_RESERVE", "SerialDelivery"]
