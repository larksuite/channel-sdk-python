"""Transcript settling.

The protocol has no "this is the final text" marker, so a later item with the
same ``sentence_id`` supersedes the earlier one. With a zero window every
revision is delivered and consumers watch a sentence grow; with a positive
window a sentence is delivered once, after it stops changing for that long.

Whatever is still buffered has to be flushed when the session goes away. The
debounce timer dies with the session, and if the sentence dies with it the last
thing anybody said in the meeting is silently lost.
"""

import asyncio
from typing import Any, Callable, Dict, Optional, Tuple

#: Buffered sentences allowed per session. A meeting with many speakers can
#: hold a lot of sentences open at once; past this the oldest is delivered
#: early rather than dropped, because dropping loses content while delivering
#: early only loses the settling guarantee for one sentence.
MAX_PENDING_TRANSCRIPTS = 256


class TranscriptStabilizer:
    """Debounces transcripts by ``sentence_id``."""

    def __init__(
        self,
        *,
        window_seconds: float,
        emit: Callable[[Any], None],
    ) -> None:
        self._window = window_seconds
        self._emit = emit
        self._pending: "Dict[str, Tuple[Any, Optional[asyncio.TimerHandle]]]" = {}

    @property
    def enabled(self) -> bool:
        return self._window > 0

    def offer(self, event: Any) -> None:
        """Deliver ``event`` now, or hold it until its sentence settles."""
        sentence_id = getattr(event, "sentence_id", None)
        if not self.enabled or not sentence_id:
            self._emit(event)
            return
        self._cancel(sentence_id)
        if len(self._pending) >= MAX_PENDING_TRANSCRIPTS:
            self._flush_oldest()
        loop = asyncio.get_event_loop()
        handle = loop.call_later(self._window, self._settle, sentence_id)
        self._pending[sentence_id] = (event, handle)

    def _settle(self, sentence_id: str) -> None:
        entry = self._pending.pop(sentence_id, None)
        if entry is not None:
            self._emit(entry[0])

    def _cancel(self, sentence_id: str) -> None:
        entry = self._pending.pop(sentence_id, None)
        if entry is not None and entry[1] is not None:
            entry[1].cancel()

    def _flush_oldest(self) -> None:
        for sentence_id in list(self._pending):
            entry = self._pending.pop(sentence_id)
            if entry[1] is not None:
                entry[1].cancel()
            self._emit(entry[0])
            return

    def flush_all(self) -> None:
        """Deliver everything still buffered. Safe to call more than once."""
        pending = list(self._pending.items())
        self._pending.clear()
        for _sentence_id, (event, handle) in pending:
            if handle is not None:
                handle.cancel()
            self._emit(event)


__all__ = ["MAX_PENDING_TRANSCRIPTS", "TranscriptStabilizer"]
