"""Heuristic guard against two bots ``@``-ing each other forever.

Opt-in, default off. Only "another bot @'d me" messages count
(``sender.is_bot and mentioned_bot``); a human message (``sender_type == 'user'``)
resets the count; when the count reaches the threshold inside a sliding window
the key is tripped. ``msg.create_time`` is the clock, so counting is
deterministic (and, being event-supplied, only best-effort — a backstop, not a
protocol-level defense).
"""

from collections import OrderedDict
from typing import Any, Dict, List, Optional, Tuple

from ..config import BotLoopGuardConfig
from ..types import InboundMessage

# Hard cap on tracked keys so the state map can't grow unbounded.
_MAX_KEYS = 5000
# Hard cap on the per-key window so a malicious bot pinning ``create_time`` to a
# fixed far-future value (which keeps the sliding-window cutoff from advancing)
# can't accumulate distinct message_ids without bound. Only the count relative
# to the threshold matters, so trimming the oldest entries past this cap is safe
# and keeps counting monotonic toward "tripped".
_MAX_ENTRIES_PER_KEY = 1024


class LoopGuard:
    def __init__(self, cfg: Optional[BotLoopGuardConfig], logger: Any) -> None:
        cfg = cfg or BotLoopGuardConfig()
        self.enabled: bool = cfg.enabled
        self.on_trip: str = cfg.on_trip
        self._window_ms: int = cfg.window_ms
        self._threshold: int = cfg.max_bot_mentions
        self._scope: str = cfg.scope
        self._logger = logger
        # key -> {"entries": List[(message_id, create_time)], "warned": bool}
        self._states: "OrderedDict[str, Dict[str, Any]]" = OrderedDict()

    def record(self, msg: InboundMessage) -> bool:
        """Record a message; return whether its key is now tripped.

        A human message resets the key and never trips; messages that aren't
        "another bot @'d me" don't count. A re-delivered ``message_id`` already
        inside the window is counted once. The first trip of a key emits exactly
        one warning.
        """
        if not self.enabled:
            return False
        key = self._key_for(msg)

        if msg.sender.sender_type == "user":
            self._states.pop(key, None)
            return False
        if not (msg.sender.is_bot and msg.mentioned_bot):
            return False

        state = self._states.get(key) or {"entries": [], "warned": False}
        entries: List[Tuple[str, int]] = state["entries"]
        cutoff = msg.create_time - self._window_ms
        entries = [(mid, t) for (mid, t) in entries if t >= cutoff]
        if not any(mid == msg.id for (mid, _t) in entries):
            entries.append((msg.id, msg.create_time))
        # Backstop against an adversarial constant/far-future create_time that
        # freezes the cutoff so entries never slide out: bound the list length.
        if len(entries) > _MAX_ENTRIES_PER_KEY:
            entries = entries[-_MAX_ENTRIES_PER_KEY:]
        state["entries"] = entries

        tripped = len(entries) >= self._threshold
        if tripped and not state["warned"]:
            self._logger.warning(
                "channel: botLoopGuard tripped for %s — >=%d bot @-mentions "
                "within %dms (on_trip=%s)",
                key,
                self._threshold,
                self._window_ms,
                self.on_trip,
            )
            state["warned"] = True
        elif not tripped:
            # Re-arm the one-time warn once the window drains below threshold.
            state["warned"] = False

        self._remember(key, state)
        return tripped

    def _remember(self, key: str, state: Dict[str, Any]) -> None:
        self._states.pop(key, None)
        self._states[key] = state
        while len(self._states) > _MAX_KEYS:
            self._states.popitem(last=False)

    def _key_for(self, msg: InboundMessage) -> str:
        chat_id = msg.conversation.chat_id
        if self._scope == "chat+sender":
            return f"{chat_id}::{msg.sender.open_id}"
        return chat_id
