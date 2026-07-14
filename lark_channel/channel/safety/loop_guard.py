"""Heuristic guard against two bots ``@``-ing each other forever.

Opt-in, default off. Only "another bot @'d me" messages count
(``sender.is_bot and mentioned_bot``); a human message (``sender_type == 'user'``)
resets the count; when the count reaches the threshold inside a sliding window
the key is tripped. ``msg.create_time`` is the clock, so counting is
deterministic — but it is event-supplied, hence only a best-effort backstop
(a bot supplying adversarial timestamps can degrade the heuristic). Out-of-order
events use a per-key monotonic clock so a stale event can't reopen a passed
window, and config values are validated so a misconfiguration can't silently
disable the guard (e.g. a threshold above the per-key cap that could never trip).
"""

import threading
from collections import OrderedDict
from typing import Any, Dict, List, Optional, Tuple

from ..config import BotLoopGuardConfig

# Hard cap on tracked keys so the state map can't grow unbounded.
_MAX_KEYS = 5000
# Hard cap on the per-key window so a malicious bot pinning ``create_time`` to a
# fixed far-future value can't accumulate distinct message_ids without bound.
_MAX_ENTRIES_PER_KEY = 1024
_DEFAULT_WINDOW_MS = 60000
_SCOPES = ("chat", "chat+sender")
_ON_TRIP = ("drop", "reject")


class LoopGuard:
    def __init__(self, cfg: Optional[BotLoopGuardConfig], logger: Any) -> None:
        self._logger = logger
        # key -> {"entries": List[(message_id, time)], "warned": bool, "clock": int}
        self._states: "OrderedDict[str, Dict[str, Any]]" = OrderedDict()
        # Guards record / reset / reconfigure so a runtime update_policy can't
        # race the hot record() path — reconfigure atomically swaps config + state.
        self._lock = threading.Lock()
        self._apply(cfg)

    # ---- configuration -------------------------------------------------------

    def _apply(self, cfg: Optional[BotLoopGuardConfig]) -> None:
        cfg = cfg or BotLoopGuardConfig()
        self.enabled = bool(cfg.enabled)
        self._window_ms = cfg.window_ms if cfg.window_ms and cfg.window_ms > 0 else _DEFAULT_WINDOW_MS
        if cfg.window_ms is not None and cfg.window_ms <= 0:
            self._warn("botLoopGuard.window_ms must be > 0; using %d", _DEFAULT_WINDOW_MS)
        threshold = cfg.max_bot_mentions
        if threshold < 1:
            self._warn("botLoopGuard.max_bot_mentions must be >= 1; clamping to 1")
            threshold = 1
        elif threshold > _MAX_ENTRIES_PER_KEY:
            # A threshold above the per-key entry cap could never be reached.
            self._warn(
                "botLoopGuard.max_bot_mentions %d exceeds the per-key cap %d and "
                "could never trip; clamping",
                threshold,
                _MAX_ENTRIES_PER_KEY,
            )
            threshold = _MAX_ENTRIES_PER_KEY
        self._threshold = threshold
        self._scope = cfg.scope if cfg.scope in _SCOPES else "chat"
        self.on_trip = cfg.on_trip if cfg.on_trip in _ON_TRIP else "drop"

    def reconfigure(self, cfg: Optional[BotLoopGuardConfig]) -> None:
        """Atomically apply a new config (e.g. from ``update_policy``). Counting
        state is preserved when only ``on_trip`` changed, and cleared otherwise
        (a new window / threshold / scope, or enable/disable, starts clean)."""
        with self._lock:
            prev = (self.enabled, self._window_ms, self._threshold, self._scope)
            self._apply(cfg)
            if prev != (self.enabled, self._window_ms, self._threshold, self._scope):
                self._states.clear()

    def reset_on_human(self, msg: Any) -> None:
        """Reset the loop counters for a chat when a human speaks — called
        BEFORE the policy gate so a plain (possibly no-mention, hence
        policy-rejected) human message still breaks a bot ping-pong. Clears the
        chat's state in ``chat`` scope, and every ``chat::bot`` key in
        ``chat+sender`` scope."""
        if not self.enabled or getattr(msg.sender, "sender_type", None) != "user":
            return
        chat_id = msg.conversation.chat_id
        with self._lock:
            if self._scope == "chat+sender":
                prefix = f"{chat_id}::"
                for key in [k for k in self._states if k == chat_id or k.startswith(prefix)]:
                    self._states.pop(key, None)
            else:
                self._states.pop(chat_id, None)

    def _warn(self, fmt: str, *args: Any) -> None:
        warn = getattr(self._logger, "warning", None)
        if callable(warn):
            warn("channel: " + fmt, *args)

    # ---- counting ------------------------------------------------------------

    def record(self, msg: Any) -> bool:
        """Record a message; return whether its key is now tripped.

        A human message resets the key and never trips; messages that aren't
        "another bot @'d me" don't count. A re-delivered ``message_id`` already
        inside the window is counted once. The first trip of a key warns once.
        """
        if not self.enabled:
            return False
        with self._lock:
            return self._record_locked(msg)

    def _record_locked(self, msg: Any) -> bool:
        key = self._key_for(msg)

        if msg.sender.sender_type == "user":
            self._states.pop(key, None)
            return False
        if not (msg.sender.is_bot and msg.mentioned_bot):
            return False

        state = self._states.get(key) or {"entries": [], "warned": False, "clock": None}
        entries: List[Tuple[str, int]] = state["entries"]
        # Monotonic per-key clock: an out-of-order (older) event can't reopen an
        # already-passed window.
        prev_clock = state["clock"]
        clock = msg.create_time if prev_clock is None else max(prev_clock, msg.create_time)
        cutoff = clock - self._window_ms
        entries = [(mid, t) for (mid, t) in entries if t >= cutoff]
        # Only count the current message if it falls inside the current window
        # (a stale, out-of-order event older than the window does not count and
        # must not be grouped with newer/future entries).
        if msg.create_time >= cutoff and not any(mid == msg.id for (mid, _t) in entries):
            entries.append((msg.id, msg.create_time))
        if len(entries) > _MAX_ENTRIES_PER_KEY:
            entries = entries[-_MAX_ENTRIES_PER_KEY:]
        state["entries"] = entries
        state["clock"] = clock

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
            state["warned"] = False

        self._remember(key, state)
        return tripped

    def _remember(self, key: str, state: Dict[str, Any]) -> None:
        self._states.pop(key, None)
        self._states[key] = state
        while len(self._states) > _MAX_KEYS:
            self._states.popitem(last=False)

    def _key_for(self, msg: Any) -> str:
        chat_id = msg.conversation.chat_id
        if self._scope == "chat+sender":
            return f"{chat_id}::{msg.sender.open_id}"
        return chat_id
