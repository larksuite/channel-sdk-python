"""Per-session send budget.

The bot's own in-meeting messages come back as meeting chat, so a handler that
replies without checking the echo flag amplifies itself at network speed. The
flag is the real fix, but it lives in application code; this is the backstop
that keeps a missing check from becoming in-meeting flooding, quota exhaustion,
and platform-side risk controls.
"""

import time
from collections import deque
from typing import Deque

_WINDOW_SECONDS = 60.0


class SendRateLimiter:
    """Sliding one-minute window over send attempts."""

    def __init__(self, per_minute: int) -> None:
        self._limit = per_minute
        self._sent: Deque[float] = deque()

    def try_acquire(self) -> bool:
        """Record a send and return whether it is within budget."""
        if self._limit <= 0:
            return True
        now = time.monotonic()
        cutoff = now - _WINDOW_SECONDS
        while self._sent and self._sent[0] < cutoff:
            self._sent.popleft()
        if len(self._sent) >= self._limit:
            return False
        self._sent.append(now)
        return True


__all__ = ["SendRateLimiter"]
