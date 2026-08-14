import asyncio
import time
from typing import Any, Dict, Optional, Tuple


class ExpiringCache(object):

    def __init__(self, clear_interval=60):
        self._cache: Dict[str, Tuple[Any, float]] = {}
        self._clear_interval: int = clear_interval
        self._loop: Optional[asyncio.AbstractEventLoop] = None
        self._cron: Optional[asyncio.TimerHandle] = None

    def bind_loop(self, loop: asyncio.AbstractEventLoop) -> None:
        """Run expiry cleanup on the loop that owns this cache's client.

        Construction deliberately has no event-loop side effects.  Callers can
        therefore create a cache from synchronous code or from an unrelated
        running loop without capturing that ambient loop.
        """
        if (
            self._loop is loop
            and self._cron is not None
            and not self._cron.cancelled()
        ):
            return
        self.close()
        self._loop = loop
        self._schedule_clear()

    def close(self) -> None:
        cron = self._cron
        if cron is not None and not cron.cancelled():
            cron.cancel()
        self._cron = None
        self._loop = None

    def __del__(self):
        self.close()

    def get(self, key: str) -> Any:
        elem = self._cache.get(key)
        if not elem:
            return None
        value, expire = elem
        if expire < time.time():
            del self._cache[key]
            return None

        return value

    # ttl: time to live, in seconds
    def set(self, key: str, value: Any, ttl: int):
        expire = time.time() + ttl
        self._cache[key] = (value, expire)

    def _clear(self):
        now = time.time()
        expired_keys = [key for key, (value, expire) in self._cache.items() if expire < now]
        for key in expired_keys:
            del self._cache[key]

    def _schedule_clear(self) -> None:
        loop = self._loop
        if loop is None or loop.is_closed():
            return
        self._cron = loop.call_later(self._clear_interval, self._run_clear_cron)

    def _run_clear_cron(self) -> None:
        self._cron = None
        self._clear()
        self._schedule_clear()
