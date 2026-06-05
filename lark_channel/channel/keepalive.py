import inspect
import time
from typing import Callable

from .config import KeepaliveConfig


class KeepaliveWatchdog:
    def __init__(
        self,
        *,
        config: KeepaliveConfig,
        probe: Callable[[], bool],
        reconnect: Callable[[], None],
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self._config = config
        self._probe = probe
        self._reconnect = reconnect
        self._clock = clock
        self._last_tick = clock()
        self._failures = 0

    async def run_once(self) -> None:
        if not self._config.enabled:
            return
        now = self._clock()
        elapsed = now - self._last_tick
        self._last_tick = now
        if elapsed < self._config.wake_threshold_seconds:
            self._failures = 0
            return
        ok = self._probe()
        if inspect.isawaitable(ok):
            ok = await ok
        if ok:
            self._failures = 0
            return
        self._failures += 1
        if self._failures >= self._config.failure_threshold:
            self._failures = 0
            self._reconnect()
