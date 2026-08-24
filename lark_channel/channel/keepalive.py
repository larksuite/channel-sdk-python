import inspect
import logging
import time
from typing import Callable

from .config import KeepaliveConfig

logger = logging.getLogger(__name__)


class KeepaliveWatchdog:
    def __init__(
        self,
        *,
        config: KeepaliveConfig,
        probe: Callable[[], bool],
        reconnect: Callable[[], None],
        last_activity: Callable[[], float],
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self._config = config
        self._probe = probe
        self._reconnect = reconnect
        self._last_activity = last_activity
        self._clock = clock
        self._failures = 0

    async def run_once(self) -> None:
        if not self._config.enabled:
            return
        now = self._clock()
        last = self._last_activity()
        idle = now - last
        if idle < self._config.wake_threshold_seconds:
            self._failures = 0
            return
        ok = self._probe()
        if inspect.isawaitable(ok):
            ok = await ok
        if ok:
            self._failures = 0
            return
        self._failures += 1
        logger.warning(
            "keepalive probe failed (%d/%d), idle %.1fs",
            self._failures,
            self._config.failure_threshold,
            idle,
        )
        if self._failures >= self._config.failure_threshold:
            self._failures = 0
            logger.info("keepalive forcing reconnect after failed probes")
            self._reconnect()
