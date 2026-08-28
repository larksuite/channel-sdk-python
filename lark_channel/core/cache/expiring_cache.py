import time
from typing import Dict, Tuple, Any


class ExpiringCache(object):
    """A dict whose entries expire, reclaimed as it is used.

    Reclamation is opportunistic rather than scheduled: ``set`` sweeps expired
    entries once ``clear_interval`` has passed since the last sweep, so the
    cadence matches a timer's without owning one.

    It used to own one. ``__init__`` created a task for a sweep coroutine, which
    meant every instance needed a **running** event loop to ever start it — and
    the instance is built in ``__init__`` of its holder, where there may not be
    one yet. A loop that never ran left the coroutine unawaited, surfacing at GC
    time as "coroutine ... was never awaited" attributed to whatever happened to
    be running then, and ``__del__`` cancelling that task raised "Event loop is
    closed" during interpreter shutdown. Neither is worth a timer here: the only
    caller keeps entries for five seconds, and ``get`` already drops an expired
    entry when it reads one, so the sweep exists purely to keep keys that are
    never read again from accumulating.
    """

    def __init__(self, clear_interval=60):
        self._cache: Dict[str, Tuple[Any, float]] = {}
        self._clear_interval: int = clear_interval
        self._last_clear: float = time.time()

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
        now = time.time()
        # Amortized: the scan is O(n), but it runs at most once per interval, so
        # a per-message `set` stays O(1) on average — the cost the timer had.
        if now - self._last_clear >= self._clear_interval:
            self._clear(now)
        expire = now + ttl
        self._cache[key] = (value, expire)

    def _clear(self, now: float = None):
        if now is None:
            now = time.time()
        self._last_clear = now
        expired_keys = [key for key, (value, expire) in self._cache.items() if expire < now]
        for key in expired_keys:
            del self._cache[key]
