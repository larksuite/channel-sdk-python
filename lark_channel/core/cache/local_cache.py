import threading
import time
from typing import Dict, Optional

from lark_channel.core.cache import ICache


class LocalCache(ICache):

    def __init__(self):
        self.cache: Dict[str, LocalCache.ValueWrap] = {}
        # This cache backs the process-global tenant/app token store, which is
        # now reachable from concurrent threads (roster token verify runs in an
        # executor). Guard get/set so an expiry eviction can't race into a
        # KeyError, and reads/writes don't tear.
        self._lock = threading.Lock()

    def get(self, key: str) -> Optional[str]:
        with self._lock:
            wrap: LocalCache.ValueWrap = self.cache.get(key)
            if wrap is None:
                return None
            if wrap.expire < time.time():
                # pop (not del) so a concurrent expiry of the same key can't
                # raise KeyError.
                self.cache.pop(key, None)
                return None
            return wrap.value

    def set(self, key: str, value: str, expire: int) -> None:
        with self._lock:
            self.cache[key] = LocalCache.ValueWrap(value, expire)

    @staticmethod
    def instance() -> "LocalCache":
        if not hasattr(LocalCache, "__instance"):
            LocalCache.__instance = LocalCache()
        return LocalCache.__instance

    class ValueWrap(object):
        def __init__(self, value: str, expire: int):
            self.value = value
            self.expire = expire
