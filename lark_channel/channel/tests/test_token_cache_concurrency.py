"""LocalCache is thread-safe on the expiry path.

The process-global token store is now reached from concurrent threads (roster
token verification runs in an executor). A concurrent expiry eviction must not
race into a KeyError.
"""

import threading
import time

from lark_channel.core.cache.local_cache import LocalCache


def test_concurrent_get_of_expired_key_never_raises_keyerror():
    cache = LocalCache()
    cache.set("k", "v", int(time.time()) - 1)  # already expired
    errors = []

    def worker():
        try:
            for _ in range(2000):
                cache.get("k")
        except Exception as e:  # noqa: BLE001
            errors.append(e)

    threads = [threading.Thread(target=worker) for _ in range(8)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert errors == []
    assert cache.get("k") is None


def test_get_returns_live_value_before_expiry():
    cache = LocalCache()
    cache.set("k", "v", int(time.time()) + 60)
    assert cache.get("k") == "v"
