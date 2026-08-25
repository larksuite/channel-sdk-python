"""Coverage for `ExpiringCache`, which had none.

The cache is internal — one caller, `ws.client`, dedups inbound message ids
with it — but it used to own a background task per instance, and that task was
the single largest source of shutdown noise in the suite.
"""

import asyncio
import gc

from lark_channel.core.cache.expiring_cache import ExpiringCache


def test_a_value_is_readable_until_it_expires():
    cache = ExpiringCache()
    cache.set("k", "v", 60)
    assert cache.get("k") == "v"


def test_an_expired_value_reads_as_absent():
    cache = ExpiringCache()
    cache.set("k", "v", -1)  # already expired
    assert cache.get("k") is None


def test_a_missing_key_reads_as_absent():
    assert ExpiringCache().get("nope") is None


def test_entries_nobody_reads_again_do_not_accumulate():
    """`get` drops an expired entry when it reads one, so the sweep only matters
    for keys that are never read again — which is the common case for message-id
    dedup, where a repeat is the exception."""
    cache = ExpiringCache(clear_interval=0)  # sweep on every set
    for i in range(50):
        cache.set("stale-%d" % i, "v", -1)
    cache.set("fresh", "v", 60)

    assert cache.get("fresh") == "v"
    assert len(cache._cache) == 1, "expired entries were never reclaimed"


def test_the_sweep_waits_for_its_interval():
    """The sweep is O(n) and `set` runs per message, so it must not run every
    time. Holding off until the interval elapses is what keeps `set` O(1)
    amortized — the same cost the timer it replaced had."""
    cache = ExpiringCache(clear_interval=3600)
    cache.set("stale", "v", -1)
    cache.set("other", "v", 60)

    assert "stale" in cache._cache, "swept on every set; that is O(n) per message"


def test_construction_creates_no_task_and_needs_no_running_loop():
    """An instance is built inside its holder's `__init__`, where there may be
    no running loop yet. Owning a task there left a coroutine that never ran,
    which surfaced at GC time as an unraisable warning attributed to unrelated
    code, and cancelling it from `__del__` raised on an already-closed loop.
    An implementation that goes back to owning a task fails this test.
    """
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        cache = ExpiringCache(clear_interval=1)
        cache.set("k", "v", 60)
        assert not asyncio.all_tasks(loop), "the cache scheduled work on the loop"
        del cache
        gc.collect()  # a leaked coroutine would warn here; pytest.ini errors on it
    finally:
        asyncio.set_event_loop(None)
        loop.close()
