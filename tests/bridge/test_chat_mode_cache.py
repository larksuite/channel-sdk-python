import pytest

from lark_channel import ChannelConfig, ChatModeCacheConfig, FeishuChannel
from lark_channel.channel.chat_mode import ChatModeCache
from lark_channel.channel.types import ChatInfo


def test_chat_info_chat_mode_property_reads_raw_without_constructor_break():
    info = ChatInfo("oc_1", raw={"chat_mode": "thread"})
    assert info.chat_id == "oc_1"
    assert info.chat_mode == "thread"


@pytest.mark.asyncio
async def test_get_chat_mode_caches_success(monkeypatch):
    ch = FeishuChannel(
        app_id="cli_x",
        app_secret="s",
        config=ChannelConfig(chat_mode_cache=ChatModeCacheConfig(enabled=True)),
    )
    calls = []

    async def fake_fetch(client, chat_id):
        calls.append(chat_id)
        return ChatInfo(chat_id=chat_id, raw={"chat_mode": "thread"})

    monkeypatch.setattr("lark_channel.channel._api_helpers.fetch_chat_info", fake_fetch)

    assert await ch.get_chat_mode("oc_1") == "thread"
    assert await ch.get_chat_mode("oc_1") == "thread"
    assert calls == ["oc_1"]


@pytest.mark.asyncio
async def test_get_chat_mode_fallback_group_does_not_pollute_cache(monkeypatch):
    ch = FeishuChannel(
        app_id="cli_x",
        app_secret="s",
        config=ChannelConfig(chat_mode_cache=ChatModeCacheConfig(fallback="group")),
    )

    async def fake_fetch(client, chat_id):
        return None

    monkeypatch.setattr("lark_channel.channel._api_helpers.fetch_chat_info", fake_fetch)

    assert await ch.get_chat_mode("oc_1") == "group"
    assert "oc_1" not in ch._chat_mode_cache._values


def test_chat_mode_cache_lru_and_ttl(monkeypatch):
    now = [100.0]
    monkeypatch.setattr("lark_channel.channel.chat_mode.time.time", lambda: now[0])
    cache = ChatModeCache(ChatModeCacheConfig(max_size=2, ttl_seconds=10))

    cache.set("oc_1", "group")
    cache.set("oc_2", "topic")
    assert cache.get("oc_1") == "group"

    cache.set("oc_3", "thread")
    assert cache.get("oc_2") is None
    assert cache.get("oc_1") == "group"

    now[0] = 111.0
    assert cache.get("oc_1") is None


def test_chat_mode_cache_non_positive_max_size_disables_writes():
    cache = ChatModeCache(ChatModeCacheConfig(max_size=0))
    cache.set("oc_1", "group")
    assert cache.get("oc_1") is None

    cache = ChatModeCache(ChatModeCacheConfig(max_size=-1))
    cache.set("oc_1", "group")
    assert cache.get("oc_1") is None
