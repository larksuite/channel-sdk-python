from types import SimpleNamespace

import pytest

from lark_channel import (
    CachedResource,
    ChatModeCacheConfig,
    CommentContext,
    CommentTarget,
    FeishuChannel,
    KeepaliveConfig,
    MediaCacheConfig,
    QuotedContext,
    QuoteResolution,
    TransportConfig,
)
import lark_channel
from lark_channel import channel as channel_exports
from lark_channel.core.enum import HttpMethod
from lark_channel.core.http import transport as http_transport
from lark_channel.core.model import BaseRequest, Config


def _request():
    req = BaseRequest()
    req.http_method = HttpMethod.GET
    req.uri = "/open-apis/ping"
    req.token_types = set()
    return req


def test_package_root_reexports_bridge_public_types():
    exports = {
        "CachedResource": CachedResource,
        "ChatModeCacheConfig": ChatModeCacheConfig,
        "CommentContext": CommentContext,
        "CommentTarget": CommentTarget,
        "KeepaliveConfig": KeepaliveConfig,
        "MediaCacheConfig": MediaCacheConfig,
        "QuotedContext": QuotedContext,
        "QuoteResolution": QuoteResolution,
    }
    for name, exported in exports.items():
        assert exported is getattr(channel_exports, name)
        assert exported is getattr(lark_channel, name)
        assert name in lark_channel.__all__


def test_channel_passes_http_timeout_to_client_config():
    ch = FeishuChannel(
        app_id="cli_x",
        app_secret="s",
        transport=TransportConfig(http_timeout_seconds=12.5),
    )
    assert ch.client.config.timeout == 12.5


def test_channel_default_timeout_matches_existing_client_default():
    ch = FeishuChannel(app_id="cli_x", app_secret="s")
    assert ch.client.config.timeout == 30


def test_sync_transport_uses_explicit_proxy(monkeypatch):
    calls = {}

    def fake_request(method, url, **kwargs):
        calls.update(kwargs)
        return SimpleNamespace(status_code=200, headers={"Content-Type": "application/json"}, content=b'{"code":0}')

    monkeypatch.setattr(http_transport.requests, "request", fake_request)
    conf = Config()
    conf.proxy_url = "http://127.0.0.1:8080"
    http_transport.Transport.execute(conf, _request())
    assert calls["proxies"] == {
        "http": "http://127.0.0.1:8080",
        "https": "http://127.0.0.1:8080",
    }


def test_sync_transport_disables_env_proxy_with_session(monkeypatch):
    calls = {}

    class FakeSession:
        trust_env = True

        def request(self, method, url, **kwargs):
            calls["trust_env"] = self.trust_env
            calls["kwargs"] = kwargs
            return SimpleNamespace(status_code=200, headers={"Content-Type": "application/json"}, content=b'{"code":0}')

    monkeypatch.setattr(http_transport.requests, "Session", lambda: FakeSession())
    conf = Config()
    conf.trust_env_proxy = False
    http_transport.Transport.execute(conf, _request())
    assert calls["trust_env"] is False
    assert "trust_env" not in calls["kwargs"]


@pytest.mark.asyncio
async def test_async_transport_disables_env_when_requested(monkeypatch):
    captured = {}

    class FakeAsyncClient:
        def __init__(self, **kwargs):
            captured.update(kwargs)

        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            return False

        async def request(self, *args, **kwargs):
            return SimpleNamespace(status_code=200, headers={"Content-Type": "application/json"}, content=b'{"code":0}')

    monkeypatch.setattr(http_transport.httpx, "AsyncClient", FakeAsyncClient)
    conf = Config()
    conf.trust_env_proxy = False
    await http_transport.Transport.aexecute(conf, _request())
    assert captured["trust_env"] is False


def test_httpx_client_kwargs_uses_proxy_parameter(monkeypatch):
    class FakeAsyncClient:
        def __init__(self, *, proxy=None, trust_env=True):
            pass

    monkeypatch.setattr(http_transport.httpx, "AsyncClient", FakeAsyncClient)

    conf = Config()
    conf.proxy_url = "http://127.0.0.1:8080"

    assert http_transport._httpx_client_kwargs(conf)["proxy"] == "http://127.0.0.1:8080"


def test_httpx_client_kwargs_uses_legacy_proxies_parameter(monkeypatch):
    class FakeAsyncClient:
        def __init__(self, *, proxies=None, trust_env=True):
            pass

    monkeypatch.setattr(http_transport.httpx, "AsyncClient", FakeAsyncClient)

    conf = Config()
    conf.proxy_url = "http://127.0.0.1:8080"

    assert http_transport._httpx_client_kwargs(conf)["proxies"] == "http://127.0.0.1:8080"
