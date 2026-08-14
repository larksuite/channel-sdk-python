import asyncio
from types import SimpleNamespace

import pytest

from lark_channel import FeishuChannel, TransportConfig
from lark_channel.channel.config import KeepaliveConfig
from lark_channel.channel.errors import FeishuChannelError
from lark_channel.channel.keepalive import KeepaliveWatchdog
from lark_channel.ws import client as ws_client


def test_ws_default_keeps_current_direct_connect_behavior(monkeypatch):
    monkeypatch.setattr(ws_client.inspect, "signature", lambda fn: type("S", (), {"parameters": {"proxy": object()}})())
    assert ws_client._ws_connect_kwargs(proxy_url=None, trust_env_proxy=None) == {"proxy": None}


def test_ws_explicit_proxy_wins(monkeypatch):
    monkeypatch.setattr(
        ws_client.inspect,
        "signature",
        lambda fn: type("S", (), {"parameters": {"proxy": object()}})(),
    )
    assert ws_client._ws_connect_kwargs(
        proxy_url="http://127.0.0.1:8080",
        trust_env_proxy=None,
    ) == {"proxy": "http://127.0.0.1:8080"}


def test_ws_explicit_proxy_requires_supported_websockets(monkeypatch):
    monkeypatch.setattr(
        ws_client.inspect,
        "signature",
        lambda fn: type("S", (), {"parameters": {}})(),
    )
    with pytest.raises(RuntimeError, match="explicit proxy"):
        ws_client._ws_connect_kwargs(
            proxy_url="http://127.0.0.1:8080",
            trust_env_proxy=None,
        )


def test_ws_client_default_has_no_handshake_timeout():
    client = ws_client.Client(app_id="cli_x", app_secret="s")
    assert client._handshake_timeout is None


def test_ws_client_accepts_handshake_timeout():
    client = ws_client.Client(
        app_id="cli_x",
        app_secret="s",
        handshake_timeout=7.5,
    )
    assert client._handshake_timeout == 7.5


def test_transport_config_positional_keepalive_compatibility():
    keepalive = KeepaliveConfig(enabled=True)

    cfg = TransportConfig("ws", True, None, 30.0, None, None, keepalive)

    assert cfg.keepalive is keepalive
    assert cfg.handshake_timeout_seconds is None


@pytest.mark.asyncio
async def test_ws_connect_omits_open_timeout_by_default(monkeypatch):
    calls = {}

    async def fake_connect(url, **kwargs):
        calls["url"] = url
        calls["kwargs"] = kwargs
        return SimpleNamespace()

    def close_task(coro):
        coro.close()

    def conn_url():
        calls["endpoint_called"] = True
        return "ws://example.test/callback?device_id=device&service_id=42"

    client = ws_client.Client(app_id="cli_x", app_secret="s")
    monkeypatch.setattr(client, "_get_conn_url", conn_url)
    monkeypatch.setattr(ws_client.websockets, "connect", fake_connect)
    monkeypatch.setattr(client, "_create_task", close_task)

    await client._connect()

    assert calls["endpoint_called"] is True
    assert calls["url"].startswith("ws://example.test/callback")
    assert "open_timeout" not in calls["kwargs"]


@pytest.mark.asyncio
async def test_ws_connect_passes_configured_open_timeout(monkeypatch):
    calls = {}

    async def fake_connect(url, **kwargs):
        calls["url"] = url
        calls["kwargs"] = kwargs
        return SimpleNamespace()

    def close_task(coro):
        coro.close()

    def conn_url(*, timeout=None):
        calls["timeout"] = timeout
        return "ws://example.test/callback?device_id=device&service_id=42"

    client = ws_client.Client(
        app_id="cli_x",
        app_secret="s",
        handshake_timeout=7.5,
    )
    monkeypatch.setattr(client, "_get_conn_url", conn_url)
    monkeypatch.setattr(ws_client.websockets, "connect", fake_connect)
    monkeypatch.setattr(client, "_create_task", close_task)

    await client._connect()

    assert calls["timeout"] == 7.5
    assert calls["kwargs"]["open_timeout"] == 7.5


def test_endpoint_discovery_disables_env_proxy_with_explicit_proxy(monkeypatch):
    calls = {}

    class FakeSession:
        trust_env = True

        def post(self, url, **kwargs):
            calls["trust_env"] = self.trust_env
            calls["url"] = url
            calls["kwargs"] = kwargs
            return SimpleNamespace(
                status_code=200,
                content=b'{"code":0,"data":{"URL":"ws://example.test/callback?device_id=device&service_id=42"}}',
            )

    monkeypatch.setattr(ws_client.requests, "Session", lambda: FakeSession())

    ws_client._post_endpoint(
        "https://open.feishu.cn/open-apis/ws/endpoint",
        headers={"x-test": "1"},
        body={"AppID": "cli_x", "AppSecret": "s"},
        timeout=None,
        proxy_url="http://127.0.0.1:8080",
        trust_env_proxy=False,
    )

    assert calls["trust_env"] is False
    assert calls["kwargs"]["headers"] == {"x-test": "1"}
    assert calls["kwargs"]["json"] == {"AppID": "cli_x", "AppSecret": "s"}
    assert calls["kwargs"]["proxies"] == {
        "http": "http://127.0.0.1:8080",
        "https": "http://127.0.0.1:8080",
    }
    assert "timeout" not in calls["kwargs"]


@pytest.mark.asyncio
async def test_keepalive_disabled_does_not_probe():
    calls = []
    watchdog = KeepaliveWatchdog(
        config=KeepaliveConfig(enabled=False, check_interval_seconds=0.01),
        probe=lambda: calls.append("probe") or True,
        reconnect=lambda: calls.append("reconnect"),
        clock=lambda: 0.0,
    )
    await watchdog.run_once()
    assert calls == []


@pytest.mark.asyncio
async def test_keepalive_reconnects_after_wake_probe_failures():
    times = iter([0.0, 200.0, 400.0])
    calls = []
    watchdog = KeepaliveWatchdog(
        config=KeepaliveConfig(
            enabled=True,
            check_interval_seconds=0.01,
            wake_threshold_seconds=90.0,
            failure_threshold=2,
        ),
        probe=lambda: False,
        reconnect=lambda: calls.append("reconnect"),
        clock=lambda: next(times),
    )
    await watchdog.run_once()
    await watchdog.run_once()
    assert calls == ["reconnect"]


@pytest.mark.asyncio
async def test_ws_request_reconnect_is_serial(monkeypatch):
    events = []
    client = ws_client.Client(app_id="cli_x", app_secret="s")

    async def fake_disconnect(**kwargs):
        events.append("disconnect")

    async def fake_reconnect_locked():
        events.append("reconnect")

    monkeypatch.setattr(client, "_disconnect", fake_disconnect)
    monkeypatch.setattr(client, "_reconnect_locked", fake_reconnect_locked)
    client._set_loop(asyncio.get_running_loop())
    client.request_reconnect()
    client.request_reconnect()
    await asyncio.sleep(0.01)
    assert events == ["disconnect", "reconnect"]


@pytest.mark.asyncio
async def test_ws_connect_returns_without_leaking_lock_when_already_connected():
    client = ws_client.Client(app_id="cli_x", app_secret="s")
    client._conn = object()

    await client._connect()

    assert not client._lock.locked()


@pytest.mark.asyncio
async def test_ws_reconnect_attempts_are_serial(monkeypatch):
    client = ws_client.Client(app_id="cli_x", app_secret="s")
    client._reconnect_nonce = 0
    client._reconnect_count = 1
    active = 0
    max_active = 0
    attempts = []

    async def fake_try_connect(cnt):
        nonlocal active, max_active
        active += 1
        max_active = max(max_active, active)
        await asyncio.sleep(0.01)
        attempts.append(cnt)
        active -= 1
        return True

    monkeypatch.setattr(client, "_try_connect", fake_try_connect)

    await asyncio.gather(client._reconnect(), client._reconnect())

    assert attempts == [0, 0]
    assert max_active == 1


@pytest.mark.asyncio
async def test_disconnect_wait_cancellation_does_not_release_unowned_lock():
    client = ws_client.Client(app_id="cli_x", app_secret="s")
    await client._lock.acquire()
    task = asyncio.create_task(client._disconnect())
    await asyncio.sleep(0)

    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    assert client._lock.locked()
    client._lock.release()


@pytest.mark.asyncio
async def test_disconnect_and_reconnect_sequence_is_serial(monkeypatch):
    client = ws_client.Client(app_id="cli_x", app_secret="s")
    active = 0
    max_active = 0
    events = []

    async def guarded(label):
        nonlocal active, max_active
        active += 1
        max_active = max(max_active, active)
        events.append(f"{label}-start")
        await asyncio.sleep(0.01)
        events.append(f"{label}-end")
        active -= 1

    async def fake_disconnect(**kwargs):
        await guarded("disconnect")

    async def fake_reconnect_locked():
        await guarded("reconnect")

    monkeypatch.setattr(client, "_disconnect", fake_disconnect)
    monkeypatch.setattr(client, "_reconnect_locked", fake_reconnect_locked)

    await asyncio.gather(
        client._request_reconnect_once(),
        client._disconnect_and_reconnect(),
    )

    assert max_active == 1
    assert events[:4] == [
        "disconnect-start",
        "disconnect-end",
        "reconnect-start",
        "reconnect-end",
    ]
    assert events[4:] == [
        "disconnect-start",
        "disconnect-end",
        "reconnect-start",
        "reconnect-end",
    ]


@pytest.mark.asyncio
async def test_stale_receive_loop_does_not_close_new_connection(monkeypatch):
    events = []
    client = ws_client.Client(app_id="cli_x", app_secret="s")

    class FakeConn:
        def __init__(self, name):
            self.name = name

        async def recv(self):
            raise RuntimeError(f"{self.name} closed")

        async def close(self):
            events.append(f"close-{self.name}")

    old_conn = FakeConn("old")
    new_conn = FakeConn("new")
    client._conn = new_conn

    async def fail_if_reconnects():
        events.append("reconnect")

    monkeypatch.setattr(client, "_reconnect_locked", fail_if_reconnects)

    await client._receive_message_loop(old_conn)

    assert client._conn is new_conn
    assert events == []


def test_channel_start_failure_cancels_keepalive(monkeypatch):
    class FailingWS:
        def __init__(self, *args, **kwargs):
            self.on_reconnecting = lambda: None
            self.on_reconnected = lambda: None

        def probe_endpoint(self, *, timeout):
            return True

        def request_reconnect(self):
            return None

        def start(self):
            raise RuntimeError("boom")

    ch = FeishuChannel(
        app_id="cli_x",
        app_secret="s",
        transport=TransportConfig(
            keepalive=KeepaliveConfig(enabled=True, check_interval_seconds=10.0)
        ),
    )
    monkeypatch.setattr(ch, "_fetch_bot_identity_sync", lambda: None)
    monkeypatch.setattr("lark_channel.channel.channel.WSClient", FailingWS)

    with pytest.raises(FeishuChannelError):
        ch.start()

    assert ch._keepalive_watchdog is None
    assert ch._keepalive_future is None
    ch.stop()
