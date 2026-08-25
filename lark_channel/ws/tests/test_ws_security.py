import asyncio
import logging
from types import SimpleNamespace

import pytest

from lark_channel.channel.config import SecurityConfig
from lark_channel.event.security import (
    InMemorySecurityAuditRecorder,
    REASON_WS_FRAGMENT_LIMIT,
    REASON_WS_INSECURE_SCHEME,
    REASON_WS_INVALID_TIMING,
)
from lark_channel.ws import client as ws_client
from lark_channel.ws.exception import ClientException
from lark_channel.ws.model import ClientConfig


@pytest.mark.asyncio
async def test_ws_compat_allows_insecure_endpoint_and_audits(monkeypatch):
    calls = {}
    recorder = InMemorySecurityAuditRecorder()

    async def fake_connect(url, **kwargs):
        calls["url"] = url
        return SimpleNamespace(close=lambda: None)

    def close_task(coro):
        coro.close()

    client = ws_client.Client(
        app_id="cli_x",
        app_secret="s",
        security=SecurityConfig(mode="compat", audit_recorder=recorder),
    )
    monkeypatch.setattr(
        client,
        "_get_conn_url",
        lambda: "ws://example.test/callback?device_id=device&service_id=42",
    )
    monkeypatch.setattr(ws_client.websockets, "connect", fake_connect)
    monkeypatch.setattr(ws_client, "loop", SimpleNamespace(create_task=close_task))

    await client._connect()

    assert calls["url"].startswith("ws://example.test")
    assert [event.reason for event in recorder.events] == [REASON_WS_INSECURE_SCHEME]


@pytest.mark.asyncio
async def test_ws_default_compat_allows_insecure_endpoint_without_audit_warning(
    monkeypatch,
    caplog,
):
    calls = {}

    async def fake_connect(url, **kwargs):
        calls["url"] = url
        return SimpleNamespace(close=lambda: None)

    def close_task(coro):
        coro.close()

    client = ws_client.Client(
        app_id="cli_x",
        app_secret="s",
        security=SecurityConfig(mode="compat"),
    )
    monkeypatch.setattr(
        client,
        "_get_conn_url",
        lambda: "ws://example.test/callback?device_id=device&service_id=42",
    )
    monkeypatch.setattr(ws_client.websockets, "connect", fake_connect)
    monkeypatch.setattr(ws_client, "loop", SimpleNamespace(create_task=close_task))

    with caplog.at_level(logging.WARNING, logger="Lark"):
        await client._connect()

    assert calls["url"].startswith("ws://example.test")
    assert "security audit" not in caplog.text


@pytest.mark.asyncio
async def test_ws_strict_rejects_insecure_endpoint_before_connect(monkeypatch):
    recorder = InMemorySecurityAuditRecorder()

    async def fail_connect(*_args, **_kwargs):
        raise AssertionError("connect should not run")

    client = ws_client.Client(
        app_id="cli_x",
        app_secret="s",
        security=SecurityConfig(mode="strict", audit_recorder=recorder),
    )
    monkeypatch.setattr(
        client,
        "_get_conn_url",
        lambda: "ws://example.test/callback?device_id=device&service_id=42",
    )
    monkeypatch.setattr(ws_client.websockets, "connect", fail_connect)

    with pytest.raises(ClientException):
        await client._connect()

    assert [event.reason for event in recorder.events] == [REASON_WS_INSECURE_SCHEME]


@pytest.mark.asyncio
async def test_ws_strict_allow_insecure_endpoint_records_allow_action(monkeypatch):
    calls = {}
    recorder = InMemorySecurityAuditRecorder()

    async def fake_connect(url, **kwargs):
        calls["url"] = url
        return SimpleNamespace(close=lambda: None)

    def close_task(coro):
        coro.close()

    client = ws_client.Client(
        app_id="cli_x",
        app_secret="s",
        security=SecurityConfig(
            mode="strict",
            allow_insecure_ws=True,
            audit_recorder=recorder,
        ),
    )
    monkeypatch.setattr(
        client,
        "_get_conn_url",
        lambda: "ws://example.test/callback?device_id=device&service_id=42",
    )
    monkeypatch.setattr(ws_client.websockets, "connect", fake_connect)
    monkeypatch.setattr(ws_client, "loop", SimpleNamespace(create_task=close_task))

    await client._connect()

    assert calls["url"].startswith("ws://example.test")
    assert [event.reason for event in recorder.events] == [REASON_WS_INSECURE_SCHEME]
    assert recorder.events[0].action == "allow"


@pytest.mark.asyncio
async def test_ws_strict_allows_local_insecure_endpoint_by_default(monkeypatch):
    calls = {}

    async def fake_connect(url, **kwargs):
        calls["url"] = url
        return SimpleNamespace(close=lambda: None)

    def close_task(coro):
        coro.close()

    client = ws_client.Client(
        app_id="cli_x",
        app_secret="s",
        security=SecurityConfig(mode="strict"),
    )
    monkeypatch.setattr(
        client,
        "_get_conn_url",
        lambda: "ws://127.0.0.1/callback?device_id=device&service_id=42",
    )
    monkeypatch.setattr(ws_client.websockets, "connect", fake_connect)
    monkeypatch.setattr(ws_client, "loop", SimpleNamespace(create_task=close_task))

    await client._connect()

    assert calls["url"].startswith("ws://127.0.0.1")


def test_ws_fragment_combine_does_not_preallocate_by_sum():
    client = ws_client.Client(app_id="cli_x", app_secret="s")

    assert client._combine("msg_1", 10_000_000, 0, b"part") is None
    cached = client._cache.get("msg_1")

    assert isinstance(cached["parts"], dict)
    assert cached["sum"] == 10_000_000
    assert cached["parts"] == {0: b"part"}


def test_ws_fragment_limits_are_audited_without_default_drop():
    recorder = InMemorySecurityAuditRecorder()
    client = ws_client.Client(
        app_id="cli_x",
        app_secret="s",
        security=SecurityConfig(
            mode="audit",
            audit_recorder=recorder,
            max_ws_fragment_parts=2,
        ),
    )

    assert client._combine("msg_1", 3, 0, b"a") is None
    assert client._combine("msg_1", 3, 1, b"b") is None
    assert client._combine("msg_1", 3, 2, b"c") == b"abc"
    assert [event.reason for event in recorder.events] == [
        REASON_WS_FRAGMENT_LIMIT,
        REASON_WS_FRAGMENT_LIMIT,
        REASON_WS_FRAGMENT_LIMIT,
    ]


def test_ws_configure_sanitizes_invalid_timing_values():
    recorder = InMemorySecurityAuditRecorder()
    client = ws_client.Client(
        app_id="cli_x",
        app_secret="s",
        security=SecurityConfig(mode="compat", audit_recorder=recorder),
    )
    conf = ClientConfig(
        {
            "ReconnectCount": -99,
            "ReconnectInterval": 0,
            "ReconnectNonce": -1,
            "PingInterval": 10**12,
        }
    )

    client._configure(conf)

    assert client._reconnect_count == -1
    assert client._reconnect_interval == 120
    assert client._reconnect_nonce == 30
    assert client._ping_interval == 120
    assert [event.reason for event in recorder.events] == [
        REASON_WS_INVALID_TIMING,
        REASON_WS_INVALID_TIMING,
        REASON_WS_INVALID_TIMING,
        REASON_WS_INVALID_TIMING,
    ]


@pytest.mark.asyncio
async def test_ws_max_concurrent_handlers_limits_active_message_tasks(monkeypatch):
    class TwoMessageConn:
        def __init__(self):
            self.count = 0

        async def recv(self):
            self.count += 1
            if self.count <= 2:
                return f"message-{self.count}"
            raise RuntimeError("stop")

    client = ws_client.Client(
        app_id="cli_x",
        app_secret="s",
        auto_reconnect=False,
        security=SecurityConfig(max_concurrent_ws_handlers=1),
    )
    conn = TwoMessageConn()
    started = []
    first_started = asyncio.Event()
    release = asyncio.Event()
    active = 0
    max_active = 0

    async def fake_handle_message(message):
        nonlocal active, max_active
        active += 1
        max_active = max(max_active, active)
        started.append(message)
        first_started.set()
        await release.wait()
        active -= 1

    async def noop_disconnect(*_args, **_kwargs):
        return None

    monkeypatch.setattr(ws_client, "loop", asyncio.get_running_loop())
    monkeypatch.setattr(client, "_handle_message", fake_handle_message)
    monkeypatch.setattr(client, "_disconnect", noop_disconnect)

    task = asyncio.create_task(client._receive_message_loop(conn))
    await asyncio.wait_for(first_started.wait(), timeout=1)
    await asyncio.sleep(0.05)

    assert started == ["message-1"]
    assert max_active == 1

    release.set()
    with pytest.raises(RuntimeError, match="stop"):
        await task
    assert started == ["message-1", "message-2"]
    assert max_active == 1


def test_a_client_can_be_built_from_a_thread_with_no_event_loop():
    """Construction must not require the calling thread to already have a loop.

    On 3.8/3.9 `asyncio.Lock()` and `asyncio.Semaphore()` resolve a loop at
    construction, and `__init__` builds three of them. That requirement was met
    by accident for a long time: `ExpiringCache.__init__`, the line above, looked
    up a loop for its own background task and installed one when there was none.
    Nothing in the suite pinned it, so removing that task turned "you may build a
    client anywhere" into a `RuntimeError` on 3.9 only — invisible on 3.10+.
    """
    previous = None
    try:
        previous = asyncio.get_event_loop()
    except RuntimeError:
        pass
    asyncio.set_event_loop(None)
    try:
        client = ws_client.Client(app_id="cli_test", app_secret="secret")
        assert client is not None
    finally:
        # Restore what was there: leaving the thread without a loop would fail
        # every later test that builds one of these, which is this bug again.
        asyncio.set_event_loop(previous)
