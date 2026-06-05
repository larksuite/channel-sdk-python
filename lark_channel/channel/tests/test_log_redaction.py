import json
import logging
from types import SimpleNamespace

import pytest
from requests_toolbelt import MultipartEncoder

from lark_channel.card.action_handler import CardActionHandler
from lark_channel.channel import SecurityConfig
from lark_channel.core.const import AUTHORIZATION
from lark_channel.core.enum import AccessTokenType, HttpMethod
from lark_channel.core.http.transport import Transport
from lark_channel.core.model import BaseRequest, Config, RawRequest, RequestOption
from lark_channel.event.dispatcher_handler import EventDispatcherHandler
from lark_channel.ws import client as ws_client
from lark_channel.ws.const import (
    HEADER_MESSAGE_ID,
    HEADER_SEQ,
    HEADER_SUM,
    HEADER_TRACE_ID,
    HEADER_TYPE,
)
from lark_channel.ws.enum import FrameType, MessageType
from lark_channel.ws.pb.pbbp2_pb2 import Frame


def test_transport_debug_log_redacts_authorization_and_secret(caplog, monkeypatch):
    captured = {}

    def fake_request(method, url, **kwargs):
        captured["headers"] = kwargs["headers"]
        return SimpleNamespace(status_code=200, headers={}, content=b"{}")

    monkeypatch.setattr("lark_channel.core.http.transport.requests.request", fake_request)

    conf = Config()
    conf.domain = "https://open.feishu.cn"
    req = BaseRequest()
    req.http_method = HttpMethod.POST
    req.uri = "/open-apis/auth/v3/tenant_access_token/internal"
    req.token_types = {AccessTokenType.TENANT}
    req.body = {
        "app_id": "cli_x",
        "app_secret": "app_secret_value",
        "refresh_token": "refresh_token_value",
    }
    option = RequestOption()
    option.tenant_access_token = "tenant_token_value"

    with caplog.at_level(logging.DEBUG, logger="Lark"):
        Transport.execute(conf, req, option)

    assert captured["headers"][AUTHORIZATION] == "Bearer tenant_token_value"
    assert "tenant_token_value" not in caplog.text
    assert "app_secret_value" not in caplog.text
    assert "refresh_token_value" not in caplog.text
    assert "Bearer ***" in caplog.text


def test_transport_debug_log_redacts_common_security_headers_and_camel_tokens(caplog, monkeypatch):
    def fake_request(method, url, **kwargs):
        return SimpleNamespace(status_code=200, headers={}, content=b"{}")

    monkeypatch.setattr("lark_channel.core.http.transport.requests.request", fake_request)

    conf = Config()
    conf.domain = "https://open.feishu.cn"
    req = BaseRequest()
    req.http_method = HttpMethod.POST
    req.uri = "/open-apis/example"
    req.headers = {
        "Cookie": "sessionid=cookie-secret-value",
        "X-Api-Key": "api-key-secret-value",
    }
    req.body = {
        "verificationToken": "verification-token-secret",
        "accessToken": "access-token-secret",
        "encrypt_key": "encrypt-key-secret",
        "private_key": "private-key-secret",
        "safe": "visible",
    }

    with caplog.at_level(logging.DEBUG, logger="Lark"):
        Transport.execute(conf, req, RequestOption())

    for secret in (
        "cookie-secret-value",
        "api-key-secret-value",
        "verification-token-secret",
        "access-token-secret",
        "encrypt-key-secret",
        "private-key-secret",
    ):
        assert secret not in caplog.text
    assert "visible" in caplog.text


def test_transport_debug_log_redacts_sensitive_query_params(caplog, monkeypatch):
    def fake_request(method, url, **kwargs):
        return SimpleNamespace(status_code=200, headers={}, content=b"{}")

    monkeypatch.setattr("lark_channel.core.http.transport.requests.request", fake_request)

    conf = Config()
    conf.domain = "https://open.feishu.cn"
    req = BaseRequest()
    req.http_method = HttpMethod.GET
    req.uri = "/open-apis/example"
    req.queries = [
        ("access_token", "query_token_value"),
        ("secret", "query_secret_value"),
        ("file_token", "doc_1"),
    ]

    with caplog.at_level(logging.DEBUG, logger="Lark"):
        Transport.execute(conf, req, RequestOption())

    assert "query_token_value" not in caplog.text
    assert "query_secret_value" not in caplog.text
    assert "doc_1" in caplog.text


def test_transport_log_handles_multipart_body_at_default_level(monkeypatch):
    captured = {}

    def fake_request(method, url, **kwargs):
        captured["data"] = kwargs["data"]
        return SimpleNamespace(status_code=200, headers={}, content=b"{}")

    monkeypatch.setattr("lark_channel.core.http.transport.requests.request", fake_request)

    conf = Config()
    conf.domain = "https://open.feishu.cn"
    req = BaseRequest()
    req.http_method = HttpMethod.POST
    req.uri = "/open-apis/im/v1/images"
    encoder = MultipartEncoder(
        fields={
            "image_type": "message",
            "image": ("secret.png", b"secret-upload-content", "image/png"),
        }
    )
    req.body = encoder

    Transport.execute(conf, req, RequestOption())

    assert captured["data"] is encoder


def test_transport_debug_log_handles_multipart_body_without_leaking_contents(caplog, monkeypatch):
    captured = {}

    def fake_request(method, url, **kwargs):
        captured["data"] = kwargs["data"]
        return SimpleNamespace(status_code=200, headers={}, content=b"{}")

    monkeypatch.setattr("lark_channel.core.http.transport.requests.request", fake_request)

    conf = Config()
    conf.domain = "https://open.feishu.cn"
    req = BaseRequest()
    req.http_method = HttpMethod.POST
    req.uri = "/open-apis/im/v1/files"
    encoder = MultipartEncoder(
        fields={
            "file_type": "stream",
            "file": ("secret.txt", b"secret-upload-content", "text/plain"),
        }
    )
    req.body = encoder

    with caplog.at_level(logging.DEBUG, logger="Lark"):
        Transport.execute(conf, req, RequestOption())

    assert captured["data"] is encoder
    assert "secret-upload-content" not in caplog.text
    assert "secret.txt" not in caplog.text
    assert "<MultipartEncoder>" in caplog.text


@pytest.mark.asyncio
async def test_async_transport_debug_log_redacts_json_multipart_file_bytes(caplog, monkeypatch):
    captured = {}

    class FakeAsyncClient:
        def __init__(self, **kwargs):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            return False

        async def request(self, *args, **kwargs):
            captured["files"] = kwargs["files"]
            return SimpleNamespace(status_code=200, headers={}, content=b"{}")

    monkeypatch.setattr("lark_channel.core.http.transport.httpx.AsyncClient", FakeAsyncClient)

    conf = Config()
    conf.domain = "https://open.feishu.cn"
    req = BaseRequest()
    req.http_method = HttpMethod.POST
    req.uri = "/open-apis/im/v1/files"
    req.files = {
        "file": (
            "report.json",
            b'{"api_key":"plain-secret","note":"file-body"}',
            "application/json",
        )
    }

    with caplog.at_level(logging.DEBUG, logger="Lark"):
        await Transport.aexecute(conf, req, RequestOption())

    assert captured["files"] is req.files
    assert "plain-secret" not in caplog.text
    assert "file-body" not in caplog.text
    assert "report.json" not in caplog.text
    assert "<file upload>" in caplog.text


def test_event_debug_log_redacts_plain_verification_token(caplog):
    req = RawRequest()
    req.uri = "/callback"
    req.headers = {}
    req.body = json.dumps(
        {
            "type": "url_verification",
            "challenge": "challenge-code",
            "token": "verification-token-value",
        }
    ).encode("utf-8")
    handler = EventDispatcherHandler.builder("", "verification-token-value").build()

    with caplog.at_level(logging.DEBUG, logger="Lark"):
        handler.do(req)

    assert "verification-token-value" not in caplog.text


def test_event_strict_error_log_redacts_exception_detail(caplog):
    message = "app_secret=event-secret-value"

    def raise_error(_event):
        raise RuntimeError(message)

    handler = (
        EventDispatcherHandler.builder(
            "",
            "verification-token",
            security=SecurityConfig(mode="strict"),
        )
        .register_p2_customized_event("example.event", raise_error)
        .build()
    )
    req = RawRequest()
    req.uri = "/callback"
    req.headers = {}
    req.body = json.dumps(
        {
            "schema": "2.0",
            "header": {
                "event_type": "example.event",
                "token": "verification-token",
            },
            "event": {"value": "ok"},
        }
    ).encode("utf-8")

    with caplog.at_level(logging.ERROR, logger="Lark"):
        handler.do(req)

    assert "event-secret-value" not in caplog.text
    assert "app_secret" in caplog.text


def test_card_debug_log_redacts_plain_verification_token(caplog):
    req = RawRequest()
    req.uri = "/callback"
    req.headers = {}
    req.body = json.dumps(
        {
            "type": "url_verification",
            "challenge": "challenge-code",
            "token": "verification-token-value",
        }
    ).encode("utf-8")
    handler = CardActionHandler.builder("", "verification-token-value").build()

    with caplog.at_level(logging.DEBUG, logger="Lark"):
        handler.do(req)

    assert "verification-token-value" not in caplog.text


def test_card_strict_error_log_redacts_exception_detail(caplog):
    message = "app_secret=card-secret-value"

    def raise_error(_card):
        raise RuntimeError(message)

    handler = (
        CardActionHandler.builder(
            "",
            "",
            security=SecurityConfig(mode="strict"),
        )
        .register(raise_error)
        .build()
    )
    req = RawRequest()
    req.uri = "/callback"
    req.headers = {}
    req.body = json.dumps(
        {
            "type": "card.action.trigger",
            "action": {"value": {"key": "value"}},
        }
    ).encode("utf-8")

    with caplog.at_level(logging.ERROR, logger="Lark"):
        handler.do(req)

    assert "card-secret-value" not in caplog.text
    assert "app_secret" in caplog.text


@pytest.mark.asyncio
async def test_ws_debug_log_does_not_emit_event_payload(caplog, monkeypatch):
    client = ws_client.Client(app_id="cli_x", app_secret="s")
    client._service_id = "42"
    client._event_handler = SimpleNamespace(_do_without_validation=lambda payload: None)

    async def fake_write_message(data):
        return None

    monkeypatch.setattr(client, "_write_message", fake_write_message)

    frame = Frame()
    frame.method = FrameType.DATA.value
    frame.service = 42
    frame.SeqID = 1
    frame.LogID = 1
    frame.payload = b'{"event":{"text":"payload-secret-value"}}'
    for key, value in (
        (HEADER_MESSAGE_ID, "msg_1"),
        (HEADER_TRACE_ID, "trace_1"),
        (HEADER_SUM, "1"),
        (HEADER_SEQ, "0"),
        (HEADER_TYPE, MessageType.EVENT.value),
    ):
        header = frame.headers.add()
        header.key = key
        header.value = value

    with caplog.at_level(logging.DEBUG, logger="Lark"):
        await client._handle_data_frame(frame)

    assert "payload-secret-value" not in caplog.text
    assert "payload_len" in caplog.text
