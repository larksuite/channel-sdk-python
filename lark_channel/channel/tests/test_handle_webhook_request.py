"""Tests for FeishuChannel.handle_webhook_request."""

import base64
import hashlib
import json
import logging

import pytest
from Crypto.Cipher import AES

from lark_channel.card.action_handler import CardActionHandler
from lark_channel.channel import FeishuChannel, SecurityConfig
from lark_channel.core.const import (
    LARK_REQUEST_NONCE,
    LARK_REQUEST_SIGNATURE,
    LARK_REQUEST_TIMESTAMP,
)
from lark_channel.core.model import RawRequest
from lark_channel.event.dispatcher_handler import EventDispatcherHandler
from lark_channel.event.security import (
    InMemorySecurityAuditRecorder,
    REASON_CARD_SIGNATURE_INVALID,
    REASON_CARD_SIGNATURE_MISSING,
    REASON_WEBHOOK_SIGNATURE_INVALID,
    REASON_WEBHOOK_SIGNATURE_MISSING,
)


def _request(body, headers=None):
    req = RawRequest()
    req.uri = "/callback"
    req.body = json.dumps(body).encode("utf-8")
    req.headers = headers or {}
    return req


def _request_bytes(body, headers=None):
    req = RawRequest()
    req.uri = "/callback"
    req.body = body
    req.headers = headers or {}
    return req


def _signature_headers():
    return {
        LARK_REQUEST_SIGNATURE: "signature-from-upstream",
        LARK_REQUEST_TIMESTAMP: "1778579753",
        LARK_REQUEST_NONCE: "nonce",
    }


def _encrypted_body(plaintext, encrypt_key):
    raw = json.dumps(plaintext).encode("utf-8")
    key = hashlib.sha256(encrypt_key.encode("utf-8")).digest()
    iv = b"0123456789abcdef"
    pad = AES.block_size - (len(raw) % AES.block_size)
    encrypted = AES.new(key, AES.MODE_CBC, iv).encrypt(raw + bytes([pad]) * pad)
    payload = {"encrypt": base64.b64encode(iv + encrypted).decode("ascii")}
    return json.dumps(payload).encode("utf-8")


def _signed_headers(body, secret, *, algorithm="sha256"):
    timestamp = "1778579753"
    nonce = "nonce"
    data = (timestamp + nonce + secret).encode("utf-8") + body
    if algorithm == "sha1":
        signature = hashlib.sha1(data).hexdigest()
    else:
        signature = hashlib.sha256(data).hexdigest()
    return {
        LARK_REQUEST_SIGNATURE: signature,
        LARK_REQUEST_TIMESTAMP: timestamp,
        LARK_REQUEST_NONCE: nonce,
    }


@pytest.mark.asyncio
async def test_url_verification_challenge_returns_200():
    """A challenge request must round-trip the challenge value."""
    ch = FeishuChannel(
        app_id="cli_x", app_secret="x", verification_token="vtok",
        transport="webhook",
    )
    ch.start()  # webhook mode: builds dispatcher, no WS connect
    try:
        body = json.dumps({
            "type": "url_verification",
            "challenge": "abc-123",
            "token": "vtok",
        }).encode("utf-8")
        status, response_body = await ch.handle_webhook_request(headers={}, body=body)
        assert status == 200
        assert b"abc-123" in response_body
    finally:
        await ch.stop_background()
        ch.stop()


@pytest.mark.asyncio
async def test_invalid_token_yields_500():
    """Mismatched verification_token must surface as a non-200 response."""
    ch = FeishuChannel(
        app_id="cli_x", app_secret="x", verification_token="vtok",
        transport="webhook",
    )
    ch.start()
    try:
        body = json.dumps({
            "type": "url_verification",
            "challenge": "abc",
            "token": "WRONG",
        }).encode("utf-8")
        status, _ = await ch.handle_webhook_request(headers={}, body=body)
        assert status >= 400
    finally:
        await ch.stop_background()
        ch.stop()


@pytest.mark.asyncio
async def test_unknown_event_does_not_raise():
    """A well-formed but unknown event yields a non-2xx but does not raise."""
    ch = FeishuChannel(
        app_id="cli_x", app_secret="x", verification_token="vtok",
        transport="webhook",
    )
    ch.start()
    try:
        body = json.dumps({
            "schema": "2.0",
            "header": {"event_type": "this.event.does.not.exist", "token": "vtok"},
        }).encode("utf-8")
        status, body_out = await ch.handle_webhook_request(headers={}, body=body)
        assert isinstance(status, int)
        assert isinstance(body_out, (bytes, bytearray))
    finally:
        await ch.stop_background()
        ch.stop()


@pytest.mark.asyncio
async def test_returns_bytes_for_response_body():
    """Output body must be bytes for direct write to wire."""
    ch = FeishuChannel(
        app_id="cli_x", app_secret="x", verification_token="vtok",
        transport="webhook",
    )
    ch.start()
    try:
        body = json.dumps({
            "type": "url_verification",
            "challenge": "x",
            "token": "vtok",
        }).encode("utf-8")
        status, body_out = await ch.handle_webhook_request(headers={}, body=body)
        assert isinstance(body_out, (bytes, bytearray))
    finally:
        await ch.stop_background()
        ch.stop()


def test_plaintext_event_with_signature_header_does_not_require_encrypt_key():
    seen = []
    handler = (
        EventDispatcherHandler.builder("", "verification-token")
        .register_p2_customized_event("example.event", lambda event: seen.append(event))
        .build()
    )

    resp = handler.do(
        _request(
            {
                "schema": "2.0",
                "header": {
                    "event_type": "example.event",
                    "token": "verification-token",
                },
                "event": {"value": "ok"},
            },
            _signature_headers(),
        )
    )

    assert resp.status_code == 200
    assert resp.content == b'{"msg":"success"}'
    assert len(seen) == 1


def test_event_url_verification_does_not_require_signature_before_dispatch():
    handler = EventDispatcherHandler.builder("encrypt-key", "verification-token").build()

    resp = handler.do(
        _request(
            {
                "type": "url_verification",
                "challenge": "challenge-code",
                "token": "verification-token",
            }
        )
    )

    assert resp.status_code == 200
    assert json.loads(resp.content) == {"challenge": "challenge-code"}


def test_card_callback_with_signature_header_does_not_require_verification_token():
    seen = []
    handler = CardActionHandler.builder("", "").register(lambda card: seen.append(card)).build()

    resp = handler.do(
        _request(
            {
                "type": "card.action.trigger",
                "action": {"value": {"key": "value"}},
            },
            _signature_headers(),
        )
    )

    assert resp.status_code == 200
    assert resp.content == b'{"msg":"success"}'
    assert len(seen) == 1


def test_signed_encrypted_event_is_verified_before_dispatch():
    seen = []
    body = _encrypted_body(
        {
            "schema": "2.0",
            "header": {
                "event_type": "example.event",
                "token": "verification-token",
            },
            "event": {"value": "ok"},
        },
        "encrypt-key",
    )
    handler = (
        EventDispatcherHandler.builder("encrypt-key", "verification-token")
        .register_p2_customized_event("example.event", lambda event: seen.append(event))
        .build()
    )

    resp = handler.do(
        _request_bytes(body, _signed_headers(body, "encrypt-key", algorithm="sha256"))
    )

    assert resp.status_code == 200
    assert resp.content == b'{"msg":"success"}'
    assert len(seen) == 1


def test_strict_event_invalid_signature_rejects_before_decrypt(monkeypatch):
    recorder = InMemorySecurityAuditRecorder()
    body = _encrypted_body({"type": "url_verification"}, "encrypt-key")

    def fail_decrypt(*_args, **_kwargs):
        raise AssertionError("decrypt should not run")

    monkeypatch.setattr(
        "lark_channel.event.dispatcher_handler.AESCipher.decrypt_str",
        fail_decrypt,
    )
    handler = EventDispatcherHandler.builder(
        "encrypt-key",
        "verification-token",
        security=SecurityConfig(mode="strict", audit_recorder=recorder),
    ).build()

    resp = handler.do(
        _request_bytes(
            body,
            {
                **_signed_headers(body, "wrong-key", algorithm="sha256"),
            },
        )
    )

    assert resp.status_code == 500
    assert json.loads(resp.content) == {"code": 500, "msg": "internal error"}
    assert [event.reason for event in recorder.events] == [
        REASON_WEBHOOK_SIGNATURE_INVALID
    ]


def test_strict_event_missing_signature_non_handshake_rejects():
    """A non-handshake body must stay blocked even though the SDK now
    peeks at decrypted content to check for a url_verification exemption.

    Registers a processor and asserts it is never invoked, so an
    implementation that merely logs the missing-signature reason while
    still letting the request through would fail this test, not just
    one that returns the wrong status code."""
    seen = []
    recorder = InMemorySecurityAuditRecorder()
    body = _encrypted_body(
        {
            "schema": "2.0",
            "header": {
                "event_type": "example.event",
                "token": "verification-token",
            },
            "event": {"value": "ok"},
        },
        "encrypt-key",
    )
    handler = (
        EventDispatcherHandler.builder(
            "encrypt-key",
            "verification-token",
            security=SecurityConfig(mode="strict", audit_recorder=recorder),
        )
        .register_p2_customized_event("example.event", lambda event: seen.append(event))
        .build()
    )

    resp = handler.do(_request_bytes(body, {}))

    assert resp.status_code == 500
    assert json.loads(resp.content) == {"code": 500, "msg": "internal error"}
    assert len(seen) == 0
    assert [event.reason for event in recorder.events] == [
        REASON_WEBHOOK_SIGNATURE_MISSING
    ]


def test_strict_event_missing_signature_undecryptable_body_rejects(monkeypatch):
    """If the url_verification-exemption peek itself can't decrypt the
    body, the request must still be rejected, not silently let through."""
    recorder = InMemorySecurityAuditRecorder()
    body = _encrypted_body({"type": "url_verification"}, "encrypt-key")

    def fail_decrypt(*_args, **_kwargs):
        raise AssertionError("decrypt failed")

    monkeypatch.setattr(
        "lark_channel.event.dispatcher_handler.AESCipher.decrypt_str",
        fail_decrypt,
    )
    handler = EventDispatcherHandler.builder(
        "encrypt-key",
        "verification-token",
        security=SecurityConfig(mode="strict", audit_recorder=recorder),
    ).build()

    resp = handler.do(_request_bytes(body, {}))

    assert resp.status_code == 500
    assert json.loads(resp.content) == {"code": 500, "msg": "internal error"}
    assert [event.reason for event in recorder.events] == [
        REASON_WEBHOOK_SIGNATURE_MISSING
    ]


def test_strict_event_unsigned_encrypted_url_verification_handshake_is_exempted():
    """Regression test for the first-time webhook setup deadlock: the
    Feishu console's "save request URL" challenge is encrypted but never
    signed, so strict mode must not block it before the url_verification
    branch gets a chance to answer it."""
    body = _encrypted_body(
        {
            "type": "url_verification",
            "challenge": "challenge-code",
            "token": "verification-token",
        },
        "encrypt-key",
    )
    handler = EventDispatcherHandler.builder(
        "encrypt-key",
        "verification-token",
        security=SecurityConfig(mode="strict"),
    ).build()

    resp = handler.do(_request_bytes(body, {}))

    assert resp.status_code == 200
    assert json.loads(resp.content) == {"challenge": "challenge-code"}


def test_strict_event_unsigned_encrypted_allow_records_allow_action():
    seen = []
    recorder = InMemorySecurityAuditRecorder()
    body = _encrypted_body(
        {
            "schema": "2.0",
            "header": {
                "event_type": "example.event",
                "token": "verification-token",
            },
            "event": {"value": "ok"},
        },
        "encrypt-key",
    )
    handler = (
        EventDispatcherHandler.builder(
            "encrypt-key",
            "verification-token",
            security=SecurityConfig(
                mode="strict",
                allow_unsigned_encrypted_webhook=True,
                audit_recorder=recorder,
            ),
        )
        .register_p2_customized_event("example.event", lambda event: seen.append(event))
        .build()
    )

    resp = handler.do(_request_bytes(body, {}))

    assert resp.status_code == 200
    assert len(seen) == 1
    assert [event.reason for event in recorder.events] == [
        REASON_WEBHOOK_SIGNATURE_MISSING
    ]
    assert recorder.events[0].action == "allow"


@pytest.mark.parametrize("mode", ["compat", "audit"])
def test_unsigned_encrypted_event_legacy_flow_is_not_blocked(mode):
    seen = []
    recorder = InMemorySecurityAuditRecorder()
    body = _encrypted_body(
        {
            "schema": "2.0",
            "header": {
                "event_type": "example.event",
                "token": "verification-token",
            },
            "event": {"value": "ok"},
        },
        "encrypt-key",
    )
    handler = (
        EventDispatcherHandler.builder(
            "encrypt-key",
            "verification-token",
            security=SecurityConfig(mode=mode, audit_recorder=recorder),
        )
        .register_p2_customized_event("example.event", lambda event: seen.append(event))
        .build()
    )

    resp = handler.do(_request_bytes(body, {}))

    assert resp.status_code == 200
    assert resp.content == b'{"msg":"success"}'
    assert len(seen) == 1
    assert [event.reason for event in recorder.events] == [
        REASON_WEBHOOK_SIGNATURE_MISSING
    ]


def test_unsigned_encrypted_event_default_compat_does_not_log_audit(caplog):
    seen = []
    body = _encrypted_body(
        {
            "schema": "2.0",
            "header": {
                "event_type": "example.event",
                "token": "verification-token",
            },
            "event": {"value": "ok"},
        },
        "encrypt-key",
    )
    handler = (
        EventDispatcherHandler.builder(
            "encrypt-key",
            "verification-token",
            security=SecurityConfig(mode="compat"),
        )
        .register_p2_customized_event("example.event", lambda event: seen.append(event))
        .build()
    )

    with caplog.at_level(logging.WARNING, logger="Lark"):
        resp = handler.do(_request_bytes(body, {}))

    assert resp.status_code == 200
    assert len(seen) == 1
    assert "security audit" not in caplog.text


def test_signed_encrypted_card_is_verified_before_dispatch():
    seen = []
    body = _encrypted_body(
        {
            "type": "card.action.trigger",
            "action": {"value": {"key": "value"}},
        },
        "encrypt-key",
    )
    handler = (
        CardActionHandler.builder("encrypt-key", "verification-token")
        .register(lambda card: seen.append(card))
        .build()
    )

    resp = handler.do(
        _request_bytes(
            body,
            _signed_headers(body, "verification-token", algorithm="sha1"),
        )
    )

    assert resp.status_code == 200
    assert resp.content == b'{"msg":"success"}'
    assert len(seen) == 1


def test_strict_card_invalid_signature_rejects_before_decrypt(monkeypatch):
    recorder = InMemorySecurityAuditRecorder()
    body = _encrypted_body({"type": "card.action.trigger"}, "encrypt-key")

    def fail_decrypt(*_args, **_kwargs):
        raise AssertionError("decrypt should not run")

    monkeypatch.setattr(
        "lark_channel.card.action_handler.AESCipher.decrypt_str",
        fail_decrypt,
    )
    handler = CardActionHandler.builder(
        "encrypt-key",
        "verification-token",
        security=SecurityConfig(mode="strict", audit_recorder=recorder),
    ).build()

    resp = handler.do(
        _request_bytes(
            body,
            _signed_headers(body, "wrong-token", algorithm="sha1"),
        )
    )

    assert resp.status_code == 500
    assert json.loads(resp.content) == {"code": 500, "msg": "internal error"}
    assert [event.reason for event in recorder.events] == [
        REASON_CARD_SIGNATURE_INVALID
    ]


def test_strict_card_missing_signature_non_handshake_rejects():
    """A non-handshake card callback must stay blocked even though the
    SDK now peeks at decrypted content to check for a url_verification
    exemption.

    Registers a processor and asserts it is never invoked, so an
    implementation that merely logs the missing-signature reason while
    still letting the request through would fail this test, not just
    one that returns the wrong status code."""
    seen = []
    recorder = InMemorySecurityAuditRecorder()
    body = _encrypted_body(
        {
            "type": "card.action.trigger",
            "action": {"value": {"key": "value"}},
        },
        "encrypt-key",
    )
    handler = (
        CardActionHandler.builder(
            "encrypt-key",
            "verification-token",
            security=SecurityConfig(mode="strict", audit_recorder=recorder),
        )
        .register(lambda card: seen.append(card))
        .build()
    )

    resp = handler.do(_request_bytes(body, {}))

    assert resp.status_code == 500
    assert json.loads(resp.content) == {"code": 500, "msg": "internal error"}
    assert len(seen) == 0
    assert [event.reason for event in recorder.events] == [
        REASON_CARD_SIGNATURE_MISSING
    ]


def test_strict_card_missing_signature_undecryptable_body_rejects(monkeypatch):
    """If the url_verification-exemption peek itself can't decrypt the
    body, the request must still be rejected, not silently let through."""
    recorder = InMemorySecurityAuditRecorder()
    body = _encrypted_body({"type": "card.action.trigger"}, "encrypt-key")

    def fail_decrypt(*_args, **_kwargs):
        raise AssertionError("decrypt failed")

    monkeypatch.setattr(
        "lark_channel.card.action_handler.AESCipher.decrypt_str",
        fail_decrypt,
    )
    handler = CardActionHandler.builder(
        "encrypt-key",
        "verification-token",
        security=SecurityConfig(mode="strict", audit_recorder=recorder),
    ).build()

    resp = handler.do(_request_bytes(body, {}))

    assert resp.status_code == 500
    assert json.loads(resp.content) == {"code": 500, "msg": "internal error"}
    assert [event.reason for event in recorder.events] == [
        REASON_CARD_SIGNATURE_MISSING
    ]


def test_strict_card_unsigned_encrypted_url_verification_handshake_is_exempted():
    """Regression test for the first-time webhook setup deadlock on the
    card callback URL: the challenge is encrypted but never signed, so
    strict mode must not block it before the url_verification branch
    gets a chance to answer it."""
    body = _encrypted_body(
        {
            "type": "url_verification",
            "challenge": "challenge-code",
            "token": "verification-token",
        },
        "encrypt-key",
    )
    handler = CardActionHandler.builder(
        "encrypt-key",
        "verification-token",
        security=SecurityConfig(mode="strict"),
    ).build()

    resp = handler.do(_request_bytes(body, {}))

    assert resp.status_code == 200
    assert json.loads(resp.content) == {"challenge": "challenge-code"}


def test_strict_card_unsigned_encrypted_allow_records_allow_action():
    seen = []
    recorder = InMemorySecurityAuditRecorder()
    body = _encrypted_body(
        {
            "type": "card.action.trigger",
            "action": {"value": {"key": "value"}},
        },
        "encrypt-key",
    )
    handler = (
        CardActionHandler.builder(
            "encrypt-key",
            "verification-token",
            security=SecurityConfig(
                mode="strict",
                allow_unsigned_encrypted_webhook=True,
                audit_recorder=recorder,
            ),
        )
        .register(lambda card: seen.append(card))
        .build()
    )

    resp = handler.do(_request_bytes(body, {}))

    assert resp.status_code == 200
    assert len(seen) == 1
    assert [event.reason for event in recorder.events] == [
        REASON_CARD_SIGNATURE_MISSING
    ]
    assert recorder.events[0].action == "allow"


def test_strict_card_empty_verification_token_rejects_encrypted_before_decrypt(monkeypatch):
    recorder = InMemorySecurityAuditRecorder()
    body = _encrypted_body({"type": "card.action.trigger"}, "encrypt-key")

    def fail_decrypt(*_args, **_kwargs):
        raise AssertionError("decrypt should not run")

    monkeypatch.setattr(
        "lark_channel.card.action_handler.AESCipher.decrypt_str",
        fail_decrypt,
    )
    handler = CardActionHandler.builder(
        "encrypt-key",
        "",
        security=SecurityConfig(mode="strict", audit_recorder=recorder),
    ).build()

    resp = handler.do(_request_bytes(body, _signature_headers()))

    assert resp.status_code == 500
    assert json.loads(resp.content) == {"code": 500, "msg": "internal error"}
    assert [event.reason for event in recorder.events] == [
        REASON_CARD_SIGNATURE_MISSING
    ]


@pytest.mark.parametrize("mode", ["compat", "audit"])
def test_unsigned_encrypted_card_legacy_flow_is_not_blocked(mode):
    seen = []
    recorder = InMemorySecurityAuditRecorder()
    body = _encrypted_body(
        {
            "type": "card.action.trigger",
            "action": {"value": {"key": "value"}},
        },
        "encrypt-key",
    )
    handler = (
        CardActionHandler.builder(
            "encrypt-key",
            "verification-token",
            security=SecurityConfig(mode=mode, audit_recorder=recorder),
        )
        .register(lambda card: seen.append(card))
        .build()
    )

    resp = handler.do(_request_bytes(body, {}))

    assert resp.status_code == 200
    assert resp.content == b'{"msg":"success"}'
    assert len(seen) == 1
    assert [event.reason for event in recorder.events] == [
        REASON_CARD_SIGNATURE_MISSING
    ]


def test_event_error_response_preserves_compat_bytes():
    message = "ordinary error"

    def raise_error(_event):
        raise RuntimeError(message)

    handler = (
        EventDispatcherHandler.builder("", "verification-token")
        .register_p2_customized_event("example.event", raise_error)
        .build()
    )

    resp = handler.do(
        _request(
            {
                "schema": "2.0",
                "header": {
                    "event_type": "example.event",
                    "token": "verification-token",
                },
                "event": {"value": "ok"},
            }
        )
    )

    assert resp.status_code == 500
    assert resp.content == b'{"msg":"ordinary error"}'


def test_event_error_response_preserves_audit_bytes():
    message = "ordinary error"

    def raise_error(_event):
        raise RuntimeError(message)

    handler = (
        EventDispatcherHandler.builder(
            "",
            "verification-token",
            security=SecurityConfig(mode="audit"),
        )
        .register_p2_customized_event("example.event", raise_error)
        .build()
    )

    resp = handler.do(
        _request(
            {
                "schema": "2.0",
                "header": {
                    "event_type": "example.event",
                    "token": "verification-token",
                },
                "event": {"value": "ok"},
            }
        )
    )

    assert resp.status_code == 500
    assert resp.content == b'{"msg":"ordinary error"}'


def test_event_strict_error_response_hides_exception_detail():
    message = "internal secret detail"

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

    resp = handler.do(
        _request(
            {
                "schema": "2.0",
                "header": {
                    "event_type": "example.event",
                    "token": "verification-token",
                },
                "event": {"value": "ok"},
            }
        )
    )

    assert resp.status_code == 500
    assert json.loads(resp.content) == {"code": 500, "msg": "internal error"}
    assert message.encode("utf-8") not in resp.content


@pytest.mark.asyncio
async def test_feishu_channel_strict_error_response_hides_exception_detail():
    ch = FeishuChannel(
        app_id="cli_x",
        app_secret="x",
        verification_token="vtok",
        transport="webhook",
        security=SecurityConfig(mode="strict"),
    )
    ch.start()
    try:
        body = json.dumps(
            {
                "schema": "2.0",
                "header": {
                    "event_type": "unknown.event",
                    "token": "vtok",
                },
                "event": {"value": "ok"},
            }
        ).encode("utf-8")

        status, response_body = await ch.handle_webhook_request(headers={}, body=body)

        assert status == 500
        assert json.loads(response_body) == {"code": 500, "msg": "internal error"}
        assert b"unknown.event" not in response_body
    finally:
        await ch.stop_background()
        ch.stop()


def test_card_error_response_preserves_compat_bytes():
    message = "ordinary error"

    def raise_error(_card):
        raise RuntimeError(message)

    handler = CardActionHandler.builder("", "").register(raise_error).build()

    resp = handler.do(
        _request(
            {
                "type": "card.action.trigger",
                "action": {"value": {"key": "value"}},
            }
        )
    )

    assert resp.status_code == 500
    assert resp.content == b'{"msg":"ordinary error"}'


def test_card_error_response_preserves_audit_bytes():
    message = "ordinary error"

    def raise_error(_card):
        raise RuntimeError(message)

    handler = (
        CardActionHandler.builder("", "", security=SecurityConfig(mode="audit"))
        .register(raise_error)
        .build()
    )

    resp = handler.do(
        _request(
            {
                "type": "card.action.trigger",
                "action": {"value": {"key": "value"}},
            }
        )
    )

    assert resp.status_code == 500
    assert resp.content == b'{"msg":"ordinary error"}'


def test_card_strict_error_response_hides_exception_detail():
    message = "internal secret detail"

    def raise_error(_card):
        raise RuntimeError(message)

    handler = (
        CardActionHandler.builder("", "", security=SecurityConfig(mode="strict"))
        .register(raise_error)
        .build()
    )

    resp = handler.do(
        _request(
            {
                "type": "card.action.trigger",
                "action": {"value": {"key": "value"}},
            }
        )
    )

    assert resp.status_code == 500
    assert json.loads(resp.content) == {"code": 500, "msg": "internal error"}
    assert message.encode("utf-8") not in resp.content
