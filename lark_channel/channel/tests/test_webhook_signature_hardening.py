"""Webhook signature hardening (issue #11).

Regression tests: a request carrying signature headers must never be accepted
silently when verification is impossible (no secret configured), and the
opt-in timestamp-freshness and replay-protection checks must reject stale or
replayed requests in strict mode while keeping the legacy accepting behaviour
(plus audit records and warnings) in compat/audit mode.
"""

import hashlib
import json
import time

import pytest

from lark_channel.card.action_handler import CardActionHandler
from lark_channel.channel.config import SecurityConfig
from lark_channel.core.const import (
    LARK_REQUEST_NONCE,
    LARK_REQUEST_SIGNATURE,
    LARK_REQUEST_TIMESTAMP,
)
from lark_channel.core.model import RawRequest
from lark_channel.core.webhook_signature import (
    REASON_WEBHOOK_REPLAY_DETECTED,
    REASON_WEBHOOK_SIGNATURE_UNVERIFIABLE,
    REASON_WEBHOOK_TIMESTAMP_STALE,
)
from lark_channel.event.dispatcher_handler import EventDispatcherHandler
from lark_channel.event.security import InMemorySecurityAuditRecorder


def _request(body, headers=None):
    req = RawRequest()
    req.uri = "https://example.com/open-apis/bot/v2/hook"
    req.headers = headers or {}
    req.body = body if isinstance(body, bytes) else json.dumps(body).encode("utf-8")
    return req


def _signed_headers(body, secret, *, algorithm="sha256", timestamp=None, nonce=None):
    timestamp = timestamp or str(int(time.time()))
    nonce = nonce or "nonce-1"
    data = (timestamp + nonce + secret).encode("utf-8") + body
    digest = (
        hashlib.sha256(data).hexdigest()
        if algorithm == "sha256"
        else hashlib.sha1(data).hexdigest()
    )
    return {
        LARK_REQUEST_SIGNATURE: digest,
        LARK_REQUEST_TIMESTAMP: timestamp,
        LARK_REQUEST_NONCE: nonce,
    }


def _plain_event():
    return {
        "schema": "2.0",
        "header": {"event_type": "example.event", "token": "verification-token"},
        "event": {"value": "ok"},
    }


def _plain_card():
    return {"type": "card.action.trigger", "action": {"value": {"k": "v"}}}


def _reasons(recorder):
    return [e.reason for e in recorder.events]


# ---------------------------------------------------------------------------
# No secret configured -> must not silently no-op
# ---------------------------------------------------------------------------


def test_compat_unverifiable_signature_is_audited_not_blocked():
    seen = []
    recorder = InMemorySecurityAuditRecorder()
    handler = (
        EventDispatcherHandler.builder(
            "",
            "verification-token",
            security=SecurityConfig(audit_recorder=recorder),
        )
        .register_p2_customized_event("example.event", lambda event: seen.append(event))
        .build()
    )

    resp = handler.do(_request(_plain_event(), _signed_headers(b"", "some-key")))

    assert resp.status_code == 200
    assert len(seen) == 1
    assert REASON_WEBHOOK_SIGNATURE_UNVERIFIABLE in _reasons(recorder)


def test_strict_unverifiable_signature_rejects():
    recorder = InMemorySecurityAuditRecorder()
    handler = (
        EventDispatcherHandler.builder(
            "",
            "verification-token",
            security=SecurityConfig(mode="strict", audit_recorder=recorder),
        )
        .register_p2_customized_event("example.event", lambda event: None)
        .build()
    )

    resp = handler.do(_request(_plain_event(), _signed_headers(b"", "some-key")))

    assert resp.status_code == 500
    assert REASON_WEBHOOK_SIGNATURE_UNVERIFIABLE in _reasons(recorder)


def test_compat_card_unverifiable_signature_is_audited_not_blocked():
    seen = []
    recorder = InMemorySecurityAuditRecorder()
    handler = (
        CardActionHandler.builder(
            "",
            "",
            security=SecurityConfig(audit_recorder=recorder),
        )
        .register(lambda card: seen.append(card))
        .build()
    )

    resp = handler.do(_request(_plain_card(), _signed_headers(b"", "some-key", algorithm="sha1")))

    assert resp.status_code == 200
    assert len(seen) == 1
    assert REASON_WEBHOOK_SIGNATURE_UNVERIFIABLE in _reasons(recorder)


# ---------------------------------------------------------------------------
# Timestamp freshness (opt-in)
# ---------------------------------------------------------------------------


def test_strict_stale_timestamp_rejects():
    recorder = InMemorySecurityAuditRecorder()
    body = json.dumps(_plain_event()).encode("utf-8")
    headers = _signed_headers(body, "encrypt-key", timestamp="1500000000")
    handler = (
        EventDispatcherHandler.builder(
            "encrypt-key",
            "verification-token",
            security=SecurityConfig(
                mode="strict",
                audit_recorder=recorder,
                max_timestamp_skew_seconds=60,
            ),
        )
        .register_p2_customized_event("example.event", lambda event: None)
        .build()
    )

    resp = handler.do(_request(body, headers))

    assert resp.status_code == 500
    assert REASON_WEBHOOK_TIMESTAMP_STALE in _reasons(recorder)


def test_compat_stale_timestamp_is_audited_not_blocked():
    seen = []
    recorder = InMemorySecurityAuditRecorder()
    body = json.dumps(_plain_event()).encode("utf-8")
    headers = _signed_headers(body, "encrypt-key", timestamp="1500000000")
    handler = (
        EventDispatcherHandler.builder(
            "encrypt-key",
            "verification-token",
            security=SecurityConfig(
                audit_recorder=recorder,
                max_timestamp_skew_seconds=60,
            ),
        )
        .register_p2_customized_event("example.event", lambda event: seen.append(event))
        .build()
    )

    resp = handler.do(_request(body, headers))

    assert resp.status_code == 200
    assert len(seen) == 1
    assert REASON_WEBHOOK_TIMESTAMP_STALE in _reasons(recorder)


def test_fresh_timestamp_passes_with_skew_enabled():
    seen = []
    recorder = InMemorySecurityAuditRecorder()
    body = json.dumps(_plain_event()).encode("utf-8")
    headers = _signed_headers(body, "encrypt-key")
    handler = (
        EventDispatcherHandler.builder(
            "encrypt-key",
            "verification-token",
            security=SecurityConfig(
                mode="strict",
                audit_recorder=recorder,
                max_timestamp_skew_seconds=60,
            ),
        )
        .register_p2_customized_event("example.event", lambda event: seen.append(event))
        .build()
    )

    resp = handler.do(_request(body, headers))

    assert resp.status_code == 200
    assert len(seen) == 1
    assert REASON_WEBHOOK_TIMESTAMP_STALE not in _reasons(recorder)


# ---------------------------------------------------------------------------
# Replay protection (opt-in)
# ---------------------------------------------------------------------------


def test_strict_replayed_request_rejects():
    recorder = InMemorySecurityAuditRecorder()
    body = json.dumps(_plain_event()).encode("utf-8")
    headers = _signed_headers(body, "encrypt-key")
    handler = (
        EventDispatcherHandler.builder(
            "encrypt-key",
            "verification-token",
            security=SecurityConfig(
                mode="strict",
                audit_recorder=recorder,
                replay_protection_seconds=60,
            ),
        )
        .register_p2_customized_event("example.event", lambda event: None)
        .build()
    )

    first = handler.do(_request(body, headers))
    assert first.status_code == 200

    replay = handler.do(_request(body, headers))
    assert replay.status_code == 500
    assert REASON_WEBHOOK_REPLAY_DETECTED in _reasons(recorder)


def test_strict_replayed_card_rejects():
    recorder = InMemorySecurityAuditRecorder()
    body = json.dumps(_plain_card()).encode("utf-8")
    headers = _signed_headers(body, "verification-token", algorithm="sha1")
    handler = (
        CardActionHandler.builder(
            "",
            "verification-token",
            security=SecurityConfig(
                mode="strict",
                audit_recorder=recorder,
                replay_protection_seconds=60,
            ),
        )
        .register(lambda card: None)
        .build()
    )

    first = handler.do(_request(body, headers))
    assert first.status_code == 200

    replay = handler.do(_request(body, headers))
    assert replay.status_code == 500
    assert REASON_WEBHOOK_REPLAY_DETECTED in _reasons(recorder)


# ---------------------------------------------------------------------------
# Configuration validation
# ---------------------------------------------------------------------------


def test_security_config_validates_new_fields():
    with pytest.raises(ValueError):
        SecurityConfig(max_timestamp_skew_seconds=0)
    with pytest.raises(ValueError):
        SecurityConfig(replay_protection_seconds=-1)
    with pytest.raises(TypeError):
        SecurityConfig(max_timestamp_skew_seconds=True)  # bool is not an int
    assert SecurityConfig(max_timestamp_skew_seconds=300).max_timestamp_skew_seconds == 300
    assert SecurityConfig(replay_protection_seconds=60).replay_protection_seconds == 60
