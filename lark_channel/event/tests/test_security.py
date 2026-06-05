import logging

import pytest

from lark_channel.channel.config import SecurityConfig
from lark_channel.event.security import (
    REASON_CARD_DECRYPT_FAILED,
    REASON_CARD_SIGNATURE_INVALID,
    REASON_CARD_SIGNATURE_MISSING,
    REASON_CARD_TOKEN_INVALID,
    REASON_CARD_TOKEN_MISSING,
    REASON_CONTENT_TEXT_UNSAFE_LEGACY,
    REASON_MENTIONS_TEXT_ONLY,
    REASON_RESOURCE_LIMIT_WOULD_BLOCK,
    REASON_TOKEN_CACHE_LEGACY_FALLBACK,
    REASON_WEBHOOK_DECRYPT_FAILED,
    REASON_WEBHOOK_DECRYPT_WITHOUT_VERIFIED_SIGNATURE,
    REASON_WEBHOOK_SIGNATURE_INVALID,
    REASON_WEBHOOK_SIGNATURE_MISSING,
    REASON_WEBHOOK_TOKEN_INVALID,
    REASON_WEBHOOK_TOKEN_MISSING,
    REASON_WS_FRAGMENT_LIMIT,
    REASON_WS_INSECURE_SCHEME,
    REASON_WS_INVALID_TIMING,
    InMemorySecurityAuditRecorder,
    SecurityAuditEvent,
    SecurityAuditRecorder,
    should_record_security_audit,
)


def _fake_private_key_block() -> str:
    begin = "-----BEGIN " + "PRIVATE " + "KEY-----"
    end = "-----END " + "PRIVATE " + "KEY-----"
    return "\n".join(
        [
            begin,
            "private-key-line-one",
            "private-key-line-two",
            end,
        ]
    )


def test_security_audit_recorder_emits_stable_reason_code(caplog):
    recorder = SecurityAuditRecorder()

    with caplog.at_level(logging.WARNING, logger="Lark"):
        recorder.record(
            REASON_WEBHOOK_SIGNATURE_MISSING,
            mode="audit",
            action="would_block",
            details={"path": "/callback"},
        )

    assert REASON_WEBHOOK_SIGNATURE_MISSING in caplog.text
    assert "mode=audit" in caplog.text
    assert "action=would_block" in caplog.text


def test_default_compat_security_does_not_record_audit_by_default():
    assert should_record_security_audit(SecurityConfig()) is False


def test_audit_and_strict_security_record_audit_by_default():
    assert should_record_security_audit(SecurityConfig(mode="audit")) is True
    assert should_record_security_audit(SecurityConfig(mode="strict")) is True


def test_compat_security_records_when_recorder_is_explicit():
    recorder = InMemorySecurityAuditRecorder()

    assert (
        should_record_security_audit(
            SecurityConfig(mode="compat", audit_recorder=recorder)
        )
        is True
    )


def test_compat_security_records_when_base_recorder_is_explicit():
    recorder = SecurityAuditRecorder()

    assert (
        should_record_security_audit(
            SecurityConfig(mode="compat", audit_recorder=recorder)
        )
        is True
    )


def test_security_reason_codes_cover_webhook_and_card_auth_failures():
    assert REASON_WEBHOOK_TOKEN_MISSING == "webhook.token_missing"
    assert REASON_WEBHOOK_TOKEN_INVALID == "webhook.token_invalid"
    assert REASON_WEBHOOK_SIGNATURE_MISSING == "webhook.signature_missing"
    assert REASON_WEBHOOK_SIGNATURE_INVALID == "webhook.signature_invalid"
    assert (
        REASON_WEBHOOK_DECRYPT_WITHOUT_VERIFIED_SIGNATURE
        == "webhook.decrypt_without_verified_signature"
    )
    assert REASON_WEBHOOK_DECRYPT_FAILED == "webhook.decrypt_failed"
    assert REASON_CARD_TOKEN_MISSING == "card.token_missing"
    assert REASON_CARD_TOKEN_INVALID == "card.token_invalid"
    assert REASON_CARD_SIGNATURE_MISSING == "card.signature_missing"
    assert REASON_CARD_SIGNATURE_INVALID == "card.signature_invalid"
    assert REASON_CARD_DECRYPT_FAILED == "card.decrypt_failed"


def test_security_reason_codes_cover_transport_and_normalization_contracts():
    assert REASON_WS_INSECURE_SCHEME == "ws.insecure_scheme"
    assert REASON_WS_INVALID_TIMING == "ws.invalid_timing"
    assert REASON_WS_FRAGMENT_LIMIT == "ws.fragment_limit"
    assert REASON_RESOURCE_LIMIT_WOULD_BLOCK == "resource.limit_would_block"
    assert REASON_MENTIONS_TEXT_ONLY == "mentions.text_only"
    assert REASON_CONTENT_TEXT_UNSAFE_LEGACY == "content_text.unsafe_legacy"
    assert REASON_TOKEN_CACHE_LEGACY_FALLBACK == "token_cache.legacy_fallback"


def test_security_audit_recorder_redacts_sensitive_details(caplog):
    recorder = SecurityAuditRecorder()

    with caplog.at_level(logging.WARNING, logger="Lark"):
        recorder.record(
            REASON_WEBHOOK_SIGNATURE_MISSING,
            mode="audit",
            action="would_block",
            details={
                "authorization": "Bearer tenant-token-value",
                "verification_token": "verification-token-value",
                "body": {"app_secret": "secret-value", "safe": "visible"},
            },
        )

    assert "tenant-token-value" not in caplog.text
    assert "verification-token-value" not in caplog.text
    assert "secret-value" not in caplog.text
    assert "Bearer ***" in caplog.text
    assert "visible" in caplog.text


def test_security_audit_recorder_redacts_sensitive_free_text(caplog):
    recorder = SecurityAuditRecorder()

    with caplog.at_level(logging.WARNING, logger="Lark"):
        recorder.record(
            REASON_WEBHOOK_SIGNATURE_MISSING,
            mode="audit",
            action="would_block",
            details={
                "message": "Authorization: Bearer tenant-token-value",
                "error": "verification_token=verification-token-value",
                "raw": '{"app_secret":"secret-value","safe":"visible"}',
            },
        )

    assert "tenant-token-value" not in caplog.text
    assert "verification-token-value" not in caplog.text
    assert "secret-value" not in caplog.text
    assert "Bearer ***" in caplog.text
    assert "visible" in caplog.text


def test_security_audit_recorder_redacts_quoted_sensitive_free_text(caplog):
    recorder = SecurityAuditRecorder()

    with caplog.at_level(logging.WARNING, logger="Lark"):
        recorder.record(
            REASON_WEBHOOK_SIGNATURE_MISSING,
            mode="audit",
            action="would_block",
            details={
                "message": '"verification_token": "verification-token-value"',
                "error": "'app_secret': 'secret-value'",
                "note": 'client_secret = "client-secret-value"',
            },
        )

    assert "verification-token-value" not in caplog.text
    assert "secret-value" not in caplog.text
    assert "client-secret-value" not in caplog.text
    assert "verification_token" in caplog.text
    assert "app_secret" in caplog.text
    assert "client_secret" in caplog.text


def test_security_audit_recorder_redacts_quoted_sensitive_free_text_with_spaces(caplog):
    recorder = SecurityAuditRecorder()

    with caplog.at_level(logging.WARNING, logger="Lark"):
        recorder.record(
            REASON_WEBHOOK_SIGNATURE_MISSING,
            mode="audit",
            action="would_block",
            details={
                "message": '"app_secret": "secret value with spaces"',
                "error": "'client_secret': 'client secret value'",
            },
    )

    assert "secret value with spaces" not in caplog.text
    assert "value with spaces" not in caplog.text
    assert "client secret value" not in caplog.text
    assert "secret value" not in caplog.text
    assert "app_secret" in caplog.text
    assert "client_secret" in caplog.text


def test_security_audit_recorder_redacts_common_auth_free_text(caplog):
    recorder = SecurityAuditRecorder()

    with caplog.at_level(logging.WARNING, logger="Lark"):
        recorder.record(
            REASON_WEBHOOK_SIGNATURE_MISSING,
            mode="audit",
            action="would_block",
            details={
                "basic": "Authorization: Basic basic-token-value",
                "auth_text": "Authorization: Token token-auth-value",
                "api_key_text": "X-Api-Key: api-key-value",
                "secret": "client_secret = client secret value",
            },
        )

    assert "basic-token-value" not in caplog.text
    assert "token-auth-value" not in caplog.text
    assert "api-key-value" not in caplog.text
    assert "client secret value" not in caplog.text
    assert "Authorization: Basic ***" in caplog.text
    assert "Authorization: Token ***" in caplog.text
    assert "X-Api-Key: ***" in caplog.text


def test_security_audit_recorder_redacts_api_key_fields_and_free_text(caplog):
    recorder = SecurityAuditRecorder()

    with caplog.at_level(logging.WARNING, logger="Lark"):
        recorder.record(
            REASON_WEBHOOK_SIGNATURE_MISSING,
            mode="audit",
            action="would_block",
            details={
                "api_key": "api-key-value",
                "x_api_key": "x-api-key-value",
                "message": "api_key=free-text-api-key",
            },
        )

    assert "api-key-value" not in caplog.text
    assert "x-api-key-value" not in caplog.text
    assert "free-text-api-key" not in caplog.text


def test_security_audit_recorder_redacts_api_key_hyphen_variant(caplog):
    recorder = SecurityAuditRecorder()

    with caplog.at_level(logging.WARNING, logger="Lark"):
        recorder.record(
            REASON_WEBHOOK_SIGNATURE_MISSING,
            mode="audit",
            action="would_block",
            details={"message": "api-key=hyphen-api-key-value"},
        )

    assert "hyphen-api-key-value" not in caplog.text


def test_security_audit_recorder_redacts_cookie_and_private_key(caplog):
    recorder = SecurityAuditRecorder()

    with caplog.at_level(logging.WARNING, logger="Lark"):
        recorder.record(
            REASON_WEBHOOK_SIGNATURE_MISSING,
            mode="audit",
            action="would_block",
            details={
                "headers": {
                    "Cookie": "sessionid=session-secret-value",
                    "Set-Cookie": "sessionid=set-cookie-secret",
                },
                "private_key": "private-key-value",
                "message": "Cookie: sessionid=free-text-cookie-secret",
            },
        )

    assert "session-secret-value" not in caplog.text
    assert "set-cookie-secret" not in caplog.text
    assert "private-key-value" not in caplog.text
    assert "free-text-cookie-secret" not in caplog.text


def test_security_audit_recorder_redacts_encrypt_key_fields_and_free_text(caplog):
    recorder = SecurityAuditRecorder()

    with caplog.at_level(logging.WARNING, logger="Lark"):
        recorder.record(
            REASON_WEBHOOK_SIGNATURE_MISSING,
            mode="audit",
            action="would_block",
            details={
                "encrypt_key": "field-encrypt-key",
                "nested": {"encrypt_key": "nested-encrypt-key"},
                "message": "encrypt_key=free-text-encrypt-key",
            },
        )

    assert "field-encrypt-key" not in caplog.text
    assert "nested-encrypt-key" not in caplog.text
    assert "free-text-encrypt-key" not in caplog.text


def test_security_audit_recorder_redacts_sensitive_detail_keys(caplog):
    recorder = SecurityAuditRecorder()

    with caplog.at_level(logging.WARNING, logger="Lark"):
        recorder.record(
            REASON_WEBHOOK_SIGNATURE_MISSING,
            mode="audit",
            action="would_block",
            details={
                "Authorization: Bearer key-token-value": "visible",
                "access_token=key-access-token": "visible",
            },
    )

    assert "key-token-value" not in caplog.text
    assert "key-access-token" not in caplog.text


def test_security_audit_recorder_truncates_long_detail_keys(caplog):
    recorder = SecurityAuditRecorder(max_detail_chars=8)
    long_key = "k" * 64

    with caplog.at_level(logging.WARNING, logger="Lark"):
        recorder.record(
            REASON_WEBHOOK_SIGNATURE_MISSING,
            mode="audit",
            action="would_block",
            details={long_key: "visible"},
        )

    assert long_key not in caplog.text
    assert "<truncated" in caplog.text


def test_security_audit_recorder_redacts_auth_free_text_with_spaces(caplog):
    recorder = SecurityAuditRecorder()

    with caplog.at_level(logging.WARNING, logger="Lark"):
        recorder.record(
            REASON_WEBHOOK_SIGNATURE_MISSING,
            mode="audit",
            action="would_block",
            details={
                "basic": "Authorization: Basic basic token with spaces",
                "token_text": "Authorization: Token token value with spaces",
                "api_key_text": "X-Api-Key: api key value with spaces",
            },
        )

    assert "basic token with spaces" not in caplog.text
    assert "token value with spaces" not in caplog.text
    assert "api key value with spaces" not in caplog.text


def test_security_audit_recorder_redacts_auth_header_with_delimited_values(caplog):
    recorder = SecurityAuditRecorder()

    with caplog.at_level(logging.WARNING, logger="Lark"):
        recorder.record(
            REASON_WEBHOOK_SIGNATURE_MISSING,
            mode="audit",
            action="would_block",
            details={
                "cookie_text": "Cookie: sessionid=secret-one; csrf=secret-two",
                "token_text": "Authorization: Token token-value;next=leak-value",
                "basic": "Authorization: Basic basic-value,second-value",
            },
        )

    assert "secret-one" not in caplog.text
    assert "secret-two" not in caplog.text
    assert "token-value" not in caplog.text
    assert "leak-value" not in caplog.text
    assert "basic-value" not in caplog.text
    assert "second-value" not in caplog.text


def test_security_audit_recorder_redacts_extended_auth_and_secret_forms(caplog):
    recorder = SecurityAuditRecorder()

    private_key = _fake_private_key_block()

    with caplog.at_level(logging.WARNING, logger="Lark"):
        recorder.record(
            REASON_WEBHOOK_SIGNATURE_MISSING,
            mode="audit",
            action="would_block",
            details={
                "query": "accessToken=query-token-value&clientSecret=client-secret-value",
                "oauth": "Authorization: OAuth oauth-token-value",
                "header_block": "Authorization: Bearer first-line-token\n second-line-token",
                "privateKey": private_key,
                "message": private_key,
            },
        )

    assert "query-token-value" not in caplog.text
    assert "client-secret-value" not in caplog.text
    assert "oauth-token-value" not in caplog.text
    assert "first-line-token" not in caplog.text
    assert "second-line-token" not in caplog.text
    assert "private-key-line-one" not in caplog.text
    assert "private-key-line-two" not in caplog.text


def test_security_audit_recorder_redacts_nonstandard_authorization_headers(caplog):
    recorder = SecurityAuditRecorder()

    with caplog.at_level(logging.WARNING, logger="Lark"):
        recorder.record(
            REASON_WEBHOOK_SIGNATURE_MISSING,
            mode="audit",
            action="would_block",
            details={
                "oauth_equals": "Authorization: OAuth=oauth-token-value",
                "basic_colon": "Authorization: Basic: basic-token-value",
                "custom": "Authorization: CustomScheme custom-token-value",
            },
        )

    assert "oauth-token-value" not in caplog.text
    assert "basic-token-value" not in caplog.text
    assert "custom-token-value" not in caplog.text


def test_security_audit_recorder_redacts_authorization_field_values(caplog):
    recorder = SecurityAuditRecorder()

    with caplog.at_level(logging.WARNING, logger="Lark"):
        recorder.record(
            REASON_WEBHOOK_SIGNATURE_MISSING,
            mode="audit",
            action="would_block",
            details={
                "Authorization": "Basic basic-token-value",
                "nested": {"authorization": "Token token-value"},
                "json": '{"Authorization": "OAuth oauth-token-value"}',
                "custom": {"authorization": "CustomScheme custom-token-value"},
            },
        )

    assert "basic-token-value" not in caplog.text
    assert "token-value" not in caplog.text
    assert "oauth-token-value" not in caplog.text
    assert "custom-token-value" not in caplog.text
    assert '"Authorization": "Basic ***"' in caplog.text
    assert '"authorization": "Token ***"' in caplog.text


def test_security_audit_recorder_redacts_authorization_path_keys(caplog):
    recorder = SecurityAuditRecorder()

    with caplog.at_level(logging.WARNING, logger="Lark"):
        recorder.record(
            REASON_WEBHOOK_SIGNATURE_MISSING,
            mode="audit",
            action="would_block",
            details={
                "headers.authorization": "Basic dotted-token-value",
                "request.headers.Authorization": "Bearer nested-token-value",
                "Authorization Header": "Token spaced-token-value",
            },
        )

    assert "dotted-token-value" not in caplog.text
    assert "nested-token-value" not in caplog.text
    assert "spaced-token-value" not in caplog.text
    assert "Basic ***" in caplog.text
    assert "Bearer ***" in caplog.text
    assert "Token ***" in caplog.text


def test_security_audit_recorder_redacts_folded_x_api_key_header(caplog):
    recorder = SecurityAuditRecorder()

    with caplog.at_level(logging.WARNING, logger="Lark"):
        recorder.record(
            REASON_WEBHOOK_SIGNATURE_MISSING,
            mode="audit",
            action="would_block",
            details={"message": "X-Api-Key: first-line-secret\n second-line-secret"},
        )

    assert "first-line-secret" not in caplog.text
    assert "second-line-secret" not in caplog.text
    assert "X-Api-Key: ***" in caplog.text


def test_security_audit_recorder_truncates_large_detail_strings(caplog):
    recorder = SecurityAuditRecorder(max_detail_chars=16)

    with caplog.at_level(logging.WARNING, logger="Lark"):
        recorder.record(
            REASON_WEBHOOK_SIGNATURE_MISSING,
            mode="audit",
            action="would_block",
            details={"raw": "x" * 64},
        )

    assert "x" * 64 not in caplog.text
    assert "<truncated" in caplog.text


def test_security_audit_recorder_limits_detail_collection_width(caplog):
    recorder = SecurityAuditRecorder(max_detail_items=2)

    with caplog.at_level(logging.WARNING, logger="Lark"):
        recorder.record(
            REASON_WEBHOOK_SIGNATURE_MISSING,
            mode="audit",
            action="would_block",
            details={
                "first": "visible-1",
                "second": "visible-2",
                "third": "hidden-3",
            },
        )

    assert "visible-1" in caplog.text
    assert "visible-2" in caplog.text
    assert "hidden-3" not in caplog.text
    assert "<truncated" in caplog.text


def test_security_audit_recorder_redacts_escaped_quote_secret_suffix(caplog):
    recorder = SecurityAuditRecorder()

    with caplog.at_level(logging.WARNING, logger="Lark"):
        recorder.record(
            REASON_WEBHOOK_SIGNATURE_MISSING,
            mode="audit",
            action="would_block",
            details={"message": r'app_secret="abc\"def"'},
        )

    assert "abc" not in caplog.text
    assert "def" not in caplog.text
    assert "app_secret" in caplog.text


def test_security_audit_recorder_redacts_quoted_secret_with_delimiters(caplog):
    recorder = SecurityAuditRecorder()

    with caplog.at_level(logging.WARNING, logger="Lark"):
        recorder.record(
            REASON_WEBHOOK_SIGNATURE_MISSING,
            mode="audit",
            action="would_block",
            details={
                "comma": 'app_secret="abc,def"',
                "semicolon": "client_secret='abc;def'",
                "ampersand": 'encrypt_key="abc&def"',
            },
        )

    assert "abc,def" not in caplog.text
    assert "abc;def" not in caplog.text
    assert "abc&def" not in caplog.text


def test_security_audit_recorder_redacts_multiline_secret_assignment(caplog):
    recorder = SecurityAuditRecorder()

    with caplog.at_level(logging.WARNING, logger="Lark"):
        recorder.record(
            REASON_WEBHOOK_SIGNATURE_MISSING,
            mode="audit",
            action="would_block",
            details={
                "message": 'app_secret="line-one-secret\nline-two-secret"',
                "camel": "clientSecret='camel-one-secret\ncamel-two-secret'",
            },
        )

    assert "line-one-secret" not in caplog.text
    assert "line-two-secret" not in caplog.text
    assert "camel-one-secret" not in caplog.text
    assert "camel-two-secret" not in caplog.text


def test_security_audit_recorder_redacts_unclosed_multiline_secret_assignment(caplog):
    recorder = SecurityAuditRecorder()

    with caplog.at_level(logging.WARNING, logger="Lark"):
        recorder.record(
            REASON_WEBHOOK_SIGNATURE_MISSING,
            mode="audit",
            action="would_block",
            details={
                "message": 'app_secret="line-one-secret\nline-two-secret',
                "camel": "clientSecret='camel-one-secret\ncamel-two-secret",
            },
        )

    assert "line-one-secret" not in caplog.text
    assert "line-two-secret" not in caplog.text
    assert "camel-one-secret" not in caplog.text
    assert "camel-two-secret" not in caplog.text


def test_security_audit_recorder_redacts_unquoted_authorization_assignment(caplog):
    recorder = SecurityAuditRecorder()

    with caplog.at_level(logging.WARNING, logger="Lark"):
        recorder.record(
            REASON_WEBHOOK_SIGNATURE_MISSING,
            mode="audit",
            action="would_block",
            details={
                "basic": "authorization=Basic basic-token-value",
                "token": "authorization = Token token-value",
                "custom": "authorization=CustomScheme custom-token-value",
            },
        )

    assert "basic-token-value" not in caplog.text
    assert "token-value" not in caplog.text
    assert "custom-token-value" not in caplog.text


def test_security_audit_recorder_redacts_pem_before_truncating(caplog):
    recorder = SecurityAuditRecorder(max_detail_chars=64)

    with caplog.at_level(logging.WARNING, logger="Lark"):
        recorder.record(
            REASON_WEBHOOK_SIGNATURE_MISSING,
            mode="audit",
            action="would_block",
            details={
                "message": _fake_private_key_block()
            },
        )

    assert "private-key-line-one" not in caplog.text
    assert "private-key-line-two" not in caplog.text
    assert "<redacted private key>" in caplog.text


def test_security_audit_recorder_does_not_return_unknown_objects(caplog):
    class SecretCarrier:
        def __init__(self):
            self.app_secret = "object-attr-secret-value"

    recorder = SecurityAuditRecorder()

    with caplog.at_level(logging.WARNING, logger="Lark"):
        recorder.record(
            REASON_WEBHOOK_SIGNATURE_MISSING,
            mode="audit",
            action="would_block",
            details={"object": SecretCarrier()},
        )

    assert "object-attr-secret-value" not in caplog.text
    assert "<SecretCarrier>" in caplog.text


def test_security_audit_recorder_handles_unserializable_unknown_objects(caplog):
    class ExplodingValue:
        def __deepcopy__(self, memo):
            raise RuntimeError("deepcopy should not be called")

    class SecretCarrier:
        def __init__(self):
            self.app_secret = "object-attr-secret-value"
            self.exploding = ExplodingValue()

    recorder = SecurityAuditRecorder()

    with caplog.at_level(logging.WARNING, logger="Lark"):
        recorder.record(
            REASON_WEBHOOK_SIGNATURE_MISSING,
            mode="audit",
            action="would_block",
            details={"object": SecretCarrier()},
        )

    assert "object-attr-secret-value" not in caplog.text
    assert "deepcopy should not be called" not in caplog.text
    assert "<SecretCarrier>" in caplog.text


def test_security_audit_recorder_truncates_before_processing_large_strings(caplog):
    recorder = SecurityAuditRecorder(max_detail_chars=32)
    raw = "prefix-" + ("x" * 10000) + "app_secret=tail-secret"

    with caplog.at_level(logging.WARNING, logger="Lark"):
        recorder.record(
            REASON_WEBHOOK_SIGNATURE_MISSING,
            mode="audit",
            action="would_block",
            details={"raw": raw},
        )

    assert "tail-secret" not in caplog.text
    assert "x" * 1000 not in caplog.text
    assert "<truncated" in caplog.text


def test_security_audit_recorder_applies_total_detail_budget(caplog):
    recorder = SecurityAuditRecorder(max_detail_chars=64, max_detail_total_chars=96)

    with caplog.at_level(logging.WARNING, logger="Lark"):
        recorder.record(
            REASON_WEBHOOK_SIGNATURE_MISSING,
            mode="audit",
            action="would_block",
            details={
                "first": "a" * 64,
                "second": "b" * 64,
                "third": "c" * 64,
            },
        )

    assert "c" * 64 not in caplog.text
    assert "<truncated details>" in caplog.text


def test_security_audit_recorder_counts_sensitive_values_in_total_budget(caplog):
    recorder = SecurityAuditRecorder(max_detail_total_chars=10)

    with caplog.at_level(logging.WARNING, logger="Lark"):
        recorder.record(
            REASON_WEBHOOK_SIGNATURE_MISSING,
            mode="audit",
            action="would_block",
            details={"api_key": "api-key-value", "x": "1"},
        )

    assert '"api_key": "***"' in caplog.text
    assert '"x": "1"' not in caplog.text
    assert "<truncated details>" in caplog.text


def test_security_audit_recorder_counts_non_string_scalars_in_total_budget(caplog):
    recorder = SecurityAuditRecorder(max_detail_total_chars=32)

    with caplog.at_level(logging.WARNING, logger="Lark"):
        recorder.record(
            REASON_WEBHOOK_SIGNATURE_MISSING,
            mode="audit",
            action="would_block",
            details={
                "first": 10**80,
                "second": "hidden",
            },
        )

    assert str(10**80) not in caplog.text
    assert "hidden" not in caplog.text
    assert "<truncated details>" in caplog.text


def test_security_audit_recorder_does_not_materialize_items_beyond_limit(caplog):
    class ExplodingMapping:
        def __iter__(self):
            yield "first"
            yield "second"
            raise AssertionError("iterated beyond max detail items")

        def __getitem__(self, key):
            if key == "first":
                return "visible-1"
            if key == "second":
                return "visible-2"
            raise KeyError(key)

        def items(self):
            for key in self:
                yield key, self[key]

    recorder = SecurityAuditRecorder(max_detail_items=2)

    with caplog.at_level(logging.WARNING, logger="Lark"):
        recorder.record(
            REASON_WEBHOOK_SIGNATURE_MISSING,
            mode="audit",
            action="would_block",
            details=ExplodingMapping(),
        )

    assert "visible-1" in caplog.text
    assert "visible-2" in caplog.text


def test_security_audit_recorder_does_not_call_custom_mapping_len(caplog):
    class LenExplodingMapping:
        def __bool__(self):
            raise AssertionError("bool called")

        def __len__(self):
            raise AssertionError("len called")

        def items(self):
            yield "first", "visible-1"
            yield "second", "visible-2"

    recorder = SecurityAuditRecorder(max_detail_items=2)

    with caplog.at_level(logging.WARNING, logger="Lark"):
        recorder.record(
            REASON_WEBHOOK_SIGNATURE_MISSING,
            mode="audit",
            action="would_block",
            details=LenExplodingMapping(),
        )

    assert "visible-1" in caplog.text
    assert "visible-2" in caplog.text


def test_security_audit_recorder_handles_mapping_items_creation_error(caplog):
    class ItemsExplodingMapping:
        def items(self):
            raise RuntimeError("items should not escape")

    recorder = SecurityAuditRecorder()

    with caplog.at_level(logging.WARNING, logger="Lark"):
        recorder.record(
            REASON_WEBHOOK_SIGNATURE_MISSING,
            mode="audit",
            action="would_block",
            details=ItemsExplodingMapping(),
        )

    assert "items should not escape" not in caplog.text
    assert "<truncated details>" in caplog.text


def test_security_audit_recorder_handles_mapping_items_property_error(caplog):
    class ItemsPropertyExplodingMapping:
        @property
        def items(self):
            raise RuntimeError("items property secret should not escape")

    recorder = SecurityAuditRecorder()

    with caplog.at_level(logging.WARNING, logger="Lark"):
        recorder.record(
            REASON_WEBHOOK_SIGNATURE_MISSING,
            mode="audit",
            action="would_block",
            details=ItemsPropertyExplodingMapping(),
        )

    assert "items property secret should not escape" not in caplog.text
    assert "<ItemsPropertyExplodingMapping>" in caplog.text


def test_security_audit_recorder_handles_mapping_iteration_error(caplog):
    class IterationExplodingMapping:
        def items(self):
            yield "first", "visible-1"
            raise RuntimeError("iteration should not escape")

    recorder = SecurityAuditRecorder()

    with caplog.at_level(logging.WARNING, logger="Lark"):
        recorder.record(
            REASON_WEBHOOK_SIGNATURE_MISSING,
            mode="audit",
            action="would_block",
            details=IterationExplodingMapping(),
        )

    assert "visible-1" in caplog.text
    assert "iteration should not escape" not in caplog.text
    assert "<truncated details>" in caplog.text


def test_security_audit_recorder_handles_cyclic_list(caplog):
    cyclic = []
    cyclic.append(cyclic)
    recorder = SecurityAuditRecorder()

    with caplog.at_level(logging.WARNING, logger="Lark"):
        recorder.record(
            REASON_WEBHOOK_SIGNATURE_MISSING,
            mode="audit",
            action="would_block",
            details={"cycle": cyclic},
        )

    assert "<cycle>" in caplog.text


def test_security_audit_recorder_handles_cyclic_mapping(caplog):
    cyclic = {}
    cyclic["self"] = cyclic
    recorder = SecurityAuditRecorder()

    with caplog.at_level(logging.WARNING, logger="Lark"):
        recorder.record(
            REASON_WEBHOOK_SIGNATURE_MISSING,
            mode="audit",
            action="would_block",
            details=cyclic,
        )

    assert "<cycle>" in caplog.text


def test_security_audit_recorder_keeps_scrubbed_json_like_keys_hashable(caplog):
    recorder = SecurityAuditRecorder()

    with caplog.at_level(logging.WARNING, logger="Lark"):
        recorder.record(
            REASON_WEBHOOK_SIGNATURE_MISSING,
            mode="audit",
            action="would_block",
            details={'{"app_secret":"key-secret-value"}': "visible"},
        )

    assert "key-secret-value" not in caplog.text
    assert "visible" in caplog.text


def test_security_audit_recorder_handles_key_string_error(caplog):
    class ExplodingKey:
        def __str__(self):
            raise RuntimeError("key string secret should not escape")

    recorder = SecurityAuditRecorder()

    with caplog.at_level(logging.WARNING, logger="Lark"):
        recorder.record(
            REASON_WEBHOOK_SIGNATURE_MISSING,
            mode="audit",
            action="would_block",
            details={ExplodingKey(): "visible"},
        )

    assert "key string secret should not escape" not in caplog.text
    assert "<ExplodingKey>" in caplog.text
    assert "visible" in caplog.text


def test_security_audit_recorder_scrubs_reason_mode_and_action(caplog):
    recorder = SecurityAuditRecorder(max_detail_chars=16)

    with caplog.at_level(logging.WARNING, logger="Lark"):
        recorder.record(
            "reason app_secret=reason-secret-value",
            mode="mode Authorization: Bearer mode-token-value",
            action="action " + "x" * 64,
            details={"safe": "visible"},
        )

    assert "reason-secret-value" not in caplog.text
    assert "mode-token-value" not in caplog.text
    assert "x" * 64 not in caplog.text
    assert "<truncated" in caplog.text


def test_in_memory_security_audit_recorder_is_injectable():
    recorder = InMemorySecurityAuditRecorder()

    recorder.record(
        REASON_WEBHOOK_SIGNATURE_MISSING,
        mode="strict",
        action="block",
        details={"request_id": "req_1"},
    )

    assert recorder.events == [
        SecurityAuditEvent(
            reason=REASON_WEBHOOK_SIGNATURE_MISSING,
            mode="strict",
            action="block",
            details={"request_id": "req_1"},
        )
    ]


def test_in_memory_security_audit_recorder_redacts_sensitive_details():
    recorder = InMemorySecurityAuditRecorder()

    recorder.record(
        REASON_WEBHOOK_SIGNATURE_MISSING,
        mode="audit",
        action="would_block",
        details={
            "authorization": "Bearer tenant-token-value",
            "verification_token": "verification-token-value",
            "body": {"app_secret": "secret-value", "safe": "visible"},
        },
    )

    event = recorder.events[0]
    assert event.details["authorization"] == "Bearer ***"
    assert event.details["verification_token"] == "***"
    assert event.details["body"]["app_secret"] == "***"
    assert event.details["body"]["safe"] == "visible"


def test_in_memory_security_audit_recorder_keeps_bounded_event_history():
    recorder = InMemorySecurityAuditRecorder(max_events=2)

    for index in range(3):
        recorder.record(
            REASON_WEBHOOK_SIGNATURE_MISSING,
            mode="audit",
            action="would_block",
            details={"request_id": f"req_{index}"},
        )

    assert [event.details["request_id"] for event in recorder.events] == [
        "req_1",
        "req_2",
    ]


def test_in_memory_security_audit_recorder_truncates_large_detail_strings():
    recorder = InMemorySecurityAuditRecorder(max_detail_chars=8)

    recorder.record(
        REASON_WEBHOOK_SIGNATURE_MISSING,
        mode="audit",
        action="would_block",
        details={"raw": "x" * 64},
    )

    assert recorder.events[0].details["raw"].startswith("xxxxxxxx")
    assert "<truncated" in recorder.events[0].details["raw"]


def test_in_memory_security_audit_recorder_limits_detail_collection_width():
    recorder = InMemorySecurityAuditRecorder(max_detail_items=2)

    recorder.record(
        REASON_WEBHOOK_SIGNATURE_MISSING,
        mode="audit",
        action="would_block",
        details={"first": "visible-1", "second": "visible-2", "third": "hidden-3"},
    )

    assert recorder.events[0].details["first"] == "visible-1"
    assert recorder.events[0].details["second"] == "visible-2"
    assert "third" not in recorder.events[0].details
    assert recorder.events[0].details["_truncated"] == "<truncated 1 items>"


def test_in_memory_security_audit_recorder_applies_total_detail_budget():
    recorder = InMemorySecurityAuditRecorder(
        max_detail_chars=64,
        max_detail_total_chars=96,
    )

    recorder.record(
        REASON_WEBHOOK_SIGNATURE_MISSING,
        mode="audit",
        action="would_block",
        details={
            "first": "a" * 64,
            "second": "b" * 64,
            "third": "c" * 64,
        },
    )

    assert recorder.events[0].details["first"] == "a" * 64
    assert "second" not in recorder.events[0].details
    assert "third" not in recorder.events[0].details
    assert recorder.events[0].details["_truncated"] == "<truncated details>"


@pytest.mark.parametrize("value", [0, -1])
def test_in_memory_security_audit_recorder_max_events_must_be_positive(value):
    with pytest.raises(ValueError, match="max_events"):
        InMemorySecurityAuditRecorder(max_events=value)


@pytest.mark.parametrize("value", [True, 1.5, "2"])
def test_in_memory_security_audit_recorder_max_events_must_be_integer(value):
    with pytest.raises(TypeError, match="max_events"):
        InMemorySecurityAuditRecorder(max_events=value)


@pytest.mark.parametrize(
    "field_name",
    ["max_detail_chars", "max_detail_items", "max_detail_total_chars"],
)
@pytest.mark.parametrize("value", [0, -1])
def test_security_audit_recorder_limits_must_be_positive(field_name, value):
    with pytest.raises(ValueError, match=field_name):
        SecurityAuditRecorder(**{field_name: value})


@pytest.mark.parametrize(
    "field_name",
    ["max_detail_chars", "max_detail_items", "max_detail_total_chars"],
)
@pytest.mark.parametrize("value", [True, 1.5, "2"])
def test_security_audit_recorder_limits_must_be_integer(field_name, value):
    with pytest.raises(TypeError, match=field_name):
        SecurityAuditRecorder(**{field_name: value})
