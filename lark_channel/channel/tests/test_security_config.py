"""Security configuration compatibility contract."""

import inspect
from dataclasses import replace

import lark_channel
import lark_channel.channel as channel_api
import pytest
from lark_channel.channel import ChannelConfig
from lark_channel.channel.config import TransportConfig


def test_security_config_defaults_to_compat_mode():
    SecurityConfig = getattr(lark_channel, "SecurityConfig")
    config = SecurityConfig()

    assert config.mode == "compat"
    assert config.is_compat is True
    assert config.is_audit is False
    assert config.is_strict is False
    assert config.enforce_strict_error_response is False
    assert config.strict_content_text is False
    assert config.audit_recorder is not None


def test_security_config_accepts_audit_and_strict_modes():
    SecurityConfig = getattr(lark_channel, "SecurityConfig")

    assert SecurityConfig(mode="audit").is_audit is True
    assert SecurityConfig(mode="strict").is_strict is True
    assert SecurityConfig(mode="strict").enforce_strict_error_response is True


def test_security_config_derived_flags_follow_replaced_mode():
    SecurityConfig = getattr(lark_channel, "SecurityConfig")

    config = replace(SecurityConfig(mode="compat"), mode="strict")

    assert config.mode == "strict"
    assert config.enforce_strict_error_response is True


def test_security_config_accepts_audit_recorder_override():
    SecurityConfig = getattr(lark_channel, "SecurityConfig")
    from lark_channel.event.security import InMemorySecurityAuditRecorder

    recorder = InMemorySecurityAuditRecorder()
    config = SecurityConfig(audit_recorder=recorder)

    assert config.audit_recorder is recorder


def test_security_config_rejects_invalid_audit_recorder():
    SecurityConfig = getattr(lark_channel, "SecurityConfig")

    with pytest.raises(TypeError, match="audit_recorder"):
        SecurityConfig(audit_recorder=object())


@pytest.mark.parametrize(
    "field_name",
    [
        "max_ws_fragment_parts",
        "max_ws_fragment_bytes",
        "max_concurrent_ws_handlers",
    ],
)
@pytest.mark.parametrize("value", [0, -1])
def test_security_config_resource_limits_must_be_positive(field_name, value):
    SecurityConfig = getattr(lark_channel, "SecurityConfig")

    with pytest.raises(ValueError, match=field_name):
        SecurityConfig(**{field_name: value})


@pytest.mark.parametrize(
    "field_name",
    [
        "max_ws_fragment_parts",
        "max_ws_fragment_bytes",
        "max_concurrent_ws_handlers",
    ],
)
@pytest.mark.parametrize("value", [1.5, "1", True])
def test_security_config_resource_limits_must_be_integer(field_name, value):
    SecurityConfig = getattr(lark_channel, "SecurityConfig")

    with pytest.raises(TypeError, match=field_name):
        SecurityConfig(**{field_name: value})


@pytest.mark.parametrize(
    "field_name",
    [
        "allow_unsigned_encrypted_webhook",
        "allow_insecure_ws",
        "allow_local_insecure_ws",
        "strict_error_response",
        "strict_content_text",
        "legacy_token_cache_fallback",
    ],
)
def test_security_config_bool_fields_reject_non_bool_values(field_name):
    SecurityConfig = getattr(lark_channel, "SecurityConfig")

    with pytest.raises(TypeError, match=field_name):
        SecurityConfig(**{field_name: "false"})


def test_security_config_rejects_unsupported_resource_overflow_policy():
    SecurityConfig = getattr(lark_channel, "SecurityConfig")

    with pytest.raises(ValueError, match="resource_overflow_policy"):
        SecurityConfig(resource_overflow_policy="reject")


def test_security_mode_type_is_public():
    SecurityMode = getattr(channel_api, "SecurityMode")
    mode: SecurityMode = "compat"

    assert mode == "compat"


def test_channel_config_security_is_optional_and_defaults_to_compat():
    SecurityConfig = getattr(lark_channel, "SecurityConfig")
    config = ChannelConfig()

    assert isinstance(config.security, SecurityConfig)
    assert config.security.mode == "compat"


def test_channel_config_keeps_existing_positional_arguments_stable():
    signature = inspect.signature(ChannelConfig)
    params = list(signature.parameters)

    # The bot-at-bot knobs are appended AFTER `security`, so the original
    # positional prefix (through `security`, adjacent to `media_cache`) is
    # unchanged and existing positional callers still map correctly.
    assert params.index("security") == params.index("media_cache") + 1
    assert params[-2:] == ["resolve_sender_names", "resolve_chat_members"]
    assert params.index("security") > params.index("media_cache")

    config = ChannelConfig(
        "cli_xxx",
        "secret",
        "https://example.invalid",
    )
    assert config.app_id == "cli_xxx"
    assert config.app_secret == "secret"
    assert config.domain == "https://example.invalid"
    assert isinstance(config.transport, TransportConfig)
    assert config.security.mode == "compat"


def test_channel_config_field_order_is_stable_for_positional_callers():
    signature = inspect.signature(ChannelConfig)

    assert list(signature.parameters) == [
        "app_id",
        "app_secret",
        "domain",
        "log_level",
        "encrypt_key",
        "verification_token",
        "transport",
        "chat_mode_cache",
        "policy",
        "safety",
        "inbound",
        "outbound",
        "uat",
        "http_executor",
        "media_cache",
        "security",
        # Bot-at-bot knobs, appended at the end (positional-compat preserving).
        "resolve_sender_names",
        "resolve_chat_members",
    ]


def test_feishu_channel_defaults_to_compat_security():
    channel = lark_channel.FeishuChannel(app_id="cli_xxx", app_secret="secret")

    assert channel.config.security.mode == "compat"


def test_feishu_channel_accepts_security_override():
    SecurityConfig = getattr(lark_channel, "SecurityConfig")
    security = SecurityConfig(mode="strict")

    channel = lark_channel.FeishuChannel(
        app_id="cli_xxx",
        app_secret="secret",
        security=security,
    )

    assert channel.config.security.mode == "strict"
    assert getattr(channel.client.config, "security") is security


def test_feishu_channel_preserves_security_from_config():
    SecurityConfig = getattr(lark_channel, "SecurityConfig")

    channel = lark_channel.FeishuChannel(
        config=ChannelConfig(
            app_id="cli_xxx",
            app_secret="secret",
            security=SecurityConfig(mode="audit"),
        )
    )

    assert channel.config.security.mode == "audit"
