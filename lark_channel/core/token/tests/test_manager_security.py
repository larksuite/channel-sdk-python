import json
import logging

from lark_channel.channel.config import SecurityConfig
from lark_channel.core.cache import ICache
from lark_channel.core.http import Transport
from lark_channel.core.model import Config, RawResponse
from lark_channel.core.token.manager import TokenManager
from lark_channel.event.security import (
    InMemorySecurityAuditRecorder,
    REASON_TOKEN_CACHE_LEGACY_FALLBACK,
)


class DictCache(ICache):
    def __init__(self):
        self.values = {}

    def get(self, key: str):
        return self.values.get(key)

    def set(self, key: str, value: str, expire: int):
        self.values[key] = value


def _config(domain: str, security=None) -> Config:
    config = Config()
    config.app_id = "cli_same"
    config.app_secret = "secret"
    config.domain = domain
    if security is not None:
        config.security = security
    return config


def _token_response(token_suffix: str) -> RawResponse:
    raw = RawResponse()
    raw.status_code = 200
    raw.content = json.dumps(
        {
            "code": 0,
            "msg": "ok",
            "tenant_access_token": f"tenant-{token_suffix}",
            "app_access_token": f"app-{token_suffix}",
            "expire": 7200,
        }
    ).encode("utf-8")
    return raw


def test_self_tenant_token_cache_is_isolated_by_domain(monkeypatch):
    cache = DictCache()
    monkeypatch.setattr(TokenManager, "cache", cache)
    calls = []

    def execute(config, request):
        calls.append(config.domain)
        return _token_response(config.domain.rsplit("/", 1)[-1])

    monkeypatch.setattr(Transport, "execute", execute)

    cn_token = TokenManager.get_self_tenant_token(
        _config("https://open.feishu.cn")
    )
    global_token = TokenManager.get_self_tenant_token(
        _config("https://open.larksuite.com")
    )

    assert cn_token == "tenant-open.feishu.cn"
    assert global_token == "tenant-open.larksuite.com"
    assert calls == ["https://open.feishu.cn", "https://open.larksuite.com"]


def test_isv_tenant_token_cache_is_isolated_by_domain(monkeypatch):
    cache = DictCache()
    monkeypatch.setattr(TokenManager, "cache", cache)
    calls = []

    def execute(config, request):
        calls.append(config.domain)
        return _token_response(config.domain.rsplit("/", 1)[-1])

    monkeypatch.setattr(Transport, "execute", execute)

    cn_token = TokenManager.get_isv_tenant_token(
        _config("https://open.feishu.cn"),
        tenant_key="tenant_a",
        app_ticket="ticket",
    )
    global_token = TokenManager.get_isv_tenant_token(
        _config("https://open.larksuite.com"),
        tenant_key="tenant_a",
        app_ticket="ticket",
    )

    assert cn_token == "tenant-open.feishu.cn"
    assert global_token == "tenant-open.larksuite.com"
    assert calls == [
        "https://open.feishu.cn",
        "https://open.feishu.cn",
        "https://open.larksuite.com",
        "https://open.larksuite.com",
    ]


def test_compat_mode_reads_legacy_token_cache_key_and_records_audit(monkeypatch):
    cache = DictCache()
    cache.values["self_tenant_token:cli_same"] = "legacy-token"
    monkeypatch.setattr(TokenManager, "cache", cache)
    monkeypatch.setattr(
        Transport,
        "execute",
        lambda config, request: (_ for _ in ()).throw(AssertionError("network called")),
    )
    recorder = InMemorySecurityAuditRecorder()

    token = TokenManager.get_self_tenant_token(
        _config(
            "https://open.feishu.cn",
            security=SecurityConfig(mode="compat", audit_recorder=recorder),
        )
    )

    assert token == "legacy-token"
    assert recorder.events[0].reason == REASON_TOKEN_CACHE_LEGACY_FALLBACK
    assert recorder.events[0].mode == "compat"
    assert recorder.events[0].action == "fallback"


def test_default_compat_reads_legacy_token_cache_without_audit_warning(
    monkeypatch,
    caplog,
):
    cache = DictCache()
    cache.values["self_tenant_token:cli_same"] = "legacy-token"
    monkeypatch.setattr(TokenManager, "cache", cache)
    monkeypatch.setattr(
        Transport,
        "execute",
        lambda config, request: (_ for _ in ()).throw(AssertionError("network called")),
    )

    with caplog.at_level(logging.WARNING, logger="Lark"):
        token = TokenManager.get_self_tenant_token(
            _config(
                "https://open.feishu.cn",
                security=SecurityConfig(mode="compat"),
            )
        )

    assert token == "legacy-token"
    assert "security audit" not in caplog.text


def test_legacy_token_cache_fallback_can_be_disabled(monkeypatch):
    cache = DictCache()
    cache.values["self_tenant_token:cli_same"] = "legacy-token"
    monkeypatch.setattr(TokenManager, "cache", cache)
    calls = []

    def execute(config, request):
        calls.append(config.domain)
        return _token_response("fresh")

    monkeypatch.setattr(Transport, "execute", execute)

    token = TokenManager.get_self_tenant_token(
        _config(
            "https://open.feishu.cn",
            security=SecurityConfig(
                mode="compat",
                legacy_token_cache_fallback=False,
            ),
        )
    )

    assert token == "tenant-fresh"
    assert calls == ["https://open.feishu.cn"]


def test_strict_mode_does_not_read_legacy_token_cache_key_by_default(monkeypatch):
    cache = DictCache()
    cache.values["self_tenant_token:cli_same"] = "legacy-token"
    monkeypatch.setattr(TokenManager, "cache", cache)
    calls = []

    def execute(config, request):
        calls.append(config.domain)
        return _token_response("strict-fresh")

    monkeypatch.setattr(Transport, "execute", execute)
    recorder = InMemorySecurityAuditRecorder()

    token = TokenManager.get_self_tenant_token(
        _config(
            "https://open.feishu.cn",
            security=SecurityConfig(mode="strict", audit_recorder=recorder),
        )
    )

    assert token == "tenant-strict-fresh"
    assert calls == ["https://open.feishu.cn"]
    assert recorder.events == []
