import time
import urllib.parse
from typing import Optional

from lark_channel.core import JSON, Strings
from lark_channel.core.cache import *
from lark_channel.core.const import UTF_8
from lark_channel.core.exception import ObtainAccessTokenException
from lark_channel.core.http import Transport
from lark_channel.core.model import Config, RawResponse
from .access_token_response import AccessTokenResponse
from .create_isv_app_token_request import CreateIsvAppTokenRequest
from .create_isv_tenant_token_request import CreateIsvTenantTokenRequest
from .create_self_app_token_request import CreateSelfAppTokenRequest
from .create_self_tenant_token_request import CreateSelfTenantTokenRequest
from .create_token_request_body import CreateTokenRequestBody


class TokenManager(object):
    cache: ICache = LocalCache.instance()

    @staticmethod
    def get_self_app_token(conf: Config) -> str:
        # Read from cache
        legacy_cache_key = f"self_app_token:{conf.app_id}"
        cache_key = _domain_scoped_cache_key(conf, legacy_cache_key)
        token = _get_cached_token(
            conf,
            cache_key=cache_key,
            legacy_cache_key=legacy_cache_key,
            token_kind="self_app_token",
        )
        if Strings.is_not_empty(token):
            return token

        # On cache miss, request a new token
        req: CreateSelfAppTokenRequest = CreateSelfAppTokenRequest.builder() \
            .request_body(CreateTokenRequestBody.builder()
                          .app_id(conf.app_id)
                          .app_secret(conf.app_secret)
                          .build()) \
            .build()
        raw: RawResponse = Transport.execute(conf, req)
        resp = JSON.unmarshal(str(raw.content, UTF_8), AccessTokenResponse)

        if not resp.success():
            raise ObtainAccessTokenException("obtain self app access token failed", resp.code, resp.msg)

        # Write to cache
        token = resp.app_access_token
        expire = time.time() + resp.expire - 10 * 60  # Expire 10 minutes early
        TokenManager.cache.set(cache_key, token, int(expire))

        return token

    @staticmethod
    def get_self_tenant_token(config: Config) -> str:
        # Read from cache
        legacy_cache_key = f"self_tenant_token:{config.app_id}"
        cache_key = _domain_scoped_cache_key(config, legacy_cache_key)
        token = _get_cached_token(
            config,
            cache_key=cache_key,
            legacy_cache_key=legacy_cache_key,
            token_kind="self_tenant_token",
        )
        if Strings.is_not_empty(token):
            return token

        # On cache miss, request a new token
        req: CreateSelfTenantTokenRequest = CreateSelfTenantTokenRequest.builder() \
            .request_body(CreateTokenRequestBody.builder()
                          .app_id(config.app_id)
                          .app_secret(config.app_secret)
                          .build()) \
            .build()
        raw: RawResponse = Transport.execute(config, req)
        resp = JSON.unmarshal(str(raw.content, UTF_8), AccessTokenResponse)

        if not resp.success():
            raise ObtainAccessTokenException("obtain self tenant access token failed", resp.code, resp.msg)

        # Write to cache
        token = resp.tenant_access_token
        expire = time.time() + resp.expire - 10 * 60  # Expire 10 minutes early
        TokenManager.cache.set(cache_key, token, int(expire))

        return token

    @staticmethod
    def get_isv_app_token(config: Config, app_ticket: str) -> str:
        # Read from cache
        legacy_cache_key = f"isv_app_token:{config.app_id}"
        cache_key = _domain_scoped_cache_key(config, legacy_cache_key)
        token = _get_cached_token(
            config,
            cache_key=cache_key,
            legacy_cache_key=legacy_cache_key,
            token_kind="isv_app_token",
        )
        if Strings.is_not_empty(token):
            return token

        if Strings.is_empty(app_ticket):
            pass

        # On cache miss, request a new token
        req: CreateIsvAppTokenRequest = CreateIsvAppTokenRequest.builder() \
            .request_body(CreateTokenRequestBody.builder()
                          .app_id(config.app_id)
                          .app_secret(config.app_secret)
                          .app_ticket(app_ticket).build()) \
            .build()
        raw: RawResponse = Transport.execute(config, req)
        resp = JSON.unmarshal(str(raw.content, UTF_8), AccessTokenResponse)

        if not resp.success():
            raise ObtainAccessTokenException("obtain isv app access token failed", resp.code, resp.msg)

        # Write to cache
        token = resp.app_access_token
        expire = time.time() + resp.expire - 10 * 60  # Expire 10 minutes early
        TokenManager.cache.set(cache_key, token, int(expire))

        return token

    @staticmethod
    def get_isv_tenant_token(config: Config, tenant_key: str, app_ticket: str) -> str:
        # Read from cache
        legacy_cache_key = f"isv_tenant_token:{config.app_id}:{tenant_key}"
        cache_key = _domain_scoped_cache_key(config, legacy_cache_key)
        token = _get_cached_token(
            config,
            cache_key=cache_key,
            legacy_cache_key=legacy_cache_key,
            token_kind="isv_tenant_token",
        )
        if Strings.is_not_empty(token):
            return token

        app_token = TokenManager.get_isv_app_token(config, app_ticket)
        # On cache miss, request a new token
        req: CreateIsvTenantTokenRequest = CreateIsvTenantTokenRequest.builder() \
            .request_body(CreateTokenRequestBody.builder()
                          .app_access_token(app_token)
                          .tenant_key(tenant_key).build()) \
            .build()
        raw: RawResponse = Transport.execute(config, req)
        resp = JSON.unmarshal(str(raw.content, UTF_8), AccessTokenResponse)

        if not resp.success():
            raise ObtainAccessTokenException("obtain isv tenant access token failed", resp.code, resp.msg)

        # Write to cache
        token = resp.tenant_access_token
        expire = time.time() + resp.expire - 10 * 60  # Expire 10 minutes early
        TokenManager.cache.set(cache_key, token, int(expire))

        return token


def _domain_scoped_cache_key(config: Config, legacy_cache_key: str) -> str:
    return f"{legacy_cache_key}:v2:{_domain_cache_origin(config)}"


def _domain_cache_origin(config: Config) -> str:
    domain = getattr(config, "domain", "") or ""
    parsed = urllib.parse.urlsplit(domain)
    if not parsed.scheme or not parsed.netloc:
        return domain.rstrip("/")
    return urllib.parse.urlunsplit(
        (parsed.scheme.lower(), parsed.netloc.lower(), "", "", "")
    )


def _get_cached_token(
    config: Config,
    *,
    cache_key: str,
    legacy_cache_key: str,
    token_kind: str,
) -> Optional[str]:
    token = TokenManager.cache.get(cache_key)
    if Strings.is_not_empty(token):
        return token
    if not _legacy_token_cache_fallback_enabled(config):
        return None
    token = TokenManager.cache.get(legacy_cache_key)
    if Strings.is_not_empty(token):
        _record_legacy_token_cache_fallback(
            config,
            token_kind=token_kind,
            legacy_cache_key=legacy_cache_key,
            cache_key=cache_key,
        )
        return token
    return None


def _legacy_token_cache_fallback_enabled(config: Config) -> bool:
    security = getattr(config, "security", None)
    if security is None:
        return True
    effective = getattr(security, "effective_legacy_token_cache_fallback", None)
    if effective is not None:
        return bool(effective)
    return bool(getattr(security, "legacy_token_cache_fallback", True))


def _record_legacy_token_cache_fallback(
    config: Config,
    *,
    token_kind: str,
    legacy_cache_key: str,
    cache_key: str,
) -> None:
    security = getattr(config, "security", None)
    recorder = getattr(security, "audit_recorder", None)
    if not callable(getattr(recorder, "record", None)):
        return
    from lark_channel.event.security import (
        REASON_TOKEN_CACHE_LEGACY_FALLBACK,
        should_record_security_audit,
    )

    if not should_record_security_audit(security):
        return

    recorder.record(
        REASON_TOKEN_CACHE_LEGACY_FALLBACK,
        mode=getattr(security, "mode", "compat"),
        action="fallback",
        details={
            "token_kind": token_kind,
            "domain": _domain_cache_origin(config),
            "legacy_cache_key": legacy_cache_key,
            "cache_key": cache_key,
        },
    )
