from dataclasses import dataclass, field
import json
import re
from typing import Any, Dict, List, Mapping, Optional, Set

from lark_channel.core.const import UTF_8
from lark_channel.core.json import JSON
from lark_channel.core.log import logger

REASON_WEBHOOK_SIGNATURE_MISSING = "webhook.signature_missing"
REASON_WEBHOOK_SIGNATURE_INVALID = "webhook.signature_invalid"
REASON_WEBHOOK_TOKEN_MISSING = "webhook.token_missing"
REASON_WEBHOOK_TOKEN_INVALID = "webhook.token_invalid"
REASON_WEBHOOK_DECRYPT_WITHOUT_VERIFIED_SIGNATURE = (
    "webhook.decrypt_without_verified_signature"
)
REASON_WEBHOOK_DECRYPT_FAILED = "webhook.decrypt_failed"
REASON_CARD_TOKEN_MISSING = "card.token_missing"
REASON_CARD_TOKEN_INVALID = "card.token_invalid"
REASON_CARD_SIGNATURE_MISSING = "card.signature_missing"
REASON_CARD_SIGNATURE_INVALID = "card.signature_invalid"
REASON_CARD_DECRYPT_FAILED = "card.decrypt_failed"
REASON_WS_INSECURE_SCHEME = "ws.insecure_scheme"
REASON_WS_INVALID_TIMING = "ws.invalid_timing"
REASON_WS_FRAGMENT_LIMIT = "ws.fragment_limit"
REASON_RESOURCE_LIMIT_WOULD_BLOCK = "resource.limit_would_block"
REASON_MENTIONS_TEXT_ONLY = "mentions.text_only"
REASON_CONTENT_TEXT_UNSAFE_LEGACY = "content_text.unsafe_legacy"
REASON_TOKEN_CACHE_LEGACY_FALLBACK = "token_cache.legacy_fallback"


@dataclass(frozen=True)
class SecurityAuditEvent:
    reason: str
    mode: str
    action: str
    details: Dict[str, Any] = field(default_factory=dict)


class SecurityAuditRecorder:
    def __init__(
        self,
        *,
        max_detail_chars: int = 4096,
        max_detail_items: int = 50,
        max_detail_total_chars: int = 16384,
    ) -> None:
        self._max_detail_chars = _validate_positive_int(
            "max_detail_chars",
            max_detail_chars,
        )
        self._max_detail_items = _validate_positive_int(
            "max_detail_items",
            max_detail_items,
        )
        self._max_detail_total_chars = _validate_positive_int(
            "max_detail_total_chars",
            max_detail_total_chars,
        )

    def record(
        self,
        reason: str,
        *,
        mode: str,
        action: str,
        details: Optional[Mapping[str, Any]] = None,
    ) -> None:
        safe_details = _redacted_details(
            details,
            max_string_chars=self._max_detail_chars,
            max_items=self._max_detail_items,
            max_total_chars=self._max_detail_total_chars,
        )
        safe_reason = _redacted_label(
            reason,
            max_string_chars=self._max_detail_chars,
        )
        safe_mode = _redacted_label(
            mode,
            max_string_chars=self._max_detail_chars,
        )
        safe_action = _redacted_label(
            action,
            max_string_chars=self._max_detail_chars,
        )
        logger.warning(
            "security audit: reason=%s mode=%s action=%s details=%s",
            safe_reason,
            safe_mode,
            safe_action,
            JSON.marshal(safe_details),
        )


class InMemorySecurityAuditRecorder(SecurityAuditRecorder):
    def __init__(
        self,
        *,
        max_events: int = 1000,
        max_detail_chars: int = 4096,
        max_detail_items: int = 50,
        max_detail_total_chars: int = 16384,
    ) -> None:
        super().__init__(
            max_detail_chars=max_detail_chars,
            max_detail_items=max_detail_items,
            max_detail_total_chars=max_detail_total_chars,
        )
        self._max_events = _validate_positive_int("max_events", max_events)
        self.events: List[SecurityAuditEvent] = []

    def record(
        self,
        reason: str,
        *,
        mode: str,
        action: str,
        details: Optional[Mapping[str, Any]] = None,
    ) -> None:
        self.events.append(
            SecurityAuditEvent(
                reason=_redacted_label(
                    reason,
                    max_string_chars=self._max_detail_chars,
                ),
                mode=_redacted_label(
                    mode,
                    max_string_chars=self._max_detail_chars,
                ),
                action=_redacted_label(
                    action,
                    max_string_chars=self._max_detail_chars,
                ),
                details=_redacted_details(
                    details,
                    max_string_chars=self._max_detail_chars,
                    max_items=self._max_detail_items,
                    max_total_chars=self._max_detail_total_chars,
                ),
            )
        )
        if len(self.events) > self._max_events:
            del self.events[: len(self.events) - self._max_events]


def should_record_security_audit(security: Any) -> bool:
    recorder = getattr(security, "audit_recorder", None)
    if not callable(getattr(recorder, "record", None)):
        return False
    if not getattr(security, "is_compat", False):
        return True
    return not bool(getattr(recorder, "_lark_channel_default_recorder", False))


def build_error_response_content(error: BaseException, *, security: Any) -> bytes:
    if getattr(security, "enforce_strict_error_response", False):
        body = {"code": 500, "msg": "internal error"}
        return JSON.marshal(body).encode(UTF_8)
    return ('{"msg":"%s"}' % str(error)).encode(UTF_8)


def _validate_positive_int(name: str, value: int) -> int:
    if not isinstance(value, int) or isinstance(value, bool):
        raise TypeError(f"{name} must be a positive integer")
    if value <= 0:
        raise ValueError(f"{name} must be a positive integer")
    return value


def _redacted_details(
    details: Optional[Mapping[str, Any]],
    *,
    max_string_chars: int,
    max_items: int,
    max_total_chars: int,
) -> Dict[str, Any]:
    budget = _DetailBudget(max_total_chars)
    safe_details = _scrub_audit_text(
        {} if details is None else details,
        max_string_chars=max_string_chars,
        max_items=max_items,
        budget=budget,
    )
    if isinstance(safe_details, dict):
        return safe_details
    return {"details": safe_details}


def _redacted_label(value: str, *, max_string_chars: int) -> str:
    safe_value = _scrub_audit_text(
        str(value),
        max_string_chars=max_string_chars,
        max_items=1,
        budget=_DetailBudget(max_string_chars),
    )
    if isinstance(safe_value, str):
        return safe_value
    return str(safe_value)


_PRIVATE_KEY_LABEL_RE = r"PRIVATE " + r"KEY"
_PRIVATE_KEY_BLOCK_RE = re.compile(
    r"(?is)-----BEGIN [A-Z0-9 ]*"
    + _PRIVATE_KEY_LABEL_RE
    + r"-----.*?-----END [A-Z0-9 ]*"
    + _PRIVATE_KEY_LABEL_RE
    + r"-----"
)
_PRIVATE_KEY_OPEN_RE = re.compile(
    r"(?is)-----BEGIN [A-Z0-9 ]*" + _PRIVATE_KEY_LABEL_RE + r"-----.*"
)
_AUTHORIZATION_HEADER_RE = re.compile(
    r"(?i)\b(Authorization\s*:\s*)([^\n\r]*)(?:\r?\n[ \t]+[^\n\r]*)*"
)
_BEARER_RE = re.compile(r"(?i)\bBearer\s+[^\n\r]+")
_API_KEY_RE = re.compile(r"(?i)\b(X-Api-Key\s*:\s*)[^\n\r]+(?:\r?\n[ \t]+[^\n\r]*)*")
_COOKIE_RE = re.compile(r"(?i)\b((?:Set-)?Cookie\s*:\s*)[^\n\r]*(?:\r?\n[ \t]+[^\n\r]*)*")
_SECRET_KEY_PATTERN = (
    r"verification[_-]?token|verificationToken|"
    r"access[_-]?token|accessToken|refresh[_-]?token|refreshToken|"
    r"tenant[_-]?access[_-]?token|tenantAccessToken|"
    r"app[_-]?access[_-]?token|appAccessToken|"
    r"user[_-]?access[_-]?token|userAccessToken|"
    r"encrypt[_-]?key|encryptKey|app[_-]?secret|appSecret|"
    r"client[_-]?secret|clientSecret|api[-_]?key|apiKey|"
    r"x[_-]?api[_-]?key|xApiKey|private[_-]?key|privateKey|"
    r"set[_-]?cookie|setCookie|cookie|password|secret|token"
)
_QUOTED_AUTHORIZATION_ASSIGNMENT_RE = re.compile(
    r"(?is)\b(authorization)([\"']?\s*[:=]\s*)([\"'])(.*?)\3"
)
_UNCLOSED_QUOTED_AUTHORIZATION_ASSIGNMENT_RE = re.compile(
    r"(?is)\b(authorization)([\"']?\s*[:=]\s*)([\"']).*"
)
_AUTHORIZATION_ASSIGNMENT_RE = re.compile(
    r"(?i)\b(authorization)([\"']?\s*[:=]\s*)([^,\n\r&;]+)"
)
_QUOTED_ASSIGNMENT_SECRET_RE = re.compile(
    rf"(?is)\b({_SECRET_KEY_PATTERN})([\"']?\s*[:=]\s*)([\"']).*?\3"
)
_UNCLOSED_QUOTED_ASSIGNMENT_SECRET_RE = re.compile(
    rf"(?is)\b({_SECRET_KEY_PATTERN})([\"']?\s*[:=]\s*)([\"']).*"
)
_ASSIGNMENT_SECRET_RE = re.compile(
    rf"(?i)\b({_SECRET_KEY_PATTERN})([\"']?\s*[:=]\s*)[^,\n\r&;]+"
)
_AUTHORIZATION_SCHEMES_TO_KEEP = {
    "basic",
    "bearer",
    "digest",
    "oauth",
    "token",
}
_CAMEL_BOUNDARY_RE = re.compile(r"(?<=[a-z0-9])(?=[A-Z])")
_NON_KEY_SEPARATOR_RE = re.compile(r"[^0-9A-Za-z]+")
_AUDIT_SENSITIVE_KEYS = {
    "authorization",
    "token",
    "verification_token",
    "access_token",
    "refresh_token",
    "tenant_access_token",
    "app_access_token",
    "user_access_token",
    "app_secret",
    "client_secret",
    "api_key",
    "x_api_key",
    "encrypt_key",
    "private_key",
    "cookie",
    "set_cookie",
    "secret",
    "password",
}


class _DetailBudget:
    def __init__(self, remaining: int) -> None:
        self.remaining = remaining
        self.exhausted = False

    def consume(self, size: int) -> bool:
        if self.exhausted:
            return False
        if size > self.remaining:
            self.exhausted = True
            return False
        self.remaining -= size
        return True


def _scrub_audit_text(
    value: Any,
    *,
    max_string_chars: int,
    max_items: int,
    budget: _DetailBudget,
    seen: Optional[Set[int]] = None,
) -> Any:
    if seen is None:
        seen = set()
    if budget.exhausted:
        return "<truncated details>"
    if isinstance(value, (bytes, bytearray, memoryview)):
        summary = f"<{len(value)} bytes>"
        if not budget.consume(len(summary)):
            return "<truncated details>"
        return summary
    if isinstance(value, str):
        original_length = len(value)
        if original_length > max_string_chars:
            value = (
                f"{value[:max_string_chars]}"
                f"<truncated {original_length - max_string_chars} chars>"
            )
        value = _PRIVATE_KEY_BLOCK_RE.sub("<redacted private key>", value)
        value = _PRIVATE_KEY_OPEN_RE.sub("<redacted private key>", value)
        parsed = _try_parse_json_audit_string(value)
        if parsed is not _NOT_JSON:
            return _scrub_audit_text(
                parsed,
                max_string_chars=max_string_chars,
                max_items=max_items,
                budget=budget,
                seen=seen,
            )
        value = _AUTHORIZATION_HEADER_RE.sub(_redact_authorization_header, value)
        value = _BEARER_RE.sub("Bearer ***", value)
        value = _API_KEY_RE.sub(lambda m: f"{m.group(1)}***", value)
        value = _COOKIE_RE.sub(lambda m: f"{m.group(1)}***", value)
        value = _QUOTED_AUTHORIZATION_ASSIGNMENT_RE.sub(
            lambda m: (
                f"{m.group(1)}{m.group(2)}{m.group(3)}"
                f"{_redact_authorization_value(m.group(4))}{m.group(3)}"
            ),
            value,
        )
        value = _UNCLOSED_QUOTED_AUTHORIZATION_ASSIGNMENT_RE.sub(
            lambda m: f"{m.group(1)}{m.group(2)}{m.group(3)}***",
            value,
        )
        value = _AUTHORIZATION_ASSIGNMENT_RE.sub(
            lambda m: f"{m.group(1)}{m.group(2)}{_redact_authorization_value(m.group(3))}",
            value,
        )
        value = _QUOTED_ASSIGNMENT_SECRET_RE.sub(
            lambda m: f"{m.group(1)}{m.group(2)}{m.group(3)}***{m.group(3)}",
            value,
        )
        value = _UNCLOSED_QUOTED_ASSIGNMENT_SECRET_RE.sub(
            lambda m: f"{m.group(1)}{m.group(2)}{m.group(3)}***",
            value,
        )
        value = _ASSIGNMENT_SECRET_RE.sub(
            lambda m: f"{m.group(1)}{m.group(2)}***",
            value,
        )
        if len(value) > max_string_chars:
            value = f"{value[:max_string_chars]}<truncated {len(value) - max_string_chars} chars>"
        if not budget.consume(len(value)):
            return "<truncated details>"
        return value
    if _is_mapping_like(value):
        value_id = id(value)
        if value_id in seen:
            return "<cycle>"
        seen.add(value_id)
        redacted = {}
        count = 0
        try:
            try:
                iterator = _iter_mapping_items(value)
            except Exception:
                redacted["_truncated"] = "<truncated details>"
                return redacted
            for _ in range(max_items):
                try:
                    key, item = next(iterator)
                except StopIteration:
                    break
                except Exception:
                    redacted["_truncated"] = "<truncated details>"
                    break
                if budget.exhausted:
                    redacted["_truncated"] = "<truncated details>"
                    break
                safe_key_text = _safe_key_text(key)
                normalized_key = _normalize_audit_key_text(safe_key_text)
                key_is_sensitive = _is_audit_sensitive_key(normalized_key)
                safe_key = _scrub_audit_key(
                    safe_key_text,
                    max_string_chars=max_string_chars,
                    max_items=max_items,
                    budget=budget,
                    seen=seen,
                )
                if budget.exhausted:
                    redacted["_truncated"] = "<truncated details>"
                    break
                if key_is_sensitive:
                    item_value = _scrub_sensitive_value(
                        normalized_key,
                        item,
                        max_string_chars=max_string_chars,
                        max_items=max_items,
                        budget=budget,
                        seen=seen,
                    )
                else:
                    item_value = _scrub_audit_text(
                        item,
                        max_string_chars=max_string_chars,
                        max_items=max_items,
                        budget=budget,
                        seen=seen,
                    )
                if budget.exhausted:
                    redacted["_truncated"] = "<truncated details>"
                    break
                redacted[safe_key] = item_value
                count += 1
            length = _safe_len(value)
            if (
                redacted.get("_truncated") != "<truncated details>"
                and length is not None
                and length > count
            ):
                redacted["_truncated"] = f"<truncated {length - count} items>"
            return redacted
        finally:
            seen.discard(value_id)
    if isinstance(value, list):
        value_id = id(value)
        if value_id in seen:
            return "<cycle>"
        seen.add(value_id)
        redacted = []
        try:
            for item in value[:max_items]:
                if budget.exhausted:
                    redacted.append("<truncated details>")
                    break
                redacted.append(_scrub_audit_text(
                    item,
                    max_string_chars=max_string_chars,
                    max_items=max_items,
                    budget=budget,
                    seen=seen,
                ))
            if len(value) > max_items:
                redacted.append(f"<truncated {len(value) - max_items} items>")
            return redacted
        finally:
            seen.discard(value_id)
    if isinstance(value, tuple):
        value_id = id(value)
        if value_id in seen:
            return "<cycle>"
        seen.add(value_id)
        redacted_items = []
        try:
            for item in value[:max_items]:
                if budget.exhausted:
                    redacted_items.append("<truncated details>")
                    break
                redacted_items.append(_scrub_audit_text(
                    item,
                    max_string_chars=max_string_chars,
                    max_items=max_items,
                    budget=budget,
                    seen=seen,
                ))
            redacted = tuple(redacted_items)
            if len(value) > max_items:
                return redacted + (f"<truncated {len(value) - max_items} items>",)
            return redacted
        finally:
            seen.discard(value_id)
    if not isinstance(value, (int, float, bool, type(None))):
        summary = f"<{type(value).__name__}>"
        if not budget.consume(len(summary)):
            return "<truncated details>"
        return summary
    if not budget.consume(len(_audit_scalar_size_text(value))):
        return "<truncated details>"
    return value


_NOT_JSON = object()


def _try_parse_json_audit_string(value: str) -> Any:
    stripped = value.strip()
    if not stripped.startswith(("{", "[")):
        return _NOT_JSON
    try:
        return json.loads(stripped)
    except (TypeError, ValueError, RecursionError):
        return _NOT_JSON


def _redact_authorization_header(match: re.Match) -> str:
    value = match.group(2).strip()
    scheme = value.split(None, 1)[0] if value else ""
    if scheme.lower() in _AUTHORIZATION_SCHEMES_TO_KEEP:
        return f"{match.group(1)}{scheme} ***"
    return f"{match.group(1)}***"


def _is_mapping_like(value: Any) -> bool:
    if isinstance(value, dict):
        return True
    try:
        items = getattr(value, "items", None)
    except Exception:
        return False
    return callable(items)


def _iter_mapping_items(value: Any):
    return iter(value.items())


def _safe_len(value: Any) -> Optional[int]:
    if not isinstance(value, (dict, list, tuple)):
        return None
    try:
        return len(value)
    except Exception:
        return None


def _scrub_audit_key(
    key_text: str,
    *,
    max_string_chars: int,
    max_items: int,
    budget: _DetailBudget,
    seen: Set[int],
) -> str:
    scrubbed = _scrub_audit_text(
        key_text,
        max_string_chars=max_string_chars,
        max_items=max_items,
        budget=budget,
        seen=seen,
    )
    if isinstance(scrubbed, str):
        return scrubbed
    try:
        marshaled = JSON.marshal(scrubbed)
    except (TypeError, ValueError):
        marshaled = None
    if marshaled:
        return marshaled
    return f"<{type(scrubbed).__name__}>"


def _normalize_audit_key(value: Any) -> str:
    return _normalize_audit_key_text(_safe_key_text(value))


def _normalize_audit_key_text(value: str) -> str:
    text = _CAMEL_BOUNDARY_RE.sub("_", value)
    return _NON_KEY_SEPARATOR_RE.sub("_", text).strip("_").lower()


def _safe_key_text(value: Any) -> str:
    try:
        return str(value)
    except Exception:
        return f"<{type(value).__name__}>"


def _is_audit_sensitive_key(normalized_key: str) -> bool:
    if _is_authorization_key(normalized_key):
        return True
    return any(
        normalized_key == key or normalized_key.endswith(f"_{key}")
        for key in _AUDIT_SENSITIVE_KEYS
    )


def _is_authorization_key(normalized_key: str) -> bool:
    return "authorization" in normalized_key.split("_")


def _scrub_sensitive_value(
    normalized_key: str,
    value: Any,
    *,
    max_string_chars: int,
    max_items: int,
    budget: _DetailBudget,
    seen: Optional[Set[int]] = None,
) -> Any:
    if _is_authorization_key(normalized_key) and isinstance(value, str):
        safe_value = _redact_authorization_value(value)
        return _scrub_audit_text(
            safe_value,
            max_string_chars=max_string_chars,
            max_items=max_items,
            budget=budget,
            seen=seen,
        )
    return _scrub_audit_text(
        "***",
        max_string_chars=max_string_chars,
        max_items=max_items,
        budget=budget,
        seen=seen,
    )


def _redact_authorization_value(value: str) -> str:
    stripped = value.strip()
    scheme = stripped.split(None, 1)[0] if stripped else ""
    if scheme.lower() in _AUTHORIZATION_SCHEMES_TO_KEEP and len(stripped) > len(scheme):
        return f"{scheme} ***"
    return "***"


def _audit_scalar_size_text(value: Any) -> str:
    try:
        marshaled = JSON.marshal(value)
    except (TypeError, ValueError):
        return str(value)
    if marshaled is None:
        return "null"
    return marshaled
