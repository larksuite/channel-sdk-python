import logging
import json
import re
import sys
from typing import Any

logger = logging.getLogger("Lark")
handler = logging.StreamHandler(sys.stdout)
handler.setFormatter(logging.Formatter("[Lark] [%(asctime)s] [%(levelname)s] %(message)s"))
logger.addHandler(handler)
logger.setLevel(logging.WARNING)

_SENSITIVE_KEY_PARTS = (
    "authorization",
    "api_key",
    "x_api_key",
    "access_token",
    "refresh_token",
    "tenant_access_token",
    "app_access_token",
    "user_access_token",
    "verification_token",
    "encrypt_key",
    "private_key",
    "app_secret",
    "client_secret",
    "cookie",
    "set_cookie",
    "secret",
    "password",
    # A one-click authorization link is a signed capability — a credential in
    # link form. `authorization_url` needs no entry: the `authorization`
    # substring above already covers it. `_normalize_key` folds camelCase, so
    # this one key covers `consoleUrl` too.
    "console_url",
)
_SENSITIVE_EXACT_KEYS = {
    "token",
    "verification_token",
}
_JSON_SCALAR_TYPES = (int, float, bool, type(None))
_AUTHORIZATION_SCHEMES_TO_KEEP = {
    "basic",
    "bearer",
    "digest",
    "oauth",
    "token",
}
_CAMEL_BOUNDARY_RE = re.compile(r"(?<=[a-z0-9])(?=[A-Z])")
_NON_KEY_SEPARATOR_RE = re.compile(r"[^0-9A-Za-z]+")
_SECRET_KEY_PATTERN = (
    r"authorization|verification[_-]?token|verificationToken|"
    r"access[_-]?token|accessToken|refresh[_-]?token|refreshToken|"
    r"tenant[_-]?access[_-]?token|tenantAccessToken|"
    r"app[_-]?access[_-]?token|appAccessToken|"
    r"user[_-]?access[_-]?token|userAccessToken|"
    r"encrypt[_-]?key|encryptKey|app[_-]?secret|appSecret|"
    r"client[_-]?secret|clientSecret|api[-_]?key|apiKey|"
    r"x[_-]?api[_-]?key|xApiKey|private[_-]?key|privateKey|"
    r"set[_-]?cookie|setCookie|cookie|password|secret|token"
)
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
_API_KEY_HEADER_RE = re.compile(
    r"(?i)\b(X-Api-Key\s*:\s*)[^\n\r]+(?:\r?\n[ \t]+[^\n\r]*)*"
)
_COOKIE_HEADER_RE = re.compile(
    r"(?i)\b((?:Set-)?Cookie\s*:\s*)[^\n\r]*(?:\r?\n[ \t]+[^\n\r]*)*"
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
_BEARER_RE = re.compile(r"(?i)\bBearer\s+[^\n\r]+")


def _is_sensitive_key(key: Any) -> bool:
    normalized = _normalize_key(key)
    return normalized in _SENSITIVE_EXACT_KEYS or any(
        part in normalized for part in _SENSITIVE_KEY_PARTS
    )


def _normalize_key(key: Any) -> str:
    text = _CAMEL_BOUNDARY_RE.sub("_", str(key))
    return _NON_KEY_SEPARATOR_RE.sub("_", text).strip("_").lower()


def _redacted_value(key: Any, value: Any) -> str:
    if "authorization" in _normalize_key(key).split("_") and isinstance(value, str):
        return _redact_authorization_value(value)
    return "***"


def _redact_authorization_value(value: str) -> str:
    stripped = value.strip()
    scheme = stripped.split(None, 1)[0] if stripped else ""
    if scheme.lower() in _AUTHORIZATION_SCHEMES_TO_KEEP and len(stripped) > len(scheme):
        return f"{scheme} ***"
    return "***"


def _redact_authorization_header(match: re.Match) -> str:
    return f"{match.group(1)}{_redact_authorization_value(match.group(2))}"


def _redact_free_text(value: str) -> str:
    value = _PRIVATE_KEY_BLOCK_RE.sub("<redacted private key>", value)
    value = _PRIVATE_KEY_OPEN_RE.sub("<redacted private key>", value)
    value = _AUTHORIZATION_HEADER_RE.sub(_redact_authorization_header, value)
    value = _BEARER_RE.sub("Bearer ***", value)
    value = _API_KEY_HEADER_RE.sub(lambda m: f"{m.group(1)}***", value)
    value = _COOKIE_HEADER_RE.sub(lambda m: f"{m.group(1)}***", value)
    value = _QUOTED_ASSIGNMENT_SECRET_RE.sub(
        lambda m: f"{m.group(1)}{m.group(2)}{m.group(3)}***{m.group(3)}",
        value,
    )
    value = _UNCLOSED_QUOTED_ASSIGNMENT_SECRET_RE.sub(
        lambda m: f"{m.group(1)}{m.group(2)}{m.group(3)}***",
        value,
    )
    return _ASSIGNMENT_SECRET_RE.sub(
        lambda m: f"{m.group(1)}{m.group(2)}***",
        value,
    )


def _redact_bytes_for_log(value: bytes, *, _depth: int) -> Any:
    try:
        text = value.decode("utf-8")
    except UnicodeDecodeError:
        return f"<{len(value)} bytes>"
    stripped = text.strip()
    if stripped.startswith(("{", "[")):
        try:
            return redact_for_log(json.loads(stripped), _depth=_depth + 1)
        except (ValueError, RecursionError):
            pass
    return f"<{len(value)} bytes>"


def redact_for_log(value: Any, *, _depth: int = 0) -> Any:
    if _depth > 8:
        return "<redacted>"
    if isinstance(value, _JSON_SCALAR_TYPES):
        return value
    if isinstance(value, bytes):
        return _redact_bytes_for_log(value, _depth=_depth)
    if isinstance(value, str):
        stripped = value.strip()
        if stripped.startswith(("{", "[")):
            try:
                return redact_for_log(json.loads(stripped), _depth=_depth + 1)
            except (ValueError, RecursionError):
                return f"<{len(value)} chars>"
        return _redact_free_text(value)
    if isinstance(value, dict):
        return {
            key: _redacted_value(key, item)
            if _is_sensitive_key(key)
            else redact_for_log(item, _depth=_depth + 1)
            for key, item in value.items()
        }
    if isinstance(value, (list, tuple)):
        return [redact_for_log(item, _depth=_depth + 1) for item in value]
    return f"<{type(value).__name__}>"


def redact_query_params_for_log(value: Any, *, _depth: int = 0) -> Any:
    if _depth > 8:
        return "<redacted>"
    if isinstance(value, dict):
        return {
            key: _redacted_value(key, item)
            if _is_sensitive_key(key)
            else redact_for_log(item, _depth=_depth + 1)
            for key, item in value.items()
        }
    if isinstance(value, (list, tuple)):
        return [_redact_query_param_item(item, _depth=_depth + 1) for item in value]
    return redact_for_log(value, _depth=_depth)


def _redact_query_param_item(item: Any, *, _depth: int) -> Any:
    if isinstance(item, (list, tuple)) and len(item) == 2:
        key, value = item
        redacted = (
            _redacted_value(key, value)
            if _is_sensitive_key(key)
            else redact_for_log(value, _depth=_depth + 1)
        )
        if isinstance(item, tuple):
            return (key, redacted)
        return [key, redacted]
    return redact_query_params_for_log(item, _depth=_depth)


def redact_files_for_log(value: Any, *, _depth: int = 0) -> Any:
    if _depth > 8:
        return "<file upload>"
    if value is None:
        return None
    if isinstance(value, dict):
        return {key: "<file upload>" for key in value}
    if isinstance(value, (list, tuple)):
        redacted = []
        for item in value:
            if isinstance(item, (list, tuple)) and len(item) == 2:
                redacted.append((item[0], "<file upload>"))
            else:
                redacted.append("<file upload>")
        return redacted
    return "<file upload>"
