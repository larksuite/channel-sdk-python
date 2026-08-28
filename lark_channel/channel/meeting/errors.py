"""Error construction, capability-link validation, and log sanitizing.

Two rules shape this module:

**Stripping happens where the error is built, not where it is logged.** The
same :class:`FeishuChannelError` that reaches the fallback logger also reaches
the application's ``error`` handler, and applications hand those straight to a
crash reporter — which walks the cause chain and every attribute on it. The
transport exception keeps the outgoing request, headers included, reachable
that way while its ``repr()`` stays clean. So the original exception is never
attached; only the fields needed to tell failures apart survive.

**Untrusted text is escaped before it can reach a log line.** Passing it as a
lazy logging argument moves it out of the message template but the logging
layer still formats it into the same output line, so a newline in a meeting
title still forges a log entry.
"""

from typing import Any, Dict, Optional
from urllib.parse import urlsplit

from ..errors import FeishuChannelError, FeishuChannelErrorCode, classify_api_error
from ..errors import classify_http_status

#: C0 plus C1. Tab is escaped along with the rest: a log line is a line, and
#: nothing in a meeting payload needs to lay out columns in it.
_CONTROL_ORDINALS = tuple(range(0x00, 0x20)) + tuple(range(0x7F, 0xA0))
_CONTROL_TRANSLATION = dict((code, "\\x%02x" % code) for code in _CONTROL_ORDINALS)


def sanitize_for_log(value: Any) -> str:
    """``value`` as a string with every control character escaped.

    Meeting titles, transcripts, chat bodies and document headings are written
    by participants, who may be external or guest users. A newline in any of
    them forges a log line; an ANSI escape repaints a terminal.
    """
    if value is None:
        return ""
    return str(value).translate(_CONTROL_TRANSLATION)


def safe_console_url(value: Any) -> Optional[str]:
    """``value`` unchanged if it is a plausible authorization link, else ``None``.

    The link is a signed one-click grant — a capability, and therefore itself a
    credential. It is opaque, so it is validated and never rewritten:
    re-encoding or reassembling any part of it stops it working.

    Two checks, and nothing else:

    * the scheme is ``https``. The domain this arrives from is configurable, so
      the field is not a trusted source, and whoever receives it renders it as
      a link — where ``javascript:`` or ``data:`` is script execution.
    * there is no userinfo. ``https://open.feishu.cn@elsewhere.example/x``
      passes a scheme check while actually pointing at ``elsewhere.example``,
      and the entire purpose of this field is that an administrator clicks it.

    The host itself is *not* checked: self-hosted and proxied deployments make
    a host allowlist reject legitimate links.

    Parsing failures count as invalid rather than propagating. This runs while
    an error object is being built, and an exception here would replace a real
    API failure with a URL-parsing one.
    """
    if not isinstance(value, str) or not value:
        return None
    try:
        parts = urlsplit(value)
        if parts.scheme != "https":
            return None
        if parts.username is not None:
            return None
    except Exception:
        return None
    return value


def _first_console_url(body: Dict[str, Any]) -> Optional[str]:
    """The console link, from either place the platform puts it."""
    for candidate in (body, body.get("error") if isinstance(body, dict) else None):
        if isinstance(candidate, dict):
            found = safe_console_url(candidate.get("console_url"))
            if found is not None:
                return found
    return None


def build_api_error(
    *,
    what: str,
    status: Optional[int],
    body: Optional[Dict[str, Any]] = None,
    transport_error: Optional[BaseException] = None,
) -> FeishuChannelError:
    """A :class:`FeishuChannelError` carrying no credentials.

    ``transport_error`` is used only to pick a code and is deliberately **not**
    attached as the cause: an ``httpx`` exception keeps the request's
    ``Authorization`` header reachable by attribute walk.
    """
    body = body if isinstance(body, dict) else {}
    feishu_code = body.get("code") or None
    msg = body.get("msg") or ""
    if feishu_code:
        code = classify_api_error(int(feishu_code), msg)
    elif status is not None:
        code = classify_http_status(status)
    else:
        code = FeishuChannelErrorCode.UNKNOWN

    context: Dict[str, Any] = {}
    if status is not None:
        context["status"] = status
    if feishu_code:
        context["feishu_code"] = int(feishu_code)
    console_url = _first_console_url(body)
    if console_url is not None:
        # Passed through byte for byte; equally a credential, so it is excluded
        # from the fallback log and masked by the redaction layer.
        context["console_url"] = console_url

    detail = sanitize_for_log(msg) if msg else ""
    if transport_error is not None and not detail:
        # The exception type, never the exception: its repr is clean but its
        # attributes are not.
        detail = type(transport_error).__name__
    parts = [what]
    if feishu_code:
        parts.append("code=%s" % int(feishu_code))
    elif status is not None:
        parts.append("status=%s" % status)
    if detail:
        parts.append(detail)
    return FeishuChannelError(code, ": ".join(parts), context=context)


def log_error_fields(meeting_id: str, error: FeishuChannelError) -> Dict[str, Any]:
    """The minimal shape for the fallback log: no ``context``.

    ``context`` may hold a console link, which is a credential.
    """
    return {
        "meeting_id": sanitize_for_log(meeting_id),
        "code": error.code.value,
        "message": sanitize_for_log(error.message),
        "feishu_code": (error.context or {}).get("feishu_code"),
    }


__all__ = [
    "build_api_error",
    "log_error_fields",
    "safe_console_url",
    "sanitize_for_log",
]
