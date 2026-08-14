"""Shared webhook signature verification with hardening (issue #11).

Feishu signs webhook requests with ``(timestamp + nonce + secret)`` plus the
raw body; the event path uses SHA-256 with the encrypt key, the card-action
path uses SHA-1 with the verification token. Both paths previously shared the
same gaps:

- with no secret configured, ``_verify_sign`` silently no-ops — a request
  carrying signature headers is accepted without any verification;
- the ``X-Lark-Request-Timestamp`` was never checked for freshness, so a
  captured request verifies forever;
- there was no ``(timestamp, nonce)`` replay dedup.

This module centralises the hardened flow. Behaviour in strict mode fails
closed (raises); in compat/audit mode the legacy accepting behaviour is kept
but every gap is surfaced through the security audit recorder and a warning
log line.
"""

import hashlib
import threading
import time
from typing import Callable, Optional

from lark_channel.core.const import (
    LARK_REQUEST_NONCE,
    LARK_REQUEST_SIGNATURE,
    LARK_REQUEST_TIMESTAMP,
)
from lark_channel.core.exception import AccessDeniedException

REASON_WEBHOOK_SIGNATURE_UNVERIFIABLE = "webhook.signature_unverifiable"
REASON_WEBHOOK_TIMESTAMP_STALE = "webhook.timestamp_stale"
REASON_WEBHOOK_REPLAY_DETECTED = "webhook.replay_detected"

_MAX_REPLAY_ENTRIES = 4096


class ReplayGuard:
    """Bounded in-memory ``(timestamp, nonce)`` dedup with TTL.

    Thread-safe; entries older than the TTL are ignored (and pruned
    opportunistically when the cache grows past ``_MAX_REPLAY_ENTRIES``).
    """

    def __init__(self, ttl_seconds: int) -> None:
        self._ttl_seconds = ttl_seconds
        self._seen = {}  # (timestamp, nonce) -> expires_at (monotonic-ish wall clock)
        self._lock = threading.Lock()

    def check_and_mark(self, timestamp: str, nonce: str) -> bool:
        """Return ``False`` when ``(timestamp, nonce)`` was seen within the TTL."""
        key = (timestamp, nonce)
        now = time.time()
        with self._lock:
            if len(self._seen) >= _MAX_REPLAY_ENTRIES:
                expired = [k for k, exp in self._seen.items() if exp < now]
                for k in expired:
                    del self._seen[k]
            expires_at = self._seen.get(key)
            if expires_at is not None and expires_at >= now:
                return False
            self._seen[key] = now + self._ttl_seconds
            return True


def _timestamp_age_seconds(timestamp: Optional[str]) -> Optional[float]:
    if not timestamp:
        return None
    try:
        ts = int(timestamp)
    except (TypeError, ValueError):
        return None
    return abs(time.time() - ts)


def _get_header(headers, name: str) -> Optional[str]:
    """Case-insensitive header lookup.

    ASGI servers (Starlette/FastAPI per the ASGI spec) hand the application
    lowercase header names, so a case-sensitive ``headers.get("X-Lark-...")``
    returns ``None`` and the signature computation crashes with a TypeError
    (issue #12). Normalize on both sides for robustness.
    """
    if not headers:
        return None
    value = headers.get(name)
    if value is not None:
        return value
    lower = name.lower()
    for key, val in headers.items():
        if str(key).lower() == lower:
            return val
    return None


def verify_webhook_signature(
    request,
    *,
    secret: Optional[str],
    algorithm: str,
    security,
    record_audit: Callable[[str, str], None],
    warn: Callable[[str], None],
    replay_guard: Optional[ReplayGuard] = None,
) -> None:
    """Verify a signed webhook request, failing loudly when verification is
    impossible or the request is stale/replayed.

    Raises ``AccessDeniedException`` in strict mode; otherwise records an
    audit event (``record_audit(reason, action)``) and warns while keeping
    the legacy accepting behaviour. The caller is responsible for auditing
    plain signature mismatches (``webhook.signature_invalid`` /
    ``card.signature_invalid``) as before.
    """
    timestamp = _get_header(request.headers, LARK_REQUEST_TIMESTAMP)
    nonce = _get_header(request.headers, LARK_REQUEST_NONCE)
    signature = _get_header(request.headers, LARK_REQUEST_SIGNATURE)
    has_signature_headers = bool(timestamp and nonce and signature)

    if not secret:
        # A request with NO signature headers is not subject to this
        # hardening — the caller's missing-signature policy applies.
        if not has_signature_headers:
            return
        # A request that DOES carry signature headers cannot be verified
        # without a secret: fail loudly instead of silently accepting.
        strict = security.is_strict
        record_audit(
            REASON_WEBHOOK_SIGNATURE_UNVERIFIABLE,
            "block" if strict else "allow_unverified",
        )
        if strict:
            raise AccessDeniedException(
                "signature verification failed: no secret configured "
                "(encrypt_key / verification_token)"
            )
        warn(
            "webhook request carries signature headers but no secret is "
            "configured; the signature cannot be verified"
        )
        return

    # Timestamp freshness (opt-in via SecurityConfig.max_timestamp_skew_seconds).
    skew_seconds = security.max_timestamp_skew_seconds
    if skew_seconds is not None:
        age = _timestamp_age_seconds(timestamp)
        if age is None or age > skew_seconds:
            strict = security.is_strict
            record_audit(
                REASON_WEBHOOK_TIMESTAMP_STALE,
                "block" if strict else "allow_stale",
            )
            if strict:
                raise AccessDeniedException(
                    "request timestamp is missing or outside the allowed window "
                    f"({skew_seconds}s)"
                )
            warn(
                "webhook request timestamp is missing or stale "
                f"(age={age if age is not None else 'unknown'}s)"
            )

    # Replay protection (opt-in via SecurityConfig.replay_protection_seconds).
    replay_ttl = security.replay_protection_seconds
    if replay_ttl is not None:
        guard = replay_guard or ReplayGuard(replay_ttl)
        if not guard.check_and_mark(timestamp or "", nonce or ""):
            strict = security.is_strict
            record_audit(
                REASON_WEBHOOK_REPLAY_DETECTED,
                "block" if strict else "allow_replay",
            )
            if strict:
                raise AccessDeniedException("replayed webhook request detected")
            warn("webhook request replay detected (duplicate timestamp/nonce)")

    data = (timestamp + nonce + secret).encode("utf-8") + request.body
    digest = (
        hashlib.sha256(data).hexdigest()
        if algorithm == "sha256"
        else hashlib.sha1(data).hexdigest()
    )
    if signature != digest:
        raise AccessDeniedException("signature verification failed")
