"""UAT device-flow runner — extracted from :class:`FeishuChannel`.

Performs the "check cache → refresh if needed → start device flow → prompt
user → poll until authorized" dance. Separated so :mod:`..channel` can stay
focused on lifecycle.
"""

import asyncio
import weakref
from typing import Any, Dict, List

from lark_channel.core.log import logger

from ..card.builder import new_card as _card_factory
from ..errors import UATAuthError
from ..types import UAT
from .device_flow import DeviceFlowClient, uat_needs_refresh
from .token_store import TokenStore


# Per-user-open-id asyncio locks so concurrent handler invocations for the
# same user don't both try to refresh an expiring token simultaneously. The
# interactive device-flow prompt/poll step runs outside this lock so a waiting
# authorization does not block unrelated cache reads forever.
# The locks bind to the loop of the first caller. A caller on a *different*
# loop gets no mutual exclusion at all when the lock is free, and a
# ``RuntimeError`` about a future attached to another loop when it is
# contended — it does not degrade gracefully. Same-user concurrency across
# loops is therefore not a supported configuration; callers that must handle
# it should treat that RuntimeError as a credential failure rather than let it
# escape as an unhandled task exception.
# Keyed by loop first, then by user. An ``asyncio.Lock`` binds to the loop it
# is first awaited on, so a single flat registry hands a lock created on one
# loop to a caller on another — which is the failure described above. The outer
# map holds loops weakly, so a loop that goes away (``stop()`` builds a fresh
# one on the next ``start()``) takes its locks with it instead of leaving
# permanently unusable entries behind.
_loop_user_locks: "weakref.WeakKeyDictionary" = weakref.WeakKeyDictionary()
_loopless_user_locks: Dict[str, asyncio.Lock] = {}


def _get_user_lock(user_open_id: str) -> asyncio.Lock:
    """The per-user lock for the loop this call is running on."""
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        # No running loop: nothing can contend, so a shared bucket is enough.
        return _loopless_user_locks.setdefault(user_open_id, asyncio.Lock())
    try:
        per_user = _loop_user_locks[loop]
    except (KeyError, TypeError):
        # TypeError, not just KeyError: `WeakKeyDictionary.__getitem__` builds a
        # weak reference to look the key up, so a loop that cannot be
        # weak-referenced raises right here rather than on assignment.
        per_user = {}
        try:
            _loop_user_locks[loop] = per_user
        except TypeError:
            # A loop implementation that cannot be weak-referenced (some
            # third-party loops). Falling back keeps credential refresh working
            # — losing the automatic cleanup is a far smaller problem than
            # raising on this path, which every UAT caller goes through.
            return _loopless_user_locks.setdefault(user_open_id, asyncio.Lock())
    lock = per_user.get(user_open_id)
    if lock is None:
        lock = asyncio.Lock()
        per_user[user_open_id] = lock
    return lock


async def require_user_auth(
    *,
    device_flow: DeviceFlowClient,
    token_store: TokenStore,
    uat_config: Any,
    user_open_id: str,
    scopes: List[str],
    context: Any,
) -> UAT:
    """Resolve a usable UAT for ``user_open_id``, running device flow if needed.

    ``uat_config`` is a :class:`~..config.UATConfig` with scope allow/block
    lists and the refresh slack; ``context`` is the object used to prompt the
    user and should expose ``respond(card)``.

    A per-user asyncio.Lock serializes concurrent callers for the same user
    through cache lookup and refresh. The prompt/poll device-flow phase is
    intentionally outside that lock.
    """
    ub = uat_config
    if ub.allowed_scopes is not None:
        for s in scopes:
            if s not in ub.allowed_scopes:
                raise UATAuthError(f"scope {s} not in allowed_scopes")
    if ub.blocked_scopes:
        for s in scopes:
            if s in ub.blocked_scopes:
                raise UATAuthError(f"scope {s} is blocked")

    async with _get_user_lock(user_open_id or ""):
        existing = await token_store.get(user_open_id or "")
        if existing is not None:
            missing = [s for s in scopes if s and s not in (existing.scopes or [])]
            if not missing:
                if uat_needs_refresh(
                    existing, slack_seconds=ub.refresh_before_expiry_seconds
                ):
                    if existing.refresh_token:
                        try:
                            refreshed = await device_flow.refresh(existing.refresh_token)
                            refreshed.open_id = user_open_id
                            if not refreshed.scopes and existing.scopes:
                                refreshed.scopes = existing.scopes
                            await token_store.set(user_open_id, refreshed)
                            return refreshed
                        except UATAuthError:
                            await token_store.delete(user_open_id)
                    else:
                        await token_store.delete(user_open_id)
                else:
                    return existing

        init = await device_flow.start(scopes)
    try:
        prompt_card = (
            _card_factory()
            .header(title="Authorization required", template="blue")
            .markdown(
                f"Please click the link to complete authorization: "
                f"{init.verification_uri_complete}\n\n"
                f"User code: `{init.user_code}`\n"
                f"Expires in: {init.expires_in}s"
            )
            .build()
        )
        if context is not None and hasattr(context, "respond"):
            await context.respond(prompt_card)
    except Exception as e:
        logger.warning("require_user_auth: failed to send prompt card: %s", e)

    uat = await device_flow.poll(
        init.device_code,
        interval=init.interval or ub.device_poll_interval_seconds,
        timeout_seconds=init.expires_in,
    )
    uat.open_id = user_open_id
    if not uat.scopes:
        uat.scopes = list(scopes)
    await token_store.set(user_open_id, uat)
    return uat


async def resolve_user_auth_non_interactive(
    *,
    device_flow: DeviceFlowClient,
    token_store: TokenStore,
    uat_config: Any,
    user_open_id: str,
) -> UAT:
    """Resolve a usable UAT for ``user_open_id`` **without** prompting anybody.

    Cache lookup plus a refresh when the ticket is close to expiry, and
    nothing else. Raises :class:`UATAuthError` when there is no usable ticket.

    This lives here, next to :func:`require_user_auth`, because it has to take
    the *same* per-user lock — the registry is module-level, so a lock created
    anywhere else excludes nothing. Two things go wrong without shared
    serialization: a refresh returns a **new** refresh token and retires the
    old one, so whichever caller arrives second with the stale one is rejected,
    and :func:`require_user_auth` answers a rejection by deleting the ticket —
    taking a perfectly good authorization away from its owner, who then gets an
    unexpected authorization card.

    Why a separate function rather than a flag on :func:`require_user_auth`:
    that one starts a device flow whenever the stored scopes do not contain the
    requested one verbatim. Ticket scopes are whatever the platform granted the
    app, so that is an ordinary state, not an error — and a polling loop asking
    every few seconds would turn it into an unbounded stream of authorization
    cards, or a silent six-hundred-second stall inside ``poll``.

    Refresh failures do **not** delete the ticket. Deleting is the interactive
    path's prerogative: it can ask for a new authorization immediately, whereas
    a polling loop can only arrange for somebody's next unrelated call to fail
    with a surprise card.
    """
    slack = getattr(uat_config, "refresh_before_expiry_seconds", 0) or 0
    async with _get_user_lock(user_open_id or ""):
        existing = await token_store.get(user_open_id or "")
        if existing is None:
            raise UATAuthError(
                "no stored user authorization for this user; authorize once "
                "interactively before starting a non-interactive flow"
            )
        if not uat_needs_refresh(existing, slack_seconds=slack):
            return existing
        if not existing.refresh_token:
            raise UATAuthError("stored user authorization expired and cannot be renewed")
        refreshed = await device_flow.refresh(existing.refresh_token)
        refreshed.open_id = user_open_id
        if not refreshed.scopes and existing.scopes:
            refreshed.scopes = existing.scopes
        # Written back inside the lock: the refresh token just rotated, and the
        # one we used is already dead.
        await token_store.set(user_open_id, refreshed)
        return refreshed
