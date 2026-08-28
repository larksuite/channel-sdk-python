"""lark_channel.channel.channel — the FeishuChannel capability layer.

A single class owning lifecycle, transport (WS / webhook), inbound
normalization, safety pipeline, outbound sender, and streaming.

The event-registration API uses string events::

    channel.on("message", handler)      # handler(inbound: InboundMessage)
    channel.on("cardAction", handler)   # handler(event: CardActionEvent)
    channel.on("reaction", handler)     # handler(event: ReactionEvent)
    channel.on("botAdded", handler)     # handler(event: BotAddedEvent)
    channel.on("botLeave", handler)
    channel.on("messageRead", handler)
    channel.on("comment", handler)
    channel.on("reject", handler)       # handler(event: RejectEvent)
    channel.on("reconnecting", handler)
    channel.on("reconnected", handler)
    channel.on("error", handler)

For sending, streaming, message ops — use the channel methods directly
(``channel.send(...)``, ``channel.stream(...)``, ``channel.update_card(...)``,
etc). There are no typed-context hooks on event payloads — work with the
plain event data and call back into the channel.

Orchestration-only. Orthogonal concerns live next door:

- Input coercion      → :mod:`._coerce`
- Raw Lark API calls  → :mod:`._api_helpers`
- UAT device flow     → :mod:`.auth.uat_runner`
"""

from __future__ import annotations

import asyncio
import concurrent.futures
import inspect
import json
import threading
import time
from collections import OrderedDict
from dataclasses import dataclass, replace
from pathlib import Path
from typing import Any, Callable, Dict, List, Literal, Mapping, Optional, Set, Union

from lark_channel.client import Client
from lark_channel.core.enum import AccessTokenType, HttpMethod, LogLevel
from lark_channel.core.http import Transport
from lark_channel.core.log import logger
from lark_channel.core.model import BaseRequest, RequestOption
# Imported as a module (not ``from …auth import verify``) so the tenant-token
# injection is reached via ``_token_auth.verify`` — tests patch it at its source
# (``lark_channel.core.token.auth.verify``), which a local alias would bypass.
from lark_channel.core.token import auth as _token_auth
from lark_channel.event.callback.model.p2_card_action_trigger import (
    P2CardActionTrigger,
    P2CardActionTriggerResponse,
)
from lark_channel.event.dispatcher_handler import EventDispatcherHandler
from lark_channel.ws.client import Client as WSClient

from . import _api_helpers, _coerce
from .auth.device_flow import DeviceFlowClient
from .events import ChannelEventName
from lark_channel.core.cache import ICache

from .auth.token_store import InMemoryTokenStore, TokenStore
from .auth.uat_runner import require_user_auth
from .auth.uat_runner import resolve_user_auth_non_interactive
from .meeting import dedup as _meeting_dedup
from .meeting.loop_affinity import await_on as _await_on_loop
from .meeting import registry as _meeting_registry
from .meeting.types import MeetingEventHealth, MeetingOptions
from .raw_events import RawEventRegistry
from .bot_identity import BotIdentity, fetch_bot_identity
from .chat_member_cache import ChatMemberCache
from .chat_mode import ChatModeCache
from .comment import CommentPrimitiveClient
from .config import (
    ChannelConfig,
    InboundConfig,
    OutboundConfig,
    PolicyConfig,
    SafetyConfig,
    SecurityConfig,
    TransportConfig,
    UATConfig,
)
from .driver import LarkClientDriver
from .errors import (
    FeishuChannelError,
    FeishuChannelErrorCode,
    OutboundSendError,
    SendError,
    classify_api_error,
    classify_http_status,
)
from .identity import IdentityResolver, NameCache
from .keepalive import KeepaliveWatchdog
from .media_cache import MediaResourceCache
from .normalize.comment import normalize_comment
from .normalize.dedup import Deduper, InMemoryDedupStore
from .normalize.pipeline import InboundPipeline, PipelineConfig, PipelineDeps
from .outbound.markdown.resolve_mentions import (
    resolve_mentions_in_text,
    resolve_name_mentions,
)
from .outbound.routing import infer_receive_id_type
from .outbound.sender import OutboundSender
from .outbound.streaming.card_stream import CardStreamController
from .outbound.streaming.markdown_stream import MarkdownStreamController
from .quote import QuoteResolver
from .safety import RejectEvent, SafetyPipeline
from .safety.dedup_cache import SeenCache
from .types import (
    UAT,
    BotAddedEvent,
    BotLeaveEvent,
    CachedResource,
    CardActionEvent,
    CardActionPayload,
    ChatInfo,
    ChatMember,
    CommentContext,
    CommentTarget,
    ConnectionSnapshot,
    EventOperator,
    MediaSource,
    MessageReadEvent,
    OutboundCard,
    OutboundPost,
    OutboundText,
    ReactionEvent,
    ResourceDescriptor,
    SendResult,
)

EventHandler = Callable[..., Any]
Unsubscribe = Callable[[], None]


@dataclass
class _SentMessageContext:
    message_id: str
    chat_id: str
    chat_type: Optional[str] = None
    receive_id_type: Optional[str] = None


def _card_action_identity(action: Any) -> str:
    """Stable dedup fragment for a card action.

    Different buttons on the same card click to different ``(tag, value)``
    pairs, and those must dedup-distinctly; a genuine WS redelivery of the
    same click hashes identically and is suppressed.
    """
    tag = getattr(action, "tag", "") or ""
    value = getattr(action, "value", None)
    extras = {
        key: getattr(action, key, None)
        for key in ("form_value", "input_value", "options", "checked")
        if getattr(action, key, None) is not None
    }
    if not extras:
        value_repr = _legacy_card_action_value_identity(value)
        return f"{tag}:{value_repr}"
    value_repr = json.dumps(
        _json_safe_identity_value(value),
        sort_keys=True,
        ensure_ascii=False,
    )
    extras_repr = json.dumps(
        _json_safe_identity_value(extras),
        sort_keys=True,
        ensure_ascii=False,
    )
    return f"{tag}:{value_repr}:{extras_repr}"


def _legacy_card_action_value_identity(value: Any) -> str:
    try:
        return json.dumps(value, sort_keys=True, ensure_ascii=False)
    except (TypeError, ValueError):
        return repr(value)


def _json_safe_identity_value(value: Any, seen: Optional[Set[int]] = None) -> Any:
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    if seen is None:
        seen = set()
    if isinstance(value, dict):
        oid = id(value)
        if oid in seen:
            return "<cycle>"
        seen.add(oid)
        try:
            return {
                _json_safe_identity_key(key): _json_safe_identity_value(val, seen)
                for key, val in value.items()
            }
        finally:
            seen.discard(oid)
    if isinstance(value, (list, tuple)):
        oid = id(value)
        if oid in seen:
            return "<cycle>"
        seen.add(oid)
        try:
            return [_json_safe_identity_value(item, seen) for item in value]
        finally:
            seen.discard(oid)
    return f"<unserializable:{type(value).__name__}>"


def _json_safe_identity_key(key: Any) -> str:
    if isinstance(key, str):
        return key
    if key is None or isinstance(key, (int, float, bool)):
        return str(key)
    return f"<key:{type(key).__name__}>"


def _extract_first_message_item(raw: Any) -> Optional[Dict[str, Any]]:
    if not isinstance(raw, dict):
        return None
    data = raw.get("data")
    if isinstance(data, dict):
        items = data.get("items")
        if isinstance(items, list) and items and isinstance(items[0], dict):
            return items[0]
        message = data.get("message")
        if isinstance(message, dict):
            return message
    if raw.get("message_id"):
        return raw
    return None


def _normalize_fetched_message_item(item: Dict[str, Any]) -> Dict[str, Any]:
    msg = dict(item)
    if not msg.get("message_type") and msg.get("msg_type"):
        msg["message_type"] = msg.get("msg_type")
    if msg.get("content") is None:
        body = msg.get("body")
        if isinstance(body, dict) and body.get("content") is not None:
            msg["content"] = body.get("content")
    return msg


def _status_error_code(status: Optional[int]) -> "FeishuChannelErrorCode":
    """Map an HTTP status to a channel error code, defaulting to UNKNOWN when
    the status is absent or a 2xx (a malformed/unexpected body on a 2xx isn't an
    HTTP-level failure)."""
    if status is not None and status >= 400:
        return classify_http_status(status)
    return FeishuChannelErrorCode.UNKNOWN


def _extract_fetched_sender(item: Dict[str, Any]) -> Any:
    sender = item.get("sender")
    if sender is not None:
        if isinstance(sender, dict) and "sender_id" not in sender:
            sender_id = {}
            if sender.get("open_id"):
                sender_id["open_id"] = sender.get("open_id")
            if sender.get("user_id"):
                sender_id["user_id"] = sender.get("user_id")
            if sender.get("union_id"):
                sender_id["union_id"] = sender.get("union_id")
            raw_id = sender.get("id")
            id_type = sender.get("id_type")
            if raw_id:
                if id_type == "user_id":
                    sender_id.setdefault("user_id", raw_id)
                elif id_type == "union_id":
                    sender_id.setdefault("union_id", raw_id)
                elif id_type in (None, "", "open_id"):
                    sender_id.setdefault("open_id", raw_id)
            if sender_id:
                return {
                    "sender_id": sender_id,
                    "sender_type": sender.get("sender_type") or sender.get("type") or "user",
                }
        return sender
    sender_id = item.get("sender_id")
    if isinstance(sender_id, dict):
        return {"sender_id": sender_id, "sender_type": item.get("sender_type") or "user"}
    return {}


# ---------------------------------------------------------------------------
# FeishuChannel
# ---------------------------------------------------------------------------


#: The only scope the follow path asks for. What the platform actually grants
#: is whatever the app has applied for, which is usually much broader — see
#: `follow_my_meeting` and docs/security.md.
_MEETING_EVENT_SCOPE = "vc:meeting.meetingevent:read"


def _meeting_payload(handler):
    """Adapt a dispatcher callback to the meeting channel's dict-in signature."""

    def _dispatch(data):
        handler(_coerce.obj_to_dict(data) or {})

    return _dispatch


class FeishuChannel:
    """Single public entry point for the Feishu Channel capability layer.

    Construct with flat keyword arguments for the common case::

        channel = FeishuChannel(app_id="cli_xxx", app_secret="***")

    Or tune any of the five functional areas (``policy`` / ``safety`` /
    ``inbound`` / ``outbound`` / ``uat``) by passing a config dataclass::

        from lark_channel import (
            FeishuChannel, SafetyConfig, DedupConfig,
            OutboundConfig, RetryConfig,
        )

        channel = FeishuChannel(
            app_id="cli_xxx",
            app_secret="***",
            safety=SafetyConfig(dedup=DedupConfig(ttl_seconds=43_200)),
            outbound=OutboundConfig(retry=RetryConfig(max_attempts=5)),
        )

    For pre-built full configs, pass ``config=ChannelConfig(...)``; per-area
    kwargs override the fields they touch.
    """

    def __init__(
        self,
        *,
        app_id: Optional[str] = None,
        app_secret: Optional[str] = None,
        encrypt_key: Optional[str] = None,
        verification_token: Optional[str] = None,
        domain: Optional[str] = None,
        log_level: Optional[LogLevel] = None,
        transport: Optional[Union[str, TransportConfig]] = None,
        policy: Optional[PolicyConfig] = None,
        safety: Optional[SafetyConfig] = None,
        inbound: Optional[InboundConfig] = None,
        outbound: Optional[OutboundConfig] = None,
        uat: Optional[UATConfig] = None,
        security: Optional[SecurityConfig] = None,
        token_store: Optional[TokenStore] = None,
        dedup_store: Any = None,
        safety_cache: Optional[ICache] = None,
        name_lookup: Optional[Callable[[List[str]], Any]] = None,
        config: Optional[ChannelConfig] = None,
    ) -> None:
        """Create a channel.

        ``dedup_store`` and ``safety_cache`` target **different** dedup
        layers — this is a deliberate split, not a redundancy:

        - ``dedup_store`` (:class:`~.normalize.dedup.DedupStore`) feeds the
          *pipeline* ``Deduper`` that catches webhook retries + WS reconnect
          backfill at the normalize step.
        - ``safety_cache`` (:class:`~lark_channel.core.cache.ICache`, usually
          Redis-backed) feeds the *safety* ``SeenCache`` used by
          :class:`SafetyPipeline` for pre-dispatch dedup + (with an atomic
          SETNX implementation) cross-process coherence.

        Both default to in-memory when left as ``None``.
        """
        cfg = config if config is not None else ChannelConfig()
        if app_id is not None:
            cfg.app_id = app_id
        if app_secret is not None:
            cfg.app_secret = app_secret
        if encrypt_key is not None:
            cfg.encrypt_key = encrypt_key
        if verification_token is not None:
            cfg.verification_token = verification_token
        if domain is not None:
            cfg.domain = domain
        if log_level is not None:
            cfg.log_level = log_level
        if policy is not None:
            cfg.policy = policy
        if safety is not None:
            cfg.safety = safety
        if inbound is not None:
            cfg.inbound = inbound
        if outbound is not None:
            cfg.outbound = outbound
        if uat is not None:
            cfg.uat = uat
        if security is not None:
            cfg.security = security
        if transport is not None:
            if isinstance(transport, str):
                if transport not in ("ws", "webhook"):
                    raise ValueError(
                        f"transport must be 'ws' or 'webhook', got {transport!r}"
                    )
                cfg.transport = TransportConfig(kind=transport)
            else:
                cfg.transport = transport
        if not cfg.app_id or not cfg.app_secret:
            raise ValueError("FeishuChannel requires app_id and app_secret")

        self._config = cfg
        self._safety_cache = safety_cache

        self._client = (
            Client.builder()
            .app_id(cfg.app_id)
            .app_secret(cfg.app_secret)
            .domain(cfg.domain)
            .log_level(cfg.log_level)
            # Required for `RequestOption.user_access_token` to have any effect
            # at all: with it off, `core.token.auth.verify` skips manual tokens
            # entirely, so a request declaring the user identity gets a freshly
            # minted tenant token instead and the user's authorization becomes
            # decoration. Additive — a request that carries no manual token
            # still mints from app credentials exactly as before.
            .enable_set_token(True)
            .timeout(cfg.transport.http_timeout_seconds)
            .proxy_url(cfg.transport.proxy_url)
            .trust_env_proxy(cfg.transport.trust_env_proxy)
            .build()
        )
        # Mark all HTTP requests originating from this capability layer with a
        # bare ``channel`` token in the User-Agent so the backend can attribute
        # traffic to the channel SDK.
        if self._client._config is not None:
            self._client._config.extra_ua_tags = ["channel"]
            self._client._config.security = cfg.security
        self._driver = LarkClientDriver(self._client)
        self._sender = OutboundSender(
            driver=self._driver.send_driver(),
            config=cfg.outbound,
            on_success=self._track_sent_message,
        )

        self._dedup_store = dedup_store or InMemoryDedupStore(
            max_entries=cfg.safety.dedup.max_entries
        )
        self._deduper = Deduper(
            store=self._dedup_store,
            ttl_seconds=cfg.safety.dedup.ttl_seconds,
            enabled=cfg.safety.dedup.enabled,
        )

        self._identity_resolver = IdentityResolver(
            lookup=name_lookup
            or (lambda ids: _api_helpers.default_name_lookup(self._client, ids)),
            cache=NameCache(cfg.inbound.name_cache),
        )
        self._chat_mode_cache = ChatModeCache(cfg.chat_mode_cache)
        # Per-chat roster (users + bots + observed mentions), shared by
        # get_chat_members/get_chat_bots, sender_name resolution and the
        # outbound "@name → open_id" normalization.
        self._chat_member_cache = ChatMemberCache()
        # In-flight roster fetches, keyed by (kind, chat_id, id_type), so a burst
        # of cold requests for the same roster collapses to one upstream call
        # (also limits concurrent hits on the shared token cache).
        self._roster_inflight: Dict[str, "asyncio.Future"] = {}
        self._media_cache = MediaResourceCache(cfg.media_cache)

        self._token_store: TokenStore = token_store or InMemoryTokenStore()
        self._device_flow = DeviceFlowClient(
            app_id=cfg.app_id,
            app_secret=cfg.app_secret,
            domain=cfg.domain,
        )

        self._pipeline = InboundPipeline(
            cfg=PipelineConfig(
                inbound=cfg.inbound, security=cfg.security, account_id=cfg.app_id
            ),
            deps=PipelineDeps(
                fetch_message=self._fetch_message_payload,
                resolve_names=self._identity_resolver.resolve_names,
                resolve_identity=self._identity_resolver.resolve,
            ),
            deduper=self._deduper,
        )
        self._pipeline.set_chat_mode_resolver(self.get_chat_mode)

        self._safety: Optional[SafetyPipeline] = None

        # String-event handlers. Multiple handlers per event name are appended
        # in registration order; `_invoke` iterates them all.
        self._handlers: Dict[str, List[EventHandler]] = {}

        self._dispatcher: Optional[EventDispatcherHandler] = None
        self._ws_client: Optional[WSClient] = None

        self._bot_identity: Optional[BotIdentity] = None
        self._bot_open_id: Optional[str] = None
        # Guards reads / writes to ``_bot_identity`` + ``_bot_open_id`` +
        # ``_safety.set_bot_open_id`` so the initial fetch, an explicit
        # ``await resolve_bot_identity()``, and the background retry loop
        # can't leave the two fields in a half-updated state when a reader
        # on the bg loop is inspecting them.
        self._bot_identity_lock = threading.Lock()
        self._bot_identity_retry_future: Optional[concurrent.futures.Future] = None

        self._sent_messages: "OrderedDict[str, float]" = OrderedDict()
        self._sent_message_context: "OrderedDict[str, _SentMessageContext]" = OrderedDict()
        self._sent_messages_max = 2048
        self._start_future: Optional[asyncio.Future] = None

        self._bg_loop: Optional[asyncio.AbstractEventLoop] = None
        self._bg_thread: Optional[threading.Thread] = None
        # Guards start/stop of the bg loop so concurrent connect() calls can't
        # spawn two loops + two threads.
        self._bg_lock = threading.Lock()

        # References to Futures returned by `schedule(...)` — kept alive so
        # exceptions surface (done callback logs) and so shutdown can drain /
        # cancel in-flight work.
        self._bg_tasks: Set[concurrent.futures.Future] = set()
        self._bg_tasks_lock = threading.Lock()

        self._shutdown = threading.Event()
        self._raw_events = RawEventRegistry(
            schedule=self.schedule, report=self._report_raw_error
        )
        self._meeting = _meeting_registry.MeetingChannel(
            client=self._client,
            config=self._config,
            # A dedicated cache: platform event ids are global, so sharing the
            # message layer's key space would let one layer's mark swallow the
            # other layer's event.
            seen=SeenCache(
                cache=safety_cache,
                namespace=_meeting_dedup.NAMESPACE,
            ),
            bot_open_id_getter=self._meeting_bot_open_id,
            schedule=self.schedule,
            resolve_ticket_interactive=self._meeting_ticket_interactive,
            resolve_ticket_quiet=self._meeting_ticket_quiet,
            emit_invited=self._emit_meeting_invited,
            timeout_seconds=self._config.transport.http_timeout_seconds or 30.0,
        )
        self._stop_requested = False
        self._started = False
        self._lifecycle_lock = threading.Lock()
        self._lifecycle_generation = 0
        self._background_generation = 0
        # Lazy-init: asyncio.Event() in older Python (<3.10) requires a
        # running loop; create on first access from a coroutine context.
        self._ready_event: Optional[asyncio.Event] = None
        self._ready_flag: bool = False
        self._connection_state: str = "idle"
        self._connection_reconnect_attempts: int = 0
        self._connection_last_connected_at: Optional[float] = None
        self._connection_last_disconnected_at: Optional[float] = None
        self._connection_last_error_at: Optional[float] = None
        self._connection_last_error: Optional[str] = None
        self._keepalive_watchdog: Optional[KeepaliveWatchdog] = None
        self._keepalive_future: Optional[concurrent.futures.Future] = None

    # ------------------------------------------------------------------
    # Event registration
    # ------------------------------------------------------------------
    def on(
        self,
        name_or_map: Union[ChannelEventName, Dict[ChannelEventName, EventHandler]],
        handler: Optional[EventHandler] = None,
    ) -> Unsubscribe:
        """Register an event handler.

        Accepts ``on(event_name, handler)`` or ``on({event_name: handler})``.
        Returns an unsubscribe callable that pops exactly this handler from
        the list.

        ``name_or_map`` is type-hinted with :data:`.events.ChannelEventName`
        so static type checkers catch typos (e.g. ``"messageReceive"`` vs
        the correct ``"message"``). At runtime, unknown names produce a
        warning log but do not raise — several historical aliases are still
        accepted via :mod:`._coerce`.
        """
        if isinstance(name_or_map, dict):
            subs = [self._register_single(k, v) for k, v in name_or_map.items() if v]
            return lambda: [u() for u in subs]
        if handler is None:
            raise TypeError(
                "FeishuChannel.on expects a handler when called with an event name"
            )
        return self._register_single(name_or_map, handler)

    def _register_single(self, name: str, handler: EventHandler) -> Unsubscribe:
        normalized = _coerce.normalize_event_name(name)
        if normalized not in _coerce.VALID_EVENTS:
            logger.warning("FeishuChannel.on: unknown event %r", name)
        self._handlers.setdefault(normalized, []).append(handler)

        def unsubscribe() -> None:
            lst = self._handlers.get(normalized)
            if not lst:
                return
            try:
                lst.remove(handler)
            except ValueError:
                return
            if not lst:
                self._handlers.pop(normalized, None)

        return unsubscribe

    async def _invoke(self, name: str, *args) -> None:
        handlers = self._handlers.get(_coerce.normalize_event_name(name))
        if not handlers:
            return
        # Iterate a snapshot so handlers unsubscribing themselves mid-invoke
        # don't mutate the list we're walking.
        for handler in list(handlers):
            try:
                result = handler(*args)
                if inspect.isawaitable(result):
                    await result
            except Exception as e:
                logger.exception(
                    "FeishuChannel: handler for %r raised: %s", name, e
                )
                for err in self._handlers.get("error", []):
                    try:
                        res = err(e)
                        if inspect.isawaitable(res):
                            await res
                    except Exception:  # pragma: no cover
                        pass

    # ------------------------------------------------------------------
    # Properties / escape hatches
    # ------------------------------------------------------------------
    @property
    def client(self) -> Client:
        """The underlying OpenAPI ``Client``."""
        return self._client

    @property
    def ws_client(self) -> Optional[WSClient]:
        """The WebSocket client if the channel was started in ``ws`` mode."""
        return self._ws_client

    @property
    def bot_identity(self) -> Optional[BotIdentity]:
        """Currently-resolved bot identity, or ``None`` if not yet fetched.

        Read under the same lock that guards writes so callers never observe
        a half-updated state (``_bot_identity`` fresh but ``_bot_open_id``
        stale or vice-versa).
        """
        with self._bot_identity_lock:
            return self._bot_identity

    def get_bot_identity(self) -> BotIdentity:
        """This bot's own identity (:class:`BotIdentity`) — e.g. to inline into
        an agent's system prompt so it can tell itself apart from other bots and
        decide whom to reply to.

        Resolved during :meth:`connect`. Raises
        :class:`FeishuChannelError` with code ``not_connected`` if called before
        the identity is available, rather than returning ``None``, so callers
        don't silently build a prompt with a missing identity. Use the
        :attr:`bot_identity` property when a ``None`` sentinel is preferred.
        """
        identity = self.bot_identity
        if identity is None:
            raise FeishuChannelError(
                FeishuChannelErrorCode.NOT_CONNECTED,
                "bot identity not resolved yet — call connect() first",
            )
        return identity

    @property
    def config(self) -> ChannelConfig:
        return self._config

    @property
    def safety(self) -> Optional[SafetyPipeline]:
        return self._safety

    @property
    def sender(self) -> OutboundSender:
        return self._sender

    @property
    def driver(self) -> LarkClientDriver:
        return self._driver

    @property
    def dispatcher(self) -> EventDispatcherHandler:
        if self._dispatcher is None:
            self._dispatcher = self._build_dispatcher()
        return self._dispatcher

    async def handle_webhook_request(
        self,
        headers: "Mapping[str, str]",
        body: bytes,
    ) -> "tuple[int, bytes]":
        """Process a single inbound webhook request.

        This is the framework-agnostic entry point — wrap with aiohttp /
        starlette / fastapi / your favorite web layer. The SDK does not ship
        a built-in webhook server; rate limiting, anomaly tracking, and IP
        allowlisting are deployment concerns that live in your service.

        Behavior:

        - Decrypts the body using ``encrypt_key`` (if configured).
        - Verifies the request via ``verification_token`` (if configured).
        - Verifies the request signature against the headers (when
          ``encrypt_key`` is set, the dispatcher checks
          ``X-Lark-Request-Timestamp`` / ``X-Lark-Request-Nonce`` /
          ``X-Lark-Signature``).
        - Routes the event to your registered ``channel.on(...)`` handlers.

        Args:
            headers: HTTP request headers (typically a dict-like; case
                sensitivity follows your framework's conventions).
            body: Raw HTTP request body bytes.

        Returns:
            ``(status_code, response_body_bytes)`` — write the body straight
            back to your HTTP response.

        Raises:
            FeishuChannelError(NOT_CONNECTED): When called before ``start()``.
                The dispatcher converts bad-token / signature failures to a
                500 response body itself; this method does not raise on those.
        """
        if self._dispatcher is None:
            raise FeishuChannelError(
                FeishuChannelErrorCode.NOT_CONNECTED,
                "handle_webhook_request called before start() — dispatcher missing",
            )

        from lark_channel.core.model.raw_request import RawRequest
        req = RawRequest()
        req.uri = "/webhook"
        req.headers = dict(headers) if headers else {}
        req.body = body

        # The dispatcher's `do` is sync; offload so we don't block the
        # caller's event loop on signature verification + JSON parse.
        loop = asyncio.get_running_loop()
        resp = await loop.run_in_executor(None, self._dispatcher.do, req)
        status = getattr(resp, "status_code", 200) or 200
        content = getattr(resp, "content", b"")
        if isinstance(content, str):
            content = content.encode("utf-8")
        if not isinstance(content, (bytes, bytearray)):
            content = bytes(content) if content is not None else b""
        return status, content

    def get_policy(self) -> PolicyConfig:
        return self._config.policy

    def update_policy(self, **changes) -> None:
        if self._safety is not None:
            self._safety.update_policy(**changes)
        for k, v in changes.items():
            if hasattr(self._config.policy, k):
                setattr(self._config.policy, k, v)

    # ------------------------------------------------------------------
    # Readiness
    # ------------------------------------------------------------------
    @property
    def is_ready(self) -> bool:
        """True after start() has fully initialized.

        Specifically: WS connect has succeeded (or webhook dispatcher is
        constructed), bot identity is resolved (or its retry loop is running),
        and the safety pipeline is initialized. Events received before this
        point are NOT dispatched to user handlers — they are dropped (the
        WS-reconnect backfill or webhook retries cover startup-window losses).
        """
        return self._ready_flag

    async def wait_ready(self, *, timeout: Optional[float] = None) -> None:
        """Block until is_ready turns True. Raises asyncio.TimeoutError on timeout."""
        ev = self._ensure_ready_event()
        if self._ready_flag:
            return
        if timeout is None:
            await ev.wait()
            return
        await asyncio.wait_for(ev.wait(), timeout=timeout)

    def _mark_ready(self) -> None:
        """Internal: flip the readiness event. Called at the end of start()
        after WS connect, bot identity fetch, and safety pipeline init.
        Idempotent.
        """
        self._ready_flag = True
        self._connection_state = "connected"
        self._connection_last_connected_at = time.time()
        ev = self._ready_event
        if ev is not None:
            try:
                ev.set()
            except Exception:  # pragma: no cover
                pass
        # One of the three reconciliation points: a seat stranded by an
        # inconclusive departure before the connection dropped is exactly what a
        # fresh connection wants back.
        try:
            self._meeting.on_connected()
        except Exception as e:  # pragma: no cover - never block readiness
            logger.debug("channel: meeting reconcile on connect skipped: %s", e)

    def _ensure_ready_event(self) -> "asyncio.Event":
        """Lazily create the asyncio.Event so we don't need a running loop in __init__."""
        if self._ready_event is None:
            self._ready_event = asyncio.Event()
            if self._ready_flag:
                self._ready_event.set()
        return self._ready_event

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------
    async def connect(self) -> None:
        """Idempotent. Blocks until WS connects or Webhook dispatcher is ready."""
        if getattr(self, "_started", False):
            return
        await asyncio.get_running_loop().run_in_executor(None, self.start)

    async def start_background(self, *, timeout: Optional[float] = 30.0) -> None:
        """Start transport in the background and return once it is ready.

        ``connect()`` keeps the historical foreground/blocking WebSocket
        behavior. Async applications that need startup to continue after the
        WebSocket handshake should use this method instead.
        """
        if self._ready_flag:
            return
        loop = asyncio.get_running_loop()
        if self._start_future is None or self._start_future.done():
            with self._lifecycle_lock:
                self._stop_requested = False
                self._background_generation += 1
                background_generation = self._background_generation
            self._start_future = loop.run_in_executor(None, self.start)
        else:
            with self._lifecycle_lock:
                background_generation = self._background_generation
        await self._wait_background_start_ready(
            timeout=timeout,
            generation=background_generation,
        )

    async def connect_until_ready(self, *, timeout: Optional[float] = 30.0) -> None:
        """Alias for :meth:`start_background` with explicit ready semantics."""
        await self.start_background(timeout=timeout)

    async def stop_background(self) -> None:
        """Stop a channel started by :meth:`start_background`.

        This is equivalent to :meth:`disconnect`; it is provided as the
        lifecycle counterpart to ``start_background``.
        """
        await self.disconnect()

    async def _wait_background_start_ready(
        self,
        *,
        timeout: Optional[float],
        generation: int,
    ) -> None:
        loop = asyncio.get_running_loop()
        deadline = None if timeout is None else loop.time() + timeout
        while True:
            if self._ready_flag:
                return
            if generation != self._background_generation:
                return
            if self._stop_requested:
                return
            fut = self._start_future
            if fut is None:
                await asyncio.sleep(0.05)
                continue
            if fut is not None and fut.done():
                if fut.cancelled():
                    if generation != self._background_generation or self._stop_requested:
                        return
                    raise FeishuChannelError(
                        FeishuChannelErrorCode.NOT_CONNECTED,
                        "Channel background start was cancelled",
                    )
                exc = fut.exception()
                if exc is not None:
                    raise exc
                if self._ready_flag or self._config.transport.kind == "webhook":
                    return
                raise FeishuChannelError(
                    FeishuChannelErrorCode.NOT_CONNECTED,
                    "Channel start exited before transport became ready",
                )
            ws = self._ws_client
            if ws is not None and getattr(ws, "_conn", None) is not None:
                self._mark_ready()
                return
            if deadline is not None and loop.time() >= deadline:
                self.stop()
                raise FeishuChannelError(
                    FeishuChannelErrorCode.NOT_CONNECTED,
                    "Timed out waiting for channel transport readiness",
                )
            await asyncio.sleep(0.05)

    async def disconnect(self) -> None:
        """Gracefully drain safety pipeline batches + stop the WS loop.

        Meeting sessions are **disposed, not left**: a reconnect must not make
        the bot walk out of every meeting it is in. The bot therefore stays a
        participant, which is why those seats stay counted and why a process
        that is really exiting should ``leave()`` first.
        """
        await self._dispose_meeting_sessions()
        if self._safety is not None:
            try:
                await self._safety.dispose()
            except Exception:  # pragma: no cover
                pass
        self.stop()

    def start(self) -> None:
        """Start WS (blocking) or return after initializing Webhook dispatcher.

        Transport-level failures (bad credentials, network unreachable, TLS
        handshake failure, ...) are wrapped into
        :class:`FeishuChannelError(NOT_CONNECTED)` with the original
        exception chained via ``__cause__``. This gives callers a stable,
        ``code``-bearing exception to match on instead of whatever raw
        exception the underlying transport happens to raise (e.g.
        ``lark_channel.ws.client.ClientException`` whose ``.code`` is an int).
        """
        if self._started:
            return
        with self._lifecycle_lock:
            self._lifecycle_generation += 1
            generation = self._lifecycle_generation
            self._stop_requested = False
            self._started = True
        self._ensure_bg_loop()
        self._fetch_bot_identity_sync()
        self._dispatcher = self._build_dispatcher()
        if self._config.transport.kind == "webhook":
            with self._lifecycle_lock:
                if not self._is_active_start(generation):
                    return
                logger.info(
                    "FeishuChannel: webhook mode ready — pass `dispatcher` to your HTTP adaptor"
                )
                self._mark_ready()
            return
        with self._lifecycle_lock:
            if not self._is_active_start(generation):
                return
            self._ws_client = WSClient(
                self._config.app_id,
                self._config.app_secret,
                log_level=self._config.log_level,
                event_handler=self._dispatcher,
                domain=self._config.domain,
                auto_reconnect=self._config.transport.auto_reconnect,
                extra_ua_tags=["channel"],
                headers=self._config.transport.headers,
                proxy_url=self._config.transport.proxy_url,
                trust_env_proxy=self._config.transport.trust_env_proxy,
                handshake_timeout=self._config.transport.handshake_timeout_seconds,
                security=self._config.security,
            )
            # Wire transport-level reconnect events to the public ``on()`` bus so
            # callers registering ``on("reconnecting", ...) / on("reconnected", ...)``
            # actually observe them.
            self._ws_client.on_reconnecting = self._notify_reconnecting
            self._ws_client.on_reconnected = self._notify_reconnected
            self._start_keepalive_watchdog()
        try:
            self._ws_client.start()
        except FeishuChannelError:
            # Already the right shape; just reset started so caller can retry.
            self._cleanup_failed_start(generation)
            raise
        except Exception as e:
            if not self._is_active_start(generation):
                self._cleanup_failed_start(generation)
                return
            # Anything else (ws client's ClientException, timeouts, DNS, ...)
            # → typed NOT_CONNECTED so callers can ``except FeishuChannelError
            # as err: if err.code == FeishuChannelErrorCode.NOT_CONNECTED``.
            self._record_connection_error(e)
            self._cleanup_failed_start(generation)
            raise FeishuChannelError(
                FeishuChannelErrorCode.NOT_CONNECTED,
                f"WebSocket connect failed: {e}",
            ) from e
        with self._lifecycle_lock:
            if not self._is_active_start(generation):
                self._stop_keepalive_watchdog()
                return
            self._mark_ready()

    def _is_active_start(self, generation: int) -> bool:
        if (
            generation != self._lifecycle_generation
            or self._shutdown.is_set()
            or self._stop_requested
        ):
            if generation == self._lifecycle_generation:
                self._started = False
            return False
        return True

    def _cleanup_failed_start(self, generation: int) -> None:
        self._stop_keepalive_watchdog()
        ws = self._ws_client
        if ws is not None:
            stopped = False
            for meth in ("stop", "close", "disconnect"):
                fn = getattr(ws, meth, None)
                if callable(fn):
                    try:
                        fn()
                    except Exception as e:  # pragma: no cover
                        logger.warning(
                            "FeishuChannel.start: ws.%s raised during cleanup: %s",
                            meth,
                            e,
                        )
                    stopped = True
                    break
            if not stopped:
                self._stop_private_ws_client(ws)
        self._cancel_bg_tasks()
        self._stop_bg_loop(join_timeout=2.0)
        with self._lifecycle_lock:
            if generation == self._lifecycle_generation:
                self._started = False
                self._ready_flag = False
                self._connection_state = "error"
                self._ws_client = None
                self._safety = None

    def stop(self, *, join_timeout: float = 5.0) -> None:
        """Tear down everything the channel owns.

        Safe to call from any thread, idempotent. Steps:

        1. Signal shutdown (sets ``self._shutdown``).
        2. Stop the WS client if one was created.
        3. Dispose meeting sessions so their tasks stop before the loop does.
        4. Cancel in-flight futures returned from :meth:`schedule`.
        5. Run ``DeviceFlowClient.close()`` on the bg loop to release httpx.
        6. Stop the bg loop and join its thread.
        """
        if self._shutdown.is_set():
            return
        self._shutdown.set()
        # Before the loop goes away: a session owns tasks and timers created
        # directly on it, which `_cancel_bg_tasks` does not know about. Left
        # running they are destroyed mid-flight when the loop closes, and the
        # resulting "Task was destroyed but it is pending" lands wherever the
        # process happens to be logging at the time.
        self._dispose_meeting_sessions_blocking()
        if self._start_future is not None:
            try:
                self._start_future.cancel()
            except Exception:  # pragma: no cover
                pass
        self._stop_keepalive_watchdog()

        # 1. Stop WS client (best-effort; some builds don't expose stop()).
        with self._lifecycle_lock:
            self._background_generation += 1
            self._lifecycle_generation += 1
            self._stop_requested = True
            ws = self._ws_client
        if ws is not None:
            stopped = False
            for meth in ("stop", "close", "disconnect"):
                fn = getattr(ws, meth, None)
                if callable(fn):
                    try:
                        fn()
                    except Exception as e:  # pragma: no cover
                        logger.warning("FeishuChannel.stop: ws.%s raised: %s", meth, e)
                    stopped = True
                    break
            if not stopped:
                self._stop_private_ws_client(ws)

        # 2. Cancel scheduled futures.
        self._cancel_bg_tasks()

        # 3. Close httpx client inside device_flow via the bg loop (if up).
        if self._bg_loop is not None and self._bg_loop.is_running():
            try:
                close_fut = asyncio.run_coroutine_threadsafe(
                    self._device_flow.close(), self._bg_loop
                )
                try:
                    close_fut.result(timeout=2.0)
                except (concurrent.futures.TimeoutError, Exception) as e:  # pragma: no cover
                    logger.warning("FeishuChannel.stop: device_flow.close timed out: %s", e)
            except RuntimeError:  # pragma: no cover - loop closed between checks
                pass

        # 4. Stop the bg loop + join thread.
        self._stop_bg_loop(join_timeout=join_timeout)
        self._ws_client = None
        self._start_future = None

        # Allow a subsequent connect()/start() to actually run. Without
        # clearing these two flags, a channel that was stopped would be
        # permanently inert — any future connect() would short-circuit on
        # ``self._started`` and ``_ensure_bg_loop`` would refuse on the
        # persisted ``_shutdown``.
        self._shutdown.clear()
        self._started = False
        self._ready_flag = False
        self._connection_state = "idle"
        self._connection_last_disconnected_at = time.time()
        if self._ready_event is not None:
            try:
                self._ready_event.clear()
            except Exception:  # pragma: no cover
                pass
        with self._bg_tasks_lock:
            self._bg_tasks.clear()

    def _stop_private_ws_client(self, ws: Any) -> None:
        disconnect = getattr(ws, "_disconnect", None)
        try:
            from lark_channel.ws import client as ws_client_module

            ws_loop = getattr(ws_client_module, "loop", None)
            if callable(disconnect) and ws_loop is not None:
                if ws_loop.is_running():
                    try:
                        running_loop = asyncio.get_running_loop()
                    except RuntimeError:
                        running_loop = None
                    if running_loop is ws_loop:
                        ws_loop.create_task(disconnect())
                    else:
                        fut = asyncio.run_coroutine_threadsafe(disconnect(), ws_loop)
                        try:
                            fut.result(timeout=2.0)
                        except Exception as e:  # pragma: no cover
                            logger.warning("FeishuChannel.stop: ws._disconnect timed out: %s", e)
                elif not ws_loop.is_closed():
                    ws_loop.run_until_complete(disconnect())
            if ws_loop is not None and ws_loop.is_running():
                ws_loop.call_soon_threadsafe(ws_loop.stop)
        except Exception as e:  # pragma: no cover
            logger.warning("FeishuChannel.stop: private ws shutdown raised: %s", e)

    def schedule(self, coro) -> "concurrent.futures.Future":
        """Submit a coroutine to the background loop; safe from any thread.

        The returned Future is also tracked internally so exceptions surface
        via a logging done-callback (no more fire-and-forget) and so
        :meth:`stop` can cancel in-flight work. Callers may still ignore the
        return value.
        """
        self._ensure_bg_loop()
        if self._bg_loop is None:
            raise RuntimeError("FeishuChannel: background loop is not running")
        try:
            fut = asyncio.run_coroutine_threadsafe(coro, self._bg_loop)
        except RuntimeError as e:  # pragma: no cover - loop closed
            logger.warning("FeishuChannel: schedule failed: %s", e)
            raise
        with self._bg_tasks_lock:
            self._bg_tasks.add(fut)

        def _done(f: "concurrent.futures.Future") -> None:
            with self._bg_tasks_lock:
                self._bg_tasks.discard(f)
            if f.cancelled():
                return
            exc = f.exception()
            if exc is not None:
                logger.exception(
                    "FeishuChannel.schedule: background task raised: %s", exc,
                    exc_info=exc,
                )

        fut.add_done_callback(_done)
        return fut

    def _call_safety_threadsafe(self, method_name: str, *args) -> None:
        self._ensure_bg_loop()
        loop = self._bg_loop
        if loop is None or getattr(loop, "is_closed", lambda: False)():
            raise RuntimeError("FeishuChannel: background loop is not running")
        try:
            running_loop = asyncio.get_running_loop()
        except RuntimeError:
            running_loop = None
        safety = self._safety
        if safety is None:
            if running_loop is loop:
                raise RuntimeError("FeishuChannel: safety pipeline is not running")
            for _ in range(50):
                time.sleep(0.01)
                safety = self._safety
                if safety is not None:
                    break
        if safety is None:
            raise RuntimeError("FeishuChannel: safety pipeline is not running")
        fn = getattr(safety, method_name)
        try:
            if running_loop is loop:
                fn(*args)
            elif loop.is_running():
                loop.call_soon_threadsafe(fn, *args)
            else:
                fn(*args)
        except RuntimeError as e:
            raise RuntimeError("FeishuChannel: background loop is not running") from e

    def block_batch_scope(self, scope: str) -> None:
        self._call_safety_threadsafe("block_batch_scope", scope)

    def unblock_batch_scope(self, scope: str) -> None:
        self._call_safety_threadsafe("unblock_batch_scope", scope)

    def cancel_batch_scope(self, scope: str) -> None:
        self._call_safety_threadsafe("cancel_batch_scope", scope)

    def _start_keepalive_watchdog(self) -> None:
        keepalive_cfg = self._config.transport.keepalive
        ws = self._ws_client
        if not keepalive_cfg.enabled or ws is None:
            return
        if self._keepalive_future is not None and not self._keepalive_future.done():
            return
        self._keepalive_watchdog = KeepaliveWatchdog(
            config=keepalive_cfg,
            probe=lambda: ws.probe_endpoint(timeout=keepalive_cfg.probe_timeout_seconds),
            reconnect=lambda: ws.request_reconnect(),
        )
        self._keepalive_future = self.schedule(self._keepalive_loop())

    def _stop_keepalive_watchdog(self) -> None:
        future = self._keepalive_future
        self._keepalive_watchdog = None
        self._keepalive_future = None
        if future is None:
            return
        try:
            future.cancel()
        except Exception:  # pragma: no cover
            pass

    async def _keepalive_loop(self) -> None:
        while not self._shutdown.is_set():
            cfg = self._config.transport.keepalive
            await asyncio.sleep(cfg.check_interval_seconds)
            watchdog = self._keepalive_watchdog
            if watchdog is not None:
                await watchdog.run_once()

    # Backoff schedule used by :meth:`_start_bot_identity_retry_loop`.
    # Covers roughly 1h 15m of retries: 10s, 30s, 2m, 10m, 1h. After that we
    # give up and log once at ERROR; a caller can always invoke
    # :meth:`resolve_bot_identity` manually to try again.
    _BOT_IDENTITY_RETRY_DELAYS_S = (10, 30, 120, 600, 3600)

    def _store_bot_identity(self, identity: Optional[BotIdentity]) -> None:
        """Atomically swap in a fresh bot identity + propagate to safety."""
        with self._bot_identity_lock:
            self._bot_identity = identity
            self._bot_open_id = identity.open_id if identity is not None else None
            self._pipeline.set_bot_open_id(self._bot_open_id)
            safety = self._safety
            if safety is not None and identity is not None:
                safety.set_bot_open_id(identity.open_id)

    async def resolve_bot_identity(self) -> Optional[BotIdentity]:
        """Fetch the bot identity + publish it to readers under a lock.

        Safe to call from any loop. Returns the resolved identity (or None if
        the upstream call failed — callers that need the identity should
        react to None by retrying later).
        """
        identity = await fetch_bot_identity(self._client.config)
        if identity is not None:
            self._store_bot_identity(identity)
        return identity

    def _fetch_bot_identity_sync(self) -> None:
        if self._bg_loop is None:
            raise RuntimeError("FeishuChannel: background loop is not running")
        fut = asyncio.run_coroutine_threadsafe(
            fetch_bot_identity(self._client.config), self._bg_loop
        )
        try:
            identity = fut.result(timeout=10)
        except Exception as e:
            logger.warning("FeishuChannel: bot identity fetch failed: %s", e)
            identity = None
        if identity is None:
            # A transient network hiccup at startup can leave group ``@Bot``
            # detection unavailable until identity resolves. Schedule a
            # background backoff retry loop instead of treating startup lookup
            # as the only chance to populate ``_bot_open_id``.
            logger.warning(
                "FeishuChannel: bot identity unresolved on startup — "
                "scheduling background retry (group @Bot detection will "
                "remain off until resolved)"
            )
            self._start_bot_identity_retry_loop()
            return
        self._store_bot_identity(identity)
        logger.info(
            "FeishuChannel: bot identity resolved — open_id=%s name=%s",
            identity.open_id, identity.name,
        )

    def _start_bot_identity_retry_loop(self) -> None:
        """Schedule a backoff retry task on the bg loop."""
        future = self._bot_identity_retry_future
        if future is not None and not future.done():
            return
        # `run_coroutine_threadsafe` accepts a callback for a loop that has
        # stopped but is not yet closed, and that callback never runs — leaving
        # the coroutine built below unawaited, and a future here that never
        # completes. Retrying on a loop that is not running has nothing to do
        # anyway, so check before building anything.
        if self._bg_loop is None or not self._bg_loop.is_running():
            return
        coro = self._bot_identity_retry_loop()
        try:
            self._bot_identity_retry_future = self.schedule(coro)
        except RuntimeError:  # pragma: no cover - loop already stopped
            coro.close()
            pass

    def _cancel_bg_tasks(self) -> None:
        with self._bg_tasks_lock:
            pending = list(self._bg_tasks)
        retry_future = self._bot_identity_retry_future
        if retry_future is not None and retry_future not in pending:
            pending.append(retry_future)
        for fut in pending:
            try:
                fut.cancel()
            except Exception:  # pragma: no cover
                pass
        self._drain_cancelled_bg_tasks()

    @staticmethod
    def _sweep_bg_loop_tasks(loop: Any, *, timeout: float = 5.0) -> None:
        """Cancel every task left on ``loop`` and let the cancellations land.

        `timeout` bounds how long cancellations are given to converge. It is
        generous because overshooting only costs a slow shutdown, while
        undershooting downgrades "every task converged" to "some tasks were
        abandoned" on a loaded machine — which surfaces later as "Task was
        destroyed but it is pending" from whatever happens to be running then,
        the least useful place to see it.

        The bound is enforced *inside* the loop rather than by waiting across
        threads, because a task that never accepts its cancellation must not be
        able to strand this sweep. Enforcing it from here instead would leave
        the sweep's own task pending on a loop about to be stopped — the exact
        residue this function exists to prevent.
        """

        async def _sweep() -> None:
            current = asyncio.current_task()
            pending = [
                task
                for task in asyncio.all_tasks()
                if task is not current and not task.done()
            ]
            for task in pending:
                task.cancel()
            if pending:
                # `wait`, not `gather`: it returns when the budget runs out
                # instead of hanging on whatever refuses to converge, so this
                # coroutine always finishes and can never become the orphan.
                await asyncio.wait(pending, timeout=timeout)

        if not loop.is_running():
            return

        # Deliberately not `run_coroutine_threadsafe`: it ties the task to a
        # `concurrent.futures.Future` through `_chain_future`, whose cancel
        # callback re-enters `loop.call_soon_threadsafe`. During shutdown the
        # loop is usually closed by then, and that raises *inside* a
        # `concurrent.futures` callback — which that module logs itself, out of
        # reach of any `try` here. Scheduling by hand keeps both ownership of
        # the coroutine and delivery of the cancellation in code we control.
        state = {}
        finished = threading.Event()

        def _schedule() -> None:
            # Built here, on the loop's own thread, so a callback the loop
            # never gets around to running leaves no coroutine behind.
            coro = _sweep()
            try:
                task = loop.create_task(coro)
            except RuntimeError:
                # A loop that is already closing refuses new tasks, and the
                # coroutine is still ours at that point.
                coro.close()
                finished.set()
                return
            state["task"] = task
            task.add_done_callback(lambda _task: finished.set())

        try:
            loop.call_soon_threadsafe(_schedule)
        except RuntimeError:
            return  # the loop closed; nothing was scheduled and nothing leaked

        # The sweep bounds itself by `timeout`, so this only expires when the
        # loop stops servicing callbacks at all. One extra second covers the
        # scheduling hop rather than racing it.
        if finished.wait(timeout + 1.0):
            return

        task = state.get("task")
        if task is None:
            return  # the loop never ran `_schedule`, so there is no task
        logger.debug(
            "FeishuChannel.stop: bg loop stopped servicing callbacks within "
            "%.1fs; cancelling the sweep",
            timeout + 1.0,
        )
        try:
            loop.call_soon_threadsafe(task.cancel)
        except RuntimeError:
            pass  # the loop closed, taking the task with it

    def _stop_bg_loop(self, *, join_timeout: float) -> None:
        loop = self._bg_loop
        thread = self._bg_thread
        if loop is not None:
            # Anything still pending on this loop is about to be destroyed
            # mid-flight, which asyncio reports as "Task was destroyed but it is
            # pending". Collaborators create tasks directly on the loop
            # (delivery queues, polling loops), so `_cancel_bg_tasks` does not
            # see them. Cancel whatever is left and give it one turn.
            self._sweep_bg_loop_tasks(loop)
        if loop is not None:
            try:
                loop.call_soon_threadsafe(loop.stop)
            except RuntimeError:  # pragma: no cover - already stopped
                pass
        if (
            thread is not None
            and thread.is_alive()
            and thread is not threading.current_thread()
        ):
            thread.join(timeout=join_timeout)
            if thread.is_alive():  # pragma: no cover
                logger.warning(
                    "FeishuChannel.stop: bg thread did not exit within %.1fs",
                    join_timeout,
                )
        self._bg_loop = None
        self._bg_thread = None

    async def _bot_identity_retry_loop(self) -> None:
        """Retry ``fetch_bot_identity`` on a backoff schedule until it
        succeeds, shutdown fires, or we exhaust the delay table."""
        for delay_s in self._BOT_IDENTITY_RETRY_DELAYS_S:
            try:
                await asyncio.sleep(delay_s)
            except asyncio.CancelledError:
                raise
            if self._shutdown.is_set():
                return
            # Someone else (manual resolve_bot_identity call) may have
            # already filled it in — bail out without a pointless fetch.
            with self._bot_identity_lock:
                if self._bot_identity is not None:
                    return
            try:
                identity = await fetch_bot_identity(self._client.config)
            except Exception as e:
                logger.warning(
                    "FeishuChannel: bot identity retry failed (delay=%ds): %s",
                    delay_s, e,
                )
                continue
            if identity is not None:
                self._store_bot_identity(identity)
                logger.info(
                    "FeishuChannel: bot identity resolved on retry — open_id=%s",
                    identity.open_id,
                )
                return
        logger.error(
            "FeishuChannel: bot identity still unresolved after all retries — "
            "group @Bot detection is disabled for this process; call "
            "channel.resolve_bot_identity() manually to try again"
        )

    def _drain_cancelled_bg_tasks(self) -> None:
        loop = self._bg_loop
        if loop is None or not loop.is_running():
            return
        try:
            drain_fut = asyncio.run_coroutine_threadsafe(asyncio.sleep(0), loop)
            drain_fut.result(timeout=0.5)
        except Exception:  # pragma: no cover
            pass

    def _ensure_bg_loop(self) -> None:
        # Double-checked locking: the outer check avoids the lock in the hot
        # path, the inner check prevents a race between two concurrent
        # callers both seeing None and each spawning their own loop+thread.
        if self._bg_loop is not None:
            return
        with self._bg_lock:
            if self._bg_loop is not None:
                return
            if self._shutdown.is_set():
                raise RuntimeError(
                    "FeishuChannel._ensure_bg_loop: channel is shutting down"
                )
            loop = asyncio.new_event_loop()

            def _runner() -> None:
                asyncio.set_event_loop(loop)
                try:
                    loop.run_forever()
                finally:
                    try:
                        loop.close()
                    except Exception:  # pragma: no cover
                        pass

            t = threading.Thread(target=_runner, name="lark-channel-bg", daemon=True)
            t.start()
            self._bg_loop = loop
            self._bg_thread = t

        async def _build_safety():
            safety_cfg = self._config.safety
            return SafetyPipeline(
                loop=self._bg_loop,
                on_message=self._dispatch_inbound_to_user,
                on_reject=self._emit_reject,
                policy=self._config.policy,
                # Safety-layer dedup cache (ICache, usually Redis) — wired
                # explicitly from the constructor kwarg, independent of the
                # pipeline-layer DedupStore that feeds `self._deduper`.
                cache=self._safety_cache,
                dedup_config=safety_cfg.dedup,
                batch_config=safety_cfg.text_batch,
                media_batch_config=safety_cfg.media_batch,
                queue_config=safety_cfg.chat_queue,
                stale_window_ms=safety_cfg.stale_message_window_ms,
                drop_self_sent=self._config.inbound.drop_self_sent,
            )

        fut = asyncio.run_coroutine_threadsafe(_build_safety(), self._bg_loop)
        self._safety = fut.result(timeout=5)
        with self._bot_identity_lock:
            open_id = self._bot_open_id
        if open_id:
            self._safety.set_bot_open_id(open_id)

    # ------------------------------------------------------------------
    # Dispatcher
    # ------------------------------------------------------------------
    def _build_dispatcher(self) -> EventDispatcherHandler:
        b = EventDispatcherHandler.builder(
            self._config.encrypt_key or "",
            self._config.verification_token or "",
            self._config.log_level,
            security=self._config.security,
        )
        b = b.register_p2_im_message_receive_v1(self._on_p2_im_message_receive_v1)
        b = b.register_p2_card_action_trigger(self._on_p2_card_action_trigger)
        for register, handler in (
            ("register_p2_im_message_reaction_created_v1", self._on_p2_reaction_created),
            ("register_p2_im_message_reaction_deleted_v1", self._on_p2_reaction_deleted),
            ("register_p2_im_chat_member_bot_added_v1", self._on_p2_bot_added),
            ("register_p2_im_chat_member_bot_deleted_v1", self._on_p2_bot_deleted),
            ("register_p2_im_message_message_read_v1", self._on_p2_message_read),
        ):
            try:
                b = getattr(b, register)(handler)
            except Exception:  # pragma: no cover
                pass
        # drive.notice.comment_add_v1 has no typed processor in the
        # generated SDK. The wire payload may arrive under either schema:
        # the legacy callback channel uses p1 (event has ``uuid``), but
        # the modern WS frontier wraps the same event in a p2 envelope
        # (``schema=2.0``). Register the customized-event handler under
        # both so neither entry point logs "processor not found".
        b = b.register_p1_customized_event(
            "drive.notice.comment_add_v1", self._on_p1_comment_add
        )
        b = b.register_p2_customized_event(
            "drive.notice.comment_add_v1", self._on_p1_comment_add
        )
        problems = self._register_meeting_events(b)
        # Raw subscriptions go on last, so they can compose with whatever the
        # built-ins above installed. The whole table is rebuilt on every
        # start(), so this replay is what keeps them alive across a restart.
        replay_problem = self._raw_events.apply(b)
        problem = problems or replay_problem
        self._meeting.mark_registration(ok=not problems, reason=problem)
        return b.build()

    def _register_meeting_events(self, builder) -> Optional[str]:
        """Install the three ``vc.bot.*`` processors. Returns a failure note.

        Not fatal: an application that has not declared these subscriptions in
        the developer console still wants its message path to start. The health
        readout is where the failure becomes visible.
        """
        wiring = (
            (_meeting_registry.ACTIVITY_EVENT, self._meeting.on_activity),
            (_meeting_registry.INVITED_EVENT, self._meeting.on_invited),
            (_meeting_registry.ENDED_EVENT, self._meeting.on_ended),
        )
        for event_type, handler in wiring:
            try:
                builder.register_p2_customized_event(
                    event_type, _meeting_payload(handler)
                )
            except Exception as e:
                logger.warning(
                    "channel: could not subscribe to %s (%s)",
                    event_type,
                    type(e).__name__,
                )
                return "%s: %s" % (event_type, type(e).__name__)
        return None

    # ------------------------------------------------------------------
    # Raw sync entry points — schedule async work on the bg loop
    # ------------------------------------------------------------------
    def _on_p2_im_message_receive_v1(self, data: Any) -> None:
        self.schedule(self._handle_message_event(data))

    def _on_p2_card_action_trigger(
        self, data: P2CardActionTrigger
    ) -> P2CardActionTriggerResponse:
        try:
            if "cardAction" in self._handlers or self._config.inbound.emit_raw_events:
                self.schedule(self._handle_interaction_event(data))
        except Exception as e:
            logger.exception("cardAction schedule failed: %s", e)
        return P2CardActionTriggerResponse({})

    def _on_p2_reaction_created(self, data: Any) -> None:
        self.schedule(self._handle_reaction_event(data, action="create"))

    def _on_p2_reaction_deleted(self, data: Any) -> None:
        self.schedule(self._handle_reaction_event(data, action="delete"))

    def _on_p2_bot_added(self, data: Any) -> None:
        self.schedule(self._handle_bot_event(data, joined=True))

    def _on_p2_bot_deleted(self, data: Any) -> None:
        self.schedule(self._handle_bot_event(data, joined=False))

    def _on_p2_message_read(self, data: Any) -> None:
        self.schedule(self._handle_message_read_event(data))

    def _on_p1_comment_add(self, data: Any) -> None:
        self.schedule(self._handle_comment_event(data))

    # ------------------------------------------------------------------
    # Async event handlers
    # ------------------------------------------------------------------
    async def _emit_raw_event(self, data: Any) -> None:
        if not self._config.inbound.emit_raw_events:
            return
        await self._invoke("raw", _coerce.obj_to_dict(data) or {})

    async def _handle_message_event(self, data: Any) -> None:
        try:
            await self._emit_raw_event(data)
            event = getattr(data, "event", None)
            header = getattr(data, "header", None)
            event_id = getattr(header, "event_id", None)
            message = getattr(event, "message", None)
            sender = getattr(event, "sender", None)
            if message is None:
                return
            inbound = await self._pipeline.process(
                event_id=event_id,
                message_event=message,
                sender=sender,
            )
            if inbound is None:
                return
            # NB: roster collection and sender-name resolution happen only AFTER
            # the safety pipeline admits the message (in _dispatch_inbound_to_user),
            # so a policy/stale/dedup/self-sent-rejected message can neither write
            # the roster nor trigger the external members API.
            if self._safety is not None:
                await self._safety.push_message(inbound)
            else:
                await self._dispatch_inbound_to_user(inbound)
        except Exception as e:
            logger.exception("FeishuChannel.handle_message_event failed: %s", e)

    def _collect_mentions_into_roster(self, inbound) -> None:
        chat_id = inbound.conversation.chat_id if inbound.conversation else None
        if not chat_id:
            return
        seen = [
            ChatMember(id=m.open_id, name=m.name, is_bot=m.is_bot)
            for m in inbound.mentions
            if m.open_id and m.name
        ]
        if seen:
            self._chat_member_cache.set_members(chat_id, seen, "mention")

    async def _resolve_sender_name_from_roster(self, inbound) -> None:
        """Warm the chat roster and fill ``sender.display_name`` from it when the
        pipeline's own name resolution didn't. Best-effort — a roster miss or API
        failure degrades to leaving the name unset, never raises into dispatch."""
        chat_id = inbound.conversation.chat_id if inbound.conversation else None
        open_id = inbound.sender.open_id if inbound.sender else None
        if not chat_id or not open_id:
            return
        try:
            await self.get_chat_members(chat_id)
            # A bot sender is NOT in the users list — warm the bots roster too so
            # a bot-to-bot handoff resolves the sender's name (the core
            # bot-at-bot case). Cached + singleflight; failure degrades below.
            if inbound.sender.is_bot:
                await self.get_chat_bots(chat_id)
        except Exception as e:  # pragma: no cover - defensive
            logger.debug("channel: sender-name roster warm failed: %s", e)
            return
        if not inbound.sender.display_name:
            name = self._chat_member_cache.resolve_name(chat_id, open_id)
            if name:
                inbound.sender.display_name = name

    async def _dispatch_inbound_to_user(self, inbound) -> None:
        # Runs only for admitted messages (the safety pipeline's on_message, or
        # the direct path when no safety pipeline). Seed the roster from observed
        # mentions (incl. bots, which the members API filters out) so a bot that
        # has "shown its face" can later be @'d by name; source 'mention' never
        # overwrites authoritative 'api' names. Then optionally fill sender_name.
        self._collect_mentions_into_roster(inbound)
        if self._config.resolve_sender_names:
            await self._resolve_sender_name_from_roster(inbound)
        await self._invoke("message", inbound)

    def _emit_reject(self, event: RejectEvent) -> None:
        handlers = self._handlers.get("reject")
        if not handlers:
            logger.debug(
                "policy reject message=%s reason=%s", event.message_id, event.reason
            )
            return
        for handler in list(handlers):
            try:
                result = handler(event)
                if inspect.isawaitable(result):
                    self.schedule(result)
            except Exception as e:  # pragma: no cover
                logger.warning("FeishuChannel.on('reject') raised: %s", e)

    async def _handle_interaction_event(self, data: P2CardActionTrigger) -> None:
        try:
            await self._emit_raw_event(data)
            event = getattr(data, "event", None)
            action = getattr(event, "action", None)
            raw_value = getattr(action, "value", None)
            if isinstance(raw_value, str):
                try:
                    parsed = json.loads(raw_value)
                    raw_value = parsed if isinstance(parsed, dict) else {"value": parsed}
                except ValueError:
                    raw_value = {"value": raw_value}
            tag = getattr(action, "tag", None)
            context = getattr(event, "context", None)
            message_id = getattr(context, "open_message_id", None)
            chat_id = getattr(context, "open_chat_id", None)
            operator_open_id = getattr(
                getattr(event, "operator", None), "open_id", None
            )
            payload = CardActionEvent(
                message_id=message_id or "",
                chat_id=chat_id or "",
                operator=EventOperator(open_id=operator_open_id or ""),
                action=CardActionPayload(
                    tag=tag or "",
                    value=raw_value,
                    name=getattr(raw_value, "name", None)
                    if hasattr(raw_value, "name")
                    else None,
                    form_value=getattr(action, "form_value", None),
                    input_value=getattr(action, "input_value", None),
                    options=getattr(action, "options", None),
                    checked=getattr(action, "checked", None),
                ),
                raw=_coerce.obj_to_dict(data) or {},
            )
            # Route through safety.push_action (tier 2): dedup on a stable
            # action identity (tag + value payload) so Feishu's at-least-once
            # WS redelivery can't double-invoke the handler, and serialize by
            # chat_id so two fast clicks in the same chat are processed in
            # order.
            await self._through_action_safety(
                event_id=f"card:{payload.message_id}:{payload.operator.open_id}:"
                         f"{_card_action_identity(payload.action)}",
                queue_scope=payload.chat_id or payload.message_id or "",
                handler=lambda: self._invoke("cardAction", payload),
            )
        except Exception as e:
            logger.exception("FeishuChannel cardAction dispatch failed: %s", e)

    async def _through_action_safety(
        self,
        *,
        event_id: str,
        queue_scope: str,
        handler: Callable[[], Any],
    ) -> None:
        """Run ``handler`` through the safety tier-2 gate (dedup + lock +
        per-scope serial queue) when the pipeline exists; fall back to a
        direct invocation when it hasn't been built yet (early events during
        startup, unit tests that bypass ``connect``)."""
        safety = self._safety
        if safety is None:
            result = handler()
            if inspect.isawaitable(result):
                await result
            return

        async def _run() -> None:
            result = handler()
            if inspect.isawaitable(result):
                await result

        await safety.push_action(event_id, queue_scope or event_id, _run)

    async def _through_light_safety(
        self,
        *,
        event_id: str,
        handler: Callable[[], Any],
    ) -> None:
        """Tier-3 variant: dedup only (reaction add/remove). Same fallback
        semantics as :meth:`_through_action_safety`."""
        safety = self._safety
        if safety is None:
            result = handler()
            if inspect.isawaitable(result):
                await result
            return

        async def _run() -> None:
            result = handler()
            if inspect.isawaitable(result):
                await result

        await safety.push_light(event_id, _run)

    async def _handle_reaction_event(self, data: Any, *, action: str) -> None:
        await self._emit_raw_event(data)
        cfg = self._config.inbound.reaction_notifications
        if cfg == "off":
            return
        try:
            event = getattr(data, "event", None) or {}
            user = getattr(event, "user_id", None)
            message_id = getattr(event, "message_id", None) or ""
            operator_open_id = (
                getattr(user, "open_id", None) if user is not None else None
            )
            emoji_type = getattr(
                getattr(event, "reaction_type", None), "emoji_type", None
            )
            action_time = getattr(event, "action_time", None)
            raw_dict = _coerce.obj_to_dict(data) or {}
            raw_event = raw_dict.get("event") if isinstance(raw_dict, dict) else {}
            if not isinstance(raw_event, dict):
                raw_event = {}
            chat_id = getattr(event, "chat_id", None) or raw_event.get("chat_id")
            chat_type = getattr(event, "chat_type", None) or raw_event.get("chat_type")
            context = self._sent_message_context.get(message_id)
            if context is not None:
                if not chat_id:
                    chat_id = context.chat_id
                if not chat_type:
                    chat_type = context.chat_type

            if cfg == "own":
                if message_id and message_id not in self._sent_messages:
                    return

            direction = "added" if action == "create" else "removed"
            payload = ReactionEvent(
                message_id=message_id,
                operator=EventOperator(open_id=operator_open_id or ""),
                emoji_type=emoji_type or "",
                action=direction,
                chat_id=chat_id or None,
                chat_type=chat_type or None,
                action_time=action_time,
                raw=raw_dict,
            )
            # Tier 3: dedup only. Reactions are idempotent state changes so
            # lock / serial queue would add latency for no benefit, but
            # WS redelivery would double-invoke without this guard.
            # The light safety tier keeps that dedup-only behavior explicit.
            await self._through_light_safety(
                event_id=(
                    f"reaction:{message_id}:{operator_open_id or ''}:"
                    f"{emoji_type or ''}:{direction}"
                ),
                handler=lambda: self._invoke("reaction", payload),
            )
        except Exception as e:
            logger.exception("FeishuChannel reaction dispatch failed: %s", e)

    async def _handle_bot_event(self, data: Any, *, joined: bool) -> None:
        try:
            await self._emit_raw_event(data)
            event = getattr(data, "event", None) or {}
            chat_id = getattr(event, "chat_id", None) or ""
            operator = getattr(event, "operator_id", None)
            open_id = getattr(operator, "open_id", None) if operator else None
            raw_dict = _coerce.obj_to_dict(data)
            payload_cls = BotAddedEvent if joined else BotLeaveEvent
            name = "botAdded" if joined else "botLeave"
            await self._invoke(
                name,
                payload_cls(
                    chat_id=chat_id,
                    operator=EventOperator(open_id=open_id or ""),
                    raw=raw_dict or {},
                ),
            )
        except Exception as e:
            logger.exception(
                "FeishuChannel bot-%s dispatch failed: %s",
                "added" if joined else "leave", e,
            )

    async def _handle_message_read_event(self, data: Any) -> None:
        try:
            await self._emit_raw_event(data)
            event = getattr(data, "event", None) or {}
            reader = getattr(event, "reader", None)
            reader_open_id = getattr(reader, "reader_id", None) and getattr(
                reader.reader_id, "open_id", None
            )
            message_ids = list(getattr(event, "message_id_list", []) or [])
            await self._invoke(
                "messageRead",
                MessageReadEvent(
                    reader=EventOperator(open_id=reader_open_id or ""),
                    message_ids=message_ids,
                    raw=_coerce.obj_to_dict(data) or {},
                ),
            )
        except Exception as e:
            logger.exception("FeishuChannel messageRead dispatch failed: %s", e)

    async def _handle_comment_event(self, data: Any) -> None:
        try:
            await self._emit_raw_event(data)
            # ``CustomizedEvent.event`` is the raw inner event payload as a
            # plain dict; the per-event timestamp lives on the envelope
            # (``header.create_time`` for p2, ``ts`` for p1) — not in the
            # inner dict — so pass it explicitly.
            raw_event = getattr(data, "event", None)
            header = getattr(data, "header", None)
            envelope_ts = (
                getattr(header, "create_time", None) if header is not None
                else getattr(data, "ts", None)
            )
            normalized = normalize_comment(
                raw_event if raw_event is not None else data,
                bot_open_id=self._bot_open_id,
                envelope_timestamp=envelope_ts,
            )
            if normalized is None:
                return
            # Tier 2: dedup + lock + per-file_token serial queue. Multiple
            # comments on the same document are ordered; redeliveries of the
            # same comment event are dropped.
            await self._through_action_safety(
                event_id=f"comment:{normalized.file_token}:{normalized.comment_id}",
                queue_scope=normalized.file_token,
                handler=lambda: self._invoke("comment", normalized),
            )
        except Exception as e:
            logger.exception("FeishuChannel comment dispatch failed: %s", e)

    def _notify_reconnecting(self) -> None:
        self._connection_state = "reconnecting"
        self._connection_reconnect_attempts += 1
        for h in list(self._handlers.get("reconnecting", [])):
            try:
                h()
            except Exception as e:  # pragma: no cover
                logger.warning("reconnecting handler raised: %s", e)

    def _notify_reconnected(self) -> None:
        self._connection_state = "connected"
        self._connection_last_connected_at = time.time()
        for h in list(self._handlers.get("reconnected", [])):
            try:
                h()
            except Exception as e:  # pragma: no cover
                logger.warning("reconnected handler raised: %s", e)

    def _record_connection_error(self, error: BaseException) -> None:
        self._connection_state = "error"
        self._connection_last_error_at = time.time()
        self._connection_last_error = str(error)

    def connection_snapshot(self) -> ConnectionSnapshot:
        return ConnectionSnapshot(
            state=self._connection_state,
            ready=self._ready_flag,
            reconnect_attempts=self._connection_reconnect_attempts,
            last_connected_at=self._connection_last_connected_at,
            last_disconnected_at=self._connection_last_disconnected_at,
            last_error_at=self._connection_last_error_at,
            last_error=self._connection_last_error,
        )

    # ------------------------------------------------------------------
    # Outbound: send / stream / message ops
    # ------------------------------------------------------------------
    async def upload_media(
        self,
        source: MediaSource,
        *,
        kind: Literal["image", "file"],
        file_name: Optional[str] = None,
        file_type: Optional[str] = None,
    ) -> str:
        """Upload a media resource and return its Feishu ``image_key`` /
        ``file_key`` without sending a message.

        Public wrapper over the internal :func:`resolve_media_key` helper —
        intended for callers that need to construct custom post AST (e.g.
        ``audio + caption`` / ``file + caption``, which Feishu does not render
        natively but accepts as ``msg_type=post`` with ``tag:audio|file``
        nodes), or want to pre-upload + cache a key for cross-chat reuse.

        ``source`` must be a :class:`MediaSource`; pass an explicit kind:

        - ``MediaSource(kind="buffer", buffer=...)`` — in-memory bytes
        - ``MediaSource(kind="file", path=...)`` — local file
        - ``MediaSource(kind="url", url=...)`` — remote URL (SSRF-checked
          against ``OutboundConfig.ssrf_allowlist``)
        - ``MediaSource(kind="key", key=...)`` — pre-uploaded key (returned
          unchanged; no upload performed)

        ``kind`` selects the upload route: ``"image"`` → image upload (returns
        ``image_key``), ``"file"`` → file upload (returns ``file_key``). For
        audio / video, use ``kind="file"`` with ``file_type="opus"`` /
        ``"mp4"`` — Feishu treats them as typed file uploads at the wire
        level. ``file_name`` is shown in Feishu UI for ``kind="file"``.

        Failures raise :class:`FeishuChannelError`:

        - ``UPLOAD_FAILED`` — server rejection, network error, missing local
          file, malformed response, or empty ``MediaSource(kind="key", key="")``
        - ``SSRF_BLOCKED`` — URL source without an allowlist match

        SSRF allowlist is taken from ``self.config.outbound.ssrf_allowlist``
        — callers do **not** mutate the source object.
        """
        if not isinstance(source, MediaSource):
            raise TypeError(
                f"upload_media: source must be a MediaSource; "
                f"got {type(source).__name__}"
            )
        if kind not in ("image", "file"):
            raise ValueError(
                f"upload_media: kind must be 'image' or 'file'; got {kind!r}"
            )

        from .outbound.media.uploader import resolve_media_key

        key = await resolve_media_key(
            self._sender._driver,
            source,
            kind,
            file_name=file_name,
            file_type=file_type,
            ssrf_allowlist=self._config.outbound.ssrf_allowlist,
        )
        if key is None:
            # ``resolve_media_key`` returns None only for "nothing to upload"
            # inputs (kind="key" with empty key, or unhandled source shape).
            # The public method commits to ``str`` — convert to UPLOAD_FAILED
            # so callers don't need to handle Optional.
            raise FeishuChannelError(
                FeishuChannelErrorCode.UPLOAD_FAILED,
                f"upload_media: source has no uploadable content "
                f"(source.kind={source.kind!r})",
                context={"source_kind": source.kind},
            )
        return key

    async def send(self, to, message, opts=None) -> SendResult:
        """Send a message to a chat / user / email.

        ``message`` may be a dict (``{"text": "..."}``, ``{"markdown": "..."}``,
        ``{"image": {...}}``, …), a typed :class:`OutboundMessage` dataclass,
        or a bare string (shorthand for markdown). See :mod:`._coerce` for the
        full accepted shape.

        Errors: both raised exceptions (coercion errors, transport failures)
        AND ``SendResult.fail(...)`` outcomes are also forwarded to any
        handler registered via ``channel.on("error", ...)``, so apps can
        centralise failure observation. The error is still returned /
        raised to the immediate caller as well — forwarding does not swallow.
        """
        try:
            outbound = _coerce.coerce_outbound(message)
            send_opts = _coerce.coerce_send_opts(opts)
            rit = send_opts.receive_id_type or infer_receive_id_type(to)
            outbound = self._resolve_outbound_mentions(to, outbound, send_opts)
            result = await self._sender.send(
                outbound,
                receive_id=to,
                receive_id_type=rit,
                reply_to=send_opts.reply_to,
                reply_in_thread=send_opts.reply_in_thread,
                reply_target_gone=send_opts.reply_target_gone,
                uuid_=send_opts.uuid,
            )
        except Exception as e:
            await self._forward_outbound_error(e)
            raise
        if not result.success and result.error is not None:
            await self._forward_outbound_error(result.error)
        if result.success:
            self._remember_sent_message_context(
                result,
                chat_id=to if rit == "chat_id" and isinstance(to, str) else None,
                receive_id_type=rit,
            )
        return result

    def _resolve_outbound_mentions(self, to, outbound, opts):
        """Resolve "@name" into real mentions against the target chat's roster
        before sending: fill open_id on name-only structured mentions, and — when
        ``opts.resolve_mentions_in_text`` — rewrite ``@name`` tokens in a
        text / markdown body. Both no-op when there's nothing to resolve (or when
        ``to`` isn't a chat with a roster), so the default send path is untouched.
        """
        def lookup(name: str) -> Optional[str]:
            return self._chat_member_cache.resolve_open_id(to, name)

        if isinstance(outbound, (OutboundText, OutboundPost)) and outbound.mentions:
            outbound = replace(
                outbound, mentions=resolve_name_mentions(outbound.mentions, lookup)
            )

        if opts.resolve_mentions_in_text:
            if isinstance(outbound, OutboundText) and outbound.text:
                outbound = replace(
                    outbound, text=resolve_mentions_in_text(outbound.text, lookup)
                )
            elif isinstance(outbound, OutboundPost) and outbound.markdown:
                outbound = replace(
                    outbound, markdown=resolve_mentions_in_text(outbound.markdown, lookup)
                )

        return outbound

    async def reply(self, msg, message, opts=None) -> SendResult:
        """Reply to a received message, following the triggering message's shape.

        Defaults ``reply_to`` to ``msg.message_id`` and — when the trigger was
        inside a topic thread (``msg.conversation.thread_id`` present) — defaults
        ``reply_in_thread`` to ``True`` so the reply lands back in that thread.
        A flat message stays flat: ``reply`` only *follows* the trigger, it never
        promotes a flat message into a thread. ``opts`` overrides either default.
        Semantically a :meth:`send`.
        """
        base = _coerce.coerce_send_opts(opts)
        conversation = getattr(msg, "conversation", None)
        thread_id = getattr(conversation, "thread_id", None)
        resolved = replace(
            base,
            reply_to=base.reply_to or msg.message_id,
            reply_in_thread=(
                base.reply_in_thread
                if base.reply_in_thread is not None
                else bool(thread_id)
            ),
        )
        return await self.send(msg.chat_id, message, resolved)

    async def _forward_outbound_error(self, err: Any) -> None:
        """Fan-out a send/stream failure to any ``on("error", ...)`` handlers.

        :class:`SendError` is a dataclass without ``__traceback__`` or a
        ``str``-friendly form, so generic diagnostic plumbing
        (``logger.exception``, Sentry) chokes on it. Wrap it in
        :class:`OutboundSendError` before dispatching. The wrapping does not
        affect the value returned by ``channel.send()`` (still
        :class:`SendResult`).

        Never swallows — the error still propagates to the direct caller.
        Handler exceptions are logged but otherwise ignored.
        """
        if isinstance(err, SendError):
            err = OutboundSendError(err)
        for h in list(self._handlers.get("error", [])):
            try:
                result = h(err)
                if inspect.isawaitable(result):
                    await result
            except Exception as inner:  # pragma: no cover - defensive
                logger.warning("on('error') handler raised: %s", inner)

    async def stream(self, to, spec: Dict[str, Any], opts=None) -> SendResult:
        """Stream a message progressively.

        ``spec`` is either ``{"markdown": producer}`` or
        ``{"card": {"initial": ..., "producer": ...}}``.

        Errors (coercion + stream controller raises) are also forwarded to
        any ``on("error", ...)`` handlers before being re-raised to the
        caller, same contract as :meth:`send`.
        """
        try:
            if not isinstance(spec, dict):
                raise TypeError("stream spec must be a dict")
            send_opts = _coerce.coerce_send_opts(opts)
            rit = send_opts.receive_id_type or infer_receive_id_type(to)
        except Exception as e:
            await self._forward_outbound_error(e)
            raise

        if "markdown" in spec:
            # Markdown streaming uses the CardKit preallocation flow — every
            # throttle tick is an `update_card_element_content` call (seq-ordered
            # element patch) instead of a full-card PATCH. See
            # :mod:`.outbound.streaming.markdown_stream` for the protocol.
            ctl = MarkdownStreamController(
                to=to,
                receive_id_type=rit,
                reply_to=send_opts.reply_to,
                reply_in_thread=send_opts.reply_in_thread,
                reply_target_gone=send_opts.reply_target_gone,
                create_card_instance=self.create_card_instance,
                send_card_by_reference=self.send_card_by_reference,
                update_card_element_content=self.update_card_element_content,
                finish_streaming_card=self.finish_streaming_card,
            )
            try:
                mid = await ctl.run(spec["markdown"])
            except Exception as e:
                await self._forward_outbound_error(e)
                raise
            return SendResult.ok(message_id=mid)

        if "card" in spec:
            card_def = spec["card"]
            initial = card_def.get("initial") if isinstance(card_def, dict) else None
            producer = card_def.get("producer") if isinstance(card_def, dict) else None
            if initial is None or not callable(producer):
                err = TypeError("card stream requires {initial, producer}")
                await self._forward_outbound_error(err)
                raise err
            ctl = CardStreamController(
                initial=initial,
                ensure_created=lambda snap: self._ensure_card_snapshot(
                    to, rit, snapshot=snap,
                    reply_to=send_opts.reply_to,
                    reply_in_thread=send_opts.reply_in_thread,
                    reply_target_gone=send_opts.reply_target_gone,
                ),
                patch_card=self._patch_card,
            )
            try:
                mid = await ctl.run(producer)
            except Exception as e:
                await self._forward_outbound_error(e)
                raise
            return SendResult.ok(message_id=mid)

        err = TypeError("stream spec must contain 'markdown' or 'card'")
        await self._forward_outbound_error(err)
        raise err

    async def update_card(self, message_id: str, card: Dict[str, Any]) -> SendResult:
        """Update a card message. Returns a :class:`SendResult`."""
        raw = await self._patch_card(message_id, card)
        return _coerce.result_from_raw(raw, message_id=message_id)

    async def recall_message(self, message_id: str) -> SendResult:
        raw = await self._driver.delete_message(message_id=message_id)
        return _coerce.result_from_raw(raw, message_id=message_id)

    async def add_reaction(self, message_id: str, emoji_type: str) -> SendResult:
        raw = await self._driver.add_reaction(
            message_id=message_id, emoji_type=emoji_type
        )
        return _coerce.result_from_raw(raw, message_id=message_id)

    async def remove_reaction(self, message_id: str, reaction_id: str) -> SendResult:
        raw = await self._driver.remove_reaction(
            message_id=message_id, reaction_id=reaction_id
        )
        return _coerce.result_from_raw(raw, message_id=message_id)

    async def list_reactions(
        self,
        message_id: str,
        emoji_type: Optional[str] = None,
        *,
        page_token: Optional[str] = None,
        page_size: Optional[int] = None,
    ) -> Dict[str, Any]:
        return await self._driver.list_reactions(
            message_id=message_id,
            emoji_type=emoji_type,
            page_token=page_token,
            page_size=page_size,
        )

    async def remove_reaction_by_emoji(
        self,
        message_id: str,
        emoji_type: str,
    ) -> SendResult:
        raw = await self._driver.list_reactions(
            message_id=message_id,
            emoji_type=emoji_type,
            page_token=None,
            page_size=None,
        )
        data = (raw or {}).get("data") if isinstance(raw, dict) else None
        items = data.get("items") if isinstance(data, dict) else None
        for item in items if isinstance(items, list) else []:
            reaction = item.get("reaction_type") if isinstance(item, dict) else None
            if not isinstance(reaction, dict) or reaction.get("emoji_type") != emoji_type:
                continue
            reaction_id = item.get("reaction_id")
            if reaction_id:
                return await self.remove_reaction(message_id, reaction_id)
        return SendResult.fail(
            SendError(
                code=FeishuChannelErrorCode.UNKNOWN,
                retryable=False,
                hint=f"reaction not found for emoji_type={emoji_type}",
            ),
            raw=raw if isinstance(raw, dict) else None,
        )

    async def add_typing_reaction(self, message_id: str) -> Optional[str]:
        try:
            result = await self.add_reaction(message_id, "Typing")
            raw = getattr(result, "raw", None)
            if raw is None and isinstance(result, dict):
                raw = result
            data = (raw or {}).get("data") or {}
            return data.get("reaction_id")
        except Exception:
            return None

    async def remove_typing_reaction(self, message_id: str, reaction_id: str) -> bool:
        try:
            result = await self.remove_reaction(message_id, reaction_id)
            return bool(getattr(result, "success", False))
        except Exception:
            return False

    def _comment_client(self) -> CommentPrimitiveClient:
        return CommentPrimitiveClient(raw_request=lambda req: self._client.arequest(req))

    async def resolve_comment_target(
        self,
        *,
        file_token: str,
        file_type: str,
    ) -> CommentTarget:
        return await self._comment_client().resolve_comment_target(
            file_token=file_token,
            file_type=file_type,
        )

    async def get_comment_context(
        self,
        *,
        target: CommentTarget,
        comment_id: str,
        event_reply_id: Optional[str] = None,
    ) -> CommentContext:
        return await self._comment_client().get_comment_context(
            target=target,
            comment_id=comment_id,
            event_reply_id=event_reply_id,
        )

    async def fetch_comment(
        self,
        *,
        target: CommentTarget,
        comment_id: str,
    ):
        return await self._comment_client().fetch_comment(
            target=target,
            comment_id=comment_id,
        )

    async def list_comments(
        self,
        *,
        target: CommentTarget,
        page_token=None,
        page_size=None,
        is_whole=None,
        is_solved=None,
    ):
        return await self._comment_client().list_comments(
            target=target,
            page_token=page_token,
            page_size=page_size,
            is_whole=is_whole,
            is_solved=is_solved,
        )

    async def list_comment_replies(
        self,
        *,
        target: CommentTarget,
        comment_id: str,
        page_token=None,
        page_size=None,
    ):
        return await self._comment_client().list_comment_replies(
            target=target,
            comment_id=comment_id,
            page_token=page_token,
            page_size=page_size,
        )

    async def reply_comment(self, context: CommentContext, content: str):
        return await self._comment_client().reply_comment(context, content)

    async def edit_message(self, message_id: str, message) -> SendResult:
        """Edit a previously sent text or post message.

        Accepted message shapes mirror send() for editable types: str,
        {"markdown": ...}, {"text": ...}, {"post": ...}, OutboundText,
        and OutboundPost. Cards must use update_card(); media/share/sticker
        messages are not editable through this method.
        """
        outbound = _coerce.coerce_outbound(message)
        if isinstance(outbound, OutboundCard):
            raise TypeError("edit_message does not support cards; use update_card()")
        if not isinstance(outbound, (OutboundText, OutboundPost)):
            raise TypeError(
                "edit_message only supports text/post messages; "
                f"got {type(outbound).__name__}"
            )
        body = await self._sender.materialize_for_edit(outbound)
        raw = await self._driver.update_message(
            message_id=message_id,
            msg_type=body["msg_type"],
            content=body["content"],
        )
        return _coerce.result_from_raw(raw, message_id=message_id)

    async def download_resource(
        self,
        file_key: str,
        resource_type: str = "image",
        message_id: Optional[str] = None,
    ) -> Optional[bytes]:
        return await self._download_media(
            message_id=message_id or "",
            file_key=file_key,
            resource_type=resource_type,
        )

    async def _download_media(
        self, *, message_id: str, file_key: str, resource_type: str
    ) -> Optional[bytes]:
        return await _api_helpers.download_media(
            self._client,
            message_id=message_id,
            file_key=file_key,
            resource_type=resource_type,
        )

    async def download_resource_to_file(
        self,
        file_key: str,
        *,
        resource_type: str = "image",
        message_id: Optional[str] = None,
        dest_dir: "Path",
        file_name: Optional[str] = None,
    ) -> "Path":
        """Download a resource to disk and return the absolute path.

        Args:
            file_key: opaque resource identifier from the inbound event.
            resource_type: ``image`` / ``file`` / ``audio`` / ``video``.
            message_id: when supplied, downloads via the message-resource
                endpoint; without it, falls back to the standalone
                image/file endpoints.
            dest_dir: target directory (auto-created if missing).
            file_name: override the auto-generated filename. When None, the
                file is named ``<file_key><suffix>`` where ``<suffix>`` is
                inferred from the response's content-type / file_name.

        Returns:
            Absolute path to the downloaded file.

        Raises:
            FeishuChannelError(DOWNLOAD_FAILED): when the download fails or
                the response has no body.
        """
        from pathlib import Path
        import os
        import tempfile

        body, meta = await _api_helpers.download_media_with_meta(
            self._client,
            message_id=message_id or "",
            file_key=file_key,
            resource_type=resource_type,
        )
        if body is None:
            raise FeishuChannelError(
                FeishuChannelErrorCode.DOWNLOAD_FAILED,
                f"download failed: file_key={file_key} resource_type={resource_type}",
            )

        dest_dir = Path(dest_dir)
        dest_dir.mkdir(parents=True, exist_ok=True)

        if file_name:
            name = file_name
        else:
            suffix = self._infer_suffix(meta, resource_type)
            name = f"{file_key}{suffix}"
        name = self._safe_download_file_name(name)
        out = dest_dir / name

        # Atomic: write to tmp file in same dir, then rename.
        fd, tmp_path = tempfile.mkstemp(prefix=".dl-", dir=str(dest_dir))
        try:
            with os.fdopen(fd, "wb") as fh:
                fh.write(body)
            os.replace(tmp_path, out)
        except Exception:
            try:
                os.unlink(tmp_path)
            except OSError:
                pass
            raise
        return out

    async def resolve_resource_to_cache(
        self,
        *,
        message_id: str,
        resource: ResourceDescriptor,
    ) -> CachedResource:
        return await self._media_cache.resolve(
            message_id=message_id,
            resource=resource,
            downloader=lambda **kwargs: _api_helpers.download_media_with_meta(
                self._client,
                **kwargs,
            ),
        )

    async def resolve_resources_to_cache(
        self, *, message_id: str, resources: List[ResourceDescriptor]
    ) -> List[CachedResource]:
        return [
            await self.resolve_resource_to_cache(message_id=message_id, resource=resource)
            for resource in resources
        ]

    @staticmethod
    def _safe_download_file_name(name: str) -> str:
        """Validate a download filename before joining it under dest_dir."""
        from pathlib import PureWindowsPath
        import os

        name = (name or "").replace("\x00", "")
        win = PureWindowsPath(name)
        if (
            not name
            or name in (".", "..")
            or os.path.isabs(name)
            or "/" in name
            or "\\" in name
            or win.drive
            or win.root
            or any(part in ("", ".", "..") for part in win.parts)
        ):
            raise FeishuChannelError(
                FeishuChannelErrorCode.DOWNLOAD_FAILED,
                f"unsafe download file name: {name!r}",
            )
        return name

    @staticmethod
    def _infer_suffix(meta: Optional[str], resource_type: str) -> str:
        """Best-effort suffix derivation from content-type / filename.

        Falls back to ``.bin`` when nothing is parseable, then to type-default
        suffixes for the four common ``resource_type`` values.
        """
        import mimetypes

        if meta:
            # If meta looks like a filename (has a dot in last segment),
            # return its suffix directly.
            if "/" not in meta and "." in meta:
                ext = "." + meta.rsplit(".", 1)[-1]
                if 1 < len(ext) <= 6:
                    return ext.lower()
            # Otherwise treat as a MIME type
            ext = mimetypes.guess_extension(meta.split(";", 1)[0].strip())
            if ext:
                return ext

        return {
            "image": ".jpg",
            "audio": ".mp3",
            "video": ".mp4",
            "file": ".bin",
        }.get(resource_type, ".bin")

    async def get_chat_info(self, chat_id: str) -> Optional[ChatInfo]:
        """Fetch chat metadata. Returns None on API failure."""
        return await _api_helpers.fetch_chat_info(self._client, chat_id)

    async def get_chat_mode(self, chat_id: str) -> Optional[str]:
        cached = self._chat_mode_cache.get(chat_id)
        if cached is not None:
            return cached
        info = await self.get_chat_info(chat_id)
        mode = info.chat_mode if info is not None else None
        if mode:
            self._chat_mode_cache.set(chat_id, mode)
            return mode
        return self._config.chat_mode_cache.fallback

    # ------------------------------------------------------------------
    # Chat roster (bot-at-bot)
    # ------------------------------------------------------------------
    async def get_chat_members(
        self,
        chat_id: str,
        *,
        page_size: int = 100,
        max_pages: int = 10,
        id_type: str = "open_id",
        force: bool = False,
    ) -> List[ChatMember]:
        """List a chat's members, following pagination.

        Returns **users only** — Feishu's chat-members API filters bots out, so
        ``is_bot`` is never ``True`` here; use :meth:`get_chat_bots` for the
        bots. ``page_size`` is clamped to Feishu's max of 100; ``max_pages``
        (default 10) caps paging. Results are cached per chat and reused by
        ``sender_name`` resolution and "@name → open_id"; a second call hits the
        cache and ``force`` bypasses it. A ``resolve_chat_members`` config hook,
        if provided, overrides the API. Raises :class:`FeishuChannelError` on
        API failure.
        """
        if max_pages < 1:
            raise FeishuChannelError(
                FeishuChannelErrorCode.UNKNOWN,
                f"get_chat_members: max_pages must be >= 1, got {max_pages}",
            )
        # Cache is keyed by id_type: a user_id query must not be satisfied by an
        # open_id snapshot (their ChatMember.id values differ).
        if not force:
            cached = self._chat_member_cache.get_members(chat_id, id_type)
            if cached is not None:
                return cached
        members, complete = await self._roster_singleflight(
            f"members::{chat_id}::{id_type}",
            lambda: self._fetch_chat_members(
                chat_id, page_size=page_size, max_pages=max_pages, id_type=id_type
            ),
        )
        # Only a COMPLETE snapshot becomes authoritative (cached + feeds the
        # name index). A truncated result is returned to this caller but never
        # poisons the cache or the name→open_id index — otherwise a partial
        # roster could make a shared name look unique and mis-@.
        if complete:
            self._chat_member_cache.set_members(
                chat_id, members, "api", id_type=id_type, complete=True
            )
        return members

    @staticmethod
    def _hook_takes_id_type(hook: Callable) -> bool:
        try:
            params = [
                p
                for p in inspect.signature(hook).parameters.values()
                if p.kind in (p.POSITIONAL_ONLY, p.POSITIONAL_OR_KEYWORD)
            ]
            return len(params) >= 2
        except (ValueError, TypeError):  # builtins / C callables without a signature
            return False

    async def _call_member_hook(
        self, hook: Callable, chat_id: str, id_type: str
    ) -> Any:
        """Invoke a ``resolve_chat_members`` hook without blocking the event loop.

        Supports both ``hook(chat_id)`` and ``hook(chat_id, id_type)``; a sync
        hook runs in an executor, an async hook is awaited — both bounded by a
        total timeout so a slow directory can't stall message processing.
        """
        timeout = self._config.transport.http_timeout_seconds or 30.0
        takes_id_type = self._hook_takes_id_type(hook)
        try:
            if inspect.iscoroutinefunction(hook):
                coro = hook(chat_id, id_type) if takes_id_type else hook(chat_id)
                return await asyncio.wait_for(coro, timeout)
            loop = asyncio.get_running_loop()
            call = (
                (lambda: hook(chat_id, id_type))
                if takes_id_type
                else (lambda: hook(chat_id))
            )
            result = await asyncio.wait_for(loop.run_in_executor(None, call), timeout)
            # A sync hook may still hand back an awaitable; await it too.
            if inspect.isawaitable(result):
                result = await asyncio.wait_for(result, timeout)
            return result
        except asyncio.TimeoutError as e:
            raise FeishuChannelError(
                FeishuChannelErrorCode.SEND_TIMEOUT,
                f"resolve_chat_members hook timed out after {timeout}s",
                cause=e,
            ) from e

    async def _fetch_chat_members(
        self, chat_id: str, *, page_size: int, max_pages: int, id_type: str
    ) -> "tuple[List[ChatMember], bool]":
        """Returns ``(members, complete)``. ``complete`` is ``False`` when paging
        stopped at ``max_pages`` while the API still reported more members (so a
        truncated list is never treated as an authoritative full roster)."""
        hook = self._config.resolve_chat_members
        if hook is not None:
            result = await self._call_member_hook(hook, chat_id, id_type)
            # A non-empty roster from the hook wins; ``None`` or an empty list
            # both mean "no data from the hook" and fall back to the API.
            if result:
                for m in result:
                    if getattr(m, "id_type", id_type) != id_type:
                        raise FeishuChannelError(
                            FeishuChannelErrorCode.UNKNOWN,
                            f"resolve_chat_members returned id_type "
                            f"{getattr(m, 'id_type', None)!r}; expected {id_type!r}",
                        )
                return list(result), True

        size = min(max(page_size, 1), 100)
        out: List[ChatMember] = []
        page_token: Optional[str] = None
        seen_tokens: Set[str] = set()
        complete = True
        for page in range(max_pages):
            queries: List[tuple] = [
                ("member_id_type", id_type),
                ("page_size", str(size)),
            ]
            if page_token:
                queries.append(("page_token", page_token))
            data = await self._raw_tenant_get(
                "/open-apis/im/v1/chats/:chat_id/members",
                {"chat_id": chat_id},
                queries,
            )
            for it in data.get("items") or []:
                member_id = it.get("member_id") or it.get("open_id")
                if not member_id:
                    continue
                out.append(
                    ChatMember(
                        id=member_id,
                        id_type=it.get("member_id_type") or id_type,
                        name=it.get("name"),
                        tenant_key=it.get("tenant_key"),
                        is_bot=False,
                    )
                )
            next_token = data.get("page_token")
            if not data.get("has_more") or not next_token:
                break
            if next_token in seen_tokens:
                logger.warning(
                    "channel: get_chat_members(%s) repeated page_token — stopping",
                    chat_id,
                )
                complete = False
                break
            seen_tokens.add(next_token)
            page_token = next_token
            if page == max_pages - 1:
                # About to exit on max_pages while more members remain.
                complete = False
                logger.warning(
                    "channel: get_chat_members(%s) truncated at max_pages=%d "
                    "(has_more still true) — result cached as incomplete",
                    chat_id,
                    max_pages,
                )
        return out, complete

    async def get_chat_bots(
        self, chat_id: str, *, force: bool = False
    ) -> List[ChatMember]:
        """List the **bots** in a chat — the companion to :meth:`get_chat_members`,
        which returns users only. Returns :class:`ChatMember`\\ s with
        ``is_bot=True`` and seeds them into the roster so another bot can be
        ``@``-ed by name without having appeared in an inbound mention first.
        Cached per chat like ``get_chat_members`` (``force`` bypasses). Raises
        :class:`FeishuChannelError` on API failure.
        """
        if not force:
            cached = self._chat_member_cache.get_bots(chat_id)
            if cached is not None:
                return cached
        bots = await self._roster_singleflight(
            f"bots::{chat_id}", lambda: self._fetch_chat_bots(chat_id)
        )
        self._chat_member_cache.set_bots(chat_id, bots)
        return bots

    async def _roster_singleflight(self, key: str, factory: Callable) -> Any:
        """Collapse concurrent cold roster fetches for the same key into one
        upstream call. Same-loop cooperative: the check-and-register below has no
        await between it, so racers within a loop share the first future."""
        existing = self._roster_inflight.get(key)
        if existing is not None:
            return await existing
        loop = asyncio.get_running_loop()
        fut = loop.create_future()
        self._roster_inflight[key] = fut
        try:
            fut.set_result(await factory())
        except BaseException as e:
            fut.set_exception(e)
        finally:
            self._roster_inflight.pop(key, None)
        return await fut

    async def _fetch_chat_bots(self, chat_id: str) -> List[ChatMember]:
        data = await self._raw_tenant_get(
            "/open-apis/im/v1/chats/:chat_id/members/bots",
            {"chat_id": chat_id},
            [],
        )
        out: List[ChatMember] = []
        for it in data.get("items") or []:
            bot_id = it.get("bot_id")
            if not bot_id:
                continue
            out.append(
                ChatMember(
                    id=bot_id,
                    id_type="open_id",
                    name=it.get("bot_name"),
                    is_bot=True,
                )
            )
        return out

    async def _raw_tenant_get(
        self, uri: str, paths: Dict[str, str], queries: List[tuple]
    ) -> Dict[str, Any]:
        """GET a tenant-scoped endpoint that has no generated SDK resource
        (mirrors :mod:`.bot_identity`). ``chat_id`` and other caller-supplied
        values go through ``req.paths`` so ``Transport._build_url`` URL-encodes
        them — never f-stringed raw into the URI (path-traversal guard).
        """
        req = BaseRequest()
        req.http_method = HttpMethod.GET
        req.uri = uri
        req.paths = dict(paths)
        req.queries = list(queries)
        req.token_types = {AccessTokenType.TENANT}
        option = RequestOption()
        timeout = self._config.transport.http_timeout_seconds or 30.0
        try:
            # Token verify may fetch/refresh the tenant token synchronously; run
            # it off the event loop so it can't block other message processing,
            # and fold token + transport errors into the unified error type. A
            # total timeout bounds the whole operation (not just a socket read).
            async def _run() -> Any:
                loop = asyncio.get_running_loop()
                await loop.run_in_executor(
                    None, _token_auth.verify, self._client.config, req, option
                )
                return await Transport.aexecute(self._client.config, req, option)

            resp = await asyncio.wait_for(_run(), timeout=timeout)
        except FeishuChannelError:
            raise
        except asyncio.TimeoutError as e:
            raise FeishuChannelError(
                FeishuChannelErrorCode.SEND_TIMEOUT,
                f"chat roster request timed out after {timeout}s",
                cause=e,
            ) from e
        except Exception as e:
            raise FeishuChannelError(
                FeishuChannelErrorCode.UNKNOWN,
                f"chat roster request failed: {e}",
                cause=e,
            ) from e
        return self._parse_tenant_body(resp)

    @staticmethod
    def _parse_tenant_body(resp: Any) -> Dict[str, Any]:
        if resp is None or getattr(resp, "content", None) is None:
            raise FeishuChannelError(
                FeishuChannelErrorCode.UNKNOWN, "empty roster response"
            )
        status = getattr(resp, "status_code", None)
        try:
            body = json.loads(resp.content.decode("utf-8"))
        except (ValueError, UnicodeDecodeError) as e:
            raise FeishuChannelError(
                _status_error_code(status),
                "malformed roster response",
                cause=e,
                context={"status": status},
            ) from e
        # A non-object top-level JSON (e.g. a bare array) must not reach `.get`.
        if not isinstance(body, dict):
            raise FeishuChannelError(
                _status_error_code(status),
                "unexpected roster response shape (expected a JSON object)",
                context={"status": status},
            )
        code = body.get("code")
        if code:
            raise FeishuChannelError(
                classify_api_error(code, body.get("msg") or ""),
                body.get("msg") or f"roster request returned code {code}",
                context={"code": code, "status": status},
            )
        # An HTTP error with no (or zero) business code must not be treated as an
        # empty success — surface it as a typed error instead of caching [].
        if status is not None and not (200 <= status < 300):
            raise FeishuChannelError(
                classify_http_status(status),
                body.get("msg") or f"roster request failed with HTTP {status}",
                context={"status": status},
            )
        data = body.get("data")
        return data if isinstance(data, dict) else {}

    # ------------------------------------------------------------------
    # CardKit preallocation API
    # ------------------------------------------------------------------
    async def create_card_instance(self, spec: Dict[str, Any]) -> str:
        """Create a pre-allocated card via ``POST /open-apis/cardkit/v1/card``.

        Returns the ``card_id`` string. Use with :meth:`send_card_by_reference`
        / :meth:`update_card_element_content` / :meth:`finish_streaming_card`
        for the CardKit typewriter-streaming flow.
        """
        raw = await self._driver.cardkit_create(
            body={"type": "card_json", "data": json.dumps(spec, ensure_ascii=False)}
        )
        if not isinstance(raw, dict) or raw.get("code", 0) != 0:
            raise FeishuChannelError(
                FeishuChannelErrorCode.UNKNOWN,
                f"create_card_instance failed: {raw}",
            )
        data = raw.get("data") or {}
        card_id = data.get("card_id")
        if not card_id:
            raise FeishuChannelError(
                FeishuChannelErrorCode.UNKNOWN,
                f"create_card_instance response missing card_id: {raw}",
            )
        return str(card_id)

    async def send_card_by_reference(
        self,
        to: str,
        card_id: str,
        *,
        receive_id_type: Optional[str] = None,
        reply_to: Optional[str] = None,
        reply_in_thread: Optional[bool] = None,
        reply_target_gone: str = "fresh",
    ) -> SendResult:
        """Send a message that references a pre-allocated card (see
        :meth:`create_card_instance`)."""
        rit = receive_id_type or infer_receive_id_type(to)
        return await self._sender.send(
            OutboundCard(card={"type": "card", "data": {"card_id": card_id}}),
            receive_id=to,
            receive_id_type=rit,
            reply_to=reply_to,
            reply_in_thread=reply_in_thread,
            reply_target_gone=reply_target_gone,
        )

    async def update_card_element_content(
        self,
        card_id: str,
        element_id: str,
        content: str,
        sequence: int,
    ) -> None:
        """Typewriter-update a card element. ``sequence`` must strictly increase."""
        raw = await self._driver.cardkit_update_element(
            card_id=card_id,
            element_id=element_id,
            body={"content": content, "sequence": sequence},
        )
        if isinstance(raw, dict) and raw.get("code", 0) != 0:
            raise FeishuChannelError(
                FeishuChannelErrorCode.UNKNOWN,
                f"update_card_element_content failed: {raw}",
            )

    async def finish_streaming_card(self, card_id: str, sequence: int) -> None:
        """Close ``streaming_mode`` on a pre-allocated card."""
        settings = json.dumps(
            {"config": {"streaming_mode": False}}, ensure_ascii=False
        )
        raw = await self._driver.cardkit_update_settings(
            card_id=card_id,
            body={"settings": settings, "sequence": sequence},
        )
        if isinstance(raw, dict) and raw.get("code", 0) != 0:
            raise FeishuChannelError(
                FeishuChannelErrorCode.UNKNOWN,
                f"finish_streaming_card failed: {raw}",
            )

    # ------------------------------------------------------------------
    # Card streaming internals
    # ------------------------------------------------------------------
    async def _ensure_card(
        self,
        to,
        rit,
        *,
        initial_text,
        reply_to,
        reply_in_thread,
        reply_target_gone="fresh",
    ) -> str:
        """Compatibility wrapper for older internal card-stream call sites."""
        return await self._ensure_card_snapshot(
            to, rit,
            snapshot={
                "schema": "2.0",
                "config": {"streaming_mode": True, "summary": {"content": ""}},
                "body": {"elements": [{"tag": "markdown", "content": initial_text or "..."}]},
            },
            reply_to=reply_to,
            reply_in_thread=reply_in_thread,
            reply_target_gone=reply_target_gone,
        )

    async def _ensure_card_snapshot(
        self,
        to,
        rit,
        *,
        snapshot,
        reply_to,
        reply_in_thread,
        reply_target_gone="fresh",
    ) -> str:
        result = await self._sender.send(
            OutboundCard(card=snapshot),
            receive_id=to,
            receive_id_type=rit,
            reply_to=reply_to,
            reply_in_thread=reply_in_thread,
            reply_target_gone=reply_target_gone,
        )
        if not result.success or not result.message_id:
            code = result.error.code if result.error else FeishuChannelErrorCode.UNKNOWN
            raise FeishuChannelError(
                code, f"failed to create streaming card: {result.error}"
            )
        return result.message_id

    async def _patch_card(self, message_id: str, card: Dict[str, Any]) -> Dict[str, Any]:
        raw = await self._driver.patch_message(
            message_id=message_id,
            content=json.dumps(card, ensure_ascii=False),
        )
        code = (raw or {}).get("code", 0)
        if code != 0:
            logger.warning(
                "channel.card_patch: patch failed code=%s msg=%s",
                code, (raw or {}).get("msg"),
            )
        return raw

    # ------------------------------------------------------------------
    # Fetch payload
    # ------------------------------------------------------------------
    async def fetch_message(self, message_id: str) -> Dict[str, Any]:
        """Fetch a message by ID and return the raw Feishu ``im.v1.message.get``
        response as a dict.

        This is a thin wrapper over the underlying OpenAPI call — no
        normalization is performed. Use it for cases like "look up the text
        of a message the user replied to" where you need ad-hoc access to
        the wire payload. For the typed :class:`InboundMessage` shape, feed
        events through the inbound pipeline normally.
        """
        return await self._driver.fetch_message(message_id)

    async def fetch_inbound_message(self, message_id: str):
        raw = await self._driver.fetch_message(message_id)
        item = _extract_first_message_item(raw)
        if item is None:
            return None
        sender = _extract_fetched_sender(item)
        message = _normalize_fetched_message_item(item)
        return await self._pipeline.normalize(message_event=message, sender=sender)

    async def fetch_quoted_context(self, message_id: str):
        resolver = QuoteResolver(
            fetcher=lambda mid: _api_helpers.fetch_message_raw(
                self._client,
                mid,
                card_content_type="user_card_content",
            )
        )
        return await resolver.fetch_quoted_context(message_id)

    async def resolve_quoted_contexts(self, messages, *, chat_mode=None):
        resolver = QuoteResolver(
            fetcher=lambda mid: _api_helpers.fetch_message_raw(
                self._client,
                mid,
                card_content_type="user_card_content",
            )
        )
        return await resolver.resolve_quoted_contexts(messages, chat_mode=chat_mode)

    async def select_quote_targets(self, messages, *, chat_mode=None):
        return await self.resolve_quoted_contexts(messages, chat_mode=chat_mode)

    # Alias used by the internal pipeline (kept stable for dependency
    # injection; external callers should prefer ``fetch_message``).
    async def _fetch_message_payload(self, message_id: str) -> Dict[str, Any]:
        return await self._driver.fetch_message(message_id)

    # ------------------------------------------------------------------
    # UAT (user access token) — exposed for callers that need it explicitly
    # ------------------------------------------------------------------
    # ------------------------------------------------------------------
    # Meeting channel
    # ------------------------------------------------------------------
    async def follow_my_meeting(
        self,
        *,
        user_open_id: str,
        prompt_context: Any = None,
        meeting_no: Optional[str] = None,
        options: Optional[MeetingOptions] = None,
    ) -> Any:
        """Follow the meeting ``user_open_id`` is currently in, without joining —
        and read the trust boundary on ``user_open_id`` and ``prompt_context``
        before the parameters, because this SDK cannot enforce either of them.

        ``user_open_id`` **must** be somebody the caller has already
        established is the requester, and ``prompt_context`` must belong to that
        same person. This SDK cannot check either: it receives a string, the
        ticket store is shared per process, and a cached ticket resolves without
        notifying its owner — so passing user-controlled input here listens in
        on somebody else's meeting with their authorization, invisibly. Pairing
        one person's ``user_open_id`` with another's ``prompt_context`` sends the
        authorization card to the wrong person and files the resulting ticket
        under the first. ``meeting.follow_allowlist`` is the available gate.

        The bot is not a participant and is not visible in the meeting, so there
        is no way to speak into it — respond over IM instead. Telling
        participants and obtaining consent is the integrating application's
        responsibility.

        Unlike :meth:`join_meeting` this does not need ``connect()``: the path
        is REST plus a user ticket, and opening a socket for it is pure
        overhead.
        """
        return await self._on_bg_loop(
            self._meeting.follow_my_meeting(
                user_open_id=user_open_id,
                prompt_context=prompt_context,
                meeting_no=meeting_no,
                options=options,
            )
        )

    async def join_meeting(
        self,
        meeting_no: str,
        *,
        password: Optional[str] = None,
        call_id: Optional[str] = None,
        options: Optional[MeetingOptions] = None,
    ) -> Any:
        """Put the bot in a meeting as a participant. Needs ``connect()``.

        ``password`` is a credential and is passed to the platform and nowhere
        else — it does not reach logs, ``raw`` payloads or error objects.

        Remember that ``dispose()`` does **not** leave the meeting, so a
        reconnect leaves the bot where it is; only an explicit ``leave()``
        removes it.
        """
        try:
            return await self._on_bg_loop(
                self._meeting.join_meeting(
                    meeting_no,
                    connected=bool(self.is_ready or self._started),
                    password=password,
                    call_id=call_id,
                    options=options,
                )
            )
        finally:
            # Dropped from this frame's locals. A wrong password is the ordinary
            # way this call fails, and on the failing path this frame is part of
            # the raised error's `__traceback__` — where a crash reporter that
            # reads frame locals would find it.
            password = None

    def get_meeting_event_health(self) -> MeetingEventHealth:
        """Diagnostics for the in-meeting event path.

        Failures here are silent by nature — an undeclared subscription, a
        missing permission and a renamed field all look like "nothing
        happened". ``received`` versus ``stats[type].empty`` separates "the
        platform never sent it" from "it sent it and we could not read it".
        """
        return self._meeting.health()

    def on_raw_event(self, event_type: str, handler: Callable) -> Unsubscribe:
        """Subscribe to a Feishu event type the channel has not wrapped.

        Multicast; returns an unsubscribe. Pass the event type **without** a
        schema prefix, e.g. ``"vc.bot.meeting_started_v1"``.

        Distinct from the ``"raw"`` event: that one mirrors already-wrapped
        events and is controlled by ``inbound.emit_raw_events``. This one
        subscribes to types the channel does not wrap and ignores that switch.

        The payload is authentic — this runs after signature verification and
        decryption — but **unredacted**, and this path sits **outside** the
        safety pipeline: no policy gate, no dedup, no processing lock, no loop
        guard. Subscribing to a type the channel already handles therefore opens
        an unpoliced path into that type: with ``dm_policy="allowlist"`` set,
        a raw subscription to ``im.message.receive_v1`` still receives direct
        messages from everybody, and redelivered events run the handler again.
        """
        subscription = self._raw_events.subscribe(event_type, handler)
        dispatcher = self._dispatcher
        if dispatcher is not None:
            # Installed onto the dispatcher **in place**. Replacing
            # `self._dispatcher` with a freshly built one would only work for
            # consumers that re-read the attribute on every event; anything
            # holding the instance it was given — which is what a transport
            # does — would not see this subscription until the next `start()`.
            self._raw_events.install(dispatcher)
        return subscription

    def _dispose_meeting_sessions_blocking(self, *, timeout: float = 10.0) -> None:
        """Dispose meeting sessions from a synchronous teardown path.

        Bounded by the sessions' own drain deadlines rather than by this
        number, which only has to be larger than theirs. Sessions drain
        concurrently, so the floor is one session — but that one is
        ``dispose_drain_timeout_seconds`` **twice**: once waiting for its
        loops to unwind and once for the delivery queue. With the 5s default
        that is 10s — exactly this budget, with nothing to spare. So raising
        that configuration, or lowering this number, times out here; lowering
        the configuration is safe. Timing out abandons the remaining sessions'
        tasks to the loop shutdown below.
        """
        loop = self._bg_loop
        if loop is None or not loop.is_running():
            self._meeting.dispose_all()
            return
        try:
            future = asyncio.run_coroutine_threadsafe(
                self._dispose_meeting_sessions_inner(), loop
            )
            future.result(timeout=timeout)
        except Exception as e:  # pragma: no cover - teardown must not raise
            logger.warning(
                "FeishuChannel.stop: disposing meeting sessions failed: %s",
                type(e).__name__,
            )

    async def _dispose_meeting_sessions(self) -> None:
        """Stop local meeting work on the loop that owns it."""
        if self._bg_loop is None:
            self._meeting.dispose_all()
            return
        try:
            await self._on_bg_loop(self._dispose_meeting_sessions_inner())
        except Exception as e:  # pragma: no cover - teardown must not raise
            logger.warning("channel: disposing meeting sessions failed: %s", e)

    async def _dispose_meeting_sessions_inner(self) -> None:
        sessions = self._meeting.dispose_all()
        await self._meeting.drain_sessions(sessions)

    async def _on_bg_loop(self, coro):
        """Run ``coro`` on the channel's background loop and await its result.

        Everything a session owns — the delivery queue, the polling tasks, the
        debounce timers — has to live on **one** loop, and it has to be the loop
        the dispatcher schedules onto. Building them on whatever loop happened
        to call ``join_meeting()`` puts an ``asyncio.Queue``'s consumer on one
        loop and its producer on another, and a put there simply never wakes the
        getter: events are accepted and silently never delivered.
        """
        self._ensure_bg_loop()
        # Deliberately not `schedule()`: that one logs anything the coroutine
        # raises, and the caller of this method already receives the exception.
        # A wrong meeting password would otherwise be reported twice, once as an
        # unhandled background failure.
        return await _await_on_loop(self._bg_loop, lambda: coro)

    def _meeting_bot_open_id(self) -> Optional[str]:
        identity = self._bot_identity
        return getattr(identity, "open_id", None) if identity is not None else None

    async def _meeting_ticket_interactive(
        self, *, user_open_id: str, prompt_context: Any
    ) -> str:
        uat = await require_user_auth(
            device_flow=self._device_flow,
            token_store=self._token_store,
            uat_config=self._config.uat,
            user_open_id=user_open_id,
            scopes=[_MEETING_EVENT_SCOPE],
            context=prompt_context,
        )
        return uat.access_token

    async def _meeting_ticket_quiet(self, user_open_id: str) -> str:
        """Per-round ticket lookup: cache and refresh only, never a prompt."""
        uat = await resolve_user_auth_non_interactive(
            device_flow=self._device_flow,
            token_store=self._token_store,
            uat_config=self._config.uat,
            user_open_id=user_open_id,
        )
        return uat.access_token

    async def _emit_meeting_invited(self, event: Any) -> None:
        await self._invoke("meetingInvited", event)

    def _report_raw_error(self, exc: BaseException) -> Any:
        return self._invoke("error", exc)

    async def require_user_auth(
        self,
        user_open_id: str,
        scopes: list,
        *,
        prompt_context: Any = None,
    ) -> UAT:
        """Resolve a user access token for ``user_open_id``, running the
        device flow if needed. ``prompt_context`` must expose
        ``respond(card)`` if the user needs a prompt card (usually the
        original interaction carrier).

        Runs on the channel's background loop, so every ticket resolution in the
        process contends on one per-user lock — including the meeting channel's
        per-round lookups. Those locks are ``asyncio.Lock``s bound to the loop
        that created them, so callers spread across loops would get no mutual
        exclusion at all, and the loser of a concurrent refresh has its still
        valid ticket deleted.

        Two consequences worth knowing: ``prompt_context.respond`` is invoked
        from that loop's thread, so an object bound to a different loop will not
        work; and a process that only ever calls this method still gets the
        channel's background thread.
        """
        # Routed onto the background loop so every ticket resolution in this
        # process — this one, and the meeting channel's per-round lookups —
        # contends on the same per-user lock. Those locks are `asyncio.Lock`s
        # bound to the loop that created them, so callers spread across loops
        # get no mutual exclusion at all; funnelling through one loop restores
        # it, which matters because the loser of a concurrent refresh has its
        # (still valid) ticket deleted.
        return await self._on_bg_loop(
            require_user_auth(
                device_flow=self._device_flow,
                token_store=self._token_store,
                uat_config=self._config.uat,
                user_open_id=user_open_id,
                scopes=scopes,
                context=prompt_context,
            )
        )

    def _track_sent_message(self, message_id: str) -> None:
        if not message_id:
            return
        self._sent_messages[message_id] = time.time()
        self._sent_messages.move_to_end(message_id)
        while len(self._sent_messages) > self._sent_messages_max:
            evicted, _ = self._sent_messages.popitem(last=False)
            self._sent_message_context.pop(evicted, None)

    def _remember_sent_message_context(
        self,
        result: SendResult,
        *,
        chat_id: Optional[str],
        receive_id_type: Optional[str],
    ) -> None:
        message_ids = list(result.chunk_ids or [])
        if result.message_id and result.message_id not in message_ids:
            message_ids.insert(0, result.message_id)
        for message_id in message_ids:
            self._track_sent_message(message_id)
            if not chat_id:
                self._sent_message_context.pop(message_id, None)
                continue
            self._sent_message_context[message_id] = _SentMessageContext(
                message_id=message_id,
                chat_id=chat_id,
                receive_id_type=receive_id_type,
            )
            self._sent_message_context.move_to_end(message_id)
