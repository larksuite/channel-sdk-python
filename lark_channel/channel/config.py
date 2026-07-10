"""Configuration for :class:`FeishuChannel`.

**One schema**: ``policy`` / ``safety`` / ``inbound`` / ``outbound`` / ``uat``
live at the top level of :class:`ChannelConfig`. Names follow Python
conventions — ``snake_case`` throughout and explicit time-unit suffixes
(``ttl_seconds``, ``delay_ms``, …). What you configure is what consumers read.

Typical construction::

    channel = FeishuChannel(app_id="cli_xxx", app_secret="***")

    channel = FeishuChannel(
        app_id="cli_xxx",
        app_secret="***",
        safety=SafetyConfig(
            dedup=DedupConfig(ttl_seconds=12 * 3600, max_entries=10_000),
            text_batch=TextBatchConfig(delay_ms=800),
        ),
        outbound=OutboundConfig(
            text_chunk_limit=2000,
            retry=RetryConfig(max_attempts=5, base_delay_ms=250),
            ssrf_allowlist=["cdn.example.com"],
        ),
    )
"""

from dataclasses import dataclass, field
from typing import Any, Awaitable, Callable, Dict, List, Literal, Optional, Union

from lark_channel.core.const import FEISHU_DOMAIN
from lark_channel.core.enum import LogLevel


# ---------------------------------------------------------------------------
# Safety-layer primitives
# ---------------------------------------------------------------------------
# These used to live in safety/types.py, but the safety package eagerly
# imports SafetyPipeline which needs PolicyConfig from this module — a
# circular import. Defining them here breaks the cycle; the safety package
# imports them from here.


@dataclass
class DedupConfig:
    """Dedup configuration shared by the pipeline ``Deduper`` and the safety
    ``SeenCache``. ``enabled`` only gates the pipeline-layer ``Deduper``; the
    safety-layer ``SeenCache`` always runs."""

    enabled: bool = True
    ttl_seconds: int = 12 * 3600
    max_entries: int = 5000
    sweep_seconds: int = 5 * 60


@dataclass
class TextBatchConfig:
    """Debounce + merge successive text messages in the same chat."""

    delay_ms: int = 600
    long_threshold_chars: int = 1000
    long_delay_ms: int = 2000
    max_messages: int = 8
    max_chars: int = 4000


@dataclass
class ChatQueueConfig:
    """Per-chat serialization queue — forces one-handler-at-a-time per chat."""

    enabled: bool = True
    merge_while_busy: bool = True

# ---------------------------------------------------------------------------
# Literal type aliases
# ---------------------------------------------------------------------------

ReactionNotifications = Literal["off", "own", "all"]
ReplyModeValue = Literal["auto", "static", "streaming"]
ChunkMode = Literal["newline", "paragraph", "none"]
TableMode = Literal["table", "bullets", "code", "off"]
TagMdMode = Literal["structured", "native"]
TransportKind = Literal["ws", "webhook"]
DmPolicy = Literal["open", "allowlist", "blocklist", "disabled"]
GroupPolicy = Literal["open", "allowlist", "blocklist", "admin_only", "disabled"]
SenderIdentityField = Literal["open_id", "user_id", "union_id"]
ChatModeFallback = Optional[Literal["unknown", "group"]]
SecurityMode = Literal["compat", "audit", "strict"]
ResourceOverflowPolicy = Literal["audit", "drop"]


# ---------------------------------------------------------------------------
# Policy
# ---------------------------------------------------------------------------


@dataclass
class GroupOverride:
    """Per-chat overrides for a single group chat_id."""

    policy: Optional[GroupPolicy] = None
    allowlist: Optional[List[str]] = None
    blocklist: Optional[List[str]] = None
    require_mention: Optional[bool] = None
    respond_to_mention_all: Optional[bool] = None
    reply_mode: Optional[ReplyModeValue] = None
    history_limit: Optional[int] = None
    enabled: Optional[bool] = None


@dataclass
class BotLoopGuardConfig:
    """Opt-in heuristic guard against two bots ``@``-ing each other in an
    endless ping-pong. Off by default.

    Only "another bot @'d me" messages count; a human message resets the count.
    When the count reaches ``max_bot_mentions`` inside the ``window_ms`` sliding
    window the guard trips. The default ``on_trip='drop'`` **silently** stops
    replying (one warn on the first trip); prefer ``'reject'`` (emits a
    ``reject`` event with ``reason='bot_loop'``) when the app needs to know.
    A heuristic backstop, not a protocol-level guarantee.
    """

    enabled: bool = False
    window_ms: int = 60000
    max_bot_mentions: int = 5
    scope: Literal["chat", "chat+sender"] = "chat"
    on_trip: Literal["drop", "reject"] = "drop"


@dataclass
class PolicyConfig:
    """Admission / routing policy for inbound messages.

    Fields:

    - ``dm_policy`` / ``group_policy`` —
      ``open`` | ``allowlist`` | ``blocklist`` | ``admin_only`` | ``disabled``
      (``admin_only`` only valid for groups)
    - ``require_mention`` — only respond in group chats when @Bot is mentioned
    - ``respond_to_mention_all`` — treat ``@all`` as a valid mention
    - ``allow_from`` / ``deny_from`` — sender identities allowed/denied
      under DM ``allowlist``/``blocklist`` modes, matched by
      ``sender_identity_fields``
    - ``group_allowlist`` / ``group_blocklist`` — chat_ids allowed/denied
      under group ``allowlist``/``blocklist`` modes. ``group_allowlist`` takes
      **chat ids** (``oc_…``); paired with ``require_mention`` it scopes the bot
      to specific rooms without enumerating every sender — a lightweight
      "allowFrom" for bot-at-bot. ``allow_from`` takes **sender ids** (``ou_…``
      / user_id / union_id). An app id (``cli_…``) belongs in neither list and
      matches nothing (the SDK logs a warning if it sees one)
    - ``admins`` — sender identities that bypass every gate (always allowed;
      required sender list for ``admin_only`` group policy), matched by
      ``sender_identity_fields``
    - ``sender_identity_fields`` — identity fields used for sender-based
      allow/block/admin lists; group chat allow/block lists remain chat_id based
    - ``group_overrides`` — per-chat overrides keyed by chat_id
    """

    dm_policy: DmPolicy = "open"
    group_policy: GroupPolicy = "open"
    require_mention: bool = True
    respond_to_mention_all: bool = False
    allow_from: Optional[List[str]] = None
    deny_from: Optional[List[str]] = None
    group_allowlist: Optional[List[str]] = None
    group_blocklist: Optional[List[str]] = None
    admins: Optional[List[str]] = None
    sender_identity_fields: List[SenderIdentityField] = field(
        default_factory=lambda: ["open_id"]
    )
    group_overrides: Dict[str, GroupOverride] = field(default_factory=dict)
    bot_loop_guard: Optional[BotLoopGuardConfig] = None


# ---------------------------------------------------------------------------
# Inbound
# ---------------------------------------------------------------------------


@dataclass
class MediaCapabilities:
    image: bool = True
    audio: bool = True
    video: bool = True
    file: bool = True
    sticker: bool = True


@dataclass
class NameCacheConfig:
    enabled: bool = True
    max_size: int = 2000
    ttl_seconds: int = 24 * 3600


@dataclass
class InboundConfig:
    """Inbound-pipeline behaviour."""

    expand_merge_forward: bool = True
    fetch_interactive_card: bool = True
    reaction_notifications: ReactionNotifications = "own"
    media_capabilities: MediaCapabilities = field(default_factory=MediaCapabilities)
    media_max_mb: Optional[int] = None
    name_cache: NameCacheConfig = field(default_factory=NameCacheConfig)
    merge_forward_max_depth: int = 3
    merge_forward_max_items: int = 50
    drop_self_sent: bool = True
    inject_chat_mode: bool = False
    include_raw: bool = True
    emit_raw_events: bool = False


# ---------------------------------------------------------------------------
# Safety
# ---------------------------------------------------------------------------


@dataclass
class SafetyConfig:
    """Safety-pipeline configuration.

    Groups dedup, per-chat queue, text batching, and the stale-cutoff window.
    ``DedupConfig``, ``TextBatchConfig``, and ``ChatQueueConfig`` live in this
    module; ``MediaBatchConfig`` lives in :mod:`lark_channel.channel.safety.types`
    and is imported lazily to avoid a circular import through the safety
    package.
    """

    dedup: DedupConfig = field(default_factory=DedupConfig)
    text_batch: TextBatchConfig = field(default_factory=TextBatchConfig)
    media_batch: Any = field(
        default_factory=lambda: _media_batch_default()
    )
    chat_queue: ChatQueueConfig = field(default_factory=ChatQueueConfig)
    stale_message_window_ms: int = 30 * 60 * 1000


def _media_batch_default():
    """Avoid importing MediaBatchConfig at module top to prevent circular
    imports back from the safety package."""
    from .safety.types import MediaBatchConfig
    return MediaBatchConfig()


# ---------------------------------------------------------------------------
# Outbound
# ---------------------------------------------------------------------------


@dataclass
class FooterConfig:
    status: bool = False
    elapsed: bool = False
    tokens: bool = False
    model: bool = False
    cache: bool = False
    context: bool = False


@dataclass
class StreamThrottleConfig:
    min_chars: int = 20
    max_chars: int = 200
    idle_ms: int = 300


@dataclass
class MarkdownConverter:
    enabled: bool = True
    table_mode: TableMode = "off"
    tag_md_mode: TagMdMode = "native"


@dataclass
class PerChatReplyMode:
    default: ReplyModeValue = "auto"
    dm: Optional[ReplyModeValue] = None
    group: Optional[ReplyModeValue] = None


@dataclass
class RetryConfig:
    """Outbound retry behaviour (see :mod:`..outbound.retry`)."""

    max_attempts: int = 3
    base_delay_ms: int = 500


@dataclass
class OversizeContext:
    """Passed to ``OutboundConfig.on_oversize`` when an outbound text exceeds
    ``text_chunk_limit``.

    Hook contract:

    - Return None or empty string -> SDK falls back to default chunking.
    - Return a non-empty string -> SDK sends that as a single replacement
      message; the original long text is dropped.
    - Raise -> exception propagates to ``channel.send(...)`` caller; no
      silent fallback.
    """

    text: str
    chat_id: str
    receive_id_type: str
    estimated_chunks: int


@dataclass
class OutboundConfig:
    """Outbound-pipeline behaviour: chunking, streaming, retries, SSRF, oversize hook."""

    reply_mode: Union[ReplyModeValue, PerChatReplyMode] = "auto"
    text_chunk_limit: int = 3500
    chunk_mode: ChunkMode = "newline"
    stream_initial_text: str = ""
    stream_throttle: StreamThrottleConfig = field(default_factory=StreamThrottleConfig)
    footer: FooterConfig = field(default_factory=FooterConfig)
    markdown_converter: MarkdownConverter = field(default_factory=MarkdownConverter)
    retry: RetryConfig = field(default_factory=RetryConfig)
    # Hostname allowlist for URL-sourced media downloads. Required by the
    # SSRF guard — without an allowlist, URL downloads are refused. See
    # :mod:`..outbound.media.ssrf_guard` for the rationale.
    ssrf_allowlist: Optional[List[str]] = None
    on_oversize: Optional[
        Callable[[OversizeContext], "Awaitable[Optional[str]]"]
    ] = None


# ---------------------------------------------------------------------------
# UAT (user access token)
# ---------------------------------------------------------------------------


@dataclass
class UATConfig:
    """User access token (device-flow) behaviour."""

    allowed_scopes: Optional[List[str]] = None
    blocked_scopes: Optional[List[str]] = None
    refresh_before_expiry_seconds: int = 300
    device_poll_interval_seconds: int = 5


# ---------------------------------------------------------------------------
# Transport
# ---------------------------------------------------------------------------


@dataclass
class ChatModeCacheConfig:
    enabled: bool = True
    max_size: int = 2048
    ttl_seconds: int = 24 * 3600
    fallback: ChatModeFallback = None


@dataclass
class MediaCacheConfig:
    enabled: bool = True
    root_dir: Optional[Any] = None
    ttl_seconds: int = 7 * 24 * 3600
    max_entries: int = 128
    max_bytes: int = 512 * 1024 * 1024
    max_file_bytes: int = 50 * 1024 * 1024
    image_max_bytes: int = 20 * 1024 * 1024


@dataclass
class KeepaliveConfig:
    enabled: bool = False
    check_interval_seconds: float = 30.0
    wake_threshold_seconds: float = 90.0
    probe_timeout_seconds: float = 5.0
    failure_threshold: int = 2


@dataclass
class TransportConfig:
    kind: TransportKind = "ws"
    auto_reconnect: bool = True
    headers: Optional[Dict[str, str]] = None
    http_timeout_seconds: float = 30.0
    proxy_url: Optional[str] = None
    trust_env_proxy: Optional[bool] = None
    keepalive: KeepaliveConfig = field(default_factory=KeepaliveConfig)
    handshake_timeout_seconds: Optional[float] = None

    # WS tuning (pingInterval / reconnectInterval / reconnectNonce / etc.) is
    # NOT exposed here intentionally: the Feishu WS endpoint delivers a
    # server-authoritative ClientConfig on every handshake (and may push
    # updates mid-session via CONTROL frames) which overrides any client-side
    # values. User-supplied overrides would be silently replaced and create
    # a false-control footgun.


# ---------------------------------------------------------------------------
# Security
# ---------------------------------------------------------------------------


@dataclass
class SecurityConfig:
    mode: SecurityMode = "compat"
    audit_recorder: Any = field(
        default_factory=lambda: _security_audit_recorder_default()
    )
    allow_unsigned_encrypted_webhook: bool = False
    allow_insecure_ws: bool = False
    allow_local_insecure_ws: bool = True
    strict_error_response: Optional[bool] = None
    strict_content_text: bool = False
    legacy_token_cache_fallback: Optional[bool] = None
    max_ws_fragment_parts: Optional[int] = None
    max_ws_fragment_bytes: Optional[int] = None
    max_concurrent_ws_handlers: Optional[int] = None
    resource_overflow_policy: ResourceOverflowPolicy = "audit"

    def __post_init__(self) -> None:
        if self.mode not in ("compat", "audit", "strict"):
            raise ValueError("security mode must be 'compat', 'audit', or 'strict'")
        if self.resource_overflow_policy not in ("audit", "drop"):
            raise ValueError("resource_overflow_policy must be 'audit' or 'drop'")
        if not callable(getattr(self.audit_recorder, "record", None)):
            raise TypeError("audit_recorder must provide a callable record method")
        for field_name in (
            "allow_unsigned_encrypted_webhook",
            "allow_insecure_ws",
            "allow_local_insecure_ws",
            "strict_content_text",
        ):
            value = getattr(self, field_name)
            if not isinstance(value, bool):
                raise TypeError(f"{field_name} must be a boolean")
        for field_name in ("strict_error_response", "legacy_token_cache_fallback"):
            value = getattr(self, field_name)
            if value is not None and not isinstance(value, bool):
                raise TypeError(f"{field_name} must be a boolean or None")
        for field_name in (
            "max_ws_fragment_parts",
            "max_ws_fragment_bytes",
            "max_concurrent_ws_handlers",
        ):
            value = getattr(self, field_name)
            if value is not None and (
                not isinstance(value, int) or isinstance(value, bool)
            ):
                raise TypeError(f"{field_name} must be a positive integer")
            if value is not None and value <= 0:
                raise ValueError(f"{field_name} must be a positive integer")

    @property
    def is_compat(self) -> bool:
        return self.mode == "compat"

    @property
    def is_audit(self) -> bool:
        return self.mode == "audit"

    @property
    def is_strict(self) -> bool:
        return self.mode == "strict"

    @property
    def enforce_strict_error_response(self) -> bool:
        if self.strict_error_response is not None:
            return self.strict_error_response
        return self.is_strict

    @property
    def effective_legacy_token_cache_fallback(self) -> bool:
        if self.legacy_token_cache_fallback is not None:
            return self.legacy_token_cache_fallback
        return not self.is_strict


def _security_audit_recorder_default():
    from lark_channel.event.security import SecurityAuditRecorder
    recorder = SecurityAuditRecorder()
    recorder._lark_channel_default_recorder = True
    return recorder


# ---------------------------------------------------------------------------
# Top-level
# ---------------------------------------------------------------------------


@dataclass
class ChannelConfig:
    """Top-level configuration for :class:`FeishuChannel`.

    Groups the five functional areas (``policy`` / ``safety`` / ``inbound`` /
    ``outbound`` / ``uat``) plus transport settings and the security fields
    (``encrypt_key`` / ``verification_token``) used by the event dispatcher.
    Every field has a sensible default; callers typically only need to set
    ``app_id`` and ``app_secret`` for a minimal setup.
    """

    app_id: str = ""
    app_secret: str = ""
    domain: str = FEISHU_DOMAIN
    log_level: LogLevel = LogLevel.INFO

    # Event dispatcher uses these regardless of transport kind; webhook mode
    # also uses ``verification_token`` for request signature verification.
    encrypt_key: Optional[str] = None
    verification_token: Optional[str] = None

    transport: TransportConfig = field(default_factory=TransportConfig)
    chat_mode_cache: ChatModeCacheConfig = field(default_factory=ChatModeCacheConfig)

    policy: PolicyConfig = field(default_factory=PolicyConfig)
    safety: SafetyConfig = field(default_factory=SafetyConfig)
    inbound: InboundConfig = field(default_factory=InboundConfig)
    outbound: OutboundConfig = field(default_factory=OutboundConfig)
    uat: UATConfig = field(default_factory=UATConfig)

    # Hook for testing / custom HTTP transport.
    http_executor: Optional[Callable] = None
    media_cache: MediaCacheConfig = field(default_factory=MediaCacheConfig)
    security: SecurityConfig = field(default_factory=SecurityConfig)

    # Bot-at-bot knobs. Appended at the end so existing positional field order
    # (guarded by test_security_config) stays stable for positional callers.
    #
    # ``resolve_sender_names``: populate ``InboundMessage.sender_name`` by
    # resolving the sender's display name from the chat's member roster (warmed
    # via a cached ``get_chat_members``). Off by default → no extra roster API.
    resolve_sender_names: bool = False
    # ``resolve_chat_members``: override how ``get_chat_members`` sources a chat's
    # roster. When provided and it returns a member list, that list is used and
    # the Feishu API call is skipped; return ``None`` to fall back to the API.
    # The callable takes a ``chat_id`` and may be sync or async; results still
    # flow through the internal roster cache. (Typed ``Any`` to avoid importing
    # ``ChatMember`` from ``types`` — that would create an import cycle back
    # through this module.)
    resolve_chat_members: Optional[Callable[[str], Any]] = None
