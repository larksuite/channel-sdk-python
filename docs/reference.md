# Channel Reference

This is the detailed reference for `FeishuChannel`. For first-run setup, see
the [Channel quickstart](./quickstart.md).

## Entry Point

```python
from lark_channel import FeishuChannel
```

`FeishuChannel` combines WebSocket or webhook event transport, inbound message
normalization, safety policy, deduplication, outbound sending, media
upload/download, streaming replies, and card helpers.

Use the lower-level WebSocket client (`lark_channel.ws.client.Client`), the
`EventDispatcherHandler`, or the OpenAPI `Client` directly when your integration
only needs raw event dispatch or direct OpenAPI calls.

## Minimal Example

```python
import asyncio
import os

from lark_channel import FeishuChannel

channel = FeishuChannel(
    app_id=os.environ["LARK_APP_ID"],
    app_secret=os.environ["LARK_APP_SECRET"],
)

async def on_message(msg):
    await channel.send(
        msg.chat_id,
        {"markdown": f"received: {msg.content_text}"},
        {"reply_to": msg.message_id},
    )

channel.on("message", on_message)

asyncio.run(channel.connect())
```

## Constructor Options

| Option | Required | Description |
|---|---:|---|
| `app_id` / `app_secret` | yes | Feishu app credentials |
| `domain` | no | Feishu, Lark, or custom OpenAPI domain |
| `log_level` | no | SDK log level |
| `transport` | no | `"ws"` by default, or `"webhook"` |
| `encrypt_key` | if configured | Webhook/event decryption key from the developer console |
| `verification_token` | if configured | Webhook/event verification token from the developer console |
| `policy` | no | `PolicyConfig` for DM/group admission and mention behavior |
| `safety` | no | `SafetyConfig` for dedup, stale window, batching, and per-chat queue |
| `inbound` | no | `InboundConfig` for normalization, media, names, and reaction behavior |
| `outbound` | no | `OutboundConfig` for chunking, retry, markdown conversion, SSRF, and streaming throttle |
| `uat` | no | `UATConfig` for user access token device-flow behavior |
| `security` | no | `SecurityConfig` for compat/audit/strict security behavior, audit logging, token cache fallback, and WebSocket limits |
| `token_store` | no | Custom user access token store |
| `dedup_store` | no | Pipeline-layer `DedupStore` |
| `safety_cache` | no | Safety-layer `ICache` |
| `name_lookup` | no | Custom `open_id` to display-name resolver |
| `config` | no | Prebuilt `ChannelConfig`; flat kwargs override touched fields |

```python
from lark_channel import (
    DedupConfig,
    FeishuChannel,
    OutboundConfig,
    RetryConfig,
    SafetyConfig,
    SecurityConfig,
)

channel = FeishuChannel(
    app_id="cli_xxx",
    app_secret="***",
    safety=SafetyConfig(dedup=DedupConfig(ttl_seconds=12 * 3600)),
    outbound=OutboundConfig(retry=RetryConfig(max_attempts=5)),
    security=SecurityConfig(mode="audit"),
)
```

## Lifecycle

| Method | Purpose |
|---|---|
| `await channel.connect()` | Start transport. In WebSocket mode, keep running until stopped. |
| `await channel.connect_until_ready(timeout=30)` | Start in the background and return after readiness. |
| `await channel.start_background(timeout=30)` | Alias-style background startup with explicit naming. |
| `await channel.disconnect()` | Drain safety batches and stop the transport. |
| `channel.start()` | Synchronous startup. In webhook mode, build the dispatcher and return. |
| `channel.stop()` | Synchronous teardown. |
| `await channel.wait_ready(timeout=30)` | Wait for readiness after startup. |

`start()` is synchronous and may block during initial setup, including bot
identity resolution. Prefer `connect_until_ready()` in async web framework
startup hooks.

## Event Listening

```python
from lark_channel import Events

channel.on(Events.MESSAGE, on_message)
channel.on(Events.CARD_ACTION, on_card_action)
channel.on(Events.REACTION, on_reaction)
channel.on(Events.BOT_ADDED, on_bot_added)
channel.on(Events.BOT_LEAVE, on_bot_leave)
channel.on(Events.MESSAGE_READ, on_message_read)
channel.on(Events.COMMENT, on_comment)
channel.on(Events.REJECT, on_reject)
channel.on(Events.RECONNECTING, on_reconnecting)
channel.on(Events.RECONNECTED, on_reconnected)
channel.on(Events.ERROR, on_error)
```

Dispatched event names:

| Event | Payload |
|---|---|
| `message` | `InboundMessage` |
| `cardAction` | `CardActionEvent` |
| `reaction` | `ReactionEvent` |
| `botAdded` | `BotAddedEvent` |
| `botLeave` | `BotLeaveEvent` |
| `messageRead` | `MessageReadEvent` |
| `comment` | `CommentEvent` |
| `reject` | `RejectEvent` |
| `reconnecting` | no argument |
| `reconnected` | no argument |
| `error` | exception or `OutboundSendError` |

Snake-case aliases such as `card_action`, `bot_added`, `bot_leave`, and
`message_read` are normalized for compatibility.

## Message Model

`message` handlers receive an `InboundMessage`.

| Field | Description |
|---|---|
| `message_id` / `id` | Feishu message id |
| `create_time` | Original event timestamp |
| `conversation` | `Conversation(chat_id, chat_type, thread_id)` |
| `chat_id` | Shortcut for `conversation.chat_id` |
| `chat_type` | `p2p`, `group`, `topic`, or `unknown` |
| `sender` | `Identity` for the sender |
| `sender_id` | Shortcut for `sender.open_id` |
| `sender_name` | Optional display name |
| `sender_type` | Raw sender kind (`user` / `bot` / `system` / `anonymous` / `app`), or `None` when the event omits it (see [Bot-at-bot](#bot-at-bot)) |
| `sender_is_bot` | `True` when the sender is a bot/app (`sender.is_bot`) |
| `mentions` | List of `Mention` objects |
| `mentioned_all` | Whether the message mentioned all members |
| `mentioned_bot` | Whether the message mentioned the bot |
| `reply_to_message_id` | Parent message id when present |
| `content` | Typed `MessageContent` dataclass |
| `content_text` | Flattened markdown/XML-style text (unchanged — keeps rendered mentions, incl. the bot's own) |
| `safe_content_text` | Escaped flattened text for security-sensitive rendering |
| `body_text` | `content_text` with the current bot's own `@`-mention removed — for command parsing / bare-`@` wake detection; equals `content_text` when the bot isn't mentioned (see [Bot-at-bot](#bot-at-bot)) |
| `resources` | Resource descriptors for download |
| `raw_content_type` | Original Feishu message type |
| `raw` | Original event payload |

## Security Configuration

`SecurityConfig` defaults to `mode="compat"` to preserve existing behavior.
Use `mode="audit"` to log security-sensitive legacy behavior without blocking
it, and `mode="strict"` to enforce stricter checks.

Common options:

| Option | Purpose |
|---|---|
| `audit_recorder` | Receives security audit calls as `record(reason, *, mode, action, details=None)` |
| `allow_unsigned_encrypted_webhook` | Allows encrypted webhook bodies with missing signature headers in strict mode; invalid signatures are still rejected |
| `allow_insecure_ws` | Allows remote `ws://` WebSocket endpoints in strict mode |
| `allow_local_insecure_ws` | Allows local `ws://` endpoints, enabled by default for local tests |
| `strict_error_response` | Uses a generic error response when strict handling is enabled |
| `strict_content_text` | Makes `content_text` use the escaped text form |
| `legacy_token_cache_fallback` | Controls fallback to the legacy token cache key |
| `max_ws_fragment_parts` / `max_ws_fragment_bytes` | Limits fragmented WebSocket payload assembly |
| `max_concurrent_ws_handlers` | Limits concurrent WebSocket handler tasks |
| `resource_overflow_policy` | Controls audit/drop behavior for WebSocket fragment limit overflow outside strict mode |

Recommended rollout:

1. Keep the default `mode="compat"` while changing imports and package names.
2. Use `mode="audit"` in staging or canary traffic to find security-sensitive
   legacy behavior.
3. Use `mode="strict"` after webhook signatures, WebSocket endpoints, and token
   cache behavior are verified.

```python
from lark_channel import FeishuChannel, SecurityConfig

channel = FeishuChannel(
    app_id="cli_xxx",
    app_secret="***",
    security=SecurityConfig(
        mode="strict",
        strict_content_text=True,
        max_ws_fragment_parts=128,
        max_ws_fragment_bytes=8 * 1024 * 1024,
    ),
)
```

See [Security configuration](./security.md) for examples and strict-mode
compatibility switches.

Effective defaults:

| Option | Default behavior |
|---|---|
| `strict_error_response` | `None`, which means enabled in strict mode and disabled otherwise |
| `legacy_token_cache_fallback` | `None`, which means disabled in strict mode and enabled otherwise |
| `max_ws_fragment_parts` / `max_ws_fragment_bytes` | `None`, no explicit SDK fragment limit |
| `max_concurrent_ws_handlers` | `None`, no explicit SDK handler concurrency limit |
| `resource_overflow_policy` | `"audit"` outside strict mode; strict mode drops WebSocket fragment overflows |

## Policy

Defaults:

- `dm_policy="open"`
- `group_policy="open"`
- `require_mention=True`
- `respond_to_mention_all=False`
- `sender_identity_fields=["open_id"]`

Runtime update:

```python
channel.update_policy(
    require_mention=False,
    respond_to_mention_all=True,
    group_policy="allowlist",
    group_allowlist=["oc_xxx"],
)
```

`update_policy()` accepts keyword changes for fields on `PolicyConfig`.

## Sending Messages

`channel.send(to, message, opts=None)` accepts:

- a bare string, treated as markdown;
- a dict;
- a typed `Outbound*` dataclass.

```python
await channel.send(chat_id, {"text": "plain text"})
await channel.send(chat_id, {"markdown": "hello **world**"})
await channel.send(chat_id, {"post": {"zh_cn": {"title": "", "content": []}}})
await channel.send(chat_id, {"card": {"schema": "2.0", "body": {"elements": []}}})
await channel.send(chat_id, {"image": {"source": "./image.png"}})
await channel.send(chat_id, {"file": {"source": b"content", "file_name": "a.txt"}})
await channel.send(chat_id, {"audio": {"source": "./audio.ogg"}})
await channel.send(chat_id, {"video": {"source": "./video.mp4"}})
await channel.send(chat_id, {"share_chat": {"chat_id": "oc_xxx"}})
await channel.send(chat_id, {"share_user": {"user_id": "ou_xxx"}})
await channel.send(chat_id, {"sticker": {"file_key": "file_v3_xxx"}})
```

`opts` may be a `SendOpts` object or a dict:

```python
await channel.send(
    chat_id,
    {"markdown": "please check"},
    {
        "reply_to": message_id,
        "reply_in_thread": True,
        "receive_id_type": "chat_id",
        "reply_target_gone": "fresh",
        "uuid": "optional-idempotency-key",
    },
)
```

For structured mentions, use typed outbound messages:

```python
from lark_channel import Identity, OutboundText

await channel.send(
    chat_id,
    OutboundText(
        text="please check",
        mentions=[Identity(open_id="ou_xxx", display_name="Alice")],
    ),
)
```

Media `source` accepts:

- HTTP(S) URL string, guarded by `OutboundConfig.ssrf_allowlist`;
- local file path string;
- `bytes`;
- existing media key string for the matching message kind: `img_...` for
  images, `file_...` for file/audio/video. Stickers use
  `{"sticker": {"file_key": ...}}` instead of media `source`.

Image and video messages support `caption`; file and audio captions are rejected
with `format_error`.

## Streaming

Markdown stream:

```python
async def producer(stream):
    for token in ["hello", " ", "world"]:
        await stream.append(token)

await channel.stream(chat_id, {"markdown": producer}, {"reply_to": message_id})
```

Card stream:

```python
async def producer(stream):
    await stream.update(next_card_json)

await channel.stream(
    chat_id,
    {"card": {"initial": initial_card_json, "producer": producer}},
)
```

For low-level CardKit preallocation, see [Streaming with CardKit](./cardkit-streaming.md).

## Helpers

| Method | Return | Notes |
|---|---|---|
| `await channel.update_card(message_id, card)` | `SendResult` | Replace a sent card message |
| `await channel.edit_message(message_id, message)` | `SendResult` | Text/post only |
| `await channel.recall_message(message_id)` | `SendResult` | Recall/delete a message |
| `await channel.add_reaction(message_id, emoji_type)` | `SendResult` | Add a reaction |
| `await channel.remove_reaction(message_id, reaction_id)` | `SendResult` | Remove a reaction by id |
| `await channel.download_resource(file_key, resource_type="image")` | `bytes \| None` | Returns `None` on API failure |
| `await channel.download_resource_to_file(...)` | `Path` | Raises `download_failed` when no body is returned |
| `await channel.get_chat_info(chat_id)` | `ChatInfo \| None` | Returns `None` on API failure |
| `await channel.get_chat_mode(chat_id)` | `str \| None` | Read `chat_mode` with cache and configured fallback |
| `await channel.resolve_quoted_contexts(messages, chat_mode="group")` | `dict[str, QuoteResolution]` | Resolve quoted parent context for a batch without refetching in-batch parents |
| `await channel.resolve_resource_to_cache(message_id="om_x", resource=resource)` | `CachedResource` | Download a message resource into the SDK-managed cache |
| `channel.block_batch_scope(scope)` / `unblock_batch_scope(scope)` / `cancel_batch_scope(scope)` | `None` | Pause, resume, or cancel debounced batches around an active run |
| `await channel.add_typing_reaction(message_id)` / `remove_typing_reaction(message_id, reaction_id)` | `str \| None` / `bool` | Best-effort IM message `Typing` reaction helpers |
| `await channel.resolve_comment_target(file_token="doc_x", file_type="docx")` / `get_comment_context(target=target, comment_id="c1")` / `reply_comment(context, "answer")` | `CommentTarget` / `CommentContext` / API result | Cloud document comment primitives for supported `doc`, `docx`, `sheet`, and `file` targets. `reply_comment` creates a whole-file comment when `context.is_whole` is true, or updates an existing reply when `context.target_reply_id` is present. |
| `channel.client` | `Client` | Underlying OpenAPI client |

## Error Handling

`send()` returns `SendResult`.

- Invalid input and transport/coercion failures may raise.
- Upstream send failures usually return `SendResult(success=False, error=...)`.
- Both raised errors and failed `SendResult.error` are forwarded to
  `channel.on("error", handler)`.

`stream()` and low-level CardKit helpers raise for controller or CardKit
failures.

Known `FeishuChannelErrorCode` values:

| Code | Meaning |
|---|---|
| `format_error` | Message/card schema rejected |
| `target_revoked` | Reply target no longer accepts replies |
| `rate_limited` | Upstream rate limit |
| `permission_denied` | Invalid credentials or missing scopes |
| `upload_failed` | Media upload failed |
| `download_failed` | Media download failed |
| `ssrf_blocked` | URL media download blocked by SSRF policy |
| `send_timeout` | Send/connect timeout |
| `not_connected` | Transport is not connected or startup failed |
| `unknown` | Uncategorized upstream or SDK error |

## Bot-at-bot

Support for multiple bots collaborating in one chat — agents `@`-ing each other
to hand off work. Everything here is **opt-in and additive**; default behavior
is unchanged.

**Know who sent it.** Every inbound message carries `sender_type`
(`user` / `bot` / …) and the convenience `sender_is_bot`, so an agent can tell a
human, itself, and another bot apart. Get the bot's own identity for its system
prompt with `channel.get_bot_identity()` → `BotIdentity(open_id, name, …)`
(raises `FeishuChannelError(code=not_connected)` before `connect()`). Set
`resolve_sender_names=True` to fill `sender_name` from the chat roster.

**Receiving events from other bots.** Feishu does **not** deliver "another bot
`@`-ed me" events unless the app has the
`im:message.group_at_msg.include_bot:readonly` permission enabled (distinct from
`im:message.group_at_msg:readonly`, which only covers *user* mentions) — and the
failure is silent. There is no API to self-check this; if bot-to-bot mentions
never arrive, verify that permission first (see the Feishu open-platform
"receive message events" documentation). An `@`-only ping (no body) still wakes
the bot: `mentioned_bot` is `True`.
`content_text` still renders the mention; use `body_text` (the bot's own mention
removed) to detect a bare poke: `msg.mentioned_bot and not msg.body_text.strip()`.

**Roster.**

| Method | Return | Notes |
|---|---|---|
| `await channel.get_chat_members(chat_id, *, page_size=100, max_pages=10, id_type="open_id", force=False)` | `list[ChatMember]` | A chat's **users** (Feishu filters bots out, so `is_bot` is never `True`); paginated + cached. `force` refetches |
| `await channel.get_chat_bots(chat_id, *, force=False)` | `list[ChatMember]` | The chat's **bots** (`is_bot=True`); cached; seeds the roster so a bot is `@`-able by name without first appearing in a mention |
| `channel.get_bot_identity()` | `BotIdentity` | This bot's own identity; raises `not_connected` before `connect()` |

`ChatMember` = `id`, `id_type`, `name`, `tenant_key`, `is_bot`. Provide a
`resolve_chat_members` config hook (`(chat_id) -> list[ChatMember] | None`, sync
or async) to source the roster from your own directory instead of the API.
**Both `None` and an empty list `[]` fall back to the API** — the hook only
overrides the roster when it returns a non-empty list
(return `None` to fall back to the API).

**Replying to the right place.** `await channel.reply(msg, message, opts=None)`
replies to `msg` and follows its shape: `reply_to` defaults to `msg.message_id`
and `reply_in_thread` defaults to whether the trigger was in a topic thread, so
a reply stays in a thread when the message was in one and stays flat when it
wasn't. `opts` overrides either default; `reply()` never promotes a flat message
into a thread on its own.

**`@`-mentioning by name.** Either pass structured mentions — a name-only
`Identity(open_id="", display_name="Alice")` on `OutboundText` / `OutboundPost`
is resolved to an open_id against the chat roster (dropped if it doesn't
resolve) — or set `SendOpts.resolve_mentions_in_text=True` to rewrite `@name`
tokens in a text / markdown body. A name that is unknown **or shared by more
than one member** is left as plain text and never mis-mentioned; for
security-sensitive handoffs, pass an explicit open_id. Roster names come from
`get_chat_members` (users), `get_chat_bots` (bots), and bots observed in earlier
inbound mentions.

**Restrict who can trigger the bot — by chat, not by sender.** Sender open_ids
are hard to get up front, so enumerating them in `allow_from` is impractical.
Instead allow only specific chats with `group_allowlist=['oc_…']` and require an
`@` with `require_mention=True`. `allow_from` takes **sender ids**
(`ou_…` / user_id / union_id); `group_allowlist` takes **chat ids** (`oc_…`); an
app id (`cli_…`) belongs in neither and logs a warning.

**Breaking ping-pong loops.** The opt-in `policy.bot_loop_guard` counts only
"another bot `@`-ed me" messages in a sliding window (a human message resets it)
and trips past a threshold:

```python
from lark_channel import BotLoopGuardConfig, PolicyConfig

policy = PolicyConfig(
    bot_loop_guard=BotLoopGuardConfig(
        enabled=True,
        window_ms=60_000,       # sliding window W
        max_bot_mentions=5,     # trip at N bot @-mentions in W
        scope="chat",           # or "chat+sender"
        on_trip="reject",       # "drop" (default) silently mutes; "reject" emits reject(reason="bot_loop")
    )
)
```

The default `on_trip="drop"` silently stops replying (one warning on the first
trip); prefer `"reject"` (emits a `reject` event with `reason="bot_loop"`) when
the app needs to know. This is a heuristic backstop, not a protocol-level
guarantee.

## Related Documents

- [Channel quickstart](./quickstart.md)
- [Migration from lark_oapi.channel](./migration-from-lark-oapi.md)
- [Security configuration](./security.md)
- [Webhook server adapter](./webhook-server.md)
- [Streaming with CardKit](./cardkit-streaming.md)
- [Markdown to post conversion](./markdown.md)
- [Two-layer dedup architecture](./dedup-architecture.md)
- [Release notes](./release-notes/v1.1.0.md)
