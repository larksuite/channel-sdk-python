# Channel Security Configuration

`SecurityConfig` controls how the Channel SDK handles security-sensitive
compatibility behavior. The default is `mode="compat"` so existing applications
continue to run after migrating to the standalone package.

Use `mode="audit"` first when you want to see legacy behavior without blocking
traffic. Move to `mode="strict"` after the audit events are understood and your
webhook, WebSocket, and token-cache paths are ready for enforcement.

```python
from lark_channel import FeishuChannel, SecurityConfig

channel = FeishuChannel(
    app_id="cli_xxx",
    app_secret="***",
    encrypt_key="...",
    verification_token="...",
    transport="webhook",
    security=SecurityConfig(mode="audit"),
)
```

## Modes

| Mode | Behavior |
|---|---|
| `compat` | Preserves legacy behavior and does not emit default audit warnings. |
| `audit` | Allows legacy behavior, but records security audit events when an audit recorder is configured. |
| `strict` | Enforces stricter checks and uses generic error responses by default. |

## Common Production Settings

```python
from lark_channel import FeishuChannel, SecurityConfig

channel = FeishuChannel(
    app_id="cli_xxx",
    app_secret="***",
    transport="webhook",
    encrypt_key="...",
    verification_token="...",
    security=SecurityConfig(
        mode="strict",
        max_ws_fragment_parts=128,
        max_ws_fragment_bytes=8 * 1024 * 1024,
        max_concurrent_ws_handlers=256,
    ),
)
```

In strict mode:

- encrypted webhook events must have a valid request signature before decrypt;
- remote `ws://` endpoints are rejected unless `allow_insecure_ws=True`;
- local `ws://` endpoints remain allowed by default for local tests;
- webhook and card errors return a generic response unless
  `strict_error_response=False`;
- `legacy_token_cache_fallback` defaults to disabled.

## Webhook Compatibility Switches

`allow_unsigned_encrypted_webhook=True` permits encrypted webhook payloads that
are missing request signature headers in strict mode. It does not allow invalid
signatures: if signature headers are present but verification fails, strict mode
still rejects the request before decrypt. Use this only as a temporary
compatibility switch while confirming developer-console and gateway behavior.
When enabled for a missing-signature request, the SDK records an allow action
through the configured audit recorder.

```python
from lark_channel import SecurityConfig

security = SecurityConfig(
    mode="strict",
    allow_unsigned_encrypted_webhook=True,
)
```

## Text Rendering

`InboundMessage.content_text` keeps the legacy flattened text by default.
`InboundMessage.safe_content_text` is always available for security-sensitive
rendering. Set `strict_content_text=True` when you want `content_text` itself to
use the escaped safe form.

```python
from lark_channel import SecurityConfig

security = SecurityConfig(
    mode="strict",
    strict_content_text=True,
)
```

## Audit Recorder

Pass a custom recorder when audit events should go to your own logging or
metrics system. The recorder only needs a callable `record(...)` method with
the same argument shape used below.

```python
class AuditRecorder:
    def record(self, reason, *, mode, action, details=None):
        print(reason, mode, action, details or {})


security = SecurityConfig(mode="audit", audit_recorder=AuditRecorder())
```

See the [Channel reference](./reference.md#security-configuration) for the full
option table.

## User access tokens

`require_user_auth` and `follow_my_meeting` act under a **user's** authorization
rather than the app's. Three properties of that are yours to handle.

**The open_id decides whose authorization is used, and the SDK cannot check it.**
It receives a string and looks up whatever ticket is filed under it, so passing
a user-controlled value acts as that person — without notifying them. The
`user_open_id` you pass must be somebody you have already established is the
requester, and `prompt_context` must belong to that same person: the
authorization card carries a one-time grant, so sending it elsewhere lets a
different person authorize *their* account while the resulting ticket is filed
under the first one's id.

**The granted scope is wider than the call suggests.** A call may ask for
`vc:meeting.meetingevent:read`, but the device flow issues a ticket carrying
every scope the application applied for — commonly calendar, documents and IM
as well. That ticket is stored per user and reused by anything else in the
process that resolves a ticket for the same user, for as long as it stays valid.
Where it is stored is your choice: the default `InMemoryTokenStore` keeps it in
process memory and loses it on restart, `FileTokenStore` writes plaintext and is
development-only, and production wants your own `TokenStore` over a secret
manager.

**Resolution runs on the channel's background loop**, serialized per user, so a
concurrent refresh cannot take a valid authorization away from its owner. Two
consequences: `prompt_context.respond` is invoked from that loop's thread, so an
object bound to a different event loop will not work; and a process that only
calls `require_user_auth` still gets the channel's background thread.

## Paths outside the message policy

Two entry points reach your handlers without passing through `PolicyConfig`,
`SeenCache` dedup, the processing lock or the loop guard. Both are deliberate,
and both default to open:

- **`on_raw_event`** — subscribing to a type the channel already handles opens an
  unpoliced path into that type. With `dm_policy="allowlist"` set, a raw
  subscription to `im.message.receive_v1` still receives direct messages from
  everybody.
- **`meetingInvited`** — the only way into a joined meeting, triggered by anybody
  who can add the bot to one. Gate it with
  `MeetingChannelConfig.invite_allowlist`.

## Meeting channel

`follow_my_meeting` reads a meeting under a user's own authorization — see
[User access tokens](#user-access-tokens) for what that authorization actually
covers — and the bot is **not visible in the meeting**. It collects every
participant's speech for as long as the meeting lasts. Informing them is the
integrating application's responsibility; this SDK does not prompt, and cannot.
The first call in a process logs a warning to that effect.
`MeetingChannelConfig.follow_allowlist` gates it by open_id, but defaults to
`None` (open) — an opt-in, not a safety net you already have.

Two values on this path are credentials that do not look like one:

- **`console_url`**, which a permission failure may carry in
  `FeishuChannelError.context`, is a signed one-click authorization link — a
  capability, not a help page. The redaction layer masks it in logs; it cannot
  mask it in your own output. Never echo it into a chat message, a web page or a
  support ticket.
- **Meeting passwords**, both the one you pass to `join_meeting` and the one some
  meeting responses hand back. Neither reaches logs, `raw` payloads, error
  objects or the session.
