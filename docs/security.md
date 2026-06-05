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
