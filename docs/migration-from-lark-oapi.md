# Migration from lark_oapi.channel

This guide covers moving Channel bot code from the monolithic
`larksuite/oapi-sdk-python` repository to the standalone `lark-channel-sdk`
package.

## What Changes

| Area | Old package | Standalone package |
|---|---|---|
| Distribution | `lark-oapi` | `lark-channel-sdk` |
| Primary import path | `lark_oapi.channel` | `lark_channel` |
| Main entry point | `FeishuChannel` | `FeishuChannel` |
| Full OpenAPI surface | Included in `lark-oapi` | Keep using `lark-oapi` when needed |

The standalone package is designed to install alongside `lark-oapi`. Use
`lark-channel-sdk` for Channel bot workflows. Keep `lark-oapi` in the same
environment if your application also imports unrelated OpenAPI resources.

## Install

```bash
pip install lark-channel-sdk
```

Optional framework extras are available for webhook adapters:

```bash
pip install "lark-channel-sdk[aiohttp]"
pip install "lark-channel-sdk[fastapi]"
pip install "lark-channel-sdk[flask]"
```

## Update Imports

Replace legacy Channel imports with the standalone root package:

```python
from lark_channel import FeishuChannel
```

Import public Channel configuration and types from the same package root:

```python
from lark_channel import (
    FeishuChannel,
    InboundConfig,
    OutboundConfig,
    PolicyConfig,
    SafetyConfig,
    SecurityConfig,
)
```

Avoid importing Channel symbols from internal module paths. The package root is
the documented stable import surface.

## Constructor and Runtime Compatibility

Most existing `FeishuChannel` constructor fields continue to map directly:

- `app_id`, `app_secret`, `domain`, `log_level`;
- `encrypt_key`, `verification_token`;
- `transport="ws"` or `transport="webhook"`;
- policy, safety, inbound, outbound, media cache, token store, and dedup store
  configuration.

The default `domain` is `https://open.feishu.cn`. Lark tenants should pass
`domain="https://open.larksuite.com"` or keep their existing custom domain
override.

The standalone package also exposes `SecurityConfig`. It defaults to
`mode="compat"` for migration safety. Use `mode="audit"` to observe
security-sensitive legacy behavior before moving production traffic to
`mode="strict"`.

```python
from lark_channel import FeishuChannel, SecurityConfig

channel = FeishuChannel(
    app_id="cli_xxx",
    app_secret="***",
    security=SecurityConfig(mode="audit"),
)
```

## WebSocket Bots

WebSocket bots can usually migrate by changing imports and package
installation. Keep the same event subscriptions, credentials, and app
permissions.

```python
import asyncio

from lark_channel import FeishuChannel

channel = FeishuChannel(app_id="cli_xxx", app_secret="***")


async def on_message(msg):
    await channel.send(msg.chat_id, {"text": f"echo: {msg.content_text}"})


channel.on("message", on_message)
asyncio.run(channel.connect())
```

In WebSocket mode, the SDK requests `domain + "/callback/ws/endpoint"` to get
the server-provided WebSocket connection URL. Applications do not need to expose
their own WebSocket route.

## Webhook Bots

Webhook bots still own the HTTP server in the application or gateway layer.
Create the channel with `transport="webhook"` and pass request headers and body
bytes to `handle_webhook_request(...)`.

```python
from lark_channel import FeishuChannel

channel = FeishuChannel(
    app_id="cli_xxx",
    app_secret="***",
    encrypt_key="...",
    verification_token="...",
    transport="webhook",
)
```

See [Webhook server adapter](./webhook-server.md) for aiohttp and FastAPI
examples.

Webhook routes remain application-owned. Keep or choose your HTTP path, expose
it publicly through your web framework or gateway, and configure that complete
callback URL in the developer console.

## OpenAPI Calls Outside Channel

The standalone package includes only the OpenAPI models and resources needed by
Channel workflows. If your application uses unrelated OpenAPI resources, keep
those imports on `lark-oapi` and use `lark-channel-sdk` only for Channel
workflows.

## Migration Checklist

- Install `lark-channel-sdk`.
- Replace legacy Channel imports with `lark_channel` imports.
- Keep `lark-oapi` installed if the application uses non-Channel OpenAPI
  resources.
- Run your bot test suite with `SecurityConfig(mode="compat")`.
- Run staging traffic with `SecurityConfig(mode="audit")` and review audit
  events.
- Move to `SecurityConfig(mode="strict")` after webhook signatures, WebSocket
  endpoint behavior, and token-cache behavior are verified.
- Rebuild and verify package metadata with `python -m build` and
  `python -m twine check dist/*` before publishing your own downstream package.

## Related Documents

- [Quickstart](./quickstart.md)
- [Channel reference](./reference.md)
- [Security configuration](./security.md)
