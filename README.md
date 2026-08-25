# Lark Channel SDK for Python

`lark-channel-sdk` is a Python package for building Feishu and Lark
conversational bots. It provides `FeishuChannel` as the main entry point for
event listening, message normalization, policy control, outbound messaging,
media handling, card callbacks, and streaming replies.

## Install

```bash
pip install lark-channel-sdk
```

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

## Documentation

- [Quickstart](docs/quickstart.md)
- [Migration from `lark_oapi.channel`](docs/migration-from-lark-oapi.md)
- [API reference](docs/reference.md) — including [Bot-at-bot](docs/reference.md#bot-at-bot) (multi-bot collaboration: sender type, roster, reply threading, `@`-by-name, loop guard)
- [Security configuration](docs/security.md)
- [Markdown messages](docs/markdown.md)
- [Webhook server adapter](docs/webhook-server.md)
- [CardKit streaming](docs/cardkit-streaming.md)
- [Deduplication architecture](docs/dedup-architecture.md)
- [Meeting channel](docs/meeting-channel.md) — agents that perceive and respond inside a live meeting
- [Release notes](docs/release-notes/v1.1.0.md)
- [Echo bot sample](samples/channel/echo_bot.py)
- [Meeting samples](samples/channel/) — [join a meeting](samples/channel/meeting_join_bot.py), [follow one without joining](samples/channel/meeting_follow_agenda.py)

## Migration from `lark_oapi.channel`

Install the standalone package and update the import path:

```bash
pip install lark-channel-sdk
```

```python
from lark_channel import FeishuChannel
```

`lark-channel-sdk` can be installed alongside `lark-oapi`. Use
`lark-channel-sdk` for Channel bot workflows and keep using `lark-oapi` when
your application needs the full OpenAPI SDK surface.

See the [migration guide](docs/migration-from-lark-oapi.md) for import mapping,
runtime compatibility notes, and a migration checklist.

## Meeting Channel

Agents that perceive and respond **inside a live meeting** — captions, meeting
chat, participants, shared documents. Two entry points, one session type.

```python
# The bot joins as a real participant and can speak into the meeting.
channel.on("meetingInvited", lambda inv: channel.join_meeting(inv.meeting_no))

# Or follow the meeting a user is already in, without joining it.
session = await channel.follow_my_meeting(user_open_id="ou_...")

session.on("transcript", lambda e: notes.append(e.text))
session.on("chat", on_chat)
await session.send_message("noted")   # joined sessions only
await session.leave()
```

Three things to know before you start:

- **The bot's own in-meeting messages come back to it.** So a meeting-chat
  handler needs `if event.self_echo: return`, or the bot spends the meeting
  replying to itself at network speed.
- **Disconnecting does not take the bot out of the meeting.** `disconnect()`
  closes the event channel; the bot stays a participant, which is what makes
  reconnects safe. A process that is really exiting should `leave()` each
  session first.
- **`follow_my_meeting` reads what every participant says, and the bot is not
  in the participant list.** Two more things: the `user_open_id` you pass has
  to be somebody you have already established is the requester — the SDK
  receives a string and cannot check that for you — and what the user grants
  in one go is
  **every** scope your app applied for, not just the meeting read. See
  [Security configuration](docs/security.md#meeting-channel).

For event ordering, how captions settle from interim to final, how many
sessions can run at once, and what to check when nothing arrives, see
[Meeting channel](docs/meeting-channel.md).

## Security Mode

`SecurityConfig` defaults to compatibility mode so existing bots continue to
run during migration. For production rollout, start with audit mode and then
move to strict mode after reviewing audit events:

```python
from lark_channel import FeishuChannel, SecurityConfig

channel = FeishuChannel(
    app_id="cli_xxx",
    app_secret="***",
    security=SecurityConfig(mode="audit"),
)
```

See [Security configuration](docs/security.md) for strict-mode behavior and
webhook compatibility switches.

## Local Development

```bash
pip install -e ".[test]"
python -m pytest
```

## License

This distribution is licensed as `MIT AND BSD-3-Clause`: project code is
licensed under the MIT License as described in [LICENSE](LICENSE), and vendored
third-party code is documented in
[THIRD_PARTY_NOTICES.md](THIRD_PARTY_NOTICES.md).
