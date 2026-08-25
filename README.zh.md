# Lark Channel Python SDK

`lark-channel-sdk` 是用于构建飞书和 Lark 会话式机器人的 Python 包。它以
`FeishuChannel` 为主要入口，封装事件监听、消息归一化、策略控制、出站消息、
媒体处理、卡片回调和流式回复。

## 安装

```bash
pip install lark-channel-sdk
```

## 最小示例

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

## 文档

- [快速开始](docs/quickstart.md)
- [从 `lark_oapi.channel` 迁移](docs/migration-from-lark-oapi.md)
- [API 参考](docs/reference.md) —— 含 [Bot-at-bot](docs/reference.md#bot-at-bot)（多 bot 协作：发送方类型、群成员 roster、回复话题跟随、按名字 `@`、死循环守卫）
- [安全配置](docs/security.md)
- [Markdown 消息](docs/markdown.md)
- [Webhook 服务适配](docs/webhook-server.md)
- [CardKit 流式回复](docs/cardkit-streaming.md)
- [去重架构](docs/dedup-architecture.md)
- [会议通道](docs/meeting-channel.md) —— 让 Agent 在会议进行中感知与响应
- [发布说明](docs/release-notes/v1.1.0.md)
- [Echo bot 示例](samples/channel/echo_bot.py)
- [会议示例](samples/channel/) —— [入会](samples/channel/meeting_join_bot.py)、[不入会跟随](samples/channel/meeting_follow_agenda.py)

## 从 `lark_oapi.channel` 迁移

安装独立包并更新 import 路径：

```bash
pip install lark-channel-sdk
```

```python
from lark_channel import FeishuChannel
```

`lark-channel-sdk` 可以与 `lark-oapi` 安装在同一环境中。Channel bot 工作流使用
`lark-channel-sdk`；如果应用需要完整 OpenAPI SDK 能力，继续使用 `lark-oapi`。

详见 [迁移手册](docs/migration-from-lark-oapi.md)，其中包含 import 映射、运行时兼容
说明和迁移检查清单。

## 会议通道

让 Agent 在**会议进行中**感知内容（字幕、会中聊天、参会人进退、文档共享）并作出响应。两个入口，同一个会话类型。

```python
# Bot 作为真实参会者入会，可在会中发言
channel.on("meetingInvited", lambda inv: channel.join_meeting(inv.meeting_no))

# 或跟随用户当前所在的会议，不入会
session = await channel.follow_my_meeting(user_open_id="ou_...")

session.on("transcript", lambda e: notes.append(e.text))
session.on("chat", on_chat)
await session.send_message("已记录")   # 仅入会的会话可用
await session.leave()
```

使用前请注意三点：

- **Bot 自己发的会中消息会再推回来一次。** 所以处理会中聊天时要先写 `if event.self_echo:
  return`，否则 Bot 会不停地回应自己刚说的话。
- **断开连接不会让 Bot 退出会议。** `disconnect()` 只是断开事件通道，Bot 仍然留在会议
  里（正因为如此，重连之后能接着收事件）。进程真要退出前，请先对每个会话调用 `leave()`。
- **`follow_my_meeting` 会读到会议里所有人的发言，而 Bot 不出现在参会人名单里。**
  另外两点：传给 `user_open_id` 的必须是你已经自行确认过身份的那个人（SDK 只收到一个
  字符串，没法替你校验）；用户一次授权给出的是你的应用申请过的**全部**权限，不只是读
  会议。详见 [安全配置](docs/security.md#meeting-channel)。

事件的先后顺序、字幕从临时结果到定稿、同时最多能开多少个会话，以及收不到事件时怎么排查，
见 [会议通道](docs/meeting-channel.md)。

## 安全模式

`SecurityConfig` 默认使用兼容模式，便于已有机器人平滑迁移。生产发布建议先使用
audit 模式观察安全审计事件，再切换到 strict 模式：

```python
from lark_channel import FeishuChannel, SecurityConfig

channel = FeishuChannel(
    app_id="cli_xxx",
    app_secret="***",
    security=SecurityConfig(mode="audit"),
)
```

strict 模式行为和 webhook 兼容开关详见 [安全配置](docs/security.md)。

## 本地开发

```bash
pip install -e ".[test]"
python -m pytest
```

## 许可证

本发布包的许可证表达式为 `MIT AND BSD-3-Clause`：项目主体代码使用
MIT License，详见 [LICENSE](LICENSE)；vendored 第三方代码详见
[THIRD_PARTY_NOTICES.md](THIRD_PARTY_NOTICES.md)。
