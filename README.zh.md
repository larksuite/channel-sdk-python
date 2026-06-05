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
- [API 参考](docs/reference.md)
- [安全配置](docs/security.md)
- [Markdown 消息](docs/markdown.md)
- [Webhook 服务适配](docs/webhook-server.md)
- [CardKit 流式回复](docs/cardkit-streaming.md)
- [去重架构](docs/dedup-architecture.md)
- [发布说明](docs/release-notes/v1.0.0.md)
- [Echo bot 示例](samples/channel/echo_bot.py)

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
