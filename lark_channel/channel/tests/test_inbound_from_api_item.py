"""im/v1/messages list item 可归一化为 InboundMessage（供补拉重放）。"""
from __future__ import annotations

import json

from lark_channel.channel import FeishuChannel
from lark_channel.channel.bot_identity import BotIdentity
from lark_channel.channel.types import InboundMessage


def _item(chat_id: str = "oc_group1", root_id: str | None = None) -> dict:
    return {
        "message_id": "om_backfill_1",
        "chat_id": chat_id,
        "create_time": "1756000000000",
        "msg_type": "text",
        "content": json.dumps(
            {
                "text": "@_user_1 帮我改登录页",
                "mentions": [{"key": "@_user_1", "id": {"open_id": "ou_bot"}}],
            },
            ensure_ascii=False,
        ),
        "sender": {"id": "ou_user", "id_type": "open_id", "sender_type": "user"},
        "root_id": root_id,
    }


def test_inbound_from_api_item_group_text() -> None:
    channel = FeishuChannel(app_id="cli_x", app_secret="s")
    channel._bot_identity = BotIdentity(open_id="ou_bot")
    msg: InboundMessage = channel.inbound_from_api_item(_item())
    assert msg.message_id == "om_backfill_1"
    assert msg.chat_id == "oc_group1"
    assert msg.chat_type == "group"
    assert msg.sender_id == "ou_user"
    assert msg.sender_is_bot is False
    assert "帮我改登录页" in msg.content_text
    assert msg.mentioned_bot is True


def test_inbound_from_api_item_p2p() -> None:
    channel = FeishuChannel(app_id="cli_x", app_secret="s")
    msg = channel.inbound_from_api_item(_item(chat_id="ou_user"))
    assert msg.chat_type == "p2p"
