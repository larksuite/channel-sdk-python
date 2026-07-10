"""FeishuChannel.reply().

``reply(msg, message, opts)`` is send() that *follows* the triggering message:
it defaults ``reply_to`` to ``msg.message_id`` and ``reply_in_thread`` to
whether the trigger was in a thread. It never upgrades a flat message into a
thread, and ``opts`` can override both defaults.

The outbound ``OutboundSender.send`` seam is mocked so the derived reply_to /
reply_in_thread are observable regardless of how ``reply`` routes internally.
"""

from unittest.mock import AsyncMock

from lark_channel.channel import Conversation, Identity, InboundMessage
from lark_channel.channel import FeishuChannel
from lark_channel.channel.types import SendOpts, SendResult, TextContent


def _channel():
    ch = FeishuChannel(app_id="cli_x", app_secret="s")
    ch._sender.send = AsyncMock(return_value=SendResult.ok(message_id="om_out"))
    return ch


def _inbound(*, thread_id=None):
    return InboundMessage(
        id="om_trigger",
        create_time=1,
        conversation=Conversation(chat_id="oc_c", chat_type="group", thread_id=thread_id),
        sender=Identity(open_id="ou_a"),
        content=TextContent(text="ping"),
    )


async def test_reply_in_thread_follows_trigger_thread():
    ch = _channel()
    await ch.reply(_inbound(thread_id="omt_root"), "pong")

    kwargs = ch._sender.send.await_args.kwargs
    assert kwargs["reply_to"] == "om_trigger"
    assert kwargs["reply_in_thread"] is True


async def test_flat_trigger_does_not_force_thread():
    ch = _channel()
    await ch.reply(_inbound(thread_id=None), "pong")

    kwargs = ch._sender.send.await_args.kwargs
    assert kwargs["reply_to"] == "om_trigger"
    assert kwargs.get("reply_in_thread") in (None, False)


async def test_opts_can_override_reply_in_thread_off():
    ch = _channel()
    await ch.reply(_inbound(thread_id="omt_root"), "pong", SendOpts(reply_in_thread=False))

    kwargs = ch._sender.send.await_args.kwargs
    assert kwargs["reply_in_thread"] is False
