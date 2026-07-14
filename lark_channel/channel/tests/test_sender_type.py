"""sender_type / sender_is_bot passthrough.

The raw ``sender.sender_type`` string is currently discarded by
``_sender_to_identity`` (only ``is_bot`` is derived). The port must preserve
it on ``Identity.sender_type`` and expose ``InboundMessage.sender_type`` /
``sender_is_bot`` read-only properties, while leaving the existing
``is_bot = sender_type in {"bot","app"}`` semantics unchanged.
"""

from lark_channel.channel import Conversation, Identity, InboundMessage
from lark_channel.channel.channel import _extract_fetched_sender
from lark_channel.channel.normalize.pipeline import _sender_to_identity
from lark_channel.channel.types import TextContent


def _sender(sender_type):
    payload = {"sender_id": {"open_id": "ou_x"}}
    if sender_type is not None:
        payload["sender_type"] = sender_type
    return payload


def test_bot_sender_type_preserved_and_flagged_bot():
    ident = _sender_to_identity(_sender("bot"))
    assert ident.sender_type == "bot"
    assert ident.is_bot is True


def test_user_sender_type_preserved_and_not_bot():
    ident = _sender_to_identity(_sender("user"))
    assert ident.sender_type == "user"
    assert ident.is_bot is False


def test_app_sender_type_still_maps_to_bot():
    # "app" keeps the existing app→bot semantics but retains the raw string.
    ident = _sender_to_identity(_sender("app"))
    assert ident.sender_type == "app"
    assert ident.is_bot is True


def test_missing_sender_type_is_none_not_bot():
    # Absent sender_type means "unknown" — must not be coerced to a value,
    # and must not be treated as a bot.
    ident = _sender_to_identity(_sender(None))
    assert ident.sender_type is None
    assert ident.is_bot is False


def test_fetch_by_id_path_preserves_sender_type():
    # The fetch-by-id path (_extract_fetched_sender) must also carry sender_type
    # through to _sender_to_identity, so a fetched message reports the sender
    # kind identically to a live event.
    extracted = _extract_fetched_sender(
        {"sender": {"id": "ou_bot", "id_type": "open_id", "sender_type": "bot"}}
    )
    ident = _sender_to_identity(extracted)
    assert ident.sender_type == "bot"
    assert ident.is_bot is True


def test_inbound_message_exposes_sender_type_and_sender_is_bot():
    msg = InboundMessage(
        id="om_1",
        create_time=1,
        conversation=Conversation(chat_id="oc_c", chat_type="group"),
        sender=Identity(open_id="ou_x", sender_type="bot", is_bot=True),
        content=TextContent(text="hi"),
    )
    assert msg.sender_type == "bot"
    assert msg.sender_is_bot is True
    assert msg.sender.is_bot is True
