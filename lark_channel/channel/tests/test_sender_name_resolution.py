"""resolve_sender_names config + roster mention-collection.

``resolve_sender_names`` is an opt-in ChannelConfig knob (default off, zero
extra API). ``resolve_chat_members`` overrides the roster source. Mention
collection seeds the roster from inbound mentions so a bot that has appeared
once can later be @-mentioned by name.
"""

from unittest.mock import AsyncMock

from lark_channel.channel import FeishuChannel
from lark_channel.channel.config import ChannelConfig
from lark_channel.channel.chat_member_cache import ChatMemberCache
from lark_channel.channel.types import (
    ChatMember,
    Conversation,
    Identity,
    InboundMessage,
    TextContent,
)


def test_resolve_sender_names_defaults_off():
    assert ChannelConfig().resolve_sender_names is False


def test_resolve_sender_names_opt_in_accepted():
    assert ChannelConfig(resolve_sender_names=True).resolve_sender_names is True


def test_resolve_chat_members_hook_defaults_none():
    assert ChannelConfig().resolve_chat_members is None


def test_inbound_mention_seeds_roster_for_later_name_lookup():
    # A bot that appeared in an inbound mention (open_id + name) becomes
    # resolvable by name afterwards.
    cache = ChatMemberCache()
    cache.set_members(
        "oc_c",
        [ChatMember(id="ou_bot", name="HelperBot", is_bot=True)],
        source="mention",
    )
    assert cache.resolve_open_id("oc_c", "HelperBot") == "ou_bot"
    assert cache.resolve_name("oc_c", "ou_bot") == "HelperBot"


async def test_bot_sender_name_resolved_by_warming_get_chat_bots():
    # The core bot-at-bot case: with resolve_sender_names on, a message from a
    # BOT sender (not in the users list) gets its name filled by warming the
    # bots roster.
    ch = FeishuChannel(app_id="cli_x", app_secret="s", config=ChannelConfig(resolve_sender_names=True))
    ch.get_chat_members = AsyncMock(return_value=[])  # users list has no bots

    async def fake_get_chat_bots(chat_id, *, force=False):
        bots = [ChatMember(id="ou_botA", name="PeerBot", is_bot=True)]
        ch._chat_member_cache.set_bots(chat_id, bots)
        return bots

    ch.get_chat_bots = fake_get_chat_bots

    inbound = InboundMessage(
        id="om_1",
        create_time=1,
        conversation=Conversation(chat_id="oc_c", chat_type="group"),
        sender=Identity(open_id="ou_botA", is_bot=True, sender_type="bot"),
        content=TextContent(text="hi"),
    )
    await ch._dispatch_inbound_to_user(inbound)
    assert inbound.sender.display_name == "PeerBot"


async def test_user_sender_does_not_warm_bots():
    ch = FeishuChannel(app_id="cli_x", app_secret="s", config=ChannelConfig(resolve_sender_names=True))
    ch.get_chat_members = AsyncMock(return_value=[])
    ch.get_chat_bots = AsyncMock(side_effect=AssertionError("must not warm bots for a user sender"))

    inbound = InboundMessage(
        id="om_2",
        create_time=1,
        conversation=Conversation(chat_id="oc_c", chat_type="group"),
        sender=Identity(open_id="ou_user", is_bot=False, sender_type="user"),
        content=TextContent(text="hi"),
    )
    await ch._dispatch_inbound_to_user(inbound)  # must not raise
