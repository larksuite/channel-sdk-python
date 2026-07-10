"""resolve_sender_names config + roster mention-collection.

``resolve_sender_names`` is an opt-in ChannelConfig knob (default off, zero
extra API). ``resolve_chat_members`` overrides the roster source. Mention
collection seeds the roster from inbound mentions so a bot that has appeared
once can later be @-mentioned by name.
"""

from lark_channel.channel.config import ChannelConfig
from lark_channel.channel.chat_member_cache import ChatMemberCache
from lark_channel.channel.types import ChatMember


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
