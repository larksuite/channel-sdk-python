"""Roster is seeded only for admitted messages.

Mention collection and sender-name resolution moved to the post-admission
dispatch path, so a message rejected by the safety pipeline (policy / stale /
dedup / self-sent) can neither write the member roster nor trigger the external
members API.
"""

from types import SimpleNamespace

from lark_channel.channel import FeishuChannel
from lark_channel.channel.config import ChannelConfig
from lark_channel.channel.types import (
    Conversation,
    Identity,
    InboundMessage,
    Mention,
    TextContent,
)


def _channel(**cfg):
    if cfg:
        return FeishuChannel(app_id="cli_x", app_secret="s", config=ChannelConfig(**cfg))
    return FeishuChannel(app_id="cli_x", app_secret="s")


def _mention_event():
    return SimpleNamespace(
        header=SimpleNamespace(event_id="e1"),
        event=SimpleNamespace(
            message={
                "message_id": "om_1",
                "create_time": 1,
                "chat_id": "oc_c",
                "chat_type": "group",
                "message_type": "text",
                "content": {"text": "@_1 hi"},
                "mentions": [{"key": "@_1", "id": {"open_id": "ou_peer"}, "name": "PeerBot"}],
            },
            sender={"sender_id": {"open_id": "ou_human"}, "sender_type": "user"},
        ),
    )


async def test_admitted_dispatch_seeds_roster():
    ch = _channel()
    inbound = InboundMessage(
        id="om_1",
        create_time=1,
        conversation=Conversation(chat_id="oc_c", chat_type="group"),
        sender=Identity(open_id="ou_human"),
        mentions=[Mention(key="@_1", open_id="ou_peer", name="PeerBot", is_bot=True)],
        content=TextContent(text="hi"),
    )
    await ch._dispatch_inbound_to_user(inbound)
    # The admitted path collected the observed mention into the roster.
    assert ch._chat_member_cache.resolve_open_id("oc_c", "PeerBot") == "ou_peer"


async def test_rejected_message_does_not_seed_roster():
    # A safety pipeline that never admits (its on_message is not invoked) must
    # leave the roster untouched — collection happens only on dispatch.
    ch = _channel()

    class _RejectAll:
        async def push_message(self, msg):
            return None  # swallow: simulates policy/stale/dedup/self-sent reject

    ch._safety = _RejectAll()
    await ch._handle_message_event(_mention_event())

    assert ch._chat_member_cache.resolve_open_id("oc_c", "PeerBot") is None


async def test_rejected_message_does_not_call_resolve_chat_members_hook():
    calls = []
    ch = _channel(resolve_sender_names=True, resolve_chat_members=lambda cid: calls.append(cid) or [])

    class _RejectAll:
        async def push_message(self, msg):
            return None

    ch._safety = _RejectAll()
    await ch._handle_message_event(_mention_event())

    assert calls == []  # rejected message never warmed the roster
