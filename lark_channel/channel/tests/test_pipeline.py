"""End-to-end inbound pipeline tests (with deduper + async enrichment)."""

import json
from types import SimpleNamespace

import pytest

from lark_channel.channel.config import InboundConfig, MediaCapabilities, SecurityConfig
from lark_channel.channel.normalize.dedup import Deduper, InMemoryDedupStore
from lark_channel.channel.normalize.pipeline import InboundPipeline, PipelineConfig, PipelineDeps
from lark_channel.channel.types import InteractiveContent, MergeForwardContent, TextContent


def _sender(open_id="ou_sender", *, sender_type="user"):
    return {
        "sender_id": {"open_id": open_id, "user_id": "u1"},
        "sender_type": sender_type,
    }


def _msg(
    *,
    message_id="om_1",
    chat_type="p2p",
    msg_type="text",
    content=None,
    parent_id=None,
    root_id=None,
    mentions=None,
):
    return {
        "message_id": message_id,
        "create_time": 1000,
        "chat_id": "oc_1",
        "chat_type": chat_type,
        "message_type": msg_type,
        "content": json.dumps(content or {"text": "hi"}, ensure_ascii=False),
        "parent_id": parent_id,
        "root_id": root_id,
        "mentions": mentions or [],
    }


@pytest.mark.asyncio
async def test_text_message_normalized():
    p = InboundPipeline(PipelineConfig(), PipelineDeps())
    inbound = await p.process(event_id="e1", message_event=_msg(), sender=_sender())
    assert inbound is not None
    assert isinstance(inbound.content, TextContent)
    assert inbound.content.text == "hi"
    assert inbound.content_text == "hi"
    assert inbound.safe_content_text == "hi"
    assert inbound.sender.open_id == "ou_sender"
    assert inbound.conversation.chat_type == "p2p"


@pytest.mark.asyncio
async def test_safe_content_text_escapes_flat_text_without_changing_default_content_text():
    p = InboundPipeline(PipelineConfig(), PipelineDeps())
    inbound = await p.process(
        event_id="e_safe_text",
        message_event=_msg(content={"text": '<at id="all">& hello'}),
        sender=_sender(),
    )

    assert inbound is not None
    assert inbound.content_text == '<at id="all">& hello'
    assert inbound.safe_content_text == "&lt;at id=&quot;all&quot;&gt;&amp; hello"


@pytest.mark.asyncio
async def test_strict_content_text_uses_safe_content_text():
    p = InboundPipeline(
        PipelineConfig(security=SecurityConfig(strict_content_text=True)),
        PipelineDeps(),
    )
    inbound = await p.process(
        event_id="e_strict_text",
        message_event=_msg(content={"text": '<at id="all">'}),
        sender=_sender(),
    )

    assert inbound is not None
    assert inbound.content_text == "&lt;at id=&quot;all&quot;&gt;"
    assert inbound.safe_content_text == "&lt;at id=&quot;all&quot;&gt;"


@pytest.mark.asyncio
async def test_sender_type_app_maps_to_bot_for_dict_sender():
    p = InboundPipeline(PipelineConfig(), PipelineDeps())
    inbound = await p.process(
        event_id="e_app",
        message_event=_msg(),
        sender=_sender(open_id="ou_peer_bot", sender_type="app"),
    )
    assert inbound is not None
    assert inbound.sender.open_id == "ou_peer_bot"
    assert inbound.sender.is_bot is True


@pytest.mark.asyncio
async def test_sender_type_app_maps_to_bot_for_object_sender():
    p = InboundPipeline(PipelineConfig(), PipelineDeps())
    sender = SimpleNamespace(
        sender_id=SimpleNamespace(open_id="ou_peer_bot", user_id="u_peer"),
        sender_type="app",
    )
    inbound = await p.process(event_id="e_app_obj", message_event=_msg(), sender=sender)
    assert inbound is not None
    assert inbound.sender.open_id == "ou_peer_bot"
    assert inbound.sender.is_bot is True


@pytest.mark.asyncio
async def test_pipeline_can_inject_chat_mode_when_enabled():
    cfg = PipelineConfig(inbound=InboundConfig(inject_chat_mode=True))
    p = InboundPipeline(cfg, PipelineDeps())

    async def chat_mode(chat_id):
        assert chat_id == "oc_1"
        return "thread"

    p.set_chat_mode_resolver(chat_mode)

    inbound = await p.process(
        event_id="e_chat_mode",
        message_event=_msg(chat_type="group"),
        sender=_sender(),
    )

    assert inbound is not None
    assert inbound.chat_mode == "thread"


@pytest.mark.asyncio
async def test_pipeline_does_not_inject_chat_mode_by_default():
    calls = []

    def resolver(chat_id):
        calls.append(chat_id)
        return "thread"

    p = InboundPipeline(PipelineConfig(), PipelineDeps())
    p.set_chat_mode_resolver(resolver)

    inbound = await p.process(
        event_id="e_no_chat_mode",
        message_event=_msg(chat_type="group"),
        sender=_sender(),
    )

    assert inbound is not None
    assert inbound.chat_mode is None
    assert calls == []


@pytest.mark.asyncio
async def test_pipeline_can_exclude_message_raw_when_configured():
    cfg = PipelineConfig(inbound=InboundConfig(include_raw=False))
    p = InboundPipeline(cfg, PipelineDeps())

    inbound = await p.process(
        event_id="e_no_raw",
        message_event=_msg(message_id="om_no_raw"),
        sender=_sender(),
    )

    assert inbound is not None
    assert inbound.raw == {}


@pytest.mark.asyncio
async def test_pipeline_chat_mode_resolver_failure_keeps_message():
    cfg = PipelineConfig(inbound=InboundConfig(inject_chat_mode=True))
    p = InboundPipeline(cfg, PipelineDeps())

    def resolver(chat_id):
        raise RuntimeError("chat mode lookup failed")

    p.set_chat_mode_resolver(resolver)

    inbound = await p.process(
        event_id="e_chat_mode_failure",
        message_event=_msg(chat_type="group"),
        sender=_sender(),
    )

    assert inbound is not None
    assert inbound.content_text == "hi"
    assert inbound.chat_mode is None


@pytest.mark.asyncio
async def test_dedup_drops_repeat():
    d = Deduper(InMemoryDedupStore(), ttl_seconds=60)
    p = InboundPipeline(PipelineConfig(), PipelineDeps(), deduper=d)
    a = await p.process(event_id="e1", message_event=_msg(), sender=_sender())
    b = await p.process(event_id="e1", message_event=_msg(), sender=_sender())
    assert a is not None
    assert b is None


@pytest.mark.asyncio
async def test_mentions_resolved_and_stripped():
    msg = _msg(
        content={"text": "hey @_user_1 ok"},
        mentions=[{"key": "@_user_1", "id": {"open_id": "ou_A"}, "name": "Alice"}],
    )
    p = InboundPipeline(PipelineConfig(), PipelineDeps())
    inbound = await p.process(event_id="e", message_event=msg, sender=_sender())
    assert inbound.content.text == "hey @Alice ok"
    assert inbound.mentions[0].open_id == "ou_A"


@pytest.mark.asyncio
async def test_pipeline_sets_mentioned_bot_without_removing_bot_mention():
    msg = _msg(
        chat_type="group",
        content={"text": "hey @_user_1"},
        mentions=[
            {"key": "@_user_1", "id": {"open_id": "ou_bot"}, "name": "Bot"},
        ],
    )
    p = InboundPipeline(PipelineConfig(), PipelineDeps())
    p.set_bot_open_id("ou_bot")

    inbound = await p.process(event_id="e_bot", message_event=msg, sender=_sender())

    assert inbound is not None
    assert inbound.mentioned_bot is True
    assert [m.open_id for m in inbound.mentions] == ["ou_bot"]
    assert inbound.content.text == "hey @Bot"


@pytest.mark.asyncio
async def test_reply_ref_from_parent_id():
    msg = _msg(parent_id="om_parent", root_id="om_root")
    p = InboundPipeline(PipelineConfig(), PipelineDeps())
    inbound = await p.process(event_id="e", message_event=msg, sender=_sender())
    assert inbound.reply is not None
    assert inbound.reply.message_id == "om_parent"


@pytest.mark.asyncio
async def test_media_cap_gate_blocks_image():
    p = InboundPipeline(
        PipelineConfig(inbound=InboundConfig(media_capabilities=MediaCapabilities(image=False))),
        PipelineDeps(),
    )
    msg = _msg(msg_type="image", content={"image_key": "img_x"})
    inbound = await p.process(event_id="e", message_event=msg, sender=_sender())
    assert inbound is None


@pytest.mark.asyncio
async def test_merge_forward_expanded_via_fetcher():
    async def fetch_message(mid: str):
        # Return a children list with one text child
        return {
            "data": {
                "items": [
                    {
                        "message_id": mid,
                        "msg_type": "merge_forward",
                        "body": {"content": "{}"},
                    },
                    {
                        "message_id": "om_child1",
                        "msg_type": "text",
                        "body": {"content": json.dumps({"text": "child text"})},
                        "sender": {"id": "ou_A"},
                        "create_time": "2000",
                    },
                ]
            }
        }

    async def resolve_names(ids):
        return {i: "Alice" for i in ids}

    p = InboundPipeline(
        PipelineConfig(),
        PipelineDeps(fetch_message=fetch_message, resolve_names=resolve_names),
    )
    msg = _msg(msg_type="merge_forward", content={})
    inbound = await p.process(event_id="e", message_event=msg, sender=_sender())
    assert inbound is not None
    assert isinstance(inbound.content, MergeForwardContent)
    assert inbound.content.loading is False
    assert len(inbound.content.items) == 1
    child = inbound.content.items[0]
    assert child.message_id == "om_child1"
    assert child.sender_name == "Alice"
    assert isinstance(child.content, TextContent)
    assert child.content.text == "child text"


@pytest.mark.asyncio
async def test_interactive_is_refetched_and_version_detected():
    async def fetch_message(mid: str):
        return {
            "data": {
                "items": [
                    {
                        "message_id": mid,
                        "msg_type": "interactive",
                        "body": {"content": json.dumps({"schema": "2.0", "body": {"elements": []}})},
                    }
                ]
            }
        }

    p = InboundPipeline(
        PipelineConfig(),
        PipelineDeps(fetch_message=fetch_message),
    )
    msg = _msg(msg_type="interactive", content={})
    inbound = await p.process(event_id="e", message_event=msg, sender=_sender())
    assert isinstance(inbound.content, InteractiveContent)
    assert inbound.content.card_version == "v2"
