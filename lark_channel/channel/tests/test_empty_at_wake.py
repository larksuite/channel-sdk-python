"""Empty @-mention wake.

A message that only @-mentions the bot with no body must still be delivered
(not dropped as empty): ``mentioned_bot`` is True and ``content_text`` is empty,
so downstream code can detect a bare "poke" via
``mentioned_bot and not content_text.strip()``.
"""

import pytest

from lark_channel.channel.normalize.pipeline import (
    InboundPipeline,
    PipelineConfig,
    PipelineDeps,
)


async def test_at_only_message_is_delivered_with_empty_body():
    # A REAL bare-@ event carries the mention placeholder in the text (e.g.
    # "@_user_1 "); the bot's own mention must be stripped so content_text
    # normalizes to empty — not rendered as "@BotName".
    pipeline = InboundPipeline(PipelineConfig(), PipelineDeps())
    pipeline.set_bot_open_id("ou_bot")

    inbound = await pipeline.process(
        event_id="e1",
        message_event={
            "message_id": "om_1",
            "create_time": 1,
            "chat_id": "oc_c",
            "chat_type": "group",
            "message_type": "text",
            "content": {"text": "@_user_1 "},
            "mentions": [{"key": "@_user_1", "id": {"open_id": "ou_bot"}, "name": "Bot"}],
        },
        sender={"sender_id": {"open_id": "ou_human"}, "sender_type": "user"},
    )

    assert inbound is not None  # not dropped
    assert inbound.mentioned_bot is True
    assert inbound.content_text.strip() == ""


async def test_bot_mention_stripped_but_body_preserved():
    # "@bot hello" → the bot mention is removed but the real body survives, so
    # the ping heuristic (mentioned_bot and not content_text.strip()) correctly
    # treats this as NOT a bare poke.
    pipeline = InboundPipeline(PipelineConfig(), PipelineDeps())
    pipeline.set_bot_open_id("ou_bot")

    inbound = await pipeline.process(
        event_id="e2",
        message_event={
            "message_id": "om_2",
            "create_time": 1,
            "chat_id": "oc_c",
            "chat_type": "group",
            "message_type": "text",
            "content": {"text": "@_user_1 hello there"},
            "mentions": [{"key": "@_user_1", "id": {"open_id": "ou_bot"}, "name": "Bot"}],
        },
        sender={"sender_id": {"open_id": "ou_human"}, "sender_type": "user"},
    )

    assert inbound is not None
    assert inbound.mentioned_bot is True
    assert inbound.content_text.strip() == "hello there"
    assert "@Bot" not in inbound.content_text


async def test_other_user_mention_not_stripped():
    # A message that @-mentions someone who is NOT the bot keeps that mention
    # rendered as @Name (only the bot's own mention is stripped).
    pipeline = InboundPipeline(PipelineConfig(), PipelineDeps())
    pipeline.set_bot_open_id("ou_bot")

    inbound = await pipeline.process(
        event_id="e3",
        message_event={
            "message_id": "om_3",
            "create_time": 1,
            "chat_id": "oc_c",
            "chat_type": "group",
            "message_type": "text",
            "content": {"text": "@_user_1 ping"},
            "mentions": [{"key": "@_user_1", "id": {"open_id": "ou_alice"}, "name": "Alice"}],
        },
        sender={"sender_id": {"open_id": "ou_human"}, "sender_type": "user"},
    )

    assert inbound is not None
    assert inbound.mentioned_bot is False
    assert "@Alice" in inbound.content_text
