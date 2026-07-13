"""Empty @-mention wake + body_text.

A message that only @-mentions the bot with no body must still be delivered
(not dropped as empty): ``mentioned_bot`` is True. ``content_text`` keeps the
rendered mention (default behavior unchanged), while ``body_text`` has the
bot's own mention removed, so downstream code detects a bare "poke" via
``mentioned_bot and not body_text.strip()``.
"""

import pytest

from lark_channel.channel.normalize.pipeline import (
    InboundPipeline,
    PipelineConfig,
    PipelineDeps,
)


async def _process(content_text, mentions, *, bot="ou_bot"):
    pipeline = InboundPipeline(PipelineConfig(), PipelineDeps())
    pipeline.set_bot_open_id(bot)
    return await pipeline.process(
        event_id="e",
        message_event={
            "message_id": "om_1",
            "create_time": 1,
            "chat_id": "oc_c",
            "chat_type": "group",
            "message_type": "text",
            "content": {"text": content_text},
            "mentions": mentions,
        },
        sender={"sender_id": {"open_id": "ou_human"}, "sender_type": "user"},
    )


async def test_at_only_message_delivers_with_empty_body_text():
    # A real bare-@ event carries the mention placeholder in the text ("@_user_1 ").
    inbound = await _process(
        "@_user_1 ",
        [{"key": "@_user_1", "id": {"open_id": "ou_bot"}, "name": "Bot"}],
    )
    assert inbound is not None  # not dropped
    assert inbound.mentioned_bot is True
    # content_text keeps the rendered mention (default behavior unchanged)...
    assert "@Bot" in inbound.content_text
    # ...body_text is empty, so `mentioned_bot and not body_text.strip()` fires.
    assert inbound.body_text.strip() == ""


async def test_content_text_default_preserved_body_text_stripped():
    # "@bot hello there": content_text unchanged (still renders the mention);
    # only body_text drops the bot's own mention.
    inbound = await _process(
        "@_user_1 hello there",
        [{"key": "@_user_1", "id": {"open_id": "ou_bot"}, "name": "Bot"}],
    )
    assert inbound.mentioned_bot is True
    assert "@Bot" in inbound.content_text
    assert "hello there" in inbound.content_text
    assert inbound.body_text.strip() == "hello there"
    assert "@Bot" not in inbound.body_text


async def test_body_text_equals_content_text_when_bot_not_mentioned():
    # A message @-mentioning someone who is NOT the bot: nothing is stripped,
    # body_text == content_text, and the other mention is preserved in both.
    inbound = await _process(
        "@_user_1 ping",
        [{"key": "@_user_1", "id": {"open_id": "ou_alice"}, "name": "Alice"}],
    )
    assert inbound.mentioned_bot is False
    assert "@Alice" in inbound.content_text
    assert inbound.body_text == inbound.content_text
