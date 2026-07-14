"""body_text survives default batching and is correct for post content."""

import asyncio
import time

from lark_channel.channel.config import PolicyConfig, TextBatchConfig
from lark_channel.channel.normalize.converters.post import convert, convert_body
from lark_channel.channel.safety import SafetyPipeline
from lark_channel.channel.safety.chat_pipeline import merge_batch
from lark_channel.channel.types import (
    Conversation,
    Identity,
    InboundMessage,
    PostContent,
    TextContent,
)


def _im(mid, text, content_text, body_text):
    return InboundMessage(
        id=mid,
        create_time=int(time.time() * 1000),
        conversation=Conversation(chat_id="oc_1", chat_type="p2p"),
        sender=Identity(open_id="ou_h"),
        content=TextContent(text=text),
        content_text=content_text,
        safe_content_text=content_text,
        body_text=body_text,
        mentioned_bot=True,
    )


# ── 2.1a: batching recombines derived text views ─────────────────────────────

def test_merge_batch_recombines_content_and_body_text():
    m1 = _im("1", "@Bot first", "@Bot first", "first")
    m2 = _im("2", "@Bot second", "@Bot second", "second")
    merged = merge_batch([m1, m2])
    assert merged.content_text == "@Bot first\n\n@Bot second"
    assert merged.body_text == "first\n\nsecond"
    assert merged.safe_content_text == "@Bot first\n\n@Bot second"
    assert merged.batched_sources == [m1, m2]


async def test_safety_pipeline_batch_preserves_body_text():
    loop = asyncio.get_running_loop()
    got = []
    sp = SafetyPipeline(
        loop=loop,
        on_message=lambda m: got.append(m),
        policy=PolicyConfig(dm_policy="open"),
        batch_config=TextBatchConfig(delay_ms=40, max_messages=10, max_chars=100000),
    )
    await sp.push_message(_im("1", "@Bot first", "@Bot first", "first"))
    await sp.push_message(_im("2", "@Bot second", "@Bot second", "second"))
    await asyncio.sleep(0.15)
    assert len(got) == 1
    assert got[0].body_text == "first\n\nsecond"
    assert got[0].content_text == "@Bot first\n\n@Bot second"


# ── 2.1b: post body_text drops only the bot's <at>, keeps title/format ───────

def test_post_convert_body_drops_bot_at_node_keeps_rest():
    ast = {
        "zh_cn": {
            "title": "Deploy",
            "content": [[
                {"tag": "at", "user_id": "ou_bot", "user_name": "Bot"},
                {"tag": "text", "text": " run ", "style": ["bold"]},
                {"tag": "at", "user_id": "ou_alice", "user_name": "Alice"},
            ]],
        }
    }
    c = PostContent(post=ast)
    full, _ = convert(c)
    body = convert_body(c, "ou_bot")

    # content_text (default) keeps everything, incl. the bot mention.
    assert "# Deploy" in full and "@Bot" in full and "@Alice" in full
    # body drops only the bot mention; title, bold and other mentions survive.
    assert "# Deploy" in body
    assert "@Bot" not in body
    assert "@Alice" in body
    assert "**" in body
