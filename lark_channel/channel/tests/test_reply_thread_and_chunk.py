"""Long thread replies stay in-thread, and chunking never splits an <at> tag."""

from typing import Any, Dict, List

from lark_channel.channel.config import OutboundConfig
from lark_channel.channel.outbound.sender import OutboundSender, SendDriver, chunk_text
from lark_channel.channel.types import OutboundText


def _driver():
    calls: List[Dict[str, Any]] = []

    async def create_message(**kwargs):
        calls.append({"op": "create", **kwargs})
        return {"code": 0, "msg": "ok", "data": {"message_id": "om_new"}}

    async def reply_message(**kwargs):
        calls.append({"op": "reply", **kwargs})
        return {"code": 0, "msg": "ok", "data": {"message_id": "om_reply"}}

    return SendDriver(create_message=create_message, reply_message=reply_message), calls


def _sender(calls_cfg=None):
    driver, calls = _driver()
    cfg = OutboundConfig(text_chunk_limit=10, chunk_mode="none")
    return OutboundSender(driver, cfg), calls


# ── #2: long thread reply must keep every chunk in the thread ────────────────

async def test_thread_reply_keeps_all_chunks_in_thread():
    s, calls = _sender()
    await s.send(
        OutboundText(text="abcdefghij" * 3),  # 30 chars → 3 chunks at limit 10
        receive_id="oc_1",
        reply_to="om_1",
        reply_in_thread=True,
    )
    assert [c["op"] for c in calls] == ["reply", "reply", "reply"]
    assert all(c.get("reply_in_thread") is True for c in calls)


async def test_flat_reply_only_first_chunk_replies():
    s, calls = _sender()
    await s.send(
        OutboundText(text="abcdefghij" * 3),
        receive_id="oc_1",
        reply_to="om_1",
        # reply_in_thread unset → flat reply: legacy behavior preserved.
    )
    ops = [c["op"] for c in calls]
    assert ops[0] == "reply"
    assert ops[1:] == ["create", "create"]


# ── #10: the chunker never splits an <at>...</at> tag ────────────────────────

def _balanced(chunks):
    return all(c.count("<at") == c.count("</at>") for c in chunks)


def test_oversized_at_tag_emitted_whole():
    tag = '<at user_id="ou_abcdef">Alice</at>'  # longer than the limit
    text = ("x" * 20) + tag + ("y" * 20)
    for mode in ("none", "newline", "paragraph"):
        chunks = chunk_text(text, limit=25, mode=mode)
        assert _balanced(chunks)
        assert any(tag in c for c in chunks)  # intact in exactly one chunk


def test_at_tag_pushed_whole_to_next_chunk():
    tag = '<at user_id="ou_a">A</at>'
    text = ("x" * 40) + tag + ("y" * 10)
    chunks = chunk_text(text, limit=50, mode="none")
    assert _balanced(chunks)
    assert any(tag in c for c in chunks)


def test_many_unclosed_at_openers_bounded_time():
    # Adversarial: many unclosed "<at " openers must not make span-scanning
    # quadratic (linear single pass). Content is preserved (nothing dropped).
    import time as _t

    text = "<at " * 50000
    start = _t.perf_counter()
    chunks = chunk_text(text, limit=100, mode="none")
    assert _t.perf_counter() - start < 2.0
    assert "".join(chunks) == text


def test_plain_text_chunking_unchanged_without_tags():
    # No <at> tags → identical to the delimiter chunker (regression guard).
    text = "line1\nline2\nline3\nline4"
    assert chunk_text(text, limit=12, mode="newline") == ["line1\nline2", "line3\nline4"]
