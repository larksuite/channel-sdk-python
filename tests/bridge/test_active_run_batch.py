import asyncio

import pytest

from lark_channel import FeishuChannel
from lark_channel.channel.config import ChatQueueConfig, TextBatchConfig
from lark_channel.channel.safety.chat_pipeline import ChatPipelineManager
from lark_channel.channel.safety.pipeline import SafetyPipeline
from lark_channel.channel.tests.test_safety import _msg


@pytest.mark.asyncio
async def test_blocked_scope_buffers_until_unblocked():
    loop = asyncio.get_event_loop()
    manager = ChatPipelineManager(
        TextBatchConfig(delay_ms=0),
        loop,
        ChatQueueConfig(enabled=True),
    )
    seen = []

    async def handler(merged, batch):
        seen.append([m.id for m in batch])

    manager.block_scope("oc_1")
    manager.push("oc_1", _msg(id="m1", text="a"), handler)
    await asyncio.sleep(0.01)
    assert seen == []
    manager.unblock_scope("oc_1")
    await manager.dispose()
    assert seen == [["m1"]]


@pytest.mark.asyncio
async def test_cancel_scope_drops_buffered_messages():
    loop = asyncio.get_event_loop()
    manager = ChatPipelineManager(
        TextBatchConfig(delay_ms=0),
        loop,
        ChatQueueConfig(enabled=True),
    )
    seen = []

    async def handler(merged, batch):
        seen.append(batch)

    manager.block_scope("oc_1")
    manager.push("oc_1", _msg(id="m1", text="a"), handler)
    manager.cancel_scope("oc_1")
    manager.unblock_scope("oc_1")
    await asyncio.sleep(0.01)
    assert seen == []


@pytest.mark.asyncio
async def test_blocked_scope_does_not_flush_on_message_cap():
    loop = asyncio.get_event_loop()
    cfg = TextBatchConfig(delay_ms=1000, max_messages=1)
    manager = ChatPipelineManager(cfg, loop, ChatQueueConfig(enabled=True))
    seen = []

    async def handler(merged, batch):
        seen.append([m.id for m in batch])

    manager.block_scope("oc_1")
    manager.push("oc_1", _msg(id="m1", text="a"), handler)
    await asyncio.sleep(0.01)
    assert seen == []
    manager.unblock_scope("oc_1")
    await manager.dispose()
    assert seen == [["m1"]]


@pytest.mark.asyncio
async def test_blocked_scopes_are_independent():
    loop = asyncio.get_event_loop()
    manager = ChatPipelineManager(
        TextBatchConfig(delay_ms=0),
        loop,
        ChatQueueConfig(enabled=True),
    )
    seen = []

    async def handler(merged, batch):
        seen.append([m.id for m in batch])

    manager.block_scope("oc_1")
    manager.push("oc_1", _msg(id="m1", text="a"), handler)
    manager.push("oc_2", _msg(id="m2", text="b"), handler)
    await asyncio.sleep(0.01)
    manager.cancel_scope("oc_1")
    await manager.dispose()
    assert seen == [["m2"]]


@pytest.mark.asyncio
async def test_unblock_respects_existing_quiet_window():
    loop = asyncio.get_event_loop()
    manager = ChatPipelineManager(
        TextBatchConfig(delay_ms=50),
        loop,
        ChatQueueConfig(enabled=True),
    )
    seen = []

    async def handler(merged, batch):
        seen.append([m.id for m in batch])

    manager.block_scope("oc_1")
    manager.push("oc_1", _msg(id="m1", text="a"), handler)
    manager.unblock_scope("oc_1")
    await asyncio.sleep(0.01)
    assert seen == []
    await manager.dispose()
    assert seen == [["m1"]]


@pytest.mark.asyncio
async def test_unblock_flushes_immediately_when_buffer_exceeds_cap():
    loop = asyncio.get_event_loop()
    manager = ChatPipelineManager(
        TextBatchConfig(delay_ms=1000, max_messages=2),
        loop,
        ChatQueueConfig(enabled=True),
    )
    seen = []

    async def handler(merged, batch):
        seen.append([m.id for m in batch])

    manager.block_scope("oc_1")
    manager.push("oc_1", _msg(id="m1", text="a"), handler)
    manager.push("oc_1", _msg(id="m2", text="b"), handler)
    await asyncio.sleep(0.01)
    assert seen == []
    manager.unblock_scope("oc_1")
    await asyncio.sleep(0.01)
    assert seen == [["m1", "m2"]]


@pytest.mark.asyncio
async def test_blocked_scope_can_disable_merge_while_busy():
    loop = asyncio.get_event_loop()
    manager = ChatPipelineManager(
        TextBatchConfig(delay_ms=0),
        loop,
        ChatQueueConfig(enabled=True, merge_while_busy=False),
    )
    seen = []

    async def handler(merged, batch):
        seen.append([m.id for m in batch])

    manager.block_scope("oc_1")
    manager.push("oc_1", _msg(id="m1", text="a"), handler)
    manager.push("oc_1", _msg(id="m2", text="b"), handler)
    await asyncio.sleep(0.01)
    assert seen == []
    manager.unblock_scope("oc_1")
    await manager.dispose()
    assert seen == [["m1"], ["m2"]]


@pytest.mark.asyncio
async def test_unmerged_busy_messages_stay_before_deferred_runs():
    loop = asyncio.get_event_loop()
    manager = ChatPipelineManager(
        TextBatchConfig(delay_ms=0),
        loop,
        ChatQueueConfig(enabled=True, merge_while_busy=False),
    )
    seen = []

    async def handler(merged, batch):
        seen.append(("message", [m.id for m in batch]))

    async def action():
        seen.append(("action", []))

    manager.block_scope("oc_1")
    manager.push("oc_1", _msg(id="m1", text="a"), handler)
    manager.push("oc_1", _msg(id="m2", text="b"), handler)
    manager.run("oc_1", action)
    await asyncio.sleep(0.01)
    assert seen == []
    manager.unblock_scope("oc_1")
    await manager.dispose()
    assert seen == [
        ("message", ["m1"]),
        ("message", ["m2"]),
        ("action", []),
    ]


@pytest.mark.asyncio
async def test_unblock_flushes_immediately_when_buffer_exceeds_char_cap():
    loop = asyncio.get_event_loop()
    manager = ChatPipelineManager(
        TextBatchConfig(delay_ms=1000, max_chars=3),
        loop,
        ChatQueueConfig(enabled=True),
    )
    seen = []

    async def handler(merged, batch):
        seen.append([m.id for m in batch])

    manager.block_scope("oc_1")
    manager.push("oc_1", _msg(id="m1", text="ab"), handler)
    manager.push("oc_1", _msg(id="m2", text="cd"), handler)
    await asyncio.sleep(0.01)
    assert seen == []
    manager.unblock_scope("oc_1")
    await asyncio.sleep(0.01)
    assert seen == [["m1", "m2"]]


@pytest.mark.asyncio
async def test_serial_run_waits_behind_blocked_buffer_until_unblock():
    loop = asyncio.get_event_loop()
    manager = ChatPipelineManager(
        TextBatchConfig(delay_ms=0),
        loop,
        ChatQueueConfig(enabled=True),
    )
    seen = []

    async def handler(merged, batch):
        seen.append(("message", [m.id for m in batch]))

    async def action():
        seen.append(("action", []))

    manager.block_scope("oc_1")
    manager.push("oc_1", _msg(id="m1", text="a"), handler)
    manager.run("oc_1", action)
    await asyncio.sleep(0.01)
    assert seen == []
    manager.unblock_scope("oc_1")
    await manager.dispose()
    assert seen == [("message", ["m1"]), ("action", [])]


@pytest.mark.asyncio
async def test_cancel_blocked_buffer_releases_deferred_run():
    loop = asyncio.get_event_loop()
    manager = ChatPipelineManager(
        TextBatchConfig(delay_ms=0),
        loop,
        ChatQueueConfig(enabled=True),
    )
    seen = []

    async def handler(merged, batch):
        seen.append(("message", [m.id for m in batch]))

    async def action():
        seen.append(("action", []))

    manager.block_scope("oc_1")
    manager.push("oc_1", _msg(id="m1", text="a"), handler)
    manager.run("oc_1", action)
    await asyncio.sleep(0.01)
    assert seen == []
    manager.cancel_scope("oc_1")
    await manager.dispose()
    assert seen == [("action", [])]


@pytest.mark.asyncio
async def test_run_first_does_not_disable_later_batching():
    loop = asyncio.get_event_loop()
    manager = ChatPipelineManager(
        TextBatchConfig(delay_ms=50, max_messages=10),
        loop,
        ChatQueueConfig(enabled=True),
    )
    seen = []

    async def action():
        seen.append(("action", []))

    async def handler(merged, batch):
        seen.append(("message", [m.id for m in batch]))

    manager.run("oc_1", action)
    await asyncio.sleep(0.01)
    manager.push("oc_1", _msg(id="m1", text="a"), handler)
    manager.push("oc_1", _msg(id="m2", text="b"), handler)
    await asyncio.sleep(0.01)
    assert seen == [("action", [])]
    await manager.dispose()
    assert seen == [("action", []), ("message", ["m1", "m2"])]


@pytest.mark.asyncio
async def test_safety_pipeline_exposes_batch_scope_controls():
    loop = asyncio.get_event_loop()
    seen = []

    async def on_message(msg):
        seen.append(msg.id)

    pipeline = SafetyPipeline(
        loop=loop,
        on_message=on_message,
        batch_config=TextBatchConfig(delay_ms=0),
    )

    pipeline.block_batch_scope("oc_1")
    await pipeline.push_message(_msg(id="m1", chat_id="oc_1", text="a"))
    await asyncio.sleep(0.01)
    assert seen == []
    pipeline.unblock_batch_scope("oc_1")
    await pipeline.dispose()
    assert seen == ["m1"]


def test_channel_batch_scope_methods_delegate_to_safety(monkeypatch):
    class FakeLoop:
        def is_running(self):
            return False

    class FakeSafety:
        def __init__(self):
            self.calls = []

        def block_batch_scope(self, scope):
            self.calls.append(("block", scope))

        def unblock_batch_scope(self, scope):
            self.calls.append(("unblock", scope))

        def cancel_batch_scope(self, scope):
            self.calls.append(("cancel", scope))

    channel = FeishuChannel(app_id="cli_x", app_secret="s")
    safety = FakeSafety()
    channel._bg_loop = FakeLoop()
    channel._safety = safety
    monkeypatch.setattr(channel, "_ensure_bg_loop", lambda: None)

    channel.block_batch_scope("oc_1")
    channel.unblock_batch_scope("oc_1")
    channel.cancel_batch_scope("oc_1")

    assert safety.calls == [
        ("block", "oc_1"),
        ("unblock", "oc_1"),
        ("cancel", "oc_1"),
    ]


@pytest.mark.asyncio
async def test_channel_batch_scope_call_on_bg_loop_runs_immediately(monkeypatch):
    class FakeSafety:
        def __init__(self):
            self.calls = []

        def block_batch_scope(self, scope):
            self.calls.append(scope)

    channel = FeishuChannel(app_id="cli_x", app_secret="s")
    safety = FakeSafety()
    channel._bg_loop = asyncio.get_running_loop()
    channel._safety = safety
    monkeypatch.setattr(channel, "_ensure_bg_loop", lambda: None)

    channel.block_batch_scope("oc_1")

    assert safety.calls == ["oc_1"]


def test_channel_batch_scope_rejects_closed_loop(monkeypatch):
    class FakeSafety:
        def block_batch_scope(self, scope):
            raise AssertionError("should not call safety on a closed loop")

    loop = asyncio.new_event_loop()
    loop.close()
    channel = FeishuChannel(app_id="cli_x", app_secret="s")
    channel._bg_loop = loop
    channel._safety = FakeSafety()
    monkeypatch.setattr(channel, "_ensure_bg_loop", lambda: None)

    with pytest.raises(RuntimeError, match="background loop is not running"):
        channel.block_batch_scope("oc_1")
