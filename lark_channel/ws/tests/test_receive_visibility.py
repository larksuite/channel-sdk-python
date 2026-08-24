"""收到 WS 数据帧必须更新活动时间戳并在 INFO 输出 message_id/type。"""
from __future__ import annotations

import logging

import pytest

from lark_channel.ws.client import Client
from lark_channel.ws.const import (
    HEADER_MESSAGE_ID,
    HEADER_SEQ,
    HEADER_SUM,
    HEADER_TRACE_ID,
    HEADER_TYPE,
)
from lark_channel.ws.enum import FrameType, MessageType
from lark_channel.ws.pb.pbbp2_pb2 import Frame


def _event_frame() -> Frame:
    frame = Frame()
    for key, value in (
        (HEADER_TYPE, MessageType.EVENT.value),
        (HEADER_MESSAGE_ID, "om_backfill_test"),
        (HEADER_TRACE_ID, "trace_1"),
        (HEADER_SUM, "1"),
        (HEADER_SEQ, "1"),
    ):
        header = frame.headers.add()
        header.key = key
        header.value = value
    frame.payload = b'{"schema":"2.0","header":{}}'
    frame.method = FrameType.DATA.value
    frame.service = 1
    frame.SeqID = 0
    frame.LogID = 0
    return frame


class _NoopDispatcher:
    def _do_without_validation(self, payload: bytes) -> None:
        return None


@pytest.mark.asyncio
async def test_data_frame_updates_activity_and_logs_info(caplog, monkeypatch) -> None:
    client = Client(app_id="cli_x", app_secret="s")
    client._event_handler = _NoopDispatcher()
    async def _noop_write(data: bytes) -> None:
        return None

    monkeypatch.setattr(client, "_write_message", _noop_write)
    frame = _event_frame()
    with caplog.at_level(logging.INFO):
        await client._handle_message(frame.SerializeToString())
    assert "receive message" in caplog.text
    assert "om_backfill_test" in caplog.text
    assert client.last_activity_at > 0
