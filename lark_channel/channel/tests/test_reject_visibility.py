"""dedupe 命中与 safety reject 必须留下 WARN 日志（此前静默）。"""
from __future__ import annotations

import logging

from lark_channel.channel.normalize.dedup import Deduper, InMemoryDedupStore


def test_deduper_duplicate_logs_warning(caplog) -> None:
    deduper = Deduper(
        store=InMemoryDedupStore(max_entries=16), ttl_seconds=60, enabled=True
    )
    assert deduper.check_and_mark("acc", "evt-1", "om-1") is True
    with caplog.at_level(logging.WARNING):
        assert deduper.check_and_mark("acc", "evt-1", "om-1") is False
    assert "dedupe hit" in caplog.text
    assert "om-1" in caplog.text
