"""看门狗必须在「无活动 ≥ wake_threshold」时真实探测；连续失败触发重连并留日志。"""
from __future__ import annotations

import logging

import pytest

from lark_channel.channel.config import KeepaliveConfig
from lark_channel.channel.keepalive import KeepaliveWatchdog


def _config() -> KeepaliveConfig:
    return KeepaliveConfig(
        enabled=True,
        check_interval_seconds=30.0,
        wake_threshold_seconds=90.0,
        probe_timeout_seconds=5.0,
        failure_threshold=2,
    )


@pytest.mark.asyncio
async def test_watchdog_probes_after_inactivity_and_reconnects(caplog) -> None:
    now = 1_000.0
    probes: list[float] = []
    reconnects: list[int] = []

    async def fake_probe() -> bool:
        probes.append(now)
        return False

    def fake_reconnect() -> None:
        reconnects.append(now)

    watchdog = KeepaliveWatchdog(
        config=_config(),
        probe=fake_probe,
        reconnect=fake_reconnect,
        last_activity=lambda: 100.0,  # 最后活动在 900s 前
        clock=lambda: now,
    )

    with caplog.at_level(logging.INFO):
        await watchdog.run_once()  # 第 1 次失败
        await watchdog.run_once()  # 第 2 次失败 → 重连

    assert probes == [1_000.0, 1_000.0]
    assert reconnects == [1_000.0]
    assert "keepalive probe failed" in caplog.text
    assert "reconnect" in caplog.text


@pytest.mark.asyncio
async def test_watchdog_skips_probe_when_activity_recent() -> None:
    now = 1_000.0
    probes: list[float] = []

    async def fake_probe() -> bool:
        probes.append(now)
        return True

    watchdog = KeepaliveWatchdog(
        config=_config(),
        probe=fake_probe,
        reconnect=lambda: None,
        last_activity=lambda: 950.0,  # 50s 前有活动
        clock=lambda: now,
    )

    await watchdog.run_once()
    assert probes == []
