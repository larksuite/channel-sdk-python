"""botLoopGuard.

Heuristic guard against two bots @-mentioning each other forever. It only
counts "another bot @-mentioned me" (``sender.is_bot and mentioned_bot``),
uses ``msg.create_time`` as its clock (deterministic), dedups by message_id
within the window, and a ``user`` message clears the key. Opt-in, default off.

Pipeline wiring: it sits after dedup/self_sent/policy and before the lock;
``drop`` silently discards, ``reject`` emits ``RejectEvent(reason="bot_loop")``.
"""

import asyncio
import time
from unittest.mock import Mock

import pytest

from lark_channel.channel.config import BotLoopGuardConfig, PolicyConfig, TextBatchConfig
from lark_channel.channel.safety import SafetyPipeline
from lark_channel.channel.safety.loop_guard import LoopGuard
from lark_channel.channel.types import (
    Conversation,
    Identity,
    InboundMessage,
    Mention,
    TextContent,
)


def _msg(mid, t, *, chat="c1", sender="ou_bot", sender_type="bot", mentioned_bot=True):
    is_bot = sender_type in {"bot", "app"}
    return InboundMessage(
        id=mid,
        create_time=t,
        conversation=Conversation(chat_id=chat, chat_type="group"),
        sender=Identity(open_id=sender, sender_type=sender_type, is_bot=is_bot),
        content=TextContent(text="hi"),
        mentioned_bot=mentioned_bot,
    )


def _guard(**cfg):
    cfg.setdefault("enabled", True)
    return LoopGuard(BotLoopGuardConfig(**cfg), Mock())


# ---- record() counting -----------------------------------------------------


def test_trips_when_window_reaches_threshold():
    guard = _guard(window_ms=60000, max_bot_mentions=3)
    assert guard.record(_msg("m1", 0)) is False
    assert guard.record(_msg("m2", 10)) is False
    assert guard.record(_msg("m3", 20)) is True


def test_old_entries_slide_out_of_window():
    guard = _guard(window_ms=1000, max_bot_mentions=2)
    assert guard.record(_msg("m1", 0)) is False
    # m1 is 5000ms before m2 → outside the 1000ms window, so it doesn't count.
    assert guard.record(_msg("m2", 5000)) is False
    assert guard.record(_msg("m3", 5500)) is True


def test_ineligible_messages_are_not_counted():
    guard = _guard(window_ms=60000, max_bot_mentions=2)
    # A human sender never counts.
    assert guard.record(_msg("m1", 0, sender_type="user", mentioned_bot=True)) is False
    # A bot that didn't @ me never counts.
    assert guard.record(_msg("m2", 10, mentioned_bot=False)) is False
    # ...so a single genuine bot@me is still below threshold.
    assert guard.record(_msg("m3", 20)) is False


def test_user_message_clears_the_key():
    guard = _guard(window_ms=60000, max_bot_mentions=2)
    assert guard.record(_msg("m1", 0)) is False
    guard.record(_msg("u1", 10, sender_type="user", mentioned_bot=False))  # clears
    # Count restarts, so the next bot@me is only #1 — not a trip.
    assert guard.record(_msg("m2", 20)) is False


def test_same_message_id_counts_once_within_window():
    guard = _guard(window_ms=60000, max_bot_mentions=2)
    assert guard.record(_msg("m1", 0)) is False
    assert guard.record(_msg("m1", 5)) is False  # redelivery — not double-counted
    assert guard.record(_msg("m2", 10)) is True


def test_first_trip_warns_exactly_once():
    logger = Mock()
    guard = LoopGuard(BotLoopGuardConfig(enabled=True, window_ms=60000, max_bot_mentions=2), logger)
    guard.record(_msg("m1", 0))
    guard.record(_msg("m2", 10))  # trip 1 → warn
    guard.record(_msg("m3", 20))  # still tripping → no second warn
    assert logger.warning.call_count == 1


def test_scope_chat_plus_sender_isolates_senders():
    guard = _guard(window_ms=60000, max_bot_mentions=2, scope="chat+sender")
    assert guard.record(_msg("m1", 0, sender="ou_botA")) is False
    assert guard.record(_msg("m2", 10, sender="ou_botB")) is False  # different key
    assert guard.record(_msg("m3", 20, sender="ou_botA")) is True   # botA now at 2


def test_scope_chat_merges_senders():
    guard = _guard(window_ms=60000, max_bot_mentions=2, scope="chat")
    assert guard.record(_msg("m1", 0, sender="ou_botA")) is False
    assert guard.record(_msg("m2", 10, sender="ou_botB")) is True   # merged count = 2


# ---- pipeline wiring -------------------------------------------------------


def _recent_bot_msg():
    return _msg("mp1", int(time.time() * 1000), chat="c_pipe", sender="ou_peer")


async def test_pipeline_drop_suppresses_delivery():
    loop = asyncio.get_running_loop()
    delivered = []
    pipe = SafetyPipeline(
        loop=loop,
        on_message=lambda m: delivered.append(m),
        policy=PolicyConfig(
            require_mention=False,
            bot_loop_guard=BotLoopGuardConfig(enabled=True, max_bot_mentions=1, on_trip="drop"),
        ),
        batch_config=TextBatchConfig(delay_ms=0),
    )
    await pipe.push_message(_recent_bot_msg())
    await asyncio.sleep(0.05)
    assert delivered == []


async def test_pipeline_reject_emits_bot_loop_reason():
    loop = asyncio.get_running_loop()
    rejects = []
    pipe = SafetyPipeline(
        loop=loop,
        on_message=lambda m: None,
        on_reject=lambda r: rejects.append(r),
        policy=PolicyConfig(
            require_mention=False,
            bot_loop_guard=BotLoopGuardConfig(enabled=True, max_bot_mentions=1, on_trip="reject"),
        ),
        batch_config=TextBatchConfig(delay_ms=0),
    )
    await pipe.push_message(_recent_bot_msg())
    await asyncio.sleep(0.05)
    assert len(rejects) == 1
    assert rejects[0].reason == "bot_loop"


def test_reconfigure_enable_disable_takes_effect():
    # disabled -> enabled makes the next eligible message trip immediately;
    # enabled -> disabled stops tripping (runtime config actually takes effect).
    guard = LoopGuard(BotLoopGuardConfig(enabled=False), Mock())
    assert guard.record(_msg("m1", 0)) is False  # disabled

    guard.reconfigure(BotLoopGuardConfig(enabled=True, max_bot_mentions=1, window_ms=60000))
    assert guard.record(_msg("m2", 10)) is True  # now enabled, threshold 1

    guard.reconfigure(BotLoopGuardConfig(enabled=False))
    assert guard.record(_msg("m3", 20)) is False  # disabled again


def test_reconfigure_threshold_change_clears_state():
    guard = _guard(window_ms=60000, max_bot_mentions=3)
    guard.record(_msg("m1", 0))
    guard.record(_msg("m2", 10))  # count 2 of 3
    # Changing the threshold clears counting state, so we start fresh at 1.
    guard.reconfigure(BotLoopGuardConfig(enabled=True, window_ms=60000, max_bot_mentions=2))
    assert guard.record(_msg("m3", 20)) is False  # count 1 of 2
    assert guard.record(_msg("m4", 30)) is True   # count 2 of 2


def test_threshold_below_one_is_clamped_and_can_trip():
    # max_bot_mentions=0 would otherwise be nonsensical; it clamps to 1 so a
    # single eligible message trips (never silently disables the guard).
    guard = _guard(window_ms=60000, max_bot_mentions=0)
    assert guard.record(_msg("m1", 0)) is True


def test_out_of_order_stale_event_does_not_join_future_window():
    # A future-timestamped event followed by a stale (older) event must not be
    # grouped into the same window — the stale event falls outside it.
    guard = _guard(window_ms=1000, max_bot_mentions=2)
    assert guard.record(_msg("m_future", 100000)) is False  # count 1
    assert guard.record(_msg("m_stale", 0)) is False        # outside window, not counted


def test_reset_on_human_chat_scope():
    guard = _guard(window_ms=60000, max_bot_mentions=2, scope="chat")
    assert guard.record(_msg("m1", 0)) is False  # count 1
    guard.reset_on_human(_msg("h", 10, sender_type="user", mentioned_bot=False))
    assert guard.record(_msg("m2", 20)) is False  # restarted at 1, not tripped


def test_reset_on_human_clears_all_bot_keys_in_chat_plus_sender():
    guard = _guard(window_ms=60000, max_bot_mentions=2, scope="chat+sender")
    assert guard.record(_msg("m1", 0, sender="ou_botA")) is False
    # A human (different sender key) must still clear the bot's counter.
    guard.reset_on_human(
        _msg("h", 10, sender="ou_human", sender_type="user", mentioned_bot=False)
    )
    assert guard.record(_msg("m2", 20, sender="ou_botA")) is False


def _bot_at_me(mid):
    m = _msg(mid, int(time.time() * 1000), chat="c_reset", sender="ou_peer")
    m.mentions = [Mention(key="@_1", open_id="ou_bot")]
    return m


def _human_plain(mid):
    return _msg(
        mid, int(time.time() * 1000), chat="c_reset",
        sender="ou_human", sender_type="user", mentioned_bot=False,
    )


async def test_pipeline_human_message_resets_guard_before_policy():
    # require_mention=True → a plain human message is policy-rejected, yet it
    # must still reset the guard (reset runs before the policy gate), so a
    # subsequent bot @-mention doesn't trip.
    loop = asyncio.get_running_loop()
    delivered, rejects = [], []
    pipe = SafetyPipeline(
        loop=loop,
        on_message=lambda m: delivered.append(m.id),
        on_reject=lambda r: rejects.append(r),
        policy=PolicyConfig(
            require_mention=True,
            bot_loop_guard=BotLoopGuardConfig(
                enabled=True, max_bot_mentions=2, window_ms=60_000, on_trip="reject"
            ),
        ),
        batch_config=TextBatchConfig(delay_ms=0),
    )
    pipe.set_bot_open_id("ou_bot")

    await pipe.push_message(_bot_at_me("b1"))
    await asyncio.sleep(0.02)
    await pipe.push_message(_human_plain("h1"))  # policy-rejected, but resets guard
    await asyncio.sleep(0.02)
    await pipe.push_message(_bot_at_me("b2"))
    await asyncio.sleep(0.02)

    assert not any(r.reason == "bot_loop" for r in rejects)  # reset prevented the trip
    assert "b2" in delivered


async def test_pipeline_update_policy_reconfigures_loop_guard():
    loop = asyncio.get_running_loop()
    rejects = []
    pipe = SafetyPipeline(
        loop=loop,
        on_message=lambda m: None,
        on_reject=lambda r: rejects.append(r),
        policy=PolicyConfig(require_mention=False),  # no guard initially
        batch_config=TextBatchConfig(delay_ms=0),
    )
    # Enable the guard at runtime; it must actually take effect on the pipeline.
    pipe.update_policy(
        bot_loop_guard=BotLoopGuardConfig(
            enabled=True, max_bot_mentions=1, window_ms=60000, on_trip="reject"
        )
    )
    await pipe.push_message(_recent_bot_msg())
    await asyncio.sleep(0.05)
    assert len(rejects) == 1 and rejects[0].reason == "bot_loop"


async def test_pipeline_without_guard_delivers_bot_mention():
    loop = asyncio.get_running_loop()
    delivered = []
    pipe = SafetyPipeline(
        loop=loop,
        on_message=lambda m: delivered.append(m),
        policy=PolicyConfig(require_mention=False),  # no bot_loop_guard
        batch_config=TextBatchConfig(delay_ms=0),
    )
    await pipe.push_message(_recent_bot_msg())
    await asyncio.sleep(0.05)
    assert len(delivered) == 1
