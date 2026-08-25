"""Speaking into the meeting."""

import json

import pytest

from lark_channel.channel.errors import FeishuChannelError, FeishuChannelErrorCode

from . import fixtures as fx


async def test_text_is_wrapped_and_given_an_idempotency_key(vc, tat_channel):
    channel = tat_channel()
    session = await channel.join_meeting(fx.MEETING_NO)

    await session.send_message("hello 会议")

    call = vc.last(fx.URI_MESSAGE)
    assert call.body["msg_type"] == "text"
    assert json.loads(call.body["content"]) == {"text": "hello 会议"}
    assert call.body["uuid"]


async def test_follow_mode_cannot_speak_in_the_meeting(vc, uat_channel):
    """In follow mode the bot is not a participant, so there is nowhere for a
    message to appear."""
    channel, _store, _flow = uat_channel(active_meeting_check_interval_seconds=300.0)
    session = await channel.follow_my_meeting(user_open_id=fx.USER_OPEN_ID)

    with pytest.raises(FeishuChannelError) as excinfo:
        await session.send_message("hello")
    assert excinfo.value.code is FeishuChannelErrorCode.NOT_SUPPORTED
    assert vc.count(fx.URI_MESSAGE) == 0


async def test_exceeding_the_per_minute_budget_refuses_before_calling_the_api(
    vc, tat_channel
):
    """The bot's own messages come back as meeting chat, so a handler that
    replies without checking the echo flag self-amplifies at network speed."""
    channel = tat_channel(send_rate_limit_per_minute=2)
    session = await channel.join_meeting(fx.MEETING_NO)

    await session.send_message("one")
    await session.send_message("two")
    sent = vc.count(fx.URI_MESSAGE)

    with pytest.raises(FeishuChannelError) as excinfo:
        await session.send_message("three")
    assert excinfo.value.code is FeishuChannelErrorCode.RATE_LIMITED
    assert vc.count(fx.URI_MESSAGE) == sent
