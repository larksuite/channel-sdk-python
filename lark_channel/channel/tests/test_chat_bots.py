"""FeishuChannel.get_chat_bots().

The members API filters out bots, so in-group bots are listed via a separate
``/members/bots`` endpoint returning ``{items:[{bot_id, bot_name}]}``. Bots are
mapped to ``ChatMember(is_bot=True)`` and seeded into the roster so they can be
@-mentioned by name without first appearing in an inbound mention.
"""

import json
from unittest.mock import patch

import pytest

from lark_channel.channel import FeishuChannel
from lark_channel.channel.errors import FeishuChannelError
from lark_channel.core.model import RawResponse

_TRANSPORT = "lark_channel.core.http.Transport.aexecute"
_VERIFY = "lark_channel.core.token.auth.verify"


def _raw(body, status=200):
    r = RawResponse()
    r.status_code = status
    r.content = json.dumps(body).encode("utf-8")
    return r


def _fake_verify(cfg, request, option):
    option.tenant_access_token = "t-test"


def _bots_page(items):
    return _raw({"code": 0, "msg": "ok", "data": {"items": items}})


def _members_page(items):
    return _raw({"code": 0, "msg": "ok", "data": {"items": items, "has_more": False}})


def _Spy(responses):
    """A coroutine-function spy for ``Transport.aexecute`` (see test_chat_members
    for why an ``async def`` closure is required over a callable instance)."""
    responses = list(responses)

    async def aexecute(conf, req, option=None):
        aexecute.requests.append(req)
        return responses[0] if len(responses) == 1 else responses.pop(0)

    aexecute.requests = []
    return aexecute


def _channel():
    return FeishuChannel(app_id="cli_x", app_secret="s")


async def test_lists_bots_mapped_is_bot_true_and_hits_bots_endpoint():
    spy = _Spy([_bots_page([{"bot_id": "ou_bot1", "bot_name": "HelperBot"}])])
    ch = _channel()
    with patch(_VERIFY, side_effect=_fake_verify), patch(_TRANSPORT, side_effect=spy):
        bots = await ch.get_chat_bots("oc_chat")

    assert len(bots) == 1
    assert bots[0].id == "ou_bot1"
    assert bots[0].name == "HelperBot"
    assert bots[0].is_bot is True
    assert (spy.requests[0].uri or "").endswith("/members/bots")


async def test_second_call_hits_cache_and_force_bypasses():
    spy = _Spy([_bots_page([{"bot_id": "ou_bot1", "bot_name": "HelperBot"}])])
    ch = _channel()
    with patch(_VERIFY, side_effect=_fake_verify), patch(_TRANSPORT, side_effect=spy):
        await ch.get_chat_bots("oc_chat")
        await ch.get_chat_bots("oc_chat")
        assert len(spy.requests) == 1
        await ch.get_chat_bots("oc_chat", force=True)
        assert len(spy.requests) == 2


async def test_seeds_roster_for_name_resolution():
    spy = _Spy([_bots_page([{"bot_id": "ou_bot1", "bot_name": "HelperBot"}])])
    ch = _channel()
    with patch(_VERIFY, side_effect=_fake_verify), patch(_TRANSPORT, side_effect=spy):
        await ch.get_chat_bots("oc_chat")

    # The per-chat roster cache lives alongside the chat-mode cache; the bot is
    # resolvable by name after the fetch without an inbound mention.
    cache = ch._chat_member_cache
    assert cache.resolve_open_id("oc_chat", "HelperBot") == "ou_bot1"
    assert cache.resolve_name("oc_chat", "ou_bot1") == "HelperBot"


async def test_does_not_overwrite_member_users_cache():
    spy = _Spy([
        _members_page([{"member_id": "ou_user", "open_id": "ou_user", "name": "Alice"}]),
        _bots_page([{"bot_id": "ou_bot1", "bot_name": "HelperBot"}]),
    ])
    ch = _channel()
    with patch(_VERIFY, side_effect=_fake_verify), patch(_TRANSPORT, side_effect=spy):
        await ch.get_chat_members("oc_chat")
        await ch.get_chat_bots("oc_chat")
        # The users list must still be cached (no third request).
        users = await ch.get_chat_members("oc_chat")

    assert [m.id for m in users] == ["ou_user"]
    assert len(spy.requests) == 2


async def test_api_failure_raises_channel_error():
    spy = _Spy([_raw({"code": 99991672, "msg": "no permission"})])
    ch = _channel()
    with patch(_VERIFY, side_effect=_fake_verify), patch(_TRANSPORT, side_effect=spy):
        with pytest.raises(FeishuChannelError):
            await ch.get_chat_bots("oc_chat")
