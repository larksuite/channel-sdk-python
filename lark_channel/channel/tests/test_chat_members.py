"""FeishuChannel.get_chat_members().

Raw-Transport pattern (mirrors ``bot_identity``): the ``im.v1.chat`` resource
has no members method, so the port builds a ``BaseRequest`` and calls
``Transport.aexecute`` directly. These tests patch that seam + the tenant-token
verify step so nothing hits the network.

Patch targets mirror ``test_bot_identity.py``: ``Transport.aexecute`` is patched
on the shared class object (``lark_channel.core.http.Transport``), and the
tenant-token injection is patched at its source
(``lark_channel.core.token.auth.verify``).
"""

import json
from unittest.mock import patch

import pytest

from lark_channel.channel import FeishuChannel
from lark_channel.channel.errors import FeishuChannelError
from lark_channel.channel.types import ChatMember
from lark_channel.channel.config import ChannelConfig
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


def _member_page(items, *, has_more=False, page_token=""):
    data = {"items": items, "has_more": has_more}
    if page_token:
        data["page_token"] = page_token
    return _raw({"code": 0, "msg": "ok", "data": data})


def _member_item(open_id, name):
    # Hedge on the wire field name for the member id (member_id is the Feishu
    # field; open_id is included so either mapping resolves the same value).
    return {"member_id_type": "open_id", "member_id": open_id, "open_id": open_id, "name": name}


def _channel(**cfg_kwargs):
    if cfg_kwargs:
        return FeishuChannel(app_id="cli_x", app_secret="s", config=ChannelConfig(**cfg_kwargs))
    return FeishuChannel(app_id="cli_x", app_secret="s")


def _Spy(responses):
    """A coroutine-function spy for ``Transport.aexecute``.

    ``patch`` auto-detects ``aexecute`` (an ``async def``) and installs an
    AsyncMock, which awaits its ``side_effect`` only when the side_effect is
    itself a coroutine *function* (``inspect.iscoroutinefunction``) — a callable
    class instance is not one. So the spy is a real ``async def`` closure that
    records requests on its ``.requests`` attribute.
    """
    responses = list(responses)

    async def aexecute(conf, req, option=None):
        aexecute.requests.append(req)
        return responses[0] if len(responses) == 1 else responses.pop(0)

    aexecute.requests = []
    return aexecute


async def test_follows_pagination_and_maps_members():
    spy = _Spy([
        _member_page([_member_item("ou_a", "Alice")], has_more=True, page_token="tok2"),
        _member_page([_member_item("ou_b", "Bob")], has_more=False),
    ])
    ch = _channel()
    with patch(_VERIFY, side_effect=_fake_verify), patch(_TRANSPORT, side_effect=spy):
        members = await ch.get_chat_members("oc_chat")

    ids = {m.id for m in members}
    assert ids == {"ou_a", "ou_b"}
    by_id = {m.id: m for m in members}
    assert by_id["ou_a"].name == "Alice"
    # Second request must carry the page_token from the first page.
    assert ("page_token", "tok2") in spy.requests[1].queries


async def test_page_size_clamped_to_100():
    spy = _Spy([_member_page([_member_item("ou_a", "Alice")])])
    ch = _channel()
    with patch(_VERIFY, side_effect=_fake_verify), patch(_TRANSPORT, side_effect=spy):
        await ch.get_chat_members("oc_chat", page_size=500)

    assert ("page_size", "100") in spy.requests[0].queries


async def test_max_pages_caps_requests():
    # has_more is always True; max_pages=1 must stop after a single request.
    always_more = _member_page([_member_item("ou_a", "Alice")], has_more=True, page_token="next")
    spy = _Spy([always_more])
    ch = _channel()
    with patch(_VERIFY, side_effect=_fake_verify), patch(_TRANSPORT, side_effect=spy):
        await ch.get_chat_members("oc_chat", max_pages=1)

    assert len(spy.requests) == 1


async def test_second_call_hits_cache_and_force_bypasses():
    spy = _Spy([_member_page([_member_item("ou_a", "Alice")])])
    ch = _channel()
    with patch(_VERIFY, side_effect=_fake_verify), patch(_TRANSPORT, side_effect=spy):
        await ch.get_chat_members("oc_chat")
        await ch.get_chat_members("oc_chat")
        assert len(spy.requests) == 1  # cache hit — no new request
        await ch.get_chat_members("oc_chat", force=True)
        assert len(spy.requests) == 2  # force bypasses the cache


async def test_resolve_chat_members_hook_short_circuits_api():
    hook_members = [ChatMember(id="ou_hook", name="Hooked")]
    spy = _Spy([_member_page([_member_item("ou_a", "Alice")])])
    ch = _channel(resolve_chat_members=lambda chat_id: hook_members)
    with patch(_VERIFY, side_effect=_fake_verify), patch(_TRANSPORT, side_effect=spy):
        members = await ch.get_chat_members("oc_chat")

    assert [m.id for m in members] == ["ou_hook"]
    assert spy.requests == []  # hook returned a list — API not called


async def test_resolve_chat_members_hook_none_falls_back_to_api():
    spy = _Spy([_member_page([_member_item("ou_a", "Alice")])])
    ch = _channel(resolve_chat_members=lambda chat_id: None)
    with patch(_VERIFY, side_effect=_fake_verify), patch(_TRANSPORT, side_effect=spy):
        members = await ch.get_chat_members("oc_chat")

    assert [m.id for m in members] == ["ou_a"]
    assert len(spy.requests) == 1


async def test_resolve_chat_members_hook_empty_list_falls_back_to_api():
    # An empty list means "no data from the hook" — same as None, it falls back
    # to the API rather than yielding an empty roster.
    spy = _Spy([_member_page([_member_item("ou_a", "Alice")])])
    ch = _channel(resolve_chat_members=lambda chat_id: [])
    with patch(_VERIFY, side_effect=_fake_verify), patch(_TRANSPORT, side_effect=spy):
        members = await ch.get_chat_members("oc_chat")

    assert [m.id for m in members] == ["ou_a"]
    assert len(spy.requests) == 1


async def test_members_are_never_bots():
    spy = _Spy([_member_page([_member_item("ou_a", "Alice")])])
    ch = _channel()
    with patch(_VERIFY, side_effect=_fake_verify), patch(_TRANSPORT, side_effect=spy):
        members = await ch.get_chat_members("oc_chat")

    assert all(m.is_bot is False for m in members)


async def test_api_failure_raises_channel_error():
    spy = _Spy([_raw({"code": 99991672, "msg": "no permission"})])
    ch = _channel()
    with patch(_VERIFY, side_effect=_fake_verify), patch(_TRANSPORT, side_effect=spy):
        with pytest.raises(FeishuChannelError):
            await ch.get_chat_members("oc_chat")


async def test_chat_id_is_url_encoded_via_paths_not_bare_fstring():
    """Security: caller-supplied chat_id must go through ``req.paths`` (encoded
    by ``_build_url``), never interpolated raw into ``req.uri`` — otherwise a
    ``/../`` chat_id walks the path."""
    evil = "oc_x/../y"
    spy = _Spy([_member_page([_member_item("ou_a", "Alice")])])
    ch = _channel()
    with patch(_VERIFY, side_effect=_fake_verify), patch(_TRANSPORT, side_effect=spy):
        await ch.get_chat_members(evil)

    req = spy.requests[0]
    assert req.paths.get("chat_id") == evil
    assert ":chat_id" in (req.uri or "")
    assert evil not in (req.uri or "")
