"""FeishuChannel's per-event async handlers — exercised with fake P2 payloads.

Drives `_handle_*_event` directly, bypassing the WS / dispatcher layer.
Uses the public `channel.on("event_name", handler)` registration API.
"""

import json
from types import SimpleNamespace

import pytest

from lark_channel.channel import FeishuChannel as _ChannelClient
from lark_channel.channel.config import InboundConfig
from lark_channel.channel.safety import RejectEvent
from lark_channel.api.im.v1.model.p2_im_message_reaction_created_v1 import (
    P2ImMessageReactionCreatedV1,
)
from lark_channel.api.im.v1.model.p2_im_message_recalled_v1 import P2ImMessageRecalledV1
from lark_channel.api.im.v1.model.p2_im_chat_disbanded_v1 import P2ImChatDisbandedV1
from lark_channel.api.im.v1.model.p2_im_chat_member_user_deleted_v1 import (
    P2ImChatMemberUserDeletedV1,
)
from lark_channel.channel.types import CardActionPayload
from lark_channel.event.callback.model.p2_card_action_trigger import P2CardActionTrigger


def _client(**kwargs):
    return _ChannelClient(app_id="cli_x", app_secret="s", **kwargs)


# ---- interaction handler -------------------------------------------------


def _fake_card_action(*, action_value, tag="button", message_id="om_xyz",
                      operator_open_id="ou_op"):
    """SimpleNamespace mimicking P2CardActionTrigger attribute tree (bypasses
    the generated model's strict typing that forbids string action.value)."""
    return SimpleNamespace(
        event=SimpleNamespace(
            action=SimpleNamespace(tag=tag, value=action_value),
            operator=SimpleNamespace(open_id=operator_open_id),
            context=SimpleNamespace(open_message_id=message_id),
        ),
    )


@pytest.mark.asyncio
async def test_handle_interaction_event_passes_parsed_action():
    c = _client()
    got = []
    c.on("cardAction", lambda event: got.append(event))
    data = _fake_card_action(action_value=json.dumps({"kind": "rate", "score": "up"}))
    await c._handle_interaction_event(data)
    assert len(got) == 1
    event = got[0]
    assert event.action.value == {"kind": "rate", "score": "up"}
    assert event.message_id == "om_xyz"
    assert event.operator.open_id == "ou_op"
    assert event.action.tag == "button"


@pytest.mark.asyncio
async def test_handle_interaction_event_non_json_value_wrapped():
    c = _client()
    got = []
    c.on("cardAction", lambda event: got.append(event))
    data = _fake_card_action(action_value="plain-string-not-json")
    await c._handle_interaction_event(data)
    assert got[0].action.value == {"value": "plain-string-not-json"}


@pytest.mark.asyncio
async def test_handle_interaction_event_exposes_cardkit_form_fields():
    c = _client()
    got = []
    c.on("cardAction", lambda event: got.append(event))
    data = SimpleNamespace(
        event=SimpleNamespace(
            action=SimpleNamespace(
                tag="form",
                value={"legacy": "v"},
                form_value={"field": "value"},
                input_value="typed",
                options=["a", "b"],
                checked=True,
            ),
            operator=SimpleNamespace(open_id="ou_op"),
            context=SimpleNamespace(open_message_id="om_xyz", open_chat_id="oc_1"),
        ),
    )

    await c._handle_interaction_event(data)

    assert got[0].action.value == {"legacy": "v"}
    assert got[0].action.form_value == {"field": "value"}
    assert got[0].action.input_value == "typed"
    assert got[0].action.options == ["a", "b"]
    assert got[0].action.checked is True


def test_card_action_payload_form_fields_do_not_affect_equality_or_repr():
    a = CardActionPayload(tag="form", value={"legacy": "v"})
    b = CardActionPayload(
        tag="form",
        value={"legacy": "v"},
        form_value={"field": "value"},
        input_value="typed",
        options=["a"],
        checked=True,
    )

    assert a == b
    text = repr(b)
    assert "form_value" not in text
    assert "input_value" not in text
    assert "options" not in text
    assert "checked" not in text


@pytest.mark.asyncio
async def test_handle_interaction_event_no_handler_is_noop():
    c = _client()
    # No cardAction handler registered — shouldn't raise
    data = P2CardActionTrigger({"event": {}})
    await c._handle_interaction_event(data)


# ---- raw handler ---------------------------------------------------------


def _fake_message_event(message_id="om_raw"):
    return SimpleNamespace(
        header=SimpleNamespace(event_id="evt_raw"),
        event=SimpleNamespace(
            sender=SimpleNamespace(
                sender_id=SimpleNamespace(open_id="ou_sender", user_id="u1"),
                sender_type="user",
            ),
            message=SimpleNamespace(
                message_id=message_id,
                create_time="1000",
                chat_id="oc_1",
                chat_type="p2p",
                message_type="text",
                content=json.dumps({"text": "hello"}),
                mentions=[],
            ),
        ),
    )


@pytest.mark.asyncio
async def test_handle_message_event_does_not_emit_raw_by_default():
    c = _client()
    raw_events = []
    messages = []
    c.on("raw", lambda event: raw_events.append(event))
    c.on("message", lambda event: messages.append(event))

    await c._handle_message_event(_fake_message_event())

    assert len(messages) == 1
    assert raw_events == []


@pytest.mark.asyncio
async def test_handle_message_event_emits_raw_when_enabled():
    c = _client(inbound=InboundConfig(emit_raw_events=True))
    raw_events = []
    c.on("raw", lambda event: raw_events.append(event))

    await c._handle_message_event(_fake_message_event())

    assert len(raw_events) == 1
    assert raw_events[0]["event"]["message"]["message_id"] == "om_raw"


# ---- reaction handler ---------------------------------------------------


@pytest.mark.asyncio
async def test_handle_reaction_event_off_mode_drops():
    c = _client()
    c._config.inbound.reaction_notifications = "off"
    got = []
    c.on("reaction", lambda event: got.append(event))
    data = P2ImMessageReactionCreatedV1({
        "event": {
            "message_id": "om_1",
            "reaction_type": {"emoji_type": "HEART"},
            "user_id": {"open_id": "ou_r"},
        },
    })
    await c._handle_reaction_event(data, action="create")
    assert got == []


# ---- bot add / leave handlers -------------------------------------------


@pytest.mark.asyncio
async def test_handle_bot_event_join_dispatches():
    c = _client()
    got = []
    c.on("botAdded", lambda event: got.append(event))

    data = SimpleNamespace(
        event=SimpleNamespace(
            chat_id="oc_new",
            operator_id=SimpleNamespace(open_id="ou_op"),
        ),
    )
    await c._handle_bot_event(data, joined=True)
    assert len(got) == 1
    assert got[0].chat_id == "oc_new"
    assert got[0].operator.open_id == "ou_op"


@pytest.mark.asyncio
async def test_handle_bot_event_leave_dispatches_when_handler_set():
    c = _client()
    got = []
    c.on("botLeave", lambda event: got.append(event))

    data = SimpleNamespace(
        event=SimpleNamespace(chat_id="oc_gone", operator_id=None),
    )
    await c._handle_bot_event(data, joined=False)
    assert len(got) == 1


@pytest.mark.asyncio
async def test_handle_bot_event_no_handler_noop():
    c = _client()
    data = SimpleNamespace(event=SimpleNamespace(chat_id="oc_x", operator_id=None))
    await c._handle_bot_event(data, joined=True)  # no raise


# ---- message_read handler -----------------------------------------------


@pytest.mark.asyncio
async def test_handle_message_read_event_dispatches():
    c = _client()
    got = []
    c.on("messageRead", lambda event: got.append(event))
    data = SimpleNamespace(event=SimpleNamespace(
        reader=SimpleNamespace(reader_id=SimpleNamespace(open_id="ou_reader")),
        message_id_list=["om_1", "om_2"],
    ))
    await c._handle_message_read_event(data)
    assert len(got) == 1
    assert got[0].message_ids == ["om_1", "om_2"]
    assert got[0].reader.open_id == "ou_reader"


# ---- lifecycle events (customized p2 registrations) ---------------------


@pytest.mark.asyncio
async def test_handle_message_recalled_event_dispatches():
    c = _client()
    got = []
    c.on("messageRecalled", lambda event: got.append(event))
    data = P2ImMessageRecalledV1(
        {
            "event": {
                "message_id": "om_1",
                "chat_id": "oc_1",
                "recall_time": "1700000000000",
                "recall_type": "initiator",
            }
        }
    )
    await c._handle_message_recalled_event(data)
    assert len(got) == 1
    assert got[0]["message_id"] == "om_1"
    assert got[0]["chat_id"] == "oc_1"


@pytest.mark.asyncio
async def test_handle_chat_disbanded_event_dispatches():
    c = _client()
    got = []
    c.on("chatDisbanded", lambda event: got.append(event))
    data = P2ImChatDisbandedV1({"event": {"chat_id": "oc_1"}})
    await c._handle_chat_disbanded_event(data)
    assert len(got) == 1
    assert got[0]["chat_id"] == "oc_1"


@pytest.mark.asyncio
async def test_handle_user_deleted_event_dispatches():
    c = _client()
    got = []
    c.on("userDeleted", lambda event: got.append(event))
    data = P2ImChatMemberUserDeletedV1(
        {
            "event": {
                "chat_id": "oc_1",
                "users": [{"user_id": {"open_id": "ou_1"}}],
            }
        }
    )
    await c._handle_user_deleted_event(data)
    assert len(got) == 1
    assert got[0]["user_open_ids"] == ["ou_1"]


@pytest.mark.asyncio
async def test_handle_lifecycle_events_no_handler_noop():
    c = _client()
    await c._handle_message_recalled_event(P2ImMessageRecalledV1({}))
    await c._handle_chat_disbanded_event(P2ImChatDisbandedV1({}))
    await c._handle_user_deleted_event(P2ImChatMemberUserDeletedV1({}))  # no raise


# ---- require_user_auth error paths --------------------------------------


@pytest.mark.asyncio
async def test_require_user_auth_raises_when_scope_blocked():
    from lark_channel.channel.errors import UATAuthError

    c = _client()
    c._config.uat.blocked_scopes = ["im:admin"]
    with pytest.raises(UATAuthError):
        await c.require_user_auth("ou_user", ["im:admin"])


@pytest.mark.asyncio
async def test_require_user_auth_raises_when_scope_not_allowed():
    from lark_channel.channel.errors import UATAuthError

    c = _client()
    c._config.uat.allowed_scopes = ["im:message"]
    with pytest.raises(UATAuthError):
        await c.require_user_auth("ou_user", ["wiki:write"])


@pytest.mark.asyncio
async def test_require_user_auth_returns_existing_uat_when_fresh():
    import time
    from lark_channel.channel.types import UAT

    c = _client()
    fresh = UAT(
        access_token="t",
        refresh_token="r",
        expires_at=time.time() + 3600,
        scopes=["im:message"],
    )
    await c._token_store.set("ou_me", fresh)
    got = await c.require_user_auth("ou_me", ["im:message"])
    assert got is fresh


# ---- dispatcher property builds lazily ----------------------------------


def test_dispatcher_property_builds_lazily():
    c = _client()
    assert c._dispatcher is None
    d = c.dispatcher
    assert d is not None
    assert c._dispatcher is d


# ---- _emit_reject with exception in handler is logged, not raised -------


def test_emit_reject_handler_exception_is_swallowed(caplog):
    c = _client()

    def bad(_):
        raise ValueError("handler bug")

    c.on("reject", bad)
    c._emit_reject(RejectEvent(
        message_id="om_x", chat_id="oc_1", sender_id="ou_1", reason="policy_no_mention",
    ))
    assert any("raised" in (r.message or "") for r in caplog.records)
