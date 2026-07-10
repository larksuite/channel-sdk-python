"""Low-level FeishuChannel helpers: recall / add_reaction / update_card /
download_resource / disconnect / client accessors."""

import json
from unittest.mock import AsyncMock

import pytest

from lark_channel.channel import FeishuChannel


@pytest.fixture
def channel():
    return FeishuChannel(app_id="cli_x", app_secret="s")


@pytest.mark.asyncio
async def test_update_card_calls_underlying_patch(channel):
    channel._driver.patch_message = AsyncMock(return_value={"code": 0})
    r = await channel.update_card("om_1", {"schema": "2.0"})
    assert r.success is True
    channel._driver.patch_message.assert_awaited_once()


@pytest.mark.asyncio
async def test_update_card_failure_returns_failed_result(channel):
    channel._driver.patch_message = AsyncMock(
        return_value={"code": 230001, "msg": "invalid card"}
    )
    r = await channel.update_card("om_1", {"schema": "2.0"})
    assert r.success is False
    assert r.error is not None
    assert r.error.raw_code == 230001
    assert r.raw == {"code": 230001, "msg": "invalid card"}


@pytest.mark.asyncio
async def test_recall_message_success(channel):
    channel._driver.delete_message = AsyncMock(return_value={"code": 0})
    r = await channel.recall_message("om_2")
    assert r.success is True


@pytest.mark.asyncio
async def test_recall_message_failure_surfaces(channel):
    channel._driver.delete_message = AsyncMock(return_value={"code": 230002, "msg": "not exist"})
    r = await channel.recall_message("om_missing")
    assert r.success is False
    assert r.error is not None


@pytest.mark.asyncio
async def test_add_reaction_success(channel):
    channel._driver.add_reaction = AsyncMock(return_value={"code": 0})
    r = await channel.add_reaction("om_1", "THUMBSUP")
    assert r.success is True


@pytest.mark.asyncio
async def test_remove_reaction_success(channel):
    channel._driver.remove_reaction = AsyncMock(return_value={"code": 0})
    r = await channel.remove_reaction("om_1", "rxn_1")
    assert r.success is True


@pytest.mark.asyncio
async def test_remove_reaction_by_emoji_lists_then_deletes_first_match(channel):
    channel._driver.list_reactions = AsyncMock(
        return_value={
            "code": 0,
            "data": {
                "items": [
                    {"reaction_id": "r1", "reaction_type": {"emoji_type": "SMILE"}},
                    {"reaction_id": "r2", "reaction_type": {"emoji_type": "THUMBSUP"}},
                ]
            },
        }
    )
    channel._driver.remove_reaction = AsyncMock(return_value={"code": 0})

    result = await channel.remove_reaction_by_emoji("om_1", "THUMBSUP")

    assert result.success is True
    channel._driver.list_reactions.assert_awaited_once_with(
        message_id="om_1",
        emoji_type="THUMBSUP",
        page_token=None,
        page_size=None,
    )
    channel._driver.remove_reaction.assert_awaited_once_with(
        message_id="om_1",
        reaction_id="r2",
    )


@pytest.mark.asyncio
async def test_remove_reaction_by_emoji_returns_failed_result_when_no_match(channel):
    channel._driver.list_reactions = AsyncMock(
        return_value={"code": 0, "data": {"items": []}}
    )

    result = await channel.remove_reaction_by_emoji("om_1", "SMILE")

    assert result.success is False
    assert result.error is not None


@pytest.mark.asyncio
async def test_fetch_inbound_message_returns_normalized_message_without_changing_fetch_message(channel):
    raw = {
        "code": 0,
        "data": {
            "items": [
                {
                    "message_id": "om_1",
                    "create_time": "123",
                    "chat_id": "oc_1",
                    "chat_type": "group",
                    "message_type": "text",
                    "content": json.dumps({"text": "hello @_user_1"}),
                    "mentions": [
                        {"key": "@_user_1", "id": {"open_id": "ou_bot"}, "name": "Bot"}
                    ],
                    "sender": {
                        "sender_id": {"open_id": "ou_sender", "user_id": "u1"},
                        "sender_type": "user",
                    },
                }
            ]
        },
    }
    channel._driver.fetch_message = AsyncMock(return_value=raw)
    channel._pipeline.set_bot_open_id("ou_bot")

    assert await channel.fetch_message("om_1") == raw
    inbound = await channel.fetch_inbound_message("om_1")

    assert inbound is not None
    assert inbound.id == "om_1"
    # The bot's own @-mention is stripped from rendered content (bot-at-bot);
    # the body survives and mentioned_bot stays True.
    assert inbound.content_text == "hello"
    assert inbound.mentioned_bot is True
    assert inbound.raw["message_id"] == "om_1"


@pytest.mark.asyncio
async def test_fetch_inbound_message_handles_get_message_sender_id_type(channel):
    raw = {
        "code": 0,
        "data": {
            "items": [
                {
                    "message_id": "om_1",
                    "create_time": 123,
                    "chat_id": "oc_1",
                    "msg_type": "text",
                    "body": {"content": json.dumps({"text": "hello"})},
                    "sender": {
                        "id": "u_sender",
                        "id_type": "user_id",
                        "sender_type": "user",
                    },
                    "mentions": [],
                }
            ]
        },
    }
    channel._driver.fetch_message = AsyncMock(return_value=raw)

    inbound = await channel.fetch_inbound_message("om_1")

    assert inbound is not None
    assert inbound.content_text == "hello"
    assert inbound.sender.open_id == ""
    assert inbound.sender.user_id == "u_sender"


@pytest.mark.asyncio
@pytest.mark.parametrize("id_type", ["open_id", None])
async def test_fetch_inbound_message_maps_open_id_sender_variants(channel, id_type):
    sender = {
        "id": "ou_sender",
        "sender_type": "user",
    }
    if id_type is not None:
        sender["id_type"] = id_type
    raw = {
        "code": 0,
        "data": {
            "items": [
                {
                    "message_id": "om_1",
                    "create_time": 123,
                    "chat_id": "oc_1",
                    "msg_type": "text",
                    "body": {"content": json.dumps({"text": "hello"})},
                    "sender": sender,
                    "mentions": [],
                }
            ]
        },
    }
    channel._driver.fetch_message = AsyncMock(return_value=raw)

    inbound = await channel.fetch_inbound_message("om_1")

    assert inbound is not None
    assert inbound.sender.open_id == "ou_sender"
    assert inbound.sender.user_id is None


@pytest.mark.asyncio
async def test_fetch_inbound_message_bypasses_pipeline_dedup(channel):
    message = {
        "message_id": "om_dedup",
        "create_time": "123",
        "chat_id": "oc_1",
        "chat_type": "p2p",
        "message_type": "text",
        "content": json.dumps({"text": "hello"}),
        "mentions": [],
    }
    sender = {
        "sender_id": {"open_id": "ou_sender", "user_id": "u1"},
        "sender_type": "user",
    }

    first = await channel._pipeline.process(
        event_id="e1",
        message_event=message,
        sender=sender,
    )
    duplicate = await channel._pipeline.process(
        event_id="e2",
        message_event=message,
        sender=sender,
    )
    channel._driver.fetch_message = AsyncMock(
        return_value={"code": 0, "data": {"items": [{**message, "sender": sender}]}}
    )

    fetched = await channel.fetch_inbound_message("om_dedup")

    assert first is not None
    assert duplicate is None
    assert fetched is not None
    assert fetched.id == "om_dedup"


@pytest.mark.asyncio
async def test_download_resource_delegates_to_hook(channel):
    channel._download_media = AsyncMock(return_value=b"\x89PNG...")
    data = await channel.download_resource("img_xxx", resource_type="image")
    assert data == b"\x89PNG..."


def test_client_exposes_underlying():
    channel = FeishuChannel(app_id="cli_x", app_secret="s")
    assert channel.client is channel._client


def test_dispatcher_accessor_triggers_build():
    channel = FeishuChannel(app_id="cli_x", app_secret="s")
    channel._ensure_bg_loop()
    d = channel.dispatcher
    assert d is not None


def test_bot_identity_accessor_default_none():
    channel = FeishuChannel(app_id="cli_x", app_secret="s")
    assert channel.bot_identity is None


def test_update_policy_syncs_into_channel_config():
    channel = FeishuChannel(app_id="cli_x", app_secret="s")
    channel._ensure_bg_loop()  # spin safety up
    channel.update_policy(dm_policy="disabled", require_mention=False)
    assert channel.get_policy().dm_policy == "disabled"
    assert channel.get_policy().require_mention is False


@pytest.mark.asyncio
async def test_disconnect_drains_safety_and_stops():
    channel = FeishuChannel(app_id="cli_x", app_secret="s")
    channel._ensure_bg_loop()
    await channel.disconnect()
    # After disconnect() the bg loop + ws client + thread are torn down,
    # and the started flag is reset so a subsequent connect() can re-run.
    # ``_shutdown`` is cleared at the end of stop() so the channel can be
    # reconnected later; we assert on the
    # observable state that actually matters.
    assert channel._bg_loop is None
    assert channel._ws_client is None
    assert channel._started is False


@pytest.mark.asyncio
async def test_connect_is_idempotent_when_started():
    channel = FeishuChannel(app_id="cli_x", app_secret="s")
    channel._started = True  # pretend start() already ran
    # Should return quickly without attempting to start WS
    await channel.connect()


@pytest.mark.asyncio
async def test_send_stream_unknown_kind_raises():
    channel = FeishuChannel(app_id="cli_x", app_secret="s")
    with pytest.raises(TypeError):
        await channel.stream("oc_x", {"nonsense": "body"})


@pytest.mark.asyncio
async def test_send_unknown_keys_raises():
    from lark_channel.channel._coerce import coerce_outbound as _coerce_outbound
    with pytest.raises(TypeError):
        _coerce_outbound({"unknown_key": "x"})


@pytest.mark.asyncio
async def test_send_media_buffer_coercion():
    from lark_channel.channel._coerce import coerce_outbound as _coerce_outbound
    from lark_channel.channel.types import OutboundFile
    ob = _coerce_outbound({"file": {"source": b"\x01\x02", "fileName": "f.bin"}})
    assert isinstance(ob, OutboundFile)
    assert ob.source.kind == "buffer"


@pytest.mark.asyncio
async def test_send_media_source_media_ref_passes_through():
    from lark_channel.channel._coerce import coerce_outbound as _coerce_outbound
    from lark_channel.channel.types import MediaSource, OutboundImage
    ob = _coerce_outbound({"image": {"source": MediaSource(kind="key", key="img_x")}})
    assert isinstance(ob, OutboundImage)
    assert ob.source.key == "img_x"


@pytest.mark.asyncio
async def test_send_markdown_stream_invokes_producer(channel):
    # MarkdownStream now uses the CardKit preallocation flow — mock the 4
    # channel methods it calls. Verify seq-ordered element updates + finish.
    channel.create_card_instance = AsyncMock(return_value="card_xyz")

    from lark_channel.channel.types import SendResult as _SR
    channel.send_card_by_reference = AsyncMock(
        return_value=_SR.ok(message_id="om_fake_stream"),
    )
    channel.update_card_element_content = AsyncMock(return_value=None)
    channel.finish_streaming_card = AsyncMock(return_value=None)

    got_controller = []

    async def producer(s):
        got_controller.append(s)
        await s.append("hello")

    result = await channel.stream("oc_1", {"markdown": producer})
    assert result.success is True
    assert result.message_id == "om_fake_stream"
    assert got_controller  # producer ran
    channel.create_card_instance.assert_awaited_once()
    channel.send_card_by_reference.assert_awaited_once()
    channel.finish_streaming_card.assert_awaited_once()
    # At least one element update was issued via the seq-ordered API,
    # NOT via the generic patch API.
    assert channel.update_card_element_content.await_count >= 1
