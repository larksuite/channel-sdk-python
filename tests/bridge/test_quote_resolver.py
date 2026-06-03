from types import SimpleNamespace

import pytest

from lark_channel import FeishuChannel
from lark_channel.channel import _api_helpers
from lark_channel.channel.quote import QuoteResolver
from lark_channel.channel.types import (
    Conversation,
    Identity,
    InboundMessage,
    ReplyRef,
    TextContent,
)


def _msg(mid, parent=None, root=None):
    return InboundMessage(
        id=mid,
        create_time=1,
        conversation=Conversation(chat_id="oc_1", chat_type="group"),
        sender=Identity(open_id="ou_1"),
        content=TextContent(text=mid),
        reply=ReplyRef(message_id=parent) if parent else None,
        raw={"message": {"message_id": mid, "parent_id": parent, "root_id": root}},
    )


def _pipeline_raw_msg(mid, parent=None, root=None):
    return InboundMessage(
        id=mid,
        create_time=1,
        conversation=Conversation(chat_id="oc_1", chat_type="group"),
        sender=Identity(open_id="ou_1"),
        content=TextContent(text=mid),
        reply=ReplyRef(message_id=parent) if parent else None,
        raw={"message_id": mid, "parent_id": parent, "root_id": root},
    )


@pytest.mark.asyncio
async def test_batch_quote_skips_parent_inside_same_batch():
    calls = []
    resolver = QuoteResolver(fetcher=lambda message_id: calls.append(message_id) or None)

    result = await resolver.resolve_quoted_contexts(
        [_msg("m1"), _msg("m2", parent="m1")],
        chat_mode="group",
    )

    assert result["m2"].decision == "in_batch"
    assert calls == []


@pytest.mark.asyncio
async def test_duplicate_parent_fetched_once():
    calls = []

    async def fetch(message_id):
        calls.append(message_id)
        return {
            "message_id": message_id,
            "msg_type": "text",
            "content": {"text": "parent"},
        }

    resolver = QuoteResolver(fetcher=fetch)
    result = await resolver.resolve_quoted_contexts(
        [_msg("m2", "p1"), _msg("m3", "p1")],
        chat_mode="group",
    )

    assert calls == ["p1"]
    assert result["m2"].context.text == "parent"
    assert result["m3"].context.text == "parent"


@pytest.mark.asyncio
async def test_topic_root_anchor_is_not_quote():
    resolver = QuoteResolver(fetcher=lambda message_id: {"message_id": message_id})

    result = await resolver.resolve_quoted_contexts(
        [_msg("m2", parent="root_1", root="root_1")],
        chat_mode="topic",
    )

    assert result["m2"].decision == "topic_root"


@pytest.mark.asyncio
async def test_thread_mode_topic_root_anchor_reads_pipeline_raw_shape():
    calls = []
    resolver = QuoteResolver(fetcher=lambda message_id: calls.append(message_id) or None)

    result = await resolver.resolve_quoted_contexts(
        [_pipeline_raw_msg("m2", parent="root_1", root="root_1")],
        chat_mode="thread",
    )

    assert result["m2"].decision == "topic_root"
    assert calls == []


@pytest.mark.asyncio
async def test_topic_root_wins_even_when_root_message_is_in_same_batch():
    calls = []
    resolver = QuoteResolver(fetcher=lambda message_id: calls.append(message_id) or None)

    result = await resolver.resolve_quoted_contexts(
        [
            _pipeline_raw_msg("root_1"),
            _pipeline_raw_msg("m2", parent="root_1", root="root_1"),
        ],
        chat_mode="topic",
    )

    assert result["m2"].decision == "topic_root"
    assert calls == []


@pytest.mark.asyncio
async def test_json_text_content_is_flattened():
    async def fetch(message_id):
        return {
            "message_id": message_id,
            "msg_type": "text",
            "content": "{\"text\":\"parent\"}",
        }

    resolver = QuoteResolver(fetcher=fetch)
    result = await resolver.resolve_quoted_contexts([_msg("m2", "p1")], chat_mode="group")

    assert result["m2"].context.text == "parent"


@pytest.mark.asyncio
async def test_interactive_and_merge_forward_content_are_flattened():
    interactive = QuoteResolver(
        fetcher=lambda message_id: {
            "message_id": message_id,
            "msg_type": "interactive",
            "content": "{\"title\":\"card title\",\"elements\":[{\"tag\":\"div\",\"text\":{\"content\":\"card body\"}}]}",
        }
    )
    result = await interactive.resolve_quoted_contexts([_msg("m2", "p1")], chat_mode="group")
    assert "card title" in result["m2"].context.text
    assert "card body" in result["m2"].context.text

    merge = QuoteResolver(
        fetcher=lambda message_id: {
            "message_id": message_id,
            "msg_type": "merge_forward",
            "content": "{\"messages\":[{\"content\":{\"text\":\"forwarded text\"}}]}",
        }
    )
    result = await merge.resolve_quoted_contexts([_msg("m3", "p2")], chat_mode="group")
    assert "forwarded text" in result["m3"].context.text


@pytest.mark.asyncio
async def test_fetch_failure_keeps_fetch_failed_decision():
    async def fetch(message_id):
        raise RuntimeError("permission denied")

    resolver = QuoteResolver(fetcher=fetch)
    result = await resolver.resolve_quoted_contexts([_msg("m2", "p1")], chat_mode="group")

    assert result["m2"].decision == "fetch_failed"
    assert result["m2"].context is None


@pytest.mark.asyncio
async def test_message_object_body_content_is_normalized():
    raw = SimpleNamespace(
        message_id="p1",
        msg_type="text",
        body=SimpleNamespace(content="{\"text\":\"body text\"}"),
        sender=SimpleNamespace(open_id="ou_1"),
    )
    resolver = QuoteResolver(fetcher=lambda message_id: raw)

    result = await resolver.resolve_quoted_contexts([_msg("m2", "p1")], chat_mode="group")

    assert result["m2"].context.text == "body text"
    assert result["m2"].context.sender_id == "ou_1"


@pytest.mark.asyncio
async def test_message_object_sender_id_is_normalized_from_get_message_shape():
    raw = {
        "data": {
            "items": [
                {
                    "message_id": "p1",
                    "msg_type": "text",
                    "body": {"content": "{\"text\":\"body text\"}"},
                    "sender": {"id": "ou_2", "id_type": "open_id"},
                }
            ]
        }
    }
    resolver = QuoteResolver(fetcher=lambda message_id: raw)

    result = await resolver.resolve_quoted_contexts([_msg("m2", "p1")], chat_mode="group")

    assert result["m2"].context.sender_id == "ou_2"


@pytest.mark.asyncio
async def test_channel_fetch_message_raw_dict_failure_keeps_fetch_failed(monkeypatch):
    ch = FeishuChannel(app_id="cli_x", app_secret="s")

    async def fake_fetch(client, message_id, *, card_content_type=None):
        assert card_content_type == "user_card_content"
        return {"code": 999, "msg": "permission denied"}

    monkeypatch.setattr("lark_channel.channel._api_helpers.fetch_message_raw", fake_fetch)

    result = await ch.resolve_quoted_contexts([_msg("m2", "p1")], chat_mode="group")

    assert result["m2"].decision == "fetch_failed"
    assert result["m2"].context is None


@pytest.mark.asyncio
async def test_fetch_message_raw_handles_dict_and_object_codes():
    class FakeMessage:
        def __init__(self, response):
            self._response = response
            self.requests = []

        async def aget(self, request):
            self.requests.append(request)
            return self._response

    class FakeClient:
        def __init__(self, response):
            self.im = SimpleNamespace(
                v1=SimpleNamespace(message=FakeMessage(response))
            )

    failure = await _api_helpers.fetch_message_raw(FakeClient({"code": 999}), "om_1")
    assert failure is None

    success_response = SimpleNamespace(
        code=0,
        data=SimpleNamespace(
            items=[
                SimpleNamespace(
                    message_id="om_1",
                    body=SimpleNamespace(content="{\"text\":\"ok\"}"),
                )
            ]
        ),
    )
    client = FakeClient(success_response)
    success = await _api_helpers.fetch_message_raw(
        client,
        "om_1",
        card_content_type="user_card_content",
    )

    assert success["code"] == 0
    assert success["data"]["items"][0]["message_id"] == "om_1"
    assert ("card_msg_content_type", "user_card_content") in client.im.v1.message.requests[0].queries
