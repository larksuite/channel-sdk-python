import json
from types import SimpleNamespace

import pytest

from lark_channel import CommentContext as RootCommentContext
from lark_channel import CommentTarget as RootCommentTarget
from lark_channel import FeishuChannel
from lark_channel.channel.comment import CommentPrimitiveClient
from lark_channel.channel.types import CommentContext, CommentTarget


@pytest.mark.asyncio
async def test_add_typing_reaction_returns_reaction_id(monkeypatch):
    ch = FeishuChannel(app_id="cli_x", app_secret="s")

    async def fake_add(message_id, emoji_type):
        assert emoji_type == "Typing"
        return SimpleNamespace(success=True, raw={"data": {"reaction_id": "r1"}})

    monkeypatch.setattr(ch, "add_reaction", fake_add)
    assert await ch.add_typing_reaction("om_1") == "r1"


@pytest.mark.asyncio
async def test_add_typing_reaction_returns_none_on_failure(monkeypatch):
    ch = FeishuChannel(app_id="cli_x", app_secret="s")

    async def fake_add(message_id, emoji_type):
        raise RuntimeError("upstream")

    monkeypatch.setattr(ch, "add_reaction", fake_add)
    assert await ch.add_typing_reaction("om_1") is None


@pytest.mark.asyncio
async def test_remove_typing_reaction_best_effort(monkeypatch):
    ch = FeishuChannel(app_id="cli_x", app_secret="s")

    async def fake_remove(message_id, reaction_id):
        raise RuntimeError("upstream")

    monkeypatch.setattr(ch, "remove_reaction", fake_remove)
    assert await ch.remove_typing_reaction("om_1", "r1") is False


@pytest.mark.asyncio
async def test_remove_typing_reaction_success(monkeypatch):
    ch = FeishuChannel(app_id="cli_x", app_secret="s")

    async def fake_remove(message_id, reaction_id):
        return SimpleNamespace(success=True)

    monkeypatch.setattr(ch, "remove_reaction", fake_remove)
    assert await ch.remove_typing_reaction("om_1", "r1") is True


def test_comment_target_rejects_unsupported_type():
    client = CommentPrimitiveClient(raw_request=lambda req: None)
    target = client.resolve_comment_target_sync(
        file_token="x",
        file_type="mindnote",
    )
    assert target.supported is False
    assert target.reason == "unsupported_file_type"


def test_comment_types_are_publicly_exported():
    assert RootCommentContext is CommentContext
    assert RootCommentTarget is CommentTarget


@pytest.mark.asyncio
async def test_reply_comment_updates_existing_thread_reply():
    calls = []

    async def raw(req):
        calls.append((req.uri, dict(req.paths), dict(req.queries), req.body))
        return {"code": 0, "data": {"reply_id": "r2"}}

    client = CommentPrimitiveClient(raw_request=raw)
    context = CommentContext(
        target=CommentTarget(file_token="doc_1", file_type="docx", supported=True),
        comment_id="c1",
        question="q",
        quote="",
        is_whole=False,
        target_reply_id="r1",
    )
    await client.reply_comment(context, "answer")

    assert calls[0][0] == (
        "/open-apis/drive/v1/files/:file_token/comments/:comment_id/replies/:reply_id"
    )
    assert calls[0][1] == {
        "file_token": "doc_1",
        "comment_id": "c1",
        "reply_id": "r1",
    }
    assert dict(calls[0][2]) == {"file_type": "docx"}
    assert calls[0][3] == {
        "content": {
            "elements": [{"type": "text_run", "text_run": {"text": "answer"}}],
        },
    }


@pytest.mark.asyncio
async def test_reply_comment_returns_none_without_reply_id_for_thread_comment():
    async def raw(req):
        raise AssertionError("should not call reply creation")

    client = CommentPrimitiveClient(raw_request=raw)
    context = CommentContext(
        target=CommentTarget(file_token="doc_1", file_type="docx", supported=True),
        comment_id="c1",
        question="q",
        quote="",
        is_whole=False,
        target_reply_id=None,
    )
    assert await client.reply_comment(context, "answer") is None


@pytest.mark.asyncio
async def test_reply_comment_creates_whole_comment():
    calls = []

    async def raw(req):
        calls.append((req.uri, dict(req.paths), dict(req.queries), req.body))
        return {"code": 0, "data": {"comment_id": "c2"}}

    client = CommentPrimitiveClient(raw_request=raw)
    context = CommentContext(
        target=CommentTarget(file_token="doc_1", file_type="docx", supported=True),
        comment_id="c1",
        question="q",
        quote="",
        is_whole=True,
        target_reply_id=None,
    )

    result = await client.reply_comment(context, "answer")

    assert result["data"]["comment_id"] == "c2"
    assert calls[0][0] == "/open-apis/drive/v1/files/:file_token/comments"
    assert calls[0][1] == {"file_token": "doc_1"}
    assert dict(calls[0][2]) == {"file_type": "docx"}
    assert calls[0][3] == {
        "reply_list": {
            "replies": [
                {
                    "content": {
                        "elements": [
                            {"type": "text_run", "text_run": {"text": "answer"}}
                        ],
                    },
                }
            ],
        },
    }


@pytest.mark.asyncio
async def test_wiki_comment_target_resolves_to_docx():
    async def raw(req):
        return SimpleNamespace(
            code=0,
            data=SimpleNamespace(
                node=SimpleNamespace(obj_token="doc_1", obj_type="docx")
            ),
        )

    client = CommentPrimitiveClient(raw_request=raw)
    target = await client.resolve_comment_target(
        file_token="wikcn_1",
        file_type="wiki",
    )
    assert target.supported is True
    assert target.file_token == "doc_1"
    assert target.file_type == "docx"


@pytest.mark.asyncio
async def test_wiki_comment_target_parses_base_response_raw_content():
    async def raw(req):
        return SimpleNamespace(
            code=0,
            raw=SimpleNamespace(
                content=json.dumps(
                    {
                        "code": 0,
                        "data": {
                            "node": {"obj_token": "doc_1", "obj_type": "docx"}
                        },
                    }
                ).encode("utf-8"),
            ),
        )

    client = CommentPrimitiveClient(raw_request=raw)
    target = await client.resolve_comment_target(
        file_token="wikcn_1",
        file_type="wiki",
    )
    assert target.supported is True
    assert target.file_token == "doc_1"
    assert target.file_type == "docx"


@pytest.mark.asyncio
async def test_wiki_comment_target_reports_resolution_failure():
    async def raw(req):
        return {
            "code": 0,
            "data": {"node": {"obj_token": "doc_1", "obj_type": "mindnote"}},
        }

    client = CommentPrimitiveClient(raw_request=raw)
    target = await client.resolve_comment_target(
        file_token="wikcn_1",
        file_type="wiki",
    )
    assert target.supported is False
    assert target.reason == "wiki_resolution_failed"


@pytest.mark.asyncio
async def test_comment_context_reply_id_selection_hit_miss_and_empty():
    replies = [
        {
            "reply_id": "r1",
            "content": {
                "elements": [{"type": "text_run", "text_run": {"text": "first"}}]
            },
        },
        {
            "reply_id": "r2",
            "content": {
                "elements": [{"type": "text_run", "text_run": {"text": "second"}}]
            },
        },
    ]

    async def raw(req):
        return {
            "code": 0,
            "data": {
                "comment": {
                    "content": {
                        "elements": [
                            {"type": "text_run", "text_run": {"text": "question"}}
                        ]
                    },
                    "quote": {
                        "elements": [
                            {"type": "text_run", "text_run": {"text": "quote"}}
                        ]
                    },
                    "reply_list": {"replies": replies},
                }
            },
        }

    client = CommentPrimitiveClient(raw_request=raw)
    target = CommentTarget(file_token="doc_1", file_type="docx", supported=True)
    context = await client.get_comment_context(
        target=target,
        comment_id="c1",
        event_reply_id="r1",
    )
    assert context.target_reply_id == "r1"
    assert context.question == "question"
    assert context.quote == "quote"

    context = await client.get_comment_context(
        target=target,
        comment_id="c1",
        event_reply_id="missing",
    )
    assert context.target_reply_id is None

    context = await client.get_comment_context(target=target, comment_id="c1")
    assert context.target_reply_id == "r2"

    async def no_replies(req):
        return {"code": 0, "data": {"comment": {"content": "q", "reply_list": {"replies": []}}}}

    client = CommentPrimitiveClient(raw_request=no_replies)
    context = await client.get_comment_context(target=target, comment_id="c1")
    assert context.target_reply_id is None


@pytest.mark.asyncio
async def test_comment_context_falls_back_to_list_on_get_failure():
    calls = []

    async def raw(req):
        calls.append(req.uri)
        if len(calls) == 1:
            return {"code": 999, "msg": "missing"}
        return {
            "code": 0,
            "data": {
                "items": [
                    {"comment_id": "other", "content": "other"},
                    {
                        "comment_id": "c1",
                        "content": "q",
                        "reply_list": {"replies": [{"reply_id": "r1"}]},
                    },
                ]
            },
        }

    client = CommentPrimitiveClient(raw_request=raw)
    target = CommentTarget(file_token="doc_1", file_type="docx", supported=True)
    context = await client.get_comment_context(target=target, comment_id="c1")

    assert calls == [
        "/open-apis/drive/v1/files/:file_token/comments/:comment_id",
        "/open-apis/drive/v1/files/:file_token/comments",
    ]
    assert context.question == "q"
    assert context.target_reply_id == "r1"


@pytest.mark.asyncio
async def test_comment_context_does_not_select_unmatched_list_item():
    async def raw(req):
        if req.uri.endswith("/:comment_id"):
            return {"code": 999, "msg": "missing"}
        return {
            "code": 0,
            "data": {
                "items": [
                    {
                        "comment_id": "other",
                        "content": "other",
                        "reply_list": {"replies": [{"reply_id": "r1"}]},
                    }
                ]
            },
        }

    client = CommentPrimitiveClient(raw_request=raw)
    target = CommentTarget(file_token="doc_1", file_type="docx", supported=True)
    context = await client.get_comment_context(target=target, comment_id="c1")

    assert context.comment_id == "c1"
    assert context.question == ""
    assert context.target_reply_id is None


@pytest.mark.asyncio
async def test_comment_client_fetch_and_list_are_raw_facades():
    calls = []

    async def raw(req):
        calls.append((req.uri, dict(req.paths), dict(req.queries)))
        return {"code": 0, "data": {"comment": {"comment_id": "c1"}}}

    client = CommentPrimitiveClient(raw_request=raw)
    target = CommentTarget(file_token="doc_1", file_type="docx", supported=True)

    fetched = await client.fetch_comment(target=target, comment_id="c1")
    listed = await client.list_comments(target=target, page_size=10)

    assert fetched["data"]["comment"]["comment_id"] == "c1"
    assert calls[0][0] == (
        "/open-apis/drive/v1/files/:file_token/comments/:comment_id"
    )
    assert calls[0][1] == {"file_token": "doc_1", "comment_id": "c1"}
    assert calls[1][0] == "/open-apis/drive/v1/files/:file_token/comments"
    assert calls[1][1] == {"file_token": "doc_1"}
    assert calls[1][2]["file_type"] == "docx"
    assert calls[1][2]["page_size"] == "10"
    assert listed["code"] == 0


@pytest.mark.asyncio
async def test_comment_client_list_replies_uses_confirmed_reply_endpoint():
    calls = []

    async def raw(req):
        calls.append((req.uri, dict(req.paths), dict(req.queries)))
        return {"code": 0, "data": {"items": [{"reply_id": "r1"}]}}

    client = CommentPrimitiveClient(raw_request=raw)
    target = CommentTarget(file_token="doc_1", file_type="docx", supported=True)

    result = await client.list_comment_replies(
        target=target,
        comment_id="c1",
        page_size=20,
    )

    assert result["data"]["items"][0]["reply_id"] == "r1"
    assert calls[0][0] == (
        "/open-apis/drive/v1/files/:file_token/comments/:comment_id/replies"
    )
    assert calls[0][1] == {"file_token": "doc_1", "comment_id": "c1"}
    assert calls[0][2]["file_type"] == "docx"
    assert calls[0][2]["page_size"] == "20"
