from lark_channel.api.drive.comment import (
    build_comment_create_request,
    build_comment_get_request,
    build_comment_list_request,
    build_comment_reply_update_request,
)
from lark_channel.api.wiki.node import build_wiki_node_get_request
from lark_channel.core.enum import AccessTokenType, HttpMethod


def test_comment_get_request_shape():
    req = build_comment_get_request(file_token="doc_1", file_type="docx", comment_id="c1")
    assert req.http_method == HttpMethod.GET
    assert req.uri == "/open-apis/drive/v1/files/:file_token/comments/:comment_id"
    assert req.paths == {"file_token": "doc_1", "comment_id": "c1"}
    assert dict(req.queries) == {"file_type": "docx"}
    assert req.token_types == {AccessTokenType.TENANT, AccessTokenType.USER}


def test_comment_list_request_shape():
    req = build_comment_list_request(file_token="doc_1", file_type="docx")
    assert req.http_method == HttpMethod.GET
    assert req.uri == "/open-apis/drive/v1/files/:file_token/comments"
    assert req.paths == {"file_token": "doc_1"}
    assert dict(req.queries) == {"file_type": "docx"}


def test_comment_reply_update_request_shape():
    req = build_comment_reply_update_request(
        file_token="doc_1",
        file_type="docx",
        comment_id="c1",
        reply_id="r1",
        content="answer",
    )
    assert req.http_method == HttpMethod.PUT
    assert req.uri == "/open-apis/drive/v1/files/:file_token/comments/:comment_id/replies/:reply_id"
    assert req.paths == {"file_token": "doc_1", "comment_id": "c1", "reply_id": "r1"}
    assert dict(req.queries) == {"file_type": "docx"}
    assert req.body == {
        "content": {
            "elements": [{"type": "text_run", "text_run": {"text": "answer"}}],
        },
    }


def test_comment_create_request_shape():
    req = build_comment_create_request(file_token="doc_1", file_type="docx", content="answer")
    assert req.http_method == HttpMethod.POST
    assert req.uri == "/open-apis/drive/v1/files/:file_token/comments"
    assert req.paths == {"file_token": "doc_1"}
    assert dict(req.queries) == {"file_type": "docx"}
    assert req.body == {
        "reply_list": {
            "replies": [
                {
                    "content": {
                        "elements": [{"type": "text_run", "text_run": {"text": "answer"}}],
                    },
                }
            ],
        },
    }


def test_wiki_node_get_request_shape():
    req = build_wiki_node_get_request(token="wikcn_x")
    assert req.http_method == HttpMethod.GET
    assert req.uri == "/open-apis/wiki/v2/spaces/get_node"
    assert dict(req.queries) == {"token": "wikcn_x"}
    assert req.token_types == {AccessTokenType.TENANT, AccessTokenType.USER}
