from lark_channel.core.enum import AccessTokenType, HttpMethod
from lark_channel.core.model import BaseRequest


def _request(method: HttpMethod, uri: str) -> BaseRequest:
    req = BaseRequest()
    req.http_method = method
    req.uri = uri
    req.token_types = {AccessTokenType.TENANT, AccessTokenType.USER}
    return req


def _reply_content(content: str):
    return {
        "elements": [
            {
                "type": "text_run",
                "text_run": {"text": content},
            }
        ],
    }


def build_comment_get_request(*, file_token: str, file_type: str, comment_id: str) -> BaseRequest:
    req = _request(HttpMethod.GET, "/open-apis/drive/v1/files/:file_token/comments/:comment_id")
    req.paths["file_token"] = file_token
    req.paths["comment_id"] = comment_id
    req.add_query("file_type", file_type)
    return req


def build_comment_list_request(
    *,
    file_token: str,
    file_type: str,
    page_token=None,
    page_size=None,
    is_whole=None,
    is_solved=None,
) -> BaseRequest:
    req = _request(HttpMethod.GET, "/open-apis/drive/v1/files/:file_token/comments")
    req.paths["file_token"] = file_token
    req.add_query("file_type", file_type)
    if page_token is not None:
        req.add_query("page_token", page_token)
    if page_size is not None:
        req.add_query("page_size", page_size)
    if is_whole is not None:
        req.add_query("is_whole", is_whole)
    if is_solved is not None:
        req.add_query("is_solved", is_solved)
    return req


def build_comment_reply_list_request(
    *,
    file_token: str,
    file_type: str,
    comment_id: str,
    page_token=None,
    page_size=None,
) -> BaseRequest:
    req = _request(
        HttpMethod.GET,
        "/open-apis/drive/v1/files/:file_token/comments/:comment_id/replies",
    )
    req.paths["file_token"] = file_token
    req.paths["comment_id"] = comment_id
    req.add_query("file_type", file_type)
    if page_token is not None:
        req.add_query("page_token", page_token)
    if page_size is not None:
        req.add_query("page_size", page_size)
    return req


def build_comment_create_request(*, file_token: str, file_type: str, content: str) -> BaseRequest:
    req = _request(HttpMethod.POST, "/open-apis/drive/v1/files/:file_token/comments")
    req.paths["file_token"] = file_token
    req.add_query("file_type", file_type)
    req.body = {"reply_list": {"replies": [{"content": _reply_content(content)}]}}
    return req


def build_comment_reply_update_request(
    *, file_token: str, file_type: str, comment_id: str, reply_id: str, content: str
) -> BaseRequest:
    req = _request(
        HttpMethod.PUT,
        "/open-apis/drive/v1/files/:file_token/comments/:comment_id/replies/:reply_id",
    )
    req.paths["file_token"] = file_token
    req.paths["comment_id"] = comment_id
    req.paths["reply_id"] = reply_id
    req.add_query("file_type", file_type)
    req.body = {"content": _reply_content(content)}
    return req
