import json
import inspect
from typing import Any, Callable, Dict, Optional

from lark_channel.api.drive.comment import (
    build_comment_create_request,
    build_comment_get_request,
    build_comment_list_request,
    build_comment_reply_list_request,
    build_comment_reply_update_request,
)
from lark_channel.api.wiki.node import build_wiki_node_get_request

from .types import CommentContext, CommentTarget


SUPPORTED_FILE_TYPES = {"doc", "docx", "sheet", "file"}


class CommentPrimitiveClient:
    def __init__(self, *, raw_request: Callable[[Any], Any]) -> None:
        self._raw_request = raw_request

    def resolve_comment_target_sync(
        self,
        *,
        file_token: str,
        file_type: str,
    ) -> CommentTarget:
        if file_type in SUPPORTED_FILE_TYPES:
            return CommentTarget(
                file_token=file_token,
                file_type=file_type,
                supported=True,
            )
        if file_type == "wiki":
            return CommentTarget(
                file_token=file_token,
                file_type=file_type,
                supported=True,
                reason="wiki_requires_async_resolution",
            )
        return CommentTarget(
            file_token=file_token,
            file_type=file_type,
            supported=False,
            reason="unsupported_file_type",
        )

    async def resolve_comment_target(
        self,
        *,
        file_token: str,
        file_type: str,
    ) -> CommentTarget:
        direct = self.resolve_comment_target_sync(
            file_token=file_token,
            file_type=file_type,
        )
        if file_type != "wiki":
            return direct

        raw = await self._call(build_wiki_node_get_request(token=file_token))
        data = (raw or {}).get("data") or {}
        node = data.get("node") or data
        obj_token = node.get("obj_token")
        obj_type = node.get("obj_type")
        if obj_token and obj_type in SUPPORTED_FILE_TYPES:
            return CommentTarget(
                file_token=obj_token,
                file_type=obj_type,
                supported=True,
                raw=node,
            )
        return CommentTarget(
            file_token=file_token,
            file_type=file_type,
            supported=False,
            reason="wiki_resolution_failed",
            raw=node if isinstance(node, dict) else {},
        )

    async def get_comment_context(
        self,
        *,
        target: CommentTarget,
        comment_id: str,
        event_reply_id: Optional[str] = None,
    ) -> CommentContext:
        if not target.supported:
            return CommentContext(target, comment_id, "", "", False, None)

        raw = await self._call(
            build_comment_get_request(
                file_token=target.file_token,
                file_type=target.file_type,
                comment_id=comment_id,
            )
        )
        if not raw or raw.get("code") != 0:
            raw = await self._call(
                build_comment_list_request(
                    file_token=target.file_token,
                    file_type=target.file_type,
                )
            )

        data = (raw or {}).get("data") or {}
        comment = _select_comment(data, comment_id)
        replies = _comment_replies(comment)
        target_reply_id = _select_reply_id(replies, event_reply_id)
        return CommentContext(
            target=target,
            comment_id=comment_id,
            question=_content_to_text(comment.get("content")),
            quote=_content_to_text(comment.get("quote")),
            is_whole=bool(comment.get("is_whole")),
            target_reply_id=target_reply_id,
            raw=comment,
        )

    async def fetch_comment(
        self,
        *,
        target: CommentTarget,
        comment_id: str,
    ):
        if not target.supported:
            return None
        return await self._call(
            build_comment_get_request(
                file_token=target.file_token,
                file_type=target.file_type,
                comment_id=comment_id,
            )
        )

    async def list_comments(
        self,
        *,
        target: CommentTarget,
        page_token=None,
        page_size=None,
        is_whole=None,
        is_solved=None,
    ):
        if not target.supported:
            return None
        return await self._call(
            build_comment_list_request(
                file_token=target.file_token,
                file_type=target.file_type,
                page_token=page_token,
                page_size=page_size,
                is_whole=is_whole,
                is_solved=is_solved,
            )
        )

    async def list_comment_replies(
        self,
        *,
        target: CommentTarget,
        comment_id: str,
        page_token=None,
        page_size=None,
    ):
        if not target.supported:
            return None
        return await self._call(
            build_comment_reply_list_request(
                file_token=target.file_token,
                file_type=target.file_type,
                comment_id=comment_id,
                page_token=page_token,
                page_size=page_size,
            )
        )

    async def reply_comment(self, context: CommentContext, content: str):
        if context.is_whole:
            return await self._call(
                build_comment_create_request(
                    file_token=context.target.file_token,
                    file_type=context.target.file_type,
                    content=content,
                )
            )
        if not context.target_reply_id:
            return None
        return await self._call(
            build_comment_reply_update_request(
                file_token=context.target.file_token,
                file_type=context.target.file_type,
                comment_id=context.comment_id,
                reply_id=context.target_reply_id,
                content=content,
            )
        )

    async def _call(self, req):
        result = self._raw_request(req)
        if inspect.isawaitable(result):
            result = await result
        return _response_to_dict(result)


def _select_reply_id(replies, event_reply_id):
    if event_reply_id:
        for reply in replies:
            if reply.get("reply_id") == event_reply_id:
                return event_reply_id
        return None
    if replies:
        return replies[-1].get("reply_id")
    return None


def _comment_replies(comment: Dict[str, Any]):
    reply_list = comment.get("reply_list")
    if isinstance(reply_list, dict):
        replies = reply_list.get("replies")
        if isinstance(replies, list):
            return replies
    replies = comment.get("replies")
    return replies if isinstance(replies, list) else []


def _select_comment(data: Dict[str, Any], comment_id: str) -> Dict[str, Any]:
    comment = data.get("comment")
    if isinstance(comment, dict):
        return comment
    items = data.get("items") or data.get("comments") or []
    for item in items:
        if item.get("comment_id") == comment_id:
            return item
    return data


def _response_to_dict(result: Any) -> Optional[Dict[str, Any]]:
    if result is None:
        return None
    if isinstance(result, dict):
        return _object_to_dict(result)
    raw = getattr(result, "raw", None)
    content = getattr(raw, "content", None)
    if content:
        if isinstance(content, bytes):
            content = content.decode("utf-8")
        if isinstance(content, str):
            try:
                parsed = json.loads(content)
            except ValueError:
                parsed = None
            if isinstance(parsed, dict):
                return _object_to_dict(parsed)
    data = getattr(result, "data", None)
    return {
        "code": getattr(result, "code", 0),
        "data": _object_to_dict(data),
    }


def _content_to_text(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, str):
        return value
    if isinstance(value, list):
        return "".join(_content_to_text(item) for item in value)
    if not isinstance(value, dict):
        return str(value)

    text_run = value.get("text_run")
    if isinstance(text_run, dict) and isinstance(text_run.get("text"), str):
        return text_run["text"]
    if isinstance(value.get("text"), str):
        return value["text"]
    if isinstance(value.get("content"), str):
        return value["content"]

    parts = []
    for key in ("elements", "content", "children"):
        nested = value.get(key)
        if nested is not None:
            parts.append(_content_to_text(nested))
    return "".join(parts)


def _object_to_dict(value: Any) -> Any:
    if isinstance(value, dict):
        return {key: _object_to_dict(val) for key, val in value.items()}
    if isinstance(value, list):
        return [_object_to_dict(item) for item in value]
    if hasattr(value, "__dict__"):
        return {
            key: _object_to_dict(val)
            for key, val in value.__dict__.items()
            if not key.startswith("_")
        }
    return value
