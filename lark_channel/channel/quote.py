import inspect
import json
from typing import Any, Callable, Dict, Iterable, List, Optional

from .types import InboundMessage, QuoteResolution, QuotedContext


class QuoteResolver:
    def __init__(self, *, fetcher: Callable[[str], Any]) -> None:
        self._fetcher = fetcher

    async def fetch_quoted_context(self, message_id: str) -> Optional[QuotedContext]:
        try:
            raw = self._fetcher(message_id)
            if inspect.isawaitable(raw):
                raw = await raw
        except Exception:
            return None
        raw = _object_to_dict(raw)
        if not isinstance(raw, dict):
            return None
        code = raw.get("code")
        if code is not None and code != 0:
            return None
        return _context_from_raw(message_id, raw)

    async def resolve_quoted_contexts(
        self,
        messages: Iterable[InboundMessage],
        *,
        chat_mode: Optional[str],
    ) -> Dict[str, QuoteResolution]:
        batch = list(messages)
        batch_ids = {msg.id for msg in batch}
        parent_ids: List[str] = []
        results: Dict[str, QuoteResolution] = {}

        for msg in batch:
            parent_id = msg.reply.message_id if msg.reply else None
            root_id = _message_raw_field(msg.raw, "root_id")
            if not parent_id:
                results[msg.id] = QuoteResolution(msg.id, None, "no_parent")
            elif chat_mode in ("topic", "thread") and parent_id == root_id:
                results[msg.id] = QuoteResolution(msg.id, parent_id, "topic_root")
            elif parent_id in batch_ids:
                results[msg.id] = QuoteResolution(msg.id, parent_id, "in_batch")
            else:
                results[msg.id] = QuoteResolution(msg.id, parent_id, "fetch_failed")
                if parent_id not in parent_ids:
                    parent_ids.append(parent_id)

        fetched = {}
        for parent_id in parent_ids:
            fetched[parent_id] = await self.fetch_quoted_context(parent_id)

        for msg in batch:
            current = results[msg.id]
            parent_id = current.parent_message_id
            if (
                current.decision == "fetch_failed"
                and parent_id in fetched
                and fetched[parent_id] is not None
            ):
                results[msg.id] = QuoteResolution(
                    msg.id,
                    parent_id,
                    "resolved",
                    fetched[parent_id],
                )
        return results


def _context_from_raw(message_id: str, raw: Dict[str, Any]) -> QuotedContext:
    data = raw.get("data") if isinstance(raw.get("data"), dict) else raw
    item = _first_message_item(data)
    body = item.get("body") if isinstance(item.get("body"), dict) else {}
    content = _decode_content(
        body.get("content") if body.get("content") is not None else item.get("content")
    )
    sender = _object_to_dict(item.get("sender") or {})
    return QuotedContext(
        message_id=item.get("message_id") or message_id,
        text=_flatten_content(content),
        sender_id=_sender_id(sender),
        create_time=_as_int(item.get("create_time")),
        content_type=item.get("msg_type") or item.get("message_type"),
        raw=item,
    )


def _first_message_item(raw: Dict[str, Any]) -> Dict[str, Any]:
    items = raw.get("items")
    if isinstance(items, list) and items and isinstance(items[0], dict):
        return items[0]
    return raw


def _message_raw_field(raw: Dict[str, Any], key: str) -> Any:
    if not isinstance(raw, dict):
        return None
    nested = raw.get("message")
    if isinstance(nested, dict) and nested.get(key) is not None:
        return nested.get(key)
    return raw.get(key)


def _sender_id(sender: Dict[str, Any]) -> Optional[str]:
    if not isinstance(sender, dict):
        return None
    return (
        sender.get("open_id")
        or sender.get("user_id")
        or sender.get("union_id")
        or sender.get("id")
    )


def _decode_content(content: Any) -> Any:
    if isinstance(content, str):
        try:
            return json.loads(content)
        except ValueError:
            return {"text": content}
    return content or {}


def _flatten_content(content: Any) -> str:
    if isinstance(content, dict):
        parts: List[str] = []
        for key in ("text", "title", "content", "summary"):
            value = content.get(key)
            if isinstance(value, str) and value:
                parts.append(value)
            elif isinstance(value, (dict, list)):
                nested = _flatten_content(value)
                if nested:
                    parts.append(nested)
        for key in ("elements", "blocks", "items", "messages"):
            value = content.get(key)
            if isinstance(value, list):
                parts.extend(part for part in (_flatten_content(item) for item in value) if part)
        return "\n".join(part for part in parts if part)
    if isinstance(content, list):
        return "\n".join(part for part in (_flatten_content(item) for item in content) if part)
    return str(content) if content else ""


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


def _as_int(value: Any) -> Optional[int]:
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None
