"""Turning one wire event into a stream of session events.

Two shapes, one output. The push transport flattens ``*_items`` onto the
activity object; the poll transport nests them under ``payload`` along with
``activity_event_type``. Both are read, because both are what the platform
sends, and an implementation that reads only one produces an event stream that
is empty for half its inputs without ever raising.
"""

import re
from typing import Any, Dict, List, Optional, Tuple

from .coerce import actor_id, meeting_id_str, to_ms
from .types import (
    ACTIVITY_ACTOR_FIELDS,
    ACTIVITY_ITEM_FIELDS,
    ActivityTypeStats,
    DOCUMENT_CONTEXT_KINDS,
    DocumentContextEvent,
    MeetingActor,
    MeetingChatEvent,
    ParticipantEvent,
    ShareDocInfo,
    ShareEvent,
    TranscriptEvent,
)

#: Bucket for activity types we will not turn into a metric key.
OTHER_STAT_KEY = "__other__"
#: Distinct stat keys allowed before everything new lands in the bucket.
MAX_STAT_KEYS = 5000
#: Shape a server-provided type must have to become a key of its own. Bounding
#: the *count* does not stop one two-hundred-character key, and does not stop a
#: key with a newline in it from reshaping a log line.
_STAT_KEY_SHAPE = re.compile(r"^[a-z0-9_]{1,64}$")

_ACTION_BY_TYPE = {
    "participant_joined": "joined",
    "participant_left": "left",
    "magic_share_started": "started",
    "magic_share_ended": "ended",
}

#: Session event name per activity type.
_EVENT_NAME_BY_TYPE = {
    "transcript_received": "transcript",
    "chat_received": "chat",
    "participant_joined": "participant",
    "participant_left": "participant",
    "magic_share_started": "share",
    "magic_share_ended": "share",
    "document_context_changed": "document_context",
}


def stat_key(activity_event_type: Any) -> str:
    """The metric key for ``activity_event_type``, or the shared bucket."""
    if isinstance(activity_event_type, str) and _STAT_KEY_SHAPE.match(
        activity_event_type
    ):
        return activity_event_type
    return OTHER_STAT_KEY


def bump_stats(
    stats: Dict[str, ActivityTypeStats], key: str, *, empty: bool
) -> None:
    """Account for one activity object, keeping the key space bounded."""
    if key not in stats and len(stats) >= MAX_STAT_KEYS:
        key = OTHER_STAT_KEY
    entry = stats.get(key)
    if entry is None:
        entry = ActivityTypeStats()
        stats[key] = entry
    entry.received += 1
    if empty:
        entry.empty += 1


def activity_type_of(activity: Dict[str, Any]) -> Optional[str]:
    """``activity_event_type``, from wherever this transport put it."""
    direct = activity.get("activity_event_type")
    if isinstance(direct, str) and direct:
        return direct
    payload = activity.get("payload")
    if isinstance(payload, dict):
        nested = payload.get("activity_event_type")
        if isinstance(nested, str) and nested:
            return nested
    return None


def items_of(activity: Dict[str, Any], activity_event_type: str) -> List[Dict[str, Any]]:
    """The inner ``*_items`` list, from either nesting."""
    field = ACTIVITY_ITEM_FIELDS.get(activity_event_type)
    if field is None:
        return []
    flat = activity.get(field)
    if isinstance(flat, list):
        return [item for item in flat if isinstance(item, dict)]
    payload = activity.get("payload")
    if isinstance(payload, dict):
        nested = payload.get(field)
        if isinstance(nested, list):
            return [item for item in nested if isinstance(item, dict)]
    return []


def meeting_id_of(activity: Dict[str, Any], fallback: Optional[str]) -> Optional[str]:
    """The long meeting id this activity belongs to, always as a string."""
    for candidate in (activity.get("meeting_id"), (activity.get("meeting") or {}).get("id")
                      if isinstance(activity.get("meeting"), dict) else None):
        resolved = meeting_id_str(candidate)
        if resolved:
            return resolved
    return fallback


def _actor_of(item: Dict[str, Any], activity_event_type: str) -> MeetingActor:
    field = ACTIVITY_ACTOR_FIELDS.get(activity_event_type)
    raw = item.get(field) if field else None
    if not isinstance(raw, dict):
        raw = {}
    return MeetingActor(
        id=actor_id(raw),
        name=raw.get("name") or raw.get("user_name"),
        user_type=raw.get("user_type"),
        user_role=raw.get("user_role"),
    )


def _doc_of(item: Dict[str, Any]) -> Optional[ShareDocInfo]:
    # The field is `share_doc`; reading `doc` yields None for every share.
    raw = item.get("share_doc")
    if not isinstance(raw, dict):
        return None
    return ShareDocInfo(url=raw.get("url"), title=raw.get("title"))


def _context_type_of(item: Dict[str, Any]) -> Optional[str]:
    """Which kind of document context this is.

    Derived from whichever sub-object is present, because the generated model
    has no discriminator. An explicit ``context_type`` in the payload wins:
    that is the platform having moved ahead of the generated types, and there
    is no reason to prefer our inference over its statement.
    """
    explicit = item.get("context_type")
    if isinstance(explicit, str) and explicit:
        return explicit
    for kind in DOCUMENT_CONTEXT_KINDS:
        if isinstance(item.get(kind), dict):
            return kind
    return None


def build_event(
    activity_event_type: str,
    item: Dict[str, Any],
    *,
    meeting_id: str,
    include_raw: bool,
) -> Optional[Tuple[str, Any]]:
    """``(session event name, event)`` for one inner item, or ``None`` to skip."""
    name = _EVENT_NAME_BY_TYPE.get(activity_event_type)
    if name is None:
        return None
    common = {
        "meeting_id": meeting_id,
        "actor": _actor_of(item, activity_event_type),
        "raw": dict(item) if include_raw else None,
    }
    if activity_event_type == "transcript_received":
        return name, TranscriptEvent(
            text=item.get("text") or "",
            sentence_id=item.get("sentence_id"),
            language=item.get("language"),
            start_ms=to_ms(item.get("start_time_ms")),
            end_ms=to_ms(item.get("end_time_ms")),
            **common
        )
    if activity_event_type == "chat_received":
        return name, MeetingChatEvent(
            content=item.get("content") or "",
            message_id=item.get("message_id"),
            message_type=item.get("message_type"),
            send_time=to_ms(item.get("send_time")),
            **common
        )
    if activity_event_type in ("participant_joined", "participant_left"):
        return name, ParticipantEvent(
            action=_ACTION_BY_TYPE[activity_event_type],
            join_time=to_ms(item.get("join_time")),
            leave_time=to_ms(item.get("leave_time")),
            leave_reason=item.get("leave_reason"),
            **common
        )
    if activity_event_type in ("magic_share_started", "magic_share_ended"):
        return name, ShareEvent(
            action=_ACTION_BY_TYPE[activity_event_type],
            share_id=item.get("share_id"),
            doc=_doc_of(item),
            time=to_ms(item.get("time")),
            **common
        )
    context_type = _context_type_of(item)
    if context_type is None:
        # A fourth kind of context the platform has started sending. Forward
        # compatibility, not a parse failure — see `unpack`.
        return None
    return name, DocumentContextEvent(
        context_type=context_type,
        share_id=item.get("share_id"),
        doc=_doc_of(item),
        time=to_ms(item.get("time")),
        comment_focus=item.get("comment_focus"),
        section_location=item.get("section_location"),
        element_preview=item.get("element_preview"),
        **common
    )


def unpack(
    activity: Dict[str, Any],
    *,
    meeting_id: str,
    include_raw: bool,
    stats: Dict[str, ActivityTypeStats],
) -> List[Tuple[str, Any]]:
    """Every session event in one activity object, in array order.

    Also does the stat accounting, because "how many arrived" and "how many
    unpacked to nothing" are only knowable here:

    * an activity type we do not know counts as ``empty`` — that is us having
      fallen behind the platform, which is what the counter is for;
    * a ``document_context_changed`` item with none of the three known
      sub-objects is skipped and **not** counted as empty. That is the platform
      adding a kind of context, and counting it would report a field-shape
      regression that did not happen.
    """
    activity_event_type = activity_type_of(activity)
    if activity_event_type is None:
        bump_stats(stats, OTHER_STAT_KEY, empty=True)
        return []
    key = stat_key(activity_event_type)
    items = items_of(activity, activity_event_type)
    built = []
    for item in items:
        event = build_event(
            activity_event_type,
            item,
            meeting_id=meeting_id,
            include_raw=include_raw,
        )
        if event is not None:
            built.append(event)
    known = activity_event_type in ACTIVITY_ITEM_FIELDS
    if known:
        # Items present but all skipped is forward compatibility, not a
        # failure; no items at all is what `empty` means.
        empty = not items
    else:
        empty = True
    bump_stats(stats, key, empty=empty)
    return built


__all__ = [
    "MAX_STAT_KEYS",
    "OTHER_STAT_KEY",
    "activity_type_of",
    "build_event",
    "bump_stats",
    "items_of",
    "meeting_id_of",
    "stat_key",
    "unpack",
]
