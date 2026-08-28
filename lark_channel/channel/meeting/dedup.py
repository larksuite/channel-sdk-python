"""Suppressing redeliveries without suppressing distinct content.

Three levels, all applied at the activity level:

1. ``event_id`` — present on every polled event, absent from every pushed
   activity item. So it catches the poll/push overlap and platform redelivery,
   but the push transport relies on level 2.
2. a digest of the content — the identifying tuple plus text, timestamp and
   actor. Stands in for a missing ``event_id``.
3. ``sentence_id`` is deliberately **not** a dedup key. It is an upsert handle:
   the platform resends a sentence as the speaker keeps talking and the text
   grows, so level 2 lets each revision through on its own merits.

Every key carries the meeting id. Two meetings running at once produce
byte-identical greetings from the same person seconds apart, and a key without
the meeting in it makes one meeting's transcript disappear from the other.
"""

import hashlib
import json
from typing import Any, Dict, List, Optional


#: Same window as the message-layer cache, but a separate namespace: platform
#: event ids are global, so sharing a key space would let one layer's mark
#: swallow the other layer's event.
NAMESPACE = "channel:meeting:seen:"

def _digest(payload: Any) -> str:
    """A fixed-length digest of ``payload``.

    Hashing rather than joining keeps whole transcripts from living in memory
    for the meeting's duration, and removes the ambiguity a separator
    introduces when the content itself contains it.
    """
    encoded = json.dumps(payload, sort_keys=True, ensure_ascii=False, default=str)
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()[:32]


def _item_identity(item: Dict[str, Any]) -> Any:
    """What makes this item this item.

    The **whole** item, not a hand-picked tuple of identifier, text, timestamp
    and actor. That narrower key over-suppresses whenever the distinguishing
    detail lives outside it: three ``document_context_changed`` items differing
    only in which sub-object they carry share one identifier, one timestamp and
    one actor, so the narrow key collapses all three into one — and a dropped
    event is silent data loss, strictly worse than a duplicate delivery.

    Redelivery of the same activity is byte-identical, so the full item still
    collapses it. What the full item gives up is robustness to the platform
    adding a per-delivery volatile field, which would weaken dedup to
    "duplicates get through" — the direction that fails loudly and that the
    poll path's ``event_id`` covers anyway.

    Items with genuinely thin content still collide: the same person joining
    twice within one timestamp's resolution is one entry. That residual is
    accepted and documented.
    """
    return item


def activity_key(
    activity: Dict[str, Any],
    items: List[Dict[str, Any]],
    *,
    meeting_id: str,
    activity_event_type: str,
    event_id: Optional[str] = None,
) -> str:
    """The dedup key for one activity object."""
    if event_id:
        return "%s|evt|%s" % (meeting_id, event_id)
    identities = [_item_identity(item) for item in items]
    return "%s|content|%s" % (
        meeting_id,
        _digest([activity_event_type, identities]),
    )


__all__ = ["NAMESPACE", "activity_key"]
