"""Identifier and timestamp coercion for meeting payloads.

Every function here exists because the generated ``vc`` models disagree with
what the platform actually sends on at least one transport. Getting any of
them wrong fails silently — no exception, no log line, just an event stream
that quietly routes nowhere or an echo check that never matches.
"""

from typing import Any, Optional

#: Order matters: the bot's own identity is an open_id, so an actor id has to
#: resolve to the same namespace for echo detection to work at all.
_ID_KEYS = ("open_id", "union_id", "user_id")


def actor_id(actor: Any) -> Optional[str]:
    """The actor's open_id, whichever shape the transport used.

    The generated model declares ``id: str``, and the poll transport does send
    a bare string — it pins ``user_id_type=open_id`` in the query. The push
    transport takes no such parameter, so it sends every namespace it has:
    ``{"open_id": ..., "union_id": ..., "user_id": ...}``.

    An implementation that only reads the string form gets ``None`` for every
    pushed actor, and ``self_echo`` then compares the bot's open_id against
    nothing for the rest of the process.
    """
    if not isinstance(actor, dict):
        return None
    raw = actor.get("id")
    if isinstance(raw, str) and raw:
        return raw
    if isinstance(raw, dict):
        for key in _ID_KEYS:
            value = raw.get(key)
            if isinstance(value, str) and value:
                return value
    # Some payloads omit `id` and put the namespaces directly on the actor.
    for key in _ID_KEYS:
        value = actor.get(key)
        if isinstance(value, str) and value:
            return value
    return None


def meeting_id_str(value: Any) -> Optional[str]:
    """The long meeting id as a string, whichever type it arrived as.

    ``bots/join`` and ``user_active_meeting`` return it as a string; the push
    envelope sends the same value as an ``int``. Python does not compare those
    as equal and ``sessions["7654321"]`` does not find ``sessions[7654321]``,
    so skipping this normalization drops every pushed event — and dropping
    events for an unrecognized meeting is by design, so nothing complains.
    """
    if value is None:
        return None
    if isinstance(value, bool):  # bool is an int subclass; never a meeting id
        return None
    if isinstance(value, str):
        return value or None
    if isinstance(value, int):
        return str(value)
    return None


def to_ms(value: Any) -> Optional[int]:
    """A millisecond timestamp as an int, or ``None`` if it is not one.

    Item-level time fields (``start_time_ms``, ``send_time``, ``join_time``,
    ``time``, ...) arrive as strings; meeting-level ones arrive as ints.
    Unparsable input yields ``None`` rather than raising: a malformed
    timestamp is not a reason to drop a transcript.
    """
    if value is None or isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return int(value)
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return None
        try:
            return int(text)
        except ValueError:
            try:
                return int(float(text))
            except ValueError:
                return None
    return None
