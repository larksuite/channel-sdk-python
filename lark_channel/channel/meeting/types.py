"""Public types for the meeting channel.

Fields marked *untrusted* are written by meeting participants or the meeting's
creator — who may be external or guest users. They must never be interpolated
into a log message body (only passed as lazy logging arguments, after control
characters are stripped), and never rendered into HTML or a chat message
without escaping.
"""

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

#: Session-level event names accepted by :meth:`MeetingSession.on`.
MEETING_EVENT_NAMES = (
    "transcript",
    "chat",
    "participant",
    "share",
    "document_context",
    "end",
    "error",
)


class MeetingEvents:
    """String constants for :meth:`MeetingSession.on` event names.

    Prefer these over literals so a typo surfaces as ``AttributeError`` at
    import time rather than a handler that is never called.
    """

    TRANSCRIPT = "transcript"
    CHAT = "chat"
    PARTICIPANT = "participant"
    SHARE = "share"
    DOCUMENT_CONTEXT = "document_context"
    END = "end"
    ERROR = "error"


@dataclass
class MeetingActor:
    """Whoever produced an item: speaker, chat sender, or participant.

    ``id`` is always an ``open_id`` — the channel pins ``user_id_type`` on
    every request so this shares a namespace with the bot's own open_id.
    """

    id: Optional[str] = None
    #: untrusted — the participant's display name.
    name: Optional[str] = None
    user_type: Optional[int] = None
    user_role: Optional[int] = None


@dataclass
class ShareDocInfo:
    """The document being shared. Both fields untrusted."""

    url: Optional[str] = None
    title: Optional[str] = None


@dataclass
class MeetingEventBase:
    """Fields every in-meeting event carries."""

    meeting_id: str
    actor: MeetingActor
    #: This item was produced by our own bot — an in-meeting message pushed
    #: back to us, or our own speech transcribed. Delivered anyway: a
    #: minute-taking application needs the bot's own turns. Always ``False``
    #: in ``uat`` mode, where the bot is not a participant.
    #:
    #: Conservative when unknown: before the bot's own open_id is resolved
    #: this is ``True``, because ``False`` means "not me" and would wave a
    #: feedback loop straight through.
    self_echo: bool = False
    #: The raw wire item, only when ``MeetingOptions.include_raw`` is set.
    #: Unredacted and untrusted, same as an ``on_raw_event`` payload.
    raw: Optional[Dict[str, Any]] = None


@dataclass
class TranscriptEvent(MeetingEventBase):
    #: untrusted — spoken words, as transcribed.
    text: str = ""
    #: Stable within one sentence. The protocol has no "final" marker, so a
    #: later item with the same id supersedes the earlier text; consumers
    #: should upsert on this.
    sentence_id: Optional[str] = None
    language: Optional[str] = None
    start_ms: Optional[int] = None
    end_ms: Optional[int] = None


@dataclass
class MeetingChatEvent(MeetingEventBase):
    #: untrusted — the message the participant typed.
    content: str = ""
    message_id: Optional[str] = None
    message_type: Optional[int] = None
    send_time: Optional[int] = None


@dataclass
class ParticipantEvent(MeetingEventBase):
    #: ``"joined"`` or ``"left"``.
    action: str = ""
    join_time: Optional[int] = None
    leave_time: Optional[int] = None
    leave_reason: Optional[int] = None


@dataclass
class ShareEvent(MeetingEventBase):
    #: ``"started"`` or ``"ended"``.
    action: str = ""
    share_id: Optional[str] = None
    doc: Optional[ShareDocInfo] = None
    time: Optional[int] = None


@dataclass
class DocumentContextEvent(MeetingEventBase):
    """A change of context inside the shared document.

    Identifiers only, never content: a comment gives you ``comment_id``, an
    image or board gives you ``element_token``. Fetching the body or the asset
    is the application's job, within the shared document's temporary grant,
    with the permissions it applied for itself.

    ``context_type`` is derived from whichever sub-object is present, because
    the generated model has no discriminator field. A payload that does carry
    an explicit ``context_type`` is believed over the derivation — that is the
    platform having moved ahead of the generated types.
    """

    #: ``"comment_focus"`` / ``"section_location"`` / ``"element_preview"``,
    #: or whatever the platform sends explicitly.
    context_type: str = ""
    share_id: Optional[str] = None
    doc: Optional[ShareDocInfo] = None
    time: Optional[int] = None
    comment_focus: Optional[Dict[str, Any]] = None
    #: untrusted — headings come from the document.
    section_location: Optional[Dict[str, Any]] = None
    #: untrusted — ``element_token`` is an opaque identifier from the document.
    element_preview: Optional[Dict[str, Any]] = None


@dataclass
class MeetingEndEvent:
    """The session is over. No further events will be delivered on it."""

    meeting_id: str
    #: ``"meeting_ended"``   the platform said the meeting ended (tat)
    #: ``"no_longer_active"`` the user left it (uat) or a probe proved the bot
    #:                        is no longer a participant (tat)
    #: ``"idle_timeout"``    no activity for ``idle_timeout_seconds`` (tat)
    #: ``"error"``           the event source stopped unrecoverably
    #: ``"left"``            the application called ``leave()``
    #: ``"disposed"``        the application called ``dispose()``, or the
    #:                       channel disconnected
    reason: str = ""


@dataclass
class MeetingInvitedEvent:
    """The bot was invited into a meeting. Delivered on the channel, not on a
    session — at this point no session exists yet.

    **This is not covered by** :class:`~..config.PolicyConfig`. It is the only
    way ``join_meeting`` gets triggered, and anybody who can add the bot to a
    meeting can trigger it. ``MeetingChannelConfig.invite_allowlist`` is the gate.
    """

    meeting_no: str
    meeting_id: Optional[str] = None
    #: untrusted — the meeting's title.
    topic: Optional[str] = None
    inviter: Optional[MeetingActor] = None
    bot: Optional[MeetingActor] = None
    call_id: Optional[str] = None
    invite_time: Optional[int] = None
    raw: Optional[Dict[str, Any]] = None


@dataclass
class MeetingOptions:
    """Per-session knobs."""

    #: Settle window for transcripts, in seconds. ``0.0`` delivers every text
    #: change, so consumers see a sentence grow word by word. A positive value
    #: delivers a sentence once, after it stops changing for that long — at
    #: the cost of one window of latency, and of a sentence never settling
    #: while somebody keeps talking.
    stabilize_seconds: float = 0.0
    #: Attach the raw wire item to each event. Off by default: the raw payload
    #: is unredacted and carries full user ids and message bodies.
    include_raw: bool = False


@dataclass
class ActivityTypeStats:
    """Per ``activity_event_type`` parse accounting.

    ``received`` counts activity objects that arrived; ``empty`` counts those
    that unpacked to nothing. Splitting them separates two diagnoses that look
    identical from outside and point in opposite directions: "the platform
    never sent it" (check the meeting setting, the subscription declaration,
    whether the bot is in the meeting) versus "it sent it and we could not
    read it" (check the field names).
    """

    received: int = 0
    empty: int = 0


@dataclass
class LivenessHealth:
    """The probe's own health.

    Without this, a tenant where the probe's permission assumption does not
    hold degrades to "reclamation never happens" with no outward sign.
    """

    last_probe_at: Optional[float] = None
    #: ``"in_meeting"`` / ``"not_in_meeting"`` / ``"unknown"``.
    last_verdict: Optional[str] = None
    consecutive_unknown: int = 0


@dataclass
class MembershipHealth:
    """Server-side participation accounting, which drives the session gate.

    Mis-accounting here shows up at the far end as ``join_meeting`` refusing
    forever; these counters are how you see it coming.
    """

    #: Seats currently held *as server-side membership* — the entries this
    #: ledger tracks. The gate's own reading is wider: it also counts live
    #: sessions, so a pure follow deployment can be refused a seat while this
    #: number is still zero.
    held: int = 0
    #: Entries kept because a departure result was inconclusive, whose session
    #: is already gone. These are what lazy reconciliation works through.
    retained_without_session: int = 0
    #: Evidence kind -> release count. Keys include ``"ok"``, ``"404"``,
    #: ``"121105"``, ``"120004"``, ``"meeting_ended"``, ``"ttl"``.
    released_by_evidence: Dict[str, int] = field(default_factory=dict)
    reconcile_attempts: int = 0


@dataclass
class MeetingEventHealth:
    """Channel-wide view of the in-meeting event path."""

    #: Whether the internal ``vc.bot.*`` registration took effect on the
    #: current dispatcher. The dispatcher is rebuilt on every ``start()``, so
    #: this reflects the most recent rebuild.
    registered: bool = False
    reason: Optional[str] = None
    received: int = 0
    last_at: Optional[float] = None
    #: Events refused because a session's delivery queue was at its ceiling,
    #: which happens when a handler is slower than the meeting. Non-zero means
    #: activity was normalized and then never handed to a handler, so the
    #: application's picture of the meeting has gaps.
    dropped: int = 0
    stats: Dict[str, ActivityTypeStats] = field(default_factory=dict)
    liveness: LivenessHealth = field(default_factory=LivenessHealth)
    membership: MembershipHealth = field(default_factory=MembershipHealth)


#: Activity type -> the array field that carries its items.
ACTIVITY_ITEM_FIELDS = {
    "transcript_received": "transcript_received_items",
    "chat_received": "chat_received_items",
    "participant_joined": "participant_joined_items",
    "participant_left": "participant_left_items",
    "magic_share_started": "magic_share_started_items",
    "magic_share_ended": "magic_share_ended_items",
    "document_context_changed": "document_context_changed_items",
}

#: Activity type -> the field naming whoever produced the item. The platform
#: spells it differently per type; the channel normalizes all of them to
#: ``actor``.
ACTIVITY_ACTOR_FIELDS = {
    "transcript_received": "speaker",
    "chat_received": "operator",
    "participant_joined": "participant",
    "participant_left": "participant",
    "magic_share_started": "operator",
    "magic_share_ended": "operator",
    "document_context_changed": "operator",
}

#: Sub-object -> the ``context_type`` it implies, in probe order.
DOCUMENT_CONTEXT_KINDS = (
    "comment_focus",
    "section_location",
    "element_preview",
)

__all__ = [
    "ACTIVITY_ACTOR_FIELDS",
    "ACTIVITY_ITEM_FIELDS",
    "ActivityTypeStats",
    "DOCUMENT_CONTEXT_KINDS",
    "DocumentContextEvent",
    "LivenessHealth",
    "MEETING_EVENT_NAMES",
    "MeetingActor",
    "MeetingChatEvent",
    "MeetingEndEvent",
    "MeetingEventBase",
    "MeetingEventHealth",
    "MeetingEvents",
    "MeetingInvitedEvent",
    "MeetingOptions",
    "MembershipHealth",
    "ParticipantEvent",
    "ShareDocInfo",
    "ShareEvent",
    "TranscriptEvent",
]
