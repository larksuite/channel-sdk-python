"""Meeting channel: agents that perceive and respond inside a live meeting.

Two entry points, one session type:

* :meth:`~..channel.FeishuChannel.follow_my_meeting` follows the meeting a user
  is currently in, under that user's own authorization. The bot is **not** a
  participant and is not visible; the only way to respond is a direct message.
* :meth:`~..channel.FeishuChannel.join_meeting` puts the bot in the meeting as
  a real participant, so it can also speak into the meeting chat.

Both return a :class:`~.session.MeetingSession` with the same event stream, so
moving from one to the other changes the entry-point line and nothing else.
"""

from .session import MeetingSession
from .types import (
    ActivityTypeStats,
    DocumentContextEvent,
    LivenessHealth,
    MeetingActor,
    MeetingChatEvent,
    MeetingEndEvent,
    MeetingEventBase,
    MeetingEventHealth,
    MeetingEvents,
    MeetingInvitedEvent,
    MeetingOptions,
    MembershipHealth,
    ParticipantEvent,
    ShareDocInfo,
    ShareEvent,
    TranscriptEvent,
)

__all__ = [
    "ActivityTypeStats",
    "DocumentContextEvent",
    "LivenessHealth",
    "MeetingActor",
    "MeetingChatEvent",
    "MeetingEndEvent",
    "MeetingEventBase",
    "MeetingEventHealth",
    "MeetingEvents",
    "MeetingInvitedEvent",
    "MeetingOptions",
    "MeetingSession",
    "MembershipHealth",
    "ParticipantEvent",
    "ShareDocInfo",
    "ShareEvent",
    "TranscriptEvent",
]
