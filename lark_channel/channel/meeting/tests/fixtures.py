"""Wire-shape fixtures and fakes for the meeting-channel tests.

Read this before touching any test in this package.

``push_activity()`` and ``poll_events()`` are deliberately **asymmetric**,
because the platform itself is:

===============================  =====================================
push (``vc.bot.meeting_activity_v1``)  poll (``GET /vc/v1/bots/events``)
===============================  =====================================
``*_items`` flattened onto the   ``*_items`` nested under
activity object                  ``events[].payload``
``meeting.id`` is an **int**     ``meeting_id`` is a **str**
actor ``id`` is a **nested       actor ``id`` is a **plain string**
dict** ``{open_id, union_id,
user_id}``
activity items carry **no**      every event carries ``event_id``
``event_id``
===============================  =====================================

Never reshape either builder to match the other, and never rebuild them from
a generated model's type hints. The generated vc models declare
``actor.id: str`` and ``meeting.id: int``; each of those disagrees with what
the platform really sends on at least one of the two transports. A fixture
built from the hints makes ``self_echo`` compare a real open_id against
``""`` forever, and makes session routing drop every pushed event — with the
whole suite still green. ``test_fixtures.py`` pins both shapes so a later
"cleanup" cannot quietly flip them.
"""

import asyncio
import inspect
import json
import time
from contextlib import contextmanager
from typing import Any, Dict, List, Optional
from unittest.mock import patch

import httpx

from lark_channel.channel.auth.device_flow import DeviceFlowInit
from lark_channel.channel.errors import UATAuthError
from lark_channel.channel.types import UAT
from lark_channel.core.http.transport import Transport, _build_header
from lark_channel.core.model import RawResponse
from lark_channel.core.token.manager import TokenManager

# Captured before anything patches ``asyncio.sleep``, so helpers in this
# module keep working inside tests that fast-forward the SDK's own sleeps.
_REAL_SLEEP = asyncio.sleep


# ---------------------------------------------------------------------------
# Well-known identifiers
# ---------------------------------------------------------------------------

MEETING_NO = "123456789"
#: Long meeting id as the join response and the poll transport spell it.
MEETING_ID_STR = "7654321"
#: The very same meeting, as the push transport spells it.
MEETING_ID_INT = 7654321

OTHER_MEETING_NO = "987654321"
OTHER_MEETING_ID_STR = "1234567"
OTHER_MEETING_ID_INT = 1234567

BOT_OPEN_ID = "ou_bot_self"
USER_OPEN_ID = "ou_ticket_owner"

ACTIVITY_EVENT_TYPE = "vc.bot.meeting_activity_v1"
INVITED_EVENT_TYPE = "vc.bot.meeting_invited_v1"
ENDED_EVENT_TYPE = "vc.bot.meeting_ended_v1"

URI_JOIN = "/open-apis/vc/v1/bots/join"
URI_LEAVE = "/open-apis/vc/v1/bots/leave"
URI_MESSAGE = "/open-apis/vc/v1/bots/message"
URI_EVENTS = "/open-apis/vc/v1/bots/events"
URI_ACTIVE_MEETING = "/open-apis/vc/v1/bots/user_active_meeting"

MEETING_EVENT_SCOPE = "vc:meeting.meetingevent:read"

ALL_ACTIVITY_TYPES = (
    "transcript_received",
    "chat_received",
    "participant_joined",
    "participant_left",
    "magic_share_started",
    "magic_share_ended",
    "document_context_changed",
)


# ---------------------------------------------------------------------------
# Actors — the single most expensive shape difference between the transports
# ---------------------------------------------------------------------------


def actor(
    open_id: str = "ou_speaker",
    *,
    shape: str,
    name: str = "Alice",
    union_id: Optional[str] = None,
    user_id: Optional[str] = None,
    user_type: int = 1,
    user_role: int = 2,
) -> Dict[str, Any]:
    """One participant, spelled the way ``shape`` spells it.

    ``shape="push"`` nests three id namespaces under ``id`` because the push
    transport takes no ``user_id_type`` query parameter and therefore hands
    back all of them. ``shape="poll"`` returns a bare string because the poll
    request pins ``user_id_type=open_id``.
    """
    if shape == "push":
        ident: Any = {
            "open_id": open_id,
            "union_id": union_id or open_id.replace("ou_", "on_"),
            "user_id": user_id or open_id.replace("ou_", "u_"),
        }
    elif shape == "poll":
        ident = open_id
    else:
        raise ValueError("shape must be 'push' or 'poll'")
    return {
        "id": ident,
        "name": name,
        "user_type": user_type,
        "user_role": user_role,
    }


def actor_without_id(open_id: str = "ou_fallback", *, name: str = "Bob") -> Dict[str, Any]:
    """An actor that omits ``id`` entirely, forcing the sibling-field fallback."""
    return {"name": name, "open_id": open_id, "user_id": open_id.replace("ou_", "u_")}


# ---------------------------------------------------------------------------
# Activity items (the inner ``*_items`` entries)
# ---------------------------------------------------------------------------


def transcript_item(
    *,
    shape: str,
    speaker: Optional[Dict[str, Any]] = None,
    text: str = "hello there",
    sentence_id: Optional[str] = "sent-1",
    language: Optional[str] = "zh_cn",
    start_time_ms: Optional[Any] = "1730000000123",
    end_time_ms: Optional[Any] = "1730000000456",
) -> Dict[str, Any]:
    item: Dict[str, Any] = {
        "speaker": speaker if speaker is not None else actor(shape=shape),
        "text": text,
    }
    if sentence_id is not None:
        item["sentence_id"] = sentence_id
    if language is not None:
        item["language"] = language
    if start_time_ms is not None:
        item["start_time_ms"] = start_time_ms
    if end_time_ms is not None:
        item["end_time_ms"] = end_time_ms
    return item


def chat_item(
    *,
    shape: str,
    operator: Optional[Dict[str, Any]] = None,
    content: str = "hi from a participant",
    message_id: Optional[str] = "om_chat_1",
    message_type: Optional[int] = 1,
    send_time: Optional[Any] = "1730000000500",
) -> Dict[str, Any]:
    item: Dict[str, Any] = {
        "operator": operator if operator is not None else actor("ou_chatter", shape=shape),
        "content": content,
    }
    if message_id is not None:
        item["message_id"] = message_id
    if message_type is not None:
        item["message_type"] = message_type
    if send_time is not None:
        item["send_time"] = send_time
    return item


def participant_joined_item(
    *,
    shape: str,
    participant: Optional[Dict[str, Any]] = None,
    join_time: Optional[Any] = "1730000000600",
) -> Dict[str, Any]:
    return {
        "participant": participant
        if participant is not None
        else actor("ou_joiner", shape=shape),
        "join_time": join_time,
    }


def participant_left_item(
    *,
    shape: str,
    participant: Optional[Dict[str, Any]] = None,
    leave_time: Optional[Any] = "1730000000700",
    leave_reason: Optional[int] = 1,
) -> Dict[str, Any]:
    return {
        "participant": participant
        if participant is not None
        else actor("ou_leaver", shape=shape),
        "leave_time": leave_time,
        "leave_reason": leave_reason,
    }


def share_doc(url: str = "https://example.test/docx/doc_1", title: str = "Design doc"):
    return {"url": url, "title": title}


def share_started_item(
    *,
    shape: str,
    operator: Optional[Dict[str, Any]] = None,
    share_id: Optional[str] = "share-1",
    doc: Optional[Dict[str, Any]] = None,
    time_: Optional[Any] = "1730000000800",
) -> Dict[str, Any]:
    item: Dict[str, Any] = {
        "operator": operator if operator is not None else actor("ou_sharer", shape=shape),
        "share_id": share_id,
        "time": time_,
    }
    # The field is `share_doc`, not `doc`. Reading `doc` yields None forever.
    item["share_doc"] = doc if doc is not None else share_doc()
    return item


def share_ended_item(**kwargs) -> Dict[str, Any]:
    item = share_started_item(**kwargs)
    item["time"] = "1730000000900"
    return item


def document_context_item(
    *,
    shape: str,
    kind: str,
    operator: Optional[Dict[str, Any]] = None,
    share_id: Optional[str] = "share-1",
    context_type: Optional[str] = None,
    section_title: str = "Chapter 2",
) -> Dict[str, Any]:
    """``kind`` is one of ``comment_focus`` / ``section_location`` /
    ``element_preview`` / ``none``.

    ``kind="none"`` models the platform shipping a fourth kind of context we
    have never seen: all three known sub-objects absent. ``context_type``
    models the platform starting to send an explicit discriminator that the
    generated model does not have a field for.
    """
    item: Dict[str, Any] = {
        "operator": operator if operator is not None else actor("ou_editor", shape=shape),
        "share_id": share_id,
        "share_doc": share_doc(),
        "time": "1730000001000",
    }
    if context_type is not None:
        item["context_type"] = context_type
    if kind == "comment_focus":
        item["comment_focus"] = {"comment_id": "cmt_1", "focused": True}
    elif kind == "section_location":
        item["section_location"] = {
            "title": section_title,
            "level": 2,
            "parent_titles": ["Chapter 1"],
        }
    elif kind == "element_preview":
        item["element_preview"] = {
            "action": "preview",
            "element_type": "image",
            "element_token": "img_token_1",
            "block_id": "blk_1",
        }
    elif kind != "none":
        raise ValueError("unknown document context kind: %s" % kind)
    return item


_ITEM_BUILDERS = {
    "transcript_received": transcript_item,
    "chat_received": chat_item,
    "participant_joined": participant_joined_item,
    "participant_left": participant_left_item,
    "magic_share_started": share_started_item,
    "magic_share_ended": share_ended_item,
}


def default_items(activity_event_type: str, *, shape: str) -> List[Dict[str, Any]]:
    """One canonical inner item for the given activity type."""
    if activity_event_type == "document_context_changed":
        return [document_context_item(shape=shape, kind="comment_focus")]
    builder = _ITEM_BUILDERS.get(activity_event_type)
    if builder is None:
        # Unknown types carry no items we know how to read.
        return []
    return [builder(shape=shape)]


# ---------------------------------------------------------------------------
# Activity objects (the outer ``meeting_activity_items`` / ``events`` entries)
# ---------------------------------------------------------------------------


def push_item(
    activity_event_type: str,
    items: Optional[List[Dict[str, Any]]] = None,
) -> Dict[str, Any]:
    """One ``meeting_activity_items[]`` entry: ``*_items`` sits flat on it."""
    if items is None:
        items = default_items(activity_event_type, shape="push")
    return {
        "activity_event_type": activity_event_type,
        "%s_items" % activity_event_type: list(items),
    }


def poll_item(
    activity_event_type: str,
    items: Optional[List[Dict[str, Any]]] = None,
    *,
    event_id: str = "evt-poll-1",
    meeting_id: str = MEETING_ID_STR,
) -> Dict[str, Any]:
    """One ``data.events[]`` entry: everything interesting hides in ``payload``."""
    if items is None:
        items = default_items(activity_event_type, shape="poll")
    return {
        "event_id": event_id,
        "meeting_id": meeting_id,
        "payload": {
            "activity_event_type": activity_event_type,
            "%s_items" % activity_event_type: list(items),
        },
    }


def push_activity(
    activities: List[Dict[str, Any]],
    *,
    meeting_id: int = MEETING_ID_INT,
    meeting_no: str = MEETING_NO,
    topic: str = "Weekly sync",
    envelope_event_id: str = "env-1",
) -> Dict[str, Any]:
    """A full ``vc.bot.meeting_activity_v1`` p2 envelope.

    ``meeting.id`` stays an ``int`` on purpose. ``envelope_event_id`` is the
    dispatcher-level id in the p2 header — it is *not* a per-activity id, so
    two pushes carrying byte-identical activity items still arrive with
    different envelope ids.
    """
    return {
        "schema": "2.0",
        "header": {
            "event_id": envelope_event_id,
            "event_type": ACTIVITY_EVENT_TYPE,
            "create_time": "1730000000000",
            "token": "",
            "app_id": "cli_x",
            "tenant_key": "tk_1",
        },
        "event": {
            "meeting": {
                "id": meeting_id,
                "topic": topic,
                "meeting_no": meeting_no,
                "start_time": 1730000000,
            },
            "meeting_activity_items": list(activities),
        },
    }


def poll_events(
    activities: List[Dict[str, Any]],
    *,
    page_token: Optional[str] = "page-token-1",
    has_more: bool = False,
) -> Dict[str, Any]:
    """A full ``GET /vc/v1/bots/events`` response body."""
    return {
        "code": 0,
        "msg": "success",
        "data": {
            "events": list(activities),
            "page_token": page_token,
            "has_more": has_more,
        },
    }


def push_meeting_invited(
    *,
    meeting_no: str = MEETING_NO,
    meeting_id: int = MEETING_ID_INT,
    inviter_open_id: str = "ou_inviter",
    topic: str = "Weekly sync",
    call_id: str = "call-1",
) -> Dict[str, Any]:
    return {
        "schema": "2.0",
        "header": {
            "event_id": "env-invited-1",
            "event_type": INVITED_EVENT_TYPE,
            "create_time": "1730000000000",
            "token": "",
            "app_id": "cli_x",
            "tenant_key": "tk_1",
        },
        "event": {
            "meeting": {"id": meeting_id, "meeting_no": meeting_no, "topic": topic},
            "inviter": actor(inviter_open_id, shape="push", name="Inviter"),
            "bot": actor(BOT_OPEN_ID, shape="push", name="Helper"),
            "call_id": call_id,
            "invite_time": 1730000000,
        },
    }


def push_meeting_ended(*, meeting_id: int = MEETING_ID_INT) -> Dict[str, Any]:
    return {
        "schema": "2.0",
        "header": {
            "event_id": "env-ended-1",
            "event_type": ENDED_EVENT_TYPE,
            "create_time": "1730000000000",
            "token": "",
            "app_id": "cli_x",
            "tenant_key": "tk_1",
        },
        "event": {
            "meeting": {"id": meeting_id, "meeting_no": MEETING_NO},
        },
    }


def active_meeting_body(
    meetings: Optional[List[Dict[str, Any]]] = None,
) -> Dict[str, Any]:
    if meetings is None:
        meetings = [
            {
                "meeting_id": MEETING_ID_STR,
                "meeting_no": MEETING_NO,
                "topic": "Weekly sync",
            }
        ]
    return {"code": 0, "msg": "success", "data": {"meetings": list(meetings)}}


def join_body(meeting_id: str = MEETING_ID_STR, *, topic: str = "Weekly sync"):
    return {
        "code": 0,
        "msg": "success",
        "data": {
            "meeting": {
                "id": meeting_id,
                "meeting_no": MEETING_NO,
                "topic": topic,
            }
        },
    }


# ---------------------------------------------------------------------------
# Fake transport — records the bytes that would go on the wire
# ---------------------------------------------------------------------------


class ApiCall:
    """One captured outbound request.

    ``headers`` is what the real ``Transport`` would have put on the wire: it
    comes out of the transport's own header assembler, so an assertion on
    ``headers["Authorization"]`` is an assertion about the request's identity
    rather than about some field on ``RequestOption`` that may still be
    discarded downstream.
    """

    def __init__(self, conf, request, option, headers):
        self.conf = conf
        self.request = request
        self.option = option
        self.headers = headers
        # Snapshotted at send time. The SDK scrubs credentials off the request
        # object once the call returns, and it does so in place — so reading
        # `request.body` afterwards cannot tell "sent, then cleaned up" apart
        # from "never sent at all".
        body_at_send = getattr(request, "body", None)
        self.sent_body = dict(body_at_send) if isinstance(body_at_send, dict) else body_at_send
        self.uri = getattr(request, "uri", None)
        method = getattr(request, "http_method", None)
        self.method = getattr(method, "name", None)
        self.queries = list(getattr(request, "queries", []) or [])
        self.body = getattr(request, "body", None)
        self.token_types = set(getattr(request, "token_types", set()) or set())

    @property
    def authorization(self) -> Optional[str]:
        for key, value in self.headers.items():
            if key.lower() == "authorization":
                return value
        return None

    def query(self, name: str) -> Optional[str]:
        for key, value in self.queries:
            if key == name:
                return value
        return None

    def __repr__(self) -> str:  # pragma: no cover - debugging aid
        return "<ApiCall %s %s>" % (self.method, self.uri)


class FakeVC:
    """Routes VC endpoints to canned responses and records every call."""

    tenant_token = "t-MINTED-TENANT"

    def __init__(self) -> None:
        self.calls: List[ApiCall] = []
        self._routes: Dict[str, Any] = {}
        self.json(URI_JOIN, join_body())
        self.json(URI_LEAVE, {"code": 0, "msg": "success"})
        self.json(URI_MESSAGE, {"code": 0, "msg": "success"})
        self.json(URI_EVENTS, poll_events([]))
        self.json(URI_ACTIVE_MEETING, active_meeting_body())

    # -- routing ---------------------------------------------------------
    def route(self, uri: str, responder) -> None:
        """``responder(call) -> (status, body_dict)``, or raises to simulate
        a transport-level failure.

        A responder may also return an awaitable, which lets a test hold a
        request open — the only way to assert on two calls actually overlapping
        rather than on whichever order the scheduler happened to pick.
        """
        self._routes[uri] = responder

    def json(self, uri: str, body: Dict[str, Any], *, status: int = 200) -> None:
        self.route(uri, lambda call: (status, body))

    def sequence(self, uri: str, responses: List[Any]) -> None:
        """Reply with ``responses`` in order; the last entry repeats.

        Each entry is a body dict, a ``(status, body)`` pair, or an exception
        instance to raise.
        """
        box = {"i": 0}

        def responder(call):
            i = min(box["i"], len(responses) - 1)
            box["i"] += 1
            entry = responses[i]
            if isinstance(entry, BaseException):
                raise entry
            if isinstance(entry, tuple):
                return entry
            return (200, entry)

        self.route(uri, responder)

    # -- inspection ------------------------------------------------------
    def count(self, uri: str) -> int:
        return sum(1 for c in self.calls if c.uri == uri)

    def for_uri(self, uri: str) -> List[ApiCall]:
        return [c for c in self.calls if c.uri == uri]

    def last(self, uri: str) -> ApiCall:
        calls = self.for_uri(uri)
        assert calls, "no call recorded for %s" % uri
        return calls[-1]

    # -- transport -------------------------------------------------------
    async def aexecute(self, conf, req, option=None):
        from lark_channel.core.model import RequestOption

        if option is None:
            option = RequestOption()
        headers = dict(_build_header(req, option, conf))
        call = ApiCall(conf, req, option, headers)
        self.calls.append(call)
        responder = self._routes.get(req.uri)
        if responder is None:
            raise AssertionError("unrouted request: %s %s" % (call.method, req.uri))
        result = responder(call)
        if inspect.isawaitable(result):
            result = await result
        status, body = result
        resp = RawResponse()
        resp.status_code = status
        resp.headers = {"Content-Type": "application/json; charset=utf-8"}
        resp.content = json.dumps(body).encode("utf-8")
        return resp

    @contextmanager
    def patched(self):
        """Patch the transport and tenant-token minting for the whole block.

        ``core.token.auth.verify`` is left *unpatched* on purpose: it is the
        function that decides which credential a request goes out with, so
        stubbing it would erase the very behaviour the identity tests exist
        to pin down. Only the network calls underneath it are faked.
        """
        with patch.object(Transport, "aexecute", new=self.aexecute), patch.object(
            TokenManager, "get_self_tenant_token", new=lambda conf: self.tenant_token
        ), patch.object(
            TokenManager, "get_self_app_token", new=lambda conf: "a-MINTED-APP"
        ):
            yield self


def error_body(code: int, msg: str = "denied", **extra) -> Dict[str, Any]:
    body: Dict[str, Any] = {"code": code, "msg": msg}
    body.update(extra)
    return body


# ---------------------------------------------------------------------------
# Credential fakes
# ---------------------------------------------------------------------------


def make_uat(
    access_token: str = "u-REAL",
    *,
    scopes: Optional[List[str]] = None,
    refresh_token: Optional[str] = "r-1",
    expires_in: Optional[float] = 3600.0,
    open_id: str = USER_OPEN_ID,
) -> UAT:
    return UAT(
        access_token=access_token,
        refresh_token=refresh_token,
        expires_at=None if expires_in is None else time.time() + expires_in,
        refresh_expires_at=time.time() + 30 * 24 * 3600,
        scopes=list(scopes) if scopes is not None else [MEETING_EVENT_SCOPE],
        open_id=open_id,
    )


class FakeTokenStore:
    """TokenStore that counts every access and can hand out a rotation."""

    def __init__(self, initial: Optional[Dict[str, UAT]] = None) -> None:
        self.data: Dict[str, UAT] = dict(initial or {})
        self.get_calls: List[str] = []
        self.set_calls: List[Any] = []
        self.delete_calls: List[str] = []
        self._rotation: Dict[str, List[UAT]] = {}
        self._rotation_pos: Dict[str, int] = {}

    def put(self, user_id: str, token: UAT) -> None:
        self.data[user_id] = token

    def rotate(self, user_id: str, tokens: List[UAT]) -> None:
        """Serve ``tokens`` one per ``get``; the last one repeats."""
        self._rotation[user_id] = list(tokens)
        self._rotation_pos[user_id] = 0

    async def get(self, user_id: str) -> Optional[UAT]:
        self.get_calls.append(user_id)
        rotation = self._rotation.get(user_id)
        if rotation:
            pos = min(self._rotation_pos[user_id], len(rotation) - 1)
            self._rotation_pos[user_id] += 1
            return rotation[pos]
        return self.data.get(user_id)

    async def set(self, user_id: str, token: UAT) -> None:
        self.set_calls.append((user_id, token))
        self.data[user_id] = token

    async def delete(self, user_id: str) -> None:
        self.delete_calls.append(user_id)
        self.data.pop(user_id, None)


class FakeDeviceFlow:
    """DeviceFlowClient stand-in with counters and controllable refresh."""

    def __init__(
        self,
        *,
        refresh_results: Optional[List[Any]] = None,
        poll_result: Optional[UAT] = None,
    ) -> None:
        self.start_calls: List[Any] = []
        self.poll_calls: List[Any] = []
        self.refresh_calls: List[str] = []
        self._refresh_results = list(refresh_results or [])
        self._poll_result = poll_result

    async def start(self, scopes) -> DeviceFlowInit:
        self.start_calls.append(list(scopes or []))
        return DeviceFlowInit(
            verification_uri="https://example.test/device",
            verification_uri_complete="https://example.test/device?code=ABC",
            user_code="ABC-123",
            device_code="dev-1",
            expires_in=600,
            interval=1,
        )

    async def poll(self, device_code, interval=None, timeout_seconds=None) -> UAT:
        self.poll_calls.append(device_code)
        if self._poll_result is None:
            raise UATAuthError("device flow was not authorized")
        return self._poll_result

    async def refresh(self, refresh_token: str) -> UAT:
        self.refresh_calls.append(refresh_token)
        if not self._refresh_results:
            raise UATAuthError("refresh not configured")
        idx = min(len(self.refresh_calls) - 1, len(self._refresh_results) - 1)
        outcome = self._refresh_results[idx]
        if isinstance(outcome, BaseException):
            raise outcome
        return outcome

    async def close(self) -> None:
        return None


def httpx_connect_error(token: str = "u-secret") -> httpx.ConnectError:
    """A transport failure carrying a bare credential on its request.

    ``httpx`` — not ``requests`` — is what ``Transport.aexecute`` uses, and an
    ``httpx`` exception keeps the outgoing request (headers included) reachable
    through attribute walks. ``repr()`` of this object is clean, so a redaction
    check that only inspects ``repr()`` proves nothing.
    """
    request = httpx.Request(
        "GET",
        "https://open.feishu.cn" + URI_EVENTS,
        headers={"Authorization": "Bearer %s" % token},
    )
    return httpx.ConnectError("connection refused", request=request)


# ---------------------------------------------------------------------------
# Channel harness
# ---------------------------------------------------------------------------


def meeting_config(**overrides):
    from lark_channel.channel.config import MeetingChannelConfig

    return MeetingChannelConfig(**overrides)


def make_channel(
    *,
    meeting=None,
    policy=None,
    inbound=None,
    token_store=None,
    device_flow=None,
    app_id: str = "cli_x",
    app_secret: str = "secret",
):
    """Build a channel wired for meeting tests. Does not touch the network."""
    from lark_channel.channel import FeishuChannel
    from lark_channel.channel.config import ChannelConfig

    cfg = ChannelConfig()
    if meeting is not None:
        cfg.meeting = meeting
    if policy is not None:
        cfg.policy = policy
    if inbound is not None:
        cfg.inbound = inbound
    channel = FeishuChannel(
        app_id=app_id,
        app_secret=app_secret,
        config=cfg,
        token_store=token_store,
    )
    if device_flow is not None:
        channel._device_flow = device_flow
    return channel


def mark_connected(channel, *, bot_open_id: Optional[str] = BOT_OPEN_ID):
    """Put the channel into the state ``connect()`` would leave it in.

    Real ``connect()`` opens a WebSocket, so tests fake the post-connect
    state instead: transport started, readiness flipped, dispatcher built
    (which is also what registers the internal ``vc.bot.*`` processors), and
    bot identity resolved so ``self_echo`` has something to compare against.
    """
    from lark_channel.channel.bot_identity import BotIdentity

    channel._ensure_bg_loop()
    channel._started = True
    if bot_open_id is not None:
        identity = BotIdentity(open_id=bot_open_id, name="Helper")
        channel._store_bot_identity(identity)
    channel._dispatcher = channel._build_dispatcher()
    channel._mark_ready()
    return channel


def deliver(channel, payload: Dict[str, Any]):
    """Feed one p2 envelope through the channel's real dispatcher."""
    return channel.dispatcher._do_without_validation(
        json.dumps(payload).encode("utf-8")
    )


async def wait_for(predicate, *, timeout: float = 3.0, what: str = "condition"):
    """Poll ``predicate`` until true. Uses the pre-patch ``asyncio.sleep`` so
    it still works in tests that fast-forward the SDK's own sleeps."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return
        await _REAL_SLEEP(0.005)
    raise AssertionError("timed out after %ss waiting for %s" % (timeout, what))


async def settle(rounds: int = 6):
    """Let queued work on this loop (and the channel's bg loop) drain."""
    for _ in range(rounds):
        await _REAL_SLEEP(0.01)


class SleepRecorder:
    """Records every ``asyncio.sleep`` duration and returns immediately.

    After ``max_sleeps`` the caller is parked on a bounded real sleep so a
    polling loop stops spinning; a session teardown cancels it. The park is
    bounded rather than infinite so a missing cancel shows up as a slow test
    instead of a hung suite.
    """

    park_seconds = 5.0

    def __init__(self, *, max_sleeps: Optional[int] = None) -> None:
        self.durations: List[float] = []
        self._max = max_sleeps

    def between(self, low: float, high: float) -> List[float]:
        return [d for d in self.durations if low <= d <= high]

    async def __call__(self, delay=0, *args, **kwargs):
        self.durations.append(delay)
        if self._max is not None and len(self.durations) > self._max:
            await _REAL_SLEEP(self.park_seconds)
            return None
        await _REAL_SLEEP(0)
        return None


@contextmanager
def fast_sleep(*, max_sleeps: Optional[int] = None):
    """Collapse ``asyncio.sleep`` for the duration of the block."""
    recorder = SleepRecorder(max_sleeps=max_sleeps)
    with patch("asyncio.sleep", new=recorder):
        yield recorder


# ---------------------------------------------------------------------------
# Deep inspection helpers for the credential-hygiene checks
# ---------------------------------------------------------------------------

_FOLLOWED_ATTRS = (
    "__cause__",
    "__context__",
    # A raised error carries the frames it unwound through, and every frame
    # exposes its locals. That is how a credential stays reachable from an
    # error whose repr is spotless — see `_frame_locals` below.
    "__traceback__",
    "tb_frame",
    "tb_next",
    "f_locals",
    "args",
    "request",
    "response",
    "headers",
    "cause",
    "context",
)


def deep_strings(root: Any, *, max_depth: int = 10, exclude=()) -> List[str]:
    """Every string reachable from ``root`` within ``max_depth`` hops.

    Walks exception chains, ``__dict__``, mappings, sequences, and the
    attribute names a transport exception hangs its request on — which is
    how a credential survives "the repr looks fine".

    ``exclude`` prunes specific objects by identity. Use it when an object
    graph loops back to a place a credential is legitimately allowed to live
    (the ticket store), so the walk answers "is it stored *here*" rather than
    "is it stored anywhere in the process".
    """
    found: List[str] = []
    seen = set(id(item) for item in exclude)

    def walk(obj: Any, depth: int) -> None:
        if depth > max_depth or obj is None:
            return
        oid = id(obj)
        if oid in seen:
            return
        seen.add(oid)
        if isinstance(obj, str):
            found.append(obj)
            return
        if isinstance(obj, (bytes, bytearray)):
            found.append(bytes(obj).decode("utf-8", "replace"))
            return
        if isinstance(obj, (int, float, bool)):
            return
        items = getattr(obj, "items", None)
        if callable(items):
            try:
                pairs = list(items())
            except Exception:
                pairs = []
            for key, value in pairs:
                walk(key, depth + 1)
                walk(value, depth + 1)
            return
        if isinstance(obj, (list, tuple, set, frozenset)):
            for item in obj:
                walk(item, depth + 1)
            return
        for attr in _FOLLOWED_ATTRS:
            if hasattr(obj, attr):
                try:
                    value = getattr(obj, attr)
                except Exception:
                    continue
                if attr == "f_locals":
                    # Snapshotted: a live frame's mapping mutates while walked.
                    try:
                        value = dict(value)
                    except Exception:
                        continue
                walk(value, depth + 1)
        state = getattr(obj, "__dict__", None)
        if isinstance(state, dict):
            for key, value in state.items():
                walk(key, depth + 1)
                walk(value, depth + 1)

    walk(root, 0)
    return found


def json_dump_all(*values: Any) -> str:
    return json.dumps(values, default=str, ensure_ascii=False)


def record_text(record) -> str:
    """Everything a log record can put in front of a human or a log file."""
    parts = [str(record.msg), record.getMessage()]
    args = record.args
    if isinstance(args, dict):
        parts.append(json_dump_all(args))
    elif args:
        parts.append(json_dump_all(*args))
    return "\n".join(parts)


CONTROL_CHARS = tuple(
    [chr(c) for c in range(0x00, 0x20)] + [chr(c) for c in range(0x7F, 0xA0)]
)


def follow_ready_channel(*, meeting=None, scopes=None, access_token: str = "u-REAL"):
    """A channel that already holds a usable ticket for ``USER_OPEN_ID``.

    Returns ``(channel, token_store, device_flow)``. The device flow is a fake
    with counters and no configured outcome, so any accidental interactive
    authorization shows up as a recorded call (and then fails loudly) instead
    of silently working.
    """
    store = FakeTokenStore()
    store.put(
        USER_OPEN_ID,
        make_uat(
            access_token,
            scopes=list(scopes) if scopes is not None else [MEETING_EVENT_SCOPE],
        ),
    )
    flow = FakeDeviceFlow()
    channel = make_channel(meeting=meeting, token_store=store, device_flow=flow)
    return channel, store, flow
