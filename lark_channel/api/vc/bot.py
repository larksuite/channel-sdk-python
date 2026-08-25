"""Request builders for the five ``vc/v1/bots`` endpoints the meeting
channel needs.

Thin builders in the shape of :mod:`lark_channel.api.drive.comment` and
:mod:`lark_channel.api.wiki.node`: they produce a :class:`BaseRequest` and
nothing else. Execution and response parsing live in the channel layer.

**Every builder declares exactly one token type.** Declaring two is not a
harmless superset:

- ``core.token.auth.verify`` walks tenant → app → user and returns at the
  first match, rewriting ``token_types`` in place. A request declaring
  ``{TENANT, USER}`` therefore resolves to a freshly minted *tenant* token
  and silently discards the user token the caller supplied.
- ``Transport._build_header`` iterates ``token_types`` and overwrites
  ``Authorization`` once per entry, last write winning. ``AccessTokenType``
  is an ``Enum``, so set iteration order follows ``hash(name)`` and varies
  with the process hash seed — the same code would send a different identity
  from one run to the next.

``bots/events`` is a dual-identity endpoint, so it gets two named builders
rather than one with a token-type argument: which identity a meeting read
happens under is the most consequential decision on this path, and it
belongs in the function name rather than in an argument that can default.
"""

from typing import Optional

from lark_channel.core.enum import AccessTokenType, HttpMethod
from lark_channel.core.model import BaseRequest

#: The actor ids in the event stream must share a namespace with the bot's own
#: open_id, or echo detection compares two unrelated random strings and never
#: matches. Pinned here rather than exposed as a parameter so there is no way
#: to get it wrong from the outside.
_USER_ID_TYPE = "open_id"

#: ``bots/join`` accepts no other value; the protocol reserves the field.
_JOIN_TYPE_BOT = 1


def _request(method: HttpMethod, uri: str, token_type: AccessTokenType) -> BaseRequest:
    req = BaseRequest()
    req.http_method = method
    req.uri = uri
    req.token_types = {token_type}
    return req


def build_bot_join_request(
    *,
    meeting_no: str,
    password: Optional[str] = None,
    call_id: Optional[str] = None,
) -> BaseRequest:
    """Bot joins a meeting. Tenant token only.

    ``password`` is a credential: it must not reach logs, ``raw`` payloads or
    error objects. It is passed straight through to the request body here and
    nowhere else.
    """
    req = _request(HttpMethod.POST, "/open-apis/vc/v1/bots/join", AccessTokenType.TENANT)
    body = {
        "join_type": _JOIN_TYPE_BOT,
        "join_identify": {"meeting_no": meeting_no},
    }
    if password is not None:
        body["password"] = password
    if call_id is not None:
        body["call_id"] = call_id
    req.body = body
    return req


def build_bot_leave_request(*, meeting_id: str) -> BaseRequest:
    """Bot leaves a meeting. Tenant token only.

    Takes the long meeting id. The endpoint rejects nine-digit meeting
    numbers (HTTP 400 / ``121105 meeting not exist``), so a caller holding
    only a meeting number has nothing useful to send here.
    """
    req = _request(
        HttpMethod.POST, "/open-apis/vc/v1/bots/leave", AccessTokenType.TENANT
    )
    req.body = {"meeting_id": meeting_id}
    return req


def build_bot_message_request(
    *,
    meeting_id: str,
    msg_type: str,
    content: str,
    uuid: str,
) -> BaseRequest:
    """Send an in-meeting message. Tenant token only.

    ``uuid`` is the caller-supplied idempotency key.
    """
    req = _request(
        HttpMethod.POST, "/open-apis/vc/v1/bots/message", AccessTokenType.TENANT
    )
    req.body = {
        "meeting_id": meeting_id,
        "msg_type": msg_type,
        "content": content,
        "uuid": uuid,
    }
    return req


def _events_request(
    token_type: AccessTokenType,
    meeting_id: str,
    page_token: Optional[str],
    page_size: Optional[int],
) -> BaseRequest:
    req = _request(HttpMethod.GET, "/open-apis/vc/v1/bots/events", token_type)
    req.add_query("meeting_id", meeting_id)
    req.add_query("user_id_type", _USER_ID_TYPE)
    if page_token is not None:
        req.add_query("page_token", page_token)
    if page_size is not None:
        req.add_query("page_size", page_size)
    return req


def build_bot_events_request_as_user(
    *,
    meeting_id: str,
    page_token: Optional[str] = None,
    page_size: Optional[int] = None,
) -> BaseRequest:
    """Read in-meeting events as the user, for ``follow_my_meeting``.

    Requires ``vc:meeting.meetingevent:read`` on a user access token, and
    requires the token to be supplied through ``RequestOption`` — which in
    turn requires the client to be built with ``enable_set_token(True)``.
    """
    return _events_request(
        AccessTokenType.USER, meeting_id, page_token, page_size
    )


def build_bot_events_request_as_app(
    *,
    meeting_id: str,
    page_token: Optional[str] = None,
    page_size: Optional[int] = None,
) -> BaseRequest:
    """Read in-meeting events as the app: liveness and backfill while joined.

    Same endpoint, tenant token, and the scope is exactly the one
    ``bots/join`` already needs — so probing a joined meeting costs no
    additional credential and no additional authorization.

    The endpoint rejects ``page_size`` below 20 (``99992402``) during field
    validation, so a probe cannot ask for a single item.
    """
    return _events_request(
        AccessTokenType.TENANT, meeting_id, page_token, page_size
    )


def build_user_active_meeting_request(*, user_id: Optional[str] = None) -> BaseRequest:
    """Look up the meeting the user is currently in. User token only.

    Omitting ``user_id`` means "whoever the token belongs to".
    """
    req = _request(
        HttpMethod.GET,
        "/open-apis/vc/v1/bots/user_active_meeting",
        AccessTokenType.USER,
    )
    req.add_query("user_id_type", _USER_ID_TYPE)
    if user_id is not None:
        req.add_query("user_id", user_id)
    return req
