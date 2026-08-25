"""Which credential each meeting request actually goes out with.

Every assertion here is on the ``Authorization`` header the transport would
put on the wire, never on ``RequestOption.user_access_token``. That field can
be set correctly and then thrown away further down: a request declaring both
tenant and user token types resolves to a freshly minted *tenant* token and
has its declaration rewritten in place, so the user's authorization becomes
decoration. Only the header tells you whose identity the read happened under.
"""

import inspect
import json

from lark_channel.api.vc.bot import (
    build_bot_events_request_as_app,
    build_bot_events_request_as_user,
    build_bot_join_request,
    build_bot_leave_request,
    build_bot_message_request,
    build_user_active_meeting_request,
)
from lark_channel.core.enum import AccessTokenType
from lark_channel.core.model import BaseRequest, RequestOption
from lark_channel.core.token.auth import verify

from . import fixtures as fx

_ALL_BUILDERS = {
    "build_bot_join_request": (build_bot_join_request, {"meeting_no": fx.MEETING_NO}),
    "build_bot_leave_request": (
        build_bot_leave_request,
        {"meeting_id": fx.MEETING_ID_STR},
    ),
    "build_bot_message_request": (
        build_bot_message_request,
        {
            "meeting_id": fx.MEETING_ID_STR,
            "msg_type": "text",
            "content": '{"text":"hi"}',
            "uuid": "uuid-1",
        },
    ),
    "build_bot_events_request_as_user": (
        build_bot_events_request_as_user,
        {"meeting_id": fx.MEETING_ID_STR},
    ),
    "build_bot_events_request_as_app": (
        build_bot_events_request_as_app,
        {"meeting_id": fx.MEETING_ID_STR},
    ),
    "build_user_active_meeting_request": (build_user_active_meeting_request, {}),
}


async def test_meeting_event_polling_goes_out_as_the_user(vc, uat_channel):
    channel, _store, _flow = uat_channel()
    with fx.fast_sleep(max_sleeps=3):
        session = await channel.follow_my_meeting(user_open_id=fx.USER_OPEN_ID)
        await fx.wait_for(
            lambda: vc.count(fx.URI_EVENTS) >= 1, what="the first event poll"
        )
        session.dispose()

    call = vc.last(fx.URI_EVENTS)
    assert call.authorization == "Bearer u-REAL"


async def test_active_meeting_lookup_goes_out_as_the_user(vc, uat_channel):
    channel, _store, _flow = uat_channel()
    with fx.fast_sleep(max_sleeps=3):
        session = await channel.follow_my_meeting(user_open_id=fx.USER_OPEN_ID)
        await fx.wait_for(
            lambda: vc.count(fx.URI_ACTIVE_MEETING) >= 1,
            what="the active-meeting lookup",
        )
        session.dispose()

    assert vc.last(fx.URI_ACTIVE_MEETING).authorization == "Bearer u-REAL"


async def test_join_leave_and_in_meeting_message_go_out_as_the_app(vc, tat_channel):
    channel = tat_channel()
    session = await channel.join_meeting(fx.MEETING_NO)
    await session.send_message("hello meeting")
    await session.leave()

    expected = "Bearer %s" % fx.FakeVC.tenant_token
    for uri in (fx.URI_JOIN, fx.URI_MESSAGE, fx.URI_LEAVE):
        assert vc.last(uri).authorization == expected, uri


def test_every_vc_builder_declares_exactly_one_token_type():
    """Two declared token types make the transport overwrite the header once
    per type, and the winner depends on set iteration order — which varies
    with the process hash seed. Same code, different identity per run."""
    for name, (builder, kwargs) in _ALL_BUILDERS.items():
        request = builder(**kwargs)
        assert len(request.token_types) == 1, "%s declares %r" % (
            name,
            request.token_types,
        )

    assert build_bot_events_request_as_user(meeting_id=fx.MEETING_ID_STR).token_types == {
        AccessTokenType.USER
    }
    assert build_user_active_meeting_request().token_types == {AccessTokenType.USER}
    assert build_bot_events_request_as_app(meeting_id=fx.MEETING_ID_STR).token_types == {
        AccessTokenType.TENANT
    }
    for name in ("build_bot_join_request", "build_bot_leave_request", "build_bot_message_request"):
        builder, kwargs = _ALL_BUILDERS[name]
        assert builder(**kwargs).token_types == {AccessTokenType.TENANT}, name


def test_channel_client_allows_manually_supplied_tokens(vc, make_ch):
    """Without this switch the transport layer skips manual tokens entirely
    and a user-scoped request either falls back to the app identity or fails
    with "need enable set token"."""
    channel = make_ch()
    assert channel.client.config.enable_set_token is True


def test_tenant_requests_still_mint_from_app_credentials(vc, make_ch):
    """Regression: flipping the manual-token switch on must not change how a
    request that carries no manual token gets its credential."""
    channel = make_ch()
    request = BaseRequest()
    request.token_types = {AccessTokenType.TENANT}
    option = RequestOption()

    verify(channel.client.config, request, option)

    assert option.tenant_access_token == fx.FakeVC.tenant_token
    assert option.user_access_token is None
    assert request.token_types == {AccessTokenType.TENANT}


async def test_each_polling_round_builds_a_fresh_request_and_option(vc, uat_channel):
    """The transport writes ``Authorization`` back onto the request object
    itself, so a reused request carries the previous round's credential into
    the next one."""
    channel, store, _flow = uat_channel()
    store.rotate(
        fx.USER_OPEN_ID,
        [
            fx.make_uat("u-CREATE"),
            fx.make_uat("u-ROUND1"),
            fx.make_uat("u-ROUND2"),
            fx.make_uat("u-ROUND3"),
        ],
    )
    with fx.fast_sleep(max_sleeps=6):
        session = await channel.follow_my_meeting(user_open_id=fx.USER_OPEN_ID)
        await fx.wait_for(
            lambda: vc.count(fx.URI_EVENTS) >= 2, what="two polling rounds"
        )
        session.dispose()

    calls = vc.for_uri(fx.URI_EVENTS)
    first, second = calls[0], calls[1]
    assert first.request is not second.request
    assert first.option is not second.option
    assert first.authorization != second.authorization
    first_token = first.authorization.split(None, 1)[1]
    assert first_token not in json.dumps(dict(second.request.headers))


def test_user_id_type_is_pinned_by_the_builders_not_exposed_as_a_parameter():
    """The actor ids in the event stream have to live in the same namespace as
    the bot's own open_id, or echo detection compares two unrelated random
    strings and never matches."""
    for name in ("build_bot_events_request_as_user", "build_bot_events_request_as_app",
                 "build_user_active_meeting_request"):
        builder, kwargs = _ALL_BUILDERS[name]
        params = inspect.signature(builder).parameters
        assert "user_id_type" not in params, name
        request = builder(**kwargs)
        assert ("user_id_type", "open_id") in request.queries, name
