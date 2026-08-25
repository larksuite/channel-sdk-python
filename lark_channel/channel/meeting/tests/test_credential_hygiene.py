"""Credentials, capability links, and untrusted meeting content.

This is the first path in the channel that handles a user access token, and it
does so on a loop, so every leak here repeats for the life of the meeting. Two
things make "I printed it and it looked clean" worthless as evidence:

* the transport exception's ``repr()`` hides the outgoing headers, while the
  token stays reachable by walking attributes — which is exactly what crash
  reporters do to an exception chain;
* the redaction layer moves a secret out of the message template but does not
  strip control characters, so a forged log line still lands in the file.

So the assertions walk object graphs and compare formatted output, not reprs.
"""

import json
import logging

import pytest

from lark_channel.channel.errors import FeishuChannelError, FeishuChannelErrorCode
from lark_channel.channel.meeting import MeetingOptions
from lark_channel.channel.meeting.errors import safe_console_url, sanitize_for_log
from lark_channel.core.log import redact_for_log

from . import fixtures as fx

_CONSOLE_URL = "https://open.feishu.cn/app/cli_x/auth?q=vc%3Ameeting&sig=SECRETSIG"
_CONTROL_SAMPLES = [
    "hello\nInfo: fake log line",
    "hello\rcarriage",
    "hello\x1b[31mred",
    "hello\x00null",
    "hello\x85nel",
]


def _has_control_chars(text):
    return any(char in text for char in fx.CONTROL_CHARS if char != "\t")


# ---------------------------------------------------------------------------
# Tokens
# ---------------------------------------------------------------------------


async def test_a_transport_failure_never_logs_the_token(vc, uat_channel, caplog):
    channel, _store, _flow = uat_channel(
        access_token="u-secret", active_meeting_check_interval_seconds=300.0
    )
    with fx.fast_sleep(max_sleeps=8):
        session = await channel.follow_my_meeting(user_open_id=fx.USER_OPEN_ID)
        errors = []
        session.on("error", lambda err: errors.append(err))
        with caplog.at_level(logging.DEBUG, logger="Lark"):
            vc.route(
                fx.URI_EVENTS,
                lambda call: (_ for _ in ()).throw(fx.httpx_connect_error("u-secret")),
            )
            await fx.wait_for(lambda: errors, what="the poll failure")
        session.dispose()

    for record in caplog.records:
        assert "u-secret" not in fx.record_text(record), record.getMessage()


async def test_the_error_handed_to_the_business_carries_no_token_anywhere(
    vc, uat_channel
):
    """This object goes straight to whatever the application reports errors
    with, and those tools walk the cause chain."""
    channel, _store, _flow = uat_channel(
        access_token="u-secret", active_meeting_check_interval_seconds=300.0
    )
    errors = []
    with fx.fast_sleep(max_sleeps=8):
        session = await channel.follow_my_meeting(user_open_id=fx.USER_OPEN_ID)
        session.on("error", lambda err: errors.append(err))
        vc.route(
            fx.URI_EVENTS,
            lambda call: (_ for _ in ()).throw(fx.httpx_connect_error("u-secret")),
        )
        await fx.wait_for(lambda: errors, what="the poll failure")
        session.dispose()

    error = errors[0]
    assert isinstance(error, FeishuChannelError)
    assert "u-secret" not in repr(error)
    assert "u-secret" not in json.dumps(error, default=str)
    reachable = fx.deep_strings(error, max_depth=10)
    assert not any("u-secret" in text for text in reachable), reachable


async def test_a_session_does_not_keep_a_copy_of_the_token(vc, uat_channel):
    """The ticket store is the one place a token is allowed to live; a token
    parked on a session outlives the request it was minted for."""
    channel, store, _flow = uat_channel(
        access_token="u-secret", active_meeting_check_interval_seconds=300.0
    )
    with fx.fast_sleep(max_sleeps=6):
        session = await channel.follow_my_meeting(user_open_id=fx.USER_OPEN_ID)
        await fx.wait_for(
            lambda: vc.count(fx.URI_EVENTS) >= 1, what="at least one poll"
        )
        session.dispose()

    assert "u-secret" not in repr(session)
    reachable = fx.deep_strings(session, max_depth=10, exclude=(channel, store))
    assert not any("u-secret" in text for text in reachable), reachable


async def test_meeting_passwords_stay_out_of_logs_events_and_the_session(
    vc, tat_channel, caplog
):
    """Both directions count as a credential: the one handed to join, and the
    one some meeting queries hand back in their response body."""
    response = fx.join_body()
    response["data"]["meeting"]["password"] = "resp-s3cr3t"
    vc.json(fx.URI_JOIN, response)

    channel = tat_channel()
    with caplog.at_level(logging.DEBUG, logger="Lark"):
        session = await channel.join_meeting(
            fx.MEETING_NO,
            password="given-s3cr3t",
            options=MeetingOptions(include_raw=True),
        )
        got = []
        session.on("transcript", lambda event: got.append(event))
        fx.deliver(channel, fx.push_activity([fx.push_item("transcript_received")]))
        await fx.wait_for(lambda: got, what="the transcript")

    for secret in ("given-s3cr3t", "resp-s3cr3t"):
        for record in caplog.records:
            assert secret not in fx.record_text(record), record.getMessage()
        assert secret not in repr(session)
        assert secret not in json.dumps(got[0].raw, default=str)
        reachable = fx.deep_strings(session, max_depth=10, exclude=(channel,))
        assert not any(secret in text for text in reachable), reachable


async def test_a_poll_failure_with_no_error_handler_stays_contained(
    vc, uat_channel, caplog
):
    channel, _store, _flow = uat_channel(active_meeting_check_interval_seconds=300.0)
    with fx.fast_sleep(max_sleeps=8):
        session = await channel.follow_my_meeting(user_open_id=fx.USER_OPEN_ID)
        with caplog.at_level(logging.DEBUG, logger="Lark"):
            vc.route(
                fx.URI_EVENTS,
                lambda call: (_ for _ in ()).throw(fx.httpx_connect_error()),
            )
            await fx.wait_for(
                lambda: vc.count(fx.URI_EVENTS) >= 2, what="a couple of poll attempts"
            )
        session.dispose()

    for needle in ("Task exception was never retrieved", "background task raised"):
        assert needle not in caplog.text


async def test_the_fallback_error_log_stays_minimal(vc, tat_channel, caplog):
    """With no error handler registered the failure is logged instead, and that
    log must not become the leak the error object was cleaned up to avoid."""
    channel = tat_channel()
    session = await channel.join_meeting(fx.MEETING_NO)
    vc.route(
        fx.URI_LEAVE,
        lambda call: (
            403,
            {
                "code": 99991672,
                "msg": "no permission",
                "console_url": _CONSOLE_URL,
            },
        ),
    )

    with caplog.at_level(logging.DEBUG, logger="Lark"):
        await session.leave()
        await fx.settle()

    logged = "\n".join(fx.record_text(record) for record in caplog.records)
    assert "SECRETSIG" not in logged
    assert _CONSOLE_URL not in logged
    assert fx.MEETING_ID_STR in logged
    assert "99991672" in logged


# ---------------------------------------------------------------------------
# The console link is itself a credential
# ---------------------------------------------------------------------------


def test_the_redaction_layer_masks_console_links():
    assert redact_for_log({"console_url": _CONSOLE_URL}) == {"console_url": "***"}
    assert redact_for_log({"consoleUrl": _CONSOLE_URL}) == {"consoleUrl": "***"}


def test_a_legitimate_console_link_survives_byte_for_byte():
    """It is a signed one-click link whose contents are opaque; re-encoding or
    reassembling any part of it makes it stop working."""
    assert safe_console_url(_CONSOLE_URL) == _CONSOLE_URL


@pytest.mark.parametrize(
    "candidate",
    [
        "javascript:alert(1)",
        "data:text/html;base64,PHNjcmlwdD4=",
        "\tjava\nscript:alert(1)",
        "http://open.feishu.cn/app",
    ],
)
def test_a_non_https_console_link_is_dropped(candidate):
    """The domain this arrives from is configurable, so the field is not a
    trusted source, and downstream renders it as a link."""
    assert safe_console_url(candidate) is None


def test_a_console_link_with_embedded_userinfo_is_dropped():
    """The whole point of the field is that an administrator clicks it, and a
    link whose prefix reads like the official domain is a ready-made lure."""
    assert safe_console_url("https://open.feishu.cn@elsewhere.example/x") is None


def test_an_unparsable_console_link_is_dropped_without_raising():
    """This validation runs while an error object is being constructed, so
    raising here replaces the real API failure with a parsing failure."""
    assert safe_console_url("https://[::1") is None


async def test_a_valid_console_link_reaches_the_error_context_unchanged(
    vc, tat_channel
):
    channel = tat_channel()
    session = await channel.join_meeting(fx.MEETING_NO)
    vc.route(
        fx.URI_MESSAGE,
        lambda call: (
            403,
            {
                "code": 99991672,
                "msg": "no permission",
                "console_url": _CONSOLE_URL,
                "error": {"console_url": _CONSOLE_URL},
            },
        ),
    )

    with pytest.raises(FeishuChannelError) as excinfo:
        await session.send_message("hello")

    assert excinfo.value.context["console_url"] == _CONSOLE_URL


async def test_an_unparsable_console_link_does_not_hide_the_api_failure(
    vc, tat_channel
):
    channel = tat_channel()
    session = await channel.join_meeting(fx.MEETING_NO)
    vc.route(
        fx.URI_MESSAGE,
        lambda call: (
            403,
            {"code": 99991672, "msg": "no permission", "console_url": "https://[::1"},
        ),
    )

    with pytest.raises(FeishuChannelError) as excinfo:
        await session.send_message("hello")

    assert "console_url" not in (excinfo.value.context or {})
    assert "99991672" in str(excinfo.value) or excinfo.value.context.get("feishu_code") == 99991672


# ---------------------------------------------------------------------------
# Meeting content is untrusted input
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("sample", _CONTROL_SAMPLES)
def test_sanitizing_removes_every_control_character(sample):
    cleaned = sanitize_for_log(sample)
    assert not _has_control_chars(cleaned)
    assert "hello" in cleaned


async def test_a_forged_meeting_title_cannot_forge_a_log_line(
    vc, uat_channel, caplog
):
    """Passing untrusted text as a formatting argument only moves it out of the
    message template; the logging layer still formats it into the same output
    line, so the assertion has to be on the formatted result."""
    hostile = "Standup\nInfo: [Lark] all clear\x1b[31m"
    channel, _store, _flow = uat_channel(active_meeting_check_interval_seconds=300.0)
    vc.json(
        fx.URI_ACTIVE_MEETING,
        fx.active_meeting_body(
            [
                {
                    "meeting_id": fx.MEETING_ID_STR,
                    "meeting_no": fx.MEETING_NO,
                    "topic": "First",
                },
                {
                    "meeting_id": fx.OTHER_MEETING_ID_STR,
                    "meeting_no": fx.OTHER_MEETING_NO,
                    "topic": hostile,
                },
            ]
        ),
    )

    with caplog.at_level(logging.DEBUG, logger="Lark"):
        session = await channel.follow_my_meeting(user_open_id=fx.USER_OPEN_ID)
        await fx.settle()
        session.dispose()

    for record in caplog.records:
        assert hostile not in str(record.msg)
        assert not _has_control_chars(record.getMessage())


async def test_a_join_failure_leaves_no_credential_in_its_traceback(vc, tat_channel):
    """A raised error carries the frames it unwound through, and each frame
    exposes its locals. So an error whose ``repr`` is spotless can still hand a
    crash reporter the password that caused the failure — and a wrong password
    is the most ordinary way for this call to fail."""
    channel = tat_channel()
    vc.json(fx.URI_JOIN, fx.error_body(120002, "wrong password"), status=400)

    with pytest.raises(FeishuChannelError) as excinfo:
        await channel.join_meeting(fx.MEETING_NO, password="given-s3cr3t")

    error = excinfo.value
    assert "given-s3cr3t" not in repr(error)
    # The outermost frame is this test's own, so the walk would otherwise reach
    # the recording transport's send-time snapshot. Excluding those handles keeps
    # the question "is it reachable through anything the SDK owns" — note that
    # `exclude` only skips those objects themselves, so any subtree still
    # reachable from SDK state is still walked.
    reachable = fx.deep_strings(error, max_depth=12, exclude=(channel, vc))
    assert not any("given-s3cr3t" in text for text in reachable), [
        text for text in reachable if "given-s3cr3t" in text
    ]


async def test_a_follow_failure_leaves_no_ticket_in_its_traceback(vc, uat_channel):
    """"No active meeting" is the everyday failure on the follow path, and the
    frames it unwinds through are the ones holding the user's ticket."""
    channel, store, _flow = uat_channel(access_token="u-secret")
    vc.json(fx.URI_ACTIVE_MEETING, fx.active_meeting_body([]))

    with pytest.raises(FeishuChannelError) as excinfo:
        await channel.follow_my_meeting(user_open_id=fx.USER_OPEN_ID)

    # The outermost frame in the traceback is this test's own, so the walk would
    # otherwise reach the recording transport and the ticket store — the one
    # place a token is supposed to live. Excluding them keeps the question
    # "is it reachable through anything the SDK owns".
    reachable = fx.deep_strings(
        excinfo.value, max_depth=12, exclude=(channel, store, vc)
    )
    assert not any("u-secret" in text for text in reachable), [
        text for text in reachable if "u-secret" in text
    ]


async def test_the_password_is_actually_sent_and_only_then_cleaned_up(
    vc, tat_channel
):
    """Two halves of one property, and the second is worthless without the
    first: cleanup happens in place on the request object, so a scrub that ran
    too early would look identical to a correct one — while password-protected
    meetings silently failed to join."""
    channel = tat_channel()

    session = await channel.join_meeting(fx.MEETING_NO, password="given-s3cr3t")
    assert session is not None

    call = vc.last(fx.URI_JOIN)
    # It really went out.
    assert call.sent_body["password"] == "given-s3cr3t"
    # And it is gone from the request object afterwards, which is what keeps it
    # out of any error raised on this path.
    assert (call.request.body or {}).get("password") is None
    assert getattr(call.option, "tenant_access_token", None) is None


def test_the_reachability_walk_can_actually_find_things():
    """A positive control. The two checks above prove "not found", and a walk
    that silently stopped finding anything — a depth limit, a pruned attribute —
    would keep proving it forever."""
    sentinel = "sentinel-value-42"
    error = FeishuChannelError(
        FeishuChannelErrorCode.UNKNOWN, "carrier", context={"probe": sentinel}
    )

    assert any(sentinel in text for text in fx.deep_strings(error, max_depth=10))


async def test_a_departure_failure_after_a_dispose_still_reaches_the_handler(
    vc, tat_channel
):
    """The documented shutdown order is dispose-then-leave, and disposal
    cancels the delivery queue — so this report has nowhere to be queued. An
    implementation that hands the fallback work back to its caller instead of
    doing it swallows the failure and leaves only a never-awaited warning."""
    channel = tat_channel()
    session = await channel.join_meeting(fx.MEETING_NO)
    errors = []
    session.on("error", lambda err: errors.append(err))
    vc.route(fx.URI_LEAVE, lambda call: (500, fx.error_body(500, "boom")))

    session.dispose()
    await fx.settle()
    await session.leave()

    await fx.wait_for(lambda: errors, what="the departure failure")
    assert isinstance(errors[0], FeishuChannelError)


async def test_an_async_error_handler_actually_runs(vc, tat_channel):
    """Every error handler in this suite used to be a synchronous lambda, which
    is why a version that returned the first coroutine instead of awaiting it
    looked correct."""
    channel = tat_channel()
    session = await channel.join_meeting(fx.MEETING_NO)
    seen = []

    async def on_error(err):
        seen.append(err)

    session.on("error", on_error)
    vc.route(fx.URI_LEAVE, lambda call: (500, fx.error_body(500, "boom")))

    await session.leave()

    await fx.wait_for(lambda: seen, what="the async error handler to run")


async def test_an_error_handler_that_raises_does_not_wedge_the_queue(
    vc, tat_channel
):
    """Reporting a handler failure by handing it to the error handlers means the
    error handlers can produce more of the same. Routing that back through the
    queue feeds it forever, and every real event queues behind it."""
    channel = tat_channel()
    session = await channel.join_meeting(fx.MEETING_NO)

    def explode(_event):
        raise RuntimeError("handler bug")

    session.on("transcript", explode)
    session.on("error", explode)
    fx.deliver(channel, fx.push_activity([fx.push_item("transcript_received")]))
    await fx.settle()

    # The queue still works afterwards.
    chats = []
    session.on("chat", lambda event: chats.append(event))
    fx.deliver(
        channel,
        fx.push_activity([fx.push_item("chat_received")], envelope_event_id="env-2"),
    )
    await fx.wait_for(lambda: chats, what="a later event to still be delivered")


def _join_reply_missing_id(*, echoed_password: str):
    """A join reply that echoes the password back but carries no meeting id."""
    body = fx.join_body()
    body["data"]["meeting"].pop("id")
    body["data"]["meeting"]["password"] = echoed_password
    return body


async def test_a_join_that_answers_without_an_id_leaks_no_echoed_password(
    vc, tat_channel
):
    """The password comes back in the response, so it can leak outbound too.

    `docs/security.md` promises meeting passwords stay out of error objects in
    both directions. The inbound direction has its own failure shape: a reply
    that carries a meeting object — password echoed — but no id, which the join
    path rejects. That raise unwinds through the frame holding the decoded
    response, so an implementation that keeps a reference to it hands the
    password to anything reading frame locals.
    """
    channel = tat_channel()
    # Built in a helper, not a local: the outermost frame in the traceback is
    # this test's own, so a reply held here would answer the question with the
    # test's own bookkeeping rather than with anything the SDK kept.
    vc.json(fx.URI_JOIN, _join_reply_missing_id(echoed_password="echoed-s3cr3t"))

    with pytest.raises(FeishuChannelError) as excinfo:
        await channel.join_meeting(fx.MEETING_NO, password="given-s3cr3t")

    reachable = fx.deep_strings(excinfo.value, max_depth=12, exclude=(channel, vc))
    assert not any("echoed-s3cr3t" in text for text in reachable), [
        text for text in reachable if "echoed-s3cr3t" in text
    ]
