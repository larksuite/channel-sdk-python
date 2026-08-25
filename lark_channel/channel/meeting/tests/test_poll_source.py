"""The follow-mode polling loop and the non-interactive ticket lookup it uses.

The loop runs every few seconds for the whole meeting, so anything it does
per round it does hundreds of times: an interactive authorization in there is
hundreds of authorization cards, and a refresh that is not written back
invalidates the ticket for everybody else holding it.
"""

import asyncio
import logging

import pytest

from lark_channel.channel.auth import uat_runner
from lark_channel.channel.config import UATConfig
from lark_channel.channel.errors import (
    FeishuChannelError,
    FeishuChannelErrorCode,
    UATAuthError,
)

from . import fixtures as fx

_CREDENTIAL_FAILURES = [
    (401, fx.error_body(0, "unauthorized")),
    (403, fx.error_body(0, "forbidden")),
    (200, fx.error_body(99991400, "invalid app_ticket")),
    (200, fx.error_body(99991401, "invalid access token")),
    (200, fx.error_body(99991668, "token expired")),
]


def _poll_sleeps(clock):
    """The empty-poll ladder, separated from the slower end-detection loop."""
    return [d for d in clock.durations if 0 < d <= 10]


# ---------------------------------------------------------------------------
# Backoff
# ---------------------------------------------------------------------------


async def test_empty_polls_back_off_from_three_seconds_to_a_ten_second_ceiling(
    vc, uat_channel
):
    channel, _store, _flow = uat_channel(active_meeting_check_interval_seconds=300.0)
    with fx.fast_sleep(max_sleeps=8) as clock:
        session = await channel.follow_my_meeting(user_open_id=fx.USER_OPEN_ID)
        await fx.wait_for(
            lambda: len(_poll_sleeps(clock)) >= 3, what="three backoff steps"
        )
        session.dispose()

    assert _poll_sleeps(clock)[:3] == [3.0, 6.0, 10.0]


async def test_receiving_events_returns_the_backoff_to_its_floor(vc, uat_channel):
    channel, _store, _flow = uat_channel(active_meeting_check_interval_seconds=300.0)
    vc.sequence(
        fx.URI_EVENTS,
        [
            fx.poll_events([]),
            fx.poll_events([]),
            fx.poll_events([fx.poll_item("transcript_received")]),
            fx.poll_events([]),
        ],
    )
    with fx.fast_sleep(max_sleeps=8) as clock:
        session = await channel.follow_my_meeting(user_open_id=fx.USER_OPEN_ID)
        await fx.wait_for(
            lambda: vc.count(fx.URI_EVENTS) >= 4, what="four polling rounds"
        )
        session.dispose()

    sleeps = _poll_sleeps(clock)
    assert sleeps[:2] == [3.0, 6.0]
    assert 3.0 in sleeps[2:]


# ---------------------------------------------------------------------------
# Ticket handling inside the loop
# ---------------------------------------------------------------------------


async def test_every_round_reads_the_ticket_store_without_going_interactive(
    vc, uat_channel, monkeypatch
):
    channel, store, flow = uat_channel(active_meeting_check_interval_seconds=300.0)
    with fx.fast_sleep(max_sleeps=10):
        session = await channel.follow_my_meeting(user_open_id=fx.USER_OPEN_ID)
        reads_at_start = len(store.get_calls)

        interactive = []

        async def _forbidden(**kwargs):
            interactive.append(kwargs)
            raise AssertionError("the polling loop must not go interactive")

        monkeypatch.setattr(
            "lark_channel.channel.channel.require_user_auth", _forbidden
        )
        await fx.wait_for(
            lambda: vc.count(fx.URI_EVENTS) >= 4, what="four polling rounds"
        )
        session.dispose()

    assert len(store.get_calls) > reads_at_start
    assert interactive == []
    assert flow.start_calls == []


async def test_a_ticket_missing_the_meeting_scope_never_triggers_an_auth_card(
    vc, uat_channel
):
    """Ticket scopes are whatever the platform granted the app, so the request
    scope failing to appear verbatim is an ordinary state, not an error. The
    interactive helper answers that state by starting a device flow."""
    channel, store, flow = uat_channel(active_meeting_check_interval_seconds=300.0)
    # A generous sleep budget: collapsing sleep lets the loop spin through its
    # allowance in microseconds, so a tight budget can be exhausted — and the
    # loop parked — before this test has even swapped the ticket. The budget is
    # only here to stop a polling loop spinning forever; it is not the contract.
    with fx.fast_sleep(max_sleeps=200):
        session = await channel.follow_my_meeting(user_open_id=fx.USER_OPEN_ID)
        store.put(
            fx.USER_OPEN_ID, fx.make_uat("u-NOSCOPE", scopes=["im:message:send_as_bot"])
        )
        # Guarded on the count first: `vc.last` raises when nothing has been
        # recorded yet, and a predicate that raises escapes the wait instead of
        # being retried.
        await fx.wait_for(
            lambda: vc.count(fx.URI_EVENTS) >= 1
            and vc.last(fx.URI_EVENTS).authorization == "Bearer u-NOSCOPE",
            what="the loop to use the scope-less ticket",
        )
        rounds = vc.count(fx.URI_EVENTS)
        await fx.wait_for(
            lambda: vc.count(fx.URI_EVENTS) >= rounds + 3, what="three more rounds"
        )
        session.dispose()

    assert flow.start_calls == []


async def test_non_interactive_lookup_refuses_rather_than_prompting(vc):
    store = fx.FakeTokenStore()
    flow = fx.FakeDeviceFlow()

    with pytest.raises(UATAuthError):
        await uat_runner.resolve_user_auth_non_interactive(
            device_flow=flow,
            token_store=store,
            uat_config=UATConfig(),
            user_open_id=fx.USER_OPEN_ID,
        )

    assert flow.start_calls == []
    # Deleting is the interactive path's prerogative: it can immediately ask
    # for a new authorization, whereas a polling loop can only make the next
    # unrelated call fail with a surprise card.
    assert store.delete_calls == []


async def test_a_rotated_refresh_token_is_written_back(vc):
    """The old refresh token is dead the moment it is used. Not persisting the
    new one makes every holder of this ticket invalidate it for the others."""
    store = fx.FakeTokenStore()
    store.put(
        fx.USER_OPEN_ID,
        fx.make_uat("u-OLD", refresh_token="r-old", expires_in=10.0),
    )
    rotated = fx.make_uat("u-NEW", refresh_token="r-new")
    flow = fx.FakeDeviceFlow(refresh_results=[rotated])

    resolved = await uat_runner.resolve_user_auth_non_interactive(
        device_flow=flow,
        token_store=store,
        uat_config=UATConfig(),
        user_open_id=fx.USER_OPEN_ID,
    )

    assert flow.refresh_calls == ["r-old"]
    assert resolved.access_token == "u-NEW"
    stored = store.data[fx.USER_OPEN_ID]
    assert stored.refresh_token == "r-new"
    assert stored.access_token == "u-NEW"


async def test_a_failed_refresh_does_not_delete_the_ticket(vc):
    store = fx.FakeTokenStore()
    store.put(
        fx.USER_OPEN_ID,
        fx.make_uat("u-OLD", refresh_token="r-old", expires_in=10.0),
    )
    flow = fx.FakeDeviceFlow(refresh_results=[UATAuthError("refresh rejected")])

    with pytest.raises(UATAuthError):
        await uat_runner.resolve_user_auth_non_interactive(
            device_flow=flow,
            token_store=store,
            uat_config=UATConfig(),
            user_open_id=fx.USER_OPEN_ID,
        )

    assert store.delete_calls == []
    assert flow.start_calls == []


async def test_concurrent_refresh_and_interactive_resolution_do_not_overlap(vc):
    """Whichever of the two arrives second with a stale refresh token gets a
    rejection — and the interactive path answers a rejection by deleting the
    ticket, so a valid ticket disappears and its owner gets an unexpected card."""
    store = fx.FakeTokenStore()
    store.put(
        fx.USER_OPEN_ID,
        fx.make_uat("u-OLD", refresh_token="r-old", expires_in=10.0),
    )
    inflight = []
    refreshed = fx.make_uat("u-NEW", refresh_token="r-new")

    class _SerializationProbe(fx.FakeDeviceFlow):
        async def refresh(self, refresh_token):
            inflight.append(refresh_token)
            assert len(inflight) == 1, "two refreshes were in flight at once"
            await asyncio.sleep(0.02)
            inflight.pop()
            self.refresh_calls.append(refresh_token)
            return refreshed

    flow = _SerializationProbe()

    await asyncio.gather(
        uat_runner.resolve_user_auth_non_interactive(
            device_flow=flow,
            token_store=store,
            uat_config=UATConfig(),
            user_open_id=fx.USER_OPEN_ID,
        ),
        uat_runner.require_user_auth(
            device_flow=flow,
            token_store=store,
            uat_config=UATConfig(),
            user_open_id=fx.USER_OPEN_ID,
            scopes=[fx.MEETING_EVENT_SCOPE],
            context=None,
        ),
    )

    assert store.data[fx.USER_OPEN_ID].access_token == "u-NEW"
    assert store.delete_calls == []


async def test_both_resolvers_serialize_on_the_same_per_user_lock(vc, monkeypatch):
    handed_out = []
    real_lock = uat_runner._get_user_lock

    def _spy(user_open_id):
        lock = real_lock(user_open_id)
        handed_out.append(lock)
        return lock

    monkeypatch.setattr(uat_runner, "_get_user_lock", _spy)
    store = fx.FakeTokenStore()
    store.put(fx.USER_OPEN_ID, fx.make_uat("u-REAL"))
    flow = fx.FakeDeviceFlow()

    await uat_runner.resolve_user_auth_non_interactive(
        device_flow=flow,
        token_store=store,
        uat_config=UATConfig(),
        user_open_id=fx.USER_OPEN_ID,
    )
    await uat_runner.require_user_auth(
        device_flow=flow,
        token_store=store,
        uat_config=UATConfig(),
        user_open_id=fx.USER_OPEN_ID,
        scopes=[fx.MEETING_EVENT_SCOPE],
        context=None,
    )

    assert len(handed_out) >= 2
    assert handed_out[0] is handed_out[1]


async def test_a_cross_loop_lock_error_is_treated_as_a_credential_failure(
    vc, uat_channel, monkeypatch
):
    """These locks are memoized per user and bound to whichever loop created
    them; a second loop touching one raises instead of merely not excluding.
    Left uncaught it surfaces as an unhandled task exception."""

    class _WrongLoopLock:
        async def __aenter__(self):
            raise RuntimeError("got Future attached to a different loop")

        async def __aexit__(self, *exc):
            return False

    channel, _store, _flow = uat_channel(active_meeting_check_interval_seconds=300.0)
    errors = []
    ended = []
    with fx.fast_sleep(max_sleeps=10):
        session = await channel.follow_my_meeting(user_open_id=fx.USER_OPEN_ID)
        session.on("error", lambda err: errors.append(err))
        session.on("end", lambda event: ended.append(event))
        monkeypatch.setattr(
            uat_runner, "_get_user_lock", lambda user_open_id: _WrongLoopLock()
        )
        await fx.wait_for(lambda: ended, what="the session to terminate")

    assert [event.reason for event in ended] == ["error"]
    assert isinstance(errors[0], FeishuChannelError)


# ---------------------------------------------------------------------------
# Picking the meeting to follow
# ---------------------------------------------------------------------------


async def test_no_active_meeting_is_reported_as_such(vc, uat_channel):
    channel, _store, _flow = uat_channel()
    vc.json(fx.URI_ACTIVE_MEETING, fx.active_meeting_body([]))

    with pytest.raises(FeishuChannelError) as excinfo:
        await channel.follow_my_meeting(user_open_id=fx.USER_OPEN_ID)
    assert excinfo.value.code is FeishuChannelErrorCode.MEETING_NOT_FOUND


async def test_several_active_meetings_pick_the_first_and_say_so(
    vc, uat_channel, caplog
):
    channel, _store, _flow = uat_channel(active_meeting_check_interval_seconds=300.0)
    vc.json(
        fx.URI_ACTIVE_MEETING,
        fx.active_meeting_body(
            [
                {"meeting_id": fx.MEETING_ID_STR, "meeting_no": fx.MEETING_NO, "topic": "First"},
                {
                    "meeting_id": fx.OTHER_MEETING_ID_STR,
                    "meeting_no": fx.OTHER_MEETING_NO,
                    "topic": "Second",
                },
            ]
        ),
    )

    with caplog.at_level(logging.WARNING, logger="Lark"):
        session = await channel.follow_my_meeting(user_open_id=fx.USER_OPEN_ID)

    assert session.meeting_id == fx.MEETING_ID_STR
    warnings = [r for r in caplog.records if r.levelno >= logging.WARNING]
    assert warnings, caplog.text
    # A meeting title is whatever its creator typed, so it may only travel as
    # a formatting argument — never pre-interpolated into the message.
    assert all("Second" not in str(record.msg) for record in warnings)


# ---------------------------------------------------------------------------
# Failure handling
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("status,body", _CREDENTIAL_FAILURES)
async def test_credential_failures_stop_the_event_source(vc, uat_channel, status, body):
    channel, store, _flow = uat_channel(active_meeting_check_interval_seconds=300.0)
    with fx.fast_sleep(max_sleeps=12):
        session = await channel.follow_my_meeting(user_open_id=fx.USER_OPEN_ID)
        ended = []
        session.on("end", lambda event: ended.append(event))
        vc.json(fx.URI_EVENTS, body, status=status)
        await fx.wait_for(lambda: ended, what="the session to terminate")
        polls = vc.count(fx.URI_EVENTS)
        reads = len(store.get_calls)
        await fx.settle()

    assert vc.count(fx.URI_EVENTS) == polls
    assert len(store.get_calls) == reads


async def test_retryable_failures_use_their_own_ladder_and_give_up_eventually(
    vc, uat_channel
):
    channel, _store, _flow = uat_channel(
        poll_max_consecutive_failures=4, active_meeting_check_interval_seconds=300.0
    )
    with fx.fast_sleep(max_sleeps=20) as clock:
        session = await channel.follow_my_meeting(user_open_id=fx.USER_OPEN_ID)
        ended = []
        session.on("end", lambda event: ended.append(event))
        vc.json(fx.URI_EVENTS, fx.error_body(500, "upstream boom"), status=500)
        await fx.wait_for(
            lambda: ended, what="the session to give up after repeated failures"
        )

    failure_sleeps = [d for d in clock.durations if d > 10.0 and d < 300.0]
    assert failure_sleeps, clock.durations
    assert max(failure_sleeps) <= 60.0
    assert [event.reason for event in ended] == ["error"]


async def test_termination_emits_error_then_end_then_unregisters_the_session(
    vc, uat_channel
):
    """Stopping the loop is not enough: neither the idle timeout nor the
    liveness probe applies to a follow session, so a half-terminated one is
    never collected by anything."""
    channel, _store, _flow = uat_channel(
        max_concurrent_sessions=1, active_meeting_check_interval_seconds=300.0
    )
    fx.mark_connected(channel)
    order = []
    with fx.fast_sleep(max_sleeps=12):
        session = await channel.follow_my_meeting(user_open_id=fx.USER_OPEN_ID)
        session.on("error", lambda err: order.append("error"))
        session.on("end", lambda event: order.append("end:%s" % event.reason))
        vc.json(fx.URI_EVENTS, fx.error_body(0, "forbidden"), status=403)
        await fx.wait_for(
            lambda: any(item.startswith("end") for item in order),
            what="the session to terminate",
        )

    assert order[:2] == ["error", "end:error"]
    # The seat came back, so the terminated session is really gone.
    joined = await channel.join_meeting(fx.OTHER_MEETING_NO)
    assert joined is not None


async def test_a_credential_failure_stops_the_end_detection_loop_too(vc, uat_channel):
    channel, _store, _flow = uat_channel(active_meeting_check_interval_seconds=0.02)
    with fx.fast_sleep(max_sleeps=12):
        session = await channel.follow_my_meeting(user_open_id=fx.USER_OPEN_ID)
        ended = []
        session.on("end", lambda event: ended.append(event))
        vc.json(fx.URI_ACTIVE_MEETING, fx.error_body(99991401, "invalid token"), status=200)
        await fx.wait_for(lambda: ended, what="the session to terminate")
        counts = (vc.count(fx.URI_EVENTS), vc.count(fx.URI_ACTIVE_MEETING))
        await fx.settle()

    assert (vc.count(fx.URI_EVENTS), vc.count(fx.URI_ACTIVE_MEETING)) == counts


async def test_retryable_end_detection_failures_never_end_the_session(vc, uat_channel):
    """These failures are correlated across every follow session — same
    endpoint, same cadence, often the same user — so ending the session on them
    would end all of them together, while their transcripts were flowing fine."""
    # Real intervals rather than collapsed sleeps. Two loops run for the whole
    # test here, and with sleep collapsed they spin as fast as the loop allows
    # and burn any sleep budget before the assertions land. Small real intervals
    # keep them at a sane rate and remove the timing dependence entirely; the
    # failure ladder's ceiling is covered by
    # `test_retryable_failures_use_their_own_ladder_and_give_up_eventually`.
    channel, _store, _flow = uat_channel(
        active_meeting_check_interval_seconds=0.02,
        poll_min_interval_seconds=0.01,
        poll_max_interval_seconds=0.02,
        poll_failure_max_interval_seconds=0.05,
    )
    got = []
    ended = []
    session = await channel.follow_my_meeting(user_open_id=fx.USER_OPEN_ID)
    session.on("transcript", lambda event: got.append(event))
    session.on("end", lambda event: ended.append(event))
    vc.json(fx.URI_ACTIVE_MEETING, fx.error_body(500, "boom"), status=500)
    await fx.wait_for(
        lambda: vc.count(fx.URI_ACTIVE_MEETING) >= 6,
        what="the end-detection loop to keep trying",
    )
    vc.sequence(
        fx.URI_EVENTS,
        [fx.poll_events([fx.poll_item("transcript_received")]), fx.poll_events([])],
    )
    await fx.wait_for(lambda: got, what="a transcript on the still-live session")
    session.dispose()

    assert ended == []


async def test_the_meeting_dropping_off_the_active_list_ends_the_session(
    vc, uat_channel
):
    channel, _store, _flow = uat_channel(active_meeting_check_interval_seconds=0.02)
    with fx.fast_sleep(max_sleeps=20):
        session = await channel.follow_my_meeting(user_open_id=fx.USER_OPEN_ID)
        ended = []
        session.on("end", lambda event: ended.append(event))
        vc.json(fx.URI_ACTIVE_MEETING, fx.active_meeting_body([]))
        await fx.wait_for(lambda: ended, what="the end signal")
        polls = vc.count(fx.URI_EVENTS)
        await fx.settle()

    assert [event.reason for event in ended] == ["no_longer_active"]
    assert vc.count(fx.URI_EVENTS) == polls
