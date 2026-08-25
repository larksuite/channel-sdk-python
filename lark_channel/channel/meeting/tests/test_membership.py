"""Server-side participation accounting — the thing the concurrency gate reads.

The gate cannot count live sessions: disposal stops local work but leaves the
bot a participant, and a failed departure removes the session while the seat
is still taken. So the seat is released on *evidence that the bot is no longer
a participant*, which is a different thing from "the leave call returned 200".
Both directions have to hold: never releasing a seat on a normal meeting end
bricks both entry points for the life of the process, and always releasing one
turns the gate off.
"""

import asyncio
import logging

import httpx
import pytest

from lark_channel.channel.errors import FeishuChannelError, FeishuChannelErrorCode

from . import fixtures as fx


def _sequential_joins(vc, start=8000001):
    """Give every join a distinct long meeting id, and report them back."""
    minted = []

    def responder(call):
        meeting_id = str(start + len(minted))
        minted.append(meeting_id)
        return (200, fx.join_body(meeting_id))

    vc.route(fx.URI_JOIN, responder)
    return minted


async def test_a_normally_ended_meeting_gives_its_seat_back(vc, tat_channel):
    """Ending is exactly when a departure call is most likely to 404, so a seat
    that only comes back on a clean departure leaks once per normal meeting."""
    channel = tat_channel(max_concurrent_sessions=1)
    minted = _sequential_joins(vc)

    for _ in range(3):
        session = await channel.join_meeting(fx.MEETING_NO)
        ended = []
        session.on("end", lambda event: ended.append(event))
        fx.deliver(channel, fx.push_meeting_ended(meeting_id=int(minted[-1])))
        await fx.wait_for(lambda: ended, what="the meeting-ended signal")
        await fx.wait_for(
            lambda: channel.get_meeting_event_health().membership.held == 0,
            what="the gate reading to fall back",
        )

    assert len(minted) == 3


@pytest.mark.parametrize(
    "outcome,evidence",
    [
        ((404, fx.error_body(404, "not found")), "404"),
        ((400, fx.error_body(121105, "meeting not exist")), "121105"),
        ((403, fx.error_body(120004, "bot is not in the meeting")), "120004"),
    ],
)
async def test_departure_errors_that_prove_absence_release_the_seat(
    vc, tat_channel, outcome, evidence
):
    channel = tat_channel(max_concurrent_sessions=1)
    _sequential_joins(vc)
    vc.route(fx.URI_LEAVE, lambda call: outcome)

    session = await channel.join_meeting(fx.MEETING_NO)
    await session.leave()
    await fx.settle()

    membership = channel.get_meeting_event_health().membership
    assert membership.held == 0
    assert membership.released_by_evidence.get(evidence) == 1

    second = await channel.join_meeting(fx.OTHER_MEETING_NO)
    assert second is not None


@pytest.mark.parametrize(
    "failure",
    [
        "server_error",
        "timeout",
    ],
)
async def test_departure_failures_of_unknown_outcome_keep_the_seat(
    vc, tat_channel, failure
):
    channel = tat_channel(
        max_concurrent_sessions=1, membership_reconcile_interval_seconds=0.0
    )
    _sequential_joins(vc)
    if failure == "server_error":
        vc.route(fx.URI_LEAVE, lambda call: (500, fx.error_body(500, "boom")))
    else:
        request = httpx.Request("POST", "https://open.feishu.cn" + fx.URI_LEAVE)
        vc.route(
            fx.URI_LEAVE,
            lambda call: (_ for _ in ()).throw(
                httpx.TimeoutException("timed out", request=request)
            ),
        )

    session = await channel.join_meeting(fx.MEETING_NO)
    await session.leave()
    await fx.settle()

    membership = channel.get_meeting_event_health().membership
    assert membership.held == 1
    assert membership.retained_without_session == 1

    with pytest.raises(FeishuChannelError) as excinfo:
        await channel.join_meeting(fx.OTHER_MEETING_NO)
    assert excinfo.value.code is FeishuChannelErrorCode.TOO_MANY_SESSIONS


async def test_meeting_ended_releases_a_seat_whose_session_is_already_gone(
    vc, tat_channel
):
    """Three rules stack into a dead end: a failed departure keeps the seat,
    still removes the session, and routing drops events for meetings it has no
    session for. Accounting has to keep listening after delivery stops."""
    channel = tat_channel(
        max_concurrent_sessions=1, membership_reconcile_interval_seconds=0.0
    )
    minted = _sequential_joins(vc)
    vc.route(fx.URI_LEAVE, lambda call: (500, fx.error_body(500, "boom")))

    session = await channel.join_meeting(fx.MEETING_NO)
    await session.leave()
    await fx.settle()
    with pytest.raises(FeishuChannelError):
        await channel.join_meeting(fx.OTHER_MEETING_NO)

    fx.deliver(channel, fx.push_meeting_ended(meeting_id=int(minted[0])))
    await fx.settle()

    second = await channel.join_meeting(fx.OTHER_MEETING_NO)
    assert second is not None


async def test_a_stale_seat_is_reclaimed_by_the_next_admission(vc, uat_channel):
    """Nothing periodic reclaims this seat, and nothing should: a loop that
    outlived ``disconnect()`` would have to sit on its own thread, and a thread
    still running after the call whose whole job is releasing resources is a
    dangling resource. So reclamation hangs off admission instead — the moment
    a seat is actually wanted. After a disconnect, joining is unavailable
    anyway; following is the entry point a stale seat can really block, and
    following goes through admission."""
    channel, _store, _flow = uat_channel(
        max_concurrent_sessions=1,
        membership_reconcile_interval_seconds=60.0,
        membership_max_age_seconds=3600.0,
        active_meeting_check_interval_seconds=300.0,
    )
    fx.mark_connected(channel)
    _sequential_joins(vc)
    vc.sequence(
        fx.URI_LEAVE,
        [(500, fx.error_body(500, "boom")), (200, {"code": 0, "msg": "success"})],
    )

    session = await channel.join_meeting(fx.MEETING_NO)
    await session.leave()
    await fx.settle()
    assert channel.get_meeting_event_health().membership.retained_without_session == 1
    before = vc.count(fx.URI_LEAVE) + vc.count(fx.URI_EVENTS)

    await channel.disconnect()
    followed = await channel.follow_my_meeting(user_open_id=fx.USER_OPEN_ID)

    assert followed.mode == "uat"
    membership = channel.get_meeting_event_health().membership
    assert membership.reconcile_attempts >= 1
    assert vc.count(fx.URI_LEAVE) + vc.count(fx.URI_EVENTS) > before
    assert membership.retained_without_session == 0
    followed.dispose()


async def test_reconciliation_is_throttled_per_entry(vc, uat_channel):
    """Admission is on the latency path of two public calls, so a stale entry
    must not be retried once per attempt."""
    channel, _store, _flow = uat_channel(
        max_concurrent_sessions=1,
        membership_reconcile_interval_seconds=60.0,
        membership_max_age_seconds=3600.0,
        active_meeting_check_interval_seconds=300.0,
    )
    fx.mark_connected(channel)
    _sequential_joins(vc)
    vc.route(fx.URI_LEAVE, lambda call: (500, fx.error_body(500, "boom")))

    session = await channel.join_meeting(fx.MEETING_NO)
    await session.leave()
    await fx.settle()
    before = vc.count(fx.URI_LEAVE) + vc.count(fx.URI_EVENTS)

    for _ in range(2):
        with pytest.raises(FeishuChannelError) as excinfo:
            await channel.follow_my_meeting(user_open_id=fx.USER_OPEN_ID)
        assert excinfo.value.code is FeishuChannelErrorCode.TOO_MANY_SESSIONS

    assert channel.get_meeting_event_health().membership.reconcile_attempts == 1
    assert vc.count(fx.URI_LEAVE) + vc.count(fx.URI_EVENTS) == before + 1


async def test_releasing_on_meeting_not_exist_is_reported_not_just_absorbed(
    vc, tat_channel, caplog
):
    """That code also fires when we have been sending the wrong meeting id all
    along. Absorbing it silently turns the gate off and keeps the suite green."""
    channel = tat_channel(max_concurrent_sessions=1)
    _sequential_joins(vc)
    vc.route(fx.URI_LEAVE, lambda call: (400, fx.error_body(121105, "meeting not exist")))

    session = await channel.join_meeting(fx.MEETING_NO)
    with caplog.at_level(logging.WARNING, logger="Lark"):
        await session.leave()
        await fx.settle()

    assert any(
        record.levelno >= logging.WARNING and "121105" in record.getMessage()
        for record in caplog.records
    ), caplog.text
    membership = channel.get_meeting_event_health().membership
    assert membership.released_by_evidence["121105"] == 1


async def test_a_seat_past_its_deadline_is_force_released_with_a_warning(
    vc, tat_channel, caplog
):
    channel = tat_channel(
        max_concurrent_sessions=1,
        membership_reconcile_interval_seconds=0.0,
        membership_max_age_seconds=0.1,
    )
    _sequential_joins(vc)
    vc.route(fx.URI_LEAVE, lambda call: (500, fx.error_body(500, "boom")))

    session = await channel.join_meeting(fx.MEETING_NO)
    with caplog.at_level(logging.WARNING, logger="Lark"):
        await session.leave()
        await asyncio.sleep(0.25)
        second = await channel.join_meeting(fx.OTHER_MEETING_NO)

    assert second is not None
    assert any(record.levelno >= logging.WARNING for record in caplog.records)
    assert (
        channel.get_meeting_event_health().membership.released_by_evidence["ttl"] == 1
    )


async def test_repeated_failed_departures_never_brick_either_entry_point(
    vc, uat_channel
):
    """One tenant member looping "invite the bot, end the meeting" is enough to
    exhaust the gate if a 5xx departure strands the seat. It takes both entry
    points down together, because they share the gate."""
    channel, _store, _flow = uat_channel(
        max_concurrent_sessions=2,
        membership_reconcile_interval_seconds=0.0,
        active_meeting_check_interval_seconds=300.0,
    )
    fx.mark_connected(channel)
    minted = _sequential_joins(vc)
    vc.route(fx.URI_LEAVE, lambda call: (500, fx.error_body(500, "boom")))

    for _ in range(3):
        session = await channel.join_meeting(fx.MEETING_NO)
        await session.leave()
        fx.deliver(channel, fx.push_meeting_ended(meeting_id=int(minted[-1])))
        await fx.settle()

    survivor = await channel.join_meeting(fx.OTHER_MEETING_NO)
    assert survivor is not None
    followed = await channel.follow_my_meeting(user_open_id=fx.USER_OPEN_ID)
    assert followed.mode == "uat"


async def test_a_probe_proving_absence_hands_the_seat_back(vc, tat_channel):
    """The one reclamation path with no meeting-ended event behind it. Nothing
    else ever produces evidence for it — lazy reconciliation only looks at
    entries whose *departure* was inconclusive — so a seat left here waits out
    the accounting deadline, and repeating the removal exhausts the ceiling."""
    channel = tat_channel(
        max_concurrent_sessions=1, liveness_probe_interval_seconds=0.02
    )
    _sequential_joins(vc)
    session = await channel.join_meeting(fx.MEETING_NO)
    ended = []
    session.on("end", lambda event: ended.append(event))
    vc.json(
        fx.URI_EVENTS,
        fx.error_body(120004, "bot is not in the meeting"),
        status=403,
    )

    await fx.wait_for(lambda: ended, what="the session to end")
    await fx.wait_for(
        lambda: channel.get_meeting_event_health().membership.held == 0,
        what="the seat to come back",
    )

    membership = channel.get_meeting_event_health().membership
    assert membership.released_by_evidence.get("120004") == 1
    # No departure call: the bot is already out, and the endpoint rejects a
    # departure for a meeting it is not in.
    assert vc.count(fx.URI_LEAVE) == 0
    # And the ceiling really is free again.
    assert await channel.join_meeting(fx.OTHER_MEETING_NO) is not None


async def test_a_live_session_is_never_expired_by_the_accounting_deadline(
    vc, tat_channel
):
    """Two hours of a meeting where nobody speaks produces no activity and no
    conclusive probe. The deadline is a backstop for mis-accounting, and a seat
    with a live session is not mis-accounted."""
    channel = tat_channel(
        max_concurrent_sessions=1,
        membership_max_age_seconds=0.05,
        liveness_probe_interval_seconds=0.0,
    )
    _sequential_joins(vc)
    session = await channel.join_meeting(fx.MEETING_NO)
    ended = []
    session.on("end", lambda event: ended.append(event))

    await asyncio.sleep(0.2)
    # Reading health is what runs the expiry sweep.
    membership = channel.get_meeting_event_health().membership
    assert membership.held == 1
    assert "ttl" not in membership.released_by_evidence
    assert ended == []


async def test_rejoining_a_meeting_resets_its_accounting_entry(vc, tat_channel):
    """Carrying the old flags over would count a meeting with a live session as
    one whose departure is unresolved, and send reconciliation after it every
    interval — each attempt refused, each one logging the opposite of the
    truth."""
    channel = tat_channel(membership_reconcile_interval_seconds=0.0)
    vc.route(fx.URI_JOIN, lambda call: (200, fx.join_body(fx.MEETING_ID_STR)))
    vc.route(fx.URI_LEAVE, lambda call: (500, fx.error_body(500, "boom")))

    first = await channel.join_meeting(fx.MEETING_NO)
    await first.leave()
    await fx.settle()
    assert channel.get_meeting_event_health().membership.retained_without_session == 1

    await channel.join_meeting(fx.MEETING_NO)
    await fx.settle()

    membership = channel.get_meeting_event_health().membership
    assert membership.held == 1
    assert membership.retained_without_session == 0


async def test_two_joins_of_one_meeting_leave_the_live_seat_alone(vc, tat_channel):
    """The supersede path accounts for the new session, then the old one's
    teardown runs, then the new one is registered. A stored "has a live
    session" flag ends up ``False`` for a meeting that is very much live, and
    the deadline then releases its seat."""
    channel = tat_channel(
        membership_max_age_seconds=0.05,
        liveness_probe_interval_seconds=0.0,
        membership_reconcile_interval_seconds=0.0,
    )
    vc.route(fx.URI_JOIN, lambda call: (200, fx.join_body(fx.MEETING_ID_STR)))

    first = await channel.join_meeting(fx.MEETING_NO)
    second = await channel.join_meeting(fx.MEETING_NO)
    # The two calls are sequential, so the in-flight table is already clear and
    # the second really is a new session superseding the first. Asserted rather
    # than assumed: if `join_meeting` ever starts handing back an existing
    # session for an already-joined meeting, this guard would stop exercising
    # the supersede path while staying green.
    assert second is not first
    await asyncio.sleep(0.2)

    membership = channel.get_meeting_event_health().membership
    assert membership.held == 1
    assert "ttl" not in membership.released_by_evidence


async def test_becoming_ready_reconciles_stranded_seats(vc, tat_channel):
    """The third reconciliation point. It was documented before it existed, so
    it gets an assertion of its own."""
    channel = tat_channel(
        membership_reconcile_interval_seconds=0.0, membership_max_age_seconds=3600.0
    )
    _sequential_joins(vc)
    vc.route(fx.URI_LEAVE, lambda call: (500, fx.error_body(500, "boom")))

    session = await channel.join_meeting(fx.MEETING_NO)
    await session.leave()
    await fx.settle()
    assert channel.get_meeting_event_health().membership.retained_without_session == 1

    attempts_before = channel.get_meeting_event_health().membership.reconcile_attempts
    channel._meeting._membership._interval = 60.0
    vc.route(fx.URI_LEAVE, lambda call: (200, {"code": 0, "msg": "success"}))

    # What a reconnect does.
    fx.mark_connected(channel)

    await fx.wait_for(
        lambda: channel.get_meeting_event_health().membership.reconcile_attempts
        > attempts_before,
        what="reconciliation on becoming ready",
    )
    await fx.wait_for(
        lambda: channel.get_meeting_event_health().membership.retained_without_session
        == 0,
        what="the stranded seat to come back",
    )
