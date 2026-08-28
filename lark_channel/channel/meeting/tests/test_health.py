"""Health readout and the liveness probe.

Failures on this path are silent by nature: a missing permission, an
undeclared subscription, or a renamed field all look identical from the
outside — nothing happens. Splitting "arrived" from "arrived but unpacked to
nothing" is what separates "the platform never sent it" from "we can no
longer read what it sends".
"""

import asyncio
import logging

import httpx
import pytest

from lark_channel.event.dispatcher_handler import EventDispatcherHandlerBuilder

from . import fixtures as fx


def _probe_calls(vc):
    return vc.for_uri(fx.URI_EVENTS)


async def test_a_quiet_channel_reports_zero_received_and_a_live_registration(
    vc, tat_channel
):
    channel = tat_channel()
    health = channel.get_meeting_event_health()
    assert health.received == 0
    assert health.last_at is None
    assert health.registered is True
    assert health.stats == {}


async def test_a_failed_internal_registration_is_reported_as_such(
    vc, tat_channel, monkeypatch
):
    channel = tat_channel()
    real_register = EventDispatcherHandlerBuilder.register_p2_customized_event

    def _refuse(self, event_type, handler):
        if event_type.startswith("vc.bot."):
            raise RuntimeError("subscription unavailable")
        return real_register(self, event_type, handler)

    monkeypatch.setattr(
        EventDispatcherHandlerBuilder, "register_p2_customized_event", _refuse
    )
    channel._dispatcher = channel._build_dispatcher()

    health = channel.get_meeting_event_health()
    assert health.registered is False
    assert health.reason


async def test_received_and_per_type_counters_move_with_traffic(vc, tat_channel):
    channel = tat_channel()
    session = await channel.join_meeting(fx.MEETING_NO)
    got = []
    session.on("transcript", lambda event: got.append(event))

    fx.deliver(channel, fx.push_activity([fx.push_item("transcript_received")]))
    await fx.wait_for(lambda: got, what="the transcript")

    health = channel.get_meeting_event_health()
    assert health.received >= 1
    assert health.last_at is not None
    stats = health.stats["transcript_received"]
    assert stats.received == 1
    assert stats.empty == 0

    fx.deliver(
        channel,
        fx.push_activity(
            [fx.push_item("transcript_received", [])], envelope_event_id="env-empty"
        ),
    )
    await fx.wait_for(
        lambda: channel.get_meeting_event_health().stats["transcript_received"].empty
        == 1,
        what="the empty unpack to be counted",
    )


async def test_the_stat_key_space_is_bounded_by_count(vc, tat_channel):
    """These keys are chosen by the server, are required to exist for types we
    do not know, and are never reset for the life of the process."""
    channel = tat_channel()
    await channel.join_meeting(fx.MEETING_NO)

    types = ["t_%04d" % index for index in range(5001)]
    fx.deliver(
        channel, fx.push_activity([fx.push_item(name) for name in types])
    )

    await fx.wait_for(
        lambda: len(channel.get_meeting_event_health().stats) >= 5000,
        what="the key space to fill up",
        timeout=20.0,
    )
    await fx.settle()

    stats = channel.get_meeting_event_health().stats
    assert sum(1 for key in stats if key.startswith("t_")) == 5000
    assert "__other__" in stats


@pytest.mark.parametrize(
    "activity_type",
    [
        "x" * 200,
        "has\nnewline",
        "has\x1b[31mansi",
        "HasUpperCase",
        "has spaces",
    ],
)
async def test_a_malformed_activity_type_never_becomes_a_key(
    vc, tat_channel, activity_type
):
    """A bounded key count does not stop one 200-character key, and it does not
    stop a key with a newline in it from reshaping a log line."""
    channel = tat_channel()
    await channel.join_meeting(fx.MEETING_NO)

    fx.deliver(channel, fx.push_activity([fx.push_item(activity_type)]))

    await fx.wait_for(
        lambda: channel.get_meeting_event_health().stats,
        what="the activity to be accounted for",
    )
    stats = channel.get_meeting_event_health().stats
    assert activity_type not in stats
    assert "__other__" in stats


async def test_probe_verdicts_and_the_unknown_streak_are_both_visible(
    vc, tat_channel
):
    """If the probe's permission assumption does not hold in some tenant it
    returns "unknown" forever, and reclamation silently degrades to a feature
    that is off by default. This counter is the only sign of that."""
    channel = tat_channel(liveness_probe_interval_seconds=0.02)
    await channel.join_meeting(fx.MEETING_NO)

    await fx.wait_for(
        lambda: channel.get_meeting_event_health().liveness.consecutive_unknown >= 2,
        what="two inconclusive probes",
    )
    liveness = channel.get_meeting_event_health().liveness
    assert liveness.last_probe_at is not None
    assert liveness.last_verdict == "unknown"

    vc.json(fx.URI_EVENTS, fx.poll_events([fx.poll_item("transcript_received")]))
    await fx.wait_for(
        lambda: channel.get_meeting_event_health().liveness.last_verdict == "in_meeting",
        what="a conclusive probe",
    )
    assert channel.get_meeting_event_health().liveness.consecutive_unknown == 0


async def test_a_probe_proving_the_bot_left_ends_the_session_without_leaving(
    vc, tat_channel
):
    """Being removed by a host produces no meeting-ended event at all, and
    calling depart for a meeting the bot is not in is a guaranteed failure."""
    channel = tat_channel(liveness_probe_interval_seconds=0.02)
    session = await channel.join_meeting(fx.MEETING_NO)
    ended = []
    session.on("end", lambda event: ended.append(event))
    vc.json(
        fx.URI_EVENTS,
        fx.error_body(120004, "bot is not in the meeting"),
        status=403,
    )

    await fx.wait_for(lambda: ended, what="the session to end")
    assert [event.reason for event in ended] == ["no_longer_active"]
    await fx.settle()
    assert vc.count(fx.URI_LEAVE) == 0


async def test_a_user_scoped_absence_code_does_not_end_the_session(vc, tat_channel):
    """``120003`` is about a user, ``120004`` about the bot; both are 403.
    Treating the user-scoped one as the bot's departure kills live sessions."""
    channel = tat_channel(liveness_probe_interval_seconds=0.02)
    session = await channel.join_meeting(fx.MEETING_NO)
    ended = []
    session.on("end", lambda event: ended.append(event))
    vc.json(
        fx.URI_EVENTS,
        fx.error_body(120003, "user is not in the meeting"),
        status=403,
    )

    await fx.wait_for(
        lambda: vc.count(fx.URI_EVENTS) >= 3, what="several probes"
    )
    assert ended == []


@pytest.mark.parametrize("failure", ["network", "permission", "empty"])
async def test_an_inconclusive_probe_keeps_the_session_alive(
    vc, tat_channel, failure
):
    """Probes run on the same schedule for every session, so their failures are
    correlated: ending sessions on an inconclusive probe takes them all out in
    one tick."""
    channel = tat_channel(liveness_probe_interval_seconds=0.02)
    session = await channel.join_meeting(fx.MEETING_NO)
    ended = []
    session.on("end", lambda event: ended.append(event))

    if failure == "network":
        request = httpx.Request("GET", "https://open.feishu.cn" + fx.URI_EVENTS)
        vc.route(
            fx.URI_EVENTS,
            lambda call: (_ for _ in ()).throw(
                httpx.ConnectError("unreachable", request=request)
            ),
        )
    elif failure == "permission":
        vc.json(fx.URI_EVENTS, fx.error_body(99991672, "no permission"), status=403)
    else:
        vc.json(fx.URI_EVENTS, fx.poll_events([]))

    await fx.wait_for(lambda: vc.count(fx.URI_EVENTS) >= 3, what="several probes")
    await fx.settle()
    assert ended == []
    assert channel.get_meeting_event_health().liveness.last_verdict == "unknown"


async def test_the_probe_asks_for_a_page_the_endpoint_will_accept(vc, tat_channel):
    """This endpoint rejects a page size below twenty at validation time, so a
    probe asking for one item never gets an answer about anything."""
    channel = tat_channel(liveness_probe_interval_seconds=0.02)
    await channel.join_meeting(fx.MEETING_NO)

    await fx.wait_for(lambda: _probe_calls(vc), what="the first probe")
    page_size = _probe_calls(vc)[0].query("page_size")
    assert page_size is not None
    assert int(page_size) >= 20


async def test_probe_backfill_is_delivered_and_resets_the_idle_clock(
    vc, tat_channel, caplog
):
    """One call does two jobs: it proves the bot is still a participant and it
    picks up whatever the push transport missed."""
    channel = tat_channel(
        liveness_probe_interval_seconds=0.02, idle_timeout_seconds=0.5
    )
    session = await channel.join_meeting(fx.MEETING_NO)
    got = []
    ended = []
    session.on("transcript", lambda event: got.append(event))
    session.on("end", lambda event: ended.append(event))

    # Every probe has to bring back something genuinely new, or dedup
    # suppresses it and the idle clock is never touched.
    served = {"n": 0}

    def _fresh_backfill(call):
        served["n"] += 1
        index = served["n"]
        return (
            200,
            fx.poll_events(
                [
                    fx.poll_item(
                        "transcript_received",
                        [
                            fx.transcript_item(
                                shape="poll",
                                text="line-%d" % index,
                                sentence_id="s-%d" % index,
                            )
                        ],
                        event_id="evt-backfill-%d" % index,
                    )
                ]
            ),
        )

    vc.route(fx.URI_EVENTS, _fresh_backfill)

    with caplog.at_level(logging.WARNING, logger="Lark"):
        await fx.wait_for(lambda: got, what="the backfilled transcript")
        await asyncio.sleep(0.7)

    assert ended == []
