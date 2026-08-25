"""The concurrency gate, session reuse, invite filtering, and follow filtering.

Session creation is triggered from outside the process — anyone who can drag
the bot into a meeting, or send the bot a chat command — so both entry points
need a ceiling and an identity filter.
"""

import asyncio
import threading

import pytest

from lark_channel.channel.config import MeetingChannelConfig, PolicyConfig
from lark_channel.channel.errors import FeishuChannelError, FeishuChannelErrorCode

from . import fixtures as fx


def _two_active_meetings():
    return fx.active_meeting_body(
        [
            {
                "meeting_id": fx.MEETING_ID_STR,
                "meeting_no": fx.MEETING_NO,
                "topic": "First",
            },
            {
                "meeting_id": fx.OTHER_MEETING_ID_STR,
                "meeting_no": fx.OTHER_MEETING_NO,
                "topic": "Second",
            },
        ]
    )


async def test_join_at_the_ceiling_refuses_without_calling_the_api(vc, tat_channel):
    channel = tat_channel(max_concurrent_sessions=1)
    await channel.join_meeting(fx.MEETING_NO)
    joins = vc.count(fx.URI_JOIN)

    with pytest.raises(FeishuChannelError) as excinfo:
        await channel.join_meeting(fx.OTHER_MEETING_NO)

    assert excinfo.value.code is FeishuChannelErrorCode.TOO_MANY_SESSIONS
    assert vc.count(fx.URI_JOIN) == joins


async def test_follow_at_the_ceiling_refuses_without_sending_anything(vc, uat_channel):
    channel, _store, _flow = uat_channel(
        max_concurrent_sessions=1, active_meeting_check_interval_seconds=300.0
    )
    vc.json(fx.URI_ACTIVE_MEETING, _two_active_meetings())
    await channel.follow_my_meeting(
        user_open_id=fx.USER_OPEN_ID, meeting_no=fx.MEETING_NO
    )
    await fx.settle()
    before = len(vc.calls)

    with pytest.raises(FeishuChannelError) as excinfo:
        await channel.follow_my_meeting(
            user_open_id=fx.USER_OPEN_ID, meeting_no=fx.OTHER_MEETING_NO
        )

    assert excinfo.value.code is FeishuChannelErrorCode.TOO_MANY_SESSIONS
    assert len(vc.calls) == before


async def test_the_ceiling_counts_followed_and_joined_meetings_together(
    vc, uat_channel
):
    """Following leaks harder than joining does: it needs no socket, no
    liveness probe applies to it, and a permanently rate-limited follow session
    is designed never to end itself. A gate that only counts joins is bolted
    to the side that does not leak."""
    channel, _store, _flow = uat_channel(
        max_concurrent_sessions=2, active_meeting_check_interval_seconds=300.0
    )
    fx.mark_connected(channel)
    vc.sequence(
        fx.URI_JOIN,
        [fx.join_body(fx.OTHER_MEETING_ID_STR), fx.join_body("999999")],
    )

    await channel.follow_my_meeting(
        user_open_id=fx.USER_OPEN_ID, meeting_no=fx.MEETING_NO
    )
    await channel.join_meeting(fx.OTHER_MEETING_NO)
    await fx.settle()

    with pytest.raises(FeishuChannelError) as excinfo:
        await channel.join_meeting("555555555")
    assert excinfo.value.code is FeishuChannelErrorCode.TOO_MANY_SESSIONS


async def test_following_the_same_meeting_twice_reuses_one_session_and_one_loop(
    vc, uat_channel
):
    channel, _store, _flow = uat_channel(active_meeting_check_interval_seconds=300.0)
    first = await channel.follow_my_meeting(user_open_id=fx.USER_OPEN_ID)
    await fx.settle()
    polls = vc.count(fx.URI_EVENTS)

    second = await channel.follow_my_meeting(user_open_id=fx.USER_OPEN_ID)
    await fx.settle()

    assert second is first
    # A second polling loop would fire its own first request immediately.
    assert vc.count(fx.URI_EVENTS) == polls


async def test_a_permanently_rate_limited_follow_session_still_holds_its_seat(
    vc, uat_channel
):
    channel, _store, _flow = uat_channel(
        max_concurrent_sessions=1,
        poll_max_consecutive_failures=1000,
        active_meeting_check_interval_seconds=300.0,
    )
    fx.mark_connected(channel)
    vc.json(fx.URI_EVENTS, fx.error_body(99991402, "too many request"), status=429)

    with fx.fast_sleep(max_sleeps=8):
        await channel.follow_my_meeting(user_open_id=fx.USER_OPEN_ID)
        await fx.wait_for(
            lambda: vc.count(fx.URI_EVENTS) >= 4, what="several failing poll rounds"
        )

    with pytest.raises(FeishuChannelError) as excinfo:
        await channel.join_meeting(fx.OTHER_MEETING_NO)
    assert excinfo.value.code is FeishuChannelErrorCode.TOO_MANY_SESSIONS


async def test_concurrent_joins_of_one_meeting_number_call_the_api_once(
    vc, tat_channel
):
    """The second caller has to join the in-flight attempt, not start its own.

    The overlap is arranged rather than hoped for: the first join is held open
    at the transport until the second has entered. `asyncio.gather` alone does
    not establish it — the fake transport returns without suspending, so the
    first call can run start to finish, release its claim, and leave the second
    with nothing in flight to find. That made this assertion pass or fail on
    which Python version happened to yield somewhere in the call chain.
    """
    channel = tat_channel()
    # A `threading.Event`, not `asyncio.Event`: the entry point marshals onto the
    # channel's background loop, so the responder runs on a different loop than
    # this test and an asyncio primitive built here would not be awaitable there.
    release = threading.Event()

    async def held(call):
        while not release.is_set():
            await asyncio.sleep(0.001)
        return (200, fx.join_body())

    vc.route(fx.URI_JOIN, held)

    first_task = asyncio.ensure_future(channel.join_meeting(fx.MEETING_NO))
    await fx.wait_for(
        lambda: vc.count(fx.URI_JOIN) == 1, what="the first join to reach the platform"
    )
    second_task = asyncio.ensure_future(channel.join_meeting(fx.MEETING_NO))
    await asyncio.sleep(0.05)  # let the second call get as far as it can
    release.set()

    first, second = await asyncio.gather(first_task, second_task)

    assert vc.count(fx.URI_JOIN) == 1
    assert first is second


async def test_disposing_a_session_does_not_hand_the_seat_back(vc, tat_channel):
    """Disposal stops local work but leaves the bot a participant server-side.
    Reading the gate off live sessions makes it drop to zero while the bot is
    still sitting in the meeting."""
    channel = tat_channel(max_concurrent_sessions=1)
    session = await channel.join_meeting(fx.MEETING_NO)
    session.dispose()
    await fx.settle()

    with pytest.raises(FeishuChannelError) as excinfo:
        await channel.join_meeting(fx.OTHER_MEETING_NO)
    assert excinfo.value.code is FeishuChannelErrorCode.TOO_MANY_SESSIONS


async def test_idle_timeout_leaves_the_meeting_and_hands_the_seat_back(
    vc, tat_channel
):
    channel = tat_channel(
        max_concurrent_sessions=1,
        idle_timeout_seconds=0.05,
        liveness_probe_interval_seconds=0.0,
    )
    vc.sequence(
        fx.URI_JOIN,
        [fx.join_body(fx.MEETING_ID_STR), fx.join_body(fx.OTHER_MEETING_ID_STR)],
    )
    session = await channel.join_meeting(fx.MEETING_NO)
    ended = []
    session.on("end", lambda event: ended.append(event))

    await fx.wait_for(lambda: ended, what="the idle timeout")
    assert ended[0].reason == "idle_timeout"
    await fx.wait_for(lambda: vc.count(fx.URI_LEAVE) == 1, what="the leave call")

    second = await channel.join_meeting(fx.OTHER_MEETING_NO)
    assert second.meeting_id == fx.OTHER_MEETING_ID_STR


async def test_idle_reclamation_is_off_by_default(vc, tat_channel):
    """Reclaiming an idle meeting means the bot visibly walks out of a meeting
    where people simply were not talking."""
    assert MeetingChannelConfig().idle_timeout_seconds == 0.0

    channel = tat_channel(liveness_probe_interval_seconds=0.0)
    session = await channel.join_meeting(fx.MEETING_NO)
    ended = []
    session.on("end", lambda event: ended.append(event))

    await asyncio.sleep(0.2)
    assert ended == []
    assert vc.count(fx.URI_LEAVE) == 0


async def test_repeated_join_and_reclaim_cycles_do_not_accumulate(vc, tat_channel):
    channel = tat_channel(max_concurrent_sessions=2)
    vc.route(fx.URI_JOIN, lambda call: (200, fx.join_body(fx.MEETING_ID_STR)))

    async def _tasks():
        counts = [len(asyncio.all_tasks())]

        async def _count_bg():
            return len(asyncio.all_tasks())

        future = asyncio.run_coroutine_threadsafe(_count_bg(), channel._bg_loop)
        counts.append(future.result(timeout=2.0))
        return sum(counts)

    baseline = None
    for round_index in range(8):
        session = await channel.join_meeting(fx.MEETING_NO)
        await session.leave()
        await fx.settle(2)
        if round_index == 2:
            baseline = await _tasks()

    assert baseline is not None
    assert await _tasks() <= baseline + 2


async def test_invite_from_outside_the_allowlist_is_dropped(vc, tat_channel):
    channel = tat_channel(invite_allowlist=["ou_trusted"])
    invited = []
    channel.on("meetingInvited", lambda event: invited.append(event))

    fx.deliver(channel, fx.push_meeting_invited(inviter_open_id="ou_stranger"))
    await fx.settle()

    assert invited == []
    assert vc.count(fx.URI_JOIN) == 0


async def test_invites_bypass_the_message_policy_by_default(vc, make_ch):
    """The invite path is the only way into a joined meeting, and none of the
    message-policy knobs apply to it. That bypass is deliberate; pinning it as
    tested behaviour is what keeps it from being rediscovered as a bug."""
    channel = make_ch(
        meeting=fx.meeting_config(invite_allowlist=None),
        policy=PolicyConfig(dm_policy="allowlist", allow_from=[], group_policy="allowlist"),
    )
    fx.mark_connected(channel)
    invited = []
    channel.on("meetingInvited", lambda event: invited.append(event))

    fx.deliver(channel, fx.push_meeting_invited(inviter_open_id="ou_anyone"))

    await fx.wait_for(lambda: invited, what="the invite to reach the handler")
    assert invited[0].meeting_no == fx.MEETING_NO
    assert invited[0].inviter.id == "ou_anyone"


async def test_follow_outside_the_allowlist_touches_neither_ticket_nor_network(
    vc, uat_channel
):
    channel, store, flow = uat_channel(follow_allowlist=["ou_trusted"])

    with pytest.raises(FeishuChannelError) as excinfo:
        await channel.follow_my_meeting(user_open_id=fx.USER_OPEN_ID)

    assert excinfo.value.code is FeishuChannelErrorCode.PERMISSION_DENIED
    assert store.get_calls == []
    assert flow.start_calls == []
    assert vc.calls == []


async def test_follow_allowlist_is_checked_before_session_reuse(vc, uat_channel):
    """Reuse is keyed on the meeting, so checking the allowlist afterwards lets
    an unlisted caller inherit somebody else's live session — and with it that
    person's transcript."""
    channel, store, _flow = uat_channel(
        follow_allowlist=["ou_listed"], active_meeting_check_interval_seconds=300.0
    )
    store.put("ou_listed", fx.make_uat("u-LISTED", open_id="ou_listed"))
    listed_session = await channel.follow_my_meeting(user_open_id="ou_listed")
    assert listed_session.meeting_id == fx.MEETING_ID_STR

    with pytest.raises(FeishuChannelError) as excinfo:
        await channel.follow_my_meeting(user_open_id="ou_not_on_the_list")
    assert excinfo.value.code is FeishuChannelErrorCode.PERMISSION_DENIED


async def test_follow_accepts_any_open_id_by_default_and_never_pings_the_owner(
    vc, uat_channel
):
    """The SDK has no way to tell whether the supplied open_id belongs to the
    caller, and a cache hit resolves silently. Both halves of that are pinned
    here so the bypass stays a documented property."""
    assert MeetingChannelConfig().follow_allowlist is None

    channel, store, flow = uat_channel(active_meeting_check_interval_seconds=300.0)
    prompts = []

    class _PromptContext:
        async def respond(self, card):
            prompts.append(card)

    session = await channel.follow_my_meeting(
        user_open_id=fx.USER_OPEN_ID, prompt_context=_PromptContext()
    )
    await fx.settle()

    assert session.mode == "uat"
    assert flow.start_calls == []
    assert prompts == []
