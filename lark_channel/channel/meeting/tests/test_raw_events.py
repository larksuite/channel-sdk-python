"""Subscribing to event types the channel has not wrapped.

Two traps shape every case here. The dispatcher keeps callback events (the
ones whose return value goes back to Feishu) in a different table from plain
events, and it consults the callback table first — so a callback type
registered into the plain table never fires and never complains. And the
dispatcher is rebuilt from scratch on every start, so a subscription that
only lives on the current dispatcher instance goes quiet after one restart,
also without complaining.
"""

import json
import logging
import time

import pytest

from lark_channel.channel.config import InboundConfig, PolicyConfig
from lark_channel.channel.errors import FeishuChannelError, FeishuChannelErrorCode
from lark_channel.core.json import JSON
from lark_channel.core.exception import EventException
from lark_channel.channel.raw_events import RawEventRegistry

from . import fixtures as fx


def _envelope(event_type, event=None, *, event_id="env-raw-1"):
    return {
        "schema": "2.0",
        "header": {
            "event_id": event_id,
            "event_type": event_type,
            "create_time": "1730000000000",
            "token": "",
            "app_id": "cli_x",
            "tenant_key": "tk_1",
        },
        "event": event if event is not None else {"marker": "payload"},
    }


def _im_message(text="hello", open_id="ou_sender", message_id="om_raw_1"):
    # A current timestamp, because the built-in inbound path runs a staleness
    # check: a fixed one from the past is rejected as stale before it reaches
    # any handler, which would make these cases fail for a reason that has
    # nothing to do with raw subscriptions.
    now = str(int(time.time() * 1000))
    return _envelope(
        "im.message.receive_v1",
        {
            "sender": {
                "sender_id": {"open_id": open_id, "user_id": "u_sender"},
                "sender_type": "user",
            },
            "message": {
                "message_id": message_id,
                "chat_id": "oc_p2p",
                "chat_type": "p2p",
                "message_type": "text",
                "create_time": now,
                "update_time": now,
                "content": json.dumps({"text": text}),
                "mentions": [],
            },
        },
        event_id="env-im-%s" % message_id,
    )


def _card_action():
    return _envelope(
        "card.action.trigger",
        {
            "operator": {"open_id": "ou_clicker", "tenant_key": "tk_1"},
            "action": {"tag": "button", "value": {"kind": "raw-test"}},
            "context": {"open_message_id": "om_card_1", "open_chat_id": "oc_1"},
            "token": "card-token",
        },
        event_id="env-card-1",
    )


async def test_an_unwrapped_event_type_reaches_its_handler(vc, tat_channel):
    channel = tat_channel()
    first, second = [], []
    off_first = channel.on_raw_event("vc.bot.meeting_started_v1", lambda p: first.append(p))
    channel.on_raw_event("vc.bot.meeting_started_v1", lambda p: second.append(p))

    fx.deliver(channel, _envelope("vc.bot.meeting_started_v1"))
    await fx.wait_for(lambda: first and second, what="both raw handlers")
    assert isinstance(first[0], dict)

    off_first()
    fx.deliver(channel, _envelope("vc.bot.meeting_started_v1", event_id="env-raw-2"))
    await fx.wait_for(lambda: len(second) >= 2, what="the surviving raw handler")
    assert len(first) == 1


async def test_a_wrapped_event_type_keeps_its_builtin_handler(vc, tat_channel):
    """Same-key registration raises in this dispatcher rather than silently
    overwriting, so the two handlers have to be combined explicitly."""
    channel = tat_channel()
    messages, raw = [], []
    channel.on("message", lambda event: messages.append(event))
    channel.on_raw_event("im.message.receive_v1", lambda p: raw.append(p))

    fx.deliver(channel, _im_message("hi there"))

    await fx.wait_for(lambda: raw, what="the raw handler")
    await fx.wait_for(lambda: messages, what="the wrapped message handler")


async def test_a_callback_event_type_fires_and_keeps_returning_the_builtin_result(
    vc, tat_channel
):
    """Registering this type as a plain event puts it in the table the
    dispatcher never reaches for it."""
    channel = tat_channel()
    raw = []
    channel.on_raw_event("card.action.trigger", lambda p: raw.append(p))

    result = fx.deliver(channel, _card_action())

    await fx.wait_for(lambda: raw, what="the raw handler on a callback event")
    assert result is not None
    assert JSON.marshal(result) is not None


async def test_a_callback_type_with_no_builtin_handler_still_answers_feishu(
    vc, tat_channel
):
    """A callback with no valid return value leaves the card's button dead in
    the user's client."""
    channel = tat_channel()
    raw = []
    channel.on_raw_event("url.preview.get", lambda p: raw.append(p))

    result = fx.deliver(channel, _envelope("url.preview.get", {"url": "https://x.test"}))

    await fx.wait_for(lambda: raw, what="the raw handler")
    assert result is not None
    assert JSON.marshal(result) is not None


@pytest.mark.parametrize(
    "event_type", ["p2.im.message.receive_v1", "p1.drive.notice.comment_add_v1"]
)
def test_a_prefixed_event_type_is_rejected(vc, tat_channel, event_type):
    """The prefix is added internally; passing one produces a key like
    ``p2.p2.x`` that can never match an incoming event."""
    channel = tat_channel()
    with pytest.raises(FeishuChannelError) as excinfo:
        channel.on_raw_event(event_type, lambda p: None)
    assert excinfo.value.code is FeishuChannelErrorCode.FORMAT_ERROR


async def test_unsubscribing_the_last_handler_does_not_uninstall_the_processor(
    vc, tat_channel
):
    """An unregistered type raises inside the dispatcher, which on the socket
    path prints a full traceback for every single event of that type."""
    channel = tat_channel()
    off = channel.on_raw_event("vc.bot.meeting_started_v1", lambda p: None)
    fx.deliver(channel, _envelope("vc.bot.meeting_started_v1"))
    off()

    fx.deliver(channel, _envelope("vc.bot.meeting_started_v1", event_id="env-raw-3"))


async def test_a_raw_handler_that_raises_leaves_the_builtin_path_alone(
    vc, tat_channel
):
    channel = tat_channel()
    messages, errors = [], []
    channel.on("message", lambda event: messages.append(event))
    channel.on("error", lambda err: errors.append(err))

    def _explode(payload):
        raise RuntimeError("handler bug")

    channel.on_raw_event("im.message.receive_v1", _explode)
    fx.deliver(channel, _im_message("still delivered"))

    await fx.wait_for(lambda: messages, what="the wrapped message handler")
    await fx.wait_for(lambda: errors, what="the raw handler failure")


async def test_raw_subscriptions_ignore_the_raw_payload_mirror_switch(vc, make_ch):
    """``on_raw_event`` and the ``raw`` event are different features: one
    subscribes to unwrapped event types, the other mirrors already-wrapped
    events, and only the latter is what that switch controls."""
    channel = make_ch(
        meeting=fx.meeting_config(), inbound=InboundConfig(emit_raw_events=False)
    )
    fx.mark_connected(channel)
    raw = []
    channel.on_raw_event("vc.bot.meeting_started_v1", lambda p: raw.append(p))

    fx.deliver(channel, _envelope("vc.bot.meeting_started_v1"))
    await fx.wait_for(lambda: raw, what="the raw handler")


async def test_a_policy_that_blocks_every_dm_does_not_block_a_raw_subscription(
    vc, make_ch
):
    """This escape hatch sits outside the policy gate, the dedup cache, and the
    loop guard by construction. Subscribing to a type the channel already
    handles therefore opens an unpoliced path into that type — deliberate, and
    pinned here so it stays a documented property rather than a later surprise."""
    channel = make_ch(
        meeting=fx.meeting_config(),
        policy=PolicyConfig(dm_policy="allowlist", allow_from=[]),
    )
    fx.mark_connected(channel)
    messages, raw = [], []
    channel.on("message", lambda event: messages.append(event))
    channel.on_raw_event("im.message.receive_v1", lambda p: raw.append(p))

    fx.deliver(channel, _im_message("from a stranger", open_id="ou_stranger"))

    await fx.wait_for(lambda: raw, what="the raw handler")
    await fx.settle()
    assert messages == []


async def test_subscriptions_survive_a_dispatcher_rebuild(vc, tat_channel):
    """Every start rebuilds the whole processor table from scratch."""
    channel = tat_channel()
    raw = []
    channel.on_raw_event("vc.bot.meeting_started_v1", lambda p: raw.append(p))

    channel._dispatcher = channel._build_dispatcher()

    fx.deliver(channel, _envelope("vc.bot.meeting_started_v1"))
    await fx.wait_for(lambda: raw, what="the raw handler after a rebuild")
    assert channel.get_meeting_event_health().registered is True


async def test_two_subscriptions_to_one_event_type_survive_a_rebuild_together(
    vc, tat_channel
):
    """Replaying handler by handler makes the second registration for a type
    raise, and that exception escapes the rebuild and takes the whole start
    down — the message path included. A single-handler check cannot see it."""
    channel = tat_channel()
    first, second = [], []
    channel.on_raw_event("vc.bot.meeting_started_v1", lambda p: first.append(p))
    channel.on_raw_event("vc.bot.meeting_started_v1", lambda p: second.append(p))

    channel._dispatcher = channel._build_dispatcher()

    fx.deliver(channel, _envelope("vc.bot.meeting_started_v1"))
    await fx.wait_for(lambda: first and second, what="both handlers after a rebuild")


async def test_a_broken_replay_entry_cannot_take_the_rebuild_down(
    vc, tat_channel, caplog, monkeypatch
):
    channel = tat_channel()
    real_install = RawEventRegistry._install

    def _poisoned(self, target, event_type):
        if event_type == "poison.event_v1":
            raise EventException("processor already registered, type: %s" % event_type)
        return real_install(self, target, event_type)

    healthy = []
    channel.on_raw_event("poison.event_v1", lambda p: None)
    channel.on_raw_event("vc.bot.meeting_started_v1", lambda p: healthy.append(p))
    monkeypatch.setattr(RawEventRegistry, "_install", _poisoned)

    with caplog.at_level(logging.WARNING, logger="Lark"):
        channel._dispatcher = channel._build_dispatcher()

    assert any(record.levelno >= logging.WARNING for record in caplog.records), caplog.text
    assert channel.get_meeting_event_health().reason


async def test_a_new_subscription_reaches_a_dispatcher_somebody_already_holds(
    vc, tat_channel
):
    """``on_raw_event`` installs onto the running dispatcher in place, so a
    subscription takes effect immediately. Rebuilding and reassigning would only
    help consumers that re-read the channel's attribute; a transport holds the
    instance it was given and would not see the subscription until the next
    start.

    Delivering through a reference captured *before* subscribing is what tells
    those two apart — the suite's own helper re-reads the attribute at delivery
    time, so every other case here passes either way.
    """
    channel = tat_channel()
    captured = channel.dispatcher

    seen = []
    channel.on_raw_event("vc.bot.meeting_started_v1", lambda p: seen.append(p))

    captured._do_without_validation(
        json.dumps(_envelope("vc.bot.meeting_started_v1")).encode("utf-8")
    )
    await fx.wait_for(lambda: seen, what="the handler through the held dispatcher")
