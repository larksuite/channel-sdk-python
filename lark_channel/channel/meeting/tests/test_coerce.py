"""Identifier coercion: actor ids arrive in two shapes, meeting ids in two types."""

from lark_channel.channel.meeting.coerce import actor_id, meeting_id_str, to_ms

from . import fixtures as fx


def test_actor_id_prefers_open_id_when_the_id_field_is_a_nested_object():
    resolved = actor_id(
        {"id": {"open_id": "ou_x", "union_id": "on_y", "user_id": "u_z"}}
    )
    assert resolved == "ou_x"


def test_actor_id_falls_back_through_union_id_then_user_id():
    assert actor_id({"id": {"union_id": "on_y", "user_id": "u_z"}}) == "on_y"
    assert actor_id({"id": {"user_id": "u_z"}}) == "u_z"


def test_actor_id_accepts_a_plain_string_id():
    assert actor_id({"id": "ou_x"}) == "ou_x"


def test_actor_id_falls_back_to_sibling_open_id_when_id_is_absent():
    assert actor_id(fx.actor_without_id("ou_fallback")) == "ou_fallback"


def test_actor_id_never_returns_a_non_string():
    # An empty result is allowed, a dict leaking through is not: downstream
    # compares this value against the bot's own open_id.
    for candidate in ({}, {"id": {}}, {"id": None}, None):
        resolved = actor_id(candidate)
        assert isinstance(resolved, str) or resolved is None


def test_meeting_id_normalizes_int_and_str_to_the_same_string():
    assert meeting_id_str(fx.MEETING_ID_INT) == fx.MEETING_ID_STR
    assert meeting_id_str(fx.MEETING_ID_STR) == fx.MEETING_ID_STR


def test_to_ms_parses_string_digits_and_never_raises_on_garbage():
    assert to_ms("1730000000123") == 1730000000123
    assert to_ms(1730000000123) == 1730000000123
    assert to_ms("abc") is None
    assert to_ms(None) is None
    assert to_ms("") is None


async def test_int_meeting_id_in_a_push_routes_to_a_session_keyed_by_string(
    vc, tat_channel
):
    """The join response spells the meeting id ``"7654321"``; the push spells
    the same meeting ``7654321``. Without normalization the session lookup
    misses and every pushed activity is dropped in silence."""
    channel = tat_channel()
    session = await channel.join_meeting(fx.MEETING_NO)
    assert session.meeting_id == fx.MEETING_ID_STR

    seen = []
    session.on("transcript", lambda event: seen.append(event))
    fx.deliver(
        channel,
        fx.push_activity(
            [fx.push_item("transcript_received")], meeting_id=fx.MEETING_ID_INT
        ),
    )

    await fx.wait_for(lambda: seen, what="a transcript routed from an int meeting id")
    assert seen[0].meeting_id == fx.MEETING_ID_STR
