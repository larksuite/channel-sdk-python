"""Locks the wire shapes the rest of this suite is built on.

These are not tests of the SDK — they are a guard rail on ``fixtures.py``.
The reason they exist: a sibling port of this feature shipped 542 unit tests
and eight review rounds without noticing that echo suppression never fired,
because every fixture had been built from the generated model's type hints
(``actor.id: str``) instead of from the bytes the platform actually sends
(``actor.id`` is a nested object on the push transport). Fixtures shaped like
the type hints make a broken implementation pass.

If one of these fails, fix the fixture back — do not adjust the expectation
to match whatever the implementation happens to want.
"""

from . import fixtures as fx


def test_push_transport_carries_meeting_id_as_int():
    payload = fx.push_activity([fx.push_item("transcript_received")])
    meeting_id = payload["event"]["meeting"]["id"]
    assert isinstance(meeting_id, int)
    assert meeting_id == fx.MEETING_ID_INT


def test_poll_transport_carries_meeting_id_as_str():
    body = fx.poll_events([fx.poll_item("transcript_received")])
    meeting_id = body["data"]["events"][0]["meeting_id"]
    assert isinstance(meeting_id, str)
    assert meeting_id == fx.MEETING_ID_STR


def test_push_and_poll_meeting_ids_denote_the_same_meeting():
    assert str(fx.MEETING_ID_INT) == fx.MEETING_ID_STR


def test_push_actor_id_is_a_nested_object_with_three_namespaces():
    payload = fx.push_activity([fx.push_item("transcript_received")])
    item = payload["event"]["meeting_activity_items"][0]["transcript_received_items"][0]
    identifier = item["speaker"]["id"]
    assert isinstance(identifier, dict)
    assert set(identifier) == {"open_id", "union_id", "user_id"}
    assert identifier["open_id"].startswith("ou_")


def test_poll_actor_id_is_a_bare_open_id_string():
    body = fx.poll_events([fx.poll_item("transcript_received")])
    item = body["data"]["events"][0]["payload"]["transcript_received_items"][0]
    identifier = item["speaker"]["id"]
    assert isinstance(identifier, str)
    assert identifier.startswith("ou_")


def test_push_activity_items_are_flat_and_carry_no_event_id():
    payload = fx.push_activity([fx.push_item("chat_received")])
    item = payload["event"]["meeting_activity_items"][0]
    assert "chat_received_items" in item
    assert "payload" not in item
    assert "event_id" not in item


def test_poll_events_nest_items_under_payload_and_carry_an_event_id():
    body = fx.poll_events([fx.poll_item("chat_received")])
    event = body["data"]["events"][0]
    assert "chat_received_items" not in event
    assert "chat_received_items" in event["payload"]
    assert "activity_event_type" in event["payload"]
    assert event["event_id"]


def test_shared_document_lives_under_share_doc_not_doc():
    item = fx.share_started_item(shape="push")
    assert "share_doc" in item
    assert "doc" not in item


def test_item_level_timestamps_are_strings_on_both_transports():
    for shape in ("push", "poll"):
        transcript = fx.transcript_item(shape=shape)
        assert isinstance(transcript["start_time_ms"], str)
        assert isinstance(fx.chat_item(shape=shape)["send_time"], str)
        assert isinstance(
            fx.participant_joined_item(shape=shape)["join_time"], str
        )


def test_document_context_items_never_carry_a_context_type_by_default():
    for kind in ("comment_focus", "section_location", "element_preview", "none"):
        item = fx.document_context_item(shape="push", kind=kind)
        assert "context_type" not in item


def test_connect_error_hides_the_token_from_repr_but_not_from_a_deep_walk():
    error = fx.httpx_connect_error("u-secret")
    assert "u-secret" not in repr(error)
    assert any("u-secret" in text for text in fx.deep_strings(error))
