"""Shared fixtures for the meeting-channel test suite."""

import pytest

from . import fixtures as fx


@pytest.fixture
def vc():
    """Fake VC transport, installed for the whole test."""
    fake = fx.FakeVC()
    with fake.patched():
        yield fake


@pytest.fixture
def make_ch():
    """Channel factory that tears every channel down afterwards.

    Channels own a background thread and an event loop; leaving them running
    would let one test's polling loop bleed into the next one's assertions.
    """
    created = []

    def _make(**kwargs):
        channel = fx.make_channel(**kwargs)
        created.append(channel)
        return channel

    yield _make
    for channel in created:
        try:
            channel.stop(join_timeout=1.0)
        except Exception:
            pass


@pytest.fixture
def tat_channel(vc, make_ch):
    """Factory for a connected channel, ready for ``join_meeting``."""

    def _make(**meeting_overrides):
        channel = make_ch(meeting=fx.meeting_config(**meeting_overrides))
        fx.mark_connected(channel)
        return channel

    return _make


@pytest.fixture
def uat_channel(vc, make_ch):
    """Factory for an unconnected channel holding a ticket, for ``follow_my_meeting``.

    Returns ``(channel, token_store, device_flow)``. Deliberately does not
    connect: the follow path is REST-only and must work without a WebSocket.
    """

    def _make(*, scopes=None, access_token="u-REAL", **meeting_overrides):
        store = fx.FakeTokenStore()
        store.put(
            fx.USER_OPEN_ID,
            fx.make_uat(
                access_token,
                scopes=list(scopes) if scopes is not None else [fx.MEETING_EVENT_SCOPE],
            ),
        )
        flow = fx.FakeDeviceFlow()
        channel = make_ch(
            meeting=fx.meeting_config(**meeting_overrides),
            token_store=store,
            device_flow=flow,
        )
        return channel, store, flow

    return _make
