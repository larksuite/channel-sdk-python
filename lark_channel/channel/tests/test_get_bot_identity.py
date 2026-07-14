"""FeishuChannel.get_bot_identity().

Unlike the existing ``bot_identity`` property (which returns Optional), the new
method raises ``FeishuChannelError(code=not_connected)`` when identity is not
yet resolved, so callers can't silently write a missing identity into a system
prompt.
"""

import pytest

from lark_channel.channel import FeishuChannel
from lark_channel.channel.bot_identity import BotIdentity
from lark_channel.channel.errors import FeishuChannelError, FeishuChannelErrorCode


def _channel():
    return FeishuChannel(app_id="cli_x", app_secret="secret")


def test_returns_resolved_identity():
    ch = _channel()
    ident = BotIdentity(open_id="ou_bot", name="Helper")
    ch._bot_identity = ident
    assert ch.get_bot_identity() is ident


def test_raises_not_connected_when_unresolved():
    ch = _channel()
    with pytest.raises(FeishuChannelError) as excinfo:
        ch.get_bot_identity()
    assert excinfo.value.code == FeishuChannelErrorCode.NOT_CONNECTED


def test_property_still_returns_none_when_unresolved():
    # Regression: the new method must not change the Optional-returning
    # ``bot_identity`` property behaviour.
    ch = _channel()
    assert ch.bot_identity is None
