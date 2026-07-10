"""allowlist / group_allowlist misconfiguration warn.

``allow_from`` expects sender ids (ou_ / user_id / union_id) and
``group_allowlist`` expects chat ids (oc_). A ``cli_`` app id in either is
almost certainly a mistake that silently matches nobody. PolicyGate warns once
per offending field — naming the field and a single offending value only (no
full-table dump / PII) — without changing matching behaviour.
"""

import logging

from lark_channel.channel import Conversation, Identity, InboundMessage
from lark_channel.channel.config import PolicyConfig
from lark_channel.channel.safety.policy_gate import PolicyGate
from lark_channel.channel.types import TextContent


def _warnings(caplog):
    return [r.getMessage() for r in caplog.records if r.levelno >= logging.WARNING]


def test_cli_in_allow_from_warns_with_field_and_value(caplog):
    with caplog.at_level(logging.WARNING, logger="Lark"):
        PolicyGate(PolicyConfig(allow_from=["cli_abc", "ou_valid"]))

    hits = [m for m in _warnings(caplog) if "allow_from" in m and "cli_abc" in m]
    assert len(hits) == 1
    assert "ou_valid" not in hits[0]


def test_cli_in_group_allowlist_warns_with_field_and_value(caplog):
    with caplog.at_level(logging.WARNING, logger="Lark"):
        PolicyGate(PolicyConfig(group_allowlist=["cli_x", "oc_valid"]))

    hits = [m for m in _warnings(caplog) if "group_allowlist" in m and "cli_x" in m]
    assert len(hits) == 1
    assert "oc_valid" not in hits[0]


def test_valid_lists_do_not_warn(caplog):
    with caplog.at_level(logging.WARNING, logger="Lark"):
        PolicyGate(PolicyConfig(allow_from=["ou_a"], group_allowlist=["oc_c"]))

    assert _warnings(caplog) == []


def test_warn_does_not_change_matching(caplog):
    # The cli_ entry still matches nobody: a real sender is rejected as usual.
    with caplog.at_level(logging.WARNING, logger="Lark"):
        gate = PolicyGate(PolicyConfig(dm_policy="allowlist", allow_from=["cli_x"]))

    msg = InboundMessage(
        id="om_1",
        create_time=1,
        conversation=Conversation(chat_id="oc_c", chat_type="p2p"),
        sender=Identity(open_id="ou_alice"),
        content=TextContent(text="hi"),
    )
    decision = gate.evaluate(msg)
    assert decision.allowed is False
    assert decision.reason == "policy_dm_not_in_allowlist"
