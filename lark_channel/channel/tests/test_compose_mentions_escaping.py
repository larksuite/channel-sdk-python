"""<at> sink hardening on outbound composition.

``display_name`` is attacker-controllable and the Feishu ``<at>`` sink does not
escape it. The text builder and the post AST builder must both run every
mention through ``escape_at_name`` + ``is_valid_open_id`` so a malicious name
can't inject a second ``<at>`` / spoof a user_id, and an invalid open_id
(``cli_`` / markup) is skipped. The @all sentinel (``open_id == "all"``) must
survive the hardening.
"""

import json

from lark_channel.channel.types import Identity, OutboundText
from lark_channel.channel.outbound.sender import _build_text
from lark_channel.channel.outbound.markdown.to_post import markdown_to_post_ast


def _text_of(built):
    return json.loads(built["content"])["text"]


def _at_nodes(ast):
    nodes = []
    for locale in ast.values():
        for row in locale.get("content", []):
            for el in row:
                if isinstance(el, dict) and el.get("tag") == "at":
                    nodes.append(el)
    return nodes


def test_malicious_display_name_cannot_inject_second_at_tag():
    evil = Identity(open_id="ou_a", display_name='</at><at user_id="ou_evil">')
    content = _text_of(_build_text(OutboundText(text="hi", mentions=[evil])))

    # The security property is that no SECOND parseable <at> tag and no forged
    # user_id can be injected — the only <at> is the legitimate wrapper, and the
    # attacker's `user_id="ou_evil"` never survives as markup. (After stripping
    # `<>"` the escaped name may still contain the inert substring "ou_evil" as
    # plain text inside the legit tag body, which is harmless.)
    assert content.count("<at ") == 1
    assert 'user_id="ou_evil"' not in content


def test_invalid_open_id_mention_is_skipped_in_text():
    for bad in ("cli_x", "<script>"):
        content = _text_of(
            _build_text(OutboundText(text="hi", mentions=[Identity(open_id=bad, display_name="X")]))
        )
        assert "<at" not in content


def test_legit_mention_is_unchanged_in_text():
    content = _text_of(
        _build_text(OutboundText(text="hi", mentions=[Identity(open_id="ou_a", display_name="Alice")]))
    )
    assert '<at user_id="ou_a">Alice</at>' in content


def test_at_all_sentinel_survives_hardening_in_text():
    content = _text_of(
        _build_text(OutboundText(text="hi", mentions=[Identity(open_id="all", display_name="所有人")]))
    )
    assert '<at user_id="all">' in content


def test_invalid_open_id_mention_is_skipped_in_post_ast():
    ast = markdown_to_post_ast("hi", mentions=[Identity(open_id="cli_x", display_name="X")])
    assert all(node.get("user_id") != "cli_x" for node in _at_nodes(ast))
