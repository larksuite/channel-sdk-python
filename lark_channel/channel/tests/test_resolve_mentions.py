"""@name → open_id normalization pure functions.

New module ``outbound/markdown/resolve_mentions.py``:
  - resolve_name_mentions(mentions, lookup): fill open_id for name-only
    Identities, drop unresolvable ones, pass valid ones through.
  - resolve_mentions_in_text(text, lookup): rewrite ``@name`` to ``<at>`` only
    for names the lookup resolves; unknown / ambiguous stay literal; @ mid-word
    is not a mention; pathological input returns in bounded time (no ReDoS).
  - is_valid_open_id / escape_at_name: the shared <at> hardening primitives.

``lookup(name) -> Optional[str]`` is Map-backed on purpose — roster names are
never compiled into a regex.
"""

import time

from lark_channel.channel.types import Identity
from lark_channel.channel.outbound.markdown.resolve_mentions import (
    escape_at_name,
    is_valid_open_id,
    resolve_mentions_in_text,
    resolve_name_mentions,
)

_LOOKUP = {"Alice": "ou_a"}.get


def test_resolve_name_mentions_fills_open_id_for_name_only():
    out = resolve_name_mentions([Identity(open_id="", display_name="Alice")], _LOOKUP)
    assert len(out) == 1
    assert out[0].open_id == "ou_a"


def test_resolve_name_mentions_drops_unresolvable():
    out = resolve_name_mentions([Identity(open_id="", display_name="Ghost")], _LOOKUP)
    assert out == []


def test_resolve_name_mentions_passes_valid_open_id_through():
    out = resolve_name_mentions([Identity(open_id="ou_x", display_name="X")], _LOOKUP)
    assert len(out) == 1
    assert out[0].open_id == "ou_x"


def test_resolve_text_rewrites_known_name_and_keeps_surrounding_text():
    out = resolve_mentions_in_text("你好 @Alice 请看", _LOOKUP)
    assert '<at user_id="ou_a">' in out
    assert "你好" in out and "请看" in out


def test_resolve_text_leaves_unknown_name_literal():
    out = resolve_mentions_in_text("hi @Ghost there", _LOOKUP)
    assert "<at" not in out
    assert out == "hi @Ghost there"


def test_resolve_text_ignores_at_inside_a_word():
    # ``a@b`` (e.g. an email) must not be treated as a mention even if "b"
    # would resolve.
    out = resolve_mentions_in_text("mail a@b end", {"b": "ou_b"}.get)
    assert "<at" not in out


def test_roster_name_is_not_used_as_a_regex():
    # A roster name with a regex metachar ("a.c") must match literally: it must
    # NOT match "axc" (which "." as a wildcard would).
    out = resolve_mentions_in_text("@axc", {"a.c": "ou_x"}.get)
    assert "<at" not in out


def test_pathological_input_returns_in_bounded_time():
    pathological = "@" + "a" * 50000
    start = time.perf_counter()
    out = resolve_mentions_in_text(pathological, lambda name: None)
    elapsed = time.perf_counter() - start
    assert isinstance(out, str)
    assert elapsed < 2.0


def test_is_valid_open_id_accepts_ou_on_and_all_sentinel():
    assert is_valid_open_id("ou_abc123") is True
    assert is_valid_open_id("on_xyz") is True
    # @all sentinel must be allowed so hardening doesn't drop <at user_id="all">.
    assert is_valid_open_id("all") is True


def test_is_valid_open_id_rejects_cli_and_markup():
    assert is_valid_open_id("cli_bad") is False
    assert is_valid_open_id("<script>") is False
    assert is_valid_open_id("") is False


def test_escape_at_name_strips_angle_brackets_and_quotes():
    escaped = escape_at_name('Ann <b>"x"')
    assert "<" not in escaped
    assert ">" not in escaped
    assert '"' not in escaped


def test_escape_at_name_leaves_benign_name_untouched():
    assert escape_at_name("Alice") == "Alice"
