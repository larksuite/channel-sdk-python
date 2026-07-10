"""``@name → open_id`` normalization + ``<at>`` sink hardening.

Two pure resolvers back the outbound path:

- :func:`resolve_name_mentions` — fill ``open_id`` on name-only structured
  mentions via a roster lookup; drop the ones that don't resolve; keep entries
  that already carry an open_id.
- :func:`resolve_mentions_in_text` — rewrite plaintext ``@<name>`` tokens into
  real ``<at>`` tags when the name resolves; leave unknown / ambiguous ``@xxx``
  untouched (syntax fallback). Matching is Map-lookup based and never compiles a
  roster name into a regex, so it stays linear on pathological input (no ReDoS).

Plus the shared ``<at>`` hardening primitives :func:`is_valid_open_id` and
:func:`escape_at_name`, applied wherever a display name reaches an ``<at>`` sink.

``lookup(name) -> Optional[str]`` returns an open_id, or ``None`` for an unknown
or ambiguous name.
"""

import re
from typing import Callable, List, Optional

from ...types import Identity

MentionLookup = Callable[[str], Optional[str]]

# A well-formed Feishu open_id / union_id for an ``<at user_id="…">``.
_OPEN_ID_RE = re.compile(r"^(ou_|on_)[A-Za-z0-9_-]+$")


def is_valid_open_id(open_id: Optional[str]) -> bool:
    """Whether ``open_id`` is safe to drop into ``<at user_id="…">``.

    Accepts ``ou_…`` / ``on_…`` ids, plus the everyone-sentinel ``"all"`` — the
    SDK renders ``@all`` as ``<at user_id="all">``, so the hardening must not
    drop it. Rejects app ids (``cli_…``), markup, and empty values.
    """
    if not open_id:
        return False
    if open_id == "all":
        return True
    return bool(_OPEN_ID_RE.match(open_id))


def escape_at_name(name: str) -> str:
    """Neutralize a display name before it lands in an ``<at>`` tag body.

    Display names are attacker-influenced (any group member sets their own) and
    Feishu renders the ``<at>`` sink without escaping — a name containing ``<``,
    ``>`` or ``"`` could inject a second, forged mention in the bot's own voice.
    Strip those characters (rather than HTML-encode, which Feishu would render
    literally in the plain-text tag body).
    """
    return re.sub(r'[<>"]', "", name or "")


def resolve_name_mentions(
    mentions: List[Identity], lookup: MentionLookup
) -> List[Identity]:
    """Fill ``open_id`` on name-only structured mentions from the roster.

    Entries that already carry an open_id pass through untouched; name-only
    entries that don't resolve (unknown / ambiguous) are dropped rather than
    sent with a wrong or missing id.
    """
    out: List[Identity] = []
    for m in mentions:
        if m.open_id:
            out.append(m)
            continue
        if not m.display_name:
            continue
        open_id = lookup(m.display_name)
        if open_id:
            out.append(
                Identity(
                    open_id=open_id,
                    union_id=m.union_id,
                    user_id=m.user_id,
                    display_name=m.display_name,
                    is_bot=m.is_bot,
                    sender_type=m.sender_type,
                )
            )
    return out


# Longest candidate name to try after an ``@``: bounded words and characters so
# the scan stays linear on hostile input — a 50k-char token yields one bounded
# window, not a quadratic prefix walk.
_MAX_NAME_WORDS = 5
_MAX_NAME_CHARS = 64
_TRAILING_PUNCTUATION = re.compile(r"[.,!?;:)\]}]+$")


def resolve_mentions_in_text(text: str, lookup: MentionLookup) -> str:
    """Rewrite plaintext ``@<name>`` tokens into real ``<at>`` tags when the
    name resolves against the roster. Unknown or ambiguous names are left
    verbatim (syntax fallback). Longest-match-first (so ``@John Smith`` resolves
    ahead of ``@John``), using per-candidate Map lookups rather than a
    roster-name-derived regex, so it stays linear even on pathological input.
    """
    if "@" not in text:
        return text
    out: List[str] = []
    i = 0
    n = len(text)
    while i < n:
        at = text.find("@", i)
        if at == -1:
            out.append(text[i:])
            break
        out.append(text[i:at])
        # An ``@`` mid-word (e.g. inside an email ``a@b``) is not a mention.
        starts_token = at == 0 or text[at - 1].isspace()
        match = _match_name_at(text, at + 1, lookup) if starts_token else None
        if match is not None:
            name, open_id, length = match
            out.append(f'<at user_id="{open_id}">{escape_at_name(name)}</at>')
            i = at + 1 + length
        else:
            out.append("@")
            i = at + 1
    return "".join(out)


def _match_name_at(text: str, start: int, lookup: MentionLookup):
    """Longest resolvable name starting at ``start``, or ``None``."""
    window = text[start : start + _MAX_NAME_CHARS]
    for candidate in _candidate_prefixes(window):
        direct = lookup(candidate)
        if is_valid_open_id(direct):
            return candidate, direct, len(candidate)
        trimmed = _TRAILING_PUNCTUATION.sub("", candidate)
        if trimmed != candidate:
            t = lookup(trimmed)
            if is_valid_open_id(t):
                return trimmed, t, len(trimmed)
    return None


def _candidate_prefixes(window: str) -> List[str]:
    """Word-boundary prefixes of ``window``, longest first (up to MAX_NAME_WORDS)."""
    if not window or window[0].isspace():
        return []
    word_ends: List[int] = []
    in_word = False
    for k, ch in enumerate(window):
        if not ch.isspace():
            in_word = True
        elif in_word:
            word_ends.append(k)
            in_word = False
            if len(word_ends) >= _MAX_NAME_WORDS:
                break
    if in_word and len(word_ends) < _MAX_NAME_WORDS:
        word_ends.append(len(window))
    return [window[:end] for end in reversed(word_ends)]
