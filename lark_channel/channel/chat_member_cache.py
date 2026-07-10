"""Per-chat roster cache backing bot-at-bot name resolution.

Consumers: :meth:`FeishuChannel.get_chat_members` (source ``'api'``,
authoritative users), :meth:`FeishuChannel.get_chat_bots` (source ``'api'``,
authoritative bots), inbound-mention collection (source ``'mention'``, can carry
bots), ``sender_name`` resolution and ``@name → open_id`` normalization.

Two safety invariants:

- A display name that maps to more than one open_id is **ambiguous** and
  resolves to ``None`` — never last-writer-wins — so a name collision (e.g. an
  attacker renaming to a real bot's name) can't misroute an ``@``. An ``'api'``
  name→open_id is never silently replaced by a ``'mention'`` one.
- Entries expire after a TTL and the number of cached chats is capped, so stale
  or poisoned mappings don't linger.

Clock, TTL and capacity are injectable so time and eviction are deterministic in
tests. Modelled on :class:`~lark_channel.channel.chat_mode.ChatModeCache`.
"""

import time
from collections import OrderedDict
from dataclasses import dataclass, field
from typing import Callable, Dict, List, Optional

from .types import ChatMember

MemberSource = str  # "api" | "mention"

# Sentinel for a display name shared by more than one open_id — unresolvable.
_AMBIGUOUS = object()

DEFAULT_TTL_SECONDS = 300.0
DEFAULT_MAX_CHATS = 500
DEFAULT_MAX_ENTRIES_PER_CHAT = 1000


@dataclass
class _Roster:
    # open_id → display name. 'api' names win over 'mention' names for the same open_id.
    by_open_id: Dict[str, str] = field(default_factory=dict)
    # display name → single open_id, or _AMBIGUOUS when the name is shared.
    by_name: Dict[str, object] = field(default_factory=dict)
    # open_id → which source last set its name, so 'api' isn't overwritten by 'mention'.
    name_source: Dict[str, MemberSource] = field(default_factory=dict)
    # Last user list from get_chat_members — served back on a cache hit, with its
    # own fetch timestamp so 'mention' writes don't keep the API cache alive.
    api_members: Optional[List[ChatMember]] = None
    api_fetched_at: Optional[float] = None
    # Last bot list from get_chat_bots — cached separately so it can't clobber api_members.
    api_bots: Optional[List[ChatMember]] = None
    api_bots_fetched_at: Optional[float] = None
    updated_at: float = 0.0


class ChatMemberCache:
    def __init__(
        self,
        *,
        now: Callable[[], float] = time.time,
        ttl_seconds: float = DEFAULT_TTL_SECONDS,
        max_chats: int = DEFAULT_MAX_CHATS,
        max_entries_per_chat: int = DEFAULT_MAX_ENTRIES_PER_CHAT,
    ) -> None:
        self._now = now
        self._ttl = ttl_seconds
        self._max_chats = max_chats
        self._max_entries = max_entries_per_chat
        self._chats: "OrderedDict[str, _Roster]" = OrderedDict()

    # ---- writes --------------------------------------------------------------

    def set_members(
        self, chat_id: str, members: List[ChatMember], source: MemberSource
    ) -> None:
        roster = self._roster_for_write(chat_id)
        self._index_all(roster, members, source)
        if source == "api":
            roster.api_members = list(members)
            roster.api_fetched_at = self._now()
        roster.updated_at = self._now()
        self._store(chat_id, roster)

    def set_bots(self, chat_id: str, bots: List[ChatMember]) -> None:
        """Cache an authoritative bot list (from ``get_chat_bots``) — separate
        from the user list so neither clobbers the other. Indexed as ``'api'``."""
        roster = self._roster_for_write(chat_id)
        self._index_all(roster, bots, "api")
        roster.api_bots = list(bots)
        roster.api_bots_fetched_at = self._now()
        roster.updated_at = self._now()
        self._store(chat_id, roster)

    # ---- reads ---------------------------------------------------------------

    def get_members(self, chat_id: str) -> Optional[List[ChatMember]]:
        """The last API user list, or ``None`` when absent or expired."""
        return self._live_list(chat_id, "members")

    def get_bots(self, chat_id: str) -> Optional[List[ChatMember]]:
        """The last API bot list, or ``None`` when absent or expired."""
        return self._live_list(chat_id, "bots")

    def resolve_name(self, chat_id: str, open_id: str) -> Optional[str]:
        roster = self._live_roster(chat_id)
        return roster.by_open_id.get(open_id) if roster else None

    def resolve_open_id(self, chat_id: str, name: str) -> Optional[str]:
        roster = self._live_roster(chat_id)
        if roster is None:
            return None
        target = roster.by_name.get(name)
        return target if isinstance(target, str) else None

    # ---- internals -----------------------------------------------------------

    def _index_all(
        self, roster: _Roster, members: List[ChatMember], source: MemberSource
    ) -> None:
        for m in members:
            if not m.id or not m.name:
                continue
            self._index_member(roster, m.id, m.name, source)

    def _index_member(
        self, roster: _Roster, open_id: str, name: str, source: MemberSource
    ) -> None:
        # open_id → name: an 'api' name is authoritative; don't let a later
        # 'mention' name overwrite it, but a fresh 'api' name always wins.
        prev_source = roster.name_source.get(open_id)
        prev_name = roster.by_open_id.get(open_id)
        if source == "api" or prev_source != "api":
            roster.by_open_id[open_id] = name
            roster.name_source[open_id] = source
            # On a rename, drop the previous name→open_id entry so the reverse
            # index doesn't accumulate every historical display name — but only
            # when it still uniquely pointed here (leave a shared/ambiguous name).
            if (
                prev_name is not None
                and prev_name != name
                and roster.by_name.get(prev_name) == open_id
            ):
                del roster.by_name[prev_name]

        # name → open_id: a second distinct open_id for the same name makes it
        # ambiguous (unresolvable) regardless of source.
        existing = roster.by_name.get(name)
        if existing is None:
            roster.by_name[name] = open_id
        elif existing != open_id:
            roster.by_name[name] = _AMBIGUOUS

        self._cap_entries(roster)

    def _cap_entries(self, roster: _Roster) -> None:
        _evict_oldest(roster.by_name, self._max_entries)
        _evict_oldest(roster.by_open_id, self._max_entries)
        _evict_oldest(roster.name_source, self._max_entries)

    def _roster_for_write(self, chat_id: str) -> _Roster:
        return self._live_roster(chat_id) or _Roster(updated_at=self._now())

    def _live_roster(self, chat_id: str) -> Optional[_Roster]:
        roster = self._chats.get(chat_id)
        if roster is None:
            return None
        if self._now() - roster.updated_at > self._ttl:
            self._chats.pop(chat_id, None)
            return None
        return roster

    def _live_list(
        self, chat_id: str, kind: str
    ) -> Optional[List[ChatMember]]:
        roster = self._chats.get(chat_id)
        if roster is None:
            return None
        if kind == "members":
            lst, fetched_at = roster.api_members, roster.api_fetched_at
        else:
            lst, fetched_at = roster.api_bots, roster.api_bots_fetched_at
        if lst is None or fetched_at is None:
            return None
        if self._now() - fetched_at > self._ttl:
            return None
        return lst

    def _store(self, chat_id: str, roster: _Roster) -> None:
        # LRU touch + evict oldest chats past the cap.
        self._chats.pop(chat_id, None)
        self._chats[chat_id] = roster
        while len(self._chats) > self._max_chats:
            self._chats.popitem(last=False)


def _evict_oldest(mapping: Dict, max_size: int) -> None:
    """Drop oldest-inserted keys until ``mapping`` is within ``max_size``."""
    while len(mapping) > max_size:
        oldest = next(iter(mapping))
        del mapping[oldest]
