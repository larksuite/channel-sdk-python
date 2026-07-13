"""Per-chat roster cache backing bot-at-bot name resolution.

Consumers: `get_chat_members` (source ``'api'``, authoritative users),
`get_chat_bots` (source ``'api'`` bots), inbound-mention collection (source
``'mention'``, short-lived, can carry bots), ``sender_name`` resolution and
``@name → open_id`` normalization.

Design (rebuilt-from-sources):

- Each chat keeps at most one authoritative **users** snapshot and one **bots**
  snapshot (from the API), plus short-lived **mention** observations. The
  name↔open_id indices are rebuilt from these live sources on every read, so a
  full API refresh that drops a member (or resolves a name collision) is
  reflected immediately — no stale merge.
- The users snapshot records the ``id_type`` it was fetched with and whether it
  was ``complete``. Only ``open_id``-typed members feed the name→open_id index
  (a ``user_id``/``union_id`` is not usable in an ``<at>``), and ``get_members``
  is keyed by ``id_type`` so a ``user_id`` query never satisfies an ``open_id``
  one.

Two safety invariants:

- A display name mapping to more than one open_id is **ambiguous** and resolves
  to ``None`` — never last-writer-wins. An ``'api'`` name→open_id is never
  overwritten by a ``'mention'`` one.
- Snapshots/observations expire after a TTL and the number of cached chats is
  capped, so stale or poisoned mappings don't linger.

Clock, TTL and capacity are injectable for deterministic tests. Modelled on
:class:`~lark_channel.channel.chat_mode.ChatModeCache`.
"""

import time
from collections import OrderedDict
from dataclasses import dataclass, field
from typing import Callable, Dict, List, Optional, Tuple

from .types import ChatMember

MemberSource = str  # "api" | "mention"

# Sentinel for a display name shared by more than one open_id — unresolvable.
_AMBIGUOUS = object()

DEFAULT_TTL_SECONDS = 300.0
DEFAULT_MAX_CHATS = 500
DEFAULT_MAX_ENTRIES_PER_CHAT = 1000


@dataclass
class _Snapshot:
    members: List[ChatMember]
    id_type: str
    complete: bool
    fetched_at: float


@dataclass
class _Roster:
    api_users: Optional[_Snapshot] = None
    api_bots: Optional[_Snapshot] = None
    # open_id -> (name, observed_at); short-lived, never overrides an api name.
    mentions: "OrderedDict[str, Tuple[str, float]]" = field(default_factory=OrderedDict)
    updated_at: float = 0.0


class ChatMemberCache:
    def __init__(
        self,
        *,
        now: Callable[[], float] = time.time,
        ttl_seconds: float = DEFAULT_TTL_SECONDS,
        max_chats: int = DEFAULT_MAX_CHATS,
        max_entries_per_chat: int = DEFAULT_MAX_ENTRIES_PER_CHAT,
        mention_ttl_seconds: Optional[float] = None,
    ) -> None:
        self._now = now
        self._ttl = ttl_seconds
        # Mention observations are best-effort and attacker-influenceable, so
        # they get their own (by default equal, optionally shorter) TTL and
        # never extend the authoritative API snapshot's lifetime.
        self._mention_ttl = mention_ttl_seconds if mention_ttl_seconds is not None else ttl_seconds
        self._max_chats = max_chats
        self._max_entries = max_entries_per_chat
        self._chats: "OrderedDict[str, _Roster]" = OrderedDict()

    # ---- writes --------------------------------------------------------------

    def set_members(
        self,
        chat_id: str,
        members: List[ChatMember],
        source: MemberSource,
        *,
        id_type: str = "open_id",
        complete: bool = True,
    ) -> None:
        roster = self._roster_for_write(chat_id)
        now = self._now()
        if source == "api":
            # Authoritative snapshot: replaces the previous users snapshot, so a
            # departed member / resolved collision is dropped on the next read.
            roster.api_users = _Snapshot(list(members), id_type, complete, now)
        else:
            for m in members:
                if not m.id or not m.name:
                    continue
                roster.mentions.pop(m.id, None)
                roster.mentions[m.id] = (m.name, now)
            self._cap(roster.mentions)
        roster.updated_at = now
        self._store(chat_id, roster)

    def set_bots(self, chat_id: str, bots: List[ChatMember], *, complete: bool = True) -> None:
        """Cache an authoritative bot list (from ``get_chat_bots``) — separate
        from the user snapshot so neither clobbers the other."""
        roster = self._roster_for_write(chat_id)
        now = self._now()
        roster.api_bots = _Snapshot(list(bots), "open_id", complete, now)
        roster.updated_at = now
        self._store(chat_id, roster)

    # ---- reads ---------------------------------------------------------------

    def get_members(self, chat_id: str, id_type: str = "open_id") -> Optional[List[ChatMember]]:
        """The last API user list for ``id_type``, or ``None`` when absent,
        fetched with a different ``id_type``, or expired."""
        snap = self._live_snapshot(chat_id, "users")
        if snap is None or snap.id_type != id_type:
            return None
        return snap.members

    def get_bots(self, chat_id: str) -> Optional[List[ChatMember]]:
        snap = self._live_snapshot(chat_id, "bots")
        return snap.members if snap is not None else None

    def resolve_name(self, chat_id: str, open_id: str) -> Optional[str]:
        by_open_id, _ = self._index(chat_id)
        return by_open_id.get(open_id)

    def resolve_open_id(self, chat_id: str, name: str) -> Optional[str]:
        _, by_name = self._index(chat_id)
        target = by_name.get(name)
        return target if isinstance(target, str) else None

    # ---- internals -----------------------------------------------------------

    def _index(self, chat_id: str) -> Tuple[Dict[str, str], Dict[str, object]]:
        """Rebuild the name↔open_id indices from the chat's currently-live
        sources. API sources (open_id-typed users + bots) are authoritative;
        mention observations only add non-conflicting entries."""
        roster = self._live_roster(chat_id)
        by_open_id: Dict[str, str] = {}
        by_name: Dict[str, object] = {}
        api_ids: set = set()
        if roster is None:
            return by_open_id, by_name

        def register(open_id: str, name: str, is_api: bool) -> None:
            if not open_id or not name:
                return
            prev_name = by_open_id.get(open_id)
            if is_api or open_id not in by_open_id:
                if (
                    prev_name is not None
                    and prev_name != name
                    and by_name.get(prev_name) == open_id
                ):
                    del by_name[prev_name]
                by_open_id[open_id] = name
                if is_api:
                    api_ids.add(open_id)
            existing = by_name.get(name)
            if existing is None:
                by_name[name] = open_id
            elif existing != open_id:
                by_name[name] = _AMBIGUOUS

        now = self._now()
        # Pass 1: authoritative API members. Only open_id-typed user snapshots
        # feed the index (a user_id/union_id can't be used in an <at>); bots are
        # always open_id.
        if roster.api_users and roster.api_users.id_type == "open_id":
            if now - roster.api_users.fetched_at <= self._ttl:
                for m in roster.api_users.members:
                    if m.id and m.name:
                        register(m.id, m.name, True)
        if roster.api_bots and now - roster.api_bots.fetched_at <= self._ttl:
            for m in roster.api_bots.members:
                if m.id and m.name:
                    register(m.id, m.name, True)
        # Pass 2: short-lived mention observations (never overwrite an api name).
        for open_id, (name, ts) in list(roster.mentions.items()):
            if now - ts > self._mention_ttl:
                roster.mentions.pop(open_id, None)
                continue
            register(open_id, name, False)
        return by_open_id, by_name

    def _live_snapshot(self, chat_id: str, kind: str) -> Optional[_Snapshot]:
        roster = self._live_roster(chat_id)
        if roster is None:
            return None
        snap = roster.api_users if kind == "users" else roster.api_bots
        if snap is None:
            return None
        if self._now() - snap.fetched_at > self._ttl:
            return None
        return snap

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

    def _store(self, chat_id: str, roster: _Roster) -> None:
        self._chats.pop(chat_id, None)
        self._chats[chat_id] = roster
        while len(self._chats) > self._max_chats:
            self._chats.popitem(last=False)

    def _cap(self, mapping: "OrderedDict") -> None:
        while len(mapping) > self._max_entries:
            mapping.popitem(last=False)
