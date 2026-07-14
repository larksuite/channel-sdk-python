"""Per-chat roster cache backing bot-at-bot name resolution.

Consumers: `get_chat_members` (source ``'api'`` users, per ``id_type``),
`get_chat_bots` (source ``'api'`` bots), inbound-mention collection (source
``'mention'``, short-lived, can carry bots), ``sender_name`` resolution and
``@name → open_id`` normalization.

Design (rebuilt-from-sources, thread-safe):

- Each chat keeps authoritative **users** snapshots **keyed by id_type**, one
  **bots** snapshot, plus short-lived **mention** observations. The name↔open_id
  indices are rebuilt from these live sources on every read, so a full API
  refresh that drops a member (or resolves a name collision) is reflected
  immediately.
- Only a **complete**, ``open_id``-typed users snapshot (and the bots snapshot)
  feeds the ``name → open_id`` index — a truncated roster is never treated as an
  authoritative full membership, and a ``user_id``/``union_id`` snapshot never
  contributes (its ids aren't usable in an ``<at>``). ``get_members`` is keyed
  by ``id_type`` so a ``user_id`` query never returns an ``open_id`` snapshot.
- On a complete API refresh the matching-category mention observations that the
  snapshot doesn't contain are dropped (a departed/renamed member can't be
  resurrected by a stale observation), and any observation for an open_id the
  API already knows is ignored (the API name/mapping is authoritative).

Two safety invariants: a name mapping to more than one open_id is **ambiguous**
→ ``None`` (never mis-@); snapshots/observations expire after a TTL and the
number of chats is capped. Reads return defensive copies so callers can't mutate
the cached fact source. Clock/TTL/capacity are injectable for deterministic tests.
"""

import threading
import time
from collections import OrderedDict
from dataclasses import dataclass, field, replace
from typing import Callable, Dict, List, Optional, Tuple

from .types import ChatMember

MemberSource = str  # "api" | "mention"

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
    api_users: Dict[str, _Snapshot] = field(default_factory=dict)  # keyed by id_type
    api_bots: Optional[_Snapshot] = None
    # open_id -> (name, observed_at, is_bot); short-lived, never overrides api.
    mentions: "OrderedDict[str, Tuple[str, float, bool]]" = field(default_factory=OrderedDict)
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
        self._mention_ttl = mention_ttl_seconds if mention_ttl_seconds is not None else ttl_seconds
        self._max_chats = max_chats
        self._max_entries = max_entries_per_chat
        self._chats: "OrderedDict[str, _Roster]" = OrderedDict()
        # Guards the whole read-modify-write of a roster so concurrent inbound
        # (background loop) and public API / send calls (possibly other threads)
        # can't lose updates or read a half-rebuilt index.
        self._lock = threading.Lock()

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
        with self._lock:
            roster = self._roster_for_write(chat_id)
            now = self._now()
            if source == "api":
                prev = roster.api_users.get(id_type)
                # Don't let a truncated refresh clobber a complete snapshot.
                if complete or prev is None or not prev.complete:
                    roster.api_users[id_type] = _Snapshot(list(members), id_type, complete, now)
                if complete and id_type == "open_id":
                    self._reconcile_mentions(roster, {m.id for m in members}, want_bot=False)
            else:
                for m in members:
                    if not m.id or not m.name:
                        continue
                    roster.mentions.pop(m.id, None)
                    roster.mentions[m.id] = (m.name, now, bool(m.is_bot))
                self._cap(roster.mentions)
            roster.updated_at = now
            self._store(chat_id, roster)

    def set_bots(self, chat_id: str, bots: List[ChatMember], *, complete: bool = True) -> None:
        with self._lock:
            roster = self._roster_for_write(chat_id)
            now = self._now()
            roster.api_bots = _Snapshot(list(bots), "open_id", complete, now)
            if complete:
                self._reconcile_mentions(roster, {b.id for b in bots}, want_bot=True)
            roster.updated_at = now
            self._store(chat_id, roster)

    def _reconcile_mentions(self, roster: _Roster, snapshot_ids: set, want_bot: bool) -> None:
        """Drop same-category mention observations the authoritative snapshot
        doesn't contain (they left / were renamed), so a complete refresh can
        negate stale observations instead of them being resurrected."""
        for open_id in list(roster.mentions):
            _name, _ts, is_bot = roster.mentions[open_id]
            if is_bot == want_bot and open_id not in snapshot_ids:
                del roster.mentions[open_id]

    # ---- reads ---------------------------------------------------------------

    def get_members(self, chat_id: str, id_type: str = "open_id") -> Optional[List[ChatMember]]:
        with self._lock:
            snap = self._live_users(chat_id, id_type)
            return [replace(m) for m in snap.members] if snap is not None else None

    def get_bots(self, chat_id: str) -> Optional[List[ChatMember]]:
        with self._lock:
            snap = self._live_bots(chat_id)
            return [replace(m) for m in snap.members] if snap is not None else None

    def resolve_name(self, chat_id: str, open_id: str) -> Optional[str]:
        with self._lock:
            by_open_id, _ = self._index(chat_id)
            return by_open_id.get(open_id)

    def resolve_open_id(self, chat_id: str, name: str) -> Optional[str]:
        with self._lock:
            _, by_name = self._index(chat_id)
            target = by_name.get(name)
            return target if isinstance(target, str) else None

    # ---- internals (call under self._lock) -----------------------------------

    def _index(self, chat_id: str) -> Tuple[Dict[str, str], Dict[str, object]]:
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
        # Only a complete, open_id-typed users snapshot feeds the name index.
        su = roster.api_users.get("open_id")
        if su and su.complete and now - su.fetched_at <= self._ttl:
            for m in su.members:
                if m.id and m.name:
                    register(m.id, m.name, True)
        sb = roster.api_bots
        if sb and sb.complete and now - sb.fetched_at <= self._ttl:
            for m in sb.members:
                if m.id and m.name:
                    register(m.id, m.name, True)
        # Short-lived mention observations: ignore any open_id the API already
        # knows (api authoritative), and expire stale ones.
        for open_id, (name, ts, _is_bot) in list(roster.mentions.items()):
            if now - ts > self._mention_ttl:
                roster.mentions.pop(open_id, None)
                continue
            if open_id in api_ids:
                continue
            register(open_id, name, False)
        return by_open_id, by_name

    def _live_users(self, chat_id: str, id_type: str) -> Optional[_Snapshot]:
        roster = self._live_roster(chat_id)
        if roster is None:
            return None
        snap = roster.api_users.get(id_type)
        if snap is None or self._now() - snap.fetched_at > self._ttl:
            return None
        return snap

    def _live_bots(self, chat_id: str) -> Optional[_Snapshot]:
        roster = self._live_roster(chat_id)
        if roster is None or roster.api_bots is None:
            return None
        if self._now() - roster.api_bots.fetched_at > self._ttl:
            return None
        return roster.api_bots

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
