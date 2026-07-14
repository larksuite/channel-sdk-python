"""ChatMemberCache.

Per-chat roster cache (OrderedDict + LRU + TTL, like ChatModeCache). The
security-critical invariant is fail-safe name→open_id resolution: a name that
maps to more than one open_id is ambiguous and resolves to None (never
last-writer-wins), so nobody can hijack an @-mention by renaming.

Clock and capacity are injected so time / eviction are deterministic.
"""

from lark_channel.channel.chat_member_cache import ChatMemberCache
from lark_channel.channel.types import ChatMember


def _member(open_id, name, is_bot=False):
    return ChatMember(id=open_id, name=name, is_bot=is_bot)


def test_bidirectional_resolution_and_unknown_is_none():
    cache = ChatMemberCache()
    cache.set_members("oc_c", [_member("ou_a", "Alice")], source="api")

    assert cache.resolve_name("oc_c", "ou_a") == "Alice"
    assert cache.resolve_open_id("oc_c", "Alice") == "ou_a"
    assert cache.resolve_name("oc_c", "ou_missing") is None
    assert cache.resolve_open_id("oc_c", "Ghost") is None


def test_duplicate_name_is_ambiguous_but_reverse_still_resolves():
    cache = ChatMemberCache()
    cache.set_members(
        "oc_c",
        [_member("ou_a", "Sam"), _member("ou_b", "Sam")],
        source="api",
    )

    # Two open_ids share the name → refuse to guess.
    assert cache.resolve_open_id("oc_c", "Sam") is None
    # But each open_id still maps back to its name.
    assert cache.resolve_name("oc_c", "ou_a") == "Sam"
    assert cache.resolve_name("oc_c", "ou_b") == "Sam"


def test_same_open_id_reobserved_is_not_a_conflict():
    cache = ChatMemberCache()
    cache.set_members("oc_c", [_member("ou_a", "Alice")], source="api")
    # A later inbound mention re-observes the same (open_id, name) pair — this
    # must not be treated as an ambiguity.
    cache.set_members("oc_c", [_member("ou_a", "Alice")], source="mention")

    assert cache.resolve_open_id("oc_c", "Alice") == "ou_a"


def test_mention_does_not_overwrite_api_and_creates_ambiguity():
    cache = ChatMemberCache()
    cache.set_members("oc_c", [_member("ou_api", "Sam")], source="api")
    # Attacker-controlled mention claims the name "Sam" for a different id.
    cache.set_members("oc_c", [_member("ou_mention", "Sam")], source="mention")

    # Conflicting ids for one name → ambiguous (fail-safe), never the mention id.
    assert cache.resolve_open_id("oc_c", "Sam") is None
    # The authoritative api open_id→name mapping is untouched.
    assert cache.resolve_name("oc_c", "ou_api") == "Sam"


def test_ttl_expiry_with_injected_clock():
    now = [1000.0]
    cache = ChatMemberCache(now=lambda: now[0], ttl_seconds=300)
    cache.set_members("oc_c", [_member("ou_a", "Alice")], source="api")
    assert cache.resolve_name("oc_c", "ou_a") == "Alice"

    now[0] += 301  # past the TTL
    assert cache.resolve_name("oc_c", "ou_a") is None
    assert not cache.get_members("oc_c")


def test_api_refresh_drops_departed_member():
    # A full API snapshot replaces the previous one, so a member who left the
    # chat is no longer resolvable (index rebuilt from the fresh snapshot).
    cache = ChatMemberCache()
    cache.set_members("c", [_member("ou_a", "Alice"), _member("ou_b", "Bob")], source="api")
    assert cache.resolve_open_id("c", "Alice") == "ou_a"

    cache.set_members("c", [_member("ou_b", "Bob")], source="api")  # Alice left
    assert cache.resolve_open_id("c", "Alice") is None
    assert cache.resolve_open_id("c", "Bob") == "ou_b"


def test_api_refresh_reevaluates_resolved_ambiguity():
    # Two "Sam"s → ambiguous; after one leaves, the name resolves again.
    cache = ChatMemberCache()
    cache.set_members("c", [_member("ou_a", "Sam"), _member("ou_b", "Sam")], source="api")
    assert cache.resolve_open_id("c", "Sam") is None  # ambiguous

    cache.set_members("c", [_member("ou_a", "Sam")], source="api")  # one Sam left
    assert cache.resolve_open_id("c", "Sam") == "ou_a"


def test_non_open_id_snapshot_does_not_feed_name_index():
    # A user_id-typed snapshot must not populate the name→open_id index (a
    # user_id can't be used in an <at>), and must not satisfy an open_id query.
    cache = ChatMemberCache()
    cache.set_members("c", [_member("u_a", "Alice")], source="api", id_type="user_id")
    assert cache.resolve_open_id("c", "Alice") is None
    assert cache.get_members("c", "user_id") is not None
    assert cache.get_members("c", "open_id") is None


def test_incomplete_snapshot_does_not_feed_name_index():
    cache = ChatMemberCache()
    cache.set_members("c", [_member("ou_a", "Alice")], source="api", complete=False)
    assert cache.resolve_open_id("c", "Alice") is None  # truncated ≠ authoritative


def test_incomplete_refresh_does_not_clobber_complete_snapshot():
    cache = ChatMemberCache()
    cache.set_members("c", [_member("ou_a", "Alice")], source="api", complete=True)
    cache.set_members("c", [_member("ou_a", "Alice")], source="api", complete=False)
    assert cache.resolve_open_id("c", "Alice") == "ou_a"


def test_per_id_type_slots_do_not_overwrite_each_other():
    cache = ChatMemberCache()
    cache.set_members("c", [_member("ou_a", "Alice")], source="api", id_type="open_id")
    cache.set_members("c", [ChatMember(id="u_a", name="Alice")], source="api", id_type="user_id")
    # The user_id snapshot must not erase the open_id name index.
    assert cache.resolve_open_id("c", "Alice") == "ou_a"
    assert cache.get_members("c", "open_id") is not None
    assert cache.get_members("c", "user_id") is not None


def test_full_refresh_drops_stale_mention_observation():
    cache = ChatMemberCache()
    cache.set_members("c", [ChatMember(id="ou_a", name="Alice", is_bot=False)], source="mention")
    assert cache.resolve_open_id("c", "Alice") == "ou_a"
    cache.set_members("c", [_member("ou_b", "Bob")], source="api", complete=True)  # Alice gone
    assert cache.resolve_open_id("c", "Alice") is None


def test_api_rename_supersedes_stale_mention_alias():
    cache = ChatMemberCache()
    cache.set_members("c", [ChatMember(id="ou_a", name="OldName")], source="mention")
    cache.set_members("c", [_member("ou_a", "NewName")], source="api", complete=True)
    assert cache.resolve_open_id("c", "OldName") is None
    assert cache.resolve_open_id("c", "NewName") == "ou_a"


def test_bots_refresh_reconciles_bot_mention():
    cache = ChatMemberCache()
    cache.set_members("c", [ChatMember(id="ou_x", name="GhostBot", is_bot=True)], source="mention")
    assert cache.resolve_open_id("c", "GhostBot") == "ou_x"
    cache.set_bots("c", [ChatMember(id="ou_y", name="RealBot", is_bot=True)], complete=True)
    assert cache.resolve_open_id("c", "GhostBot") is None


def test_reads_return_defensive_copies():
    cache = ChatMemberCache()
    cache.set_members("c", [_member("ou_a", "Alice")], source="api")
    got = cache.get_members("c", "open_id")
    got.clear()
    got[:] = []
    again = cache.get_members("c", "open_id")
    assert [m.id for m in again] == ["ou_a"]  # list mutation didn't affect cache
    again[0].name = "Hacked"
    assert cache.resolve_open_id("c", "Alice") == "ou_a"  # member mutation didn't corrupt index


def test_lru_evicts_oldest_chat_over_max_chats():
    cache = ChatMemberCache(max_chats=2)
    cache.set_members("c1", [_member("ou_1", "One")], source="api")
    cache.set_members("c2", [_member("ou_2", "Two")], source="api")
    cache.set_members("c3", [_member("ou_3", "Three")], source="api")

    assert not cache.get_members("c1")  # oldest evicted
    assert cache.get_members("c3")
