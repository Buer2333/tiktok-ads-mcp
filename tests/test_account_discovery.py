"""Tests for AccountDiscoveryCache (cache layer only).

Covers the resurrect-watch additions (get_resurrect_candidates /
record_status_check / resurrect) and the put() field-preservation fix
(2026-07-08: fixed-key rebuild used to wipe banned_at and any extension
fields on every rediscovery).
"""

from datetime import date

import pytest

from tiktok_ads_mcp.cache.account_discovery import AccountDiscoveryCache


@pytest.fixture
def cache(tmp_path):
    return AccountDiscoveryCache(tmp_path)


def _seed(cache, adv_id, **overrides):
    """Write a raw entry directly (mimics data-level migrations)."""
    data = cache._load()
    entry = {
        "store_ids": [],
        "ad_type": "gmvmax",
        "ad_name": "seed",
        "discovered_at": "2026-03-01",
        "last_seen": "2026-03-01",
        "banned": False,
    }
    entry.update(overrides)
    data[adv_id] = entry
    cache._save()
    return entry


class TestPutFieldPreservation:
    def test_put_preserves_banned_at(self, cache):
        cache.put("A", store_ids=["s1"], ad_type="gmvmax")
        cache.mark_banned("A")
        assert cache.get("A")["banned_at"]
        cache.put("A", store_ids=["s2"], ad_type="gmvmax")
        assert cache.get("A")["banned_at"], "put() must not wipe banned_at"

    def test_put_preserves_extension_fields(self, cache):
        _seed(cache, "A", api_status="STATUS_ENABLE", status_checked_at="2026-07-01")
        cache.put("A", store_ids=["s1"], ad_type="gmvmax")
        entry = cache.get("A")
        assert entry["api_status"] == "STATUS_ENABLE"
        assert entry["status_checked_at"] == "2026-07-01"

    def test_put_clears_archive_markers_on_gmvmax_flip(self, cache):
        _seed(
            cache,
            "A",
            ad_type="archived_gmvmax",
            archived_at="2026-05-18",
            archive_reason="zombie",
        )
        cache.put("A", store_ids=["s1"], ad_type="gmvmax")
        entry = cache.get("A")
        assert entry["ad_type"] == "gmvmax"
        assert "archived_at" not in entry
        assert "archive_reason" not in entry

    def test_put_keeps_archive_markers_without_flip(self, cache):
        """A non-gmvmax write (e.g. unknown) must not touch archive markers."""
        _seed(cache, "A", ad_type="archived_gmvmax", archived_at="2026-05-18")
        cache.put("A", store_ids=[], ad_type="unknown")
        assert cache.get("A")["archived_at"] == "2026-05-18"

    def test_put_union_semantics_unchanged(self, cache):
        cache.put("A", store_ids=["s1"], ad_type="gmvmax")
        cache.put("A", store_ids=["s2"], ad_type="gmvmax")
        assert cache.get("A")["store_ids"] == ["s1", "s2"]

    def test_put_preserves_discovered_at(self, cache):
        _seed(cache, "A", discovered_at="2026-03-01")
        cache.put("A", store_ids=["s1"], ad_type="gmvmax")
        assert cache.get("A")["discovered_at"] == "2026-03-01"


class TestResurrectCandidates:
    def test_two_pools(self, cache):
        _seed(cache, "archived", ad_type="archived_gmvmax")
        _seed(cache, "retired", ad_type="gmvmax", banned=True)
        _seed(cache, "active", ad_type="gmvmax", banned=False)
        _seed(cache, "unknown", ad_type="unknown")
        candidates = cache.get_resurrect_candidates()
        assert set(candidates) == {"archived", "retired"}

    def test_returns_copies(self, cache):
        _seed(cache, "archived", ad_type="archived_gmvmax")
        candidates = cache.get_resurrect_candidates()
        candidates["archived"]["ad_type"] = "mutated"
        assert cache.get("archived")["ad_type"] == "archived_gmvmax"


class TestRecordStatusCheck:
    def test_writes_status_and_date(self, cache):
        _seed(cache, "A")
        cache.record_status_check("A", "STATUS_LIMIT")
        entry = cache.get("A")
        assert entry["api_status"] == "STATUS_LIMIT"
        assert entry["status_checked_at"] == date.today().isoformat()

    def test_missing_entry_noop(self, cache):
        cache.record_status_check("ghost", "STATUS_ENABLE")
        assert cache.get("ghost") is None


class TestResurrect:
    def test_archived_entry_revives(self, cache):
        _seed(
            cache,
            "A",
            ad_type="archived_gmvmax",
            archived_at="2026-05-18",
            archive_reason="zombie",
            banned_at="2026-04-01",
            store_ids=["old_store"],
        )
        cache.resurrect("A", ["new_store"], "New Name")
        entry = cache.get("A")
        assert entry["ad_type"] == "gmvmax"
        assert entry["banned"] is False
        assert entry["discovered_at"] == date.today().isoformat()
        assert entry["last_seen"] == date.today().isoformat()
        assert entry["ad_name"] == "New Name"
        # union: old binding history is preserved
        assert entry["store_ids"] == ["old_store", "new_store"]
        for stale in ("archived_at", "archive_reason", "banned_at"):
            assert stale not in entry

    def test_resurrected_entry_enters_gmvmax_view(self, cache):
        """After resurrect the entry is visible to get_all_gmvmax →
        enrich_with_discovery picks it up with zero lark-bot changes."""
        _seed(cache, "A", ad_type="archived_gmvmax")
        assert "A" not in cache.get_all_gmvmax()
        cache.resurrect("A", ["s1"])
        assert "A" in cache.get_all_gmvmax()
        assert cache.get_active_for_store("s1") == ["A"]
