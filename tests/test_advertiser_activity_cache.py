"""Tests for tiktok_ads_mcp.cache.advertiser_activity.

Cache backs the Active Roster Filter (F0). Triple key
(advertiser_id, store_id, ad_type) mirrors ad_cost cache.
"""

import json
import pytest
from tiktok_ads_mcp.cache.advertiser_activity import (
    AdvertiserActivityCache,
    _build_key,
)


STORE_A = "1000000000000000001"
STORE_B = "1000000000000000002"


@pytest.fixture
def cache(tmp_path):
    return AdvertiserActivityCache(cache_dir=tmp_path)


def test_build_key_gmvmax_requires_store_id():
    with pytest.raises(ValueError, match="store_id"):
        _build_key("adv1", "", "gmvmax")


def test_build_key_ads_allows_empty_store_id():
    assert _build_key("adv1", "", "ads") == "adv1::ads"


def test_build_key_lowercases_ad_type():
    assert _build_key("adv1", STORE_A, "GMVMAX") == f"adv1:{STORE_A}:gmvmax"


def test_record_probe_with_spend_sets_last_spend_date(cache):
    cache.record_probe("adv1", STORE_A, "gmvmax", "2026-05-10", 100.0)
    entry = cache.get("adv1", STORE_A, "gmvmax")
    assert entry["last_spend_date"] == "2026-05-10"
    assert entry["last_probe_date"] == "2026-05-10"
    assert entry["last_probe_cost"] == 100.0


def test_record_probe_zero_cost_does_not_set_last_spend(cache):
    cache.record_probe("adv1", STORE_A, "gmvmax", "2026-05-10", 0.0)
    entry = cache.get("adv1", STORE_A, "gmvmax")
    assert entry["last_spend_date"] == ""
    assert entry["last_probe_date"] == "2026-05-10"


def test_record_probe_monotonic_last_spend(cache):
    cache.record_probe("adv1", STORE_A, "gmvmax", "2026-05-10", 100.0)
    cache.record_probe("adv1", STORE_A, "gmvmax", "2026-05-05", 50.0)  # older
    entry = cache.get("adv1", STORE_A, "gmvmax")
    # last_spend_date stays 2026-05-10, not regressed
    assert entry["last_spend_date"] == "2026-05-10"


def test_record_probe_advances_last_spend_on_newer_date(cache):
    cache.record_probe("adv1", STORE_A, "gmvmax", "2026-05-05", 50.0)
    cache.record_probe("adv1", STORE_A, "gmvmax", "2026-05-10", 100.0)
    entry = cache.get("adv1", STORE_A, "gmvmax")
    assert entry["last_spend_date"] == "2026-05-10"


def test_record_probe_zero_after_spend_preserves_last_spend(cache):
    cache.record_probe("adv1", STORE_A, "gmvmax", "2026-05-10", 100.0)
    cache.record_probe("adv1", STORE_A, "gmvmax", "2026-05-12", 0.0)
    entry = cache.get("adv1", STORE_A, "gmvmax")
    assert entry["last_spend_date"] == "2026-05-10"
    # but last_probe advances
    assert entry["last_probe_date"] == "2026-05-12"
    assert entry["last_probe_cost"] == 0.0


def test_days_since_last_spend_basic(cache):
    cache.record_probe("adv1", STORE_A, "gmvmax", "2026-05-05", 100.0)
    assert cache.days_since_last_spend("adv1", STORE_A, "gmvmax", "2026-05-12") == 7


def test_days_since_last_spend_zero_when_same_day(cache):
    cache.record_probe("adv1", STORE_A, "gmvmax", "2026-05-12", 100.0)
    assert cache.days_since_last_spend("adv1", STORE_A, "gmvmax", "2026-05-12") == 0


def test_days_since_last_spend_no_record_returns_none(cache):
    assert cache.days_since_last_spend("never", STORE_A, "gmvmax", "2026-05-12") is None


def test_days_since_last_spend_only_zero_probe_returns_none(cache):
    # probe with cost=0 leaves last_spend_date="" → treated as no record
    cache.record_probe("adv1", STORE_A, "gmvmax", "2026-05-10", 0.0)
    assert cache.days_since_last_spend("adv1", STORE_A, "gmvmax", "2026-05-12") is None


def test_cross_store_keys_decay_independently(cache):
    cache.record_probe("adv1", STORE_A, "gmvmax", "2026-05-10", 100.0)
    cache.record_probe("adv1", STORE_B, "gmvmax", "2026-05-01", 50.0)
    assert cache.get("adv1", STORE_A, "gmvmax")["last_spend_date"] == "2026-05-10"
    assert cache.get("adv1", STORE_B, "gmvmax")["last_spend_date"] == "2026-05-01"


def test_ads_type_uses_empty_store_id(cache):
    cache.record_probe("adv1", "", "ads", "2026-05-10", 100.0)
    assert cache.get("adv1", "", "ads")["last_spend_date"] == "2026-05-10"


def test_get_returns_none_for_unknown_key(cache):
    assert cache.get("never", STORE_A, "gmvmax") is None


def test_persistence_round_trip(tmp_path):
    c1 = AdvertiserActivityCache(cache_dir=tmp_path)
    c1.record_probe("adv1", STORE_A, "gmvmax", "2026-05-10", 100.0)
    # new instance reads from disk
    c2 = AdvertiserActivityCache(cache_dir=tmp_path)
    assert c2.get("adv1", STORE_A, "gmvmax")["last_spend_date"] == "2026-05-10"


def test_corrupted_json_treated_as_empty(tmp_path):
    cache_file = tmp_path / "advertiser_activity.json"
    cache_file.write_text("{not valid json}")
    c = AdvertiserActivityCache(cache_dir=tmp_path)
    assert c.get("adv1", STORE_A, "gmvmax") is None
    # and we can still write to it
    c.record_probe("adv1", STORE_A, "gmvmax", "2026-05-10", 100.0)
    assert c.get("adv1", STORE_A, "gmvmax")["last_spend_date"] == "2026-05-10"


def test_seed_file_overlay(tmp_path):
    seed_file = tmp_path / "seed" / "activity_seed.json"
    seed_file.parent.mkdir()
    seed_file.write_text(
        json.dumps(
            {
                f"adv1:{STORE_A}:gmvmax": {
                    "last_spend_date": "2026-04-01",
                    "last_probe_date": "2026-04-01",
                    "last_probe_cost": 50.0,
                    "updated_at": "2026-04-01T00:00:00",
                }
            }
        )
    )
    cache_dir = tmp_path / "cache"
    c = AdvertiserActivityCache(cache_dir=cache_dir, seed_file=seed_file)
    assert c.get("adv1", STORE_A, "gmvmax")["last_spend_date"] == "2026-04-01"


def test_seed_file_overridden_by_local_cache(tmp_path):
    seed_file = tmp_path / "seed" / "activity_seed.json"
    seed_file.parent.mkdir()
    seed_file.write_text(
        json.dumps(
            {
                f"adv1:{STORE_A}:gmvmax": {
                    "last_spend_date": "2026-04-01",
                    "last_probe_date": "2026-04-01",
                    "last_probe_cost": 50.0,
                    "updated_at": "2026-04-01T00:00:00",
                }
            }
        )
    )
    cache_dir = tmp_path / "cache"
    cache_dir.mkdir()
    (cache_dir / "advertiser_activity.json").write_text(
        json.dumps(
            {
                f"adv1:{STORE_A}:gmvmax": {
                    "last_spend_date": "2026-05-10",
                    "last_probe_date": "2026-05-10",
                    "last_probe_cost": 100.0,
                    "updated_at": "2026-05-10T00:00:00",
                }
            }
        )
    )
    c = AdvertiserActivityCache(cache_dir=cache_dir, seed_file=seed_file)
    # Local cache wins
    assert c.get("adv1", STORE_A, "gmvmax")["last_spend_date"] == "2026-05-10"


def test_seed_last_spend_only_advances(cache):
    cache.record_probe("adv1", STORE_A, "gmvmax", "2026-05-10", 100.0)
    cache.seed_last_spend("adv1", STORE_A, "gmvmax", "2026-05-05")  # older
    assert cache.get("adv1", STORE_A, "gmvmax")["last_spend_date"] == "2026-05-10"
    cache.seed_last_spend("adv1", STORE_A, "gmvmax", "2026-05-15")  # newer
    assert cache.get("adv1", STORE_A, "gmvmax")["last_spend_date"] == "2026-05-15"


def test_seed_last_spend_creates_entry_when_missing(cache):
    cache.seed_last_spend("adv1", STORE_A, "gmvmax", "2026-05-01")
    entry = cache.get("adv1", STORE_A, "gmvmax")
    assert entry["last_spend_date"] == "2026-05-01"
    assert entry["last_probe_date"] == ""


def test_clear_removes_all(cache):
    cache.record_probe("adv1", STORE_A, "gmvmax", "2026-05-10", 100.0)
    cache.clear()
    assert cache.get("adv1", STORE_A, "gmvmax") is None


def test_all_keys_lists_all(cache):
    cache.record_probe("adv1", STORE_A, "gmvmax", "2026-05-10", 100.0)
    cache.record_probe("adv2", "", "ads", "2026-05-10", 50.0)
    keys = cache.all_keys()
    assert f"adv1:{STORE_A}:gmvmax" in keys
    assert "adv2::ads" in keys
