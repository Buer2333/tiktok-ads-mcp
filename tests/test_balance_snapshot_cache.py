"""Tests for tiktok_ads_mcp.cache.balance_snapshot."""

import json
import pytest
from tiktok_ads_mcp.cache.balance_snapshot import BalanceSnapshotCache


@pytest.fixture
def cache(tmp_path):
    return BalanceSnapshotCache(cache_dir=tmp_path)


def test_put_and_get(cache):
    cache.put("111", "2026-03-10", 5000.0, group="Test", ad_name="TestAd")
    result = cache.get("111", "2026-03-10")
    assert result == {"balance": 5000.0, "group": "Test", "ad_name": "TestAd"}


def test_get_missing(cache):
    assert cache.get("999", "2026-01-01") is None


def test_overwrite(cache):
    cache.put("111", "2026-03-10", 5000.0)
    cache.put("111", "2026-03-10", 4500.0)
    result = cache.get("111", "2026-03-10")
    assert result["balance"] == 4500.0


def test_estimate_cost(cache):
    cache.put("111", "2026-03-09", 5000.0)
    cache.put("111", "2026-03-10", 4500.0)
    cost = cache.estimate_cost("111", "2026-03-10")
    assert cost == 500.0


def test_estimate_cost_negative_treated_as_zero(cache):
    """Negative delta (top-up) should return 0."""
    cache.put("111", "2026-03-09", 4000.0)
    cache.put("111", "2026-03-10", 5000.0)  # Balance went up (top-up)
    cost = cache.estimate_cost("111", "2026-03-10")
    assert cost == 0.0


def test_estimate_cost_missing_prev(cache):
    cache.put("111", "2026-03-10", 4500.0)
    assert cache.estimate_cost("111", "2026-03-10") is None


def test_estimate_cost_missing_current(cache):
    cache.put("111", "2026-03-09", 5000.0)
    assert cache.estimate_cost("111", "2026-03-10") is None


def test_clear(cache):
    cache.put("111", "2026-03-10", 5000.0)
    cache.clear()
    assert cache.get("111", "2026-03-10") is None


def test_seed_fallback(tmp_path):
    seed_file = tmp_path / "seed" / "balance_snapshot_seed.json"
    seed_file.parent.mkdir()
    seed_file.write_text(
        json.dumps(
            {
                "111:2026-03-10": {
                    "balance": 3000.0,
                    "group": "Seed",
                    "ad_name": "SeedAd",
                    "snapshot_at": 9999999999,
                }
            }
        )
    )
    cache_dir = tmp_path / "cache"
    cache = BalanceSnapshotCache(cache_dir=cache_dir, seed_file=seed_file)
    result = cache.get("111", "2026-03-10")
    assert result["balance"] == 3000.0


def test_persistence(tmp_path):
    cache1 = BalanceSnapshotCache(cache_dir=tmp_path)
    cache1.put("111", "2026-03-10", 5000.0)

    cache2 = BalanceSnapshotCache(cache_dir=tmp_path)
    result = cache2.get("111", "2026-03-10")
    assert result["balance"] == 5000.0
