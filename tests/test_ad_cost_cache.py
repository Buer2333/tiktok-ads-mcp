"""Tests for tiktok_ads_mcp.cache.ad_cost."""

import json
import pytest
from tiktok_ads_mcp.cache.ad_cost import AdCostCache


@pytest.fixture
def cache(tmp_path):
    return AdCostCache(cache_dir=tmp_path)


@pytest.fixture
def seeded_cache(tmp_path):
    seed_file = tmp_path / "seed" / "ad_cost_seed.json"
    seed_file.parent.mkdir()
    seed_file.write_text(
        json.dumps(
            {
                "111:2026-03-10:gmvmax": {
                    "cost": 50.0,
                    "gmv": 100.0,
                    "orders": 5,
                    "cached_at": 9999999999,
                }
            }
        )
    )
    cache_dir = tmp_path / "cache"
    return AdCostCache(cache_dir=cache_dir, seed_file=seed_file)


def test_put_and_get_daily(cache):
    cache.put_daily("111", "2026-03-10", "gmvmax", 100.0, 300.0, 10)
    result = cache.get_daily("111", "2026-03-10", "gmvmax")
    assert result == {"cost": 100.0, "gmv": 300.0, "orders": 10}


def test_get_daily_missing(cache):
    assert cache.get_daily("999", "2026-01-01", "ads") is None


def test_get_range_complete(cache):
    cache.put_daily("111", "2026-03-01", "gmvmax", 10.0, 30.0, 1)
    cache.put_daily("111", "2026-03-02", "gmvmax", 20.0, 60.0, 2)
    cache.put_daily("111", "2026-03-03", "gmvmax", 30.0, 90.0, 3)
    result = cache.get_range("111", "2026-03-01", "2026-03-03", "gmvmax")
    assert result == {"cost": 60.0, "gmv": 180.0, "orders": 6}


def test_get_range_incomplete_returns_none(cache):
    cache.put_daily("111", "2026-03-01", "gmvmax", 10.0, 30.0, 1)
    # Missing 03-02
    cache.put_daily("111", "2026-03-03", "gmvmax", 30.0, 90.0, 3)
    assert cache.get_range("111", "2026-03-01", "2026-03-03", "gmvmax") is None


def test_get_range_partial(cache):
    cache.put_daily("111", "2026-03-01", "gmvmax", 10.0, 30.0, 1)
    cache.put_daily("111", "2026-03-03", "gmvmax", 30.0, 90.0, 3)
    result = cache.get_range(
        "111", "2026-03-01", "2026-03-03", "gmvmax", allow_partial=True
    )
    assert result["cost"] == 40.0
    assert result["cached_days"] == 2
    assert result["total_days"] == 3


def test_get_range_no_data_partial(cache):
    result = cache.get_range(
        "999", "2026-03-01", "2026-03-03", "gmvmax", allow_partial=True
    )
    assert result is None


def test_seed_fallback(seeded_cache):
    result = seeded_cache.get_daily("111", "2026-03-10", "gmvmax")
    assert result["cost"] == 50.0


def test_clear(cache):
    cache.put_daily("111", "2026-03-10", "gmvmax", 100.0, 300.0, 10)
    cache.clear()
    assert cache.get_daily("111", "2026-03-10", "gmvmax") is None


def test_different_ad_types(cache):
    cache.put_daily("111", "2026-03-10", "gmvmax", 100.0, 300.0, 10)
    cache.put_daily("111", "2026-03-10", "ads", 50.0, 150.0, 5)
    assert cache.get_daily("111", "2026-03-10", "gmvmax")["cost"] == 100.0
    assert cache.get_daily("111", "2026-03-10", "ads")["cost"] == 50.0


def test_overwrite(cache):
    cache.put_daily("111", "2026-03-10", "gmvmax", 100.0, 300.0, 10)
    cache.put_daily("111", "2026-03-10", "gmvmax", 200.0, 600.0, 20)
    result = cache.get_daily("111", "2026-03-10", "gmvmax")
    assert result["cost"] == 200.0


def test_persistence(tmp_path):
    cache1 = AdCostCache(cache_dir=tmp_path)
    cache1.put_daily("111", "2026-03-10", "gmvmax", 100.0, 300.0, 10)

    cache2 = AdCostCache(cache_dir=tmp_path)
    result = cache2.get_daily("111", "2026-03-10", "gmvmax")
    assert result["cost"] == 100.0
