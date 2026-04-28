"""Tests for tiktok_ads_mcp.cache.ad_cost.

Cache key contract:
  - GMVMAX: {advertiser_id}:{date_str}:gmvmax:{store_id} — store_id REQUIRED
  - Ads:    {advertiser_id}:{date_str}:ads               — no store dimension

Most tests pin the GMVMAX path since that's where Bug B (cross-store
aggregation from a store-less key) lived; Ads has no store dimension at the
TikTok API level so its key shape is unchanged.
"""

import json
import pytest
from tiktok_ads_mcp.cache.ad_cost import AdCostCache


STORE_A = "1000000000000000001"
STORE_B = "1000000000000000002"


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
                f"111:2026-03-10:gmvmax:{STORE_A}": {
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
    cache.put_daily("111", "2026-03-10", "gmvmax", 100.0, 300.0, 10, store_id=STORE_A)
    result = cache.get_daily("111", "2026-03-10", "gmvmax", store_id=STORE_A)
    assert result == {"cost": 100.0, "gmv": 300.0, "orders": 10}


def test_get_daily_missing(cache):
    assert cache.get_daily("999", "2026-01-01", "ads") is None


def test_get_range_complete(cache):
    cache.put_daily("111", "2026-03-01", "gmvmax", 10.0, 30.0, 1, store_id=STORE_A)
    cache.put_daily("111", "2026-03-02", "gmvmax", 20.0, 60.0, 2, store_id=STORE_A)
    cache.put_daily("111", "2026-03-03", "gmvmax", 30.0, 90.0, 3, store_id=STORE_A)
    result = cache.get_range(
        "111", "2026-03-01", "2026-03-03", "gmvmax", store_id=STORE_A
    )
    assert result == {"cost": 60.0, "gmv": 180.0, "orders": 6}


def test_get_range_incomplete_returns_none(cache):
    cache.put_daily("111", "2026-03-01", "gmvmax", 10.0, 30.0, 1, store_id=STORE_A)
    # Missing 03-02
    cache.put_daily("111", "2026-03-03", "gmvmax", 30.0, 90.0, 3, store_id=STORE_A)
    assert (
        cache.get_range("111", "2026-03-01", "2026-03-03", "gmvmax", store_id=STORE_A)
        is None
    )


def test_get_range_partial(cache):
    cache.put_daily("111", "2026-03-01", "gmvmax", 10.0, 30.0, 1, store_id=STORE_A)
    cache.put_daily("111", "2026-03-03", "gmvmax", 30.0, 90.0, 3, store_id=STORE_A)
    result = cache.get_range(
        "111",
        "2026-03-01",
        "2026-03-03",
        "gmvmax",
        allow_partial=True,
        store_id=STORE_A,
    )
    assert result["cost"] == 40.0
    assert result["cached_days"] == 2
    assert result["total_days"] == 3


def test_get_range_no_data_partial(cache):
    result = cache.get_range(
        "999",
        "2026-03-01",
        "2026-03-03",
        "gmvmax",
        allow_partial=True,
        store_id=STORE_A,
    )
    assert result is None


def test_seed_fallback(seeded_cache):
    result = seeded_cache.get_daily("111", "2026-03-10", "gmvmax", store_id=STORE_A)
    assert result["cost"] == 50.0


def test_clear(cache):
    cache.put_daily("111", "2026-03-10", "gmvmax", 100.0, 300.0, 10, store_id=STORE_A)
    cache.clear()
    assert cache.get_daily("111", "2026-03-10", "gmvmax", store_id=STORE_A) is None


def test_different_ad_types(cache):
    cache.put_daily("111", "2026-03-10", "gmvmax", 100.0, 300.0, 10, store_id=STORE_A)
    cache.put_daily("111", "2026-03-10", "ads", 50.0, 150.0, 5)
    assert (
        cache.get_daily("111", "2026-03-10", "gmvmax", store_id=STORE_A)["cost"]
        == 100.0
    )
    assert cache.get_daily("111", "2026-03-10", "ads")["cost"] == 50.0


def test_overwrite(cache):
    cache.put_daily("111", "2026-03-10", "gmvmax", 100.0, 300.0, 10, store_id=STORE_A)
    cache.put_daily("111", "2026-03-10", "gmvmax", 200.0, 600.0, 20, store_id=STORE_A)
    result = cache.get_daily("111", "2026-03-10", "gmvmax", store_id=STORE_A)
    assert result["cost"] == 200.0


def test_persistence(tmp_path):
    cache1 = AdCostCache(cache_dir=tmp_path)
    cache1.put_daily("111", "2026-03-10", "gmvmax", 100.0, 300.0, 10, store_id=STORE_A)

    cache2 = AdCostCache(cache_dir=tmp_path)
    result = cache2.get_daily("111", "2026-03-10", "gmvmax", store_id=STORE_A)
    assert result["cost"] == 100.0


# ── Bug B contract: store_id MUST be present for GMVMAX (no silent fallback) ──


def test_gmvmax_put_without_store_id_raises(cache):
    """Missing store_id on GMVMAX put → ValueError. Silent fallback would risk
    cross-store aggregation (the 2026-04-28 incident root cause)."""
    with pytest.raises(ValueError, match="GMVMAX cache access requires store_id"):
        cache.put_daily("111", "2026-03-10", "gmvmax", 100.0, 300.0, 10)


def test_gmvmax_get_daily_without_store_id_raises(cache):
    with pytest.raises(ValueError, match="GMVMAX cache access requires store_id"):
        cache.get_daily("111", "2026-03-10", "gmvmax")


def test_gmvmax_get_range_without_store_id_raises(cache):
    cache.put_daily("111", "2026-03-01", "gmvmax", 10.0, 30.0, 1, store_id=STORE_A)
    with pytest.raises(ValueError, match="GMVMAX cache access requires store_id"):
        cache.get_range("111", "2026-03-01", "2026-03-03", "gmvmax")


def test_ads_no_store_id_works(cache):
    """Ads has no store dimension at TikTok API; cache key omits store_id."""
    cache.put_daily("111", "2026-03-10", "ads", 50.0, 150.0, 5)
    result = cache.get_daily("111", "2026-03-10", "ads")
    assert result["cost"] == 50.0


# ── Bug B fix invariant: per-store entries are isolated, never aggregated ──


def test_two_stores_same_advertiser_isolated(cache):
    """Same advertiser_id, two stores → two independent cache entries.

    Previous bug: key = {adv}:{date}:gmvmax (no store_id) meant the second
    put overwrote the first, and reads returned a single store's value
    masquerading as the advertiser's total.
    """
    cache.put_daily("111", "2026-03-10", "gmvmax", 100.0, 300.0, 10, store_id=STORE_A)
    cache.put_daily("111", "2026-03-10", "gmvmax", 250.0, 700.0, 25, store_id=STORE_B)

    a = cache.get_daily("111", "2026-03-10", "gmvmax", store_id=STORE_A)
    b = cache.get_daily("111", "2026-03-10", "gmvmax", store_id=STORE_B)

    assert a == {"cost": 100.0, "gmv": 300.0, "orders": 10}
    assert b == {"cost": 250.0, "gmv": 700.0, "orders": 25}


def test_get_range_per_store_does_not_aggregate(cache):
    """get_range(store_id=X) returns only store X's data — not adv-wide sum.

    Regression test for the 2026-04-28 incident: querying FN-US-Shilajit's
    store_id should NOT pull in Hiileathy-US-Shilajit's spend even when both
    routes share the same advertiser_id.
    """
    # Account splits: $4k/day on store_A (FN-US), $5k/day on store_B (Hiileathy)
    for date in ("2026-04-01", "2026-04-02", "2026-04-03"):
        cache.put_daily("111", date, "gmvmax", 4000.0, 8000.0, 100, store_id=STORE_A)
        cache.put_daily("111", date, "gmvmax", 5000.0, 10000.0, 150, store_id=STORE_B)

    range_a = cache.get_range(
        "111", "2026-04-01", "2026-04-03", "gmvmax", store_id=STORE_A
    )
    range_b = cache.get_range(
        "111", "2026-04-01", "2026-04-03", "gmvmax", store_id=STORE_B
    )

    # Each store must see only its own spend
    assert range_a["cost"] == 12000.0  # 3 × $4k
    assert range_b["cost"] == 15000.0  # 3 × $5k
    # Sum across stores would be $27k — neither single-store query should return that
    assert range_a["cost"] != 27000.0
    assert range_b["cost"] != 27000.0
