"""Tests for tiktok_ads_mcp.cache.ban_status."""

import json
import pytest
from tiktok_ads_mcp.cache.ban_status import BanStatusCache


@pytest.fixture
def cache(tmp_path):
    return BanStatusCache(cache_dir=tmp_path)


@pytest.fixture
def seeded_cache(tmp_path):
    seed_file = tmp_path / "seed" / "ban_status_seed.json"
    seed_file.parent.mkdir()
    seed_file.write_text(
        json.dumps(
            {
                "111": {
                    "banned": True,
                    "status": "STATUS_LIMIT",
                    "detected_at": "2026-03-10",
                    "last_active_date": "2026-03-09",
                    "ad_type": "gmvmax",
                    "group_key": "Test",
                    "shop_tz": "",
                    "ad_tz": "",
                }
            }
        )
    )
    cache_dir = tmp_path / "cache"
    return BanStatusCache(cache_dir=cache_dir, seed_file=seed_file)


def test_initially_not_banned(cache):
    assert not cache.is_banned("111")


def test_set_banned_and_check(cache):
    cache.set_banned("111", status="STATUS_LIMIT", detected_at="2026-03-10")
    assert cache.is_banned("111")


def test_get_status(cache):
    cache.set_banned(
        "111",
        status="STATUS_LIMIT",
        detected_at="2026-03-10",
        last_active_date="2026-03-09",
        ad_type="gmvmax",
        group_key="Test-Group",
        shop_tz="America/Los_Angeles",
        ad_tz="Asia/Shanghai",
    )
    status = cache.get_status("111")
    assert status["banned"] is True
    assert status["status"] == "STATUS_LIMIT"
    assert status["last_active_date"] == "2026-03-09"
    assert status["shop_tz"] == "America/Los_Angeles"


def test_get_status_missing(cache):
    assert cache.get_status("999") is None


def test_set_active_removes(cache):
    cache.set_banned("111", status="STATUS_LIMIT", detected_at="2026-03-10")
    cache.set_active("111")
    assert not cache.is_banned("111")
    assert cache.get_status("111") is None


def test_set_active_nonexistent(cache):
    # Should not raise
    cache.set_active("999")


def test_get_all_banned(cache):
    cache.set_banned("111", status="STATUS_LIMIT", detected_at="2026-03-10")
    cache.set_banned("222", status="STATUS_FROZEN", detected_at="2026-03-11")
    banned = cache.get_all_banned()
    assert "111" in banned
    assert "222" in banned
    assert len(banned) == 2


def test_set_banned_preserves_detected_at(cache):
    """Re-banning should keep the original detected_at."""
    cache.set_banned("111", status="STATUS_LIMIT", detected_at="2026-03-10")
    cache.set_banned("111", status="STATUS_FROZEN", detected_at="2026-03-15")
    status = cache.get_status("111")
    assert status["detected_at"] == "2026-03-10"
    assert status["status"] == "STATUS_FROZEN"


def test_set_banned_fills_missing_fields(cache):
    """Setting banned with partial fields should merge with existing."""
    cache.set_banned(
        "111",
        status="STATUS_LIMIT",
        detected_at="2026-03-10",
        ad_type="gmvmax",
        group_key="Group-A",
    )
    cache.set_banned("111", status="STATUS_FROZEN", detected_at="2026-03-15")
    status = cache.get_status("111")
    assert status["ad_type"] == "gmvmax"
    assert status["group_key"] == "Group-A"


def test_seed_fallback(seeded_cache):
    assert seeded_cache.is_banned("111")
    status = seeded_cache.get_status("111")
    assert status["status"] == "STATUS_LIMIT"


def test_clear(cache):
    cache.set_banned("111", status="STATUS_LIMIT", detected_at="2026-03-10")
    cache.clear()
    assert not cache.is_banned("111")


def test_persistence(tmp_path):
    cache1 = BanStatusCache(cache_dir=tmp_path)
    cache1.set_banned("111", status="STATUS_LIMIT", detected_at="2026-03-10")

    cache2 = BanStatusCache(cache_dir=tmp_path)
    assert cache2.is_banned("111")
