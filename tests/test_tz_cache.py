"""Tests for tz_cache module."""

from unittest.mock import AsyncMock, MagicMock
from zoneinfo import ZoneInfo

import pytest

from tiktok_ads_mcp.tz_cache import _tz_cache, get_ad_tz, warmup_tz_cache


@pytest.fixture(autouse=True)
def clear_cache():
    _tz_cache.clear()
    yield
    _tz_cache.clear()


@pytest.fixture
def mock_client():
    client = MagicMock()
    client._make_request = AsyncMock()
    return client


def _tz_response(advertiser_id: str, tz_str: str) -> dict:
    return {
        "code": 0,
        "data": {"list": [{"advertiser_id": advertiser_id, "timezone": tz_str}]},
    }


def _batch_tz_response(pairs: list) -> dict:
    return {
        "code": 0,
        "data": {"list": [{"advertiser_id": aid, "timezone": tz} for aid, tz in pairs]},
    }


@pytest.mark.asyncio
async def test_get_ad_tz_fetches_and_caches(mock_client):
    mock_client._make_request.return_value = _tz_response("111", "America/New_York")

    tz = await get_ad_tz(mock_client, "111")
    assert tz == ZoneInfo("America/New_York")
    assert _tz_cache["111"] == ZoneInfo("America/New_York")

    # Second call should use cache, not call API
    tz2 = await get_ad_tz(mock_client, "111")
    assert tz2 == ZoneInfo("America/New_York")
    assert mock_client._make_request.call_count == 1


@pytest.mark.asyncio
async def test_get_ad_tz_defaults_to_utc(mock_client):
    mock_client._make_request.return_value = {"code": 40001, "message": "error"}

    tz = await get_ad_tz(mock_client, "222")
    assert tz == ZoneInfo("UTC")


@pytest.mark.asyncio
async def test_warmup_populates_cache(mock_client):
    mock_client._make_request.return_value = _batch_tz_response(
        [
            ("111", "America/New_York"),
            ("222", "Asia/Bangkok"),
        ]
    )

    await warmup_tz_cache(mock_client, ["111", "222"])
    assert _tz_cache["111"] == ZoneInfo("America/New_York")
    assert _tz_cache["222"] == ZoneInfo("Asia/Bangkok")


@pytest.mark.asyncio
async def test_warmup_skips_cached(mock_client):
    _tz_cache["111"] = ZoneInfo("UTC")
    mock_client._make_request.return_value = _batch_tz_response(
        [
            ("222", "Asia/Bangkok"),
        ]
    )

    await warmup_tz_cache(mock_client, ["111", "222"])
    # 111 should remain UTC (cached), 222 should be Bangkok
    assert _tz_cache["111"] == ZoneInfo("UTC")
    assert _tz_cache["222"] == ZoneInfo("Asia/Bangkok")
    assert mock_client._make_request.call_count == 1


@pytest.mark.asyncio
async def test_warmup_fills_missing_with_utc(mock_client):
    mock_client._make_request.side_effect = Exception("network error")

    await warmup_tz_cache(mock_client, ["333"])
    assert _tz_cache["333"] == ZoneInfo("UTC")
