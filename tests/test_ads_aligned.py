"""Tests for Ads timezone-aligned report tool."""

from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock, patch
from zoneinfo import ZoneInfo

import pytest

from tiktok_ads_mcp.tz_cache import _tz_cache
from tiktok_ads_mcp.tools.ads_report_aligned import get_ads_report_aligned


@pytest.fixture(autouse=True)
def clear_tz_cache():
    _tz_cache.clear()
    yield
    _tz_cache.clear()


@pytest.fixture
def mock_client():
    client = MagicMock()
    client._make_request = AsyncMock()
    return client


def _make_hourly_row(hour_str: str, spend: float, gmv: float, orders: int) -> dict:
    return {
        "dimensions": {"stat_time_hour": hour_str, "advertiser_id": "123"},
        "metrics": {
            "spend": str(spend),
            "total_onsite_shopping_value": str(gmv),
            "onsite_shopping": str(orders),
        },
    }


def _tz_response(tz_str: str = "UTC") -> dict:
    return {
        "code": 0,
        "data": {"list": [{"advertiser_id": "123", "timezone": tz_str}]},
    }


def _hourly_response(rows: list) -> dict:
    return {"code": 0, "data": {"list": rows, "page_info": {"total_number": len(rows)}}}


@pytest.mark.asyncio
async def test_basic_aggregation(mock_client):
    """Hours within shop-day should be summed correctly."""
    mock_client._make_request.side_effect = [
        _tz_response("UTC"),
        _hourly_response(
            [
                _make_hourly_row("2026-03-10 10:00:00", 10.0, 50.0, 2),
                _make_hourly_row("2026-03-10 11:00:00", 15.0, 75.0, 3),
            ]
        ),
    ]

    with patch("tiktok_ads_mcp.tools.ads_report_aligned.datetime") as mock_dt:
        mock_dt.now.return_value = datetime(2026, 3, 10, 23, 59, tzinfo=timezone.utc)
        mock_dt.strptime = datetime.strptime
        mock_dt.side_effect = lambda *a, **kw: datetime(*a, **kw)

        result = await get_ads_report_aligned(
            mock_client, "123", "2026-03-10", shop_tz="UTC"
        )

    assert result["metrics"]["cost"] == 25.0
    assert result["metrics"]["gmv"] == 125.0
    assert result["metrics"]["orders"] == 5
    assert result["roas"] == 5.0
    assert result["hours_included"] == 2


@pytest.mark.asyncio
async def test_cross_day_two_fetches(mock_client):
    """Shop PST day should fetch 2 native dates when ad tz is UTC."""
    mock_client._make_request.side_effect = [
        _tz_response("UTC"),
        _hourly_response(
            [
                _make_hourly_row("2026-01-15 10:00:00", 10.0, 50.0, 1),
            ]
        ),
        _hourly_response(
            [
                _make_hourly_row("2026-01-16 05:00:00", 20.0, 100.0, 2),
            ]
        ),
    ]

    with patch("tiktok_ads_mcp.tools.ads_report_aligned.datetime") as mock_dt:
        mock_dt.now.return_value = datetime(2026, 1, 17, 0, 0, tzinfo=timezone.utc)
        mock_dt.strptime = datetime.strptime
        mock_dt.side_effect = lambda *a, **kw: datetime(*a, **kw)

        result = await get_ads_report_aligned(
            mock_client, "123", "2026-01-15", shop_tz="America/Los_Angeles"
        )

    assert result["metrics"]["cost"] == 30.0
    assert result["metrics"]["gmv"] == 150.0
    assert result["hours_included"] == 2


@pytest.mark.asyncio
async def test_empty_data_returns_zeros(mock_client):
    mock_client._make_request.side_effect = [
        _tz_response("UTC"),
        _hourly_response([]),
    ]

    with patch("tiktok_ads_mcp.tools.ads_report_aligned.datetime") as mock_dt:
        mock_dt.now.return_value = datetime(2026, 3, 10, 23, 59, tzinfo=timezone.utc)
        mock_dt.strptime = datetime.strptime
        mock_dt.side_effect = lambda *a, **kw: datetime(*a, **kw)

        result = await get_ads_report_aligned(
            mock_client, "123", "2026-03-10", shop_tz="UTC"
        )

    assert result["metrics"]["cost"] == 0.0
    assert result["metrics"]["gmv"] == 0.0
    assert result["metrics"]["orders"] == 0
    assert result["roas"] == 0.0


@pytest.mark.asyncio
async def test_tz_cache_reused(mock_client):
    """Pre-populated cache should prevent advertiser/info/ call."""
    _tz_cache["123"] = ZoneInfo("UTC")

    mock_client._make_request.side_effect = [
        _hourly_response(
            [
                _make_hourly_row("2026-03-10 10:00:00", 5.0, 25.0, 1),
            ]
        ),
    ]

    with patch("tiktok_ads_mcp.tools.ads_report_aligned.datetime") as mock_dt:
        mock_dt.now.return_value = datetime(2026, 3, 10, 23, 59, tzinfo=timezone.utc)
        mock_dt.strptime = datetime.strptime
        mock_dt.side_effect = lambda *a, **kw: datetime(*a, **kw)

        result = await get_ads_report_aligned(
            mock_client, "123", "2026-03-10", shop_tz="UTC"
        )

    assert mock_client._make_request.call_count == 1
    assert result["hours_included"] == 1
