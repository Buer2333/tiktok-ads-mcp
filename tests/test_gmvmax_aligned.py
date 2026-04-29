"""Tests for GMVMAX timezone-aligned report tool."""

from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock, patch
from zoneinfo import ZoneInfo

import pytest

from tiktok_ads_mcp.tz_cache import _tz_cache
from tiktok_ads_mcp.tools.gmvmax_report_aligned import (
    get_gmvmax_report_aligned,
)


@pytest.fixture(autouse=True)
def clear_tz_cache():
    """Clear timezone cache between tests."""
    _tz_cache.clear()
    yield
    _tz_cache.clear()


@pytest.fixture(autouse=True)
def relax_completeness_tolerance():
    """Existing tests use 1-2 hourly rows; relax production threshold so they
    don't have to fabricate full-day data. Completeness logic itself is tested
    explicitly in test_partial_response_*."""
    with patch("tiktok_ads_mcp.tools.gmvmax_report_aligned._HOURS_LAG_TOLERANCE", 24):
        yield


@pytest.fixture
def mock_client():
    client = MagicMock()
    client._make_request = AsyncMock()
    return client


def _make_hourly_row(hour_str: str, cost: float, gmv: float, orders: int) -> dict:
    return {
        "dimensions": {"stat_time_hour": hour_str, "advertiser_id": "123"},
        "metrics": {
            "cost": str(cost),
            "gross_revenue": str(gmv),
            "orders": str(orders),
        },
    }


def _tz_response(tz_str: str = "America/New_York") -> dict:
    return {
        "code": 0,
        "data": {"list": [{"advertiser_id": "123", "timezone": tz_str}]},
    }


def _hourly_response(rows: list) -> dict:
    return {"code": 0, "data": {"list": rows, "page_info": {"total_number": len(rows)}}}


@pytest.mark.asyncio
async def test_basic_aggregation(mock_client):
    """Hours within shop-day should be summed correctly."""
    # Ad tz = UTC, shop tz = UTC → same day, no cross-day complexity
    mock_client._make_request.side_effect = [
        _tz_response("UTC"),
        _hourly_response(
            [
                _make_hourly_row("2026-03-10 10:00:00", 10.0, 50.0, 2),
                _make_hourly_row("2026-03-10 11:00:00", 15.0, 75.0, 3),
            ]
        ),
    ]

    with patch("tiktok_ads_mcp.tools.gmvmax_report_aligned.datetime") as mock_dt:
        mock_dt.now.return_value = datetime(2026, 3, 10, 23, 59, tzinfo=timezone.utc)
        mock_dt.strptime = datetime.strptime
        mock_dt.side_effect = lambda *a, **kw: datetime(*a, **kw)

        result = await get_gmvmax_report_aligned(
            mock_client, "123", "2026-03-10", ["store1"], shop_tz="UTC"
        )

    assert result["metrics"]["cost"] == 25.0
    assert result["metrics"]["gross_revenue"] == 125.0
    assert result["metrics"]["orders"] == 5.0
    assert result["roi"] == 5.0
    assert result["hours_included"] == 2


@pytest.mark.asyncio
async def test_cross_day_two_fetches(mock_client):
    """Shop PST day should fetch 2 native dates when ad tz is UTC."""
    # Shop PST day 2026-01-15: UTC 08:00 Jan 15 to 08:00 Jan 16
    # Ad tz = UTC → need to query Jan 15 and Jan 16
    mock_client._make_request.side_effect = [
        _tz_response("UTC"),
        # Jan 15 hourly data
        _hourly_response(
            [
                _make_hourly_row("2026-01-15 10:00:00", 10.0, 50.0, 1),
            ]
        ),
        # Jan 16 hourly data
        _hourly_response(
            [
                _make_hourly_row("2026-01-16 05:00:00", 20.0, 100.0, 2),
            ]
        ),
    ]

    with patch("tiktok_ads_mcp.tools.gmvmax_report_aligned.datetime") as mock_dt:
        mock_dt.now.return_value = datetime(2026, 1, 17, 0, 0, tzinfo=timezone.utc)
        mock_dt.strptime = datetime.strptime
        mock_dt.side_effect = lambda *a, **kw: datetime(*a, **kw)

        result = await get_gmvmax_report_aligned(
            mock_client, "123", "2026-01-15", ["store1"], shop_tz="America/Los_Angeles"
        )

    assert result["metrics"]["cost"] == 30.0
    assert result["metrics"]["gross_revenue"] == 150.0
    assert result["hours_included"] == 2


@pytest.mark.asyncio
async def test_future_hours_filtered(mock_client):
    """Hours with UTC time > now should be excluded."""
    mock_client._make_request.side_effect = [
        _tz_response("UTC"),
        _hourly_response(
            [
                _make_hourly_row("2026-03-10 10:00:00", 10.0, 50.0, 1),
                _make_hourly_row("2026-03-10 20:00:00", 99.0, 99.0, 99),  # future
            ]
        ),
    ]

    with patch("tiktok_ads_mcp.tools.gmvmax_report_aligned.datetime") as mock_dt:
        # "now" is 15:00 UTC, so 20:00 row should be excluded
        mock_dt.now.return_value = datetime(2026, 3, 10, 15, 0, tzinfo=timezone.utc)
        mock_dt.strptime = datetime.strptime
        mock_dt.side_effect = lambda *a, **kw: datetime(*a, **kw)

        result = await get_gmvmax_report_aligned(
            mock_client, "123", "2026-03-10", ["store1"], shop_tz="UTC"
        )

    assert result["metrics"]["cost"] == 10.0
    assert result["hours_included"] == 1


@pytest.mark.asyncio
async def test_empty_data_returns_zeros(mock_client):
    """No data → zero metrics and 0 hours."""
    mock_client._make_request.side_effect = [
        _tz_response("UTC"),
        _hourly_response([]),
    ]

    with patch("tiktok_ads_mcp.tools.gmvmax_report_aligned.datetime") as mock_dt:
        mock_dt.now.return_value = datetime(2026, 3, 10, 23, 59, tzinfo=timezone.utc)
        mock_dt.strptime = datetime.strptime
        mock_dt.side_effect = lambda *a, **kw: datetime(*a, **kw)

        result = await get_gmvmax_report_aligned(
            mock_client, "123", "2026-03-10", ["store1"], shop_tz="UTC"
        )

    assert result["metrics"]["cost"] == 0.0
    assert result["metrics"]["gross_revenue"] == 0.0
    assert result["metrics"]["orders"] == 0.0
    assert result["roi"] == 0.0
    assert result["hours_included"] == 0


@pytest.mark.asyncio
async def test_tz_cache_used_on_second_call(mock_client):
    """Second call should use cached tz, not call advertiser/info/ again."""
    # Pre-populate cache
    _tz_cache["123"] = ZoneInfo("UTC")

    mock_client._make_request.side_effect = [
        _hourly_response(
            [
                _make_hourly_row("2026-03-10 10:00:00", 5.0, 25.0, 1),
            ]
        ),
    ]

    with patch("tiktok_ads_mcp.tools.gmvmax_report_aligned.datetime") as mock_dt:
        mock_dt.now.return_value = datetime(2026, 3, 10, 23, 59, tzinfo=timezone.utc)
        mock_dt.strptime = datetime.strptime
        mock_dt.side_effect = lambda *a, **kw: datetime(*a, **kw)

        result = await get_gmvmax_report_aligned(
            mock_client, "123", "2026-03-10", ["store1"], shop_tz="UTC"
        )

    # Only 1 API call (hourly data), no advertiser/info/ call
    assert mock_client._make_request.call_count == 1
    assert result["hours_included"] == 1


# ── Completeness check (incomplete-data retry) ────────────────────────────


@pytest.fixture
def disable_completeness_relax(monkeypatch):
    """Override the autouse relax fixture so production tolerance applies."""
    from tiktok_ads_mcp.tools import gmvmax_report_aligned as mod

    monkeypatch.setattr(mod, "_HOURS_LAG_TOLERANCE", 2)


@pytest.mark.asyncio
async def test_partial_response_triggers_retry(mock_client, disable_completeness_relax):
    """Partial hourly response (1 ≤ hours < threshold) must raise so the
    @api_retry decorator backs off and re-fetches. This is the 2026-04-28
    silent-data-loss bug pattern."""
    from tiktok_ads_mcp.client import TikTokIncompleteDataError

    _tz_cache["123"] = ZoneInfo("UTC")

    # Day far in the past from now's perspective: expected = 24, threshold = 22.
    # Mock returns only 5 rows → triggers TikTokIncompleteDataError.
    mock_client._make_request.side_effect = [
        _hourly_response(
            [
                _make_hourly_row(f"2026-03-10 {h:02d}:00:00", 1.0, 5.0, 1)
                for h in range(5)
            ]
        ),
    ] * 3  # decorator retries up to 3 times — feed identical partial each time

    with patch("tiktok_ads_mcp.tools.gmvmax_report_aligned.datetime") as mock_dt:
        mock_dt.now.return_value = datetime(2026, 3, 11, 12, 0, tzinfo=timezone.utc)
        mock_dt.strptime = datetime.strptime
        mock_dt.side_effect = lambda *a, **kw: datetime(*a, **kw)

        with pytest.raises(TikTokIncompleteDataError) as exc:
            await get_gmvmax_report_aligned(
                mock_client, "123", "2026-03-10", ["store1"], shop_tz="UTC"
            )

    # Latest hour we received is 04:00 UTC; now=2026-03-11 12:00 UTC, so the
    # most-recent-row lag is ~31h — far above tol — and triggers retry.
    assert "lags" in str(exc.value)
    assert "truncated mid-window" in str(exc.value)
    # Decorator retried 3 times, each consuming 1 mock response
    assert mock_client._make_request.call_count == 3


@pytest.mark.asyncio
async def test_partial_rows_with_zero_cost_not_retried(
    mock_client, disable_completeness_relax
):
    """If TikTok returns 12 hourly rows but every row has cost=0, that's an
    INACTIVE advertiser — not a partial response. Must NOT retry. Regression
    test for 2026-04-28 false positive (Hi-NAD+ Ads ...192017 had $0 spend
    on 4/27 but endpoint returned 12 rows of zeros, kept raising forever)."""
    _tz_cache["123"] = ZoneInfo("UTC")
    # 12 rows with cost=0 — would've been < threshold 22 under old logic
    mock_client._make_request.side_effect = [
        _hourly_response(
            [
                _make_hourly_row(f"2026-03-10 {h:02d}:00:00", 0.0, 0.0, 0)
                for h in range(12)
            ]
        ),
    ]

    with patch("tiktok_ads_mcp.tools.gmvmax_report_aligned.datetime") as mock_dt:
        mock_dt.now.return_value = datetime(2026, 3, 11, 12, 0, tzinfo=timezone.utc)
        mock_dt.strptime = datetime.strptime
        mock_dt.side_effect = lambda *a, **kw: datetime(*a, **kw)

        result = await get_gmvmax_report_aligned(
            mock_client, "123", "2026-03-10", ["store1"], shop_tz="UTC"
        )

    assert result["metrics"]["cost"] == 0.0
    assert result["hours_included"] == 12
    assert mock_client._make_request.call_count == 1  # NO retry — passed through


@pytest.mark.asyncio
async def test_zero_rows_treated_as_inactive(mock_client, disable_completeness_relax):
    """Empty list (advertiser had no spend that day) must not trigger retry —
    it's a legitimate state, not a partial response."""
    _tz_cache["123"] = ZoneInfo("UTC")
    mock_client._make_request.side_effect = [_hourly_response([])]

    with patch("tiktok_ads_mcp.tools.gmvmax_report_aligned.datetime") as mock_dt:
        mock_dt.now.return_value = datetime(2026, 3, 11, 12, 0, tzinfo=timezone.utc)
        mock_dt.strptime = datetime.strptime
        mock_dt.side_effect = lambda *a, **kw: datetime(*a, **kw)

        result = await get_gmvmax_report_aligned(
            mock_client, "123", "2026-03-10", ["store1"], shop_tz="UTC"
        )

    assert result["metrics"]["cost"] == 0.0
    assert result["hours_included"] == 0
    assert mock_client._make_request.call_count == 1  # no retries


@pytest.mark.asyncio
async def test_complete_response_passes(mock_client, disable_completeness_relax):
    """Full 24-hour response on a past day — no retry, no exception."""
    _tz_cache["123"] = ZoneInfo("UTC")
    mock_client._make_request.side_effect = [
        _hourly_response(
            [
                _make_hourly_row(f"2026-03-10 {h:02d}:00:00", 1.0, 5.0, 1)
                for h in range(24)
            ]
        ),
    ]

    with patch("tiktok_ads_mcp.tools.gmvmax_report_aligned.datetime") as mock_dt:
        mock_dt.now.return_value = datetime(2026, 3, 11, 12, 0, tzinfo=timezone.utc)
        mock_dt.strptime = datetime.strptime
        mock_dt.side_effect = lambda *a, **kw: datetime(*a, **kw)

        result = await get_gmvmax_report_aligned(
            mock_client, "123", "2026-03-10", ["store1"], shop_tz="UTC"
        )

    assert result["hours_included"] == 24
    assert result["metrics"]["cost"] == 24.0
    assert mock_client._make_request.call_count == 1


@pytest.mark.asyncio
async def test_today_partial_within_tolerance_passes(
    mock_client, disable_completeness_relax
):
    """Today, 23h elapsed, 22h received — within 2h lag tolerance, no retry."""
    _tz_cache["123"] = ZoneInfo("UTC")
    # Shop UTC day 2026-03-10, now=2026-03-10 23:00 UTC → expected=23, threshold=21
    # Receive 22 hours → passes (22 ≥ 21)
    mock_client._make_request.side_effect = [
        _hourly_response(
            [
                _make_hourly_row(f"2026-03-10 {h:02d}:00:00", 1.0, 5.0, 1)
                for h in range(22)
            ]
        ),
    ]

    with patch("tiktok_ads_mcp.tools.gmvmax_report_aligned.datetime") as mock_dt:
        mock_dt.now.return_value = datetime(2026, 3, 10, 23, 0, tzinfo=timezone.utc)
        mock_dt.strptime = datetime.strptime
        mock_dt.side_effect = lambda *a, **kw: datetime(*a, **kw)

        result = await get_gmvmax_report_aligned(
            mock_client, "123", "2026-03-10", ["store1"], shop_tz="UTC"
        )

    assert result["hours_included"] == 22
