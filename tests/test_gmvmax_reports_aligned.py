"""Tests for the dimensions-aware tz-aligned GMVMAX helpers and the future
`get_gmvmax_reports_aligned()` dispatcher path.

These cover the new building blocks added for the THB-advertiser timezone
misalignment fix (see /Users/shining/.claude/plans/dynamic-spinning-seahorse.md).
"""

from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock
from zoneinfo import ZoneInfo

import pytest

from tiktok_ads_mcp.tools.gmvmax_report_aligned import (
    _aggregate_by_dims,
    _fetch_hourly_by_dim,
)


@pytest.fixture
def mock_client():
    client = MagicMock()
    client._make_request = AsyncMock()
    return client


def _hour_row(
    dim_value: str,
    hour: str,
    cost: float,
    gmv: float,
    orders: int,
    dim_key: str = "campaign_id",
) -> dict:
    return {
        "dimensions": {dim_key: dim_value, "stat_time_hour": hour},
        "metrics": {
            "cost": str(cost),
            "gross_revenue": str(gmv),
            "orders": str(orders),
        },
    }


@pytest.mark.asyncio
async def test_fetch_hourly_by_dim_appends_stat_time_hour(mock_client):
    """The helper must request `[*dims, "stat_time_hour"]` dimensions."""
    mock_client._make_request.return_value = {
        "code": 0,
        "data": {"list": [], "page_info": {"total_page": 1, "total_number": 0}},
    }

    await _fetch_hourly_by_dim(
        mock_client,
        "adv1",
        "2026-05-19",
        ["store1"],
        dimensions=["campaign_id"],
        metrics=["cost", "gross_revenue", "orders"],
    )

    args, kwargs = mock_client._make_request.call_args
    sent_params = args[2]
    import json as _json

    assert _json.loads(sent_params["dimensions"]) == ["campaign_id", "stat_time_hour"]
    assert sent_params["start_date"] == "2026-05-19"
    assert sent_params["end_date"] == "2026-05-19"


@pytest.mark.asyncio
async def test_fetch_hourly_by_dim_returns_24_rows(mock_client):
    """Single-campaign × 24h fixture should return all 24 raw rows untouched."""
    rows_24h = [
        _hour_row("c1", f"2026-05-19 {h:02d}:00:00", 1.0 + h, 2.0 + h, 1)
        for h in range(24)
    ]
    mock_client._make_request.return_value = {
        "code": 0,
        "data": {
            "list": rows_24h,
            "page_info": {"total_page": 1, "total_number": 24},
        },
    }

    out = await _fetch_hourly_by_dim(
        mock_client,
        "adv1",
        "2026-05-19",
        ["store1"],
        dimensions=["campaign_id"],
        metrics=["cost", "gross_revenue", "orders"],
    )

    assert len(out) == 24
    assert out[0]["dimensions"]["stat_time_hour"] == "2026-05-19 00:00:00"
    assert out[23]["metrics"]["cost"] == "24.0"


@pytest.mark.asyncio
async def test_fetch_hourly_by_dim_paginates(mock_client):
    """When total_page > 1, helper must loop and concat all pages."""
    page1 = [_hour_row("c1", "2026-05-19 00:00:00", 1.0, 2.0, 1)]
    page2 = [_hour_row("c2", "2026-05-19 01:00:00", 3.0, 4.0, 1)]
    page3 = [_hour_row("c3", "2026-05-19 02:00:00", 5.0, 6.0, 1)]

    mock_client._make_request.side_effect = [
        {
            "code": 0,
            "data": {"list": page1, "page_info": {"total_page": 3, "total_number": 3}},
        },
        {
            "code": 0,
            "data": {"list": page2, "page_info": {"total_page": 3, "total_number": 3}},
        },
        {
            "code": 0,
            "data": {"list": page3, "page_info": {"total_page": 3, "total_number": 3}},
        },
    ]

    out = await _fetch_hourly_by_dim(
        mock_client,
        "adv1",
        "2026-05-19",
        ["store1"],
        dimensions=["campaign_id"],
        metrics=["cost", "gross_revenue", "orders"],
    )

    assert len(out) == 3
    assert mock_client._make_request.call_count == 3
    # Verify page parameter advanced
    pages_sent = [c.args[2]["page"] for c in mock_client._make_request.call_args_list]
    assert pages_sent == [1, 2, 3]


@pytest.mark.asyncio
async def test_fetch_hourly_by_dim_filtering_passthrough(mock_client):
    """`filtering` dict (campaign_ids / item_group_ids) must reach the API."""
    mock_client._make_request.return_value = {
        "code": 0,
        "data": {"list": [], "page_info": {"total_page": 1, "total_number": 0}},
    }

    filtering = {"campaign_ids": ["camp_a", "camp_b"], "item_group_ids": ["g1"]}
    await _fetch_hourly_by_dim(
        mock_client,
        "adv1",
        "2026-05-19",
        ["store1"],
        dimensions=["item_id"],
        metrics=["cost", "gross_revenue", "orders"],
        filtering=filtering,
    )

    import json as _json

    sent_params = mock_client._make_request.call_args.args[2]
    assert _json.loads(sent_params["filtering"]) == filtering


@pytest.mark.asyncio
async def test_fetch_hourly_by_dim_nonzero_code_raises(mock_client):
    """Non-zero `code` must surface as Exception (mirrors `_fetch_hourly`)."""
    mock_client._make_request.return_value = {
        "code": 40100,
        "message": "rate limited",
    }

    with pytest.raises(Exception, match=r"hourly_by_dim.*code=40100"):
        await _fetch_hourly_by_dim(
            mock_client,
            "adv1",
            "2026-05-19",
            ["store1"],
            dimensions=["campaign_id"],
            metrics=["cost"],
        )


# ─── _aggregate_by_dims tests ───────────────────────────────────────────


def _bangkok_hour_row(
    dim_key: str, dim_value: str, hour_local: str, cost: float, gmv: float, orders: int
) -> dict:
    """Build a row as TikTok returns it: stat_time_hour string is in advertiser
    local time (Bangkok in these tests). The `_aggregate_by_dims` helper is
    responsible for converting to UTC via `ad_zone` and filtering."""
    return {
        "dimensions": {dim_key: dim_value, "stat_time_hour": hour_local},
        "metrics": {
            "cost": str(cost),
            "gross_revenue": str(gmv),
            "orders": str(orders),
        },
    }


def test_aggregate_by_dims_sums_within_window():
    """Bangkok hours falling inside PT-day UTC window should sum per campaign;
    hours outside the window should be dropped."""
    # Shop PT day 2026-05-19 = PT 00:00–24:00 = UTC 07:00 May 19 – 07:00 May 20.
    # Bangkok (UTC+7) hours equivalent: BKK 14:00 May 19 – 14:00 May 20.
    ad_zone = ZoneInfo("Asia/Bangkok")
    pt_zone = ZoneInfo("America/Los_Angeles")
    start_utc = datetime(2026, 5, 19, 7, 0, tzinfo=timezone.utc)
    end_utc = datetime(2026, 5, 20, 7, 0, tzinfo=timezone.utc)
    now_utc = datetime(2026, 5, 20, 8, 0, tzinfo=timezone.utc)  # PT day fully past

    rows = [
        # OUT — before window (BKK 13:00 May 19 = UTC 06:00 May 19)
        _bangkok_hour_row("campaign_id", "c1", "2026-05-19 13:00:00", 100.0, 0.0, 0),
        # IN  — first valid hour (BKK 14:00 May 19 = UTC 07:00 May 19)
        _bangkok_hour_row("campaign_id", "c1", "2026-05-19 14:00:00", 10.0, 30.0, 1),
        # IN  — second valid hour, same campaign (sum target)
        _bangkok_hour_row("campaign_id", "c1", "2026-05-19 20:00:00", 15.0, 50.0, 2),
        # IN  — different campaign (separate group)
        _bangkok_hour_row("campaign_id", "c2", "2026-05-19 18:00:00", 8.0, 0.0, 0),
        # IN  — Bangkok next day, still within PT-day window
        _bangkok_hour_row("campaign_id", "c1", "2026-05-20 13:00:00", 5.0, 20.0, 1),
        # OUT — at exact end_utc (BKK 14:00 May 20 = UTC 07:00 May 20) excluded
        _bangkok_hour_row("campaign_id", "c1", "2026-05-20 14:00:00", 999.0, 0.0, 0),
        # SKIP — TikTok "-" placeholder
        _bangkok_hour_row("campaign_id", "c1", "-", 7777.0, 0.0, 0),
    ]

    out, last_utc, hours = _aggregate_by_dims(
        rows,
        dimensions=["campaign_id"],
        metrics=["cost", "gross_revenue", "orders"],
        start_utc=start_utc,
        end_utc=end_utc,
        ad_zone=ad_zone,
        now_utc=now_utc,
    )

    by_id = {r["dimensions"]["campaign_id"]: r["metrics"] for r in out}
    assert set(by_id.keys()) == {"c1", "c2"}
    # c1: 10 + 15 + 5 = 30 cost; 30 + 50 + 20 = 100 gmv; 1 + 2 + 1 = 4 orders
    assert float(by_id["c1"]["cost"]) == 30.0
    assert float(by_id["c1"]["gross_revenue"]) == 100.0
    assert float(by_id["c1"]["orders"]) == 4.0
    # c2: 8 cost
    assert float(by_id["c2"]["cost"]) == 8.0
    assert hours == 4  # 4 IN rows accepted
    # last_row_utc should be Bangkok 2026-05-20 13:00 = UTC 06:00 May 20
    assert last_utc == datetime(2026, 5, 20, 6, 0, tzinfo=timezone.utc)


def test_aggregate_by_dims_item_id_grouping():
    """`dimensions=["item_id"]` aggregates correctly; verifies parametric dim
    handling beyond `campaign_id`."""
    ad_zone = ZoneInfo("UTC")  # simplify: no tz conversion
    start_utc = datetime(2026, 5, 19, 0, 0, tzinfo=timezone.utc)
    end_utc = datetime(2026, 5, 20, 0, 0, tzinfo=timezone.utc)
    now_utc = datetime(2026, 5, 20, 0, 1, tzinfo=timezone.utc)

    rows = [
        _bangkok_hour_row("item_id", "vid_A", "2026-05-19 10:00:00", 5.0, 20.0, 1),
        _bangkok_hour_row("item_id", "vid_A", "2026-05-19 11:00:00", 7.0, 30.0, 1),
        _bangkok_hour_row("item_id", "vid_B", "2026-05-19 12:00:00", 3.0, 0.0, 0),
    ]

    out, _, hours = _aggregate_by_dims(
        rows,
        dimensions=["item_id"],
        metrics=["cost", "gross_revenue", "orders"],
        start_utc=start_utc,
        end_utc=end_utc,
        ad_zone=ad_zone,
        now_utc=now_utc,
    )

    by_id = {r["dimensions"]["item_id"]: r["metrics"] for r in out}
    assert float(by_id["vid_A"]["cost"]) == 12.0
    assert float(by_id["vid_A"]["gross_revenue"]) == 50.0
    assert float(by_id["vid_B"]["cost"]) == 3.0
    assert hours == 3


def test_aggregate_by_dims_future_hour_excluded():
    """Hours past `now_utc` (e.g. partial-day query mid-day) must not be
    included even if they fall inside [start_utc, end_utc)."""
    ad_zone = ZoneInfo("UTC")
    start_utc = datetime(2026, 5, 19, 0, 0, tzinfo=timezone.utc)
    end_utc = datetime(2026, 5, 20, 0, 0, tzinfo=timezone.utc)
    now_utc = datetime(2026, 5, 19, 12, 0, tzinfo=timezone.utc)  # mid-day

    rows = [
        # past: included
        _bangkok_hour_row("campaign_id", "c1", "2026-05-19 08:00:00", 10.0, 0, 0),
        _bangkok_hour_row("campaign_id", "c1", "2026-05-19 11:00:00", 5.0, 0, 0),
        # future relative to now_utc: excluded
        _bangkok_hour_row("campaign_id", "c1", "2026-05-19 15:00:00", 999.0, 0, 0),
    ]

    out, _, hours = _aggregate_by_dims(
        rows,
        dimensions=["campaign_id"],
        metrics=["cost", "gross_revenue", "orders"],
        start_utc=start_utc,
        end_utc=end_utc,
        ad_zone=ad_zone,
        now_utc=now_utc,
    )

    assert len(out) == 1
    assert float(out[0]["metrics"]["cost"]) == 15.0
    assert hours == 2


# ─── get_gmvmax_reports_aligned (end-to-end) ─────────────────────────────


@pytest.fixture
def clear_caches():
    """Reset tz cache + currency cache between tests so each test injects fresh."""
    from tiktok_ads_mcp import currency_cache as _ccm
    from tiktok_ads_mcp.tz_cache import _tz_cache

    _tz_cache.clear()
    _ccm._currency_cache.clear()
    yield
    _tz_cache.clear()
    _ccm._currency_cache.clear()


@pytest.fixture
def relax_completeness():
    """Lillian fixtures only seed 4 hours; production threshold (2h lag) would
    falsely trigger. Real completeness tests use a dedicated test with the
    production threshold restored."""
    from unittest.mock import patch

    with patch("tiktok_ads_mcp.tools.gmvmax_report_aligned._HOURS_LAG_TOLERANCE", 48):
        yield


def _bkk_item_row(
    item_id: str, hour_local: str, cost: float, gmv: float, orders: int
) -> dict:
    """Item-level Bangkok-local hourly row (THB amounts, pre-FX)."""
    return {
        "dimensions": {"item_id": item_id, "stat_time_hour": hour_local},
        "metrics": {
            "cost": str(cost),
            "gross_revenue": str(gmv),
            "orders": str(orders),
        },
    }


@pytest.mark.asyncio
async def test_lillian_pt_window_recovers_dropped_evening_hours(
    mock_client, clear_caches, relax_completeness
):
    """Recreates the Lillian bug (vid 7641536365347228959 on AMSOLAR 0424539,
    PT 5-19): the original path with start_date=2026-05-19 queried Bangkok 5-19
    (PT 5-18 10:00–5-19 10:00) and saw 2 orders. The aligned path with
    shop_tz=PT should query Bangkok 5-19 AND 5-20, then slice to PT 5-19's
    UTC window (07:00 5-19 – 07:00 5-20) and recover all 13 orders.

    Simplified fixture: 4 hourly rows for the same vid:
      - BKK 14:00 May 19 (PT 00:00 5-19) — first hour of PT 5-19, IN
      - BKK 20:00 May 19 (PT 06:00 5-19) — IN
      - BKK 10:00 May 20 (PT 20:00 5-19) — IN (the PT evening that
        Bangkok-day query missed)
      - BKK 14:00 May 20 (PT 00:00 5-20) — at boundary, EXCLUDED
    """
    # Inject ad timezone (Bangkok) + currency (THB) directly into caches so
    # the function doesn't hit /advertiser/info/ during the test.
    from tiktok_ads_mcp import currency_cache as _ccm
    from tiktok_ads_mcp import fx as _fxm
    from tiktok_ads_mcp.tools.gmvmax_report_aligned import get_gmvmax_reports_aligned
    from tiktok_ads_mcp.tz_cache import _tz_cache
    from unittest.mock import patch

    _tz_cache["amsolar"] = ZoneInfo("Asia/Bangkok")
    _ccm._currency_cache["amsolar"] = "THB"

    bkk_may_19_rows = [
        # BKK 13:00 May 19 = PT May 18 23:00 — OUT (before PT 5-19 day)
        _bkk_item_row("vid_lillian", "2026-05-19 13:00:00", 999.0, 0, 0),
        # BKK 14:00 May 19 = PT 00:00 May 19 — IN
        _bkk_item_row("vid_lillian", "2026-05-19 14:00:00", 10.0, 30.0, 1),
        # BKK 20:00 May 19 = PT 06:00 May 19 — IN
        _bkk_item_row("vid_lillian", "2026-05-19 20:00:00", 15.0, 50.0, 2),
    ]
    bkk_may_20_rows = [
        # BKK 10:00 May 20 = PT 20:00 May 19 — IN (the recovered evening hour)
        _bkk_item_row("vid_lillian", "2026-05-20 10:00:00", 200.0, 400.0, 10),
        # BKK 14:00 May 20 = PT 00:00 May 20 — OUT (boundary, exclusive end)
        _bkk_item_row("vid_lillian", "2026-05-20 14:00:00", 999.0, 0, 0),
    ]

    def _resp(rows):
        return {
            "code": 0,
            "data": {
                "list": rows,
                "page_info": {"total_page": 1, "total_number": len(rows)},
            },
        }

    # Order matters: native_dates = [2026-05-19, 2026-05-20], parallel gather
    # may call in either order — but asyncio.gather preserves submission order.
    mock_client._make_request.side_effect = [
        _resp(bkk_may_19_rows),
        _resp(bkk_may_20_rows),
    ]

    # FX: pin to 0.03 THB→USD (close to 1/33.5 real rate)
    with patch.object(_fxm, "_fetch_from_frankfurter", AsyncMock(return_value=0.03)):
        # Fake "now" to be well past PT day end so future-hour filter doesn't drop
        with patch("tiktok_ads_mcp.tools.gmvmax_report_aligned.datetime") as mock_dt:
            mock_dt.now.return_value = datetime(2026, 5, 20, 12, 0, tzinfo=timezone.utc)
            mock_dt.strptime = datetime.strptime
            mock_dt.side_effect = lambda *a, **kw: datetime(*a, **kw)

            result = await get_gmvmax_reports_aligned(
                mock_client,
                "amsolar",
                "2026-05-19",
                "2026-05-19",
                store_ids=["7495609170861329178"],
                dimensions=["item_id"],
                metrics=["cost", "gross_revenue", "orders"],
                shop_tz="America/Los_Angeles",
            )

    # Shape parity with get_gmvmax_reports
    assert result["currency"] == "USD"
    assert result["source_currency"] == "THB"
    assert "page_info" in result and "list" in result

    # Should have aggregated 3 IN hours into a single vid row (+200 cost / +400 gmv / +10 orders)
    assert len(result["list"]) == 1
    row = result["list"][0]
    assert row["dimensions"]["item_id"] == "vid_lillian"
    # Sum: 10 + 15 + 200 = 225 THB cost × 0.03 FX = $6.75
    assert round(float(row["metrics"]["cost"]), 4) == round(225.0 * 0.03, 4)
    # Sum: 30 + 50 + 400 = 480 THB gmv × 0.03 = $14.40
    assert round(float(row["metrics"]["gross_revenue"]), 4) == round(480.0 * 0.03, 4)
    # 1 + 2 + 10 = 13 orders (matches real Lillian 2026-05-19 PT total)
    assert float(row["metrics"]["orders"]) == 13.0


@pytest.mark.asyncio
async def test_aligned_usd_advertiser_no_fx_call(
    mock_client, clear_caches, relax_completeness
):
    """USD advertiser must skip the FX HTTP call (fast-path through helper)."""
    from tiktok_ads_mcp import currency_cache as _ccm
    from tiktok_ads_mcp import fx as _fxm
    from tiktok_ads_mcp.tools.gmvmax_report_aligned import get_gmvmax_reports_aligned
    from tiktok_ads_mcp.tz_cache import _tz_cache
    from unittest.mock import patch

    _tz_cache["usd_adv"] = ZoneInfo("America/Los_Angeles")
    _ccm._currency_cache["usd_adv"] = "USD"

    mock_client._make_request.return_value = {
        "code": 0,
        "data": {
            "list": [
                _bkk_item_row("vid", "2026-05-19 10:00:00", 50.0, 150.0, 3),
            ],
            "page_info": {"total_page": 1, "total_number": 1},
        },
    }

    with patch.object(_fxm, "_fetch_from_frankfurter") as fx_spy:
        with patch("tiktok_ads_mcp.tools.gmvmax_report_aligned.datetime") as mock_dt:
            mock_dt.now.return_value = datetime(2026, 5, 20, 12, 0, tzinfo=timezone.utc)
            mock_dt.strptime = datetime.strptime
            mock_dt.side_effect = lambda *a, **kw: datetime(*a, **kw)

            result = await get_gmvmax_reports_aligned(
                mock_client,
                "usd_adv",
                "2026-05-19",
                "2026-05-19",
                store_ids=["s"],
                dimensions=["item_id"],
                metrics=["cost", "gross_revenue", "orders"],
                shop_tz="America/Los_Angeles",
            )

    fx_spy.assert_not_called()
    assert result["currency"] == "USD"
    assert result["source_currency"] == "USD"
    # Cost passes through unchanged (no FX)
    assert float(result["list"][0]["metrics"]["cost"]) == 50.0


# ─── Shape parity (aligned ↔ get_gmvmax_reports) ─────────────────────────


@pytest.mark.asyncio
async def test_aligned_output_shape_matches_get_gmvmax_reports(
    mock_client, clear_caches, relax_completeness
):
    """Drop-in compatibility: aligned and non-aligned outputs must have the
    same top-level keys and per-row shape so wrappers in core/tiktok_api_mcp.py
    can swap one for the other without caller-side adaptation.

    Validates:
      - Same top-level keys: {page_info, list, currency, source_currency}
      - page_info is a dict (per TikTok's response convention)
      - list items each have {dimensions: dict, metrics: dict}
      - metric values are str (matches get_gmvmax_reports stringification)
      - currency == "USD" (post-FX)
    """
    from tiktok_ads_mcp import currency_cache as _ccm
    from tiktok_ads_mcp import fx as _fxm
    from tiktok_ads_mcp.tools.gmvmax_report_aligned import get_gmvmax_reports_aligned
    from tiktok_ads_mcp.tools.gmvmax_reports import get_gmvmax_reports
    from tiktok_ads_mcp.tz_cache import _tz_cache
    from unittest.mock import patch

    _tz_cache["adv_x"] = ZoneInfo("America/Los_Angeles")
    _ccm._currency_cache["adv_x"] = "USD"

    # Non-aligned fixture: daily row (stat_time_day dimension is the typical
    # one used by lark-bot's get_gmvmax_campaign_reports default).
    daily_response = {
        "code": 0,
        "data": {
            "list": [
                {
                    "dimensions": {"campaign_id": "c1"},
                    "metrics": {
                        "cost": "100.0",
                        "gross_revenue": "300.0",
                        "orders": "5",
                    },
                }
            ],
            "page_info": {"total_number": 1, "total_page": 1, "page": 1},
        },
    }

    # Aligned fixture: hourly rows that aggregate to the same totals
    hourly_response = {
        "code": 0,
        "data": {
            "list": [
                _bkk_item_row("c1", "2026-05-19 10:00:00", 100.0, 300.0, 5),
            ],
            "page_info": {"total_page": 1, "total_number": 1},
        },
    }
    # Aligned variant uses item_id key for dims, swap key for parity
    hourly_response["data"]["list"][0]["dimensions"] = {
        "campaign_id": "c1",
        "stat_time_hour": "2026-05-19 10:00:00",
    }

    # Non-aligned call
    mock_client._make_request.return_value = daily_response
    non_aligned = await get_gmvmax_reports(
        mock_client,
        "adv_x",
        "2026-05-19",
        "2026-05-19",
        store_ids=["s"],
        dimensions=["campaign_id"],
        metrics=["cost", "gross_revenue", "orders"],
    )

    # Aligned call (USD adv, but tz check is what triggers aligned path; we just
    # exercise the shape generator)
    mock_client._make_request.return_value = hourly_response
    with patch.object(_fxm, "_fetch_from_frankfurter", AsyncMock(return_value=1.0)):
        with patch("tiktok_ads_mcp.tools.gmvmax_report_aligned.datetime") as mock_dt:
            mock_dt.now.return_value = datetime(2026, 5, 20, 12, 0, tzinfo=timezone.utc)
            mock_dt.strptime = datetime.strptime
            mock_dt.side_effect = lambda *a, **kw: datetime(*a, **kw)

            aligned = await get_gmvmax_reports_aligned(
                mock_client,
                "adv_x",
                "2026-05-19",
                "2026-05-19",
                store_ids=["s"],
                dimensions=["campaign_id"],
                metrics=["cost", "gross_revenue", "orders"],
                shop_tz="America/Los_Angeles",
            )

    # Top-level keys
    assert (
        set(aligned.keys())
        == set(non_aligned.keys())
        == {
            "page_info",
            "list",
            "currency",
            "source_currency",
        }
    )
    # Types parity
    assert isinstance(aligned["page_info"], dict)
    assert isinstance(non_aligned["page_info"], dict)
    assert isinstance(aligned["list"], list)
    assert isinstance(non_aligned["list"], list)
    assert isinstance(aligned["currency"], str)
    assert isinstance(non_aligned["currency"], str)
    assert isinstance(aligned["source_currency"], str)

    # Per-row shape
    for src in (aligned, non_aligned):
        for row in src["list"]:
            assert set(row.keys()) == {"dimensions", "metrics"}
            assert isinstance(row["dimensions"], dict)
            assert isinstance(row["metrics"], dict)
            # Metric values stringified (caller does float(...))
            for v in row["metrics"].values():
                assert isinstance(v, str)

    # Numerical parity for this fixture (since aligned + USD = no FX)
    a_metrics = aligned["list"][0]["metrics"]
    n_metrics = non_aligned["list"][0]["metrics"]
    for k in ("cost", "gross_revenue", "orders"):
        assert float(a_metrics[k]) == float(n_metrics[k])


# ─── Dispatcher (GMVMAX_ALIGNED_MODE) ────────────────────────────────────


@pytest.fixture
def dispatcher_env(monkeypatch):
    """Helper to set/clear GMVMAX_ALIGNED_MODE around each test."""

    def _set(mode):
        if mode is None:
            monkeypatch.delenv("GMVMAX_ALIGNED_MODE", raising=False)
        else:
            monkeypatch.setenv("GMVMAX_ALIGNED_MODE", mode)

    return _set


@pytest.mark.asyncio
async def test_dispatcher_off_uses_original(mock_client, clear_caches, dispatcher_env):
    """Mode=off → original path regardless of shop_tz argument."""
    from tiktok_ads_mcp import currency_cache as _ccm
    from tiktok_ads_mcp.tools.gmvmax_reports import get_gmvmax_reports
    from tiktok_ads_mcp.tz_cache import _tz_cache

    dispatcher_env("off")
    _tz_cache["adv1"] = ZoneInfo("Asia/Bangkok")  # cross-tz vs PT shop
    _ccm._currency_cache["adv1"] = "USD"

    mock_client._make_request.return_value = {
        "code": 0,
        "data": {
            "list": [
                {
                    "dimensions": {"campaign_id": "c1"},
                    "metrics": {
                        "cost": "100.0",
                        "gross_revenue": "200.0",
                        "orders": "5",
                    },
                }
            ],
            "page_info": {"total_number": 1},
        },
    }

    result = await get_gmvmax_reports(
        mock_client,
        "adv1",
        "2026-05-19",
        "2026-05-19",
        store_ids=["s"],
        dimensions=["campaign_id"],
        metrics=["cost", "gross_revenue", "orders"],
        shop_tz="America/Los_Angeles",
    )

    # Original path makes exactly 1 API call (no hourly+native_dates expansion)
    assert mock_client._make_request.call_count == 1
    assert float(result["list"][0]["metrics"]["cost"]) == 100.0


@pytest.mark.asyncio
async def test_dispatcher_no_shop_tz_skips_align(
    mock_client, clear_caches, dispatcher_env
):
    """No shop_tz passed → original path even when mode=on (existing callers
    without shop_tz must keep current behavior exactly)."""
    from tiktok_ads_mcp import currency_cache as _ccm
    from tiktok_ads_mcp.tools.gmvmax_reports import get_gmvmax_reports
    from tiktok_ads_mcp.tz_cache import _tz_cache

    dispatcher_env("on")
    _tz_cache["adv1"] = ZoneInfo("Asia/Bangkok")
    _ccm._currency_cache["adv1"] = "USD"

    mock_client._make_request.return_value = {
        "code": 0,
        "data": {"list": [], "page_info": {"total_number": 0}},
    }

    await get_gmvmax_reports(
        mock_client,
        "adv1",
        "2026-05-19",
        "2026-05-19",
        store_ids=["s"],
        dimensions=["campaign_id"],
        metrics=["cost"],
    )

    assert mock_client._make_request.call_count == 1  # original path only


@pytest.mark.asyncio
async def test_dispatcher_same_tz_skips_align(
    mock_client, clear_caches, dispatcher_env
):
    """shop_tz == adv_tz → original path (no FX, no hourly multiplication)."""
    from tiktok_ads_mcp import currency_cache as _ccm
    from tiktok_ads_mcp.tools.gmvmax_reports import get_gmvmax_reports
    from tiktok_ads_mcp.tz_cache import _tz_cache

    dispatcher_env("on")
    _tz_cache["adv_pt"] = ZoneInfo("America/Los_Angeles")
    _ccm._currency_cache["adv_pt"] = "USD"

    mock_client._make_request.return_value = {
        "code": 0,
        "data": {"list": [], "page_info": {"total_number": 0}},
    }

    await get_gmvmax_reports(
        mock_client,
        "adv_pt",
        "2026-05-19",
        "2026-05-19",
        store_ids=["s"],
        dimensions=["campaign_id"],
        metrics=["cost"],
        shop_tz="America/Los_Angeles",
    )

    assert mock_client._make_request.call_count == 1


@pytest.mark.asyncio
async def test_dispatcher_time_dimension_skips_align(
    mock_client, clear_caches, dispatcher_env
):
    """If user already groups by stat_time_day/hour, skip align (don't
    double-aggregate)."""
    from tiktok_ads_mcp import currency_cache as _ccm
    from tiktok_ads_mcp.tools.gmvmax_reports import get_gmvmax_reports
    from tiktok_ads_mcp.tz_cache import _tz_cache

    dispatcher_env("on")
    _tz_cache["adv1"] = ZoneInfo("Asia/Bangkok")
    _ccm._currency_cache["adv1"] = "USD"

    mock_client._make_request.return_value = {
        "code": 0,
        "data": {"list": [], "page_info": {"total_number": 0}},
    }

    await get_gmvmax_reports(
        mock_client,
        "adv1",
        "2026-05-19",
        "2026-05-19",
        store_ids=["s"],
        dimensions=["advertiser_id", "stat_time_day"],  # caller wants per-day
        metrics=["cost"],
        shop_tz="America/Los_Angeles",
    )

    assert mock_client._make_request.call_count == 1  # original only


@pytest.mark.asyncio
async def test_dispatcher_shadow_runs_both_returns_original(
    mock_client, clear_caches, dispatcher_env, relax_completeness, caplog
):
    """Shadow mode: BOTH paths run; journal log emitted; original returned."""
    from tiktok_ads_mcp import currency_cache as _ccm
    from tiktok_ads_mcp import fx as _fxm
    from tiktok_ads_mcp.tools.gmvmax_reports import get_gmvmax_reports
    from tiktok_ads_mcp.tz_cache import _tz_cache
    from unittest.mock import patch

    dispatcher_env("shadow")
    _tz_cache["adv1"] = ZoneInfo("Asia/Bangkok")
    _ccm._currency_cache["adv1"] = "THB"

    # Build distinct fixtures so we can tell which path "won"
    original_resp = {
        "code": 0,
        "data": {
            "list": [
                {
                    "dimensions": {"campaign_id": "c1"},
                    "metrics": {
                        "cost": "23.5",
                        "gross_revenue": "69.81",
                        "orders": "2",
                    },
                }
            ],
            "page_info": {"total_number": 1},
        },
    }
    hourly_resp = {
        "code": 0,
        "data": {
            "list": [
                _bkk_item_row("c1", "2026-05-19 14:00:00", 197.6, 417.99, 13),
            ],
            "page_info": {"total_page": 1, "total_number": 1},
        },
    }
    # Fix the item_id key → campaign_id for shape parity
    hourly_resp["data"]["list"][0]["dimensions"] = {
        "campaign_id": "c1",
        "stat_time_hour": "2026-05-19 14:00:00",
    }

    # Call sequence: original (1 call) → aligned (2 native dates × 1 page each = 2 calls)
    mock_client._make_request.side_effect = [
        original_resp,
        hourly_resp,
        hourly_resp,
    ]

    with patch.object(_fxm, "_fetch_from_frankfurter", AsyncMock(return_value=0.03)):
        with patch("tiktok_ads_mcp.tools.gmvmax_report_aligned.datetime") as mock_dt:
            mock_dt.now.return_value = datetime(2026, 5, 20, 12, 0, tzinfo=timezone.utc)
            mock_dt.strptime = datetime.strptime
            mock_dt.side_effect = lambda *a, **kw: datetime(*a, **kw)

            import logging

            caplog.set_level(logging.INFO, logger="tiktok_ads_mcp.tools.gmvmax_reports")
            result = await get_gmvmax_reports(
                mock_client,
                "adv1",
                "2026-05-19",
                "2026-05-19",
                store_ids=["s"],
                dimensions=["campaign_id"],
                metrics=["cost", "gross_revenue", "orders"],
                shop_tz="America/Los_Angeles",
            )

    # Both paths ran (1 + 2 calls)
    assert mock_client._make_request.call_count == 3
    # Returned is the ORIGINAL (cost ≈ 23.5 × FX 0.03)
    assert round(float(result["list"][0]["metrics"]["cost"]), 4) == round(
        23.5 * 0.03, 4
    )
    # Shadow log emitted with diff
    assert any("[aligned_shadow]" in rec.message for rec in caplog.records)


@pytest.mark.asyncio
async def test_dispatcher_on_uses_aligned(
    mock_client, clear_caches, dispatcher_env, relax_completeness
):
    """Mode=on + cross-tz + non-time dims → aligned path returned."""
    from tiktok_ads_mcp import currency_cache as _ccm
    from tiktok_ads_mcp import fx as _fxm
    from tiktok_ads_mcp.tools.gmvmax_reports import get_gmvmax_reports
    from tiktok_ads_mcp.tz_cache import _tz_cache
    from unittest.mock import patch

    dispatcher_env("on")
    _tz_cache["adv1"] = ZoneInfo("Asia/Bangkok")
    _ccm._currency_cache["adv1"] = "THB"

    hourly_rows_d1 = [
        _bkk_item_row("c1", "2026-05-19 14:00:00", 100.0, 200.0, 5),
    ]
    hourly_rows_d2 = [
        _bkk_item_row("c1", "2026-05-20 10:00:00", 197.6, 417.99, 13),
    ]
    # Use campaign_id key for parity
    for r in hourly_rows_d1 + hourly_rows_d2:
        r["dimensions"] = {
            "campaign_id": "c1",
            "stat_time_hour": r["dimensions"]["stat_time_hour"],
        }

    mock_client._make_request.side_effect = [
        {
            "code": 0,
            "data": {
                "list": hourly_rows_d1,
                "page_info": {"total_page": 1, "total_number": 1},
            },
        },
        {
            "code": 0,
            "data": {
                "list": hourly_rows_d2,
                "page_info": {"total_page": 1, "total_number": 1},
            },
        },
    ]

    with patch.object(_fxm, "_fetch_from_frankfurter", AsyncMock(return_value=0.03)):
        with patch("tiktok_ads_mcp.tools.gmvmax_report_aligned.datetime") as mock_dt:
            mock_dt.now.return_value = datetime(2026, 5, 20, 12, 0, tzinfo=timezone.utc)
            mock_dt.strptime = datetime.strptime
            mock_dt.side_effect = lambda *a, **kw: datetime(*a, **kw)

            result = await get_gmvmax_reports(
                mock_client,
                "adv1",
                "2026-05-19",
                "2026-05-19",
                store_ids=["s"],
                dimensions=["campaign_id"],
                metrics=["cost", "gross_revenue", "orders"],
                shop_tz="America/Los_Angeles",
            )

    # Aligned path only — 2 native_date fetches, no original 1-call
    assert mock_client._make_request.call_count == 2
    # Both hours aggregated: 100 + 197.6 = 297.6 THB × 0.03 = 8.928 USD
    assert round(float(result["list"][0]["metrics"]["cost"]), 4) == round(
        297.6 * 0.03, 4
    )
    assert float(result["list"][0]["metrics"]["orders"]) == 18.0


@pytest.mark.asyncio
async def test_dispatcher_on_falls_back_on_aligned_exception(
    mock_client, clear_caches, dispatcher_env
):
    """Aligned-path exception in `on` mode must fail-soft to original (the
    crown jewel: morning_brief MUST NOT break)."""
    from tiktok_ads_mcp import currency_cache as _ccm
    from tiktok_ads_mcp.tools.gmvmax_reports import get_gmvmax_reports
    from tiktok_ads_mcp.tz_cache import _tz_cache

    dispatcher_env("on")
    _tz_cache["adv1"] = ZoneInfo("Asia/Bangkok")
    _ccm._currency_cache["adv1"] = "USD"

    # Sequence: aligned issues 2 concurrent hourly fetches (2 native dates).
    # Each raises a plain Exception (non-zero TikTok code, NOT a
    # TikTokRateLimitError so @api_retry doesn't catch it — aligned raises out
    # immediately). Dispatcher logs and falls back to original, which then
    # makes 1 successful call.
    #
    # Why not a retryable exception class? Aligned's @api_retry only retries
    # TikTokRateLimitError / TikTokIncompleteDataError. A `code=40100` from
    # _fetch_hourly_by_dim becomes plain Exception → no retry → fast fall-through
    # to the dispatcher's except clause. That IS the fail-soft contract we want
    # to test.
    rate_limit_resp = {"code": 40100, "message": "rate limited"}
    original_success = {
        "code": 0,
        "data": {
            "list": [
                {
                    "dimensions": {"campaign_id": "c1"},
                    "metrics": {
                        "cost": "50.0",
                        "gross_revenue": "100.0",
                        "orders": "3",
                    },
                }
            ],
            "page_info": {"total_number": 1},
        },
    }
    # 2 concurrent aligned fetches (both fail) + 1 original fallback (success)
    mock_client._make_request.side_effect = [
        rate_limit_resp,
        rate_limit_resp,
        original_success,
    ]

    result = await get_gmvmax_reports(
        mock_client,
        "adv1",
        "2026-05-19",
        "2026-05-19",
        store_ids=["s"],
        dimensions=["campaign_id"],
        metrics=["cost", "gross_revenue", "orders"],
        shop_tz="America/Los_Angeles",
    )

    # Result is original-path success despite aligned failures
    assert float(result["list"][0]["metrics"]["cost"]) == 50.0


# ─── _vid_prorate_aligned (Solution H) ───────────────────────────────────
#
# Real-world baseline: AMSOLAR 0424539 (THB advertiser, Bangkok UTC+7) on
# Hiileathy NAD+ store (PT shop) for PT 2026-05-19. Hardcoded values come
# from live TikTok API queries documented in the planning conversation; they
# are the "truth" we want the proration math to reproduce. See the plan file
# at /Users/shining/.claude/plans/dynamic-spinning-seahorse.md.
#
# PT 5-19 UTC window = [UTC 5-19 07:00, UTC 5-20 07:00)
# Covered by BKK 5-19 14:00-23:00 (10h) + BKK 5-20 00:00-13:00 (14h)


# Pinned campaign-hourly USD-converted truth for campaign 1865181863053394.
# Values are post-FX (THB×0.030 ≈ USD), so the proration test can use them
# directly without additional FX gymnastics — the helper applies FX once
# to the prorated rows, and our fixture pretends the raw API already returns
# USD-magnitude amounts (we patch _fetch_from_frankfurter to rate=1.0).
LILLIAN_CAMPAIGN = "1865181863053394"
LILLIAN_VID = "7641536365347228959"

# BKK 5-19 hourly (24 entries; values match live API capture)
_BKK_5_19_HOURLY = {
    "00:00:00": (12.0872, 40.0164, 1),
    "01:00:00": (17.1928, 0.0, 0),
    "02:00:00": (1.6832, 29.9999, 1),
    "03:00:00": (2.8397, 0.0, 0),
    "04:00:00": (2.9331, 0.0, 0),
    "05:00:00": (3.3186, 0.0, 0),
    "06:00:00": (5.39, 29.9999, 1),
    "07:00:00": (6.0879, 0.0, 0),
    "08:00:00": (12.1044, 0.0, 0),
    "09:00:00": (5.4246, 0.0, 0),
    "10:00:00": (2.1648, 0.0, 0),
    "11:00:00": (1.1954, 30.0367, 1),
    "12:00:00": (0.8356, 0.0, 0),
    "13:00:00": (1.0993, 0.0, 0),
    "14:00:00": (0.6537, 0.0, 0),
    "15:00:00": (0.564, 0.0, 0),
    "16:00:00": (1.1783, 0.0, 0),
    "17:00:00": (0.6589, 0.0, 0),
    "18:00:00": (2.2377, 0.0, 0),
    "19:00:00": (1.3574, 30.0367, 1),
    "20:00:00": (4.7538, 0.0, 0),
    "21:00:00": (10.5866, 0.0, 0),
    "22:00:00": (20.0264, 40.0654, 1),
    "23:00:00": (19.1972, 0.0, 0),
}

# BKK 5-20 hourly (24 entries)
_BKK_5_20_HOURLY = {
    "00:00:00": (20.0301, 40.0654, 1),
    "01:00:00": (20.6838, 110.1674, 3),
    "02:00:00": (21.6021, 100.1384, 3),
    "03:00:00": (31.2428, 90.1098, 3),
    "04:00:00": (39.2205, 0.0, 0),
    "05:00:00": (33.6348, 70.102, 2),
    "06:00:00": (34.3737, 90.1098, 3),
    "07:00:00": (15.8896, 0.0, 0),
    "08:00:00": (30.97, 69.6734, 2),
    "09:00:00": (20.3972, 69.6734, 2),
    "10:00:00": (0.4945, 0.0, 0),
    "11:00:00": (0.0, 0.0, 0),
    "12:00:00": (0.0, 0.0, 0),
    "13:00:00": (0.0, 0.0, 0),
    "14:00:00": (0.0, 0.0, 0),
    "15:00:00": (0.0, 0.0, 0),
    "16:00:00": (0.0, 0.0, 0),
    "17:00:00": (0.0, 0.0, 0),
    "18:00:00": (0.0, 0.0, 0),
    "19:00:00": (0.0, 0.0, 0),
    "20:00:00": (0.0, 0.0, 0),
    "21:00:00": (0.0, 0.0, 0),
    "22:00:00": (0.0, 0.0, 0),
    "23:00:00": (0.0, 0.0, 0),
}


def _camp_hourly_rows(day_iso: str, hours_dict: dict) -> list:
    """Build campaign-hourly mock rows in the shape get_gmvmax_reports returns."""
    rows = []
    for h, (c, g, o) in hours_dict.items():
        rows.append(
            {
                "dimensions": {
                    "campaign_id": LILLIAN_CAMPAIGN,
                    "stat_time_hour": f"{day_iso} {h}",
                },
                "metrics": {
                    "cost": str(c),
                    "gross_revenue": str(g),
                    "orders": str(o),
                },
            }
        )
    return rows


# Vid 2-day totals at item_id level (live API capture):
# - BKK 5-19 vid total: cost=$23.5, gmv=$69.81, orders=2
# - BKK 5-20 vid total (within campaign + item_group): cost=$197.60, gmv=$417.99, orders=13
_VID_5_19_DAILY = {"cost": 23.5, "gross_revenue": 69.81, "orders": 2}
_VID_5_20_DAILY = {"cost": 197.6, "gross_revenue": 417.99, "orders": 13}


def _vid_daily_response(vid_metrics: dict) -> dict:
    """One-page response shape from get_gmvmax_reports for a single vid."""
    return {
        "code": 0,
        "data": {
            "list": [
                {
                    "dimensions": {"item_id": LILLIAN_VID},
                    "metrics": {
                        "cost": str(vid_metrics["cost"]),
                        "gross_revenue": str(vid_metrics["gross_revenue"]),
                        "orders": str(vid_metrics["orders"]),
                    },
                }
            ],
            "page_info": {"total_page": 1, "total_number": 1},
        },
    }


@pytest.mark.asyncio
async def test_lillian_vid_prorate_matches_baseline(
    mock_client, clear_caches, relax_completeness
):
    """End-to-end Solution H validation against PT 2026-05-19 baseline:

    Expected per-vid proration math (computed by hand from real API capture):
      camp_pt_5_19   = (cost=$329.75, gmv=$710.14, orders=21)
      camp_2day_advtz = (cost=$404.11, gmv=$839.77, orders=25)
      vid_2day_advtz  = (cost=$221.10, gmv=$487.80, orders=15)
      vid_pt_aligned  = camp_pt × (vid_2day / camp_2day)
                      ≈ cost=$180.40, gmv=$412.46, orders=12.6

    Tolerance: 1% absolute on the rounded values (we control the fixture, so
    tighter than the ~5-15% real-world spread).
    """
    from tiktok_ads_mcp import currency_cache as _ccm
    from tiktok_ads_mcp import fx as _fxm
    from tiktok_ads_mcp.tools.gmvmax_report_aligned import _vid_prorate_aligned
    from tiktok_ads_mcp.tz_cache import _tz_cache
    from unittest.mock import patch

    # Pretend AMSOLAR's tz is Bangkok and currency is THB-but-rate-1.0 (so the
    # fixture numbers are already USD-magnitude).
    _tz_cache["amsolar"] = ZoneInfo("Asia/Bangkok")
    _ccm._currency_cache["amsolar"] = "THB"

    # Call sequence within one shop-tz day:
    #   1. campaign-hourly fetch on BKK 5-19 (asyncio.gather pos 0)
    #   2. campaign-hourly fetch on BKK 5-20 (gather pos 1)
    #   3. vid-daily fetch on BKK 5-19 (per-campaign loop, gather pos 0)
    #   4. vid-daily fetch on BKK 5-20 (gather pos 1)
    mock_client._make_request.side_effect = [
        # Campaign-hourly responses
        {
            "code": 0,
            "data": {
                "list": _camp_hourly_rows("2026-05-19", _BKK_5_19_HOURLY),
                "page_info": {"total_page": 1, "total_number": 24},
            },
        },
        {
            "code": 0,
            "data": {
                "list": _camp_hourly_rows("2026-05-20", _BKK_5_20_HOURLY),
                "page_info": {"total_page": 1, "total_number": 24},
            },
        },
        # Vid-daily responses (one per native_date for the single campaign)
        _vid_daily_response(_VID_5_19_DAILY),
        _vid_daily_response(_VID_5_20_DAILY),
    ]

    # FX rate=1.0 → fixture numbers stay magnitude-equal post-conversion
    with patch.object(_fxm, "_fetch_from_frankfurter", AsyncMock(return_value=1.0)):
        with patch("tiktok_ads_mcp.tools.gmvmax_report_aligned.datetime") as mock_dt:
            mock_dt.now.return_value = datetime(2026, 5, 20, 14, 0, tzinfo=timezone.utc)
            mock_dt.strptime = datetime.strptime
            mock_dt.side_effect = lambda *a, **kw: datetime(*a, **kw)

            result = await _vid_prorate_aligned(
                mock_client,
                "amsolar",
                "2026-05-19",
                "2026-05-19",
                store_ids=["7495609170861329178"],
                metrics=["cost", "gross_revenue", "orders"],
                shop_tz="America/Los_Angeles",
                filtering={
                    "campaign_ids": [LILLIAN_CAMPAIGN],
                    "item_group_ids": ["1731864028716962586"],
                },
            )

    # Shape parity
    assert result["currency"] == "USD"
    assert result["source_currency"] == "THB"
    assert "page_info" in result and "list" in result
    assert len(result["list"]) == 1
    row = result["list"][0]
    assert row["dimensions"]["item_id"] == LILLIAN_VID

    # Numeric expectations (hand-computed, see docstring above)
    cost_actual = float(row["metrics"]["cost"])
    gmv_actual = float(row["metrics"]["gross_revenue"])
    orders_actual = float(row["metrics"]["orders"])

    EXPECTED_COST = 180.40
    EXPECTED_GMV = 412.46
    EXPECTED_ORDERS = 12.6

    # Allow 1% tolerance — proration math is deterministic, fixture is pinned
    assert abs(cost_actual - EXPECTED_COST) / EXPECTED_COST < 0.01, (
        f"cost={cost_actual:.2f} vs expected={EXPECTED_COST:.2f}"
    )
    assert abs(gmv_actual - EXPECTED_GMV) / EXPECTED_GMV < 0.01, (
        f"gmv={gmv_actual:.2f} vs expected={EXPECTED_GMV:.2f}"
    )
    assert abs(orders_actual - EXPECTED_ORDERS) / EXPECTED_ORDERS < 0.01, (
        f"orders={orders_actual:.2f} vs expected={EXPECTED_ORDERS:.2f}"
    )


@pytest.mark.asyncio
async def test_vid_prorate_requires_campaign_filter(mock_client, clear_caches):
    """_vid_prorate_aligned must raise when filtering.campaign_ids absent."""
    from tiktok_ads_mcp import currency_cache as _ccm
    from tiktok_ads_mcp.tools.gmvmax_report_aligned import _vid_prorate_aligned
    from tiktok_ads_mcp.tz_cache import _tz_cache

    _tz_cache["amsolar"] = ZoneInfo("Asia/Bangkok")
    _ccm._currency_cache["amsolar"] = "THB"

    with pytest.raises(ValueError, match="requires filtering.campaign_ids"):
        await _vid_prorate_aligned(
            mock_client,
            "amsolar",
            "2026-05-19",
            "2026-05-19",
            store_ids=["s1"],
            metrics=["cost", "gross_revenue", "orders"],
            shop_tz="America/Los_Angeles",
            filtering={},  # missing campaign_ids
        )


@pytest.mark.asyncio
async def test_vid_prorate_drops_non_additive_metrics(
    mock_client, clear_caches, relax_completeness
):
    """Ratio/rate metrics (roi, cost_per_order, *_rate) can't be summed and
    must be filtered out before fetch — verify the helper raises when ALL
    supplied metrics are non-additive."""
    from tiktok_ads_mcp import currency_cache as _ccm
    from tiktok_ads_mcp.tools.gmvmax_report_aligned import _vid_prorate_aligned
    from tiktok_ads_mcp.tz_cache import _tz_cache

    _tz_cache["amsolar"] = ZoneInfo("Asia/Bangkok")
    _ccm._currency_cache["amsolar"] = "THB"

    with pytest.raises(ValueError, match="no additive metrics"):
        await _vid_prorate_aligned(
            mock_client,
            "amsolar",
            "2026-05-19",
            "2026-05-19",
            store_ids=["s1"],
            metrics=["roi", "cost_per_order", "ad_click_rate"],
            shop_tz="America/Los_Angeles",
            filtering={"campaign_ids": ["c1"]},
        )
