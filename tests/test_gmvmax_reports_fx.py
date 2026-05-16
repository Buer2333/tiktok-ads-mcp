"""FX-conversion regression tests for the per-item GMVMAX report tool.

The aligned/range-report variants already test FX (see test_gmvmax_aligned),
but per-item callers (material_report → core.tiktok_api_mcp.get_gmvmax_item_
reports → tools.gmvmax_reports.get_gmvmax_reports) flow through a *different*
function. 2026-05-16 regression: AMSOLAR (THB) item-level cost surfaced raw
in morning_briefing material cards because this code path wasn't FX-aware.
"""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from tiktok_ads_mcp import currency_cache as _ccm
from tiktok_ads_mcp import fx as _fxm
from tiktok_ads_mcp.tools.gmvmax_reports import get_gmvmax_reports


@pytest.fixture
def mock_client():
    client = MagicMock()
    client._make_request = AsyncMock()
    return client


def _item_row(item_id: str, cost: float, gmv: float, orders: int) -> dict:
    return {
        "dimensions": {"item_id": item_id},
        "metrics": {
            "cost": str(cost),
            "gross_revenue": str(gmv),
            "orders": str(orders),
            "net_cost": str(cost * 0.4),  # mimic discount account
        },
    }


@pytest.mark.asyncio
async def test_thb_item_metrics_converted_to_usd(mock_client):
    """Item-level metrics for a THB advertiser must be converted before return."""
    _ccm._currency_cache["amsolar"] = "THB"
    mock_client._make_request.return_value = {
        "code": 0,
        "data": {
            "list": [
                _item_row("vid_nd155", cost=1842.0, gmv=5146.0, orders=4),
            ],
            "page_info": {"total_number": 1},
        },
    }

    with patch.object(_fxm, "_fetch_from_frankfurter", AsyncMock(return_value=0.030)):
        result = await get_gmvmax_reports(
            mock_client,
            "amsolar",
            "2026-05-14",
            "2026-05-14",
            store_ids=["7495609170861329178"],
            dimensions=["item_id"],
        )

    assert result["currency"] == "USD"
    assert result["source_currency"] == "THB"
    metrics = result["list"][0]["metrics"]
    # 1842 THB × 0.030 = $55.26 — not the original $1,842 USD-misread
    assert round(float(metrics["cost"]), 2) == round(1842.0 * 0.030, 2)
    assert round(float(metrics["gross_revenue"]), 2) == round(5146.0 * 0.030, 2)
    assert round(float(metrics["net_cost"]), 2) == round(1842.0 * 0.4 * 0.030, 2)
    # Non-monetary unchanged
    assert metrics["orders"] == "4"


@pytest.mark.asyncio
async def test_usd_advertiser_fast_path_no_conversion(mock_client):
    """USD advertiser must hit fast-path: no FX HTTP call, values byte-equal."""
    _ccm._currency_cache["usd_adv"] = "USD"
    mock_client._make_request.return_value = {
        "code": 0,
        "data": {
            "list": [
                _item_row("vid_x", cost=500.0, gmv=1200.0, orders=10),
            ],
            "page_info": {"total_number": 1},
        },
    }

    with patch.object(_fxm, "_fetch_from_frankfurter") as spy:
        result = await get_gmvmax_reports(
            mock_client,
            "usd_adv",
            "2026-05-14",
            "2026-05-14",
            store_ids=["s"],
            dimensions=["item_id"],
        )

    spy.assert_not_called()
    metrics = result["list"][0]["metrics"]
    assert float(metrics["cost"]) == 500.0
    assert float(metrics["gross_revenue"]) == 1200.0


@pytest.mark.asyncio
async def test_per_day_dim_uses_per_day_fx(mock_client):
    """When stat_time_day is in dimensions, each row gets its own day's rate."""
    _ccm._currency_cache["amsolar"] = "THB"
    mock_client._make_request.return_value = {
        "code": 0,
        "data": {
            "list": [
                {
                    "dimensions": {
                        "advertiser_id": "amsolar",
                        "stat_time_day": "2026-05-13 00:00:00",
                    },
                    "metrics": {"cost": "1000.0", "gross_revenue": "3000.0"},
                },
                {
                    "dimensions": {
                        "advertiser_id": "amsolar",
                        "stat_time_day": "2026-05-14 00:00:00",
                    },
                    "metrics": {"cost": "2000.0", "gross_revenue": "5000.0"},
                },
            ],
            "page_info": {"total_number": 2},
        },
    }

    # Different rates per day to verify per-row routing
    async def _rate(date_str, currency):
        return {"2026-05-13": 0.029, "2026-05-14": 0.031}.get(date_str, 0.030)

    with patch.object(_fxm, "_fetch_from_frankfurter", side_effect=_rate):
        result = await get_gmvmax_reports(
            mock_client,
            "amsolar",
            "2026-05-13",
            "2026-05-14",
            store_ids=["s"],
            dimensions=["advertiser_id", "stat_time_day"],
        )

    by_day = {r["dimensions"]["stat_time_day"]: r["metrics"] for r in result["list"]}
    assert round(float(by_day["2026-05-13 00:00:00"]["cost"]), 2) == round(
        1000.0 * 0.029, 2
    )
    assert round(float(by_day["2026-05-14 00:00:00"]["cost"]), 2) == round(
        2000.0 * 0.031, 2
    )
