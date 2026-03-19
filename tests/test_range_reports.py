"""Tests for range report tools."""

from unittest.mock import AsyncMock, MagicMock

import pytest

from tiktok_ads_mcp.tools.range_reports import (
    get_ads_range_report,
    get_gmvmax_range_report,
)


@pytest.fixture
def mock_client():
    client = MagicMock()
    client._make_request = AsyncMock()
    return client


def _gmvmax_response(cost: float, gmv: float, orders: int) -> dict:
    return {
        "code": 0,
        "data": {
            "list": [
                {
                    "dimensions": {"advertiser_id": "123"},
                    "metrics": {
                        "cost": str(cost),
                        "gross_revenue": str(gmv),
                        "orders": str(orders),
                    },
                }
            ],
            "page_info": {"total_page": 1},
        },
    }


def _ads_response(spend: float, gmv: float, orders: int) -> dict:
    return {
        "code": 0,
        "data": {
            "list": [
                {
                    "dimensions": {"advertiser_id": "123"},
                    "metrics": {
                        "spend": str(spend),
                        "total_onsite_shopping_value": str(gmv),
                        "onsite_shopping": str(orders),
                    },
                }
            ],
            "page_info": {"total_page": 1},
        },
    }


@pytest.mark.asyncio
async def test_gmvmax_range_basic(mock_client):
    mock_client._make_request.return_value = _gmvmax_response(100.0, 500.0, 10)

    result = await get_gmvmax_range_report(
        mock_client, "123", ["store1"], "2026-03-01", "2026-03-15"
    )

    assert result["cost"] == 100.0
    assert result["gmv"] == 500.0
    assert result["orders"] == 10
    assert result["roi"] == 5.0


@pytest.mark.asyncio
async def test_gmvmax_range_zero_cost(mock_client):
    mock_client._make_request.return_value = _gmvmax_response(0.0, 0.0, 0)

    result = await get_gmvmax_range_report(
        mock_client, "123", ["store1"], "2026-03-01", "2026-03-15"
    )

    assert result["roi"] == 0.0


@pytest.mark.asyncio
async def test_ads_range_basic(mock_client):
    mock_client._make_request.return_value = _ads_response(80.0, 400.0, 8)

    result = await get_ads_range_report(mock_client, "123", "2026-03-01", "2026-03-15")

    assert result["cost"] == 80.0
    assert result["gmv"] == 400.0
    assert result["orders"] == 8
    assert result["roas"] == 5.0


@pytest.mark.asyncio
async def test_ads_range_zero_cost(mock_client):
    mock_client._make_request.return_value = _ads_response(0.0, 0.0, 0)

    result = await get_ads_range_report(mock_client, "123", "2026-03-01", "2026-03-15")

    assert result["roas"] == 0.0


@pytest.mark.asyncio
async def test_gmvmax_range_empty_response(mock_client):
    mock_client._make_request.return_value = {
        "code": 0,
        "data": {"list": [], "page_info": {"total_page": 1}},
    }

    result = await get_gmvmax_range_report(
        mock_client, "123", ["store1"], "2026-03-01", "2026-03-15"
    )

    assert result["cost"] == 0.0
    assert result["gmv"] == 0.0
    assert result["orders"] == 0
    assert result["roi"] == 0.0
