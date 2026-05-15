"""Tests for advertiser currency cache."""

from unittest.mock import AsyncMock, MagicMock

import pytest

from tiktok_ads_mcp import currency_cache as cc


@pytest.fixture(autouse=True)
def reset_currency_cache():
    """Override the conftest autouse-USD-default cache so we can exercise the
    real lookup path here."""
    saved = cc._currency_cache
    cc._currency_cache = {}
    yield cc._currency_cache
    cc._currency_cache = saved


@pytest.fixture
def mock_client():
    client = MagicMock()
    client._make_request = AsyncMock()
    return client


@pytest.mark.asyncio
async def test_get_currency_returns_thb_for_bangkok_account(mock_client):
    mock_client._make_request.return_value = {
        "code": 0,
        "data": {"list": [{"advertiser_id": "962385", "currency": "THB"}]},
    }
    ccy = await cc.get_currency(mock_client, "962385")
    assert ccy == "THB"


@pytest.mark.asyncio
async def test_get_currency_normalizes_case(mock_client):
    mock_client._make_request.return_value = {
        "code": 0,
        "data": {"list": [{"advertiser_id": "1", "currency": "thb"}]},
    }
    assert await cc.get_currency(mock_client, "1") == "THB"


@pytest.mark.asyncio
async def test_get_currency_defaults_to_usd_on_api_error(mock_client):
    mock_client._make_request.side_effect = RuntimeError("API down")
    assert await cc.get_currency(mock_client, "X") == "USD"


@pytest.mark.asyncio
async def test_cache_hit_skips_api(mock_client):
    cc._currency_cache["999"] = "MXN"
    assert await cc.get_currency(mock_client, "999") == "MXN"
    mock_client._make_request.assert_not_called()


@pytest.mark.asyncio
async def test_warmup_batches_and_fills_cache(mock_client):
    mock_client._make_request.return_value = {
        "code": 0,
        "data": {
            "list": [
                {"advertiser_id": "1", "currency": "USD"},
                {"advertiser_id": "2", "currency": "THB"},
                {"advertiser_id": "3", "currency": "MXN"},
            ]
        },
    }
    await cc.warmup_currency_cache(mock_client, ["1", "2", "3"])
    assert cc._currency_cache["1"] == "USD"
    assert cc._currency_cache["2"] == "THB"
    assert cc._currency_cache["3"] == "MXN"
    # Subsequent lookups don't re-call.
    mock_client._make_request.reset_mock()
    await cc.get_currency(mock_client, "2")
    mock_client._make_request.assert_not_called()


@pytest.mark.asyncio
async def test_warmup_fills_misses_with_usd(mock_client):
    """API returned only 1 of 3 advertisers (e.g. permission gap) — others must
    still be cached as USD so they don't repeatedly retry the API."""
    mock_client._make_request.return_value = {
        "code": 0,
        "data": {"list": [{"advertiser_id": "1", "currency": "THB"}]},
    }
    await cc.warmup_currency_cache(mock_client, ["1", "2", "3"])
    assert cc._currency_cache["1"] == "THB"
    assert cc._currency_cache["2"] == "USD"
    assert cc._currency_cache["3"] == "USD"
