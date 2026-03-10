"""Tests for get_advertiser_balance tool."""

import pytest
from unittest.mock import AsyncMock, MagicMock

from tiktok_ads_mcp.tools.advertiser_balance import get_advertiser_balance


@pytest.fixture
def mock_client():
    client = MagicMock()
    client._make_request = AsyncMock()
    return client


@pytest.mark.asyncio
async def test_get_advertiser_balance_success(mock_client):
    """Test successful balance retrieval for multiple advertisers."""
    mock_client._make_request.return_value = {
        "code": 0,
        "data": {
            "list": [
                {
                    "advertiser_id": "111",
                    "name": "Test Account 1",
                    "balance": 500.50,
                    "currency": "USD",
                    "status": "STATUS_ENABLE",
                },
                {
                    "advertiser_id": "222",
                    "name": "Test Account 2",
                    "balance": 100.00,
                    "currency": "USD",
                    "status": "STATUS_ENABLE",
                },
            ]
        },
    }

    result = await get_advertiser_balance(mock_client, advertiser_ids=["111", "222"])

    assert len(result) == 2
    assert result[0]["advertiser_id"] == "111"
    assert result[0]["balance"] == 500.50
    assert result[0]["name"] == "Test Account 1"
    assert result[0]["currency"] == "USD"
    assert result[1]["advertiser_id"] == "222"
    assert result[1]["balance"] == 100.00

    # Verify API was called with correct params
    call_args = mock_client._make_request.call_args
    assert call_args[0][0] == "GET"
    assert call_args[0][1] == "advertiser/info/"
    params = call_args[0][2]
    assert '["111", "222"]' in params["advertiser_ids"] or '["111","222"]' in params["advertiser_ids"]


@pytest.mark.asyncio
async def test_get_advertiser_balance_empty_ids(mock_client):
    """Test that empty advertiser_ids raises ValueError."""
    with pytest.raises(ValueError, match="advertiser_ids is required"):
        await get_advertiser_balance(mock_client, advertiser_ids=[])


@pytest.mark.asyncio
async def test_get_advertiser_balance_too_many_ids(mock_client):
    """Test that more than 100 IDs raises ValueError."""
    ids = [str(i) for i in range(101)]
    with pytest.raises(ValueError, match="Maximum 100"):
        await get_advertiser_balance(mock_client, advertiser_ids=ids)


@pytest.mark.asyncio
async def test_get_advertiser_balance_api_error(mock_client):
    """Test that API errors propagate correctly."""
    mock_client._make_request.side_effect = Exception("TikTok API error 40001: Unauthorized")

    with pytest.raises(Exception, match="Unauthorized"):
        await get_advertiser_balance(mock_client, advertiser_ids=["111"])


@pytest.mark.asyncio
async def test_get_advertiser_balance_empty_response(mock_client):
    """Test handling of empty data list from API."""
    mock_client._make_request.return_value = {
        "code": 0,
        "data": {"list": []},
    }

    result = await get_advertiser_balance(mock_client, advertiser_ids=["999"])
    assert result == []


@pytest.mark.asyncio
async def test_get_advertiser_balance_missing_fields(mock_client):
    """Test graceful handling of missing optional fields in response."""
    mock_client._make_request.return_value = {
        "code": 0,
        "data": {
            "list": [
                {
                    "advertiser_id": "111",
                    "balance": 50.0,
                }
            ]
        },
    }

    result = await get_advertiser_balance(mock_client, advertiser_ids=["111"])
    assert len(result) == 1
    assert result[0]["advertiser_id"] == "111"
    assert result[0]["balance"] == 50.0
    assert result[0]["name"] == "Unknown"
    assert result[0]["currency"] == ""
    assert result[0]["status"] == "Unknown"
