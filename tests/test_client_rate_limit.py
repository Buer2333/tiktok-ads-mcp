"""Tests for rate limit detection in TikTokAdsClient."""


import pytest
from unittest.mock import AsyncMock, MagicMock, patch

from tiktok_ads_mcp.client import (
    TikTokAdsClient,
    TikTokPermissionError,
    TikTokRateLimitError,
)


@pytest.fixture
def mock_client():
    """Create a client with mocked config."""
    with patch("tiktok_ads_mcp.client.config") as mock_config:
        mock_config.validate_credentials.return_value = True
        mock_config.APP_ID = "test_app"
        mock_config.SECRET = "test_secret"
        mock_config.BASE_URL = "https://business-api.tiktok.com/open_api"
        mock_config.API_VERSION = "v1.3"
        mock_config.REQUEST_TIMEOUT = 30
        mock_config.ACCESS_TOKEN = "token1"
        mock_config.ACCESS_TOKEN_2 = None
        client = TikTokAdsClient()
    return client


@pytest.mark.asyncio
async def test_rate_limit_detected(mock_client):
    """Rate limit message should raise TikTokRateLimitError."""
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = {
        "code": 40100,
        "message": "Too many requests, please try again later",
    }

    with patch("httpx.AsyncClient") as mock_httpx:
        ctx = AsyncMock()
        ctx.get = AsyncMock(return_value=mock_response)
        mock_httpx.return_value.__aenter__ = AsyncMock(return_value=ctx)
        mock_httpx.return_value.__aexit__ = AsyncMock(return_value=False)

        with pytest.raises(TikTokRateLimitError, match="Too many"):
            await mock_client._do_request("token1", "GET", "test/endpoint/")


@pytest.mark.asyncio
async def test_rate_limit_variant_message(mock_client):
    """'rate limit' keyword should also trigger TikTokRateLimitError."""
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = {
        "code": 40100,
        "message": "Rate limit exceeded for this advertiser",
    }

    with patch("httpx.AsyncClient") as mock_httpx:
        ctx = AsyncMock()
        ctx.get = AsyncMock(return_value=mock_response)
        mock_httpx.return_value.__aenter__ = AsyncMock(return_value=ctx)
        mock_httpx.return_value.__aexit__ = AsyncMock(return_value=False)

        with pytest.raises(TikTokRateLimitError, match="Rate limit"):
            await mock_client._do_request("token1", "GET", "test/endpoint/")


@pytest.mark.asyncio
async def test_permission_error_not_mistaken_for_rate_limit(mock_client):
    """Permission error should still raise TikTokPermissionError, not rate limit."""
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = {
        "code": 40001,
        "message": "No permission to access this advertiser",
    }

    with patch("httpx.AsyncClient") as mock_httpx:
        ctx = AsyncMock()
        ctx.get = AsyncMock(return_value=mock_response)
        mock_httpx.return_value.__aenter__ = AsyncMock(return_value=ctx)
        mock_httpx.return_value.__aexit__ = AsyncMock(return_value=False)

        with pytest.raises(TikTokPermissionError, match="No permission"):
            await mock_client._do_request("token1", "GET", "test/endpoint/")


@pytest.mark.asyncio
async def test_generic_error_still_works(mock_client):
    """Non-rate-limit, non-permission errors should raise generic Exception."""
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = {
        "code": 50000,
        "message": "Internal server error",
    }

    with patch("httpx.AsyncClient") as mock_httpx:
        ctx = AsyncMock()
        ctx.get = AsyncMock(return_value=mock_response)
        mock_httpx.return_value.__aenter__ = AsyncMock(return_value=ctx)
        mock_httpx.return_value.__aexit__ = AsyncMock(return_value=False)

        with pytest.raises(Exception, match="Internal server error"):
            await mock_client._do_request("token1", "GET", "test/endpoint/")
