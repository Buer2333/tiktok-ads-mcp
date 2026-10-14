"""Tests for 51010 transient-error retry in TikTokAdsClient (2026-10-14).

TikTok answers HTTP 200 + code 51010 ("Internal service timed out") when its
backend times out; the same request usually succeeds on a re-ask. Only 51010
is retried — every other non-zero code must keep failing on the first attempt.
"""

import logging

import pytest
from unittest.mock import AsyncMock, MagicMock, patch

from tiktok_ads_mcp.client import (
    TikTokAdsClient,
    TikTokPermissionError,
    TikTokRateLimitError,
    TikTokTransientError,
)

OK = {"code": 0, "message": "OK", "data": {"list": [{"id": 1}]}}
E51010 = {"code": 51010, "message": "Internal service timed out. Please retry in sometime."}


@pytest.fixture
def mock_client():
    with patch("tiktok_ads_mcp.client.config") as mock_config:
        mock_config.validate_credentials.return_value = True
        mock_config.APP_ID = "test_app"
        mock_config.SECRET = "test_secret"
        mock_config.BASE_URL = "https://business-api.tiktok.com/open_api"
        mock_config.API_VERSION = "v1.3"
        mock_config.REQUEST_TIMEOUT = 30
        mock_config.ACCESS_TOKEN = "token1"
        mock_config.ACCESS_TOKEN_2 = None
        mock_config.PROXY = None
        client = TikTokAdsClient()
    return client


@pytest.fixture(autouse=True)
def no_retry_sleep(monkeypatch):
    """Skip tenacity's backoff sleeps (the wrapped copy inherits .sleep)."""

    async def _no_sleep(_seconds):
        return None

    monkeypatch.setattr(TikTokAdsClient._do_request.retry, "sleep", _no_sleep)


def _responses(*bodies):
    out = []
    for body in bodies:
        r = MagicMock()
        r.status_code = 200
        r.json.return_value = body
        out.append(r)
    return out


def _patch_httpx(mock_httpx, bodies):
    ctx = AsyncMock()
    ctx.get = AsyncMock(side_effect=_responses(*bodies))
    mock_httpx.return_value.__aenter__ = AsyncMock(return_value=ctx)
    mock_httpx.return_value.__aexit__ = AsyncMock(return_value=False)
    return ctx


@pytest.mark.asyncio
async def test_51010_twice_then_success_returns_result(mock_client):
    with patch("httpx.AsyncClient") as mock_httpx:
        ctx = _patch_httpx(mock_httpx, [E51010, E51010, OK])
        result = await mock_client._make_request(
            "GET", "gmv_max/report/get/", {"advertiser_id": "123", "page": 2}
        )
    assert result == OK
    assert ctx.get.call_count == 3


@pytest.mark.asyncio
async def test_51010_exhausted_raises_typed_error_with_original_text(mock_client):
    with patch("httpx.AsyncClient") as mock_httpx:
        ctx = _patch_httpx(mock_httpx, [E51010] * 5)
        with pytest.raises(TikTokTransientError) as ei:
            await mock_client._make_request(
                "GET", "gmv_max/report/get/", {"advertiser_id": "123"}
            )
    # Downstream (lark-bot) matches on this exact text; also still an Exception.
    assert str(ei.value) == f"TikTok API error 51010: {E51010['message']}"
    assert isinstance(ei.value, Exception)
    assert ctx.get.call_count == 3  # mcp_retry default max_attempts


@pytest.mark.asyncio
async def test_51010_string_code_also_retried(mock_client):
    body = {"code": "51010", "message": "Internal time out."}
    with patch("httpx.AsyncClient") as mock_httpx:
        ctx = _patch_httpx(mock_httpx, [body, OK])
        result = await mock_client._make_request("GET", "x/", {"advertiser_id": "1"})
    assert result == OK
    assert ctx.get.call_count == 2


@pytest.mark.asyncio
async def test_51010_retry_log_has_context_but_no_secrets(mock_client, caplog):
    with caplog.at_level(logging.WARNING, logger="tiktok_ads_mcp.client"):
        with patch("httpx.AsyncClient") as mock_httpx:
            _patch_httpx(mock_httpx, [E51010, E51010, OK])
            await mock_client._make_request(
                "GET", "gmv_max/report/get/", {"advertiser_id": "7495652255284431231", "page": 3}
            )
    lines = [r.getMessage() for r in caplog.records if r.name == "tiktok_ads_mcp.client"]
    assert len(lines) == 2
    assert "gmv_max/report/get/" in lines[0]
    assert "advertiser_id=7495652255284431231" in lines[0]
    assert "page=3" in lines[0]
    assert "attempt=1" in lines[0] and "attempt=2" in lines[1]
    for line in lines:
        assert "token1" not in line and "test_secret" not in line
        assert "https://" not in line


@pytest.mark.asyncio
async def test_rate_limit_path_unchanged(mock_client):
    body = {"code": 40100, "message": "Too many requests, please try again later"}
    with patch("httpx.AsyncClient") as mock_httpx:
        ctx = _patch_httpx(mock_httpx, [body] * 5)
        with pytest.raises(TikTokRateLimitError):
            await mock_client._make_request("GET", "x/", {"advertiser_id": "1"})
    assert ctx.get.call_count == 3  # retried exactly as before


@pytest.mark.asyncio
async def test_permission_error_not_retried(mock_client):
    body = {"code": 40001, "message": "No permission to access this advertiser"}
    with patch("httpx.AsyncClient") as mock_httpx:
        ctx = _patch_httpx(mock_httpx, [body] * 5)
        with pytest.raises(TikTokPermissionError):
            await mock_client._do_request("token1", "GET", "x/", {"advertiser_id": "1"})
    assert ctx.get.call_count == 1


@pytest.mark.asyncio
async def test_other_error_code_not_retried(mock_client):
    body = {"code": 40002, "message": "Invalid parameter"}
    with patch("httpx.AsyncClient") as mock_httpx:
        ctx = _patch_httpx(mock_httpx, [body] * 5)
        with pytest.raises(Exception) as ei:
            await mock_client._make_request("GET", "x/", {"advertiser_id": "1"})
    assert type(ei.value) is Exception
    assert str(ei.value) == "TikTok API error 40002: Invalid parameter"
    assert ctx.get.call_count == 1
