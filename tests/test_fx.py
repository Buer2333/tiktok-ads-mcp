"""Tests for FX conversion module."""

import json
from unittest.mock import AsyncMock, patch

import pytest

from tiktok_ads_mcp import fx


@pytest.mark.asyncio
async def test_usd_is_fast_path_no_http():
    """USD must be a no-op: rate=1.0, no API call, regardless of date."""
    with patch.object(fx, "_fetch_from_frankfurter") as spy:
        rate = await fx.get_rate_to_usd("USD", "2026-05-14")
    assert rate == 1.0
    spy.assert_not_called()


@pytest.mark.asyncio
async def test_thb_conversion_via_frankfurter():
    with patch.object(fx, "_fetch_from_frankfurter", AsyncMock(return_value=0.0303)):
        amt_usd = await fx.to_usd(26691.77, "THB", "2026-05-14")
    assert round(amt_usd, 2) == round(26691.77 * 0.0303, 2)


@pytest.mark.asyncio
async def test_fallback_table_used_on_api_failure(caplog):
    """If Frankfurter is unreachable AND no cache, use built-in fallback rate
    so the report job degrades instead of crashing."""
    with patch.object(fx, "_fetch_from_frankfurter", AsyncMock(return_value=None)):
        rate = await fx.get_rate_to_usd("THB", "2026-05-14")
    assert rate == fx._FALLBACK_RATES_TO_USD["THB"]


@pytest.mark.asyncio
async def test_unknown_currency_returns_one_with_log(caplog):
    """Unknown currency + no API + no fallback → 1.0 + ERROR log. The 1.0 keeps
    downstream math the same magnitude as the pre-FX-aware code path."""
    with patch.object(fx, "_fetch_from_frankfurter", AsyncMock(return_value=None)):
        rate = await fx.get_rate_to_usd("ZZZ", "2026-05-14")
    assert rate == 1.0


@pytest.mark.asyncio
async def test_disk_cache_persists_across_resets(tmp_path, monkeypatch):
    monkeypatch.setenv("LARK_BOT_CACHE_DIR", str(tmp_path))
    fx._reset_cache_for_test()

    with patch.object(fx, "_fetch_from_frankfurter", AsyncMock(return_value=0.0303)):
        r1 = await fx.get_rate_to_usd("THB", "2026-05-13")

    # Reset in-memory cache; disk should still have the rate.
    fx._reset_cache_for_test()
    with patch.object(fx, "_fetch_from_frankfurter") as spy:
        r2 = await fx.get_rate_to_usd("THB", "2026-05-13")
    assert r1 == r2 == 0.0303
    spy.assert_not_called()

    # Disk file should exist with the entry.
    saved = json.loads((tmp_path / "fx_rates.json").read_text())
    assert saved["2026-05-13"]["THB"] == 0.0303


@pytest.mark.asyncio
async def test_historical_dates_immutable():
    """A historical date hit must not refetch even when called twice."""
    with patch.object(
        fx, "_fetch_from_frankfurter", AsyncMock(return_value=0.030)
    ) as spy:
        await fx.get_rate_to_usd("THB", "2026-05-13")
        await fx.get_rate_to_usd("THB", "2026-05-13")
    assert spy.call_count == 1


@pytest.mark.asyncio
async def test_zero_amount_skips_lookup():
    with patch.object(fx, "_fetch_from_frankfurter") as spy:
        out = await fx.to_usd(0.0, "THB", "2026-05-14")
    assert out == 0.0
    spy.assert_not_called()
