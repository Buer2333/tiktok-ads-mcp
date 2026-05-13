"""Integration tests: fetch_ad_cost with Active Roster Filter (F0).

Verifies the three-mode behavior contract:
  off    → filter not consulted; legacy path always reaches API
  shadow → filter consulted, decision logged; API still called regardless
  on     → filter consulted; SKIP_* decisions short-circuit to zero
           before API is called
"""

import pytest
from unittest.mock import AsyncMock, MagicMock, patch

from tiktok_ads_mcp.business import AdAccountManager
from tiktok_ads_mcp.cache import (
    AdCostCache,
    AdvertiserActivityCache,
    AccountDiscoveryCache,
    BanStatusCache,
    BalanceSnapshotCache,
)


STORE_A = "1000000000000000001"


@pytest.fixture
def caches(tmp_path):
    return {
        "ad_cost": AdCostCache(cache_dir=tmp_path / "cost"),
        "ban_status": BanStatusCache(cache_dir=tmp_path / "ban"),
        "balance": BalanceSnapshotCache(cache_dir=tmp_path / "balance"),
        "discovery": AccountDiscoveryCache(cache_dir=tmp_path / "discovery"),
        "activity": AdvertiserActivityCache(cache_dir=tmp_path / "activity"),
    }


@pytest.fixture
def mock_client():
    c = MagicMock()
    c._make_request = AsyncMock()
    return c


@pytest.fixture
def manager(mock_client, caches):
    return AdAccountManager(
        client=mock_client,
        ad_cost_cache=caches["ad_cost"],
        ban_status_cache=caches["ban_status"],
        balance_cache=caches["balance"],
        discovery_cache=caches["discovery"],
        activity_cache=caches["activity"],
    )


# ── mode=off: filter disabled ─────────────────────────────────────────


@pytest.mark.asyncio
async def test_mode_off_fetches_even_cold_advertiser(manager, caches, monkeypatch):
    """With mode=off, no activity record + non-banned → API still called."""
    monkeypatch.setenv("ACTIVE_ROSTER_MODE", "off")
    with patch.object(
        AdAccountManager,
        "_fetch_single_report",
        new_callable=AsyncMock,
        return_value={"cost": 5.0, "gmv": 10.0, "orders": 1, "roi": 2.0},
    ) as fetch_mock:
        result = await manager.fetch_ad_cost(
            "adv-cold", "2026-05-12", "gmvmax", store_ids=[STORE_A]
        )
    fetch_mock.assert_awaited_once()
    assert result["cost"] == 5.0


# ── mode=shadow: filter consulted but still fetches ───────────────────


@pytest.mark.asyncio
async def test_mode_shadow_logs_but_still_fetches(manager, monkeypatch, caplog):
    """Shadow mode: filter computes decision, logs it, but API is still called."""
    monkeypatch.setenv("ACTIVE_ROSTER_MODE", "shadow")
    import logging

    caplog.set_level(logging.INFO)
    with patch.object(
        AdAccountManager,
        "_fetch_single_report",
        new_callable=AsyncMock,
        return_value={"cost": 5.0, "gmv": 10.0, "orders": 1, "roi": 2.0},
    ) as fetch_mock:
        result = await manager.fetch_ad_cost(
            "adv-cold", "2026-05-12", "gmvmax", store_ids=[STORE_A]
        )
    # API was called despite filter likely saying SKIP_COLD_WEEKLY
    fetch_mock.assert_awaited_once()
    assert result["cost"] == 5.0
    # And the decision was logged
    assert any("[active_roster:shadow]" in r.message for r in caplog.records)


# ── mode=on: SKIP_* returns zero before API ───────────────────────────


@pytest.mark.asyncio
async def test_mode_on_cold_advertiser_skips_api(manager, monkeypatch):
    """Cold (no spend record, non-probe hour) advertiser → SKIP_COLD_WEEKLY → zero."""
    monkeypatch.setenv("ACTIVE_ROSTER_MODE", "on")
    monkeypatch.setenv("ACTIVE_ROSTER_PROBE_HOUR", "8")
    # Pin shop-tz "now" to Tuesday 14:00 (non-probe, non-Monday)
    # Since fetch_ad_cost reads datetime.now(ZoneInfo(shop_tz)), we patch
    # the datetime module in active_roster's path
    import datetime as _dt

    real_dt = _dt.datetime

    class FakeDT(real_dt):
        @classmethod
        def now(cls, tz=None):
            return real_dt(2026, 5, 12, 14, 0, 0, tzinfo=tz)  # Tue 14:00

    with (
        patch("tiktok_ads_mcp.business.account_manager.datetime", FakeDT),
        patch.object(
            AdAccountManager,
            "_fetch_single_report",
            new_callable=AsyncMock,
            return_value={"cost": 5.0, "gmv": 10.0, "orders": 1, "roi": 2.0},
        ) as fetch_mock,
    ):
        result = await manager.fetch_ad_cost(
            "adv-cold", "2026-05-12", "gmvmax", store_ids=[STORE_A]
        )
    fetch_mock.assert_not_awaited()  # API skipped
    assert result == {"cost": 0.0, "gmv": 0.0, "orders": 0, "roi": 0.0}


@pytest.mark.asyncio
async def test_mode_on_hot_advertiser_still_fetches(manager, caches, monkeypatch):
    """Recent spend → FETCH_HOT → API IS called even with mode=on."""
    monkeypatch.setenv("ACTIVE_ROSTER_MODE", "on")
    caches["activity"].record_probe("adv-hot", STORE_A, "gmvmax", "2026-05-10", 100.0)

    import datetime as _dt

    real_dt = _dt.datetime

    class FakeDT(real_dt):
        @classmethod
        def now(cls, tz=None):
            return real_dt(2026, 5, 12, 14, 0, 0, tzinfo=tz)

    with (
        patch("tiktok_ads_mcp.business.account_manager.datetime", FakeDT),
        patch.object(
            AdAccountManager,
            "_fetch_single_report",
            new_callable=AsyncMock,
            return_value={"cost": 50.0, "gmv": 100.0, "orders": 5, "roi": 2.0},
        ) as fetch_mock,
    ):
        result = await manager.fetch_ad_cost(
            "adv-hot", "2026-05-12", "gmvmax", store_ids=[STORE_A]
        )
    fetch_mock.assert_awaited_once()
    assert result["cost"] == 50.0


@pytest.mark.asyncio
async def test_mode_on_records_probe_on_successful_fetch(manager, caches, monkeypatch):
    """After a successful fetch with cost > 0, activity cache should record it."""
    monkeypatch.setenv("ACTIVE_ROSTER_MODE", "on")
    caches["activity"].record_probe("adv1", STORE_A, "gmvmax", "2026-05-10", 100.0)

    import datetime as _dt

    real_dt = _dt.datetime

    class FakeDT(real_dt):
        @classmethod
        def now(cls, tz=None):
            return real_dt(2026, 5, 12, 14, 0, 0, tzinfo=tz)

    with (
        patch("tiktok_ads_mcp.business.account_manager.datetime", FakeDT),
        patch.object(
            AdAccountManager,
            "_fetch_single_report",
            new_callable=AsyncMock,
            return_value={"cost": 75.0, "gmv": 150.0, "orders": 3, "roi": 2.0},
        ),
    ):
        await manager.fetch_ad_cost("adv1", "2026-05-12", "gmvmax", store_ids=[STORE_A])
    # last_spend_date advanced to 2026-05-12
    entry = caches["activity"].get("adv1", STORE_A, "gmvmax")
    assert entry["last_spend_date"] == "2026-05-12"
    assert entry["last_probe_date"] == "2026-05-12"
    assert entry["last_probe_cost"] == 75.0


# ── filter exception safety ───────────────────────────────────────────


@pytest.mark.asyncio
async def test_filter_exception_falls_back_to_fetch(manager, monkeypatch, caplog):
    """If should_fetch raises, we must fall back to FETCH (never block API)."""
    monkeypatch.setenv("ACTIVE_ROSTER_MODE", "on")

    def _boom(*a, **kw):
        raise RuntimeError("simulated filter bug")

    import logging

    caplog.set_level(logging.ERROR)

    with (
        patch("tiktok_ads_mcp.business.active_roster.should_fetch", side_effect=_boom),
        patch.object(
            AdAccountManager,
            "_fetch_single_report",
            new_callable=AsyncMock,
            return_value={"cost": 5.0, "gmv": 10.0, "orders": 1, "roi": 2.0},
        ) as fetch_mock,
    ):
        result = await manager.fetch_ad_cost(
            "adv1", "2026-05-12", "gmvmax", store_ids=[STORE_A]
        )
    fetch_mock.assert_awaited_once()
    assert result["cost"] == 5.0
    assert any("filter error, fallback to FETCH" in r.message for r in caplog.records)


# ── banned-cache path NOT touched by filter ───────────────────────────


@pytest.mark.asyncio
async def test_banned_non_today_cache_path_bypasses_filter(
    manager, caches, monkeypatch
):
    """The legacy banned+non-today cache lookup runs before filter; pre-ban
    spend in cache must still be returned even with mode=on."""
    monkeypatch.setenv("ACTIVE_ROSTER_MODE", "on")
    # Cache pre-ban spend
    caches["ad_cost"].put_daily(
        "adv-banned", "2026-04-01", "gmvmax", 200.0, 400.0, 8, store_id=STORE_A
    )
    # Mark account banned on 2026-04-15
    caches["ban_status"].set_banned(
        "adv-banned",
        status="NO_ACCESS_CONFIRMED_BANNED",
        detected_at="2026-04-15",
    )
    with patch.object(
        AdAccountManager,
        "_fetch_single_report",
        new_callable=AsyncMock,
    ) as fetch_mock:
        result = await manager.fetch_ad_cost(
            "adv-banned",
            "2026-04-01",  # pre-ban date
            "gmvmax",
            store_ids=[STORE_A],
            period="yesterday",  # non-today triggers banned cache path
            banned=True,
        )
    # API NOT called (legacy cache hit), pre-ban cost returned
    fetch_mock.assert_not_awaited()
    assert result["cost"] == 200.0


# ── mode=on without filter caches: degrade gracefully ─────────────────


@pytest.mark.asyncio
async def test_mode_on_skipped_when_caches_missing(mock_client, caches, monkeypatch):
    """If activity_cache wasn't injected, filter must not run at all
    (manager built without activity_cache) — verify legacy behavior intact."""
    monkeypatch.setenv("ACTIVE_ROSTER_MODE", "on")
    mgr = AdAccountManager(
        client=mock_client,
        ad_cost_cache=caches["ad_cost"],
        ban_status_cache=caches["ban_status"],
        balance_cache=caches["balance"],
        # discovery_cache + activity_cache deliberately omitted
    )
    with patch.object(
        AdAccountManager,
        "_fetch_single_report",
        new_callable=AsyncMock,
        return_value={"cost": 5.0, "gmv": 10.0, "orders": 1, "roi": 2.0},
    ) as fetch_mock:
        result = await mgr.fetch_ad_cost(
            "adv1", "2026-05-12", "gmvmax", store_ids=[STORE_A]
        )
    fetch_mock.assert_awaited_once()
    assert result["cost"] == 5.0
