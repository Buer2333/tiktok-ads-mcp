"""Perf / API-budget regression tests (F4).

Catches silent perf regressions from refactors. Each test asserts a
budget — if a refactor accidentally e.g. removes the F0 active-roster
filter or breaks cache-first reuse, these tests fail and block the
merge.

Conventions:
  - Mark with @pytest.mark.perf so devs can opt out via -m "not perf"
  - Prefer RATIO budgets over absolute counts (durable across config drift)
  - api_counter fixture hooks _fetch_single_report — counts real API
    invocations that would have left the process
"""

import pytest
from datetime import datetime
from unittest.mock import AsyncMock, MagicMock, patch

from tiktok_ads_mcp.business import AdAccountManager
from tiktok_ads_mcp.cache import (
    AccountDiscoveryCache,
    AdCostCache,
    AdvertiserActivityCache,
    BalanceSnapshotCache,
    BanStatusCache,
)


# ── Shared fixtures ──────────────────────────────────────────────────


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
def manager(caches):
    return AdAccountManager(
        client=MagicMock(),
        ad_cost_cache=caches["ad_cost"],
        ban_status_cache=caches["ban_status"],
        balance_cache=caches["balance"],
        discovery_cache=caches["discovery"],
        activity_cache=caches["activity"],
    )


@pytest.fixture
def api_counter(manager):
    """Drop-in wrap of manager._fetch_single_report. Reports .count attr.

    Used to assert "did this code path actually call the API N times"
    independently of return value (the wrapped function returns $0).
    """
    counter = {"n": 0, "calls": []}

    async def _wrapped(advertiser_id, ad_type, store_id, date_str, shop_tz):
        counter["n"] += 1
        counter["calls"].append((advertiser_id, ad_type, store_id, date_str))
        return {"cost": 0.0, "gmv": 0.0, "orders": 0}

    with patch.object(
        AdAccountManager,
        "_fetch_single_report",
        new_callable=AsyncMock,
        side_effect=_wrapped,
    ):
        yield counter


@pytest.fixture
def freeze_time_tue_14():
    """Pin shop-tz 'now' to Tue 14:00 — off-probe-hour, regular weekday."""
    real_dt = datetime

    class FakeDT(real_dt):
        @classmethod
        def now(cls, tz=None):
            return real_dt(2026, 5, 12, 14, 0, 0, tzinfo=tz)

    with patch("tiktok_ads_mcp.business.account_manager.datetime", FakeDT):
        yield


# Helper: builds a mixed roster of (hot, cold) accounts in caches
def _seed_roster(caches, n_hot: int, n_cold_recent: int, n_cold_old: int):
    """Seed activity_cache so should_fetch can decide each tier.

    hot:        last_spend within 6 days
    cold_recent: last_spend 7-30 days ago (daily_probe tier)
    cold_old:   last_spend > 30 days ago (weekly_probe tier)

    Returns list of (advertiser_id, store_id, ad_type) tuples used.
    """
    pairs = []
    today = "2026-05-12"
    hot_dt = "2026-05-10"
    cold_recent_dt = "2026-04-27"  # 15d ago
    cold_old_dt = "2026-04-01"  # 41d ago

    idx = 0
    for _ in range(n_hot):
        adv = f"hot_{idx:03d}"
        store = f"store_h{idx:03d}"
        caches["activity"].record_probe(adv, store, "gmvmax", hot_dt, 100.0)
        pairs.append((adv, store, "gmvmax"))
        idx += 1

    for _ in range(n_cold_recent):
        adv = f"warm_{idx:03d}"
        store = f"store_w{idx:03d}"
        caches["activity"].record_probe(adv, store, "gmvmax", cold_recent_dt, 50.0)
        pairs.append((adv, store, "gmvmax"))
        idx += 1

    for _ in range(n_cold_old):
        adv = f"old_{idx:03d}"
        store = f"store_o{idx:03d}"
        caches["activity"].record_probe(adv, store, "gmvmax", cold_old_dt, 30.0)
        pairs.append((adv, store, "gmvmax"))
        idx += 1

    return pairs, today


# ── Tests ────────────────────────────────────────────────────────────


@pytest.mark.asyncio
@pytest.mark.perf
async def test_api_counter_fixture_exposes_count(
    manager, caches, api_counter, monkeypatch
):
    """Sanity: counter starts at 0 and increments per fetch."""
    monkeypatch.setenv("ACTIVE_ROSTER_MODE", "off")
    assert api_counter["n"] == 0
    await manager.fetch_ad_cost("adv1", "2026-05-12", "gmvmax", store_ids=["store1"])
    assert api_counter["n"] == 1
    await manager.fetch_ad_cost("adv2", "2026-05-12", "gmvmax", store_ids=["store2"])
    assert api_counter["n"] == 2


@pytest.mark.asyncio
@pytest.mark.perf
async def test_filter_off_calls_api_per_pair(
    manager, caches, api_counter, monkeypatch, freeze_time_tue_14
):
    """mode=off → every fetch_ad_cost invocation reaches the API."""
    monkeypatch.setenv("ACTIVE_ROSTER_MODE", "off")
    pairs, today = _seed_roster(caches, n_hot=5, n_cold_recent=5, n_cold_old=20)
    for adv, store, ad_type in pairs:
        await manager.fetch_ad_cost(adv, today, ad_type, store_ids=[store])
    assert api_counter["n"] == len(pairs), (
        f"mode=off should call API for all {len(pairs)} pairs, got {api_counter['n']}"
    )


@pytest.mark.asyncio
@pytest.mark.perf
async def test_filter_on_savings_at_least_60pct(
    manager, caches, api_counter, monkeypatch, freeze_time_tue_14
):
    """F0 contract: mode=on must save ≥60% API calls on a realistic roster.

    Realistic: 5 hot (always fetch), 5 warm (daily probe — skip off-hour),
    20 old (weekly probe — skip non-Mon). At Tue 14:00 (off-probe), only
    5/30 should fetch → 83% savings.
    """
    monkeypatch.setenv("ACTIVE_ROSTER_MODE", "on")
    pairs, today = _seed_roster(caches, n_hot=5, n_cold_recent=5, n_cold_old=20)
    for adv, store, ad_type in pairs:
        await manager.fetch_ad_cost(adv, today, ad_type, store_ids=[store])
    total = len(pairs)
    fetched = api_counter["n"]
    ratio = fetched / total
    assert ratio <= 0.40, (
        f"F0 should yield ≥60% savings; got {fetched}/{total} = "
        f"{ratio:.0%} fetched (budget: ≤40%)"
    )


@pytest.mark.asyncio
@pytest.mark.perf
async def test_filter_savings_ratio_on_vs_off(
    manager, caches, monkeypatch, freeze_time_tue_14
):
    """Ratio budget: on_count / off_count ≤ 0.40.

    Runs the same roster twice, once with mode=off, once mode=on,
    and compares the fetch counts. Insulates from absolute count
    drift when the test seed changes.
    """
    pairs, today = _seed_roster(caches, n_hot=5, n_cold_recent=5, n_cold_old=20)

    async def _count(mode):
        monkeypatch.setenv("ACTIVE_ROSTER_MODE", mode)
        counter = {"n": 0}

        async def _wrapped(*a, **kw):
            counter["n"] += 1
            return {"cost": 0.0, "gmv": 0.0, "orders": 0}

        with patch.object(
            AdAccountManager,
            "_fetch_single_report",
            new_callable=AsyncMock,
            side_effect=_wrapped,
        ):
            for adv, store, ad_type in pairs:
                await manager.fetch_ad_cost(adv, today, ad_type, store_ids=[store])
        return counter["n"]

    off_count = await _count("off")
    on_count = await _count("on")
    ratio = on_count / off_count if off_count else 0
    assert ratio <= 0.40, (
        f"Filter savings regression: on/off = {on_count}/{off_count} "
        f"= {ratio:.0%} (budget: ≤40%)"
    )


@pytest.mark.asyncio
@pytest.mark.perf
async def test_record_probe_writes_after_fetch(
    manager, caches, api_counter, monkeypatch
):
    """Every successful fetch must record into activity_cache so the
    filter has fresh data on the next hourly pass. If this regresses,
    last_spend_date never advances → all hot accounts decay to weekly_only
    → cost catastrophically under-reported (the cold-start bug we seeded
    around).
    """
    monkeypatch.setenv("ACTIVE_ROSTER_MODE", "off")
    # No existing activity record
    assert caches["activity"].get("adv1", "store1", "gmvmax") is None
    await manager.fetch_ad_cost("adv1", "2026-05-12", "gmvmax", store_ids=["store1"])
    entry = caches["activity"].get("adv1", "store1", "gmvmax")
    assert entry is not None, "fetch should have recorded a probe"
    assert entry["last_probe_date"] == "2026-05-12"


@pytest.mark.asyncio
@pytest.mark.perf
async def test_hot_accounts_always_fetch_regardless_of_hour(
    manager, caches, monkeypatch
):
    """A hot advertiser (recent spend) must be fetched at every hour.

    Guards against a buggy refactor that accidentally puts hot accounts
    under the probe-hour gate.
    """
    monkeypatch.setenv("ACTIVE_ROSTER_MODE", "on")
    pairs, today = _seed_roster(caches, n_hot=3, n_cold_recent=0, n_cold_old=0)

    real_dt = datetime
    fetched_hours = set()

    for hour in (0, 4, 8, 12, 16, 20, 23):

        class FakeDT(real_dt):
            @classmethod
            def now(cls, tz=None):
                return real_dt(2026, 5, 12, hour, 0, 0, tzinfo=tz)

        counter = {"n": 0}

        async def _wrapped(*a, **kw):
            counter["n"] += 1
            return {"cost": 0.0, "gmv": 0.0, "orders": 0}

        with (
            patch("tiktok_ads_mcp.business.account_manager.datetime", FakeDT),
            patch.object(
                AdAccountManager,
                "_fetch_single_report",
                new_callable=AsyncMock,
                side_effect=_wrapped,
            ),
        ):
            for adv, store, ad_type in pairs:
                await manager.fetch_ad_cost(adv, today, ad_type, store_ids=[store])
        if counter["n"] == len(pairs):
            fetched_hours.add(hour)

    assert fetched_hours == {0, 4, 8, 12, 16, 20, 23}, (
        f"hot accounts must fetch at every hour; got fetched_hours={fetched_hours}"
    )


@pytest.mark.asyncio
@pytest.mark.perf
async def test_cold_old_account_fetched_on_monday_probe_hour(
    manager, caches, monkeypatch
):
    """Cold-old accounts must be reachable on Mon@probe_hour — never
    permanently skipped. Guards against regression that breaks the
    weekly probe path (the safety valve that prevents permanent skips).
    """
    monkeypatch.setenv("ACTIVE_ROSTER_MODE", "on")
    pairs, today = _seed_roster(caches, n_hot=0, n_cold_recent=0, n_cold_old=3)

    real_dt = datetime

    class FakeDT(real_dt):
        @classmethod
        def now(cls, tz=None):
            # Mon 2026-05-11 08:00 (probe hour)
            return real_dt(2026, 5, 11, 8, 0, 0, tzinfo=tz)

    counter = {"n": 0}

    async def _wrapped(*a, **kw):
        counter["n"] += 1
        return {"cost": 0.0, "gmv": 0.0, "orders": 0}

    with (
        patch("tiktok_ads_mcp.business.account_manager.datetime", FakeDT),
        patch.object(
            AdAccountManager,
            "_fetch_single_report",
            new_callable=AsyncMock,
            side_effect=_wrapped,
        ),
    ):
        for adv, store, ad_type in pairs:
            await manager.fetch_ad_cost(adv, today, ad_type, store_ids=[store])

    assert counter["n"] == 3, (
        f"Mon 08:00 (weekly probe) must reach all cold-old; got {counter['n']}/3"
    )
