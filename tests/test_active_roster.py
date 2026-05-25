"""Tests for tiktok_ads_mcp.business.active_roster.

Covers the full decision matrix + grace/daily/weekly boundary + DST edge.

Fixtures use real AdvertiserActivityCache / BanStatusCache /
AccountDiscoveryCache instances (file-backed under tmp_path) — they're cheap
to spin up and exercise the same code paths production uses.
"""

import pytest
from tiktok_ads_mcp.business.active_roster import (
    Decision,
    RosterDecision,
    get_mode,
    get_probe_hour,
    should_fetch,
)
from tiktok_ads_mcp.cache.advertiser_activity import AdvertiserActivityCache
from tiktok_ads_mcp.cache.ban_status import BanStatusCache
from tiktok_ads_mcp.cache.account_discovery import AccountDiscoveryCache


STORE_A = "1000000000000000001"
TODAY = "2026-05-12"  # Tuesday


@pytest.fixture
def caches(tmp_path):
    return {
        "ban": BanStatusCache(cache_dir=tmp_path),
        "discovery": AccountDiscoveryCache(cache_dir=tmp_path),
        "activity": AdvertiserActivityCache(cache_dir=tmp_path),
    }


def _decide(
    caches,
    *,
    adv="adv1",
    store=STORE_A,
    ad_type="gmvmax",
    today=TODAY,
    hour=14,
    weekday=1,  # Tuesday by default (not Mon=0, not probe hour 8)
    banned=False,
    probe_hour=8,
):
    return should_fetch(
        adv,
        store,
        ad_type,
        shop_today=today,
        shop_now_hour=hour,
        shop_now_weekday=weekday,
        banned=banned,
        ban_cache=caches["ban"],
        discovery_cache=caches["discovery"],
        activity_cache=caches["activity"],
        probe_hour=probe_hour,
    )


# ── env helpers ──


def test_get_mode_default_off(monkeypatch):
    monkeypatch.delenv("ACTIVE_ROSTER_MODE", raising=False)
    assert get_mode() == "off"


def test_get_mode_normalizes_case_and_whitespace(monkeypatch):
    monkeypatch.setenv("ACTIVE_ROSTER_MODE", "  ON  ")
    assert get_mode() == "on"


def test_get_mode_empty_string_treated_as_off(monkeypatch):
    monkeypatch.setenv("ACTIVE_ROSTER_MODE", "")
    assert get_mode() == "off"


def test_get_probe_hour_default():
    assert get_probe_hour() == 8


def test_get_probe_hour_env_override(monkeypatch):
    monkeypatch.setenv("ACTIVE_ROSTER_PROBE_HOUR", "9")
    assert get_probe_hour() == 9


def test_get_probe_hour_invalid_falls_back_to_8(monkeypatch):
    monkeypatch.setenv("ACTIVE_ROSTER_PROBE_HOUR", "not-a-number")
    assert get_probe_hour() == 8


def test_get_probe_hour_out_of_range_falls_back_to_8(monkeypatch):
    monkeypatch.setenv("ACTIVE_ROSTER_PROBE_HOUR", "25")
    assert get_probe_hour() == 8


# ── decision matrix ──


def test_skip_banned_no_access(caches):
    caches["ban"].set_banned(
        "adv1", status="NO_ACCESS_CONFIRMED_BANNED", detected_at="2026-04-01"
    )
    dec = _decide(caches, banned=True)
    assert dec.decision == Decision.SKIP_BANNED
    assert dec.fetch is False


def test_skip_removed_from_bc(caches):
    caches["ban"].set_banned("adv1", status="REMOVED_FROM_BC", detected_at="2026-04-01")
    dec = _decide(caches, banned=True)
    assert dec.decision == Decision.SKIP_REMOVED_FROM_BC
    assert dec.fetch is False


def test_skip_removed_from_bc_fallback_via_discovery(caches):
    """ban_cache has no record, but discovery says banned → treat as removed."""
    caches["discovery"].put(
        "adv1", store_ids=[STORE_A], ad_type="gmvmax", ad_name="test"
    )
    caches["discovery"].mark_banned("adv1")
    dec = _decide(caches, banned=False)
    assert dec.decision == Decision.SKIP_REMOVED_FROM_BC


def test_no_access_banned_takes_precedence_over_removed_fallback(caches):
    """If ban_cache says NO_ACCESS, that wins over discovery fallback."""
    caches["ban"].set_banned(
        "adv1", status="NO_ACCESS_CONFIRMED_BANNED", detected_at="2026-04-01"
    )
    # discovery also has banned=True, but ban_cache wins
    caches["discovery"].put(
        "adv1", store_ids=[STORE_A], ad_type="gmvmax", ad_name="test"
    )
    caches["discovery"].mark_banned("adv1")
    dec = _decide(caches, banned=True)
    assert dec.decision == Decision.SKIP_BANNED


def test_status_limit_does_not_skip_when_active(caches):
    """STATUS_LIMIT advertiser with recent spend = hot, not skipped."""
    caches["ban"].set_banned("adv1", status="STATUS_LIMIT", detected_at="2026-04-01")
    caches["activity"].record_probe("adv1", STORE_A, "gmvmax", "2026-05-10", 100.0)
    # banned=False because legacy _is_banned() only returns True for NO_ACCESS
    dec = _decide(caches, banned=False)
    assert dec.decision == Decision.FETCH_HOT


def test_fetch_grace_new_advertiser(caches, tmp_path):
    """gmvmax advertiser discovered 3 days ago, no spend → grace.

    Explicit discovered_at avoids brittleness when test runs on a date
    after the hardcoded TODAY (real bug: 5/13 run treated TODAY=5/12 as
    "before discovery" yielding days_since_discovery=-1).
    """
    import json

    cache_file = tmp_path / "account_discovery.json"
    cache_file.write_text(
        json.dumps(
            {
                "adv1": {
                    "store_ids": [STORE_A],
                    "ad_type": "gmvmax",
                    "ad_name": "new",
                    "discovered_at": "2026-05-09",  # 3 days before TODAY
                    "last_seen": "2026-05-09",
                    "banned": False,
                }
            }
        )
    )
    caches["discovery"] = AccountDiscoveryCache(cache_dir=tmp_path)
    dec = _decide(caches)
    assert dec.decision == Decision.FETCH_GRACE
    assert dec.days_since_discovery == 3


def test_grace_period_ends_at_day_7(caches, tmp_path):
    """Edge: discovered_at == today - 7 → grace expired."""
    # Manually craft entry to control discovered_at
    import json

    cache_file = tmp_path / "account_discovery.json"
    cache_file.write_text(
        json.dumps(
            {
                "adv1": {
                    "store_ids": [STORE_A],
                    "ad_type": "gmvmax",
                    "ad_name": "old",
                    "discovered_at": "2026-05-05",  # today - 7
                    "last_seen": "2026-05-05",
                    "banned": False,
                }
            }
        )
    )
    caches["discovery"] = AccountDiscoveryCache(cache_dir=tmp_path)
    dec = _decide(caches)
    assert dec.decision != Decision.FETCH_GRACE
    # No spend ever → weekly_skip (Tuesday non-probe)
    assert dec.decision == Decision.SKIP_COLD_WEEKLY


def test_grace_period_inclusive_day_6(caches, tmp_path):
    """Edge: discovered_at == today - 6 → still in grace."""
    import json

    cache_file = tmp_path / "account_discovery.json"
    cache_file.write_text(
        json.dumps(
            {
                "adv1": {
                    "store_ids": [STORE_A],
                    "ad_type": "gmvmax",
                    "ad_name": "newish",
                    "discovered_at": "2026-05-06",
                    "last_seen": "2026-05-06",
                    "banned": False,
                }
            }
        )
    )
    caches["discovery"] = AccountDiscoveryCache(cache_dir=tmp_path)
    dec = _decide(caches)
    assert dec.decision == Decision.FETCH_GRACE
    assert dec.days_since_discovery == 6


def test_ads_type_has_no_grace_period(caches):
    """Ads accounts aren't in discovery → grace clause skipped, goes to decay tier."""
    caches["activity"].record_probe("adv1", "", "ads", "2026-05-10", 100.0)
    dec = _decide(caches, store="", ad_type="ads")
    # last_spend 2 days ago → hot
    assert dec.decision == Decision.FETCH_HOT


def test_fetch_hot_recent_spend(caches):
    caches["activity"].record_probe("adv1", STORE_A, "gmvmax", "2026-05-10", 100.0)
    dec = _decide(caches)
    assert dec.decision == Decision.FETCH_HOT
    assert dec.days_since_spend == 2


def test_hot_boundary_day_6(caches):
    """last_spend == today - 6 still hot."""
    caches["activity"].record_probe("adv1", STORE_A, "gmvmax", "2026-05-06", 100.0)
    dec = _decide(caches)
    assert dec.decision == Decision.FETCH_HOT
    assert dec.days_since_spend == 6


def test_daily_window_at_probe_hour(caches):
    """last_spend == today - 15, hour == probe_hour → fetch daily probe."""
    caches["activity"].record_probe("adv1", STORE_A, "gmvmax", "2026-04-27", 100.0)
    dec = _decide(caches, hour=8)  # probe_hour=8 default
    assert dec.decision == Decision.FETCH_DAILY_PROBE
    assert dec.days_since_spend == 15


def test_daily_window_off_probe_hour_skips(caches):
    caches["activity"].record_probe("adv1", STORE_A, "gmvmax", "2026-04-27", 100.0)
    dec = _decide(caches, hour=14)
    assert dec.decision == Decision.SKIP_COLD_DAILY


def test_daily_boundary_day_7(caches):
    """last_spend == today - 7 → daily window (≥ 7)."""
    caches["activity"].record_probe("adv1", STORE_A, "gmvmax", "2026-05-05", 100.0)
    dec = _decide(caches, hour=14)
    assert dec.decision == Decision.SKIP_COLD_DAILY


def test_daily_boundary_day_30(caches):
    """last_spend == today - 30 still daily window."""
    caches["activity"].record_probe("adv1", STORE_A, "gmvmax", "2026-04-12", 100.0)
    dec = _decide(caches, hour=8)
    assert dec.decision == Decision.FETCH_DAILY_PROBE


def test_weekly_window_day_31(caches):
    """last_spend == today - 31 enters weekly window.

    Note: also seed last_probe=today (fresh) to reflect production state where
    SKIP path records probe each run — without it, stuck-cold tier would fire.
    """
    caches["activity"].record_probe("adv1", STORE_A, "gmvmax", "2026-04-11", 100.0)
    caches["activity"].seed_last_probe("adv1", STORE_A, "gmvmax", TODAY)
    # Tuesday non-probe-hour → skip
    dec = _decide(caches, hour=14, weekday=1)
    assert dec.decision == Decision.SKIP_COLD_WEEKLY


def test_weekly_probe_monday_probe_hour(caches):
    caches["activity"].record_probe("adv1", STORE_A, "gmvmax", "2026-04-01", 100.0)
    caches["activity"].seed_last_probe("adv1", STORE_A, "gmvmax", TODAY)
    dec = _decide(caches, hour=8, weekday=0)
    assert dec.decision == Decision.FETCH_WEEKLY_PROBE


def test_weekly_probe_monday_off_hour_skips(caches):
    caches["activity"].record_probe("adv1", STORE_A, "gmvmax", "2026-04-01", 100.0)
    caches["activity"].seed_last_probe("adv1", STORE_A, "gmvmax", TODAY)
    dec = _decide(caches, hour=14, weekday=0)
    assert dec.decision == Decision.SKIP_COLD_WEEKLY


def test_weekly_probe_tuesday_probe_hour_skips(caches):
    """Tue at probe hour is NOT the weekly window."""
    caches["activity"].record_probe("adv1", STORE_A, "gmvmax", "2026-04-01", 100.0)
    caches["activity"].seed_last_probe("adv1", STORE_A, "gmvmax", TODAY)
    dec = _decide(caches, hour=8, weekday=1)
    assert dec.decision == Decision.SKIP_COLD_WEEKLY


def test_never_spent_weekly_probe(caches):
    """No activity entry, no discovery → weekly probe path (unchanged by
    stuck-cold tier: None last_probe falls through to original logic;
    stuck-cold only fires for stale-but-existing probes)."""
    dec = _decide(caches, hour=8, weekday=0)
    assert dec.decision == Decision.FETCH_WEEKLY_PROBE
    assert dec.days_since_spend is None


def test_never_spent_off_probe_skips(caches):
    dec = _decide(caches, hour=14, weekday=1)
    assert dec.decision == Decision.SKIP_COLD_WEEKLY


def test_probe_hour_env_override_applies(caches, monkeypatch):
    monkeypatch.setenv("ACTIVE_ROSTER_PROBE_HOUR", "10")
    caches["activity"].record_probe("adv1", STORE_A, "gmvmax", "2026-04-27", 100.0)
    # Use module-level default (no probe_hour kwarg) so env is read
    dec = should_fetch(
        "adv1",
        STORE_A,
        "gmvmax",
        shop_today=TODAY,
        shop_now_hour=10,
        shop_now_weekday=1,
        banned=False,
        ban_cache=caches["ban"],
        discovery_cache=caches["discovery"],
        activity_cache=caches["activity"],
    )
    assert dec.decision == Decision.FETCH_DAILY_PROBE


def test_fetch_property_consistency(caches):
    """Every FETCH_* decision should have .fetch=True; SKIP_* should be False."""
    cases = [
        (Decision.FETCH, True),
        (Decision.FETCH_HOT, True),
        (Decision.FETCH_GRACE, True),
        (Decision.FETCH_DAILY_PROBE, True),
        (Decision.FETCH_WEEKLY_PROBE, True),
        (Decision.SKIP_BANNED, False),
        (Decision.SKIP_REMOVED_FROM_BC, False),
        (Decision.SKIP_COLD_DAILY, False),
        (Decision.SKIP_COLD_WEEKLY, False),
    ]
    for d, expected_fetch in cases:
        rd = RosterDecision(d, "test", None, None)
        assert rd.fetch is expected_fetch, f"{d.value}"


def test_discovery_without_discovered_at_field_not_grace(caches, tmp_path):
    """Malformed discovery entry missing discovered_at → no grace, goes to decay."""
    import json

    cache_file = tmp_path / "account_discovery.json"
    cache_file.write_text(
        json.dumps(
            {
                "adv1": {
                    "store_ids": [STORE_A],
                    "ad_type": "gmvmax",
                    "ad_name": "weird",
                    # no discovered_at
                    "last_seen": "2026-05-05",
                    "banned": False,
                }
            }
        )
    )
    caches["discovery"] = AccountDiscoveryCache(cache_dir=tmp_path)
    dec = _decide(caches, hour=14)
    assert dec.decision != Decision.FETCH_GRACE


def test_dst_boundary_does_not_break(caches):
    """Spring DST in US: 2026-03-08 lost an hour; verify date arithmetic OK.

    The decision function uses date-only arithmetic, so DST doesn't affect
    days_since_spend computation. This test pins that behavior.
    """
    caches["activity"].record_probe(
        "adv1", STORE_A, "gmvmax", "2026-03-05", 100.0
    )  # 3 days before DST
    dec = should_fetch(
        "adv1",
        STORE_A,
        "gmvmax",
        shop_today="2026-03-15",  # 10 days after probe
        shop_now_hour=14,
        shop_now_weekday=6,  # Sun
        banned=False,
        ban_cache=caches["ban"],
        discovery_cache=caches["discovery"],
        activity_cache=caches["activity"],
        probe_hour=8,
    )
    assert dec.days_since_spend == 10
    assert dec.decision == Decision.SKIP_COLD_DAILY


def test_shop_today_invalid_format_no_crash(caches):
    """Malformed shop_today → days_since_spend None → weekly path."""
    caches["activity"].record_probe("adv1", STORE_A, "gmvmax", "2026-05-10", 100.0)
    dec = should_fetch(
        "adv1",
        STORE_A,
        "gmvmax",
        shop_today="not-a-date",
        shop_now_hour=14,
        shop_now_weekday=1,
        banned=False,
        ban_cache=caches["ban"],
        discovery_cache=caches["discovery"],
        activity_cache=caches["activity"],
        probe_hour=8,
    )
    # days_since_spend resolved to None due to bad today → weekly window
    assert dec.decision == Decision.SKIP_COLD_WEEKLY


# ── Cross-store recently-seen fallback (clause 3b) ──


def test_fetch_recently_seen_when_activity_miss_and_discovery_fresh(caches, tmp_path):
    """Regression: FlyNew-MX ...896976 had activity record keyed by an old
    store_id; after reassignment, (adv, new_store, gmvmax) misses but
    discovery.last_seen is recent. Must FETCH instead of SKIP."""
    import json

    cache_file = tmp_path / "account_discovery.json"
    cache_file.write_text(
        json.dumps(
            {
                "adv1": {
                    "store_ids": [STORE_A],
                    "ad_type": "gmvmax",
                    "ad_name": "reassigned",
                    # discovered long ago — past 7d grace
                    "discovered_at": "2026-03-01",
                    # but seen by discovery yesterday — actively running
                    "last_seen": "2026-05-11",
                    "banned": False,
                }
            }
        )
    )
    caches["discovery"] = AccountDiscoveryCache(cache_dir=tmp_path)
    # Note: activity_cache deliberately empty for (adv1, STORE_A, gmvmax) —
    # simulating the cross-store reassignment.
    dec = _decide(caches)
    assert dec.decision == Decision.FETCH_RECENTLY_SEEN
    assert dec.fetch is True
    assert dec.days_since_spend is None


def test_recently_seen_fallback_doesnt_fire_when_last_seen_stale(caches, tmp_path):
    """If discovery.last_seen > 7 days ago, fallback does NOT fire — keeps
    existing SKIP_COLD_WEEKLY behavior for genuinely-cold accounts."""
    import json

    cache_file = tmp_path / "account_discovery.json"
    cache_file.write_text(
        json.dumps(
            {
                "adv1": {
                    "store_ids": [STORE_A],
                    "ad_type": "gmvmax",
                    "ad_name": "really_cold",
                    "discovered_at": "2026-01-01",
                    "last_seen": "2026-04-20",  # 22 days ago
                    "banned": False,
                }
            }
        )
    )
    caches["discovery"] = AccountDiscoveryCache(cache_dir=tmp_path)
    dec = _decide(caches)
    assert dec.decision == Decision.SKIP_COLD_WEEKLY


def test_recently_seen_fallback_doesnt_override_existing_spend_skip(caches, tmp_path):
    """When activity_cache HAS a stale-spend record (e.g. 60 days), the
    normal SKIP_COLD_WEEKLY path must win — fallback only fires when
    activity has zero record for this key. Protects existing rate-limit
    savings on truly cold accounts."""
    import json

    cache_file = tmp_path / "account_discovery.json"
    cache_file.write_text(
        json.dumps(
            {
                "adv1": {
                    "store_ids": [STORE_A],
                    "ad_type": "gmvmax",
                    "ad_name": "old_but_recently_seen",
                    "discovered_at": "2026-01-01",
                    "last_seen": "2026-05-10",  # recent, but…
                    "banned": False,
                }
            }
        )
    )
    caches["discovery"] = AccountDiscoveryCache(cache_dir=tmp_path)
    # …existing spend record from 60 days ago — genuinely dormant on this store.
    # Use record_probe with a non-zero cost so last_spend_date gets set;
    # record_probe with cost=0 only updates last_probe_date, leaving
    # days_since_last_spend=None which would trip the new fallback.
    caches["activity"].record_probe("adv1", STORE_A, "gmvmax", "2026-03-13", 50.0)
    # Seed fresh last_probe so stuck-cold tier doesn't fire (reflects production
    # state where SKIP path records probe each run, keeping last_probe ≈ today).
    caches["activity"].seed_last_probe("adv1", STORE_A, "gmvmax", TODAY)
    dec = _decide(caches)
    assert dec.decision == Decision.SKIP_COLD_WEEKLY


def test_recently_seen_does_not_apply_to_ads(caches, tmp_path):
    """Ads accounts have no discovery entry → fallback is a no-op for them,
    keeps existing decay path."""
    # No discovery entry for ads adv → fallback inert
    dec = _decide(caches, store="", ad_type="ads")
    # No activity record, no discovery, never spent → weekly skip
    assert dec.decision == Decision.SKIP_COLD_WEEKLY


def test_recently_seen_doesnt_override_banned(caches, tmp_path):
    """SKIP_BANNED short-circuits BEFORE fallback — banned accounts stay
    skipped even if discovery saw them recently."""
    import json

    cache_file = tmp_path / "account_discovery.json"
    cache_file.write_text(
        json.dumps(
            {
                "adv1": {
                    "store_ids": [STORE_A],
                    "ad_type": "gmvmax",
                    "ad_name": "banned_but_recent",
                    "discovered_at": "2026-01-01",
                    "last_seen": "2026-05-11",
                    "banned": False,
                }
            }
        )
    )
    caches["discovery"] = AccountDiscoveryCache(cache_dir=tmp_path)
    caches["ban"].set_banned(
        "adv1",
        status="NO_ACCESS_CONFIRMED_BANNED",
        detected_at="2026-05-10",
        last_active_date="2026-05-09",
    )
    dec = _decide(caches, banned=True)
    assert dec.decision == Decision.SKIP_BANNED


# ── stuck-cold tier (2026-05-25 deadlock fix) ──


def test_stuck_cold_force_refresh_after_14_days(caches):
    """Weekly-tier cold + last_probe >= 14d ago → FETCH_STUCK_COLD.

    Models the NAD+ 文善7 case: advertiser was active long ago (last_spend 60d),
    SKIP_COLD_WEEKLY in non-probe windows kept silently skipping, last_probe
    eventually crossed 14d threshold → force a fetch to verify still cold.
    """
    caches["activity"].record_probe("adv1", STORE_A, "gmvmax", "2026-03-13", 100.0)
    caches["activity"].seed_last_probe(
        "adv1", STORE_A, "gmvmax", "2026-04-27"
    )  # 15d ago
    dec = _decide(caches, hour=14, weekday=1)
    assert dec.decision == Decision.FETCH_STUCK_COLD
    assert dec.fetch is True
    assert dec.days_since_spend == 60
    assert "stuck_cold_refresh" in dec.reason


def test_cold_within_14_day_window_still_skip(caches):
    """Weekly-tier cold + last_probe < 14d → SKIP_COLD_WEEKLY (stuck-cold not fired).

    Ensures stuck-cold doesn't burn API every run — only after 14d quiet.
    """
    caches["activity"].record_probe("adv1", STORE_A, "gmvmax", "2026-03-13", 100.0)
    caches["activity"].seed_last_probe(
        "adv1", STORE_A, "gmvmax", "2026-05-07"
    )  # 5d ago
    dec = _decide(caches, hour=14, weekday=1)
    assert dec.decision == Decision.SKIP_COLD_WEEKLY
    assert dec.fetch is False


def test_stuck_cold_does_not_fire_for_hot_accounts(caches):
    """Hot accounts (last_spend < 7d) take FETCH_HOT path before stuck-cold check.
    Stale last_probe shouldn't override hot tier."""
    caches["activity"].record_probe(
        "adv1", STORE_A, "gmvmax", "2026-05-09", 100.0
    )  # 3d
    caches["activity"].seed_last_probe("adv1", STORE_A, "gmvmax", "2026-04-12")  # 30d
    dec = _decide(caches, hour=14, weekday=1)
    assert dec.decision == Decision.FETCH_HOT


def test_skip_path_record_probe_signature_compatible(caches):
    """Phase 3 SKIP path calls record_probe(cost=0.0, update_spend=False).
    Verify signature + semantics: last_spend NOT updated, last_probe updated."""
    caches["activity"].record_probe(
        "adv1", STORE_A, "gmvmax", "2026-05-25", cost=0.0, update_spend=False
    )
    assert (
        caches["activity"].days_since_last_spend(
            "adv1", STORE_A, "gmvmax", "2026-05-25"
        )
        is None
    )
    assert (
        caches["activity"].days_since_last_probe(
            "adv1", STORE_A, "gmvmax", "2026-05-25"
        )
        == 0
    )
