"""Active Roster Filter (F0) — decide whether to skip an advertiser fetch.

Why this exists
---------------
Empirical: 78 (advertiser, store_id) pairs are configured for hourly ad_report,
but only ~13 advertisers actually had spend in the last 7 days. The other ~76%
are STATUS_LIMIT / dormant / REMOVED_FROM_BC residues from operator account
churn. Polling them every hour wastes ~167 TikTok API calls per push and
collides with team_lead_report's BC token quota → rate limit 40100 storms.

This module supplies a pure-function decision per fetch_ad_cost call. The
manager.fetch_ad_cost entry consults it and either fetches (current behavior)
or returns zero (when mode='on' and decision is SKIP_*).

Three modes (env ACTIVE_ROSTER_MODE):
  - "off"    : do nothing (default; deploys as no-op).
  - "shadow" : compute decision and log it, but still fetch. Used for 24h
               validation that SKIP decisions don't correlate with actual spend.
  - "on"     : decision enforced.

Decision matrix (short-circuit order)
-------------------------------------
1. ban_status == REMOVED_FROM_BC                       → SKIP_REMOVED_FROM_BC
   (or: discovery.banned=True AND ban_cache miss → same)
2. banned=True AND status==NO_ACCESS_CONFIRMED_BANNED  → SKIP_BANNED
3. days_since_discovery < 7 (gmvmax only, ads has no   → FETCH_GRACE
   discovery cache so this clause is skipped)
3b. activity_cache MISS for (adv, store, type) AND      → FETCH_RECENTLY_SEEN
    discovery.last_seen within 7 days. Catches the
    cross-store mismatch case where an advertiser was
    reassigned to a new store_id and the old activity
    record's per-store key no longer matches. Does NOT
    override existing cold-decay skip — only fires when
    activity_cache has zero record for this exact key.
4. days_since_last_spend:
     None or >= 31  → weekly window
                      now_weekday==0 AND now_hour==PROBE_HOUR → FETCH_WEEKLY_PROBE
                      otherwise                                → SKIP_COLD_WEEKLY
     7..30          → daily window
                      now_hour==PROBE_HOUR                     → FETCH_DAILY_PROBE
                      otherwise                                → SKIP_COLD_DAILY
     < 7            → FETCH_HOT
5. fallthrough                                         → FETCH

PROBE_HOUR defaults to 8 (shop_tz); override via ACTIVE_ROSTER_PROBE_HOUR.

Safety
------
Any exception inside should_fetch is caller-handled (manager wraps in try
/except and falls back to FETCH). The decision logic itself is total — every
input path produces a Decision.
"""

import logging
import os
from dataclasses import dataclass
from datetime import date
from enum import Enum
from typing import Optional

logger = logging.getLogger(__name__)


class Decision(str, Enum):
    FETCH = "fetch"
    FETCH_HOT = "fetch_hot"
    FETCH_GRACE = "fetch_grace"
    FETCH_RECENTLY_SEEN = "fetch_recently_seen"
    FETCH_DAILY_PROBE = "fetch_daily_probe"
    FETCH_WEEKLY_PROBE = "fetch_weekly_probe"
    FETCH_STUCK_COLD = "fetch_stuck_cold"
    SKIP_BANNED = "skip_banned"
    SKIP_REMOVED_FROM_BC = "skip_removed_from_bc"
    SKIP_COLD_DAILY = "skip_cold_daily"
    SKIP_COLD_WEEKLY = "skip_cold_weekly"


_FETCH_DECISIONS = {
    Decision.FETCH,
    Decision.FETCH_HOT,
    Decision.FETCH_GRACE,
    Decision.FETCH_RECENTLY_SEEN,
    Decision.FETCH_DAILY_PROBE,
    Decision.FETCH_WEEKLY_PROBE,
    Decision.FETCH_STUCK_COLD,
}

# Stuck-cold threshold: weekly-tier cold account that hasn't been probed in
# >= this many days → force a refresh fetch (breaks SKIP_COLD_* deadlock).
# Rationale: weekly probe is single window (Mon 08:00 advertiser tz). If missed,
# next chance is 7 days later. After 14d no probe = 2 missed windows → safe to
# assume probe schedule broken or account state changed; spend 1 API to verify.
_STUCK_COLD_DAYS = 14


@dataclass
class RosterDecision:
    decision: Decision
    reason: str
    days_since_spend: Optional[int]
    days_since_discovery: Optional[int]

    @property
    def fetch(self) -> bool:
        return self.decision in _FETCH_DECISIONS


# ── env helpers ──


def get_mode() -> str:
    """Return current filter mode: 'off' (default) | 'shadow' | 'on'."""
    return os.environ.get("ACTIVE_ROSTER_MODE", "off").strip().lower() or "off"


def get_probe_hour() -> int:
    """Return the hour (0-23, shop_tz) that triggers daily/weekly probes."""
    raw = os.environ.get("ACTIVE_ROSTER_PROBE_HOUR", "8").strip()
    try:
        h = int(raw)
        if 0 <= h <= 23:
            return h
    except ValueError:
        pass
    logger.warning(
        f"[active_roster] invalid ACTIVE_ROSTER_PROBE_HOUR={raw!r}, falling back to 8"
    )
    return 8


# ── decision logic ──


def _days_between(later: str, earlier: str) -> Optional[int]:
    try:
        return (date.fromisoformat(later) - date.fromisoformat(earlier)).days
    except (ValueError, TypeError):
        return None


def should_fetch(
    advertiser_id: str,
    store_id: str,
    ad_type: str,
    *,
    shop_today: str,
    shop_now_hour: int,
    shop_now_weekday: int,
    banned: bool,
    ban_cache,
    discovery_cache,
    activity_cache,
    probe_hour: Optional[int] = None,
) -> RosterDecision:
    """Pure-function decision. Caller supplies shop-tz time triple.

    Args:
      advertiser_id, store_id, ad_type: identify the (adv, store, type) key.
      shop_today: 'YYYY-MM-DD' in shop timezone.
      shop_now_hour: 0-23 in shop timezone.
      shop_now_weekday: 0=Mon, 6=Sun.
      banned: whatever the caller already deduced for this advertiser.
      ban_cache: BanStatusCache instance.
      discovery_cache: AccountDiscoveryCache instance (may have no entry for Ads).
      activity_cache: AdvertiserActivityCache instance.
      probe_hour: optional override; defaults to get_probe_hour().

    Returns RosterDecision with .fetch property indicating action.
    """
    if probe_hour is None:
        probe_hour = get_probe_hour()

    ad_type_l = ad_type.lower()
    ban_entry = ban_cache.get_status(advertiser_id) if ban_cache else None
    ban_status = (ban_entry or {}).get("status", "")

    # 1. REMOVED_FROM_BC: hard-skip with alert hook
    if ban_status == "REMOVED_FROM_BC":
        return RosterDecision(
            decision=Decision.SKIP_REMOVED_FROM_BC,
            reason="account removed from Business Center; clean config",
            days_since_spend=None,
            days_since_discovery=None,
        )
    # Fallback: discovery says banned but ban_cache has nothing → likely removed
    discovery_entry = discovery_cache.get(advertiser_id) if discovery_cache else None
    if discovery_entry and discovery_entry.get("banned") and ban_entry is None:
        return RosterDecision(
            decision=Decision.SKIP_REMOVED_FROM_BC,
            reason="discovery banned + no ban_status record → treat as removed",
            days_since_spend=None,
            days_since_discovery=None,
        )

    # 2. NO_ACCESS_CONFIRMED_BANNED: skip (same effect as legacy ban-aware)
    if banned and ban_status == "NO_ACCESS_CONFIRMED_BANNED":
        return RosterDecision(
            decision=Decision.SKIP_BANNED,
            reason="NO_ACCESS_CONFIRMED_BANNED",
            days_since_spend=None,
            days_since_discovery=None,
        )

    # Compute spend/discovery recencies (used below)
    days_since_spend = (
        activity_cache.days_since_last_spend(
            advertiser_id, store_id, ad_type_l, shop_today
        )
        if activity_cache
        else None
    )
    days_since_probe = (
        activity_cache.days_since_last_probe(
            advertiser_id, store_id, ad_type_l, shop_today
        )
        if activity_cache
        else None
    )
    days_since_discovery: Optional[int] = None
    if discovery_entry and ad_type_l == "gmvmax":
        discovered_at = discovery_entry.get("discovered_at", "")
        days_since_discovery = _days_between(shop_today, discovered_at)

    # 3. New advertiser grace period (gmvmax only; Ads has no discovery)
    if days_since_discovery is not None and 0 <= days_since_discovery < 7:
        return RosterDecision(
            decision=Decision.FETCH_GRACE,
            reason=f"new account, discovered {days_since_discovery}d ago",
            days_since_spend=days_since_spend,
            days_since_discovery=days_since_discovery,
        )

    # 3b. Cross-store fallback: activity_cache has no record for THIS specific
    # (advertiser, store, ad_type) key, but discovery saw the advertiser
    # active within the last 7 days. This catches the case where an advertiser
    # was reassigned to a new store (config change) — the old activity record
    # is keyed by the previous store, so the per-store lookup misses and the
    # advertiser would otherwise SKIP_COLD_WEEKLY despite running fresh spend
    # on its current store. Only fires when activity_cache has no record at
    # all for this key (days_since_spend is None) — does NOT override the
    # normal cold-decay skip when there's a real stale-spend signal.
    # Regression for FlyNew-MX-Shilajit ...896976 on 2026-05-15: cost=$174.27
    # silently dropped because cache key used old store_id.
    if days_since_spend is None and discovery_entry:
        last_seen = discovery_entry.get("last_seen", "")
        days_since_last_seen = _days_between(shop_today, last_seen)
        if days_since_last_seen is not None and 0 <= days_since_last_seen < 7:
            return RosterDecision(
                decision=Decision.FETCH_RECENTLY_SEEN,
                reason=(
                    f"recently seen in discovery {days_since_last_seen}d ago, "
                    f"no activity record for this store"
                ),
                days_since_spend=days_since_spend,
                days_since_discovery=days_since_discovery,
            )

    # 4. Spend-based decay tiers
    if days_since_spend is None or days_since_spend >= 31:
        # 4a. Stuck-cold force-refresh tier (breaks SKIP_COLD_* deadlock):
        # weekly-tier cold account whose last_probe is stale >= _STUCK_COLD_DAYS
        # → force a fetch to verify still cold. Without this, any advertiser
        # that revives (spend resumes) after going cold gets silently dropped
        # until next Mon 08:00 probe.
        # NOTE: triggers only when last_probe exists but is stale. None case
        # (never probed) falls through to original weekly_probe / SKIP path —
        # SKIP-path record_probe will establish baseline on first SKIP, and
        # stuck-cold fires _STUCK_COLD_DAYS later if still cold.
        if days_since_probe is not None and days_since_probe >= _STUCK_COLD_DAYS:
            return RosterDecision(
                decision=Decision.FETCH_STUCK_COLD,
                reason=(
                    f"stuck_cold_refresh: last_probe={days_since_probe}d ago "
                    f"(spend stale {days_since_spend}d, threshold {_STUCK_COLD_DAYS}d)"
                ),
                days_since_spend=days_since_spend,
                days_since_discovery=days_since_discovery,
            )
        # weekly window: probe on Monday at PROBE_HOUR
        if shop_now_weekday == 0 and shop_now_hour == probe_hour:
            return RosterDecision(
                decision=Decision.FETCH_WEEKLY_PROBE,
                reason=(
                    f"weekly probe (last_spend={days_since_spend}d ago)"
                    if days_since_spend is not None
                    else "weekly probe (never spent)"
                ),
                days_since_spend=days_since_spend,
                days_since_discovery=days_since_discovery,
            )
        return RosterDecision(
            decision=Decision.SKIP_COLD_WEEKLY,
            reason=(
                f"cold: last_spend={days_since_spend}d ago, non-probe window"
                if days_since_spend is not None
                else "cold: never spent, non-probe window"
            ),
            days_since_spend=days_since_spend,
            days_since_discovery=days_since_discovery,
        )

    if 7 <= days_since_spend <= 30:
        # daily window: probe at PROBE_HOUR
        if shop_now_hour == probe_hour:
            return RosterDecision(
                decision=Decision.FETCH_DAILY_PROBE,
                reason=f"daily probe (last_spend={days_since_spend}d ago)",
                days_since_spend=days_since_spend,
                days_since_discovery=days_since_discovery,
            )
        return RosterDecision(
            decision=Decision.SKIP_COLD_DAILY,
            reason=(
                f"warm-dormant: last_spend={days_since_spend}d ago, non-probe hour"
            ),
            days_since_spend=days_since_spend,
            days_since_discovery=days_since_discovery,
        )

    # days_since_spend < 7
    return RosterDecision(
        decision=Decision.FETCH_HOT,
        reason=f"hot: last_spend={days_since_spend}d ago",
        days_since_spend=days_since_spend,
        days_since_discovery=days_since_discovery,
    )
