"""Core business logic: ban detection, cached fetch, balance tracking, discovery.

AdAccountManager holds a TikTokAdsClient and four caches.
All TikTok API interaction goes through the client (dual-token fallback,
retry, semaphore). Caches are injected so callers control file paths.
"""

from __future__ import annotations

import json
import logging
from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING, Dict, List, Optional, Set

from ..client import TikTokAdsClient, TikTokPermissionError
from ..cache import (
    AdCostCache,
    BanStatusCache,
    BalanceSnapshotCache,
)

if TYPE_CHECKING:
    from ..cache import AccountDiscoveryCache

logger = logging.getLogger(__name__)


class AdAccountManager:
    """Owns all TikTok Ads business logic: API + cache + ban state machine."""

    def __init__(
        self,
        client: Optional[TikTokAdsClient] = None,
        ad_cost_cache: Optional[AdCostCache] = None,
        ban_status_cache: Optional[BanStatusCache] = None,
        balance_cache: Optional[BalanceSnapshotCache] = None,
        client_factory=None,
        discovery_cache: Optional["AccountDiscoveryCache"] = None,
    ):
        self._client = client
        self._client_factory = client_factory
        self.ad_cost_cache = ad_cost_cache
        self.ban_status_cache = ban_status_cache
        self.balance_cache = balance_cache
        self.discovery_cache = discovery_cache

    @property
    def client(self) -> TikTokAdsClient:
        """Lazy client — only created when API calls are needed."""
        if self._client is None:
            if self._client_factory:
                self._client = self._client_factory()
            else:
                self._client = TikTokAdsClient()
        return self._client

    # ── Account discovery ─────────────────────────────────────────────

    async def discover_new_accounts(
        self,
        known_store_ids: Set[str],
        authorized_accounts: Optional[List[Dict]] = None,
    ) -> List[Dict]:
        """Incremental scan: find new GMVMAX accounts from BC authorized list.

        1. get_authorized_ad_accounts() → all BC accounts (~1000+)
           (or use pre-fetched authorized_accounts to avoid duplicate API call)
        2. Filter: already in discovery_cache → skip
        3. For each new account: get_gmvmax_store_list(adv_id)
           - Has store → GMVMAX, write to cache
           - No store → "unknown", write to cache (won't rescan)
        4. Return list of newly discovered GMVMAX accounts

        known_store_ids: set of store_ids from static config (STORE_PRODUCT_GROUP).
        Used to tag matched vs unmatched stores in the result.

        authorized_accounts: pre-fetched list from get_authorized_ad_accounts().
        If None, fetches from API (costs one API call).
        """
        if not self.discovery_cache:
            return []

        from ..tools.get_authorized_ad_accounts import get_authorized_ad_accounts
        from ..tools.gmvmax_store_list import get_gmvmax_store_list

        # Step 1: Get all BC authorized accounts (reuse if provided)
        if authorized_accounts is None:
            all_accounts = await get_authorized_ad_accounts(self.client)
        else:
            all_accounts = authorized_accounts
        all_ids = {a["advertiser_id"] for a in all_accounts}

        # Step 2: Filter to unknown accounts
        new_ids = self.discovery_cache.get_unknown_ids(all_ids)
        if not new_ids:
            logger.info("discover: no new accounts found")
            return []

        logger.info(f"discover: {len(new_ids)} new accounts to classify")

        # Build name lookup from authorized list
        name_map = {
            a["advertiser_id"]: a.get("advertiser_name", "") for a in all_accounts
        }

        # Step 3: Classify each new account
        discovered = []
        for adv_id in new_ids:
            ad_name = name_map.get(adv_id, "")
            try:
                store_data = await get_gmvmax_store_list(self.client, adv_id)
                stores = store_data.get("store_list", [])
                if stores:
                    store_ids = [
                        s.get("store_id", "") for s in stores if s.get("store_id")
                    ]
                    self.discovery_cache.put(
                        adv_id,
                        store_ids=store_ids,
                        ad_type="gmvmax",
                        ad_name=ad_name,
                    )
                    matched = [sid for sid in store_ids if sid in known_store_ids]
                    unmatched = [sid for sid in store_ids if sid not in known_store_ids]
                    entry = {
                        "advertiser_id": adv_id,
                        "ad_name": ad_name,
                        "store_ids": store_ids,
                        "matched_stores": matched,
                        "unmatched_stores": unmatched,
                    }
                    discovered.append(entry)
                    logger.info(
                        f"discover: {adv_id} → GMVMAX, "
                        f"{len(matched)} matched, {len(unmatched)} unmatched stores"
                    )
                else:
                    # No stores → not GMVMAX, mark as unknown to skip next time
                    self.discovery_cache.put(
                        adv_id,
                        store_ids=[],
                        ad_type="unknown",
                        ad_name=ad_name,
                    )
            except TikTokPermissionError:
                # No permission → skip, don't cache (might get permission later)
                logger.debug(f"discover: {adv_id} no permission, skipping")
            except Exception as e:
                logger.warning(f"discover: {adv_id} error: {e}")

        logger.info(f"discover: done, {len(discovered)} new GMVMAX accounts found")
        return discovered

    # ── Probe account status ─────────────────────────────────────────

    async def probe_account(self, advertiser_id: str) -> Dict:
        """Check account status via /advertiser/info/.

        Uses client._make_request for dual-token fallback.
        Returns dict with keys: name, status, ad_tz, error.

        Special handling: TikTokPermissionError means all tokens lack access,
        returned as status="NO_ACCESS" (not raised).
        """
        params = {
            "advertiser_ids": json.dumps([advertiser_id]),
            "fields": json.dumps(["name", "status", "timezone"]),
        }
        try:
            data = await self.client._make_request("GET", "advertiser/info/", params)
            adv_list = data.get("data", {}).get("list", [])
            if adv_list:
                info = adv_list[0]
                return {
                    "name": info.get("name", ""),
                    "status": info.get("status", "UNKNOWN"),
                    "ad_tz": info.get("timezone", ""),
                    "error": None,
                }
            return {"name": "", "status": "UNKNOWN", "ad_tz": "", "error": None}
        except TikTokPermissionError:
            return {
                "name": "",
                "status": "NO_ACCESS",
                "ad_tz": "",
                "error": "No token has permission",
            }
        except Exception as e:
            return {"name": "", "status": "ERROR", "ad_tz": "", "error": str(e)}

    # ── Cache helpers (sync — called from ThreadPoolExecutor) ────────

    def find_last_active_date(
        self, advertiser_id: str, ad_type: str, shop_tz: str = ""
    ) -> str:
        """Find the last date with non-zero cost in ad_cost_cache.

        Scans from today backwards up to 45 days.
        Uses shop timezone for date calculation.
        Returns date string like '2026-03-16', or '' if nothing found.
        """
        from zoneinfo import ZoneInfo

        if shop_tz:
            tz = ZoneInfo(shop_tz)
            today = datetime.now(timezone.utc).astimezone(tz).date()
        else:
            today = datetime.now(timezone.utc).date()
        ad_type_lower = ad_type.lower()
        for i in range(45):
            d = today - timedelta(days=i)
            date_str = d.strftime("%Y-%m-%d")
            entry = self.ad_cost_cache.get_daily(advertiser_id, date_str, ad_type_lower)
            if entry and entry["cost"] > 0:
                return date_str
        return ""

    def backfill_zero_days(
        self,
        advertiser_id: str,
        ad_type: str,
        last_active_date: str,
        shop_tz: str = "",
        detected_at: str = "",
        force_overwrite: bool = False,
    ):
        """Fill $0 cost in ad_cost_cache for all days after last_active_date through yesterday.

        Ensures get_range() has complete coverage for banned accounts.
        Today is excluded — it will be filled on the next run if still banned.

        If last_active_date is empty but detected_at is provided, uses
        detected_at - 1 day as the start point (ensures post-ban days are zeroed).

        If force_overwrite is True, overwrites existing non-zero cache entries
        (used to clear stale data for NO_ACCESS_CONFIRMED_BANNED accounts).
        """
        start_date = last_active_date
        if not start_date and detected_at:
            # Use day before ban detection as fallback start
            start_date = (
                datetime.strptime(detected_at, "%Y-%m-%d") - timedelta(days=1)
            ).strftime("%Y-%m-%d")

        if not start_date:
            return

        from zoneinfo import ZoneInfo

        last_active = datetime.strptime(start_date, "%Y-%m-%d").date()
        if shop_tz:
            tz = ZoneInfo(shop_tz)
            today = datetime.now(timezone.utc).astimezone(tz).date()
        else:
            today = datetime.now(timezone.utc).date()
        ad_type_lower = ad_type.lower()

        filled = 0
        current = last_active + timedelta(days=1)
        while current < today:
            date_str = current.strftime("%Y-%m-%d")
            existing = self.ad_cost_cache.get_daily(
                advertiser_id, date_str, ad_type_lower
            )
            if existing is None or (force_overwrite and existing.get("cost", 0) > 0):
                self.ad_cost_cache.put_daily(
                    advertiser_id, date_str, ad_type_lower, 0.0, 0.0, 0
                )
                filled += 1
            current += timedelta(days=1)

        if filled > 0:
            print(f"    Backfilled {filled} zero-cost days after {start_date}")

    # ── Rescue cache (async — tries to save data before API access revoked) ──

    async def rescue_cache(
        self,
        advertiser_id: str,
        ad_type: str,
        store_id: str = "",
        shop_tz: str = "America/Los_Angeles",
        get_day_boundaries=None,
    ):
        """Try to cache today+yesterday data for a newly banned account.

        get_day_boundaries is injected by the caller (lark-bot's shop_api function)
        since it depends on the Shop API domain.
        """
        if get_day_boundaries is None:
            return

        boundaries = get_day_boundaries(shop_tz)
        for period in ("today", "yesterday"):
            b = boundaries[period]
            date_str = b["date_str"]
            existing = self.ad_cost_cache.get_daily(
                advertiser_id, date_str, ad_type.lower()
            )
            if existing is not None:
                continue
            try:
                m = await self._fetch_single_report(
                    advertiser_id, ad_type, store_id, date_str, shop_tz
                )
                self.ad_cost_cache.put_daily(
                    advertiser_id,
                    date_str,
                    ad_type.lower(),
                    m["cost"],
                    m["gmv"],
                    m["orders"],
                )
                print(
                    f"    Rescued {period} cache for ...{advertiser_id[-6:]}: ${m['cost']:,.2f}"
                )
            except Exception as e:
                print(f"    Rescue {period} failed for ...{advertiser_id[-6:]}: {e}")

    # ── Fetch ad cost (single day, with ban-check + cache) ───────────

    async def fetch_ad_cost(
        self,
        advertiser_id: str,
        date_str: str,
        ad_type: str,
        shop_tz: str = "America/Los_Angeles",
        store_ids: Optional[List[str]] = None,
        period: str = "today",
        banned: bool = False,
    ) -> Dict:
        """Fetch single-day ad cost with ban-awareness, API call, cache, and fallback.

        Ban-aware logic (all TikTok business logic lives here, not in callers):
        - banned + today: try API (captures pre-ban spend), fallback to $0
        - banned + non-today: try cache first (avoid API call), skip if no cache
        - not banned: try API, fallback to cache on permission error

        Returns dict with keys: cost, gmv, orders, and roi/roas.
        """
        roi_key = "roi" if ad_type.lower() == "gmvmax" else "roas"
        zero = {"cost": 0.0, "gmv": 0.0, "orders": 0, roi_key: 0.0}

        # Banned + non-today: cache-first (avoid unnecessary API calls)
        if banned and period != "today":
            # Validate: don't return stale cache from before ban detection
            # If date_str is after detected_at and status is NO_ACCESS,
            # the cache might be stale (written before ban was detected)
            ban_info = (
                self.ban_status_cache.get_status(advertiser_id)
                if self.ban_status_cache
                else None
            )
            detected_at = ban_info.get("detected_at", "") if ban_info else ""
            ban_status = ban_info.get("status", "") if ban_info else ""

            cached = self.ad_cost_cache.get_daily(
                advertiser_id, date_str, ad_type.lower()
            )
            if cached and cached["cost"] > 0:
                # For NO_ACCESS accounts, reject cache for dates after ban
                # (stale data written before ban was detected)
                if (
                    ban_status == "NO_ACCESS_CONFIRMED_BANNED"
                    and detected_at
                    and date_str >= detected_at
                ):
                    logger.info(
                        f"{ad_type} ...{advertiser_id[-6:]}: "
                        f"${cached['cost']:,.2f} in cache REJECTED "
                        f"(NO_ACCESS, date {date_str} >= detected {detected_at})"
                    )
                    return zero
                logger.info(
                    f"{ad_type} ...{advertiser_id[-6:]}: "
                    f"${cached['cost']:,.2f} from cache (banned)"
                )
                return cached
            return zero

        # Not banned, or banned + today: try API
        try:
            m = await self._fetch_single_report(
                advertiser_id,
                ad_type,
                store_ids[0] if store_ids else "",
                date_str,
                shop_tz,
            )
            # Cache the result
            self.ad_cost_cache.put_daily(
                advertiser_id,
                date_str,
                ad_type.lower(),
                m["cost"],
                m["gmv"],
                m["orders"],
            )
            if banned and m["cost"] > 0:
                logger.info(
                    f"{ad_type} ...{advertiser_id[-6:]}: "
                    f"${m['cost']:,.2f} (banned, pre-ban spend)"
                )
            return m
        except TikTokPermissionError:
            cached = self.ad_cost_cache.get_daily(
                advertiser_id, date_str, ad_type.lower()
            )
            if cached and cached["cost"] > 0:
                logger.info(
                    f"{ad_type} ...{advertiser_id[-6:]}: "
                    f"${cached['cost']:,.2f} from cache (no permission)"
                )
                return cached
            logger.info(f"{ad_type} ...{advertiser_id[-6:]}: no permission, no cache")
            return zero

    async def fetch_ad_cost_range(
        self,
        advertiser_id: str,
        start: str,
        end: str,
        ad_type: str,
        store_ids: Optional[List[str]] = None,
        banned: bool = False,
    ) -> Dict:
        """Fetch date-range ad cost with ban-awareness and cache fallback.

        Ban-aware: banned accounts use cache directly (avoid API calls for ranges).
        Returns dict with keys: cost, gmv, orders.
        """
        roi_key = "roi" if ad_type.lower() == "gmvmax" else "roas"
        zero = {"cost": 0.0, "gmv": 0.0, "orders": 0, roi_key: 0.0}

        # Banned accounts: cache-only for ranges (too many API calls otherwise)
        if banned:
            cached = self.ad_cost_cache.get_range(
                advertiser_id, start, end, ad_type.lower(), allow_partial=True
            )
            if cached:
                days_info = ""
                if "cached_days" in cached:
                    days_info = (
                        f" ({cached['cached_days']}/{cached['total_days']} days)"
                    )
                logger.info(
                    f"{ad_type} ...{advertiser_id[-6:]}: "
                    f"${cached['cost']:.2f} from cache (banned){days_info}"
                )
                return cached
            return zero

        try:
            m = await self._fetch_range_report(
                advertiser_id,
                ad_type,
                store_ids[0] if store_ids else "",
                start,
                end,
            )
            return m
        except TikTokPermissionError:
            cached = self.ad_cost_cache.get_range(
                advertiser_id, start, end, ad_type.lower(), allow_partial=True
            )
            if cached:
                logger.info(
                    f"{ad_type} ...{advertiser_id[-6:]}: "
                    f"${cached['cost']:.2f} from cache (no permission)"
                )
                return cached
            return zero

    # ── Balance ──────────────────────────────────────────────────────

    async def get_advertiser_balance(self, advertiser_id: str) -> Dict:
        """Get advertiser balance via /advertiser/info/.

        Returns dict with keys: balance, name.
        Automatically saves balance snapshot.
        """
        params = {
            "advertiser_ids": json.dumps([advertiser_id]),
            "fields": json.dumps(["balance", "name"]),
        }
        data = await self.client._make_request("GET", "advertiser/info/", params)
        adv_list = data.get("data", {}).get("list", [])
        if adv_list:
            info = adv_list[0]
            return {
                "balance": str(info.get("balance", 0)),
                "name": info.get("name", ""),
            }
        raise Exception(f"Fund API error {data.get('code')}: {data.get('message')}")

    # ── Internal helpers ─────────────────────────────────────────────

    async def _fetch_single_report(
        self,
        advertiser_id: str,
        ad_type: str,
        store_id: str,
        date_str: str,
        shop_tz: str,
    ) -> Dict:
        """Dispatch to the appropriate aligned report tool."""
        from ..tools.ads_report_aligned import get_ads_report_aligned
        from ..tools.gmvmax_report_aligned import get_gmvmax_report_aligned

        if ad_type.lower() == "gmvmax" and store_id:
            result = await get_gmvmax_report_aligned(
                self.client,
                advertiser_id,
                date_str,
                [store_id],
                shop_tz=shop_tz,
            )
            metrics = result.get("metrics", {})
            return {
                "cost": metrics.get("cost", 0.0),
                "gmv": metrics.get("gross_revenue", 0.0),
                "orders": int(metrics.get("orders", 0)),
                "roi": result.get("roi", 0.0),
            }
        else:
            result = await get_ads_report_aligned(
                self.client,
                advertiser_id,
                date_str,
                shop_tz=shop_tz,
            )
            metrics = result.get("metrics", {})
            return {
                "cost": metrics.get("cost", 0.0),
                "gmv": metrics.get("gmv", 0.0),
                "orders": int(metrics.get("orders", 0)),
                "roas": result.get("roas", 0.0),
            }

    async def _fetch_range_report(
        self,
        advertiser_id: str,
        ad_type: str,
        store_id: str,
        start_date: str,
        end_date: str,
    ) -> Dict:
        """Dispatch to the appropriate range report tool."""
        from ..tools.range_reports import (
            get_ads_range_report,
            get_gmvmax_range_report,
        )

        if ad_type.lower() == "gmvmax" and store_id:
            return await get_gmvmax_range_report(
                self.client,
                advertiser_id,
                [store_id],
                start_date,
                end_date,
            )
        else:
            return await get_ads_range_report(
                self.client,
                advertiser_id,
                start_date,
                end_date,
            )
