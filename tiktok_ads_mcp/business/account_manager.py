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
        """Discover GMVMAX advertisers via store_list + campaign_info fallback.

        Phase 1: Uses exclusive_authorized_advertiser_info from store_list
                 (1 API call, covers accounts that are the sole GMVMAX per store).
        Phase 2: For authorized accounts NOT found in Phase 1, checks if they
                 have GMVMAX campaigns → extracts store_id from campaign_info.
                 Catches non-exclusive accounts (multiple GMVMAX per store).

        known_store_ids: set of store_ids from STORE_PRODUCT_GROUP.
        authorized_accounts: pre-fetched list (need any advertiser_id to call API).
        """
        if not self.discovery_cache:
            return []

        from ..tools.gmvmax_store_list import get_gmvmax_store_list

        # Need any advertiser_id to call store_list (returns BC-wide data)
        if authorized_accounts is None:
            from ..tools.get_authorized_ad_accounts import get_authorized_ad_accounts

            authorized_accounts = await get_authorized_ad_accounts(self.client)

        if not authorized_accounts:
            logger.warning("discover: no authorized accounts available")
            return []

        any_adv_id = authorized_accounts[0]["advertiser_id"]

        # Phase 1: exclusive GMVMAX advertisers from store_list (1 API call)
        try:
            store_data = await get_gmvmax_store_list(self.client, any_adv_id)
        except Exception as e:
            logger.warning(f"discover: store_list failed: {e}")
            return []

        stores = store_data.get("store_list", [])
        if not stores:
            return []

        # Parse exclusive advertiser → store mapping (deduplicate by store_id)
        discovered = []
        seen_stores = set()
        exclusive_ids = set()
        for store in stores:
            sid = store.get("store_id", "")
            if not sid or sid in seen_stores:
                continue
            seen_stores.add(sid)

            exclusive = store.get("exclusive_authorized_advertiser_info", {})
            exc_adv_id = exclusive.get("advertiser_id", "")
            if not exc_adv_id:
                continue

            exclusive_ids.add(exc_adv_id)
            exc_name = exclusive.get("advertiser_name", "")
            exc_status = exclusive.get("advertiser_status", "")

            # Update discovery cache: 1 advertiser → 1 store (exclusive)
            is_banned = exc_status in (
                "STATUS_LIMIT",
                "STATUS_DISABLE_BY_BINDTOP",
                "STATUS_FROZEN",
            )
            existing = self.discovery_cache.get(exc_adv_id)
            is_new = existing is None
            was_different_store = existing and existing.get("store_ids") != [sid]

            self.discovery_cache.put(
                exc_adv_id,
                store_ids=[sid],
                ad_type="gmvmax",
                ad_name=exc_name,
            )
            if is_banned:
                self.discovery_cache.mark_banned(exc_adv_id)

            if sid in known_store_ids and (is_new or was_different_store):
                discovered.append(
                    {
                        "advertiser_id": exc_adv_id,
                        "ad_name": exc_name,
                        "store_ids": [sid],
                        "status": exc_status,
                    }
                )
                logger.info(
                    f"discover: {exc_adv_id} → store {sid} "
                    f"({'new' if is_new else 'updated'}, {exc_status})"
                )

        logger.info(
            f"discover phase 1: {len(seen_stores)} stores scanned, "
            f"{len(discovered)} new/changed exclusive accounts"
        )

        # Phase 2: non-exclusive GMVMAX accounts via campaign_info
        all_authorized_ids = {a["advertiser_id"] for a in authorized_accounts}
        cached_ids = set(self.discovery_cache.get_all_gmvmax().keys())
        # Also skip accounts already cached as "unknown" (checked recently)
        all_cached = (
            set(self.discovery_cache._load().keys()) if self.discovery_cache else set()
        )
        unknown_ids = all_authorized_ids - exclusive_ids - all_cached

        if unknown_ids:
            phase2 = await self._discover_via_campaigns(unknown_ids, known_store_ids)
            discovered.extend(phase2)

        return discovered

    async def _discover_via_campaigns(
        self,
        unknown_ids: Set[str],
        known_store_ids: Set[str],
    ) -> List[Dict]:
        """Phase 2: discover non-exclusive GMVMAX accounts via campaign_info.

        For each unknown authorized account:
        1. get_gmvmax_campaigns(page_size=1) — check if GMVMAX account
        2. If yes: get_gmvmax_campaign_info → extract store_id
        3. Cache result (gmvmax or unknown) to avoid re-checking

        Returns list of newly discovered accounts on known stores.
        """
        from ..tools.gmvmax_campaigns import get_gmvmax_campaigns
        from ..tools.gmvmax_campaign_info import get_gmvmax_campaign_info

        discovered = []
        checked = 0

        for adv_id in unknown_ids:
            try:
                # Step 1: check if account has any GMVMAX campaigns
                result = await get_gmvmax_campaigns(self.client, adv_id, page_size=1)
                campaigns = result.get("campaigns", [])

                if not campaigns:
                    # Not a GMVMAX account — cache as "unknown" to skip next time
                    self.discovery_cache.put(adv_id, store_ids=[], ad_type="unknown")
                    continue

                # Step 2: get store_id from campaign_info
                camp_id = campaigns[0]["campaign_id"]
                info = await get_gmvmax_campaign_info(self.client, adv_id, camp_id)
                store_id = info.get("info", {}).get("store_id", "")

                if not store_id:
                    self.discovery_cache.put(
                        adv_id,
                        store_ids=[],
                        ad_type="gmvmax",
                        ad_name=campaigns[0].get("campaign_name", ""),
                    )
                    logger.info(
                        f"discover phase 2: {adv_id} has campaigns but no store_id"
                    )
                    continue

                # Step 3: cache and report
                camp_status = campaigns[0].get("operation_status", "")
                existing = self.discovery_cache.get(adv_id)
                is_new = existing is None or existing.get("ad_type") != "gmvmax"
                was_different_store = existing and existing.get("store_ids") != [
                    store_id
                ]

                self.discovery_cache.put(
                    adv_id,
                    store_ids=[store_id],
                    ad_type="gmvmax",
                    ad_name=campaigns[0].get("campaign_name", ""),
                )

                if store_id in known_store_ids and (is_new or was_different_store):
                    discovered.append(
                        {
                            "advertiser_id": adv_id,
                            "ad_name": campaigns[0].get("campaign_name", ""),
                            "store_ids": [store_id],
                            "status": camp_status,
                        }
                    )
                    logger.info(
                        f"discover phase 2: {adv_id} → store {store_id} "
                        f"(non-exclusive, {camp_status})"
                    )

                checked += 1
            except TikTokPermissionError:
                # No access — mark as unknown
                self.discovery_cache.put(adv_id, store_ids=[], ad_type="unknown")
            except Exception as e:
                logger.debug(f"discover phase 2: {adv_id} error: {e}")
                continue

        logger.info(
            f"discover phase 2: {len(unknown_ids)} unknown accounts checked, "
            f"{checked} GMVMAX found, {len(discovered)} on known stores"
        )
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

        # Banned + today: skip API if banned before today (no spend possible)
        if banned and period == "today":
            ban_info = (
                self.ban_status_cache.get_status(advertiser_id)
                if self.ban_status_cache
                else None
            )
            if ban_info:
                detected_at = ban_info.get("detected_at", "")
                if detected_at and detected_at < date_str:
                    logger.info(
                        f"{ad_type} ...{advertiser_id[-6:]}: "
                        f"$0 (banned since {detected_at}, skipping today)"
                    )
                    return zero

        # Not banned, or banned + today (same-day ban): try API
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
