"""Core business logic: ban detection, cached fetch, balance tracking.

AdAccountManager holds a TikTokAdsClient and three caches.
All TikTok API interaction goes through the client (dual-token fallback,
retry, semaphore). Caches are injected so callers control file paths.
"""

import json
import logging
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional

from ..client import TikTokAdsClient, TikTokPermissionError
from ..cache import AdCostCache, BanStatusCache, BalanceSnapshotCache

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
    ):
        self._client = client
        self._client_factory = client_factory
        self.ad_cost_cache = ad_cost_cache
        self.ban_status_cache = ban_status_cache
        self.balance_cache = balance_cache

    @property
    def client(self) -> TikTokAdsClient:
        """Lazy client — only created when API calls are needed."""
        if self._client is None:
            if self._client_factory:
                self._client = self._client_factory()
            else:
                self._client = TikTokAdsClient()
        return self._client

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
    ):
        """Fill $0 cost in ad_cost_cache for all days after last_active_date through yesterday.

        Ensures get_range() has complete coverage for banned accounts.
        Today is excluded — it will be filled on the next run if still banned.
        """
        if not last_active_date:
            return

        from zoneinfo import ZoneInfo

        last_active = datetime.strptime(last_active_date, "%Y-%m-%d").date()
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
            if existing is None:
                self.ad_cost_cache.put_daily(
                    advertiser_id, date_str, ad_type_lower, 0.0, 0.0, 0
                )
                filled += 1
            current += timedelta(days=1)

        if filled > 0:
            print(f"    Backfilled {filled} zero-cost days after {last_active_date}")

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
    ) -> Dict:
        """Fetch single-day ad cost with ban-check, API call, cache write, and fallback.

        Returns dict with keys: cost, gmv, orders, and roi/roas.
        On TikTokPermissionError, falls back to cached data.
        """
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
            roi_key = "roi" if ad_type.lower() == "gmvmax" else "roas"
            return {"cost": 0.0, "gmv": 0.0, "orders": 0, roi_key: 0.0}

    async def fetch_ad_cost_range(
        self,
        advertiser_id: str,
        start: str,
        end: str,
        ad_type: str,
        store_ids: Optional[List[str]] = None,
    ) -> Dict:
        """Fetch date-range ad cost with cache fallback on permission error.

        Returns dict with keys: cost, gmv, orders.
        """
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
            roi_key = "roi" if ad_type.lower() == "gmvmax" else "roas"
            return {"cost": 0.0, "gmv": 0.0, "orders": 0, roi_key: 0.0}

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
