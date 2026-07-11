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

from ..client import (
    TikTokAdsClient,
    TikTokIncompleteDataError,
    TikTokPermissionError,
)
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
        editor_data_cache: Optional[EditorDataCache] = None,
        activity_cache: Optional["AdvertiserActivityCache"] = None,
    ):
        self._client = client
        self._client_factory = client_factory
        self.ad_cost_cache = ad_cost_cache
        self.ban_status_cache = ban_status_cache
        self.balance_cache = balance_cache
        self.discovery_cache = discovery_cache
        self.editor_data_cache = editor_data_cache
        self.activity_cache = activity_cache

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

        # Prune accounts banned > 60 days — they won't be reused
        pruned = self.discovery_cache.prune_stale_banned(max_days=60)
        if pruned:
            logger.info(f"discover: pruned {pruned} stale banned accounts (>60d)")

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
            # store_ids unions across rediscoveries (see AccountDiscoveryCache.put);
            # "different store" = this sid is NOT yet in the recorded history.
            was_different_store = existing and sid not in existing.get("store_ids", [])

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

        # Phase 1b: backfill store binding for stale gmvmax entries with
        # empty store_ids. Two sources of stale entries:
        #   (1) Phase 2 path "campaigns exist but campaign_info.store_id=''"
        #       (line ~270) writes store_ids=[] permanently.
        #   (2) An advertiser later became exclusive on a store, but BC-wide
        #       store_list response from previous scans didn't list that store
        #       in the perspective of the any_adv_id used (observed 2026-05).
        # First try the cheap path (use already-fetched `stores` response),
        # then per-advertiser store_list calls for the remainder (capped).
        backfilled = await self._backfill_empty_store_ids(stores, known_store_ids)
        if backfilled:
            discovered.extend(backfilled)

        # Phase 2: non-exclusive GMVMAX accounts via campaign_info
        all_authorized_ids = {a["advertiser_id"] for a in authorized_accounts}
        all_cached = (
            set(self.discovery_cache._load().keys()) if self.discovery_cache else set()
        )
        unknown_ids = all_authorized_ids - exclusive_ids - all_cached

        # Bulk seed: if too many unknowns (cold start), seed them as "unknown"
        # without API calls. Phase 1 (store_list) already found the GMVMAX ones.
        # Phase 2 only needs to check small deltas (newly authorized accounts).
        _BULK_SEED_THRESHOLD = 50
        if len(unknown_ids) > _BULK_SEED_THRESHOLD:
            logger.info(
                f"discover: bulk-seeding {len(unknown_ids)} authorized accounts "
                f"as 'unknown' (cold start, skipping Phase 2 API calls)"
            )
            for adv_id in unknown_ids:
                self.discovery_cache.put(adv_id, store_ids=[], ad_type="unknown")
            unknown_ids = set()  # All seeded, no Phase 2 needed

        # Re-validate stale "unknown" entries (>14d since last check):
        # accounts seeded as "unknown" may have become GMVMAX since.
        stale = self.discovery_cache.get_stale_unknowns(max_days=14)
        # Only re-check accounts still authorized (skip revoked ones)
        stale_authorized = stale & all_authorized_ids
        if stale_authorized:
            logger.info(
                f"discover: {len(stale_authorized)} stale unknowns "
                f"due for re-validation (>14d)"
            )
            unknown_ids |= stale_authorized

        if unknown_ids:
            phase2 = await self._discover_via_campaigns(unknown_ids, known_store_ids)
            discovered.extend(phase2)

        # Phase 3: resurrect watch — archived/retired accounts silently
        # reused by ops (2026-07-08 Hiileathy NMN incident: 7 days of
        # unreported spend). Failure must never break main discovery.
        try:
            revived = await self._resurrect_watch(known_store_ids, all_authorized_ids)
            discovered.extend(revived)
        except Exception as e:
            logger.warning(f"resurrect watch failed (non-fatal): {e}")

        return discovered

    _BACKFILL_BATCH_LIMIT = 10  # Max per-advertiser store_list calls per run

    async def _backfill_empty_store_ids(
        self,
        stores: List[Dict],
        known_store_ids: Set[str],
    ) -> List[Dict]:
        """Backfill store binding for cached gmvmax entries with store_ids=[].

        Step 1 (cheap, zero extra API): scan `stores` (already fetched in
        Phase 1) for any exclusive_authorized_advertiser_info whose advertiser
        is cached with empty store_ids — update those directly.

        Step 2 (per-advertiser API call, capped by _BACKFILL_BATCH_LIMIT):
        for the remainder, call get_gmvmax_store_list(adv_id) — that
        advertiser's perspective always includes the store it is exclusive on.

        Returns list of newly fixed accounts on known stores.
        """
        from ..tools.gmvmax_store_list import get_gmvmax_store_list

        fixed = []
        cache = self.discovery_cache._load()
        stale_ids = [
            adv_id
            for adv_id, entry in cache.items()
            if isinstance(entry, dict)
            and entry.get("ad_type") == "gmvmax"
            and not entry.get("store_ids")
        ]
        if not stale_ids:
            return []

        # Step 1: cheap pass using the response we already have
        sid_by_exclusive = {}
        for store in stores:
            sid = store.get("store_id", "")
            exc = store.get("exclusive_authorized_advertiser_info", {})
            eid = exc.get("advertiser_id", "")
            if sid and eid:
                sid_by_exclusive[eid] = (sid, exc.get("advertiser_name", ""))

        remaining = []
        for adv_id in stale_ids:
            if adv_id in sid_by_exclusive:
                sid, name = sid_by_exclusive[adv_id]
                existing = cache.get(adv_id, {})
                self.discovery_cache.put(
                    adv_id,
                    store_ids=[sid],
                    ad_type="gmvmax",
                    ad_name=name or existing.get("ad_name", ""),
                )
                logger.info(f"discover backfill (cheap): {adv_id} → store {sid}")
                if sid in known_store_ids:
                    fixed.append(
                        {
                            "advertiser_id": adv_id,
                            "ad_name": name or existing.get("ad_name", ""),
                            "store_ids": [sid],
                            "status": "BACKFILL_CHEAP",
                        }
                    )
            else:
                remaining.append(adv_id)

        # Step 2: per-advertiser store_list for remaining (capped to limit
        # API cost). Prioritize the least-recently-seen so all entries get
        # checked over multiple runs.
        if remaining:

            def _last_seen(adv_id: str) -> str:
                return cache.get(adv_id, {}).get("last_seen", "")

            batch = sorted(remaining, key=_last_seen)[: self._BACKFILL_BATCH_LIMIT]
            if len(remaining) > self._BACKFILL_BATCH_LIMIT:
                logger.info(
                    f"discover backfill: {len(remaining)} stale entries, "
                    f"checking {len(batch)} this run (batch limit)"
                )

            for adv_id in batch:
                try:
                    resp = await get_gmvmax_store_list(self.client, adv_id)
                except Exception as e:
                    logger.warning(
                        f"discover backfill: store_list failed for {adv_id}: {e}"
                    )
                    continue
                stores_for_adv = resp.get("store_list", []) or []
                found_sid = None
                found_name = ""
                for s in stores_for_adv:
                    exc = s.get("exclusive_authorized_advertiser_info", {})
                    if exc.get("advertiser_id") == adv_id:
                        found_sid = s.get("store_id", "")
                        found_name = exc.get("advertiser_name", "")
                        break
                if not found_sid:
                    # Still not exclusive of any store; leave as-is for now
                    continue
                existing = cache.get(adv_id, {})
                self.discovery_cache.put(
                    adv_id,
                    store_ids=[found_sid],
                    ad_type="gmvmax",
                    ad_name=found_name or existing.get("ad_name", ""),
                )
                logger.info(
                    f"discover backfill (per-adv): {adv_id} → store {found_sid}"
                )
                if found_sid in known_store_ids:
                    fixed.append(
                        {
                            "advertiser_id": adv_id,
                            "ad_name": found_name or existing.get("ad_name", ""),
                            "store_ids": [found_sid],
                            "status": "BACKFILL_PER_ADV",
                        }
                    )

        if fixed:
            logger.info(
                f"discover backfill: fixed {len(fixed)} entries on known stores"
            )
        return fixed

    _RESURRECT_PROBE_LIMIT = 10  # Max per-account probes per run
    # API-confirmed ban states are terminal: banned accounts do not come
    # back (user-confirmed business rule, mirrors ban_alert's skip-probe
    # short-circuit). Entries classified into these states are never
    # re-classified or probed again.
    _TERMINAL_STATUSES = frozenset(
        {"STATUS_LIMIT", "STATUS_DISABLE_BY_BINDTOP", "STATUS_FROZEN"}
    )

    async def _resurrect_watch(
        self,
        known_store_ids: Set[str],
        authorized_ids: Set[str],
    ) -> List[Dict]:
        """Phase 3: detect archived/retired accounts that ops reused.

        Candidate pool = archived (data-level ad_type="archived_gmvmax")
        ∪ retired (gmvmax + banned=True), intersected with authorized_ids —
        accounts removed from BC throw 40001 on /advertiser/info/ AND
        poison the whole batch call (PoC 2026-07-08); a retired account
        re-appearing in the authorized list is itself the entry signal.

        Steps:
        1. Classify (per entry ≤ once/24h): batch /advertiser/info/ status.
           Terminal ban states short-circuit permanently.
        2. Probe (capped, STATUS_ENABLE only, least-recently-seen first):
           tier 1 own-view store_list exclusive match; tier 2 fallback via
           active GMVMAX campaigns → campaign_info store_id (non-exclusive
           reuse).
        3. On hit: archived pool → auto-resurrect (never touches
           ban_status_cache — archived entries were never retired).
           Retired pool (ban_status REMOVED_FROM_BC) → alert-only
           "RESURRECT_SUSPECT": the 2026-04-24 safety valve (commit
           55d2ee8) forbids auto-reverting retired accounts; a human
           confirms via lark-bot scripts/unretire_account.py. The entry
           stays unchanged, so the hit re-fires every run — alert
           throttling is the consumer's job (ban_alert AlertTracker).
        """
        from ..tools.gmvmax_campaign_info import get_gmvmax_campaign_info
        from ..tools.gmvmax_campaigns import get_gmvmax_campaigns
        from ..tools.gmvmax_store_list import get_gmvmax_store_list

        if not self.discovery_cache:
            return []

        candidates = {
            adv_id: entry
            for adv_id, entry in self.discovery_cache.get_resurrect_candidates().items()
            if adv_id in authorized_ids
            and entry.get("api_status") not in self._TERMINAL_STATUSES
        }
        if not candidates:
            return []

        today = datetime.now().strftime("%Y-%m-%d")

        # Step 1: classify entries not checked in the last 24h (date-granular:
        # status_checked_at == today means "already done this calendar day").
        to_classify = [
            adv_id
            for adv_id, entry in candidates.items()
            if entry.get("status_checked_at", "") < today
        ]
        for i in range(0, len(to_classify), 100):
            batch = to_classify[i : i + 100]
            try:
                resp = await self.client._make_request(
                    "GET",
                    "advertiser/info/",
                    {
                        "advertiser_ids": json.dumps(batch),
                        "fields": json.dumps(["advertiser_id", "name", "status"]),
                    },
                )
            except Exception as e:
                logger.warning(f"resurrect watch: classify batch failed: {e}")
                continue
            for info in resp.get("data", {}).get("list", []):
                adv_id = info.get("advertiser_id", "")
                status = info.get("status", "")
                if not adv_id or not status:
                    continue
                self.discovery_cache.record_status_check(adv_id, status)
                if adv_id in candidates:
                    candidates[adv_id]["api_status"] = status

        # Step 2: probe STATUS_ENABLE candidates, least-recently-seen first.
        enabled = [
            adv_id
            for adv_id, entry in candidates.items()
            if entry.get("api_status") == "STATUS_ENABLE"
        ]
        batch = sorted(
            enabled, key=lambda a: candidates[a].get("last_seen", "")
        )[: self._RESURRECT_PROBE_LIMIT]
        if len(enabled) > self._RESURRECT_PROBE_LIMIT:
            logger.info(
                f"resurrect watch: {len(enabled)} enabled candidates, "
                f"probing {len(batch)} this run (batch limit)"
            )

        revived: List[Dict] = []
        for adv_id in batch:
            found_sid = ""
            found_name = ""
            evidence = ""
            try:
                # Tier 1: own-view store_list — an advertiser's own
                # perspective always includes the store it is exclusive on
                # (bypasses Phase 1's single-BC-view blindness).
                resp = await get_gmvmax_store_list(self.client, adv_id)
                for s in resp.get("store_list", []) or []:
                    exc = s.get("exclusive_authorized_advertiser_info", {})
                    if exc.get("advertiser_id") == adv_id:
                        found_sid = s.get("store_id", "")
                        found_name = exc.get("advertiser_name", "")
                        evidence = "exclusive store binding"
                        break

                # Tier 2: non-exclusive reuse — active GMVMAX campaigns
                # reveal the store via campaign_info (Phase 2 pattern).
                if not found_sid:
                    result = await get_gmvmax_campaigns(
                        self.client, adv_id, page_size=1
                    )
                    campaigns = result.get("campaigns", [])
                    if campaigns:
                        info = await get_gmvmax_campaign_info(
                            self.client, adv_id, campaigns[0]["campaign_id"]
                        )
                        found_sid = info.get("info", {}).get("store_id", "")
                        found_name = campaigns[0].get("campaign_name", "")
                        evidence = "active gmvmax campaign"
            except Exception as e:
                logger.debug(f"resurrect watch: probe {adv_id} failed: {e}")
                continue

            if not found_sid:
                # Not reused (yet). Refresh last_seen so the probe queue
                # rotates instead of starving later candidates (backfill
                # Step 2 lacks this and is a known counter-example).
                self.discovery_cache.mark_seen(adv_id)
                continue

            ban_entry = (
                self.ban_status_cache.get_status(adv_id)
                if self.ban_status_cache
                else None
            )
            if ban_entry and ban_entry.get("status") == "REMOVED_FROM_BC":
                # Retired pool: alert-only, never auto-revert (2026-04-24
                # safety valve). No cache mutation here.
                logger.info(
                    f"resurrect watch: RETIRED account {adv_id} suspected "
                    f"reused on store {found_sid} ({evidence}) — alert only"
                )
                if found_sid in known_store_ids:
                    revived.append(
                        {
                            "advertiser_id": adv_id,
                            "ad_name": found_name
                            or candidates[adv_id].get("ad_name", ""),
                            "store_ids": [found_sid],
                            "status": "RESURRECT_SUSPECT",
                            "evidence": evidence,
                        }
                    )
                continue

            self.discovery_cache.resurrect(adv_id, [found_sid], found_name)
            logger.info(
                f"resurrect watch: RESURRECTED {adv_id} → store {found_sid} "
                f"({evidence})"
            )
            if found_sid in known_store_ids:
                revived.append(
                    {
                        "advertiser_id": adv_id,
                        "ad_name": found_name
                        or candidates[adv_id].get("ad_name", ""),
                        "store_ids": [found_sid],
                        "status": "RESURRECTED",
                        "evidence": evidence,
                    }
                )

        # Always log a per-run summary: a silently-broken watch must not
        # look identical to a quiet round in journalctl.
        logger.info(
            f"resurrect watch: {len(candidates)} candidates, "
            f"{len(to_classify)} classified, {len(batch)} probed, "
            f"{len(revived)} hits"
        )
        return revived

    _PHASE2_BATCH_LIMIT = 20  # Max accounts to check per run (cache builds up)

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

        Capped at _PHASE2_BATCH_LIMIT per run to avoid timeout/rate-limit.
        Cache persists, so all accounts get checked over multiple runs.

        Returns list of newly discovered accounts on known stores.
        """
        from ..client import TikTokRateLimitError
        from ..tools.gmvmax_campaign_info import get_gmvmax_campaign_info
        from ..tools.gmvmax_campaigns import get_gmvmax_campaigns

        discovered = []
        checked = 0

        # Pick the staleest first (least-recently-checked or never-checked).
        # Without this, set→list iteration order is hash-based, so the same 20
        # accounts could get re-checked every run while others starve.
        # ISO date strings sort lexicographically; missing entry → "" wins (= highest priority).
        def _last_seen(adv_id: str) -> str:
            entry = self.discovery_cache.get(adv_id)
            return entry.get("last_seen", "") if entry else ""

        batch = sorted(unknown_ids, key=_last_seen)[: self._PHASE2_BATCH_LIMIT]

        if len(unknown_ids) > self._PHASE2_BATCH_LIMIT:
            logger.info(
                f"discover phase 2: {len(unknown_ids)} unknown, "
                f"checking {len(batch)} this run (batch limit)"
            )

        for adv_id in batch:
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
                # store_ids unions across rediscoveries (see AccountDiscoveryCache.put);
                # "different store" = this store_id is NOT yet in the recorded history.
                was_different_store = existing and store_id not in existing.get(
                    "store_ids", []
                )

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
            except TikTokRateLimitError:
                logger.warning(
                    f"discover phase 2: rate limited after {checked} checks, "
                    f"stopping (will resume next run)"
                )
                break
            except Exception as e:
                logger.debug(f"discover phase 2: {adv_id} error: {e}")
                continue

        remaining = len(unknown_ids) - len(batch)
        logger.info(
            f"discover phase 2: checked {len(batch)}/{len(unknown_ids)}, "
            f"{checked} GMVMAX found, {len(discovered)} on known stores"
            + (f", {remaining} deferred to next run" if remaining > 0 else "")
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
        self,
        advertiser_id: str,
        ad_type: str,
        store_ids: Optional[List[str]] = None,
        shop_tz: str = "",
    ) -> str:
        """Find the last date with non-zero cost in ad_cost_cache.

        Scans from today backwards up to 45 days.
        Uses shop timezone for date calculation.
        Returns date string like '2026-03-16', or '' if nothing found.

        For GMVMAX, `store_ids` MUST list every store this advertiser operates
        across; "active that day" = ANY store had cost > 0. For Ads, pass `[""]`
        (default) — Ads keys carry no store dimension.
        """
        from zoneinfo import ZoneInfo

        if shop_tz:
            tz = ZoneInfo(shop_tz)
            today = datetime.now(timezone.utc).astimezone(tz).date()
        else:
            today = datetime.now(timezone.utc).date()
        ad_type_lower = ad_type.lower()
        keys_for_iter = list(store_ids) if store_ids else [""]
        for i in range(45):
            d = today - timedelta(days=i)
            date_str = d.strftime("%Y-%m-%d")
            for store_id in keys_for_iter:
                entry = self.ad_cost_cache.get_daily(
                    advertiser_id, date_str, ad_type_lower, store_id=store_id
                )
                if entry and entry["cost"] > 0:
                    return date_str
        return ""

    def backfill_zero_days(
        self,
        advertiser_id: str,
        ad_type: str,
        last_active_date: str,
        store_ids: Optional[List[str]] = None,
        shop_tz: str = "",
        detected_at: str = "",
        force_overwrite: bool = False,
    ):
        """Fill $0 cost in ad_cost_cache for all days after last_active_date through yesterday.

        Ensures get_range() has complete coverage for banned accounts.
        Today is excluded — it will be filled on the next run if still banned.

        For GMVMAX, `store_ids` MUST contain every store this advertiser operates
        across (one cache key per store) — caller is responsible for collecting
        the full set, since one advertiser_id may span multiple PRODUCT_GROUPS
        rows. For Ads, pass `[""]` (or omit; the default `[""]` matches Ads' no-
        store-dimension key).

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
        keys_for_iter = list(store_ids) if store_ids else [""]

        filled = 0
        current = last_active + timedelta(days=1)
        while current < today:
            date_str = current.strftime("%Y-%m-%d")
            for store_id in keys_for_iter:
                existing = self.ad_cost_cache.get_daily(
                    advertiser_id, date_str, ad_type_lower, store_id=store_id
                )
                if existing is None or (
                    force_overwrite and existing.get("cost", 0) > 0
                ):
                    self.ad_cost_cache.put_daily(
                        advertiser_id,
                        date_str,
                        ad_type_lower,
                        0.0,
                        0.0,
                        0,
                        store_id=store_id,
                    )
                    filled += 1
            current += timedelta(days=1)

        if filled > 0:
            store_note = (
                f" × {len(keys_for_iter)} store(s)" if len(keys_for_iter) > 1 else ""
            )
            print(
                f"    Backfilled {filled} zero-cost days{store_note} after {start_date}"
            )

    # ── Rescue cache (async — tries to save data before API access revoked) ──

    async def rescue_cache(
        self,
        advertiser_id: str,
        ad_type: str,
        store_id: str = "",
        shop_tz: str = "Etc/GMT+8",
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
                advertiser_id, date_str, ad_type.lower(), store_id=store_id
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
                    store_id=store_id,
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
        shop_tz: str = "Etc/GMT+8",
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

        store_id_for_cache = store_ids[0] if store_ids else ""

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
                advertiser_id, date_str, ad_type.lower(), store_id=store_id_for_cache
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

        # Active Roster Filter (F0): decide whether to skip an unneeded API call.
        # Only consulted when legacy ban-aware paths above didn't already return.
        # Mode 'off' (default) → no-op; 'shadow' → log decision but still fetch;
        # 'on' → enforce SKIP_* by returning zero.
        if self.activity_cache and self.ban_status_cache and self.discovery_cache:
            try:
                from .active_roster import get_mode, should_fetch
                from zoneinfo import ZoneInfo

                roster_mode = get_mode()
                if roster_mode != "off":
                    now_shop = datetime.now(ZoneInfo(shop_tz))
                    dec = should_fetch(
                        advertiser_id,
                        store_id_for_cache,
                        ad_type.lower(),
                        shop_today=date_str,
                        shop_now_hour=now_shop.hour,
                        shop_now_weekday=now_shop.weekday(),
                        banned=banned,
                        ban_cache=self.ban_status_cache,
                        discovery_cache=self.discovery_cache,
                        activity_cache=self.activity_cache,
                    )
                    logger.info(
                        f"[active_roster:{roster_mode}] adv=...{advertiser_id[-6:]} "
                        f"store={store_id_for_cache} type={ad_type} period={period} "
                        f"decision={dec.decision.value} reason={dec.reason} "
                        f"days_since_spend={dec.days_since_spend}"
                    )
                    if roster_mode == "on" and not dec.fetch:
                        # Record SKIP as a probe event (no spend update) so
                        # active_roster's stuck-cold tier can use last_probe_date
                        # to detect when a cold account has been silently skipped
                        # too long (>= _STUCK_COLD_DAYS). Without this, SKIP_COLD_*
                        # decisions never update activity_cache → deadlock.
                        if self.activity_cache:
                            try:
                                self.activity_cache.record_probe(
                                    advertiser_id,
                                    store_id_for_cache,
                                    ad_type.lower(),
                                    date_str,
                                    cost=0.0,
                                    update_spend=False,
                                )
                            except Exception as e:  # noqa: BLE001
                                logger.error(
                                    f"[active_roster] SKIP-path record_probe error: {e}"
                                )
                        return zero
            except Exception as e:  # noqa: BLE001 — never let filter take down fetch
                logger.error(f"[active_roster] filter error, fallback to FETCH: {e}")

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
                store_id=store_id_for_cache,
            )
            # Active Roster: record probe for future decay decisions.
            if self.activity_cache:
                try:
                    self.activity_cache.record_probe(
                        advertiser_id,
                        store_id_for_cache,
                        ad_type.lower(),
                        date_str,
                        m["cost"],
                    )
                except Exception as e:  # noqa: BLE001
                    logger.error(f"[active_roster] record_probe error: {e}")
            if banned and m["cost"] > 0:
                logger.info(
                    f"{ad_type} ...{advertiser_id[-6:]}: "
                    f"${m['cost']:,.2f} (banned, pre-ban spend)"
                )
            return m
        except TikTokPermissionError:
            cached = self.ad_cost_cache.get_daily(
                advertiser_id, date_str, ad_type.lower(), store_id=store_id_for_cache
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
        aligned: bool = False,
        shop_tz: str = "Etc/GMT+8",
    ) -> Dict:
        """Fetch date-range ad cost with ban-awareness and cache fallback.

        Ban-aware: banned accounts use cache directly (avoid API calls for ranges).

        When `aligned=True`, the underlying range fetch uses the per-day shop-tz
        aligned aggregator (`get_*_range_report_aligned`) which sums hourly
        single-day fetches. This produces shop-day-correct totals matching
        AdCostCache writes (which also go through the aligned single-day path).
        Default `aligned=False` preserves the legacy ad_tz native range API.

        Returns dict with keys: cost, gmv, orders.
        """
        roi_key = "roi" if ad_type.lower() == "gmvmax" else "roas"
        zero = {"cost": 0.0, "gmv": 0.0, "orders": 0, roi_key: 0.0}
        store_id_for_cache = store_ids[0] if store_ids else ""

        # Banned accounts: cache-only for ranges (too many API calls otherwise)
        if banned:
            cached = self.ad_cost_cache.get_range(
                advertiser_id,
                start,
                end,
                ad_type.lower(),
                allow_partial=True,
                store_id=store_id_for_cache,
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
                store_id_for_cache,
                start,
                end,
                aligned=aligned,
                shop_tz=shop_tz,
            )
            return m
        except TikTokPermissionError:
            cached = self.ad_cost_cache.get_range(
                advertiser_id,
                start,
                end,
                ad_type.lower(),
                allow_partial=True,
                store_id=store_id_for_cache,
            )
            if cached:
                logger.info(
                    f"{ad_type} ...{advertiser_id[-6:]}: "
                    f"${cached['cost']:.2f} from cache (no permission)"
                )
                return cached
            return zero
        except TikTokIncompleteDataError:
            # Transient API lag (rate-limit truncated mid-window). Cache typically
            # holds the prior successful fetch — prefer that over hard-failing,
            # since a slightly stale value beats dropping the entire account.
            # If cache is also empty, propagate so caller can mark the error.
            cached = self.ad_cost_cache.get_range(
                advertiser_id,
                start,
                end,
                ad_type.lower(),
                allow_partial=True,
                store_id=store_id_for_cache,
            )
            if cached:
                logger.warning(
                    f"{ad_type} ...{advertiser_id[-6:]}: incomplete API data, "
                    f"using cache ${cached['cost']:.2f}"
                )
                return cached
            raise

    # ── Per-store breakdown (decoupled from bitable per-row binding) ──

    async def fetch_gmvmax_breakdown(
        self,
        advertiser_id: str,
        date_str: str,
        store_ids: List[str],
        shop_tz: str = "Etc/GMT+8",
        period: str = "today",
        banned: bool = False,
    ) -> Dict[str, Dict]:
        """Per-store GMVMAX breakdown for one shop-tz day.

        TikTok API limits store_ids to 1 per call, so we query each store
        independently in parallel and assemble the breakdown. Callers
        attribute each store's spend to the right product group via
        STORE_PRODUCT_GROUP, eliminating the bitable (advertiser, store)
        binding as a routing dependency.

        Returns {store_id: {cost, gmv, orders, roi}}. Stores with no spend
        are omitted.
        """
        import asyncio

        if banned and period != "today":
            return {}
        if banned and period == "today":
            ban_info = (
                self.ban_status_cache.get_status(advertiser_id)
                if self.ban_status_cache
                else None
            )
            if ban_info:
                detected_at = ban_info.get("detected_at", "")
                if detected_at and detected_at < date_str:
                    return {}

        async def _one(sid):
            try:
                m = await self._fetch_single_report(
                    advertiser_id, "GMVMAX", sid, date_str, shop_tz
                )
                return sid, m, None
            except TikTokPermissionError as e:
                return sid, None, e
            except Exception as e:
                return sid, None, e

        results = await asyncio.gather(*[_one(sid) for sid in store_ids])
        out: Dict[str, Dict] = {}
        first_perm_err = None
        for sid, m, err in results:
            if err is not None:
                if isinstance(err, TikTokPermissionError) and first_perm_err is None:
                    first_perm_err = err
                continue
            cost = m.get("cost", 0.0)
            gmv = m.get("gmv", 0.0)
            orders = int(m.get("orders", 0))
            if cost == 0 and gmv == 0 and orders == 0:
                continue
            out[sid] = {
                "cost": cost,
                "gmv": gmv,
                "orders": orders,
                "roi": m.get("roi", 0.0),
            }

        if not out and first_perm_err is not None:
            logger.info(f"GMVMAX ...{advertiser_id[-6:]}: no permission across stores")
        return out

    async def fetch_gmvmax_range_breakdown(
        self,
        advertiser_id: str,
        start: str,
        end: str,
        store_ids: List[str],
        banned: bool = False,
    ) -> Dict[str, Dict]:
        """Per-store GMVMAX date-range breakdown. Single-store API loop."""
        import asyncio

        if banned:
            return {}

        async def _one(sid):
            try:
                m = await self._fetch_range_report(
                    advertiser_id, "GMVMAX", sid, start, end
                )
                return sid, m, None
            except TikTokPermissionError as e:
                return sid, None, e
            except Exception as e:
                return sid, None, e

        results = await asyncio.gather(*[_one(sid) for sid in store_ids])
        out: Dict[str, Dict] = {}
        for sid, m, err in results:
            if err is not None:
                continue
            cost = m.get("cost", 0.0)
            gmv = m.get("gmv", 0.0)
            orders = int(m.get("orders", 0))
            if cost == 0 and gmv == 0 and orders == 0:
                continue
            out[sid] = {
                "cost": cost,
                "gmv": gmv,
                "orders": orders,
                "roi": m.get("roi", 0.0),
            }
        return out

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
        aligned: bool = False,
        shop_tz: str = "Etc/GMT+8",
    ) -> Dict:
        """Dispatch to the appropriate range report tool.

        aligned=True: uses shop-tz per-day aligned aggregator (matches
        AdCostCache write path, more API calls but business-canonical).
        aligned=False (default): legacy ad_tz native single-call range API.
        """
        from ..tools.range_reports import (
            get_ads_range_report,
            get_ads_range_report_aligned,
            get_gmvmax_range_report,
            get_gmvmax_range_report_aligned,
        )

        if ad_type.lower() == "gmvmax" and store_id:
            if aligned:
                return await get_gmvmax_range_report_aligned(
                    self.client,
                    advertiser_id,
                    [store_id],
                    start_date,
                    end_date,
                    shop_tz=shop_tz,
                )
            return await get_gmvmax_range_report(
                self.client,
                advertiser_id,
                [store_id],
                start_date,
                end_date,
            )
        else:
            if aligned:
                return await get_ads_range_report_aligned(
                    self.client,
                    advertiser_id,
                    start_date,
                    end_date,
                    shop_tz=shop_tz,
                )
            return await get_ads_range_report(
                self.client,
                advertiser_id,
                start_date,
                end_date,
            )
