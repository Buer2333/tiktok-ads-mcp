"""Cache for auto-discovered GMVMAX advertiser accounts.

Maps advertiser_id → {store_ids, ad_type, ad_name, discovered_at, last_seen, banned}.
Populated by scanning BC authorized accounts + gmv_max/store/list/ API.
Replaces manual bitable maintenance for GMVMAX account mapping.

Schema:
{
  "<advertiser_id>": {
    "store_ids": ["7495613592836409756"],
    "ad_type": "gmvmax",           // "gmvmax" | "unknown" | "archived_gmvmax" (data-level, see below)
    "ad_name": "FLYL-CSW-...",
    "discovered_at": "2026-03-28",
    "last_seen": "2026-03-28",
    "banned": false,
    "api_status": "STATUS_ENABLE",       // optional: resurrect-watch classification
    "status_checked_at": "2026-07-08"    // optional: last classification date
  }
}

"archived_gmvmax" is written by data-level migrations (2026-05-18 zombie
cleanup), never by this library. All downstream consumers filter on
ad_type == "gmvmax", so archived entries are invisible until resurrect()
flips them back. The resurrect watch (account_manager._resurrect_watch)
is the only reader of archived entries — without it they are a one-way
door (2026-07-08 Hiileathy NMN incident: an archived account was reused
by ops and its spend went unreported for 7 days).
"""

import json
import threading
from datetime import date
from pathlib import Path
from typing import Dict, List, Optional, Set


def _union_store_ids(existing_ids: List[str], new_ids: List[str]) -> List[str]:
    """Union new store_ids into existing history — never shrink a real
    store history (2026-05-25 NAD+ gap: binding flips must not orphan
    month-to-date attribution)."""
    merged = list(existing_ids)
    for sid in new_ids:
        if sid not in merged:
            merged.append(sid)
    return merged


class AccountDiscoveryCache:
    """Thread-safe file-based cache for auto-discovered ad accounts."""

    def __init__(self, cache_dir: Path, seed_file: Optional[Path] = None):
        self._cache_file = cache_dir / "account_discovery.json"
        self._seed_file = seed_file
        self._lock = threading.Lock()
        self._data: Optional[Dict] = None

    def _load(self) -> Dict:
        if self._data is not None:
            return self._data
        seed_data = {}
        if self._seed_file:
            try:
                seed_data = json.loads(self._seed_file.read_text())
            except (FileNotFoundError, json.JSONDecodeError):
                pass
        cache_data = {}
        try:
            cache_data = json.loads(self._cache_file.read_text())
        except (FileNotFoundError, json.JSONDecodeError):
            pass
        self._data = {**seed_data, **cache_data}
        return self._data

    def _save(self):
        self._cache_file.parent.mkdir(parents=True, exist_ok=True)
        self._cache_file.write_text(json.dumps(self._data, indent=2))
        if self._seed_file:
            try:
                self._seed_file.parent.mkdir(parents=True, exist_ok=True)
                self._seed_file.write_text(json.dumps(self._data, indent=2))
            except OSError:
                pass

    def get(self, advertiser_id: str) -> Optional[Dict]:
        """Get discovery entry for an account (returns a copy)."""
        with self._lock:
            entry = self._load().get(advertiser_id)
            return dict(entry) if entry else None

    def get_all_gmvmax(self) -> Dict[str, Dict]:
        """Return all GMVMAX account entries (active + banned)."""
        with self._lock:
            cache = self._load()
            return {k: v for k, v in cache.items() if v.get("ad_type") == "gmvmax"}

    def get_active_for_store(self, store_id: str) -> List[str]:
        """Return active (non-banned) advertiser_ids linked to a store."""
        with self._lock:
            cache = self._load()
            return [
                adv_id
                for adv_id, entry in cache.items()
                if entry.get("ad_type") == "gmvmax"
                and not entry.get("banned")
                and store_id in entry.get("store_ids", [])
            ]

    def put(
        self,
        advertiser_id: str,
        *,
        store_ids: List[str],
        ad_type: str,
        ad_name: str = "",
    ):
        """Add or update a discovered account.

        For GMVMAX accounts, store_ids accumulate as a UNION across
        rediscoveries rather than overwriting. An advertiser that drove store A
        early in the month then switched to store B keeps BOTH bindings, so
        month-to-date attribution for store A is not orphaned when the binding
        flips. (2026-05-25 NAD+ gap: advertiser 7632253804267962385 moved
        hiileathy_life→flynew_us and $6,159 of NAD+ spend vanished from MTD.)
        Empty or 'unknown' writes keep overwrite semantics.
        """
        today = date.today().isoformat()
        with self._lock:
            cache = self._load()
            existing = cache.get(advertiser_id, {})
            if ad_type == "gmvmax" and store_ids:
                new_store_ids = _union_store_ids(
                    existing.get("store_ids", []), store_ids
                )
            else:
                new_store_ids = store_ids
            # Spread existing first so extra fields (banned_at, api_status,
            # status_checked_at, …) survive rediscovery instead of being
            # silently wiped by the fixed-key rebuild.
            entry = {
                **existing,
                "store_ids": new_store_ids,
                "ad_type": ad_type,
                "ad_name": ad_name or existing.get("ad_name", ""),
                "discovered_at": existing.get("discovered_at", today),
                "last_seen": today,
                "banned": existing.get("banned", False),
            }
            if ad_type == "gmvmax" and existing.get("ad_type") not in (None, "", "gmvmax"):
                # ad_type flipped back to gmvmax (e.g. natural Phase 1
                # rediscovery of a data-level archived entry) — drop the
                # stale archive markers so the entry doesn't read as
                # "resurrected but still archived".
                entry.pop("archived_at", None)
                entry.pop("archive_reason", None)
            cache[advertiser_id] = entry
            self._save()

    def mark_banned(self, advertiser_id: str):
        """Mark a discovered account as banned (records banned_at date)."""
        today = date.today().isoformat()
        with self._lock:
            cache = self._load()
            if advertiser_id in cache:
                cache[advertiser_id]["banned"] = True
                if not cache[advertiser_id].get("banned_at"):
                    cache[advertiser_id]["banned_at"] = today
                self._save()

    # ── Resurrect watch (archived / retired account reuse detection) ──

    def get_resurrect_candidates(self) -> Dict[str, Dict]:
        """Return entries that may be silently reused by ops.

        Two pools (2026-07-08 incident root-fix):
        - data-level archived entries (ad_type == "archived_gmvmax")
        - retired entries (ad_type == "gmvmax" + banned=True, written by
          lark-bot scripts/retire_account.py via mark_banned)

        Returns copies keyed by advertiser_id.
        """
        with self._lock:
            cache = self._load()
            return {
                adv_id: dict(entry)
                for adv_id, entry in cache.items()
                if isinstance(entry, dict)
                and (
                    entry.get("ad_type") == "archived_gmvmax"
                    or (entry.get("ad_type") == "gmvmax" and entry.get("banned"))
                )
            }

    def record_status_check(self, advertiser_id: str, api_status: str):
        """Persist the advertiser/info status classification for an entry.

        api_status drives the resurrect watch's terminal short-circuit:
        API-confirmed ban states never get re-classified or probed again
        (banned accounts do not come back — user-confirmed business rule).
        """
        today = date.today().isoformat()
        with self._lock:
            cache = self._load()
            if advertiser_id in cache:
                cache[advertiser_id]["api_status"] = api_status
                cache[advertiser_id]["status_checked_at"] = today
                self._save()

    def resurrect(self, advertiser_id: str, store_ids: List[str], ad_name: str = ""):
        """Bring an archived/retired entry back into active discovery.

        Unlike put(), this bumps discovered_at to today so the account
        lands in active_roster's 7-day FETCH_GRACE window and daily cost
        fetching resumes immediately (no manual activity-cache seeding).
        """
        today = date.today().isoformat()
        with self._lock:
            cache = self._load()
            existing = cache.get(advertiser_id, {})
            entry = {
                **existing,
                "store_ids": _union_store_ids(
                    existing.get("store_ids", []), store_ids
                ),
                "ad_type": "gmvmax",
                "ad_name": ad_name or existing.get("ad_name", ""),
                "discovered_at": today,
                "last_seen": today,
                "banned": False,
            }
            for stale_key in ("archived_at", "archive_reason", "banned_at"):
                entry.pop(stale_key, None)
            cache[advertiser_id] = entry
            self._save()

    def get_stale_unknowns(self, max_days: int = 14) -> Set[str]:
        """Return 'unknown' accounts not re-checked in max_days.

        These need periodic re-validation: an account seeded as 'unknown'
        may have become GMVMAX since (e.g., bound to a store after seed).
        """
        from datetime import timedelta

        cutoff = (date.today() - timedelta(days=max_days)).isoformat()
        with self._lock:
            cache = self._load()
            return {
                adv_id
                for adv_id, entry in cache.items()
                if entry.get("ad_type") == "unknown"
                and not entry.get("banned")
                and entry.get("last_seen", "") < cutoff
            }

    def mark_seen(self, advertiser_id: str):
        """Update last_seen timestamp for an account."""
        today = date.today().isoformat()
        with self._lock:
            cache = self._load()
            if advertiser_id in cache:
                cache[advertiser_id]["last_seen"] = today
                self._save()

    def get_unknown_ids(self, all_ids: Set[str]) -> Set[str]:
        """Return IDs from all_ids that are not yet in the cache."""
        with self._lock:
            cache = self._load()
            return all_ids - set(cache.keys())

    def seed_from_product_groups(self, groups: dict):
        """Bootstrap cache from existing PRODUCT_GROUPS config.

        Imports all known GMVMAX accounts so the cache starts warm.
        Does not overwrite existing entries (preserves discovered_at).
        """
        today = date.today().isoformat()
        with self._lock:
            cache = self._load()
            for grp in groups.values():
                for acct in grp.get("gmvmax_accounts", []):
                    adv_id = acct.get("advertiser_id", "")
                    if not adv_id or adv_id in cache:
                        continue
                    cache[adv_id] = {
                        "store_ids": [acct["store_id"]] if acct.get("store_id") else [],
                        "ad_type": "gmvmax",
                        "ad_name": acct.get("ad_name", ""),
                        "discovered_at": today,
                        "last_seen": today,
                        "banned": acct.get("banned", False),
                    }
            self._save()

    def prune_stale_banned(self, max_days: int = 60) -> int:
        """Remove accounts banned for more than max_days.

        These accounts won't be reused — their data is stale.
        Uses banned_at if available, falls back to last_seen.
        """
        from datetime import timedelta

        cutoff = (date.today() - timedelta(days=max_days)).isoformat()
        with self._lock:
            cache = self._load()
            to_remove = []
            for adv_id, entry in cache.items():
                if not entry.get("banned"):
                    continue
                ban_date = entry.get("banned_at") or entry.get("last_seen", "")
                if ban_date and ban_date < cutoff:
                    to_remove.append(adv_id)
            for adv_id in to_remove:
                del cache[adv_id]
            if to_remove:
                self._save()
            return len(to_remove)

    def clear(self):
        """Clear all cached data."""
        with self._lock:
            self._data = {}
            if self._cache_file.exists():
                self._cache_file.unlink()
