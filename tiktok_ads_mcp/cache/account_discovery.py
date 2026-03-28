"""Cache for auto-discovered GMVMAX advertiser accounts.

Maps advertiser_id → {store_ids, ad_type, ad_name, discovered_at, last_seen, banned}.
Populated by scanning BC authorized accounts + gmv_max/store/list/ API.
Replaces manual bitable maintenance for GMVMAX account mapping.

Schema:
{
  "<advertiser_id>": {
    "store_ids": ["7495613592836409756"],
    "ad_type": "gmvmax",           // "gmvmax" | "unknown"
    "ad_name": "FLYL-CSW-...",
    "discovered_at": "2026-03-28",
    "last_seen": "2026-03-28",
    "banned": false
  }
}
"""

import json
import threading
from datetime import date
from pathlib import Path
from typing import Dict, List, Optional, Set


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
        """Add or update a discovered account."""
        today = date.today().isoformat()
        with self._lock:
            cache = self._load()
            existing = cache.get(advertiser_id, {})
            cache[advertiser_id] = {
                "store_ids": store_ids,
                "ad_type": ad_type,
                "ad_name": ad_name or existing.get("ad_name", ""),
                "discovered_at": existing.get("discovered_at", today),
                "last_seen": today,
                "banned": existing.get("banned", False),
            }
            self._save()

    def mark_banned(self, advertiser_id: str):
        """Mark a discovered account as banned."""
        with self._lock:
            cache = self._load()
            if advertiser_id in cache:
                cache[advertiser_id]["banned"] = True
                self._save()

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

    def clear(self):
        """Clear all cached data."""
        with self._lock:
            self._data = {}
            if self._cache_file.exists():
                self._cache_file.unlink()
