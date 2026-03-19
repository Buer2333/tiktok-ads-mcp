"""Cache for advertiser account ban status.

Authoritative source for "is this account banned" — takes priority over
bitable mapping and config fallback. Updated by ban_alert job before
ad_report runs.

Schema:
{
  "<advertiser_id>": {
    "banned": true,
    "status": "STATUS_LIMIT",
    "detected_at": "2026-03-16",
    "last_active_date": "2026-03-16",
    "ad_type": "gmvmax",
    "group_key": "Hiileathy-US-Shilajit",
    "shop_tz": "America/Los_Angeles",
    "ad_tz": "Asia/Shanghai"
  }
}
"""

import json
import threading
from pathlib import Path
from typing import Dict, Optional


class BanStatusCache:
    """Thread-safe file-based cache for advertiser ban status."""

    def __init__(self, cache_dir: Path, seed_file: Optional[Path] = None):
        self._cache_file = cache_dir / "ban_status.json"
        self._seed_file = seed_file
        self._lock = threading.Lock()
        self._data: Optional[Dict] = None

    def _load(self) -> Dict:
        if self._data is not None:
            return self._data
        # Load seed as baseline (committed from CI), then overlay local cache
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
        # Merge: seed provides baseline, cache overrides
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

    def is_banned(self, advertiser_id: str) -> bool:
        """Check if an account is banned (from cache)."""
        with self._lock:
            cache = self._load()
            entry = cache.get(advertiser_id)
            return bool(entry and entry.get("banned"))

    def get_status(self, advertiser_id: str) -> Optional[Dict]:
        """Return full status entry for an account, or None."""
        with self._lock:
            cache = self._load()
            return cache.get(advertiser_id)

    def set_banned(
        self,
        advertiser_id: str,
        *,
        status: str,
        detected_at: str,
        last_active_date: str = "",
        ad_type: str = "",
        group_key: str = "",
        shop_tz: str = "",
        ad_tz: str = "",
    ):
        """Mark an account as banned."""
        with self._lock:
            cache = self._load()
            existing = cache.get(advertiser_id, {})
            cache[advertiser_id] = {
                "banned": True,
                "status": status,
                "detected_at": existing.get("detected_at", detected_at),
                "last_active_date": last_active_date
                or existing.get("last_active_date", ""),
                "ad_type": ad_type or existing.get("ad_type", ""),
                "group_key": group_key or existing.get("group_key", ""),
                "shop_tz": shop_tz or existing.get("shop_tz", ""),
                "ad_tz": ad_tz or existing.get("ad_tz", ""),
            }
            self._save()

    def set_active(self, advertiser_id: str):
        """Mark an account as active (not banned). Removes from cache."""
        with self._lock:
            cache = self._load()
            if advertiser_id in cache:
                del cache[advertiser_id]
                self._save()

    def get_all_banned(self) -> Dict:
        """Return all banned account entries."""
        with self._lock:
            cache = self._load()
            return {k: v for k, v in cache.items() if v.get("banned")}

    def clear(self):
        """Clear all cached data."""
        with self._lock:
            self._data = {}
            if self._cache_file.exists():
                self._cache_file.unlink()
