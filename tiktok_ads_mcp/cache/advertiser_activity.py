"""File-based cache tracking each advertiser's most recent spend activity.

Backs the Active Roster Filter (F0): a `last_spend_date` is needed to decide
which advertisers can be downgraded from hourly polling to daily/weekly probes
without losing the ability to detect re-activation.

Why a dedicated cache (not reusing ad_cost.json):
  - ad_cost entries expire at 45 days, but weekly-probe accounts need
    last_spend_date memory that crosses the expire boundary.
  - Different semantics: ad_cost stores per-day cost values; this one stores
    "max date with cost > 0 ever observed", which is monotonic and small.

Schema (~/.cache/lark-bot/advertiser_activity.json):
{
  "<advertiser_id>:<store_id>:<ad_type>": {
    "last_spend_date": "2026-05-08",   # YYYY-MM-DD or "" if never seen w/ spend
    "last_probe_date": "2026-05-12",   # last time fetch_ad_cost actually fired
    "last_probe_cost": 0.0,            # cost from last probe (debug aid)
    "updated_at":      "2026-05-12T10:00:00"
  }
}

Key triple matches ad_cost._build_key contract (Ads → store_id is "").
"""

import json
import threading
from datetime import date, datetime
from pathlib import Path
from typing import Dict, Optional


def _build_key(advertiser_id: str, store_id: str, ad_type: str) -> str:
    """Match ad_cost._build_key triple: GMVMAX requires store_id; Ads leaves it ''."""
    ad_type_lower = ad_type.lower()
    if ad_type_lower == "gmvmax" and not store_id:
        raise ValueError(
            f"GMVMAX activity cache requires store_id (advertiser={advertiser_id}). "
            f"Silent store-less fallback would risk cross-store decay confusion."
        )
    return f"{advertiser_id}:{store_id}:{ad_type_lower}"


class AdvertiserActivityCache:
    """Thread-safe file-based cache of per-(adv, store, ad_type) spend recency."""

    def __init__(self, cache_dir: Path, seed_file: Optional[Path] = None):
        self._cache_file = cache_dir / "advertiser_activity.json"
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
        self._cache_file.write_text(json.dumps(self._data, indent=2, sort_keys=True))
        if self._seed_file:
            try:
                self._seed_file.parent.mkdir(parents=True, exist_ok=True)
                self._seed_file.write_text(
                    json.dumps(self._data, indent=2, sort_keys=True)
                )
            except OSError:
                pass

    def get(self, advertiser_id: str, store_id: str, ad_type: str) -> Optional[Dict]:
        """Return cached entry copy or None."""
        with self._lock:
            entry = self._load().get(_build_key(advertiser_id, store_id, ad_type))
            return dict(entry) if entry else None

    def record_probe(
        self,
        advertiser_id: str,
        store_id: str,
        ad_type: str,
        date_str: str,
        cost: float,
    ):
        """Record a probe result. Updates last_spend_date only when cost > 0
        AND date_str is monotonically newer than existing last_spend_date.

        Always updates last_probe_date / last_probe_cost / updated_at.
        """
        with self._lock:
            cache = self._load()
            key = _build_key(advertiser_id, store_id, ad_type)
            existing = cache.get(key, {})
            new_last_spend = existing.get("last_spend_date", "")
            if cost > 0 and date_str > new_last_spend:
                new_last_spend = date_str
            cache[key] = {
                "last_spend_date": new_last_spend,
                "last_probe_date": date_str,
                "last_probe_cost": float(cost),
                "updated_at": datetime.now().isoformat(timespec="seconds"),
            }
            self._save()

    def days_since_last_spend(
        self,
        advertiser_id: str,
        store_id: str,
        ad_type: str,
        today: str,
    ) -> Optional[int]:
        """Return integer days from last_spend_date to today, or None if no record."""
        with self._lock:
            entry = self._load().get(_build_key(advertiser_id, store_id, ad_type))
        if not entry:
            return None
        last_spend = entry.get("last_spend_date", "")
        if not last_spend:
            return None
        try:
            d_today = date.fromisoformat(today)
            d_last = date.fromisoformat(last_spend)
        except ValueError:
            return None
        return (d_today - d_last).days

    def seed_last_spend(
        self,
        advertiser_id: str,
        store_id: str,
        ad_type: str,
        last_spend_date: str,
    ):
        """Backfill last_spend_date without recording a probe (used by
        seed_activity_cache.py during cold-start migration)."""
        with self._lock:
            cache = self._load()
            key = _build_key(advertiser_id, store_id, ad_type)
            existing = cache.get(key, {})
            if last_spend_date > existing.get("last_spend_date", ""):
                cache[key] = {
                    "last_spend_date": last_spend_date,
                    "last_probe_date": existing.get("last_probe_date", ""),
                    "last_probe_cost": existing.get("last_probe_cost", 0.0),
                    "updated_at": datetime.now().isoformat(timespec="seconds"),
                }
                self._save()

    def all_keys(self):
        """Return iterable of all cache keys (CLI/diagnostics)."""
        with self._lock:
            return list(self._load().keys())

    def clear(self):
        """Clear all cached data."""
        with self._lock:
            self._data = {}
            if self._cache_file.exists():
                self._cache_file.unlink()
