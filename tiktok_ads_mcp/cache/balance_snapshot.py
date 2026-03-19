"""File-based cache for advertiser balance snapshots.

Stores the latest balance for each advertiser per date, enabling
cost estimation when API access is revoked (banned accounts):
  estimated_cost(date) = balance(date-1) - balance(date)

Cache key: {advertiser_id}:{date_str}
Cache value: {balance, group, ad_name, snapshot_at}
"""

import json
import threading
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, Optional


class BalanceSnapshotCache:
    """Thread-safe file-based cache for advertiser balance snapshots."""

    def __init__(
        self,
        cache_dir: Path,
        seed_file: Optional[Path] = None,
        max_age: int = 45 * 86400,
    ):
        self._cache_file = cache_dir / "balance_snapshot.json"
        self._seed_file = seed_file
        self._max_age = max_age
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

    def put(
        self,
        advertiser_id: str,
        date_str: str,
        balance: float,
        group: str = "",
        ad_name: str = "",
    ):
        """Store balance snapshot for an advertiser on a specific date."""
        with self._lock:
            cache = self._load()
            key = f"{advertiser_id}:{date_str}"
            cache[key] = {
                "balance": balance,
                "group": group,
                "ad_name": ad_name,
                "snapshot_at": int(time.time()),
            }

            cutoff = int(time.time()) - self._max_age
            expired = [k for k, v in cache.items() if v.get("snapshot_at", 0) < cutoff]
            for k in expired:
                del cache[k]

            self._save()

    def get(self, advertiser_id: str, date_str: str) -> Optional[Dict]:
        """Return cached {balance, group, ad_name} for one advertiser on one date."""
        with self._lock:
            cache = self._load()
            key = f"{advertiser_id}:{date_str}"
            entry = cache.get(key)
            if entry:
                return {
                    "balance": entry["balance"],
                    "group": entry.get("group", ""),
                    "ad_name": entry.get("ad_name", ""),
                }
            return None

    def estimate_cost(self, advertiser_id: str, date_str: str) -> Optional[float]:
        """Estimate daily cost from balance delta: balance(date-1) - balance(date).

        Returns None if either snapshot is missing.
        Note: does not account for top-ups, so may underestimate cost.
        """
        current = datetime.strptime(date_str, "%Y-%m-%d")
        prev = (current - timedelta(days=1)).strftime("%Y-%m-%d")

        with self._lock:
            cache = self._load()
            prev_entry = cache.get(f"{advertiser_id}:{prev}")
            curr_entry = cache.get(f"{advertiser_id}:{date_str}")
            if prev_entry and curr_entry:
                delta = prev_entry["balance"] - curr_entry["balance"]
                return max(delta, 0.0)
            return None

    def clear(self):
        """Clear all cached data."""
        with self._lock:
            self._data = {}
            if self._cache_file.exists():
                self._cache_file.unlink()
