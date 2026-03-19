"""File-based cache for daily ad account cost data.

Solves the banned-account problem: when accounts get banned mid-month,
TikTok revokes API access so historical spend can't be queried.
By caching each account's daily cost from yesterday/today fetches,
MTD can sum cached daily costs instead of relying on date-range queries
that fail for banned accounts.

Cache key: {advertiser_id}:{date_str}:{type}  (type = gmvmax or ads)
Cache value: {cost, gmv, orders, cached_at}
"""

import json
import threading
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, Optional


class AdCostCache:
    """Thread-safe file-based cache for daily ad account cost data."""

    def __init__(
        self,
        cache_dir: Path,
        seed_file: Optional[Path] = None,
        max_age: int = 45 * 86400,
    ):
        self._cache_file = cache_dir / "ad_cost.json"
        self._seed_file = seed_file
        self._max_age = max_age
        self._lock = threading.Lock()
        self._data: Optional[Dict] = None

    def _load(self) -> Dict:
        if self._data is not None:
            return self._data
        try:
            self._data = json.loads(self._cache_file.read_text())
        except (FileNotFoundError, json.JSONDecodeError):
            if self._seed_file:
                try:
                    self._data = json.loads(self._seed_file.read_text())
                except (FileNotFoundError, json.JSONDecodeError):
                    self._data = {}
            else:
                self._data = {}
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

    def put_daily(
        self,
        advertiser_id: str,
        date_str: str,
        ad_type: str,
        cost: float,
        gmv: float,
        orders: int,
    ):
        """Cache one account's cost for a specific date.

        Called after successfully fetching today/yesterday data.
        ad_type: 'gmvmax' or 'ads'
        """
        with self._lock:
            cache = self._load()
            key = f"{advertiser_id}:{date_str}:{ad_type}"
            cache[key] = {
                "cost": cost,
                "gmv": gmv,
                "orders": orders,
                "cached_at": int(time.time()),
            }

            cutoff = int(time.time()) - self._max_age
            expired = [k for k, v in cache.items() if v.get("cached_at", 0) < cutoff]
            for k in expired:
                del cache[k]

            self._save()

    def get_daily(
        self, advertiser_id: str, date_str: str, ad_type: str
    ) -> Optional[Dict]:
        """Return cached {cost, gmv, orders} for one account on one date."""
        with self._lock:
            cache = self._load()
            key = f"{advertiser_id}:{date_str}:{ad_type}"
            entry = cache.get(key)
            if entry:
                return {
                    "cost": entry["cost"],
                    "gmv": entry["gmv"],
                    "orders": entry["orders"],
                }
            return None

    def get_range(
        self,
        advertiser_id: str,
        start_date: str,
        end_date: str,
        ad_type: str,
        allow_partial: bool = False,
    ) -> Optional[Dict]:
        """Sum cached daily costs for a date range.

        Returns aggregated {cost, gmv, orders} if ALL dates in range have cache.
        Returns None if any date is missing and allow_partial=False.

        With allow_partial=True, returns whatever is cached (for banned accounts
        where API is inaccessible). Result includes 'cached_days' and 'total_days'.
        """
        start = datetime.strptime(start_date, "%Y-%m-%d")
        end = datetime.strptime(end_date, "%Y-%m-%d")

        with self._lock:
            cache = self._load()
            total_cost = 0.0
            total_gmv = 0.0
            total_orders = 0
            cached_days = 0
            total_days = 0

            current = start
            while current <= end:
                total_days += 1
                date_str = current.strftime("%Y-%m-%d")
                key = f"{advertiser_id}:{date_str}:{ad_type}"
                entry = cache.get(key)
                if entry is None:
                    if not allow_partial:
                        return None
                else:
                    total_cost += entry["cost"]
                    total_gmv += entry["gmv"]
                    total_orders += entry["orders"]
                    cached_days += 1
                current += timedelta(days=1)

            if cached_days == 0:
                return None

            result = {"cost": total_cost, "gmv": total_gmv, "orders": total_orders}
            if allow_partial:
                result["cached_days"] = cached_days
                result["total_days"] = total_days
            return result

    def clear(self):
        """Clear all cached data."""
        with self._lock:
            self._data = {}
            if self._cache_file.exists():
                self._cache_file.unlink()
