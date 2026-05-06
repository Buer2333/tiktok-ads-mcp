"""File-based cache for per-day editor-scoped GMVMAX attribution.

Companion to AdCostCache. AdCostCache stores advertiser-level cost/gmv/orders;
EditorDataCache stores the editor-attribution breakdown (per-editor cost/gmv/
orders + organic + errors) computed by `fetch_gmvmax_data` for a (group, date,
team, banned) tuple.

Settled days (T-2 and earlier) are the read target — once attribution stabilizes
on TikTok's side, the breakdown is invariant. Hot days (T-1, T-0) are always
re-fetched.

Cache key:
  {group_key}:{date_str}:{team}:{banned_int}

  team is the lead's name (or empty for full-group queries); banned_int is
  0 or 1 reflecting include_banned semantics.

Cache value:
  {
    "editor_data": {editor_name: {cost: float, gmv: float, orders: int}, ...},
    "organic":     {cost: float, gmv: float},
    "errors":      [str, ...],   # error message list, may be empty
    "cached_at":   unix_timestamp,
  }

Identity assignments inside editor_data are stable forever once set
(material_report.py: identity-already-set-never-overwritten contract). This
makes daily entries safe to merge by simple per-editor cost/gmv/orders
addition; identity strings are never disputed across days.
"""

import json
import threading
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional


def _build_key(group_key: str, date_str: str, team: str, banned: bool) -> str:
    """Build cache key. team may be empty for full-group queries."""
    return f"{group_key}:{date_str}:{team}:{int(banned)}"


class EditorDataCache:
    """Thread-safe file-based cache for per-day editor-scoped breakdowns."""

    def __init__(
        self,
        cache_dir: Path,
        seed_file: Optional[Path] = None,
        max_age: int = 45 * 86400,
    ):
        self._cache_file = cache_dir / "editor_data.json"
        self._seed_file = seed_file
        self._max_age = max_age
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

    def put_daily(
        self,
        group_key: str,
        date_str: str,
        team: str,
        banned: bool,
        editor_data: Dict,
        organic: Dict,
        errors: List[str],
    ):
        """Cache one day's editor breakdown.

        editor_data: {editor_name: {cost, gmv, orders}}
        organic: {cost, gmv}
        errors: list of error message strings (may be empty)
        """
        with self._lock:
            cache = self._load()
            key = _build_key(group_key, date_str, team, banned)
            cache[key] = {
                "editor_data": editor_data,
                "organic": organic,
                "errors": errors,
                "cached_at": int(time.time()),
            }

            cutoff = int(time.time()) - self._max_age
            expired = [k for k, v in cache.items() if v.get("cached_at", 0) < cutoff]
            for k in expired:
                del cache[k]

            self._save()

    def get_daily(
        self,
        group_key: str,
        date_str: str,
        team: str,
        banned: bool,
    ) -> Optional[Dict]:
        """Return cached single-day breakdown or None."""
        with self._lock:
            cache = self._load()
            key = _build_key(group_key, date_str, team, banned)
            entry = cache.get(key)
            if not entry:
                return None
            return {
                "editor_data": entry.get("editor_data", {}),
                "organic": entry.get("organic", {"cost": 0.0, "gmv": 0.0}),
                "errors": entry.get("errors", []),
            }

    def get_range(
        self,
        group_key: str,
        start_date: str,
        end_date: str,
        team: str,
        banned: bool,
        allow_partial: bool = False,
    ) -> Optional[Dict]:
        """Merge cached daily breakdowns over a date range.

        Sums editor_data per editor and organic; concatenates errors.
        Returns None if any date missing AND allow_partial=False.
        With allow_partial=True returns whatever is cached.
        """
        start = datetime.strptime(start_date, "%Y-%m-%d")
        end = datetime.strptime(end_date, "%Y-%m-%d")

        with self._lock:
            cache = self._load()
            merged_editor: Dict[str, Dict[str, float]] = {}
            merged_organic = {"cost": 0.0, "gmv": 0.0}
            merged_errors: List[str] = []
            cached_days = 0
            total_days = 0

            current = start
            while current <= end:
                total_days += 1
                date_str = current.strftime("%Y-%m-%d")
                key = _build_key(group_key, date_str, team, banned)
                entry = cache.get(key)
                if entry is None:
                    if not allow_partial:
                        return None
                else:
                    for editor, stats in entry.get("editor_data", {}).items():
                        bucket = merged_editor.setdefault(
                            editor, {"cost": 0.0, "gmv": 0.0, "orders": 0}
                        )
                        bucket["cost"] += float(stats.get("cost", 0))
                        bucket["gmv"] += float(stats.get("gmv", 0))
                        bucket["orders"] += int(stats.get("orders", 0))
                    organic = entry.get("organic", {})
                    merged_organic["cost"] += float(organic.get("cost", 0))
                    merged_organic["gmv"] += float(organic.get("gmv", 0))
                    merged_errors.extend(entry.get("errors", []))
                    cached_days += 1
                current += timedelta(days=1)

            if cached_days == 0:
                return None

            result = {
                "editor_data": merged_editor,
                "organic": merged_organic,
                "errors": merged_errors,
            }
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
