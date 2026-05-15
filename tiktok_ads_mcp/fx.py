"""FX conversion to USD for ad-account-native currencies.

TikTok ad report metrics (`cost`, `gross_revenue`, `net_cost`, `spend`,
`total_onsite_shopping_value`) are returned in the advertiser's native
currency. Operations dashboards expect USD. This module converts a single
amount on a given date using ECB midpoint rates served by Frankfurter.

Design:
- `to_usd(amount, currency, date)` is the only public conversion entry point.
- USD is a fast-path (no HTTP, rate=1.0).
- Historical dates are immutable: once cached, never refetched.
- "today" is treated as "fetch on first ask, then cache for the day".
- Disk cache lives at `~/.cache/lark-bot/fx_rates.json`; survives restarts and
  is shared by every report tool inside the same VPS process tree.
- API failure falls back to a small built-in table (THB/MXN) plus a logged
  warning rather than crashing the report job.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Optional

import httpx

logger = logging.getLogger(__name__)

# Frankfurter publishes ECB midpoint rates daily (weekdays). Weekends / holidays
# return the last published value, which is the conventional industry behaviour.
_FRANKFURTER_BASE = "https://api.frankfurter.dev/v1"

# Fallback table — only used when Frankfurter is unreachable AND no cached rate
# exists. Values are intentionally conservative; keep small so an obvious wrong
# answer surfaces in dashboards rather than silently drifting.
_FALLBACK_RATES_TO_USD: Dict[str, float] = {
    "USD": 1.0,
    "THB": 0.030,  # ~33 THB / USD
    "MXN": 0.050,  # ~20 MXN / USD
    "EUR": 1.08,
    "GBP": 1.27,
    "PHP": 0.018,
    "MYR": 0.22,
}

_DEFAULT_CACHE_PATH = (
    Path(os.environ.get("LARK_BOT_CACHE_DIR") or Path.home() / ".cache" / "lark-bot")
    / "fx_rates.json"
)


def _cache_path() -> Path:
    # Re-evaluate each call so tests can override LARK_BOT_CACHE_DIR per-test.
    return (
        Path(
            os.environ.get("LARK_BOT_CACHE_DIR") or Path.home() / ".cache" / "lark-bot"
        )
        / "fx_rates.json"
    )


# In-memory mirror of disk cache: {date: {currency: rate_to_usd}}.
_mem_cache: Dict[str, Dict[str, float]] = {}
_cache_loaded = False
_io_lock = asyncio.Lock()


def _load_disk() -> None:
    global _cache_loaded
    if _cache_loaded:
        return
    path = _cache_path()
    if path.exists():
        try:
            data = json.loads(path.read_text())
            if isinstance(data, dict):
                _mem_cache.update(
                    {
                        d: {c: float(r) for c, r in rates.items()}
                        for d, rates in data.items()
                        if isinstance(rates, dict)
                    }
                )
        except (json.JSONDecodeError, ValueError, OSError) as e:
            logger.warning(f"fx_rates.json read failed, starting empty: {e}")
    _cache_loaded = True


def _save_disk() -> None:
    path = _cache_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    try:
        path.write_text(json.dumps(_mem_cache, indent=2, sort_keys=True))
    except OSError as e:
        logger.warning(f"fx_rates.json write failed (continuing in-memory only): {e}")


def _today_str() -> str:
    return datetime.now(timezone.utc).date().isoformat()


async def _fetch_from_frankfurter(date_str: str, currency: str) -> Optional[float]:
    """Return rate_to_usd for (date, currency), or None on failure.

    Frankfurter accepts `latest` or `YYYY-MM-DD`; weekend / holiday dates roll
    forward to the last business day server-side. We pass the literal date.
    """
    endpoint = f"{_FRANKFURTER_BASE}/{date_str}"
    params = {"base": currency, "symbols": "USD"}
    try:
        async with httpx.AsyncClient(timeout=10.0) as c:
            r = await c.get(endpoint, params=params)
            r.raise_for_status()
            payload = r.json()
            rate = (payload.get("rates") or {}).get("USD")
            if rate is None:
                logger.warning(
                    f"Frankfurter returned no USD rate for {currency} on {date_str}: {payload}"
                )
                return None
            return float(rate)
    except (httpx.HTTPError, ValueError) as e:
        logger.warning(f"Frankfurter fetch failed for {currency}@{date_str}: {e}")
        return None


async def get_rate_to_usd(currency: str, date_str: str) -> float:
    """Return rate to multiply (amount in `currency`) → USD for `date_str`.

    USD is a no-op fast path. All other currencies route through cache → API →
    fallback table. The function never raises; on total failure it logs and
    returns the built-in fallback (or 1.0 if the currency is unknown, so
    downstream sums stay in the same order of magnitude as before).
    """
    ccy = (currency or "USD").upper()
    if ccy == "USD":
        return 1.0

    _load_disk()
    today = _today_str()
    is_today = date_str == today

    # Cache hit — historical dates are immutable.
    cached = (_mem_cache.get(date_str) or {}).get(ccy)
    if cached is not None and not is_today:
        return cached
    if cached is not None and is_today:
        # Refresh once per day. The cache entry for today is keyed by date, so
        # the second-call cost is one membership check + return.
        return cached

    async with _io_lock:
        # Re-check after acquiring lock (another coroutine may have just written).
        cached = (_mem_cache.get(date_str) or {}).get(ccy)
        if cached is not None:
            return cached

        rate = await _fetch_from_frankfurter(date_str, ccy)
        if rate is None:
            rate = _FALLBACK_RATES_TO_USD.get(ccy)
            if rate is None:
                logger.error(
                    f"No FX rate for {ccy}@{date_str} (no API, no fallback) — "
                    f"returning 1.0 which means {ccy} amounts will look ~"
                    f"{1.0:.0f}x too large until you add a fallback entry."
                )
                return 1.0
            logger.warning(
                f"Using fallback FX rate {ccy}→USD={rate} for {date_str} (API failed)"
            )

        _mem_cache.setdefault(date_str, {})[ccy] = rate
        _save_disk()
        return rate


async def to_usd(amount: float, currency: str, date_str: str) -> float:
    """Convert `amount` in `currency` on `date_str` to USD.

    `currency` is case-insensitive. Empty string / None falls through as USD.
    """
    if not amount:
        return 0.0
    rate = await get_rate_to_usd(currency, date_str)
    return float(amount) * rate


def _reset_cache_for_test() -> None:
    """Clear in-memory cache. Tests only."""
    global _cache_loaded
    _mem_cache.clear()
    _cache_loaded = False
