"""Advertiser currency cache with batch warmup.

Mirrors tz_cache.py — same TikTok advertiser/info endpoint, same caching
pattern. Kept separate so the two concerns are independently testable.
"""

import json
import logging
from typing import Dict, List

from .client import TikTokAdsClient

logger = logging.getLogger(__name__)

# Module-level cache: advertiser_id -> currency code (e.g. "USD", "THB").
_currency_cache: Dict[str, str] = {}


async def get_currency(client: TikTokAdsClient, advertiser_id: str) -> str:
    """Return advertiser native currency, defaulting to USD on lookup failure.

    The default-USD fallback matches the historical behaviour of the report
    tools (which silently assumed USD pre-FX-aware), so a misbehaving lookup
    cannot make things worse than the pre-FX-aware baseline.
    """
    if advertiser_id in _currency_cache:
        return _currency_cache[advertiser_id]

    params = {
        "advertiser_ids": json.dumps([advertiser_id]),
        "fields": json.dumps(["currency"]),
    }
    currency = "USD"
    try:
        response = await client._make_request("GET", "advertiser/info/", params)
        if response.get("code") == 0:
            adv_list = response.get("data", {}).get("list", [])
            if adv_list:
                currency = (adv_list[0].get("currency") or "USD").upper()
                logger.info(f"Ad currency for {advertiser_id}: {currency}")
    except Exception as e:
        logger.warning(f"currency lookup failed for {advertiser_id}, assuming USD: {e}")

    _currency_cache[advertiser_id] = currency
    return currency


async def warmup_currency_cache(
    client: TikTokAdsClient, advertiser_ids: List[str]
) -> None:
    """Batch-fetch currencies for multiple advertiser IDs (up to 100/batch)."""
    uncached = [aid for aid in advertiser_ids if aid not in _currency_cache]
    if not uncached:
        return

    for i in range(0, len(uncached), 100):
        batch = uncached[i : i + 100]
        try:
            params = {
                "advertiser_ids": json.dumps(batch),
                "fields": json.dumps(["advertiser_id", "currency"]),
            }
            response = await client._make_request("GET", "advertiser/info/", params)
            if response.get("code") == 0:
                adv_list = response.get("data", {}).get("list", [])
                for adv in adv_list:
                    aid = str(adv.get("advertiser_id", ""))
                    ccy = (adv.get("currency") or "USD").upper()
                    if aid:
                        _currency_cache[aid] = ccy
        except Exception as e:
            logger.warning(f"Batch currency warmup failed: {e}")

    # Fill misses with USD so subsequent lookups don't retry the API.
    for aid in uncached:
        _currency_cache.setdefault(aid, "USD")
