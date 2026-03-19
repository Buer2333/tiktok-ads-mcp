"""Advertiser timezone cache with batch warmup.

Extracted from gmvmax_report_aligned.py for reuse across aligned report tools.
"""

import json
import logging
from typing import Dict, List
from zoneinfo import ZoneInfo

from .client import TikTokAdsClient
from .timezone import parse_tz

logger = logging.getLogger(__name__)

# Module-level cache: advertiser_id -> ZoneInfo
_tz_cache: Dict[str, ZoneInfo] = {}


async def get_ad_tz(client: TikTokAdsClient, advertiser_id: str) -> ZoneInfo:
    """Get advertiser timezone via API, with in-memory cache."""
    if advertiser_id in _tz_cache:
        return _tz_cache[advertiser_id]

    params = {
        "advertiser_ids": json.dumps([advertiser_id]),
        "fields": json.dumps(["timezone"]),
    }
    response = await client._make_request("GET", "advertiser/info/", params)
    tz = ZoneInfo("UTC")
    if response.get("code") == 0:
        adv_list = response.get("data", {}).get("list", [])
        if adv_list:
            tz_str = adv_list[0].get("timezone", "UTC")
            tz = parse_tz(tz_str)
            logger.info(f"Ad tz for {advertiser_id}: {tz_str}")

    _tz_cache[advertiser_id] = tz
    return tz


async def warmup_tz_cache(client: TikTokAdsClient, advertiser_ids: List[str]) -> None:
    """Batch-fetch timezones for multiple advertiser IDs.

    Populates _tz_cache so subsequent get_ad_tz() calls return instantly.
    TikTok API supports up to 100 IDs per batch request.
    """
    uncached = [aid for aid in advertiser_ids if aid not in _tz_cache]
    if not uncached:
        return

    for i in range(0, len(uncached), 100):
        batch = uncached[i : i + 100]
        try:
            params = {
                "advertiser_ids": json.dumps(batch),
                "fields": json.dumps(["advertiser_id", "timezone"]),
            }
            response = await client._make_request("GET", "advertiser/info/", params)
            if response.get("code") == 0:
                adv_list = response.get("data", {}).get("list", [])
                for adv in adv_list:
                    aid = str(adv.get("advertiser_id", ""))
                    tz_str = adv.get("timezone", "UTC")
                    if aid:
                        _tz_cache[aid] = parse_tz(tz_str)
                        logger.info(f"Ad tz for {aid}: {tz_str}")
        except Exception as e:
            logger.warning(f"Batch tz warmup failed: {e}")

    # Fill missing with UTC fallback
    for aid in uncached:
        if aid not in _tz_cache:
            _tz_cache[aid] = ZoneInfo("UTC")
