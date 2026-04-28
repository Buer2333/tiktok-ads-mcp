"""Ads Report with timezone alignment.

Fetches hourly Ads (manual bid) data and re-aggregates to match a shop-timezone day,
regardless of the ad account's native timezone setting.
"""

import json
import logging
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from mcp_retry import api_retry

from ..client import (
    TikTokAdsClient,
    TikTokIncompleteDataError,
    TikTokRateLimitError,
)
from ..timezone import day_utc_range, hour_to_utc, native_dates_for_day, parse_tz
from ..tz_cache import get_ad_tz as _get_ad_tz

logger = logging.getLogger(__name__)

ALIGNED_ADS_METRICS = ["spend", "onsite_shopping", "total_onsite_shopping_value"]

# Match gmvmax_report_aligned: tolerate up to 2h hourly-report lag.
_HOURS_LAG_TOLERANCE = 2


def _expected_hours(date_str: str, shop_zone, now_utc) -> int:
    start_utc, end_utc = day_utc_range(date_str, shop_zone)
    if now_utc >= end_utc:
        return 24
    if now_utc <= start_utc:
        return 0
    return int((now_utc - start_utc).total_seconds() // 3600)


async def _fetch_ads_hourly(
    client: TikTokAdsClient,
    advertiser_id: str,
    date_str: str,
    metrics: List[str],
) -> List[Dict]:
    """Fetch one day of hourly Ads data via report/integrated/get/."""
    params = {
        "advertiser_id": advertiser_id,
        "report_type": "BASIC",
        "data_level": "AUCTION_ADVERTISER",
        "dimensions": json.dumps(["advertiser_id", "stat_time_hour"]),
        "metrics": json.dumps(metrics),
        "start_date": date_str,
        "end_date": date_str,
        "service_type": "AUCTION",
        "page": 1,
        "page_size": 1000,
    }
    response = await client._make_request("GET", "report/integrated/get/", params)
    if response.get("code") == 0:
        return response.get("data", {}).get("list", [])
    # Non-0 code: surface so caller (and @api_retry) can react. Previously
    # silently returned [], which masked rate-limit / auth failures and let
    # partial data flow into cache (2026-04-28 incident).
    raise Exception(
        f"report/integrated/get/ returned code={response.get('code')} "
        f"msg={response.get('message')!r} for advertiser={advertiser_id} date={date_str}"
    )


@api_retry(
    max_attempts=3,
    min_wait=3,
    max_wait=15,
    retryable_exceptions=(TikTokRateLimitError, TikTokIncompleteDataError),
)
async def get_ads_report_aligned(
    client: TikTokAdsClient,
    advertiser_id: str,
    date: str,
    shop_tz: str = "America/Los_Angeles",
    metrics: Optional[List[str]] = None,
) -> Dict[str, Any]:
    """Get Ads report aligned to shop timezone.

    Fetches hourly data in ad-account native timezone, converts each hour to UTC,
    then filters to only include hours that fall within the shop-timezone day.

    Args:
        client: TikTok API client
        advertiser_id: TikTok advertiser ID
        date: Date string (YYYY-MM-DD) in shop timezone
        shop_tz: Shop timezone (IANA name, default America/Los_Angeles)
        metrics: Metrics to aggregate (default: spend, onsite_shopping, total_onsite_shopping_value)

    Returns:
        Dict with date, timezones, aggregated metrics (cost, gmv, orders), and roas.
    """
    if metrics is None:
        metrics = list(ALIGNED_ADS_METRICS)

    shop_zone = parse_tz(shop_tz)
    ad_zone = await _get_ad_tz(client, advertiser_id)

    start_utc, end_utc = day_utc_range(date, shop_zone)
    now_utc = datetime.now(timezone.utc)

    dates_to_query = native_dates_for_day(date, shop_zone, ad_zone)

    all_rows: List[Dict] = []
    for d in dates_to_query:
        rows = await _fetch_ads_hourly(client, advertiser_id, d, metrics)
        all_rows.extend(rows)

    # Aggregate
    cost = 0.0
    gmv = 0.0
    orders = 0
    hours_included = 0

    for row in all_rows:
        dims = row.get("dimensions", {})
        hour_str = dims.get("stat_time_hour", "")
        if not hour_str:
            continue

        utc_dt = hour_to_utc(hour_str, ad_zone)

        if start_utc <= utc_dt < end_utc and utc_dt <= now_utc:
            row_metrics = row.get("metrics", {})
            cost += float(row_metrics.get("spend", 0))
            gmv += float(row_metrics.get("total_onsite_shopping_value", 0))
            orders += int(row_metrics.get("onsite_shopping", 0))
            hours_included += 1

    # Completeness check: see gmvmax_report_aligned for the full incident
    # explanation (TikTok degrades to code=0 + partial-hours under token
    # rate-limit pressure). Trigger retry instead of silently aggregating.
    expected = _expected_hours(date, shop_zone, now_utc)
    threshold = max(1, expected - _HOURS_LAG_TOLERANCE)
    # See gmvmax_report_aligned: 0 means inactive (acceptable), 1..threshold-1
    # means partial (retry).
    if expected > 0 and 0 < hours_included < threshold:
        raise TikTokIncompleteDataError(
            f"Ads advertiser={advertiser_id} date={date}: "
            f"hours_included={hours_included} < threshold={threshold} "
            f"(expected={expected}, tol={_HOURS_LAG_TOLERANCE}) — "
            f"likely token rate-limit partial response"
        )

    roas = round(gmv / cost, 2) if cost > 0 else 0.0

    return {
        "date": date,
        "shop_tz": shop_tz,
        "ad_tz": str(ad_zone),
        "metrics": {
            "cost": round(cost, 2),
            "gmv": round(gmv, 2),
            "orders": orders,
        },
        "roas": roas,
        "hours_included": hours_included,
    }
