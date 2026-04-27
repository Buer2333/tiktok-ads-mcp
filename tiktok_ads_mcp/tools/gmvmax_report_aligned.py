"""GMVMAX Report with timezone alignment.

Fetches hourly GMVMAX data and re-aggregates it to match a shop-timezone day,
regardless of the ad account's native timezone setting.
"""

import json
import logging
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from mcp_retry import api_retry

from ..client import TikTokAdsClient, TikTokRateLimitError
from ..timezone import day_utc_range, hour_to_utc, native_dates_for_day, parse_tz
from ..tz_cache import get_ad_tz as _get_ad_tz

logger = logging.getLogger(__name__)

ALIGNED_DEFAULT_METRICS = ["cost", "gross_revenue", "orders"]


async def _fetch_hourly(
    client: TikTokAdsClient,
    advertiser_id: str,
    date_str: str,
    store_ids: List[str],
    metrics: List[str],
) -> List[Dict]:
    """Fetch one day of hourly GMVMAX data."""
    params = {
        "advertiser_id": advertiser_id,
        "start_date": date_str,
        "end_date": date_str,
        "dimensions": json.dumps(["advertiser_id", "stat_time_hour"]),
        "metrics": json.dumps(metrics),
        "store_ids": json.dumps(store_ids),
        "page": 1,
        "page_size": 1000,
    }
    response = await client._make_request("GET", "gmv_max/report/get/", params)
    if response.get("code") == 0:
        return response.get("data", {}).get("list", [])
    # Non-0 code: surface so caller (and @api_retry) can react instead of
    # silently treating as "no data". 2026-04-21 bug: swallowed non-0 responses
    # caused 4/5 groups to under-report today cost by $50-$1610 simultaneously.
    raise Exception(
        f"gmv_max/report/get/ returned code={response.get('code')} "
        f"msg={response.get('message')!r} for advertiser={advertiser_id} date={date_str}"
    )


@api_retry(
    max_attempts=3,
    min_wait=3,
    max_wait=15,
    retryable_exceptions=(TikTokRateLimitError,),
)
async def get_gmvmax_report_aligned(
    client: TikTokAdsClient,
    advertiser_id: str,
    date: str,
    store_ids: List[str],
    shop_tz: str = "America/Los_Angeles",
    metrics: Optional[List[str]] = None,
) -> Dict[str, Any]:
    """Get GMVMAX report aligned to shop timezone.

    Fetches hourly data in ad-account native timezone, converts each hour to UTC,
    then filters to only include hours that fall within the shop-timezone day.

    Args:
        client: TikTok API client
        advertiser_id: TikTok advertiser ID
        date: Date string (YYYY-MM-DD) in shop timezone
        store_ids: TikTok Shop store IDs
        shop_tz: Shop timezone (IANA name, default America/Los_Angeles)
        metrics: Metrics to aggregate (default: cost, gross_revenue, orders)

    Returns:
        Dict with date, timezones, aggregated metrics, ROI, and hours included.
    """
    if metrics is None:
        metrics = list(ALIGNED_DEFAULT_METRICS)

    shop_zone = parse_tz(shop_tz)
    ad_zone = await _get_ad_tz(client, advertiser_id)

    start_utc, end_utc = day_utc_range(date, shop_zone)
    now_utc = datetime.now(timezone.utc)

    # Determine which native dates to query
    dates_to_query = native_dates_for_day(date, shop_zone, ad_zone)

    # Fetch hourly data for each native date
    all_rows: List[Dict] = []
    for d in dates_to_query:
        rows = await _fetch_hourly(client, advertiser_id, d, store_ids, metrics)
        all_rows.extend(rows)

    # Filter and aggregate
    aggregated: Dict[str, float] = {m: 0.0 for m in metrics}
    hours_included = 0

    for row in all_rows:
        dims = row.get("dimensions", {})
        hour_str = dims.get("stat_time_hour", "")
        if not hour_str:
            continue

        utc_dt = hour_to_utc(hour_str, ad_zone)

        # Must be within shop-day UTC range and not in the future
        if start_utc <= utc_dt < end_utc and utc_dt <= now_utc:
            row_metrics = row.get("metrics", {})
            for m in metrics:
                val = row_metrics.get(m, "0")
                try:
                    aggregated[m] += float(val)
                except (ValueError, TypeError):
                    pass
            hours_included += 1

    # Calculate ROI
    cost = aggregated.get("cost", 0.0)
    gmv = aggregated.get("gross_revenue", 0.0)
    roi = round(gmv / cost, 2) if cost > 0 else 0.0

    # Round monetary values
    for m in aggregated:
        aggregated[m] = round(aggregated[m], 2)

    return {
        "date": date,
        "shop_tz": shop_tz,
        "ad_tz": str(ad_zone),
        "metrics": aggregated,
        "roi": roi,
        "hours_included": hours_included,
    }


async def _fetch_hourly_breakdown(
    client: TikTokAdsClient,
    advertiser_id: str,
    date_str: str,
    store_ids: List[str],
    metrics: List[str],
) -> List[Dict]:
    """Fetch one day of hourly GMVMAX data with per-store breakdown.

    Same as _fetch_hourly but adds store_id to dimensions so callers can
    attribute spend to product groups via STORE_PRODUCT_GROUP without
    relying on bitable's per-row (advertiser, store) binding being correct.
    """
    params = {
        "advertiser_id": advertiser_id,
        "start_date": date_str,
        "end_date": date_str,
        "dimensions": json.dumps(["store_id", "stat_time_hour"]),
        "metrics": json.dumps(metrics),
        "store_ids": json.dumps(store_ids),
        "page": 1,
        "page_size": 1000,
    }
    response = await client._make_request("GET", "gmv_max/report/get/", params)
    if response.get("code") == 0:
        return response.get("data", {}).get("list", [])
    raise Exception(
        f"gmv_max/report/get/ returned code={response.get('code')} "
        f"msg={response.get('message')!r} for advertiser={advertiser_id} date={date_str}"
    )


@api_retry(
    max_attempts=3,
    min_wait=3,
    max_wait=15,
    retryable_exceptions=(TikTokRateLimitError,),
)
async def get_gmvmax_report_aligned_breakdown(
    client: TikTokAdsClient,
    advertiser_id: str,
    date: str,
    store_ids: List[str],
    shop_tz: str = "America/Los_Angeles",
    metrics: Optional[List[str]] = None,
) -> Dict[str, Any]:
    """Per-store GMVMAX breakdown aligned to shop timezone.

    Returns per-store metrics so callers route each store's spend to the
    correct product group via STORE_PRODUCT_GROUP — independent of any
    operator-maintained bitable (advertiser, store) binding.

    Returns:
        Dict with date, timezones, breakdown {store_id: {cost, gross_revenue,
        orders, roi}}, and hours included.
    """
    if metrics is None:
        metrics = list(ALIGNED_DEFAULT_METRICS)

    shop_zone = parse_tz(shop_tz)
    ad_zone = await _get_ad_tz(client, advertiser_id)

    start_utc, end_utc = day_utc_range(date, shop_zone)
    now_utc = datetime.now(timezone.utc)

    dates_to_query = native_dates_for_day(date, shop_zone, ad_zone)

    all_rows: List[Dict] = []
    for d in dates_to_query:
        rows = await _fetch_hourly_breakdown(
            client, advertiser_id, d, store_ids, metrics
        )
        all_rows.extend(rows)

    # Aggregate per-store
    by_store: Dict[str, Dict[str, float]] = {}
    hours_seen: Dict[str, int] = {}

    for row in all_rows:
        dims = row.get("dimensions", {})
        hour_str = dims.get("stat_time_hour", "")
        store_id = str(dims.get("store_id", ""))
        if not hour_str or not store_id:
            continue

        utc_dt = hour_to_utc(hour_str, ad_zone)
        if not (start_utc <= utc_dt < end_utc and utc_dt <= now_utc):
            continue

        bucket = by_store.setdefault(store_id, {m: 0.0 for m in metrics})
        row_metrics = row.get("metrics", {})
        for m in metrics:
            try:
                bucket[m] += float(row_metrics.get(m, "0"))
            except (ValueError, TypeError):
                pass
        hours_seen[store_id] = hours_seen.get(store_id, 0) + 1

    breakdown: Dict[str, Dict[str, Any]] = {}
    for store_id, bucket in by_store.items():
        cost = bucket.get("cost", 0.0)
        gmv = bucket.get("gross_revenue", 0.0)
        breakdown[store_id] = {
            "cost": round(cost, 2),
            "gross_revenue": round(gmv, 2),
            "orders": int(bucket.get("orders", 0)),
            "roi": round(gmv / cost, 2) if cost > 0 else 0.0,
            "hours_included": hours_seen.get(store_id, 0),
        }

    return {
        "date": date,
        "shop_tz": shop_tz,
        "ad_tz": str(ad_zone),
        "breakdown": breakdown,
    }
