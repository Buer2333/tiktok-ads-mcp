"""Date-range aggregate reports (GMVMAX and Ads).

Two flavors:
  - `get_*_range_report` — single API call, server-side aggregation in
    advertiser native timezone (ad_tz). Faster but ad_tz-bound.
  - `get_*_range_report_aligned` — per-day loop calling the shop-tz
    aligned single-day fetcher, then sum. Matches the AdCostCache write
    path (which stores shop_tz-aligned single-day values), so cache-first
    reads can return identical numbers to a fresh API call.

Use the aligned variants when consumers expect shop-day semantics
(daily/MTD reports for operations who view shop-tz dashboards).
"""

import json
import logging
from datetime import datetime, timedelta
from typing import Any, Dict, List

from mcp_retry import api_retry

from ..client import TikTokAdsClient, TikTokRateLimitError

logger = logging.getLogger(__name__)


def _date_iter(start_date: str, end_date: str) -> List[str]:
    """Inclusive YYYY-MM-DD date list for [start, end]."""
    s = datetime.strptime(start_date, "%Y-%m-%d")
    e = datetime.strptime(end_date, "%Y-%m-%d")
    out = []
    cur = s
    while cur <= e:
        out.append(cur.strftime("%Y-%m-%d"))
        cur += timedelta(days=1)
    return out


@api_retry(
    max_attempts=3,
    min_wait=3,
    max_wait=15,
    retryable_exceptions=(TikTokRateLimitError,),
)
async def get_gmvmax_range_report(
    client: TikTokAdsClient,
    advertiser_id: str,
    store_ids: List[str],
    start_date: str,
    end_date: str,
) -> Dict[str, Any]:
    """Aggregate GMVMAX data for a date range.

    Returns:
        Dict with cost, gmv, orders, roi.
    """
    total = {"cost": 0.0, "gmv": 0.0, "orders": 0}
    page = 1

    while True:
        params = {
            "advertiser_id": advertiser_id,
            "store_ids": json.dumps(store_ids),
            "start_date": start_date,
            "end_date": end_date,
            "dimensions": json.dumps(["advertiser_id"]),
            "metrics": json.dumps(["cost", "gross_revenue", "orders"]),
            "page": page,
            "page_size": 1000,
        }
        response = await client._make_request("GET", "gmv_max/report/get/", params)
        if response.get("code") != 0:
            # Surface instead of silently returning partial data. Same rationale
            # as _fetch_hourly in gmvmax_report_aligned.py.
            raise Exception(
                f"gmv_max/report/get/ returned code={response.get('code')} "
                f"msg={response.get('message')!r} for advertiser={advertiser_id} "
                f"range={start_date}~{end_date}"
            )

        items = response.get("data", {}).get("list", [])
        for item in items:
            m = item.get("metrics", {})
            total["cost"] += float(m.get("cost", 0))
            total["gmv"] += float(m.get("gross_revenue", 0))
            total["orders"] += int(m.get("orders", 0))

        page_info = response.get("data", {}).get("page_info", {})
        if page >= page_info.get("total_page", 1) or not items:
            break
        page += 1

    total["roi"] = round(total["gmv"] / total["cost"], 2) if total["cost"] > 0 else 0.0
    total["cost"] = round(total["cost"], 2)
    total["gmv"] = round(total["gmv"], 2)
    return total


@api_retry(
    max_attempts=3,
    min_wait=3,
    max_wait=15,
    retryable_exceptions=(TikTokRateLimitError,),
)
async def get_gmvmax_range_report_breakdown(
    client: TikTokAdsClient,
    advertiser_id: str,
    store_ids: List[str],
    start_date: str,
    end_date: str,
) -> Dict[str, Any]:
    """Per-store GMVMAX date-range breakdown.

    Returns {store_id: {cost, gmv, orders, roi}} so callers route to product
    groups via STORE_PRODUCT_GROUP without depending on bitable's per-row
    (advertiser, store) binding being correct.
    """
    by_store: Dict[str, Dict[str, float]] = {}
    page = 1

    while True:
        params = {
            "advertiser_id": advertiser_id,
            "store_ids": json.dumps(store_ids),
            "start_date": start_date,
            "end_date": end_date,
            "dimensions": json.dumps(["store_id"]),
            "metrics": json.dumps(["cost", "gross_revenue", "orders"]),
            "page": page,
            "page_size": 1000,
        }
        response = await client._make_request("GET", "gmv_max/report/get/", params)
        if response.get("code") != 0:
            raise Exception(
                f"gmv_max/report/get/ returned code={response.get('code')} "
                f"msg={response.get('message')!r} for advertiser={advertiser_id} "
                f"range={start_date}~{end_date}"
            )

        items = response.get("data", {}).get("list", [])
        for item in items:
            dims = item.get("dimensions", {})
            store_id = str(dims.get("store_id", ""))
            if not store_id:
                continue
            m = item.get("metrics", {})
            bucket = by_store.setdefault(
                store_id, {"cost": 0.0, "gmv": 0.0, "orders": 0}
            )
            bucket["cost"] += float(m.get("cost", 0))
            bucket["gmv"] += float(m.get("gross_revenue", 0))
            bucket["orders"] += int(m.get("orders", 0))

        page_info = response.get("data", {}).get("page_info", {})
        if page >= page_info.get("total_page", 1) or not items:
            break
        page += 1

    out: Dict[str, Dict[str, Any]] = {}
    for store_id, bucket in by_store.items():
        cost = bucket["cost"]
        gmv = bucket["gmv"]
        out[store_id] = {
            "cost": round(cost, 2),
            "gmv": round(gmv, 2),
            "orders": bucket["orders"],
            "roi": round(gmv / cost, 2) if cost > 0 else 0.0,
        }
    return out


@api_retry(
    max_attempts=3,
    min_wait=3,
    max_wait=15,
    retryable_exceptions=(TikTokRateLimitError,),
)
async def get_ads_range_report(
    client: TikTokAdsClient,
    advertiser_id: str,
    start_date: str,
    end_date: str,
) -> Dict[str, Any]:
    """Aggregate Ads data for a date range.

    Returns:
        Dict with cost, gmv, orders, roas.
    """
    total = {"cost": 0.0, "gmv": 0.0, "orders": 0}
    page = 1

    while True:
        params = {
            "advertiser_id": advertiser_id,
            "report_type": "BASIC",
            "data_level": "AUCTION_ADVERTISER",
            "dimensions": json.dumps(["advertiser_id"]),
            "metrics": json.dumps(
                ["spend", "onsite_shopping", "total_onsite_shopping_value"]
            ),
            "start_date": start_date,
            "end_date": end_date,
            "service_type": "AUCTION",
            "page": page,
            "page_size": 1000,
        }
        response = await client._make_request("GET", "report/integrated/get/", params)
        if response.get("code") != 0:
            break

        items = response.get("data", {}).get("list", [])
        for item in items:
            m = item.get("metrics", {})
            total["cost"] += float(m.get("spend", 0))
            total["gmv"] += float(m.get("total_onsite_shopping_value", 0))
            total["orders"] += int(m.get("onsite_shopping", 0))

        page_info = response.get("data", {}).get("page_info", {})
        if page >= page_info.get("total_page", 1) or not items:
            break
        page += 1

    total["roas"] = round(total["gmv"] / total["cost"], 2) if total["cost"] > 0 else 0.0
    total["cost"] = round(total["cost"], 2)
    total["gmv"] = round(total["gmv"], 2)
    return total


# ── Shop-tz aligned variants ──────────────────────────────────────────


async def get_gmvmax_range_report_aligned(
    client: TikTokAdsClient,
    advertiser_id: str,
    store_ids: List[str],
    start_date: str,
    end_date: str,
    shop_tz: str = "America/Los_Angeles",
) -> Dict[str, Any]:
    """GMVMAX date-range report, shop-tz aligned per day.

    Loops each day in [start, end], calls `get_gmvmax_report_aligned` (which
    fetches hourly data and slices to shop_tz day boundary), and sums. The
    per-day shop-tz alignment matches AdCostCache writes (also via the same
    aligned fetcher), so cache-first reads return identical numbers.

    Slower than `get_gmvmax_range_report` (N HTTP calls vs 1) but produces
    shop-day-correct totals regardless of advertiser tz config.
    """
    import asyncio as _asyncio

    from .gmvmax_report_aligned import get_gmvmax_report_aligned

    days = _date_iter(start_date, end_date)
    if not days:
        return {"cost": 0.0, "gmv": 0.0, "orders": 0, "roi": 0.0}

    # Fetch days concurrently — client's Semaphore(5) caps real concurrency.
    per_day = await _asyncio.gather(
        *(
            get_gmvmax_report_aligned(client, advertiser_id, d, store_ids, shop_tz)
            for d in days
        )
    )

    total_cost = 0.0
    total_gmv = 0.0
    total_orders = 0
    for r in per_day:
        m = r.get("metrics", {})
        total_cost += float(m.get("cost", 0))
        total_gmv += float(m.get("gross_revenue", 0))
        total_orders += int(m.get("orders", 0))

    return {
        "cost": round(total_cost, 2),
        "gmv": round(total_gmv, 2),
        "orders": total_orders,
        "roi": round(total_gmv / total_cost, 2) if total_cost > 0 else 0.0,
    }


async def get_ads_range_report_aligned(
    client: TikTokAdsClient,
    advertiser_id: str,
    start_date: str,
    end_date: str,
    shop_tz: str = "America/Los_Angeles",
) -> Dict[str, Any]:
    """Ads date-range report, shop-tz aligned per day.

    Companion to `get_gmvmax_range_report_aligned`. See that docstring for
    motivation.
    """
    import asyncio as _asyncio

    from .ads_report_aligned import get_ads_report_aligned

    days = _date_iter(start_date, end_date)
    if not days:
        return {"cost": 0.0, "gmv": 0.0, "orders": 0, "roas": 0.0}

    per_day = await _asyncio.gather(
        *(get_ads_report_aligned(client, advertiser_id, d, shop_tz) for d in days)
    )

    total_cost = 0.0
    total_gmv = 0.0
    total_orders = 0
    for r in per_day:
        m = r.get("metrics", {})
        total_cost += float(m.get("cost", 0))
        total_gmv += float(m.get("gmv", 0))
        total_orders += int(m.get("orders", 0))

    return {
        "cost": round(total_cost, 2),
        "gmv": round(total_gmv, 2),
        "orders": total_orders,
        "roas": round(total_gmv / total_cost, 2) if total_cost > 0 else 0.0,
    }
