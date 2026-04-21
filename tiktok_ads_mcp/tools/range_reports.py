"""Date-range aggregate reports (GMVMAX and Ads).

Simple date-range aggregation without timezone alignment.
Used for MTD summaries where per-hour cutting is not needed.
"""

import json
import logging
from typing import Any, Dict, List

from mcp_retry import api_retry

from ..client import TikTokAdsClient, TikTokRateLimitError

logger = logging.getLogger(__name__)


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
