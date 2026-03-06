"""Get GMV Max Reports Tool

Uses the dedicated GMVMAX report endpoint: GET /gmv_max/report/get/
This endpoint supports rich metrics including cost, orders, ROI, gross_revenue, net_cost,
and product/ad performance metrics.

Available dimensions: advertiser_id, stat_time_day, item_id
Available metrics:
  - Core: cost, orders, cost_per_order, gross_revenue, roi, net_cost
  - Product: product_impressions, product_clicks, product_click_rate
  - Ad performance: ad_click_rate, ad_conversion_rate
  - Video: ad_video_view_rate_2s, ad_video_view_rate_6s,
           ad_video_view_rate_p25, ad_video_view_rate_p50,
           ad_video_view_rate_p75, ad_video_view_rate_p100
  - Status: creative_delivery_status
Filtering: campaign_ids, item_group_ids
"""

import json
import logging
from typing import Dict, Any, List, Optional

logger = logging.getLogger(__name__)

# Full metrics available on /gmv_max/report/get/
GMVMAX_DEFAULT_METRICS = [
    "cost",
    "orders",
    "cost_per_order",
    "gross_revenue",
    "roi",
    "net_cost",
]

GMVMAX_ALL_METRICS = GMVMAX_DEFAULT_METRICS + [
    "creative_delivery_status",
    "product_impressions",
    "product_clicks",
    "product_click_rate",
    "ad_click_rate",
    "ad_conversion_rate",
    "ad_video_view_rate_2s",
    "ad_video_view_rate_6s",
    "ad_video_view_rate_p25",
    "ad_video_view_rate_p50",
    "ad_video_view_rate_p75",
    "ad_video_view_rate_p100",
]

GMVMAX_DEFAULT_DIMENSIONS = ["advertiser_id", "stat_time_day"]


async def get_gmvmax_reports(
    client,
    advertiser_id: str,
    start_date: str,
    end_date: str,
    store_ids: Optional[List[str]] = None,
    dimensions: Optional[List[str]] = None,
    metrics: Optional[List[str]] = None,
    filtering: Optional[Dict] = None,
    page: int = 1,
    page_size: int = 1000,
    **kwargs
) -> Dict[str, Any]:
    """Get GMV Max performance reports via dedicated /gmv_max/report/get/ endpoint.

    Args:
        advertiser_id: TikTok advertiser ID
        start_date: Start date (YYYY-MM-DD)
        end_date: End date (YYYY-MM-DD)
        store_ids: Optional list of TikTok Shop store IDs to filter by
        dimensions: Grouping dimensions (default: advertiser_id + stat_time_day).
                    Also supports: item_id
        metrics: Metrics to retrieve (default: cost, orders, cost_per_order, gross_revenue, roi, net_cost).
                 Also supports: creative_delivery_status, product_impressions, product_clicks,
                 product_click_rate, ad_click_rate, ad_conversion_rate,
                 ad_video_view_rate_2s/6s/p25/p50/p75/p100
        filtering: Optional filter dict, supports keys:
                   campaign_ids (list of str), item_group_ids (list of str)
        page: Page number (default 1)
        page_size: Page size (default 1000)
    """
    if not advertiser_id:
        raise ValueError("advertiser_id is required")
    if not start_date or not end_date:
        raise ValueError("start_date and end_date are required")
    if page < 1:
        raise ValueError("page must be >= 1")
    if page_size < 1 or page_size > 1000:
        raise ValueError("page_size must be between 1 and 1000")

    if dimensions is None:
        dimensions = GMVMAX_DEFAULT_DIMENSIONS
    if metrics is None:
        metrics = GMVMAX_DEFAULT_METRICS

    params = {
        'advertiser_id': advertiser_id,
        'start_date': start_date,
        'end_date': end_date,
        'dimensions': json.dumps(dimensions),
        'metrics': json.dumps(metrics),
        'page': page,
        'page_size': page_size,
    }

    if store_ids:
        params['store_ids'] = json.dumps(store_ids)

    if filtering:
        params['filtering'] = json.dumps(filtering)

    try:
        response = await client._make_request('GET', 'gmv_max/report/get/', params)

        if response.get('code') == 0:
            data = response.get('data', {})

            report_data = {
                "page_info": data.get("page_info", {}),
                "list": []
            }

            for item in data.get("list", []):
                report_data["list"].append({
                    "dimensions": item.get("dimensions", {}),
                    "metrics": item.get("metrics", {})
                })

            return report_data
        else:
            raise Exception(f"API returned code {response.get('code')}: {response.get('message', 'Unknown error')}")

    except Exception as e:
        logger.error(f"Failed to get GMV Max reports: {e}")
        raise
