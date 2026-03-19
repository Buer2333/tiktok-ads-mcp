#!/usr/bin/env python3
"""TikTok Ads MCP Server

A modern MCP server implementation for TikTok Business API integration using FastMCP.
This provides a clean, efficient interface to the TikTok Ads API with automatic schema generation.
"""

import json
import logging
import functools
from typing import Dict, List, Optional

# MCP imports
from mcp.server import FastMCP

# TikTok Marketing API client
from .client import TikTokAdsClient
from .config import config

from .tools import (
    get_business_centers,
    get_authorized_ad_accounts,
    get_campaigns,
    get_ad_groups,
    get_ads,
    get_reports,
    get_gmvmax_campaigns,
    get_gmvmax_reports,
    get_gmvmax_campaign_info,
    get_video_info,
    get_creative_report,
    get_gmvmax_videos,
    get_bc_balance,
    get_bc_account_cost,
    get_bc_transactions,
    get_bc_budget_changelog,
    get_gmvmax_store_list,
    get_advertiser_balance,
)

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Global client instance (initialized on first use)
tiktok_client: Optional[TikTokAdsClient] = None

# Create MCP server instance
app = FastMCP("tiktok-ads")


def get_tiktok_client() -> TikTokAdsClient:
    """Get or create TikTok API client instance"""
    global tiktok_client

    if tiktok_client is None:
        try:
            tiktok_client = TikTokAdsClient()
            logger.info("TikTok API client initialized successfully")
        except Exception as e:
            logger.error(f"Failed to initialize TikTok client: {e}")
            raise

    return tiktok_client


def handle_errors(func):
    """Decorator to handle errors in tool functions"""

    @functools.wraps(func)
    async def wrapper(*args, **kwargs):
        try:
            return await func(*args, **kwargs)
        except Exception as e:
            return json.dumps(
                {
                    "error": True,
                    "message": f"Error: {str(e)}",
                    "suggestion": "Please check your configuration and try again.",
                },
                indent=2,
            )

    return wrapper


@app.tool()
@handle_errors
async def get_business_centers_tool(
    bc_id: str = "", page: int = 1, page_size: int = 10
) -> str:
    """Get business centers accessible by the current access token"""
    client = get_tiktok_client()
    # Note: tools need to be updated to be async or we wrap them here if they are synchronous but use async client
    # Since we updated client to be async, the tools calling client._make_request must be awaited.
    # We will assume tools are updated to be async or return awaitables.
    centers = await get_business_centers(
        client, bc_id=bc_id, page=page, page_size=page_size
    )

    return json.dumps(
        {"success": True, "count": len(centers), "centers": centers}, indent=2
    )


@app.tool()
@handle_errors
async def get_authorized_ad_accounts_tool(random_string: str = "") -> str:
    """Get all authorized ad accounts accessible by the current access token"""
    client = get_tiktok_client()
    advertisers = await get_authorized_ad_accounts(client)

    return json.dumps(
        {"success": True, "count": len(advertisers), "advertisers": advertisers},
        indent=2,
    )


@app.tool()
@handle_errors
async def get_campaigns_tool(advertiser_id: str, filters: Dict = None) -> str:
    """Get campaigns for a specific advertiser with optional filtering"""
    if not advertiser_id:
        raise ValueError("advertiser_id is required")

    client = get_tiktok_client()
    campaigns = await get_campaigns(
        client, advertiser_id=advertiser_id, filters=filters or {}
    )

    return json.dumps(
        {
            "success": True,
            "advertiser_id": advertiser_id,
            "count": len(campaigns),
            "campaigns": campaigns,
        },
        indent=2,
    )


@app.tool()
@handle_errors
async def get_ad_groups_tool(
    advertiser_id: str,
    campaign_id: Optional[str] = None,
    filters: Dict = None,
    page: int = 1,
    page_size: int = 10,
) -> str:
    """Get ad groups for a specific advertiser with optional filtering"""
    if not advertiser_id:
        raise ValueError("advertiser_id is required")

    client = get_tiktok_client()
    ad_groups = await get_ad_groups(
        client,
        advertiser_id=advertiser_id,
        campaign_id=campaign_id,
        filters=filters or {},
    )

    return json.dumps(
        {
            "success": True,
            "advertiser_id": advertiser_id,
            "campaign_id": campaign_id,
            "count": len(ad_groups),
            "ad_groups": ad_groups,
        },
        indent=2,
    )


@app.tool()
@handle_errors
async def get_ads_tool(
    advertiser_id: str,
    adgroup_id: Optional[str] = None,
    filters: Dict = None,
    page: int = 1,
    page_size: int = 10,
) -> str:
    """Get ads for a specific advertiser with optional filtering"""
    if not advertiser_id:
        raise ValueError("advertiser_id is required")

    client = get_tiktok_client()
    ads = await get_ads(
        client,
        advertiser_id=advertiser_id,
        adgroup_id=adgroup_id,
        filters=filters or {},
    )

    return json.dumps(
        {
            "success": True,
            "advertiser_id": advertiser_id,
            "adgroup_id": adgroup_id,
            "count": len(ads),
            "ads": ads,
        },
        indent=2,
    )


@app.tool()
@handle_errors
async def get_reports_tool(
    advertiser_id: Optional[str] = None,
    advertiser_ids: Optional[List[str]] = None,
    bc_id: Optional[str] = None,
    report_type: str = "BASIC",
    data_level: str = "AUCTION_CAMPAIGN",
    dimensions: Optional[List[str]] = None,
    metrics: Optional[List[str]] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    filters: Optional[List[Dict]] = None,
    page: int = 1,
    page_size: int = 10,
    service_type: str = "AUCTION",
    query_lifetime: bool = False,
    enable_total_metrics: bool = False,
    multi_adv_report_in_utc_time: bool = False,
    order_field: Optional[str] = None,
    order_type: str = "DESC",
) -> str:
    """Get performance reports and analytics with comprehensive filtering and grouping options"""

    client = get_tiktok_client()

    # Set smart defaults based on report_type
    if dimensions is None:
        if report_type == "TT_SHOP":
            dimensions = ["advertiser_id", "country_code"]
        else:
            dimensions = ["campaign_id", "stat_time_day"]
    if metrics is None:
        if report_type == "TT_SHOP":
            metrics = ["spend", "billed_cost"]
        else:
            metrics = ["spend", "impressions"]

    reports = await get_reports(
        client,
        advertiser_id=advertiser_id,
        advertiser_ids=advertiser_ids,
        bc_id=bc_id,
        report_type=report_type,
        data_level=data_level,
        dimensions=dimensions,
        metrics=metrics,
        start_date=start_date,
        end_date=end_date,
        filters=filters,
        page=page,
        page_size=page_size,
        service_type=service_type,
        query_lifetime=query_lifetime,
        enable_total_metrics=enable_total_metrics,
        multi_adv_report_in_utc_time=multi_adv_report_in_utc_time,
        order_field=order_field,
        order_type=order_type,
    )

    return json.dumps(
        {
            "success": True,
            "report_type": report_type,
            "data_level": data_level,
            "total_metrics": reports.get("total_metrics"),
            "page_info": reports.get("page_info", {}),
            "count": len(reports.get("list", [])),
            "reports": reports.get("list", []),
        },
        indent=2,
    )


@app.tool()
@handle_errors
async def get_gmvmax_campaigns_tool(
    advertiser_id: str,
    campaign_ids: Optional[List[str]] = None,
    filtering: Optional[Dict] = None,
    page: int = 1,
    page_size: int = 10,
) -> str:
    """Get GMV Max campaigns via /gmv_max/campaign/get/. Returns campaign list with status and ROI protection info. Default filter: PRODUCT_GMV_MAX."""
    if not advertiser_id:
        raise ValueError("advertiser_id is required")

    client = get_tiktok_client()
    result = await get_gmvmax_campaigns(
        client,
        advertiser_id=advertiser_id,
        campaign_ids=campaign_ids,
        filtering=filtering,
        page=page,
        page_size=page_size,
    )

    return json.dumps(
        {
            "success": True,
            "advertiser_id": advertiser_id,
            "count": len(result["campaigns"]),
            "campaigns": result["campaigns"],
            "page_info": result["page_info"],
        },
        indent=2,
    )


@app.tool()
@handle_errors
async def get_gmvmax_reports_tool(
    advertiser_id: str,
    start_date: str,
    end_date: str,
    store_ids: List[str],
    dimensions: Optional[List[str]] = None,
    metrics: Optional[List[str]] = None,
    filtering: Optional[Dict] = None,
    page: int = 1,
    page_size: int = 1000,
) -> str:
    """Get GMV Max performance reports via /gmv_max/report/get/.
    store_ids is REQUIRED (get from campaign info endpoint).
    Metrics: cost, orders, cost_per_order, gross_revenue, roi, net_cost, creative_delivery_status, product_impressions, product_clicks, product_click_rate, ad_click_rate, ad_conversion_rate, ad_video_view_rate_2s/6s/p25/p50/p75/p100.
    Dimensions: advertiser_id, stat_time_day, item_id (item_id requires filtering with campaign_ids AND item_group_ids).
    Filtering: {"campaign_ids": ["..."], "item_group_ids": ["..."]}."""

    client = get_tiktok_client()
    reports = await get_gmvmax_reports(
        client,
        advertiser_id=advertiser_id,
        start_date=start_date,
        end_date=end_date,
        store_ids=store_ids,
        dimensions=dimensions,
        metrics=metrics,
        filtering=filtering,
        page=page,
        page_size=page_size,
    )

    return json.dumps(
        {
            "success": True,
            "endpoint": "/gmv_max/report/get/",
            "page_info": reports.get("page_info", {}),
            "count": len(reports.get("list", [])),
            "reports": reports.get("list", []),
        },
        indent=2,
    )


@app.tool()
@handle_errors
async def get_gmvmax_campaign_info_tool(advertiser_id: str, campaign_id: str) -> str:
    """Get detailed info for a specific GMV Max campaign via /campaign/gmv_max/info/. Returns budget, bid, product, and scheduling details."""
    if not advertiser_id:
        raise ValueError("advertiser_id is required")
    if not campaign_id:
        raise ValueError("campaign_id is required")

    client = get_tiktok_client()
    result = await get_gmvmax_campaign_info(
        client, advertiser_id=advertiser_id, campaign_id=campaign_id
    )

    return json.dumps(
        {
            "success": True,
            "advertiser_id": advertiser_id,
            "campaign_id": campaign_id,
            "info": result,
        },
        indent=2,
    )


@app.tool()
@handle_errors
async def get_video_info_tool(advertiser_id: str, video_ids: List[str]) -> str:
    """Get video asset details (URL, thumbnail, duration) by video IDs. Use with get_ads_tool to map ad → video_id → video URL/poster."""
    if not advertiser_id:
        raise ValueError("advertiser_id is required")
    if not video_ids:
        raise ValueError("video_ids is required")

    client = get_tiktok_client()
    videos = await get_video_info(
        client, advertiser_id=advertiser_id, video_ids=video_ids
    )

    return json.dumps(
        {
            "success": True,
            "advertiser_id": advertiser_id,
            "count": len(videos),
            "videos": videos,
        },
        indent=2,
    )


@app.tool()
@handle_errors
async def get_creative_report_tool(
    advertiser_id: str,
    start_date: str,
    end_date: str,
    report_type: str = "VIDEO_INSIGHT",
    page: int = 1,
    page_size: int = 50,
    filtering: Optional[Dict] = None,
) -> str:
    """Get creative-level performance report (video insights, engagement, play metrics). report_type: VIDEO_INSIGHT (default)."""
    if not advertiser_id:
        raise ValueError("advertiser_id is required")

    client = get_tiktok_client()
    result = await get_creative_report(
        client,
        advertiser_id=advertiser_id,
        start_date=start_date,
        end_date=end_date,
        report_type=report_type,
        page=page,
        page_size=page_size,
        filtering=filtering,
    )

    return json.dumps(
        {
            "success": True,
            "advertiser_id": advertiser_id,
            "report_type": report_type,
            "page_info": result.get("page_info", {}),
            "count": len(result.get("list", [])),
            "reports": result.get("list", []),
        },
        indent=2,
    )


@app.tool()
@handle_errors
async def get_gmvmax_videos_tool(
    advertiser_id: str,
    store_id: str,
    store_authorized_bc_id: Optional[str] = None,
    need_auth_code_video: bool = True,
    identity_list: Optional[List[Dict]] = None,
    page: int = 1,
    page_size: int = 50,
) -> str:
    """Get videos available for GMVMAX campaigns for a given store. Returns video list with IDs for cross-referencing with creative reports."""
    if not advertiser_id:
        raise ValueError("advertiser_id is required")
    if not store_id:
        raise ValueError("store_id is required")

    client = get_tiktok_client()
    result = await get_gmvmax_videos(
        client,
        advertiser_id=advertiser_id,
        store_id=store_id,
        store_authorized_bc_id=store_authorized_bc_id,
        need_auth_code_video=need_auth_code_video,
        identity_list=identity_list,
        page=page,
        page_size=page_size,
    )

    return json.dumps(
        {
            "success": True,
            "advertiser_id": advertiser_id,
            "store_id": store_id,
            "page_info": result.get("page_info", {}),
            "count": len(result.get("list", [])),
            "videos": result.get("list", []),
        },
        indent=2,
    )


@app.tool()
@handle_errors
async def get_bc_balance_tool(bc_id: str) -> str:
    """Get Business Center balance (available funds)."""
    if not bc_id:
        raise ValueError("bc_id is required")
    client = get_tiktok_client()
    result = await get_bc_balance(client, bc_id=bc_id)
    return json.dumps({"success": True, "bc_id": bc_id, "balance": result}, indent=2)


@app.tool()
@handle_errors
async def get_bc_account_cost_tool(
    bc_id: str, start_date: str, end_date: str, page: int = 1, page_size: int = 50
) -> str:
    """Get cost summary for all ad accounts under a BC within a date range. Useful for weekly/monthly spend review."""
    if not bc_id:
        raise ValueError("bc_id is required")
    client = get_tiktok_client()
    result = await get_bc_account_cost(
        client,
        bc_id=bc_id,
        start_date=start_date,
        end_date=end_date,
        page=page,
        page_size=page_size,
    )
    return json.dumps({"success": True, "bc_id": bc_id, "data": result}, indent=2)


@app.tool()
@handle_errors
async def get_bc_transactions_tool(
    bc_id: str,
    start_time: str,
    end_time: str,
    transaction_level: str = "BC",
    page: int = 1,
    page_size: int = 50,
) -> str:
    """Get BC transaction records (top-ups, deductions). transaction_level: BC or ADVERTISER. Times format: YYYY-MM-DD HH:MM:SS."""
    if not bc_id:
        raise ValueError("bc_id is required")
    client = get_tiktok_client()
    result = await get_bc_transactions(
        client,
        bc_id=bc_id,
        start_time=start_time,
        end_time=end_time,
        transaction_level=transaction_level,
        page=page,
        page_size=page_size,
    )
    return json.dumps({"success": True, "bc_id": bc_id, "data": result}, indent=2)


@app.tool()
@handle_errors
async def get_bc_budget_changelog_tool(
    bc_id: str,
    advertiser_id: str,
    start_date: str,
    end_date: str,
    page: int = 1,
    page_size: int = 50,
) -> str:
    """Get budget change history for a specific advertiser under BC. Track who changed budget and when."""
    if not bc_id:
        raise ValueError("bc_id is required")
    if not advertiser_id:
        raise ValueError("advertiser_id is required")
    client = get_tiktok_client()
    result = await get_bc_budget_changelog(
        client,
        bc_id=bc_id,
        advertiser_id=advertiser_id,
        start_date=start_date,
        end_date=end_date,
        page=page,
        page_size=page_size,
    )
    return json.dumps(
        {
            "success": True,
            "bc_id": bc_id,
            "advertiser_id": advertiser_id,
            "data": result,
        },
        indent=2,
    )


@app.tool()
@handle_errors
async def get_gmvmax_store_list_tool(advertiser_id: str) -> str:
    """Get stores linked to a GMVMAX advertiser account. Useful to verify store-advertiser bindings."""
    if not advertiser_id:
        raise ValueError("advertiser_id is required")
    client = get_tiktok_client()
    result = await get_gmvmax_store_list(client, advertiser_id=advertiser_id)
    return json.dumps(
        {"success": True, "advertiser_id": advertiser_id, "data": result}, indent=2
    )


@app.tool()
@handle_errors
async def get_advertiser_balance_tool(advertiser_ids: List[str]) -> str:
    """Get balance and basic info for individual advertiser accounts. Max 100 IDs per request."""
    if not advertiser_ids:
        raise ValueError("advertiser_ids is required")
    client = get_tiktok_client()
    result = await get_advertiser_balance(client, advertiser_ids=advertiser_ids)
    return json.dumps(
        {
            "success": True,
            "count": len(result),
            "advertisers": result,
        },
        indent=2,
    )


@app.tool()
@handle_errors
async def get_gmvmax_report_aligned_tool(
    advertiser_id: str,
    date: str,
    store_ids: List[str],
    shop_tz: str = "America/Los_Angeles",
    metrics: Optional[List[str]] = None,
) -> str:
    """Get GMVMAX report aligned to shop timezone. Fetches hourly data and re-aggregates to match a shop-tz day, regardless of the ad account's native timezone. Returns aggregated metrics (cost, gross_revenue, orders), ROI, and hours included."""
    if not advertiser_id:
        raise ValueError("advertiser_id is required")
    if not date:
        raise ValueError("date is required")
    if not store_ids:
        raise ValueError("store_ids is required")

    client = get_tiktok_client()
    result = await get_gmvmax_report_aligned(
        client,
        advertiser_id=advertiser_id,
        date=date,
        store_ids=store_ids,
        shop_tz=shop_tz,
        metrics=metrics,
    )
    return json.dumps({"success": True, **result}, indent=2)


@app.tool()
@handle_errors
async def get_ads_report_aligned_tool(
    advertiser_id: str,
    date: str,
    shop_tz: str = "America/Los_Angeles",
    metrics: Optional[List[str]] = None,
) -> str:
    """Get Ads (manual bid) report aligned to shop timezone. Fetches hourly data and re-aggregates to match a shop-tz day. Returns cost, gmv, orders, and roas."""
    if not advertiser_id:
        raise ValueError("advertiser_id is required")
    if not date:
        raise ValueError("date is required")

    from .tools import get_ads_report_aligned

    client = get_tiktok_client()
    result = await get_ads_report_aligned(
        client,
        advertiser_id=advertiser_id,
        date=date,
        shop_tz=shop_tz,
        metrics=metrics,
    )
    return json.dumps({"success": True, **result}, indent=2)


@app.tool()
@handle_errors
async def get_gmvmax_range_report_tool(
    advertiser_id: str,
    store_ids: List[str],
    start_date: str,
    end_date: str,
) -> str:
    """Get GMVMAX aggregate report for a date range (no timezone alignment). Returns cost, gmv, orders, roi."""
    if not advertiser_id:
        raise ValueError("advertiser_id is required")
    if not store_ids:
        raise ValueError("store_ids is required")

    from .tools import get_gmvmax_range_report

    client = get_tiktok_client()
    result = await get_gmvmax_range_report(
        client,
        advertiser_id=advertiser_id,
        store_ids=store_ids,
        start_date=start_date,
        end_date=end_date,
    )
    return json.dumps({"success": True, **result}, indent=2)


@app.tool()
@handle_errors
async def get_ads_range_report_tool(
    advertiser_id: str,
    start_date: str,
    end_date: str,
) -> str:
    """Get Ads (manual bid) aggregate report for a date range (no timezone alignment). Returns cost, gmv, orders, roas."""
    if not advertiser_id:
        raise ValueError("advertiser_id is required")

    from .tools import get_ads_range_report

    client = get_tiktok_client()
    result = await get_ads_range_report(
        client,
        advertiser_id=advertiser_id,
        start_date=start_date,
        end_date=end_date,
    )
    return json.dumps({"success": True, **result}, indent=2)


def main():
    """Main function to run the MCP server"""
    logger.info("Starting TikTok Ads MCP Server...")

    # Log configuration status
    try:
        if not config.validate_credentials():
            logger.warning(
                "Missing credentials detected. Server will start but API calls will fail."
            )
            missing = config.get_missing_credentials()
            logger.warning(f"Missing: {', '.join(missing)}")
        else:
            logger.info("Configuration validated successfully")
    except Exception as e:
        logger.error(f"Failed to check configuration: {e}")

    # Run the MCP server using stdio transport
    app.run(transport="stdio")


if __name__ == "__main__":
    main()
