"""Get Discovery Trending List Tool

Endpoint: GET /discovery/trending_list/
Returns trending hashtags on TikTok for a given country and date range.

Example curl:
  curl 'https://business-api.tiktok.com/open_api/v1.3/discovery/trending_list/
    ?advertiser_id=XXX&discovery_type=HASHTAG&country_code=US&date_range=7DAY' \
    -H 'Access-Token: xxx'

Required params:
  - advertiser_id
  - discovery_type: HASHTAG (only known supported value)

Optional params:
  - country_code: e.g. "US" (default by API: US)
  - date_range: "7DAY" | "30DAY" (default: 7DAY)
  - category_id: numeric category filter
  - page_size: number of results (API default applies)
"""

import logging
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)

# Fields to keep per trending item (drop verbose trending_history by default)
_KEEP_FIELDS = {
    "hashtag_id",
    "hashtag_name",
    "rank_position",
    "rank_change",
    "posts",
    "views",
    "top_country_list",
}


def _slim_item(item: Dict[str, Any], include_history: bool) -> Dict[str, Any]:
    """Keep only key fields from a trending item."""
    slim = {k: v for k, v in item.items() if k in _KEEP_FIELDS}
    if include_history:
        slim["trending_history"] = item.get("trending_history", [])
    return slim


async def get_trending_list(
    client,
    advertiser_id: str,
    discovery_type: str = "HASHTAG",
    country_code: str = "US",
    date_range: str = "7DAY",
    category_id: Optional[int] = None,
    page_size: Optional[int] = None,
    include_history: bool = False,
    **kwargs,
) -> Dict[str, Any]:
    """Get trending hashtags from TikTok Discovery API.

    Args:
        advertiser_id: TikTok advertiser ID (required for auth scope)
        discovery_type: Type of discovery data — only "HASHTAG" is confirmed
        country_code: Two-letter country code, e.g. "US"
        date_range: "7DAY" or "30DAY"
        category_id: Optional category filter (numeric)
        page_size: Number of results to return
        include_history: If True, include per-day trending_history in each item
    """
    if not advertiser_id:
        raise ValueError("advertiser_id is required")
    if not discovery_type:
        raise ValueError("discovery_type is required")

    params: Dict[str, Any] = {
        "advertiser_id": advertiser_id,
        "discovery_type": discovery_type,
        "country_code": country_code,
        "date_range": date_range,
    }
    if category_id is not None:
        params["category_id"] = category_id
    if page_size is not None:
        params["page_size"] = page_size

    try:
        response = await client._make_request("GET", "discovery/trending_list/", params)

        if response.get("code") == 0:
            data = response.get("data", {})
            raw_list: List[Dict[str, Any]] = data.get("list", [])
            slim_list = [_slim_item(item, include_history) for item in raw_list]
            return {
                "filter_info": data.get("filter_info", {}),
                "count": len(slim_list),
                "trending_list": slim_list,
            }
        else:
            raise Exception(
                f"API returned code {response.get('code')}: {response.get('message', 'Unknown error')}"
            )

    except Exception as e:
        logger.error(f"Failed to get trending list: {e}")
        raise
