"""Get Creative Report Tool

Endpoint: GET /creative/report/get/
Returns creative-level performance reports including video insights.

Example curl:
  curl --location --request GET \
    'https://business-api.tiktok.com/open_api/v1.3/creative/report/get/
    ?report_type=VIDEO_INSIGHT&advertiser_id=XXX&start_date=2023-12-24&end_date=2024-01-23' \
    --header 'Access-Token: xxx'
"""

import json
import logging
from typing import Dict, Any, List, Optional

logger = logging.getLogger(__name__)


async def get_creative_report(
    client,
    advertiser_id: str,
    start_date: str,
    end_date: str,
    report_type: str = "VIDEO_INSIGHT",
    page: int = 1,
    page_size: int = 50,
    filtering: Optional[Dict] = None,
    **kwargs
) -> Dict[str, Any]:
    """Get creative-level performance report.

    Args:
        advertiser_id: TikTok advertiser ID
        start_date: Start date (YYYY-MM-DD)
        end_date: End date (YYYY-MM-DD)
        report_type: Report type (VIDEO_INSIGHT, etc.)
        page: Page number (default 1)
        page_size: Page size (default 50)
        filtering: Optional filter dict
    """
    if not advertiser_id:
        raise ValueError("advertiser_id is required")
    if not start_date or not end_date:
        raise ValueError("start_date and end_date are required")

    params = {
        'advertiser_id': advertiser_id,
        'report_type': report_type,
        'start_date': start_date,
        'end_date': end_date,
        'page': page,
        'page_size': page_size,
    }

    if filtering:
        params['filtering'] = json.dumps(filtering)

    try:
        response = await client._make_request('GET', 'creative/report/get/', params)

        if response.get('code') == 0:
            data = response.get('data', {})
            return {
                "page_info": data.get("page_info", {}),
                "list": data.get("list", []),
            }
        else:
            raise Exception(f"API returned code {response.get('code')}: {response.get('message', 'Unknown error')}")

    except Exception as e:
        logger.error(f"Failed to get creative report: {e}")
        raise
