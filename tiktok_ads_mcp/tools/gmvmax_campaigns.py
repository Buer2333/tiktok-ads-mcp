"""Get GMV Max Campaigns Tool

Correct endpoint: GET /gmv_max/campaign/get/
Requires filtering parameter with gmv_max_promotion_types.
"""

import json
import logging
from typing import Dict, Any, List, Optional

logger = logging.getLogger(__name__)


async def get_gmvmax_campaigns(client, advertiser_id: str, campaign_ids: Optional[List[str]] = None,
                                filtering: Optional[Dict] = None,
                                page: int = 1, page_size: int = 10, **kwargs) -> Dict[str, Any]:
    """Get GMV Max campaigns for an advertiser.

    Endpoint: GET /gmv_max/campaign/get/
    Default filter: {"gmv_max_promotion_types": ["PRODUCT_GMV_MAX"]}
    """
    params = {
        'advertiser_id': advertiser_id,
        'page': page,
        'page_size': page_size
    }

    # Apply filtering - default to PRODUCT_GMV_MAX if not specified
    if filtering is None:
        filtering = {"gmv_max_promotion_types": ["PRODUCT_GMV_MAX"]}
    params['filtering'] = json.dumps(filtering)

    if campaign_ids:
        params['campaign_ids'] = json.dumps(campaign_ids)

    try:
        response = await client._make_request('GET', 'gmv_max/campaign/get/', params)

        if response.get('code') != 0:
            raise Exception(f"API returned code {response.get('code')}: {response.get('message', 'Unknown error')}")

        data = response.get('data', {})
        campaigns = data.get('list', [])
        page_info = data.get('page_info', {})

        return {
            "campaigns": [
                {
                    "campaign_id": camp.get("campaign_id"),
                    "campaign_name": camp.get("campaign_name", "Unknown"),
                    "advertiser_id": camp.get("advertiser_id"),
                    "objective_type": camp.get("objective_type"),
                    "operation_status": camp.get("operation_status", "Unknown"),
                    "secondary_status": camp.get("secondary_status", "Unknown"),
                    "roi_protection_compensation_status": camp.get("roi_protection_compensation_status"),
                    "create_time": camp.get("create_time"),
                    "modify_time": camp.get("modify_time"),
                }
                for camp in campaigns
            ],
            "page_info": page_info,
        }
    except Exception as e:
        logger.error(f"Failed to get GMV Max campaigns: {e}")
        raise
