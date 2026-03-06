"""Get GMV Max Campaign Info Tool

Endpoint: GET /campaign/gmv_max/info/
Returns detailed info for a specific GMVMAX campaign including budget, bid, products, etc.

Example curl:
  GET /open_api/v1.3/campaign/gmv_max/info/?advertiser_id=XXX&campaign_id=YYY
"""

import logging
from typing import Dict, Any

logger = logging.getLogger(__name__)


async def get_gmvmax_campaign_info(client, advertiser_id: str, campaign_id: str, **kwargs) -> Dict[str, Any]:
    """Get detailed info for a specific GMV Max campaign.

    Endpoint: GET /campaign/gmv_max/info/
    """
    if not advertiser_id:
        raise ValueError("advertiser_id is required")
    if not campaign_id:
        raise ValueError("campaign_id is required")

    params = {
        'advertiser_id': advertiser_id,
        'campaign_id': campaign_id,
    }

    try:
        response = await client._make_request('GET', 'campaign/gmv_max/info/', params)

        if response.get('code') != 0:
            raise Exception(f"API returned code {response.get('code')}: {response.get('message', 'Unknown error')}")

        data = response.get('data', {})
        return data
    except Exception as e:
        logger.error(f"Failed to get GMV Max campaign info: {e}")
        raise
