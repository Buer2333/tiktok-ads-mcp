"""Get GMV Max Store List Tool

Endpoint: GET /gmv_max/store/list/
Returns stores linked to a GMVMAX advertiser account.
"""

import logging
from typing import Dict, Any

logger = logging.getLogger(__name__)


async def get_gmvmax_store_list(client, advertiser_id: str, **kwargs) -> Dict[str, Any]:
    """Get stores linked to a GMVMAX advertiser.

    Args:
        advertiser_id: TikTok advertiser ID
    """
    if not advertiser_id:
        raise ValueError("advertiser_id is required")

    params = {'advertiser_id': advertiser_id}

    try:
        response = await client._make_request('GET', 'gmv_max/store/list/', params)
        if response.get('code') == 0:
            return response.get('data', {})
        else:
            raise Exception(f"API returned code {response.get('code')}: {response.get('message', 'Unknown error')}")
    except Exception as e:
        logger.error(f"Failed to get GMVMAX store list: {e}")
        raise
