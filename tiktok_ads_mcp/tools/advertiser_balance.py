"""Get Advertiser Balance Tool

Endpoint: GET /advertiser/info/
Returns balance and basic info for individual advertiser accounts.
"""

import json
import logging
from typing import Dict, Any, List

logger = logging.getLogger(__name__)


async def get_advertiser_balance(client, advertiser_ids: List[str], **kwargs) -> List[Dict[str, Any]]:
    """Get balance and info for individual advertiser accounts.

    Args:
        advertiser_ids: List of advertiser ID strings (max 100)
    """
    if not advertiser_ids:
        raise ValueError("advertiser_ids is required")
    if len(advertiser_ids) > 100:
        raise ValueError("Maximum 100 advertiser IDs per request")

    params = {
        'advertiser_ids': json.dumps(advertiser_ids),
        'fields': json.dumps(["advertiser_id", "balance", "name", "currency", "status"]),
    }

    try:
        response = await client._make_request('GET', 'advertiser/info/', params)
        advertisers = response.get('data', {}).get('list', [])

        return [
            {
                "advertiser_id": adv.get("advertiser_id"),
                "name": adv.get("name", "Unknown"),
                "balance": adv.get("balance"),
                "currency": adv.get("currency", ""),
                "status": adv.get("status", "Unknown"),
            }
            for adv in advertisers
        ]
    except Exception as e:
        logger.error(f"Failed to get advertiser balance: {e}")
        raise
