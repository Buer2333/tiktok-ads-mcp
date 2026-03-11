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

    fields = json.dumps(["advertiser_id", "balance", "name", "currency", "status"])

    # Try batch first; if it fails (e.g. one unauthorized ID), fall back to one-by-one
    try:
        response = await client._make_request('GET', 'advertiser/info/', {
            'advertiser_ids': json.dumps(advertiser_ids),
            'fields': fields,
        })
        advertisers = response.get('data', {}).get('list', [])
        return [_extract(adv) for adv in advertisers]
    except Exception as e:
        logger.warning(f"Batch query failed ({e}), falling back to individual queries")

    # Individual fallback
    results = []
    for aid in advertiser_ids:
        try:
            response = await client._make_request('GET', 'advertiser/info/', {
                'advertiser_ids': json.dumps([aid]),
                'fields': fields,
            })
            for adv in response.get('data', {}).get('list', []):
                results.append(_extract(adv))
        except Exception as e:
            logger.warning(f"Skipping {aid}: {e}")
            results.append({
                "advertiser_id": aid,
                "name": "Unknown",
                "balance": None,
                "currency": "",
                "status": "ERROR",
                "error": str(e),
            })
    return results


def _extract(adv: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "advertiser_id": adv.get("advertiser_id"),
        "name": adv.get("name", "Unknown"),
        "balance": adv.get("balance"),
        "currency": adv.get("currency", ""),
        "status": adv.get("status", "Unknown"),
    }
