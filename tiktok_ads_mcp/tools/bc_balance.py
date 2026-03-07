"""Get Business Center Balance Tool

Endpoint: GET /bc/balance/get/
Returns the balance of a Business Center.
"""

import logging
from typing import Dict, Any

logger = logging.getLogger(__name__)


async def get_bc_balance(client, bc_id: str, **kwargs) -> Dict[str, Any]:
    """Get Business Center balance.

    Args:
        bc_id: Business Center ID
    """
    if not bc_id:
        raise ValueError("bc_id is required")

    params = {'bc_id': bc_id}

    try:
        response = await client._make_request('GET', 'bc/balance/get/', params)
        if response.get('code') == 0:
            return response.get('data', {})
        else:
            raise Exception(f"API returned code {response.get('code')}: {response.get('message', 'Unknown error')}")
    except Exception as e:
        logger.error(f"Failed to get BC balance: {e}")
        raise
