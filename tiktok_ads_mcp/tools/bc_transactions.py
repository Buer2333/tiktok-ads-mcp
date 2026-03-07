"""Get Business Center Account Transactions Tool

Endpoint: GET /bc/account/transaction/get/
Returns transaction records (top-ups, deductions) for a Business Center.
"""

import json
import logging
from typing import Dict, Any, Optional

logger = logging.getLogger(__name__)


async def get_bc_transactions(
    client,
    bc_id: str,
    start_time: str,
    end_time: str,
    transaction_level: str = "BC",
    page: int = 1,
    page_size: int = 50,
    **kwargs
) -> Dict[str, Any]:
    """Get BC account transactions.

    Args:
        bc_id: Business Center ID
        start_time: Start time (YYYY-MM-DD HH:MM:SS)
        end_time: End time (YYYY-MM-DD HH:MM:SS)
        transaction_level: BC or ADVERTISER (default BC)
        page: Page number (default 1)
        page_size: Page size (default 50)
    """
    if not bc_id:
        raise ValueError("bc_id is required")
    if not start_time or not end_time:
        raise ValueError("start_time and end_time are required")

    filtering = {"start_time": start_time, "end_time": end_time}
    params = {
        'bc_id': bc_id,
        'filtering': json.dumps(filtering),
        'page': page,
        'page_size': page_size,
    }

    if transaction_level:
        params['transaction_level'] = transaction_level

    try:
        response = await client._make_request('GET', 'bc/account/transaction/get/', params)
        if response.get('code') == 0:
            return response.get('data', {})
        else:
            raise Exception(f"API returned code {response.get('code')}: {response.get('message', 'Unknown error')}")
    except Exception as e:
        logger.error(f"Failed to get BC transactions: {e}")
        raise
