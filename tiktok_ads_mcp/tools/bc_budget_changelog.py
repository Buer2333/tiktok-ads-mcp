"""Get Business Center Budget Changelog Tool

Endpoint: GET /bc/account/budget/changelog/get/
Returns budget change history for a specific advertiser under a BC.
"""

import json
import logging
from typing import Dict, Any, Optional

logger = logging.getLogger(__name__)


async def get_bc_budget_changelog(
    client,
    bc_id: str,
    advertiser_id: str,
    start_date: str,
    end_date: str,
    page: int = 1,
    page_size: int = 50,
    **kwargs
) -> Dict[str, Any]:
    """Get budget change log for an advertiser under BC.

    Args:
        bc_id: Business Center ID
        advertiser_id: Advertiser ID to query changelog for
        start_date: Start date (YYYY-MM-DD)
        end_date: End date (YYYY-MM-DD)
        page: Page number (default 1)
        page_size: Page size (default 50)
    """
    if not bc_id:
        raise ValueError("bc_id is required")
    if not advertiser_id:
        raise ValueError("advertiser_id is required")
    if not start_date or not end_date:
        raise ValueError("start_date and end_date are required")

    filtering = {"start_date": start_date, "end_date": end_date}
    params = {
        'bc_id': bc_id,
        'advertiser_id': advertiser_id,
        'filtering': json.dumps(filtering),
        'page': page,
        'page_size': page_size,
    }

    try:
        response = await client._make_request('GET', 'bc/account/budget/changelog/get/', params)
        if response.get('code') == 0:
            return response.get('data', {})
        else:
            raise Exception(f"API returned code {response.get('code')}: {response.get('message', 'Unknown error')}")
    except Exception as e:
        logger.error(f"Failed to get budget changelog: {e}")
        raise
