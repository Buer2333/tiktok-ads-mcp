"""Get Business Centers Tool"""

import logging
from typing import Dict, Any, List, Optional

logger = logging.getLogger(__name__)

async def get_business_centers(client, bc_id: Optional[str] = None, page: int = 1, page_size: int = 10, **kwargs) -> List[Dict[str, Any]]:
    """Get business centers accessible by the current access token"""
    
    # Validate parameters
    if page < 1:
        raise ValueError("page must be >= 1")
    if page_size < 1 or page_size > 50:
        raise ValueError("page_size must be between 1 and 50")
    
    # Prepare parameters
    params = {
        'page': page,
        'page_size': page_size
    }
    
    # Add bc_id if provided
    if bc_id:
        params['bc_id'] = bc_id
    
    try:
        response = await client._make_request('GET', 'bc/get/', params)
        
        if response.get('code') == 0:
            business_centers = response.get('data', {}).get('list', [])
            
            return [
                {
                    "bc_id": bc.get("bc_info", {}).get("bc_id") or bc.get("bc_id"),
                    "name": bc.get("bc_info", {}).get("name") or bc.get("name", "Unknown"),
                    "company": bc.get("bc_info", {}).get("company") or bc.get("company", ""),
                    "currency": bc.get("bc_info", {}).get("currency") or bc.get("currency", ""),
                    "registered_area": bc.get("bc_info", {}).get("registered_area") or bc.get("registered_area", ""),
                    "status": bc.get("bc_info", {}).get("status") or bc.get("status", "Unknown"),
                    "timezone": bc.get("bc_info", {}).get("timezone") or bc.get("timezone", ""),
                    "type": bc.get("bc_info", {}).get("type") or bc.get("type", "Unknown"),
                    "user_role": bc.get("user_role", "Unknown"),
                    "finance_role": bc.get("finance_role"),
                    "ext_user_role": bc.get("ext_user_role")
                }
                for bc in business_centers
            ]
        else:
            raise Exception(f"API returned code {response.get('code')}: {response.get('message', 'Unknown error')}")
            
    except Exception as e:
        logger.error(f"Failed to get business centers: {e}")
        raise 