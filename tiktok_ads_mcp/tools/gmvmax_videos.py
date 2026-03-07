"""Get GMV Max Videos Tool

Endpoint: GET /gmv_max/video/get/
Returns videos available for GMVMAX campaigns for a given store.

Example curl:
  curl --location --request GET \
    'https://business-api.tiktok.com/open_api/v1.3/gmv_max/video/get/
    ?advertiser_id=XXX&store_id=YYY&store_authorized_bc_id=ZZZ
    &need_auth_code_video=true
    &identity_list=[{"identity_type":"BC_AUTH_TT","identity_id":"...","identity_authorized_bc_id":"..."}]
    &page=1&page_size=50' \
    --header 'Access-Token: xxx'
"""

import json
import logging
from typing import Dict, Any, List, Optional

logger = logging.getLogger(__name__)


async def get_gmvmax_videos(
    client,
    advertiser_id: str,
    store_id: str,
    store_authorized_bc_id: Optional[str] = None,
    need_auth_code_video: bool = True,
    identity_list: Optional[List[Dict]] = None,
    page: int = 1,
    page_size: int = 50,
    **kwargs
) -> Dict[str, Any]:
    """Get videos available for GMVMAX campaigns.

    Args:
        advertiser_id: TikTok advertiser ID
        store_id: TikTok Shop store ID (shop_cipher)
        store_authorized_bc_id: BC ID that authorized the store (optional)
        need_auth_code_video: Include auth code videos (default True)
        identity_list: List of identity dicts with identity_type, identity_id,
                       identity_authorized_bc_id
        page: Page number (default 1)
        page_size: Page size (default 50, max 50)
    """
    if not advertiser_id:
        raise ValueError("advertiser_id is required")
    if not store_id:
        raise ValueError("store_id is required")
    if page_size > 50:
        raise ValueError("page_size max 50")

    params = {
        'advertiser_id': advertiser_id,
        'store_id': store_id,
        'need_auth_code_video': str(need_auth_code_video).lower(),
        'page': page,
        'page_size': page_size,
    }

    if store_authorized_bc_id:
        params['store_authorized_bc_id'] = store_authorized_bc_id

    if identity_list:
        params['identity_list'] = json.dumps(identity_list)

    try:
        response = await client._make_request('GET', 'gmv_max/video/get/', params)

        if response.get('code') == 0:
            data = response.get('data', {})
            return {
                "page_info": data.get("page_info", {}),
                "list": data.get("list", []),
            }
        else:
            raise Exception(f"API returned code {response.get('code')}: {response.get('message', 'Unknown error')}")

    except Exception as e:
        logger.error(f"Failed to get GMVMAX videos: {e}")
        raise
