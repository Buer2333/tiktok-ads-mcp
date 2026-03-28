"""Get TikTok identities (creator accounts) authorized to an advertiser."""

import logging
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)


async def get_identities(
    client,
    advertiser_id: str,
    identity_type: Optional[str] = None,
    identity_authorized_bc_id: Optional[str] = None,
    page: int = 1,
    page_size: int = 100,
    **kwargs,
) -> List[Dict[str, Any]]:
    """Get TikTok identities bound to an advertiser account.

    Args:
        advertiser_id: The advertiser account ID
        identity_type: Filter by type: CUSTOMIZED_USER, AUTH_CODE, TT_USER, BC_AUTH_TT
        identity_authorized_bc_id: Required when identity_type=BC_AUTH_TT
        page: Page number
        page_size: Results per page (max 100)
    """
    if not advertiser_id:
        raise ValueError("advertiser_id is required")

    if identity_type == "BC_AUTH_TT" and not identity_authorized_bc_id:
        raise ValueError(
            "identity_authorized_bc_id is required when identity_type=BC_AUTH_TT"
        )

    params: Dict[str, Any] = {
        "advertiser_id": advertiser_id,
        "page": page,
        "page_size": min(page_size, 100),
    }

    if identity_type:
        params["identity_type"] = identity_type
    if identity_authorized_bc_id:
        params["identity_authorized_bc_id"] = identity_authorized_bc_id

    response = await client._make_request("GET", "identity/get/", params)

    identities = response.get("data", {}).get("identity_list", [])

    return [
        {
            "identity_id": i.get("identity_id", ""),
            "identity_type": i.get("identity_type", ""),
            "display_name": i.get("display_name", ""),
            "user_name": i.get("user_name", ""),
        }
        for i in identities
    ]
