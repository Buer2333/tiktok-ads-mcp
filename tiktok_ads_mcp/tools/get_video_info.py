"""Get Video Ad Info Tool

Endpoint: GET /file/video/ad/info/
Returns video asset details (URL, thumbnail, duration, etc.) for given video IDs.

Example curl:
  curl --get -H "Access-Token:xxx" \
    --data-urlencode "advertiser_id=ADVERTISER_ID" \
    --data-urlencode "video_ids=VIDEO_IDS" \
    https://business-api.tiktok.com/open_api/v1.3/file/video/ad/info/
"""

import json
import logging
from typing import Dict, Any, List

logger = logging.getLogger(__name__)


async def get_video_info(
    client,
    advertiser_id: str,
    video_ids: List[str],
    **kwargs
) -> List[Dict[str, Any]]:
    """Get video asset details by video IDs.

    Args:
        advertiser_id: TikTok advertiser ID
        video_ids: List of video IDs to look up (max 100)
    """
    if not advertiser_id:
        raise ValueError("advertiser_id is required")
    if not video_ids:
        raise ValueError("video_ids is required")
    if len(video_ids) > 100:
        raise ValueError("video_ids max 100 per request")

    params = {
        'advertiser_id': advertiser_id,
        'video_ids': json.dumps(video_ids),
    }

    try:
        response = await client._make_request('GET', 'file/video/ad/info/', params)

        if response.get('code') == 0:
            videos = response.get('data', {}).get('list', [])
            return [
                {
                    "video_id": v.get("video_id"),
                    "material_id": v.get("material_id"),
                    "width": v.get("width"),
                    "height": v.get("height"),
                    "duration": v.get("duration"),
                    "url": v.get("url"),
                    "preview_url": v.get("preview_url"),
                    "preview_url_expire_time": v.get("preview_url_expire_time"),
                    "poster_url": v.get("poster_url"),
                    "bit_rate": v.get("bit_rate"),
                    "file_name": v.get("file_name"),
                    "file_size": v.get("file_size"),
                    "format": v.get("format"),
                    "displayable": v.get("displayable"),
                    "allow_download": v.get("allow_download"),
                    "create_time": v.get("create_time"),
                    "modify_time": v.get("modify_time"),
                }
                for v in videos
            ]
        else:
            raise Exception(f"API returned code {response.get('code')}: {response.get('message', 'Unknown error')}")

    except Exception as e:
        logger.error(f"Failed to get video info: {e}")
        raise
