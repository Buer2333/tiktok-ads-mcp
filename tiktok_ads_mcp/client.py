"""TikTok Ads API Client for MCP Server"""

import httpx
import json
import logging
from typing import Dict, List, Optional, Any
from urllib.parse import urljoin, urlencode
from mcp_retry import httpx_retry

from .config import config

# Set up logging
logger = logging.getLogger(__name__)

class TikTokAdsClient:
    """TikTok Business API client for campaign operations."""
    
    def __init__(self):
        """Initialize TikTok API client"""
        # Validate credentials on initialization
        if not config.validate_credentials():
            missing = config.get_missing_credentials()
            raise Exception(
                f"Missing required credentials: {', '.join(missing)}. "
                f"Please check your configuration and ensure all required fields are set."
            )
        
        self.app_id = config.APP_ID
        self.secret = config.SECRET
        self.access_token = config.ACCESS_TOKEN
        self.base_url = config.BASE_URL
        self.api_version = config.API_VERSION
        self.request_timeout = config.REQUEST_TIMEOUT
        
        logger.info("TikTok API client initialized")
    
    @httpx_retry()
    async def _make_request(self, method: str, endpoint: str, params: Optional[Dict] = None,
                     data: Optional[Dict] = None) -> Dict[str, Any]:
        """Make HTTP request to TikTok API with retry on transient errors."""

        if params is None:
            params = {}

        if 'oauth2' in endpoint:
            params.update({
                'app_id': self.app_id,
                'secret': self.secret
            })

        if params:
            query_string = urlencode(params)
            url = f"{self.base_url}/{self.api_version}/{endpoint}?{query_string}"
        else:
            url = f"{self.base_url}/{self.api_version}/{endpoint}"

        headers = {
            'Access-Token': self.access_token,
            'Content-Type': 'application/json'
        }

        async with httpx.AsyncClient(timeout=self.request_timeout) as client:
            logger.debug(f"Making {method} request to {url}")

            if method == 'GET':
                response = await client.get(url, headers=headers)
            elif method == 'POST':
                response = await client.post(url, json=data, headers=headers)
            else:
                raise Exception(f"Unsupported HTTP method: {method}")

            # Non-retryable client errors
            if response.status_code == 401:
                raise Exception("Invalid access token or credentials")
            if response.status_code == 403:
                raise Exception("Access forbidden - check your API permissions")

            # Retryable: 429 and 5xx → raise HTTPStatusError for retry decorator
            if response.status_code == 429 or response.status_code >= 500:
                response.raise_for_status()

            # Other client errors (400, 404, etc.)
            if response.status_code >= 400:
                raise Exception(f"HTTP {response.status_code}: {response.text}")

            # Parse response
            try:
                result = response.json()
            except json.JSONDecodeError:
                raise Exception(f"Invalid JSON response: {response.text}")

            # Check TikTok API response code
            if result.get('code') != 0:
                error_msg = result.get('message', 'Unknown API error')
                raise Exception(f"TikTok API error {result.get('code')}: {error_msg}")

            return result