"""TikTok Ads API Client for MCP Server — dual BC token fallback."""

import asyncio
import httpx
import json
import logging
from typing import Dict, List, Optional, Any
from urllib.parse import urlencode
from mcp_retry import httpx_retry

from .config import config

# Set up logging
logger = logging.getLogger(__name__)


class TikTokPermissionError(Exception):
    """Raised when advertiser is not authorized under current token."""

    pass


class TikTokRateLimitError(Exception):
    """Raised on TikTok API rate limit."""

    pass


class TikTokAdsClient:
    """TikTok Business API client with dual-token fallback."""

    def __init__(self):
        """Initialize TikTok API client with one or two tokens."""
        if not config.validate_credentials():
            missing = config.get_missing_credentials()
            raise Exception(
                f"Missing required credentials: {', '.join(missing)}. "
                f"Please check your configuration and ensure all required fields are set."
            )

        self.app_id = config.APP_ID
        self.secret = config.SECRET
        self.base_url = config.BASE_URL
        self.api_version = config.API_VERSION
        self.request_timeout = config.REQUEST_TIMEOUT

        # Build token list (token 2 is optional)
        self.tokens: List[str] = [config.ACCESS_TOKEN]
        if config.ACCESS_TOKEN_2:
            self.tokens.append(config.ACCESS_TOKEN_2)

        # Cache: advertiser_id -> token index (which token works)
        self._token_map: Dict[str, int] = {}
        # Concurrency limiter for API calls (429 retry handles bursts)
        self._semaphore = asyncio.Semaphore(5)

        n = len(self.tokens)
        logger.info(f"TikTok API client initialized with {n} token(s)")

    def _build_url(self, endpoint: str, params: Optional[Dict]) -> str:
        if params:
            query_string = urlencode(params)
            return f"{self.base_url}/{self.api_version}/{endpoint}?{query_string}"
        return f"{self.base_url}/{self.api_version}/{endpoint}"

    @httpx_retry(
        retryable_exceptions=(
            httpx.RequestError,
            httpx.HTTPStatusError,
            TikTokRateLimitError,
        )
    )
    async def _do_request(
        self,
        token: str,
        method: str,
        endpoint: str,
        params: Optional[Dict] = None,
        data: Optional[Dict] = None,
    ) -> Dict[str, Any]:
        """Execute a single HTTP request with retry on transient errors."""
        url = self._build_url(endpoint, params)
        headers = {"Access-Token": token, "Content-Type": "application/json"}

        async with (
            self._semaphore,
            httpx.AsyncClient(timeout=self.request_timeout) as client,
        ):
            logger.debug(f"Making {method} request to {url}")

            if method == "GET":
                response = await client.get(url, headers=headers)
            elif method == "POST":
                response = await client.post(url, json=data, headers=headers)
            else:
                raise Exception(f"Unsupported HTTP method: {method}")

            # Non-retryable client errors
            if response.status_code == 401:
                raise Exception("Invalid access token or credentials")
            if response.status_code == 403:
                raise Exception("Access forbidden - check your API permissions")

            # Retryable: 429 and 5xx
            if response.status_code == 429 or response.status_code >= 500:
                response.raise_for_status()

            # Other client errors
            if response.status_code >= 400:
                raise Exception(f"HTTP {response.status_code}: {response.text}")

            try:
                result = response.json()
            except json.JSONDecodeError:
                raise Exception(f"Invalid JSON response: {response.text}")

            # Check TikTok API response code
            if result.get("code") != 0:
                error_msg = result.get("message", "Unknown API error")
                error_code = result.get("code", 0)
                # Detect permission errors for token fallback
                msg_lower = error_msg.lower()
                if (
                    "no permission" in msg_lower
                    or "not authorized" in msg_lower
                    or "punish" in msg_lower
                ):
                    raise TikTokPermissionError(
                        f"TikTok API error {error_code}: {error_msg}"
                    )
                if "too many" in msg_lower or "rate limit" in msg_lower:
                    raise TikTokRateLimitError(
                        f"TikTok API error {error_code}: {error_msg}"
                    )
                raise Exception(f"TikTok API error {error_code}: {error_msg}")

            return result

    async def _make_request(
        self,
        method: str,
        endpoint: str,
        params: Optional[Dict] = None,
        data: Optional[Dict] = None,
    ) -> Dict[str, Any]:
        """Make request with automatic token fallback on permission errors."""
        if params is None:
            params = {}

        # oauth2 endpoints: inject app credentials, always use token[0]
        if "oauth2" in endpoint:
            params.update({"app_id": self.app_id, "secret": self.secret})
            return await self._do_request(
                self.tokens[0], method, endpoint, params, data
            )

        # Extract advertiser_id for token selection
        adv_id = params.get("advertiser_id") or (data or {}).get("advertiser_id")

        # Determine token order
        if adv_id and adv_id in self._token_map:
            # Known advertiser → try cached token first, then others
            cached_idx = self._token_map[adv_id]
            indices = [cached_idx] + [
                i for i in range(len(self.tokens)) if i != cached_idx
            ]
        else:
            indices = list(range(len(self.tokens)))

        last_error = None
        for idx in indices:
            try:
                result = await self._do_request(
                    self.tokens[idx], method, endpoint, params, data
                )
                # Success — cache token mapping
                if adv_id:
                    self._token_map[adv_id] = idx
                return result
            except TikTokPermissionError as e:
                logger.info(
                    f"Token {idx} permission denied for {adv_id or 'unknown'}, "
                    f"trying next token..."
                )
                last_error = e
                continue

        # All tokens exhausted
        raise last_error
