"""Configuration for TikTok Ads MCP Server.

Holds credentials + global API constants ONLY. Shop ↔ advertiser mapping
lives in `tiktok_ads_mcp.cache.AccountDiscoveryCache` (the canonical real-time
source, populated by `discover_new_accounts()`). Do NOT add a hardcoded
shop/advertiser dict here — every prior attempt drifted out of sync within
weeks and broke `/ads-report` aggregation.
"""

import os
from typing import Dict, Any, List, Optional


class TikTokConfig:
    """Configuration class for TikTok Business API.

    All env vars are read at access time (properties), not at import time,
    so that callers like lark-bot can load .env files before first use.
    """

    # API URLs (static)
    API_VERSION: str = "v1.3"

    @property
    def APP_ID(self) -> str:
        return os.getenv("TIKTOK_APP_ID", "")

    @property
    def SECRET(self) -> str:
        return os.getenv("TIKTOK_SECRET", "")

    @property
    def ACCESS_TOKEN(self) -> str:
        return os.getenv("TIKTOK_ACCESS_TOKEN_XINCHENG", "")

    @property
    def ACCESS_TOKEN_2(self) -> str:
        return os.getenv("TIKTOK_ACCESS_TOKEN_ZECHENG", "")

    @property
    def ADVERTISER_ID(self) -> str:
        return os.getenv("TIKTOK_ADVERTISER_ID", "")

    @property
    def SANDBOX(self) -> bool:
        return os.getenv("TIKTOK_SANDBOX", "false").lower() == "true"

    @property
    def BASE_URL(self) -> str:
        if self.SANDBOX:
            return "https://sandbox-ads.tiktok.com/open_api"
        return "https://business-api.tiktok.com/open_api"

    @property
    def REQUEST_TIMEOUT(self) -> int:
        return int(os.getenv("TIKTOK_REQUEST_TIMEOUT", "30"))

    @property
    def PROXY(self) -> Optional[str]:
        """Explicit proxy for business-api calls (e.g. library consumers like
        lark-bot whose process must NOT set global HTTP(S)_PROXY).

        business-api.tiktok.com is proxy-only reachable from some networks
        (launch.sh 72beccc). Unset -> None -> httpx default behavior
        (trust_env picks up HTTP(S)_PROXY as before) — zero change for
        existing deployments (MCP server launcher / VPS direct).
        """
        return os.getenv("TIKTOK_ADS_PROXY") or None

    def validate_credentials(self) -> bool:
        """Validate that all required credentials are present"""
        required_fields = [self.APP_ID, self.SECRET, self.ACCESS_TOKEN]
        return all(field.strip() for field in required_fields)

    def get_missing_credentials(self) -> List[str]:
        """Get list of missing credential fields"""
        missing = []
        if not self.APP_ID.strip():
            missing.append("TIKTOK_APP_ID")
        if not self.SECRET.strip():
            missing.append("TIKTOK_SECRET")
        if not self.ACCESS_TOKEN.strip():
            missing.append("TIKTOK_ACCESS_TOKEN")
        return missing

    def get_health_info(self) -> Dict[str, Any]:
        """Get system health information"""
        return {
            "config_valid": self.validate_credentials(),
            "base_url": self.BASE_URL,
            "api_version": self.API_VERSION,
        }


# Global config instance
config = TikTokConfig()
