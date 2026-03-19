"""Configuration management for TikTok Ads MCP Server"""

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

    # Shop-Ads Account Mapping
    # Each shop maps to its GMVMAX and manual-bid (Ads) advertiser accounts
    SHOP_ADS_MAP: Dict[str, Dict[str, Any]] = {
        # --- Hiileathy (NAD+) ---
        "HIILEATHY Life": {
            "shop_id": "7495609170861329178",
            "shop_cipher": "TTP_tenLNwAAAACTRCLUE6BkmD9OlOxIhh92",
            "gmvmax_advertiser_id": "7502362341712330753",
            "ads_advertiser_id": "7537256848392192017",
        },
        "HIILEATHY US": {
            "shop_id": "7496222088046217765",
            "shop_cipher": "TTP_l4_gOQAAAAA1tDYYCsd5mD0aGd3MtYCa",
            "gmvmax_advertiser_id": "7546418505336586256",
            "ads_advertiser_id": None,
        },
        "HIILEATHY SHOP": {
            "shop_id": "7496100159169727050",
            "shop_cipher": "TTP_hHofRAAAAAAqhXpGsEPf5rV1WqrPp4pa",
            "gmvmax_advertiser_id": "7537240766474207240",
            "ads_advertiser_id": "7537261788128444417",
        },
        "HIILEATHY Global": {
            "shop_id": "7496213316913039393",
            "shop_cipher": "TTP_1PPwJwAAAAAcKUKfRZ9DKFGZ6oActeE-",
            "gmvmax_advertiser_id": "7589102514184830992",
            "ads_advertiser_id": None,
        },
        # --- FlyNew (Shilajit) ---
        "FLYNEW INC": {
            "shop_id": "7495613592836409756",
            "shop_cipher": "TTP_b7Jl1gAAAACmxpRDvItoWOxgSoLh-8pt",
            "gmvmax_advertiser_id": "7519334523298021393",
            "ads_advertiser_id": "7468251353308250128",
        },
        "FLYNEW US": {
            "shop_id": "7496304150093465844",
            "shop_cipher": "TTP_ZY09YgAAAAAQIU7w2O6kiournkujtV7m",
            "gmvmax_advertiser_id": "7546419360982908944",
            "ads_advertiser_id": "7468251353308250128",
        },
        "FLYNEW GLOBAL": {
            "shop_id": "7496213303501359425",
            "shop_cipher": "TTP_7a5tbQAAAADT6c7MYw09mppYB8Xz8fgF",
            "gmvmax_advertiser_id": "7519334212135141393",
            "ads_advertiser_id": "7468251353308250128",
        },
        "FLYNEW SHOP": {
            "shop_id": "7495652255284431231",
            "shop_cipher": "TTP_C_7bFQAAAABMifiMeLHxC55wcAuVzFJ7",
            "gmvmax_advertiser_id": "7519334426691174417",
            "ads_advertiser_id": "7468251353308250128",
        },
        "Flynew Health": {
            "shop_id": "7494115611898513226",
            "shop_cipher": "TTP_GYWHkQAAAAAoI9A-02S3OeCHr8I43gUI",
            "gmvmax_advertiser_id": "7520134971017822226",
            "ads_advertiser_id": "7468251353308250128",
        },
        "FLYNEW LIFE": {
            "shop_id": "7494222027955930495",
            "shop_cipher": "TTP_OaspCAAAAADDR3PWM6HgMja9EYZM8z6U",
            "gmvmax_advertiser_id": "7549087351114694663",
            "ads_advertiser_id": "7520135320541724690",
        },
        "FLYNEW USA": {
            "shop_id": "7494236950747186403",
            "shop_cipher": "TTP_uPam8wAAAADZsGhRHd3vxS-_LwOBOuAS",
            "gmvmax_advertiser_id": "7549087498392338433",
            "ads_advertiser_id": "7520135320541724690",
        },
        "FLYNEW S.A. (MX)": {
            "shop_id": "7494234571109795132",
            "shop_cipher": "ROW_I8N_fgAAAAD-G5STAOxxrEC5SW8Uivez",
            "gmvmax_advertiser_id": "7541294499696279570",
            "ads_advertiser_id": "7520135323477917697",
        },
    }

    @classmethod
    def get_shop(cls, shop_name: str) -> Optional[Dict[str, Any]]:
        """Get shop config by name (case-insensitive partial match)"""
        name_lower = shop_name.lower()
        for key, val in cls.SHOP_ADS_MAP.items():
            if name_lower in key.lower() or key.lower() in name_lower:
                return {"name": key, **val}
        return None

    @classmethod
    def get_shop_by_id(cls, shop_id: str) -> Optional[Dict[str, Any]]:
        """Get shop config by shop_id"""
        for key, val in cls.SHOP_ADS_MAP.items():
            if val["shop_id"] == shop_id:
                return {"name": key, **val}
        return None

    @classmethod
    def get_advertiser_shop(cls, advertiser_id: str) -> Optional[Dict[str, Any]]:
        """Find which shop an advertiser_id belongs to"""
        for key, val in cls.SHOP_ADS_MAP.items():
            if (
                val["gmvmax_advertiser_id"] == advertiser_id
                or val.get("ads_advertiser_id") == advertiser_id
            ):
                return {"name": key, **val}
        return None

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
