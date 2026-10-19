"""TikTok GMV Max MCP (distribution: tiktok-gmvmax-mcp)

A pure MCP (Model Context Protocol) server for TikTok Business API integration.
"""

__version__ = "0.2.0"
__author__ = "Buer2333, Yu Shengnan"

from .server import app, main
from .client import TikTokAdsClient
from .config import config

__all__ = ["app", "main", "TikTokAdsClient", "config"] 