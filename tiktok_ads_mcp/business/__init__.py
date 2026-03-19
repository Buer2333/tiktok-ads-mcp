"""Business logic layer for TikTok Ads account management.

Encapsulates ban detection, cached fetch, and balance tracking.
Designed for use as a library via Python import.
"""

from .account_manager import AdAccountManager

__all__ = ["AdAccountManager"]
