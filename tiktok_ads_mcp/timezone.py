"""Timezone alignment utilities for TikTok Ads API.

Pure functions for converting between shop timezone and ad-account timezone.
Ported from lark-bot tiktok_api.py.
"""

from datetime import datetime, timedelta, timezone
from typing import List, Tuple
from zoneinfo import ZoneInfo


def parse_tz(tz_str: str) -> ZoneInfo:
    """Parse TikTok timezone string to ZoneInfo. Handles Etc/GMT and IANA names."""
    if tz_str.startswith("Etc/GMT"):
        try:
            return ZoneInfo(tz_str)
        except KeyError:
            return ZoneInfo("UTC")
    try:
        return ZoneInfo(tz_str)
    except KeyError:
        return ZoneInfo("UTC")


def day_utc_range(date_str: str, tz: ZoneInfo) -> Tuple[datetime, datetime]:
    """Given a date string and timezone, return (start_utc, end_utc) as aware datetimes."""
    naive = datetime.strptime(date_str, "%Y-%m-%d")
    local_start = naive.replace(tzinfo=tz)
    local_end = (naive + timedelta(days=1)).replace(tzinfo=tz)
    return local_start.astimezone(timezone.utc), local_end.astimezone(timezone.utc)


def native_dates_for_day(
    date_str: str, shop_tz: ZoneInfo, ad_tz: ZoneInfo
) -> List[str]:
    """Which ad-native-tz dates does a shop-tz day span?"""
    start_utc, end_utc = day_utc_range(date_str, shop_tz)
    native_start = start_utc.astimezone(ad_tz)
    native_end = (end_utc - timedelta(seconds=1)).astimezone(ad_tz)
    dates = set()
    d = native_start.date()
    while d <= native_end.date():
        dates.add(d.strftime("%Y-%m-%d"))
        d += timedelta(days=1)
    return sorted(dates)


def hour_to_utc(hour_str: str, ad_tz: ZoneInfo) -> datetime:
    """Convert '2026-03-05 16:00:00' in ad timezone to aware UTC datetime.

    Uses fold=1 so DST gap times resolve to the post-transition offset.
    """
    naive = datetime.strptime(hour_str, "%Y-%m-%d %H:%M:%S")
    local_dt = naive.replace(tzinfo=ad_tz, fold=1)
    return local_dt.astimezone(timezone.utc)
