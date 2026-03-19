"""Tests for timezone alignment pure functions."""

from datetime import datetime, timezone
from zoneinfo import ZoneInfo

from tiktok_ads_mcp.timezone import (
    day_utc_range,
    hour_to_utc,
    native_dates_for_day,
    parse_tz,
)


class TestParseTz:
    def test_iana_name(self):
        tz = parse_tz("America/Los_Angeles")
        assert tz == ZoneInfo("America/Los_Angeles")

    def test_etc_gmt(self):
        tz = parse_tz("Etc/GMT+8")
        assert tz == ZoneInfo("Etc/GMT+8")

    def test_invalid_fallback_utc(self):
        tz = parse_tz("Not/A/Timezone")
        assert tz == ZoneInfo("UTC")

    def test_utc(self):
        tz = parse_tz("UTC")
        assert tz == ZoneInfo("UTC")


class TestDayUtcRange:
    def test_pst(self):
        """PST is UTC-8, so 2026-01-15 00:00 PST = 2026-01-15 08:00 UTC."""
        tz = ZoneInfo("America/Los_Angeles")
        start, end = day_utc_range("2026-01-15", tz)
        assert start == datetime(2026, 1, 15, 8, 0, tzinfo=timezone.utc)
        assert end == datetime(2026, 1, 16, 8, 0, tzinfo=timezone.utc)

    def test_pdt(self):
        """PDT is UTC-7, so 2026-07-15 00:00 PDT = 2026-07-15 07:00 UTC."""
        tz = ZoneInfo("America/Los_Angeles")
        start, end = day_utc_range("2026-07-15", tz)
        assert start == datetime(2026, 7, 15, 7, 0, tzinfo=timezone.utc)
        assert end == datetime(2026, 7, 16, 7, 0, tzinfo=timezone.utc)

    def test_utc(self):
        tz = ZoneInfo("UTC")
        start, end = day_utc_range("2026-03-10", tz)
        assert start == datetime(2026, 3, 10, 0, 0, tzinfo=timezone.utc)
        assert end == datetime(2026, 3, 11, 0, 0, tzinfo=timezone.utc)


class TestNativeDatesForDay:
    def test_same_timezone(self):
        """Same timezone → exactly 1 native date."""
        tz = ZoneInfo("America/Los_Angeles")
        dates = native_dates_for_day("2026-03-10", tz, tz)
        assert dates == ["2026-03-10"]

    def test_cross_timezone_two_dates(self):
        """Shop PST, ad UTC → shop day spans 2 UTC dates."""
        shop_tz = ZoneInfo("America/Los_Angeles")
        ad_tz = ZoneInfo("UTC")
        # PST day 2026-01-15: UTC 08:00 Jan 15 to 08:00 Jan 16
        dates = native_dates_for_day("2026-01-15", shop_tz, ad_tz)
        assert dates == ["2026-01-15", "2026-01-16"]

    def test_east_to_west(self):
        """Shop UTC, ad PST → shop day may map to 2 PST dates."""
        shop_tz = ZoneInfo("UTC")
        ad_tz = ZoneInfo("America/Los_Angeles")
        # UTC day 2026-01-15 00:00-24:00 → PST Jan 14 16:00 to Jan 15 16:00
        dates = native_dates_for_day("2026-01-15", shop_tz, ad_tz)
        assert dates == ["2026-01-14", "2026-01-15"]


class TestHourToUtc:
    def test_basic_conversion(self):
        """PST hour → UTC should add 8 hours."""
        ad_tz = ZoneInfo("America/Los_Angeles")
        # January = PST (UTC-8)
        utc = hour_to_utc("2026-01-15 10:00:00", ad_tz)
        assert utc == datetime(2026, 1, 15, 18, 0, tzinfo=timezone.utc)

    def test_pdt_conversion(self):
        """PDT hour → UTC should add 7 hours."""
        ad_tz = ZoneInfo("America/Los_Angeles")
        # July = PDT (UTC-7)
        utc = hour_to_utc("2026-07-15 10:00:00", ad_tz)
        assert utc == datetime(2026, 7, 15, 17, 0, tzinfo=timezone.utc)

    def test_dst_gap_fold(self):
        """DST spring-forward gap: 02:00 doesn't exist, fold=1 → post-transition."""
        ad_tz = ZoneInfo("America/Los_Angeles")
        # 2026 spring forward: Mar 8 at 2:00 AM → 3:00 AM
        # 02:00 with fold=1 → treated as PDT (UTC-7), so UTC = 09:00
        utc = hour_to_utc("2026-03-08 02:00:00", ad_tz)
        assert utc == datetime(2026, 3, 8, 9, 0, tzinfo=timezone.utc)

    def test_utc_no_change(self):
        ad_tz = ZoneInfo("UTC")
        utc = hour_to_utc("2026-03-10 15:00:00", ad_tz)
        assert utc == datetime(2026, 3, 10, 15, 0, tzinfo=timezone.utc)
