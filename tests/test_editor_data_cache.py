"""Tests for EditorDataCache: per-day editor breakdown persistence + merge."""

import tempfile
from pathlib import Path

from tiktok_ads_mcp.cache.editor_data import EditorDataCache


def test_empty_get_returns_none():
    with tempfile.TemporaryDirectory() as tmp:
        c = EditorDataCache(Path(tmp))
        assert c.get_daily("g1", "2026-05-01", "t1", False) is None
        assert c.get_range("g1", "2026-05-01", "2026-05-03", "t1", False) is None


def test_single_day_round_trip():
    with tempfile.TemporaryDirectory() as tmp:
        c = EditorDataCache(Path(tmp))
        c.put_daily(
            "g1",
            "2026-05-01",
            "t1",
            False,
            editor_data={"A": {"cost": 10, "gmv": 30, "orders": 1}},
            organic={"cost": 5, "gmv": 12},
            errors=[],
        )
        got = c.get_daily("g1", "2026-05-01", "t1", False)
        assert got["editor_data"] == {"A": {"cost": 10, "gmv": 30, "orders": 1}}
        assert got["organic"] == {"cost": 5, "gmv": 12}


def test_multi_day_merge_sums_per_editor():
    with tempfile.TemporaryDirectory() as tmp:
        c = EditorDataCache(Path(tmp))
        c.put_daily(
            "g1",
            "2026-05-01",
            "t1",
            False,
            editor_data={"A": {"cost": 10, "gmv": 30, "orders": 1}},
            organic={"cost": 1, "gmv": 5},
            errors=[],
        )
        c.put_daily(
            "g1",
            "2026-05-02",
            "t1",
            False,
            editor_data={
                "A": {"cost": 5, "gmv": 12, "orders": 1},
                "B": {"cost": 8, "gmv": 20, "orders": 0},
            },
            organic={"cost": 2, "gmv": 7},
            errors=["err1"],
        )
        merged = c.get_range("g1", "2026-05-01", "2026-05-02", "t1", False)
        assert merged["editor_data"]["A"] == {"cost": 15.0, "gmv": 42.0, "orders": 2}
        assert merged["editor_data"]["B"] == {"cost": 8.0, "gmv": 20.0, "orders": 0}
        assert merged["organic"] == {"cost": 3.0, "gmv": 12.0}
        assert merged["errors"] == ["err1"]


def test_strict_miss_returns_none():
    with tempfile.TemporaryDirectory() as tmp:
        c = EditorDataCache(Path(tmp))
        c.put_daily(
            "g1", "2026-05-01", "t1", False, {"A": {}}, {"cost": 0, "gmv": 0}, []
        )
        # Day 5-02 missing — strict mode returns None
        assert c.get_range("g1", "2026-05-01", "2026-05-02", "t1", False) is None


def test_allow_partial_returns_present_days():
    with tempfile.TemporaryDirectory() as tmp:
        c = EditorDataCache(Path(tmp))
        c.put_daily(
            "g1",
            "2026-05-01",
            "t1",
            False,
            editor_data={"A": {"cost": 10, "gmv": 30, "orders": 1}},
            organic={"cost": 0, "gmv": 0},
            errors=[],
        )
        partial = c.get_range(
            "g1", "2026-05-01", "2026-05-03", "t1", False, allow_partial=True
        )
        assert partial["cached_days"] == 1
        assert partial["total_days"] == 3
        assert partial["editor_data"]["A"]["cost"] == 10.0


def test_team_separation():
    with tempfile.TemporaryDirectory() as tmp:
        c = EditorDataCache(Path(tmp))
        c.put_daily(
            "g1", "2026-05-01", "t1", False, {"A": {}}, {"cost": 0, "gmv": 0}, []
        )
        # Different team key → separate entry
        assert c.get_daily("g1", "2026-05-01", "t2", False) is None


def test_banned_separation():
    with tempfile.TemporaryDirectory() as tmp:
        c = EditorDataCache(Path(tmp))
        c.put_daily(
            "g1", "2026-05-01", "t1", False, {"A": {}}, {"cost": 0, "gmv": 0}, []
        )
        # Different banned flag → separate entry
        assert c.get_daily("g1", "2026-05-01", "t1", True) is None


def test_clear_wipes_data():
    with tempfile.TemporaryDirectory() as tmp:
        c = EditorDataCache(Path(tmp))
        c.put_daily(
            "g1", "2026-05-01", "t1", False, {"A": {}}, {"cost": 0, "gmv": 0}, []
        )
        c.clear()
        assert c.get_daily("g1", "2026-05-01", "t1", False) is None


def test_persistence_across_instances():
    """Two cache instances on same dir share state via JSON file."""
    with tempfile.TemporaryDirectory() as tmp:
        c1 = EditorDataCache(Path(tmp))
        c1.put_daily(
            "g1",
            "2026-05-01",
            "t1",
            False,
            editor_data={"A": {"cost": 7, "gmv": 14, "orders": 1}},
            organic={"cost": 0, "gmv": 0},
            errors=[],
        )
        c2 = EditorDataCache(Path(tmp))
        got = c2.get_daily("g1", "2026-05-01", "t1", False)
        assert got is not None
        assert got["editor_data"]["A"]["cost"] == 7
