"""Tests for AdAccountManager.discover_new_accounts()."""

from unittest.mock import AsyncMock, patch

import pytest

from tiktok_ads_mcp.cache.account_discovery import AccountDiscoveryCache
from tiktok_ads_mcp.business.account_manager import AdAccountManager
from tiktok_ads_mcp.client import TikTokPermissionError


@pytest.fixture
def discovery_cache(tmp_path):
    return AccountDiscoveryCache(tmp_path)


@pytest.fixture
def manager(discovery_cache):
    client = AsyncMock()
    return AdAccountManager(client=client, discovery_cache=discovery_cache)


def _make_authorized(ids):
    """Helper: build authorized accounts list from IDs."""
    return [{"advertiser_id": aid, "advertiser_name": f"Name-{aid}"} for aid in ids]


def _make_store_response(store_ids):
    """Helper: build gmvmax store list response."""
    return {"store_list": [{"store_id": sid} for sid in store_ids]}


class TestDiscoverNewAccounts:
    @pytest.mark.asyncio
    async def test_no_discovery_cache(self):
        """Returns empty if no discovery_cache injected."""
        mgr = AdAccountManager(client=AsyncMock())
        result = await mgr.discover_new_accounts(set())
        assert result == []

    @pytest.mark.asyncio
    async def test_no_new_accounts(self, manager, discovery_cache):
        """All accounts already in cache → returns empty."""
        discovery_cache.put("111", store_ids=["S1"], ad_type="gmvmax")
        discovery_cache.put("222", store_ids=[], ad_type="unknown")

        with patch(
            "tiktok_ads_mcp.tools.get_authorized_ad_accounts.get_authorized_ad_accounts",
            new_callable=AsyncMock,
            return_value=_make_authorized(["111", "222"]),
        ):
            result = await manager.discover_new_accounts(set())
        assert result == []

    @pytest.mark.asyncio
    async def test_discovers_gmvmax(self, manager, discovery_cache):
        """New account with stores → classified as GMVMAX."""
        with (
            patch(
                "tiktok_ads_mcp.tools.get_authorized_ad_accounts.get_authorized_ad_accounts",
                new_callable=AsyncMock,
                return_value=_make_authorized(["111"]),
            ),
            patch(
                "tiktok_ads_mcp.tools.gmvmax_store_list.get_gmvmax_store_list",
                new_callable=AsyncMock,
                return_value=_make_store_response(["S1", "S2"]),
            ),
        ):
            result = await manager.discover_new_accounts({"S1"})

        assert len(result) == 1
        assert result[0]["advertiser_id"] == "111"
        assert result[0]["store_ids"] == ["S1", "S2"]
        assert result[0]["matched_stores"] == ["S1"]
        assert result[0]["unmatched_stores"] == ["S2"]

        # Cache should be populated
        entry = discovery_cache.get("111")
        assert entry["ad_type"] == "gmvmax"
        assert entry["store_ids"] == ["S1", "S2"]

    @pytest.mark.asyncio
    async def test_discovers_unknown(self, manager, discovery_cache):
        """New account with no stores → classified as unknown, not returned."""
        with (
            patch(
                "tiktok_ads_mcp.tools.get_authorized_ad_accounts.get_authorized_ad_accounts",
                new_callable=AsyncMock,
                return_value=_make_authorized(["111"]),
            ),
            patch(
                "tiktok_ads_mcp.tools.gmvmax_store_list.get_gmvmax_store_list",
                new_callable=AsyncMock,
                return_value={"store_list": []},
            ),
        ):
            result = await manager.discover_new_accounts(set())

        assert result == []
        entry = discovery_cache.get("111")
        assert entry["ad_type"] == "unknown"

    @pytest.mark.asyncio
    async def test_permission_error_skips(self, manager, discovery_cache):
        """TikTokPermissionError → skip, don't cache."""
        with (
            patch(
                "tiktok_ads_mcp.tools.get_authorized_ad_accounts.get_authorized_ad_accounts",
                new_callable=AsyncMock,
                return_value=_make_authorized(["111"]),
            ),
            patch(
                "tiktok_ads_mcp.tools.gmvmax_store_list.get_gmvmax_store_list",
                new_callable=AsyncMock,
                side_effect=TikTokPermissionError("no access"),
            ),
        ):
            result = await manager.discover_new_accounts(set())

        assert result == []
        assert discovery_cache.get("111") is None  # Not cached

    @pytest.mark.asyncio
    async def test_generic_error_skips(self, manager, discovery_cache):
        """Other exceptions → skip, don't cache."""
        with (
            patch(
                "tiktok_ads_mcp.tools.get_authorized_ad_accounts.get_authorized_ad_accounts",
                new_callable=AsyncMock,
                return_value=_make_authorized(["111"]),
            ),
            patch(
                "tiktok_ads_mcp.tools.gmvmax_store_list.get_gmvmax_store_list",
                new_callable=AsyncMock,
                side_effect=Exception("timeout"),
            ),
        ):
            result = await manager.discover_new_accounts(set())

        assert result == []
        assert discovery_cache.get("111") is None

    @pytest.mark.asyncio
    async def test_mixed_accounts(self, manager, discovery_cache):
        """Mix of known, GMVMAX, unknown, and errored accounts."""
        # Pre-populate one known account
        discovery_cache.put("known", store_ids=["S0"], ad_type="gmvmax")

        async def mock_store_list(client, adv_id):
            if adv_id == "new_gmvmax":
                return _make_store_response(["S1"])
            if adv_id == "new_unknown":
                return {"store_list": []}
            raise Exception("api error")

        with (
            patch(
                "tiktok_ads_mcp.tools.get_authorized_ad_accounts.get_authorized_ad_accounts",
                new_callable=AsyncMock,
                return_value=_make_authorized(
                    ["known", "new_gmvmax", "new_unknown", "new_error"]
                ),
            ),
            patch(
                "tiktok_ads_mcp.tools.gmvmax_store_list.get_gmvmax_store_list",
                side_effect=mock_store_list,
            ),
        ):
            result = await manager.discover_new_accounts({"S1"})

        # Only new_gmvmax should be returned
        assert len(result) == 1
        assert result[0]["advertiser_id"] == "new_gmvmax"

        # Cache state
        assert discovery_cache.get("known")["ad_type"] == "gmvmax"
        assert discovery_cache.get("new_gmvmax")["ad_type"] == "gmvmax"
        assert discovery_cache.get("new_unknown")["ad_type"] == "unknown"
        assert discovery_cache.get("new_error") is None  # Not cached
