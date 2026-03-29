"""Tests for AdAccountManager.discover_new_accounts()."""

from unittest.mock import AsyncMock, patch

import pytest

from tiktok_ads_mcp.cache.account_discovery import AccountDiscoveryCache
from tiktok_ads_mcp.business.account_manager import AdAccountManager


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


def _make_store_list(*entries):
    """Helper: build store_list response.

    Each entry: (store_id, store_name, exclusive_adv_id, exclusive_adv_name, status)
    """
    stores = []
    for sid, sname, adv_id, adv_name, status in entries:
        store = {
            "store_id": sid,
            "store_name": sname,
            "is_gmv_max_available": not bool(adv_id),
        }
        if adv_id:
            store["exclusive_authorized_advertiser_info"] = {
                "advertiser_id": adv_id,
                "advertiser_name": adv_name,
                "advertiser_status": status,
            }
        stores.append(store)
    return {"store_list": stores}


class TestDiscoverNewAccounts:
    @pytest.mark.asyncio
    async def test_no_discovery_cache(self):
        """Returns empty if no discovery_cache injected."""
        mgr = AdAccountManager(client=AsyncMock())
        result = await mgr.discover_new_accounts(set())
        assert result == []

    @pytest.mark.asyncio
    async def test_no_authorized_accounts(self, manager):
        """Returns empty if no authorized accounts."""
        with patch(
            "tiktok_ads_mcp.tools.get_authorized_ad_accounts.get_authorized_ad_accounts",
            new_callable=AsyncMock,
            return_value=[],
        ):
            result = await manager.discover_new_accounts(set())
        assert result == []

    @pytest.mark.asyncio
    async def test_discovers_exclusive_gmvmax(self, manager, discovery_cache):
        """Discovers exclusive GMVMAX advertiser for a known store."""
        store_resp = _make_store_list(
            ("S1", "Store One", "ADV1", "Ad Account 1", "STATUS_ENABLE"),
        )
        with patch(
            "tiktok_ads_mcp.tools.gmvmax_store_list.get_gmvmax_store_list",
            new_callable=AsyncMock,
            return_value=store_resp,
        ):
            result = await manager.discover_new_accounts(
                known_store_ids={"S1"},
                authorized_accounts=_make_authorized(["ADV1"]),
            )

        assert len(result) == 1
        assert result[0]["advertiser_id"] == "ADV1"
        assert result[0]["store_ids"] == ["S1"]

        entry = discovery_cache.get("ADV1")
        assert entry["ad_type"] == "gmvmax"
        assert entry["store_ids"] == ["S1"]
        assert not entry["banned"]

    @pytest.mark.asyncio
    async def test_banned_advertiser_marked(self, manager, discovery_cache):
        """Banned exclusive advertiser gets marked in cache."""
        store_resp = _make_store_list(
            ("S1", "Store One", "ADV1", "Banned Account", "STATUS_LIMIT"),
        )
        with patch(
            "tiktok_ads_mcp.tools.gmvmax_store_list.get_gmvmax_store_list",
            new_callable=AsyncMock,
            return_value=store_resp,
        ):
            result = await manager.discover_new_accounts(
                known_store_ids={"S1"},
                authorized_accounts=_make_authorized(["ADV1"]),
            )

        assert len(result) == 1
        entry = discovery_cache.get("ADV1")
        assert entry["banned"] is True

    @pytest.mark.asyncio
    async def test_unknown_store_not_returned(self, manager, discovery_cache):
        """Advertiser on unknown store: cached but not in result."""
        store_resp = _make_store_list(
            ("S_UNKNOWN", "Unknown Store", "ADV1", "Account", "STATUS_ENABLE"),
        )
        with patch(
            "tiktok_ads_mcp.tools.gmvmax_store_list.get_gmvmax_store_list",
            new_callable=AsyncMock,
            return_value=store_resp,
        ):
            result = await manager.discover_new_accounts(
                known_store_ids={"S1"},  # S_UNKNOWN not in known
                authorized_accounts=_make_authorized(["ADV1"]),
            )

        assert result == []
        # Still cached
        entry = discovery_cache.get("ADV1")
        assert entry["store_ids"] == ["S_UNKNOWN"]

    @pytest.mark.asyncio
    async def test_already_known_not_returned(self, manager, discovery_cache):
        """Known account with same store → not returned."""
        discovery_cache.put("ADV1", store_ids=["S1"], ad_type="gmvmax")

        store_resp = _make_store_list(
            ("S1", "Store One", "ADV1", "Account", "STATUS_ENABLE"),
        )
        with patch(
            "tiktok_ads_mcp.tools.gmvmax_store_list.get_gmvmax_store_list",
            new_callable=AsyncMock,
            return_value=store_resp,
        ):
            result = await manager.discover_new_accounts(
                known_store_ids={"S1"},
                authorized_accounts=_make_authorized(["ADV1"]),
            )

        assert result == []

    @pytest.mark.asyncio
    async def test_store_change_detected(self, manager, discovery_cache):
        """Account moves from one store to another → returned as changed."""
        discovery_cache.put("ADV1", store_ids=["S_OLD"], ad_type="gmvmax")

        store_resp = _make_store_list(
            ("S1", "Store New", "ADV1", "Account", "STATUS_ENABLE"),
        )
        with patch(
            "tiktok_ads_mcp.tools.gmvmax_store_list.get_gmvmax_store_list",
            new_callable=AsyncMock,
            return_value=store_resp,
        ):
            result = await manager.discover_new_accounts(
                known_store_ids={"S1"},
                authorized_accounts=_make_authorized(["ADV1"]),
            )

        assert len(result) == 1
        assert result[0]["advertiser_id"] == "ADV1"
        # Cache updated
        assert discovery_cache.get("ADV1")["store_ids"] == ["S1"]

    @pytest.mark.asyncio
    async def test_deduplicates_stores(self, manager, discovery_cache):
        """Duplicate store entries (same store_id) → only processed once."""
        store_resp = _make_store_list(
            ("S1", "Store One", "ADV1", "Account", "STATUS_ENABLE"),
            ("S1", "Store One Copy", "ADV1", "Account", "STATUS_ENABLE"),
        )
        with patch(
            "tiktok_ads_mcp.tools.gmvmax_store_list.get_gmvmax_store_list",
            new_callable=AsyncMock,
            return_value=store_resp,
        ):
            result = await manager.discover_new_accounts(
                known_store_ids={"S1"},
                authorized_accounts=_make_authorized(["ADV1"]),
            )

        assert len(result) == 1

    @pytest.mark.asyncio
    async def test_store_without_exclusive(self, manager, discovery_cache):
        """Store with no exclusive advertiser → skipped."""
        store_resp = _make_store_list(
            ("S1", "Available Store", "", "", ""),
        )
        with patch(
            "tiktok_ads_mcp.tools.gmvmax_store_list.get_gmvmax_store_list",
            new_callable=AsyncMock,
            return_value=store_resp,
        ):
            result = await manager.discover_new_accounts(
                known_store_ids={"S1"},
                authorized_accounts=_make_authorized(["ADV1"]),
            )

        assert result == []

    @pytest.mark.asyncio
    async def test_api_error_returns_empty(self, manager):
        """store_list API error → returns empty gracefully."""
        with patch(
            "tiktok_ads_mcp.tools.gmvmax_store_list.get_gmvmax_store_list",
            new_callable=AsyncMock,
            side_effect=Exception("timeout"),
        ):
            result = await manager.discover_new_accounts(
                known_store_ids={"S1"},
                authorized_accounts=_make_authorized(["ADV1"]),
            )
        assert result == []

    @pytest.mark.asyncio
    async def test_multiple_stores_multiple_advertisers(self, manager, discovery_cache):
        """Real-world scenario: multiple stores each with different advertisers."""
        store_resp = _make_store_list(
            ("S1", "FlyNew INC", "ADV1", "FN-Account-1", "STATUS_ENABLE"),
            ("S2", "FlyNew SHOP", "ADV2", "FN-Account-2", "STATUS_ENABLE"),
            ("S3", "HI Life", "ADV3", "HI-Account-3", "STATUS_LIMIT"),
            ("S_NEW", "New Store", "ADV4", "New-Account", "STATUS_ENABLE"),
        )
        with patch(
            "tiktok_ads_mcp.tools.gmvmax_store_list.get_gmvmax_store_list",
            new_callable=AsyncMock,
            return_value=store_resp,
        ):
            result = await manager.discover_new_accounts(
                known_store_ids={"S1", "S2", "S3"},
                authorized_accounts=_make_authorized(["ADV1", "ADV2", "ADV3"]),
            )

        # ADV1, ADV2, ADV3 are on known stores; ADV4 on unknown store
        adv_ids = {r["advertiser_id"] for r in result}
        assert adv_ids == {"ADV1", "ADV2", "ADV3"}

        # ADV3 should be banned
        assert discovery_cache.get("ADV3")["banned"] is True
        # ADV4 should be cached but not returned
        assert discovery_cache.get("ADV4")["store_ids"] == ["S_NEW"]


class TestDiscoverViaCampaigns:
    """Phase 2: discover non-exclusive GMVMAX accounts via campaign_info."""

    @pytest.mark.asyncio
    async def test_discovers_non_exclusive_via_campaign(self, manager, discovery_cache):
        """Account not in store_list exclusive but has GMVMAX campaign → discovered."""
        # Phase 1: ADV1 is exclusive on S1
        store_resp = _make_store_list(
            ("S1", "Store One", "ADV1", "Account-1", "STATUS_ENABLE"),
        )
        # ADV2 is authorized but not exclusive — has a campaign on S1
        campaigns_resp = {
            "campaigns": [
                {
                    "campaign_id": "C1",
                    "campaign_name": "Test Campaign",
                    "operation_status": "ENABLE",
                }
            ]
        }
        campaign_info_resp = {"info": {"store_id": "S1"}}

        with (
            patch(
                "tiktok_ads_mcp.tools.gmvmax_store_list.get_gmvmax_store_list",
                new_callable=AsyncMock,
                return_value=store_resp,
            ),
            patch(
                "tiktok_ads_mcp.tools.gmvmax_campaigns.get_gmvmax_campaigns",
                new_callable=AsyncMock,
                return_value=campaigns_resp,
            ),
            patch(
                "tiktok_ads_mcp.tools.gmvmax_campaign_info.get_gmvmax_campaign_info",
                new_callable=AsyncMock,
                return_value=campaign_info_resp,
            ),
        ):
            result = await manager.discover_new_accounts(
                known_store_ids={"S1"},
                authorized_accounts=_make_authorized(["ADV1", "ADV2"]),
            )

        adv_ids = {r["advertiser_id"] for r in result}
        assert "ADV2" in adv_ids
        # ADV2 cached with correct store
        entry = discovery_cache.get("ADV2")
        assert entry["store_ids"] == ["S1"]
        assert entry["ad_type"] == "gmvmax"

    @pytest.mark.asyncio
    async def test_non_gmvmax_cached_as_unknown(self, manager, discovery_cache):
        """Account with no GMVMAX campaigns → cached as unknown, not returned."""
        store_resp = _make_store_list(
            ("S1", "Store One", "ADV1", "Account-1", "STATUS_ENABLE"),
        )
        no_campaigns = {"campaigns": []}

        with (
            patch(
                "tiktok_ads_mcp.tools.gmvmax_store_list.get_gmvmax_store_list",
                new_callable=AsyncMock,
                return_value=store_resp,
            ),
            patch(
                "tiktok_ads_mcp.tools.gmvmax_campaigns.get_gmvmax_campaigns",
                new_callable=AsyncMock,
                return_value=no_campaigns,
            ),
        ):
            result = await manager.discover_new_accounts(
                known_store_ids={"S1"},
                authorized_accounts=_make_authorized(["ADV1", "ADV_ADS"]),
            )

        # ADV_ADS is not returned (not GMVMAX)
        adv_ids = {r["advertiser_id"] for r in result}
        assert "ADV_ADS" not in adv_ids
        # But cached as unknown to avoid re-checking
        entry = discovery_cache.get("ADV_ADS")
        assert entry["ad_type"] == "unknown"

    @pytest.mark.asyncio
    async def test_already_cached_skips_phase2(self, manager, discovery_cache):
        """Account already in cache → not re-checked in Phase 2."""
        discovery_cache.put("ADV2", store_ids=["S1"], ad_type="gmvmax")

        store_resp = _make_store_list(
            ("S1", "Store One", "ADV1", "Account-1", "STATUS_ENABLE"),
        )

        with (
            patch(
                "tiktok_ads_mcp.tools.gmvmax_store_list.get_gmvmax_store_list",
                new_callable=AsyncMock,
                return_value=store_resp,
            ) as mock_store,
            patch(
                "tiktok_ads_mcp.tools.gmvmax_campaigns.get_gmvmax_campaigns",
                new_callable=AsyncMock,
            ) as mock_campaigns,
        ):
            result = await manager.discover_new_accounts(
                known_store_ids={"S1"},
                authorized_accounts=_make_authorized(["ADV1", "ADV2"]),
            )

        # Phase 2 should not have been called for ADV2
        mock_campaigns.assert_not_called()

    @pytest.mark.asyncio
    async def test_two_accounts_same_store(self, manager, discovery_cache):
        """Two GMVMAX accounts on same store: one exclusive, one via campaign."""
        store_resp = _make_store_list(
            ("S1", "Store One", "ADV1", "Exclusive-Account", "STATUS_ENABLE"),
        )
        campaigns_resp = {
            "campaigns": [
                {
                    "campaign_id": "C1",
                    "campaign_name": "Second Account",
                    "operation_status": "ENABLE",
                }
            ]
        }
        campaign_info_resp = {"info": {"store_id": "S1"}}

        with (
            patch(
                "tiktok_ads_mcp.tools.gmvmax_store_list.get_gmvmax_store_list",
                new_callable=AsyncMock,
                return_value=store_resp,
            ),
            patch(
                "tiktok_ads_mcp.tools.gmvmax_campaigns.get_gmvmax_campaigns",
                new_callable=AsyncMock,
                return_value=campaigns_resp,
            ),
            patch(
                "tiktok_ads_mcp.tools.gmvmax_campaign_info.get_gmvmax_campaign_info",
                new_callable=AsyncMock,
                return_value=campaign_info_resp,
            ),
        ):
            result = await manager.discover_new_accounts(
                known_store_ids={"S1"},
                authorized_accounts=_make_authorized(["ADV1", "ADV2"]),
            )

        # Both discovered
        adv_ids = {r["advertiser_id"] for r in result}
        assert adv_ids == {"ADV1", "ADV2"}
        # Both cached with same store
        assert discovery_cache.get("ADV1")["store_ids"] == ["S1"]
        assert discovery_cache.get("ADV2")["store_ids"] == ["S1"]
