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
        """Account moves from one store to another → returned as changed,
        and the OLD store binding is retained (union) so its month-to-date
        spend stays attributable instead of being orphaned."""
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
        # Cache unions old + new store (history preserved, not overwritten)
        assert discovery_cache.get("ADV1")["store_ids"] == ["S_OLD", "S1"]

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


class TestBackfillEmptyStoreIds:
    """Phase 1b: backfill store binding for cached gmvmax with store_ids=[].

    Regression: 2026-05-18 Hiileathy Shilajit advertiser 7633354825347563521
    was cached by Phase 2 with store_ids=[] (had campaigns but
    campaign_info.store_id=""), later became exclusive of HIILEATHY Home but
    Phase 1 didn't pick it up — enrich_with_discovery skipped it → daily
    monthly report missed ~$5K cost. Backfill closes the loop.
    """

    @pytest.mark.asyncio
    async def test_cheap_backfill_from_response(self, manager, discovery_cache):
        """Cached entry with store_ids=[] is fixed by the Phase 1 response."""
        # Pre-seed: ADV2 was cached as gmvmax with no store (Phase 2 path).
        discovery_cache.put("ADV2", store_ids=[], ad_type="gmvmax", ad_name="Old Name")

        # Today's store_list shows ADV2 as exclusive on S1.
        store_resp = _make_store_list(
            ("S1", "Store One", "ADV2", "Now-Exclusive", "STATUS_ENABLE"),
        )
        with patch(
            "tiktok_ads_mcp.tools.gmvmax_store_list.get_gmvmax_store_list",
            new_callable=AsyncMock,
            return_value=store_resp,
        ):
            result = await manager.discover_new_accounts(
                known_store_ids={"S1"},
                authorized_accounts=_make_authorized(["ADV2"]),
            )

        # Main Phase 1 loop (line ~143) catches this as `was_different_store`
        # and returns it; we also want the cache to be correct.
        entry = discovery_cache.get("ADV2")
        assert entry["store_ids"] == ["S1"]
        # Result includes ADV2 once (no duplicates from backfill).
        adv_ids = [r["advertiser_id"] for r in result]
        assert adv_ids.count("ADV2") == 1

    @pytest.mark.asyncio
    async def test_per_advertiser_backfill_when_phase1_missed(
        self, manager, discovery_cache
    ):
        """Phase 1 BC-wide call misses a store; per-advertiser store_list
        finds it. Mirrors the real-world 7633354825347563521 case where the
        BC-wide response from the chosen any_adv_id didn't include Home store.
        """
        # ADV_STALE was cached as gmvmax with empty store_ids (Phase 2 stuck).
        discovery_cache.put(
            "ADV_STALE", store_ids=[], ad_type="gmvmax", ad_name="Five Treasure"
        )

        # Phase 1's BC-wide call (using any_adv_id=ADV1) only returns S1.
        bc_wide_resp = _make_store_list(
            ("S1", "Store One", "ADV1", "Owner-1", "STATUS_ENABLE"),
        )
        # But ADV_STALE's own perspective shows it's exclusive on S_HOME.
        per_adv_resp = _make_store_list(
            ("S1", "Store One", "ADV1", "Owner-1", "STATUS_ENABLE"),
            ("S_HOME", "Home", "ADV_STALE", "DFHDFAESFAS-Home", "STATUS_ENABLE"),
        )

        async def _mock_store_list(_client, adv_id, **_kw):
            # First call: any_adv_id = ADV1 → BC-wide view (missing S_HOME).
            # Second call: adv_id = ADV_STALE → finds S_HOME.
            if adv_id == "ADV_STALE":
                return per_adv_resp
            return bc_wide_resp

        with patch(
            "tiktok_ads_mcp.tools.gmvmax_store_list.get_gmvmax_store_list",
            new=AsyncMock(side_effect=_mock_store_list),
        ):
            result = await manager.discover_new_accounts(
                known_store_ids={"S_HOME"},
                authorized_accounts=_make_authorized(["ADV1", "ADV_STALE"]),
            )

        # ADV_STALE now correctly bound to S_HOME.
        entry = discovery_cache.get("ADV_STALE")
        assert entry["store_ids"] == ["S_HOME"]
        assert entry["ad_name"] == "DFHDFAESFAS-Home"

        # And it appears in result list since S_HOME is a known store.
        adv_ids = {r["advertiser_id"] for r in result}
        assert "ADV_STALE" in adv_ids

    @pytest.mark.asyncio
    async def test_backfill_skips_unknown_type(self, manager, discovery_cache):
        """Cache entries with ad_type='unknown' are NOT touched by backfill
        (only Phase 2's re-validation handles them)."""
        discovery_cache.put("ADV_U", store_ids=[], ad_type="unknown")

        store_resp = _make_store_list(
            ("S1", "Store One", "ADV1", "Owner-1", "STATUS_ENABLE"),
        )
        with patch(
            "tiktok_ads_mcp.tools.gmvmax_store_list.get_gmvmax_store_list",
            new_callable=AsyncMock,
            return_value=store_resp,
        ) as mock_sl:
            await manager.discover_new_accounts(
                known_store_ids={"S1"},
                authorized_accounts=_make_authorized(["ADV1", "ADV_U"]),
            )

        # ADV_U still unknown; backfill only operates on gmvmax type.
        entry = discovery_cache.get("ADV_U")
        assert entry["ad_type"] == "unknown"
        # store_list called once for Phase 1; not called per-adv for ADV_U.
        assert mock_sl.call_count == 1

    @pytest.mark.asyncio
    async def test_backfill_capped_by_batch_limit(self, manager, discovery_cache):
        """When many stale entries exist, only _BACKFILL_BATCH_LIMIT get
        per-advertiser calls per run; the rest carry over to next run."""
        # Seed 15 stale gmvmax entries (limit is 10).
        for i in range(15):
            discovery_cache.put(f"STALE_{i}", store_ids=[], ad_type="gmvmax")

        # BC-wide returns no overlap with stale ids.
        bc_wide_resp = _make_store_list(
            ("S1", "Owner-Store", "ADV_OWNER", "Owner", "STATUS_ENABLE"),
        )

        # All stale ids return a generic empty response from per-adv call.
        empty_resp = {"store_list": []}

        call_count = {"n": 0}

        async def _mock_store_list(_client, adv_id, **_kw):
            call_count["n"] += 1
            if adv_id == "ADV_OWNER":
                return bc_wide_resp
            return empty_resp

        with patch(
            "tiktok_ads_mcp.tools.gmvmax_store_list.get_gmvmax_store_list",
            new=AsyncMock(side_effect=_mock_store_list),
        ):
            await manager.discover_new_accounts(
                known_store_ids={"S1"},
                authorized_accounts=_make_authorized(["ADV_OWNER"]),
            )

        # 1 BC-wide call + 10 per-adv backfill calls = 11.
        assert call_count["n"] == 1 + manager._BACKFILL_BATCH_LIMIT


class TestResurrectWatch:
    """Phase 3: archived/retired account reuse detection (2026-07-08 incident)."""

    def _seed(self, cache, adv_id, **overrides):
        data = cache._load()
        entry = {
            "store_ids": [],
            "ad_type": "archived_gmvmax",
            "ad_name": "seed",
            "discovered_at": "2026-03-01",
            "last_seen": "2026-03-01",
            "banned": False,
        }
        entry.update(overrides)
        data[adv_id] = entry
        cache._save()

    def _info_response(self, statuses):
        """advertiser/info batch response: {adv_id: status}."""
        return {
            "data": {
                "list": [
                    {"advertiser_id": aid, "name": f"Name-{aid}", "status": st}
                    for aid, st in statuses.items()
                ]
            }
        }

    def _exclusive_store_resp(self, adv_id, store_id):
        return {
            "store_list": [
                {
                    "store_id": store_id,
                    "store_name": "Store",
                    "exclusive_authorized_advertiser_info": {
                        "advertiser_id": adv_id,
                        "advertiser_name": f"Name-{adv_id}",
                        "advertiser_status": "STATUS_ENABLE",
                    },
                }
            ]
        }

    @pytest.mark.asyncio
    async def test_archived_exclusive_hit_resurrects(self, manager, discovery_cache):
        """Archived + ENABLE + exclusive binding → RESURRECTED; ban_status_cache
        is never mutated (archived accounts were never retired)."""
        from unittest.mock import MagicMock

        self._seed(discovery_cache, "A")
        ban_cache = MagicMock()
        ban_cache.get_status.return_value = None
        manager.ban_status_cache = ban_cache
        manager.client._make_request = AsyncMock(
            return_value=self._info_response({"A": "STATUS_ENABLE"})
        )

        with patch(
            "tiktok_ads_mcp.tools.gmvmax_store_list.get_gmvmax_store_list",
            new=AsyncMock(return_value=self._exclusive_store_resp("A", "S1")),
        ):
            revived = await manager._resurrect_watch({"S1"}, {"A"})

        assert len(revived) == 1
        assert revived[0]["status"] == "RESURRECTED"
        assert revived[0]["store_ids"] == ["S1"]
        entry = discovery_cache.get("A")
        assert entry["ad_type"] == "gmvmax"
        assert entry["banned"] is False
        # boundary lock: archived path must not mutate ban status
        ban_cache.set_active.assert_not_called()
        ban_cache.set_banned.assert_not_called()

    @pytest.mark.asyncio
    async def test_retired_hit_is_suspect_only(self, manager, discovery_cache):
        """REMOVED_FROM_BC retired account → RESURRECT_SUSPECT, no auto-revert
        (2026-04-24 safety valve, commit 55d2ee8)."""
        from unittest.mock import MagicMock

        self._seed(discovery_cache, "R", ad_type="gmvmax", banned=True)
        ban_cache = MagicMock()
        ban_cache.get_status.return_value = {"status": "REMOVED_FROM_BC"}
        manager.ban_status_cache = ban_cache
        manager.client._make_request = AsyncMock(
            return_value=self._info_response({"R": "STATUS_ENABLE"})
        )

        with patch(
            "tiktok_ads_mcp.tools.gmvmax_store_list.get_gmvmax_store_list",
            new=AsyncMock(return_value=self._exclusive_store_resp("R", "S1")),
        ):
            revived = await manager._resurrect_watch({"S1"}, {"R"})

        assert len(revived) == 1
        assert revived[0]["status"] == "RESURRECT_SUSPECT"
        entry = discovery_cache.get("R")
        assert entry["banned"] is True, "retired entry must stay banned"
        ban_cache.set_active.assert_not_called()

    @pytest.mark.asyncio
    async def test_terminal_status_skipped(self, manager, discovery_cache):
        """API-confirmed terminal states are never re-classified or probed."""
        self._seed(
            discovery_cache,
            "T",
            api_status="STATUS_LIMIT",
            status_checked_at="2026-01-01",
        )
        manager.client._make_request = AsyncMock()
        store_list_mock = AsyncMock()
        with patch(
            "tiktok_ads_mcp.tools.gmvmax_store_list.get_gmvmax_store_list",
            new=store_list_mock,
        ):
            revived = await manager._resurrect_watch({"S1"}, {"T"})
        assert revived == []
        manager.client._make_request.assert_not_called()
        store_list_mock.assert_not_called()

    @pytest.mark.asyncio
    async def test_enable_without_binding_marks_seen(self, manager, discovery_cache):
        """No binding found → not resurrected, last_seen refreshed (anti-starvation)."""
        from datetime import date

        self._seed(discovery_cache, "A", last_seen="2026-03-01")
        manager.client._make_request = AsyncMock(
            return_value=self._info_response({"A": "STATUS_ENABLE"})
        )
        with patch(
            "tiktok_ads_mcp.tools.gmvmax_store_list.get_gmvmax_store_list",
            new=AsyncMock(return_value={"store_list": []}),
        ), patch(
            "tiktok_ads_mcp.tools.gmvmax_campaigns.get_gmvmax_campaigns",
            new=AsyncMock(return_value={"campaigns": []}),
        ):
            revived = await manager._resurrect_watch({"S1"}, {"A"})
        assert revived == []
        entry = discovery_cache.get("A")
        assert entry["ad_type"] == "archived_gmvmax", "must stay archived"
        assert entry["last_seen"] == date.today().isoformat()

    @pytest.mark.asyncio
    async def test_fresh_status_check_skips_classify(self, manager, discovery_cache):
        """status_checked_at == today → no advertiser/info call this run."""
        from datetime import date

        self._seed(
            discovery_cache,
            "A",
            api_status="STATUS_ENABLE",
            status_checked_at=date.today().isoformat(),
        )
        manager.client._make_request = AsyncMock()
        with patch(
            "tiktok_ads_mcp.tools.gmvmax_store_list.get_gmvmax_store_list",
            new=AsyncMock(return_value={"store_list": []}),
        ), patch(
            "tiktok_ads_mcp.tools.gmvmax_campaigns.get_gmvmax_campaigns",
            new=AsyncMock(return_value={"campaigns": []}),
        ):
            await manager._resurrect_watch({"S1"}, {"A"})
        manager.client._make_request.assert_not_called()

    @pytest.mark.asyncio
    async def test_probe_cap(self, manager, discovery_cache):
        """More ENABLE candidates than the cap → only cap-many probed."""
        n = manager._RESURRECT_PROBE_LIMIT + 5
        ids = [f"A{i}" for i in range(n)]
        for aid in ids:
            self._seed(
                discovery_cache,
                aid,
                api_status="STATUS_ENABLE",
                status_checked_at="2026-01-01",
            )
        manager.client._make_request = AsyncMock(
            return_value=self._info_response({a: "STATUS_ENABLE" for a in ids})
        )
        store_list_mock = AsyncMock(return_value={"store_list": []})
        with patch(
            "tiktok_ads_mcp.tools.gmvmax_store_list.get_gmvmax_store_list",
            new=store_list_mock,
        ), patch(
            "tiktok_ads_mcp.tools.gmvmax_campaigns.get_gmvmax_campaigns",
            new=AsyncMock(return_value={"campaigns": []}),
        ):
            await manager._resurrect_watch({"S1"}, set(ids))
        assert store_list_mock.call_count == manager._RESURRECT_PROBE_LIMIT

    @pytest.mark.asyncio
    async def test_classify_failure_does_not_break_discovery(
        self, manager, discovery_cache
    ):
        """advertiser/info batch exception → watch degrades, discovery survives."""
        self._seed(discovery_cache, "A")
        manager.client._make_request = AsyncMock(side_effect=RuntimeError("boom"))
        revived = await manager._resurrect_watch({"S1"}, {"A"})
        assert revived == []

    @pytest.mark.asyncio
    async def test_unauthorized_candidates_excluded(self, manager, discovery_cache):
        """Candidates outside authorized_ids never reach the classify batch
        (REMOVED_FROM_BC accounts 40001-poison the whole batch — PoC 2026-07-08)."""
        self._seed(discovery_cache, "A")
        manager.client._make_request = AsyncMock()
        revived = await manager._resurrect_watch({"S1"}, set())
        assert revived == []
        manager.client._make_request.assert_not_called()

    @pytest.mark.asyncio
    async def test_nonexclusive_reuse_via_campaigns(self, manager, discovery_cache):
        """Tier 2: exclusive miss but ACTIVE campaign exists → store from
        campaign_info, entry resurrected (non-exclusive reuse blind spot)."""
        self._seed(discovery_cache, "A")
        manager.client._make_request = AsyncMock(
            return_value=self._info_response({"A": "STATUS_ENABLE"})
        )
        with patch(
            "tiktok_ads_mcp.tools.gmvmax_store_list.get_gmvmax_store_list",
            new=AsyncMock(return_value={"store_list": []}),
        ), patch(
            "tiktok_ads_mcp.tools.gmvmax_campaigns.get_gmvmax_campaigns",
            new=AsyncMock(
                return_value={
                    "campaigns": [
                        {
                            "campaign_id": "C1",
                            "campaign_name": "Camp",
                            "operation_status": "ENABLE",
                        }
                    ]
                }
            ),
        ), patch(
            "tiktok_ads_mcp.tools.gmvmax_campaign_info.get_gmvmax_campaign_info",
            new=AsyncMock(return_value={"info": {"store_id": "S1"}}),
        ):
            revived = await manager._resurrect_watch({"S1"}, {"A"})
        assert len(revived) == 1
        assert revived[0]["status"] == "RESURRECTED"
        assert revived[0]["evidence"] == "active gmvmax campaign"
        assert discovery_cache.get("A")["ad_type"] == "gmvmax"
