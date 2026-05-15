"""Shared pytest fixtures.

The currency_cache and fx modules were added when ad-account FX conversion
landed. Tests that pre-date FX awareness assume single-currency USD math and
do not pump advertiser/info responses for a currency lookup. To keep those
tests honest, auto-pre-fill the currency cache with USD for any advertiser ID
the tests touch, so the new code path becomes a no-op.

Tests that specifically exercise the FX path override this by clearing the
caches and injecting their own currency / FX rate.
"""


import pytest

from tiktok_ads_mcp import currency_cache as _ccm
from tiktok_ads_mcp import fx as _fxm


@pytest.fixture(autouse=True)
def assume_usd_currency():
    """Default to USD so legacy tests don't need to mock advertiser/info."""

    class _USDDefault(dict):
        def __contains__(self, key):  # noqa: D401
            return True

        def __getitem__(self, key):
            try:
                return super().__getitem__(key)
            except KeyError:
                return "USD"

        def get(self, key, default=None):
            try:
                return super().__getitem__(key)
            except KeyError:
                return "USD"

    # Swap the module-level dict for one that defaults to USD.
    original = _ccm._currency_cache
    _ccm._currency_cache = _USDDefault()
    try:
        yield _ccm._currency_cache
    finally:
        _ccm._currency_cache = original
        # Restore an empty dict so per-test state never leaks.
        _ccm._currency_cache.clear()


@pytest.fixture(autouse=True)
def reset_fx_cache(tmp_path, monkeypatch):
    """Point fx disk cache at a per-test tmp dir so tests never touch ~/.cache."""
    monkeypatch.setenv("LARK_BOT_CACHE_DIR", str(tmp_path))
    _fxm._reset_cache_for_test()
    yield
    _fxm._reset_cache_for_test()
