"""Get GMV Max Reports Tool

Uses the dedicated GMVMAX report endpoint: GET /gmv_max/report/get/
This endpoint supports rich metrics including cost, orders, ROI, gross_revenue, net_cost,
and product/ad performance metrics.

Available dimensions: advertiser_id, stat_time_day, item_id
Available metrics:
  - Core: cost, orders, cost_per_order, gross_revenue, roi, net_cost
  - Product: product_impressions, product_clicks, product_click_rate
  - Ad performance: ad_click_rate, ad_conversion_rate
  - Video: ad_video_view_rate_2s, ad_video_view_rate_6s,
           ad_video_view_rate_p25, ad_video_view_rate_p50,
           ad_video_view_rate_p75, ad_video_view_rate_p100
  - Status: creative_delivery_status
Filtering: campaign_ids, item_group_ids
"""

import json
import logging
import os
from typing import Any, Dict, List, Optional

from ..currency_cache import get_currency as _get_currency
from ..fx import get_rate_to_usd as _get_rate_to_usd

logger = logging.getLogger(__name__)

# Three-mode rollout flag (env GMVMAX_ALIGNED_MODE), mirrors active_roster:
#   off    → no-op, original adv-tz path (default; safe for deploy)
#   shadow → both paths run, journal diff logged, original returned
#   on     → aligned path returned; original on aligned-path exception (fail-soft)
_ALIGNED_OFF = "off"
_ALIGNED_SHADOW = "shadow"
_ALIGNED_ON = "on"


def _get_aligned_mode() -> str:
    """Read GMVMAX_ALIGNED_MODE env. Defaults to 'off' (safe-by-default)."""
    return (
        os.environ.get("GMVMAX_ALIGNED_MODE", _ALIGNED_OFF).strip().lower()
        or _ALIGNED_OFF
    )


# Metrics whose values are monetary (need FX conversion when advertiser
# currency != USD). cost_per_order is monetary; roi is dimensionless
# (gmv/cost — rate cancels); net_cost is cash-out denominator.
_MONETARY_METRICS = {"cost", "gross_revenue", "net_cost", "cost_per_order"}


async def _apply_fx_to_rows(
    client,
    advertiser_id: str,
    rows: List[Dict],
    fallback_date: str,
) -> tuple:
    """FX-convert monetary metric values in `rows` from advertiser-native
    currency to USD. Reusable by both the non-aligned and tz-aligned report
    paths so the conversion semantics stay identical.

    Per-row rate when `dimensions["stat_time_day"]` is present (handles ranges
    spanning multiple days with daily-resolution FX); else uses a fallback
    rate resolved from `fallback_date` (typically end_date — most recent rate
    best represents currently-active spend).

    Returns: (converted_rows, source_currency_upper). For USD advertisers
    rows pass through unchanged with rate=1.0 (no HTTP call to Frankfurter).
    """
    currency = (await _get_currency(client, advertiser_id)) or "USD"
    source_currency = currency.upper()
    if source_currency == "USD":
        return rows, source_currency

    fallback_rate = await _get_rate_to_usd(currency, fallback_date)

    converted: List[Dict] = []
    for row in rows:
        dims = row.get("dimensions", {})
        metrics = dict(row.get("metrics", {}))  # copy — caller-safe

        row_day = dims.get("stat_time_day", "")
        if row_day:
            row_day = str(row_day).split(" ")[0]
            rate = await _get_rate_to_usd(currency, row_day)
        else:
            rate = fallback_rate

        for k in list(metrics.keys()):
            if k in _MONETARY_METRICS:
                raw = metrics.get(k)
                if raw in (None, ""):
                    continue
                try:
                    metrics[k] = str(round(float(raw) * rate, 4))
                except (ValueError, TypeError):
                    # Non-numeric (e.g. creative_delivery_status if ever
                    # miscategorized as monetary) — leave as-is.
                    pass

        converted.append({"dimensions": dims, "metrics": metrics})

    return converted, source_currency


# Full metrics available on /gmv_max/report/get/
GMVMAX_DEFAULT_METRICS = [
    "cost",
    "orders",
    "cost_per_order",
    "gross_revenue",
    "roi",
    "net_cost",
]

GMVMAX_ALL_METRICS = GMVMAX_DEFAULT_METRICS + [
    "creative_delivery_status",
    "product_impressions",
    "product_clicks",
    "product_click_rate",
    "ad_click_rate",
    "ad_conversion_rate",
    "ad_video_view_rate_2s",
    "ad_video_view_rate_6s",
    "ad_video_view_rate_p25",
    "ad_video_view_rate_p50",
    "ad_video_view_rate_p75",
    "ad_video_view_rate_p100",
]

GMVMAX_DEFAULT_DIMENSIONS = ["advertiser_id", "stat_time_day"]


def _totals_for_diff(rows: List[Dict]) -> tuple:
    """Sum cost / gross_revenue / orders across `rows` for shadow-diff logging.

    Returns (cost, gmv, orders). Non-numeric / missing metric values are
    silently treated as 0 so logging never crashes the request path.
    """
    cost = gmv = 0.0
    orders = 0
    for r in rows:
        m = r.get("metrics", {})
        try:
            cost += float(m.get("cost", 0) or 0)
        except (ValueError, TypeError):
            pass
        try:
            gmv += float(m.get("gross_revenue", 0) or 0)
        except (ValueError, TypeError):
            pass
        try:
            orders += int(float(m.get("orders", 0) or 0))
        except (ValueError, TypeError):
            pass
    return cost, gmv, orders


def _log_shadow_diff(
    advertiser_id: str,
    start_date: str,
    end_date: str,
    dimensions: List[str],
    original: Dict,
    aligned: Dict,
) -> None:
    """Emit a journal line comparing original (adv-tz-day) vs aligned
    (shop-tz-day) fetch totals.

    Used by the dispatcher in shadow mode for 24h validation: operators grep
    `aligned_shadow` in `journalctl -u morning-briefing` to see per-advertiser
    diffs before flipping `GMVMAX_ALIGNED_MODE=on`. Aggregates to group totals
    so a multi-row report still fits in a single log line.

    Failure-isolated: any exception is caught so a logging bug can never break
    the request path itself.
    """
    try:
        orig_cost, orig_gmv, orig_orders = _totals_for_diff(original.get("list", []))
        aln_cost, aln_gmv, aln_orders = _totals_for_diff(aligned.get("list", []))
        logger.info(
            f"[aligned_shadow] adv=...{advertiser_id[-6:]} "
            f"dates={start_date}~{end_date} dims={dimensions} "
            f"rows_orig={len(original.get('list', []))} "
            f"rows_aligned={len(aligned.get('list', []))} "
            f"cost_orig={orig_cost:.2f} cost_aligned={aln_cost:.2f} "
            f"cost_delta={aln_cost - orig_cost:+.2f} "
            f"gmv_orig={orig_gmv:.2f} gmv_aligned={aln_gmv:.2f} "
            f"gmv_delta={aln_gmv - orig_gmv:+.2f} "
            f"orders_orig={orig_orders} orders_aligned={aln_orders} "
            f"orders_delta={aln_orders - orig_orders:+d}"
        )
    except Exception as e:  # noqa: BLE001 — never let logging crash request
        logger.warning(f"[aligned_shadow] diff log failed adv={advertiser_id}: {e}")


async def _get_gmvmax_reports_original(
    client,
    advertiser_id: str,
    start_date: str,
    end_date: str,
    store_ids: Optional[List[str]] = None,
    dimensions: Optional[List[str]] = None,
    metrics: Optional[List[str]] = None,
    filtering: Optional[Dict] = None,
    page: int = 1,
    page_size: int = 1000,
) -> Dict[str, Any]:
    """Original (advertiser-tz interpreted) report path. Kept separately so
    the dispatcher in `get_gmvmax_reports` can route to it explicitly when
    `GMVMAX_ALIGNED_MODE=off` or when fail-soft fallback fires.

    Identical pre-refactor semantics; new code should NOT call this directly —
    go through `get_gmvmax_reports` which handles tz alignment + flags.
    """
    if dimensions is None:
        dimensions = GMVMAX_DEFAULT_DIMENSIONS
    if metrics is None:
        metrics = GMVMAX_DEFAULT_METRICS

    params = {
        "advertiser_id": advertiser_id,
        "start_date": start_date,
        "end_date": end_date,
        "dimensions": json.dumps(dimensions),
        "metrics": json.dumps(metrics),
        "page": page,
        "page_size": page_size,
    }

    if store_ids:
        params["store_ids"] = json.dumps(store_ids)

    if filtering:
        params["filtering"] = json.dumps(filtering)

    response = await client._make_request("GET", "gmv_max/report/get/", params)
    if response.get("code") != 0:
        raise Exception(
            f"API returned code {response.get('code')}: "
            f"{response.get('message', 'Unknown error')}"
        )
    data = response.get("data", {})
    raw_rows = data.get("list", [])
    converted_rows, source_currency = await _apply_fx_to_rows(
        client, advertiser_id, raw_rows, fallback_date=end_date
    )
    return {
        "page_info": data.get("page_info", {}),
        "list": converted_rows,
        "currency": "USD",
        "source_currency": source_currency,
    }


# Dimensions that mean "user already wants per-time breakdown". The aligned
# path collapses hours into the supplied non-time dimensions, so if a caller
# already groups by stat_time_day / stat_time_hour we must NOT enable aligned
# (would double-aggregate or mis-bucket). Bypass to original path keeps the
# MCP tool surface flexible for analytical queries beyond the lark-bot wrappers.
_TIME_DIMENSIONS = {"stat_time_day", "stat_time_hour"}


async def get_gmvmax_reports(
    client,
    advertiser_id: str,
    start_date: str,
    end_date: str,
    store_ids: Optional[List[str]] = None,
    dimensions: Optional[List[str]] = None,
    metrics: Optional[List[str]] = None,
    filtering: Optional[Dict] = None,
    page: int = 1,
    page_size: int = 1000,
    shop_tz: Optional[str] = None,
    **kwargs,
) -> Dict[str, Any]:
    """Get GMV Max performance reports via dedicated /gmv_max/report/get/ endpoint.

    Dispatcher: routes to the advertiser-tz original path or the shop-tz aligned
    path based on `GMVMAX_ALIGNED_MODE` env (off/shadow/on) and whether
    `shop_tz` is supplied and differs from the advertiser's native timezone.

    The aligned path solves the cross-tz misalignment bug where TikTok's
    /gmv_max/report/get/ interprets start_date/end_date in advertiser_tz,
    causing THB-Bangkok advertisers bound to PT shops to miss ~14h of PT
    evening data per query. See plan/dynamic-spinning-seahorse.md.

    Args:
        advertiser_id: TikTok advertiser ID
        start_date: Start date (YYYY-MM-DD) — in shop_tz when aligned path
                    runs; otherwise interpreted by API in advertiser_tz
        end_date: End date (YYYY-MM-DD), same tz semantics as start_date
        store_ids: Optional list of TikTok Shop store IDs to filter by
        dimensions: Grouping dimensions (default: advertiser_id + stat_time_day).
                    Also supports: item_id, campaign_id
        metrics: Metrics to retrieve (default: cost, orders, cost_per_order,
                 gross_revenue, roi, net_cost). Also supports:
                 creative_delivery_status, product_impressions, product_clicks,
                 product_click_rate, ad_click_rate, ad_conversion_rate,
                 ad_video_view_rate_2s/6s/p25/p50/p75/p100
        filtering: Optional filter dict, supports keys:
                   campaign_ids (list of str), item_group_ids (list of str)
        page: Page number (default 1)
        page_size: Page size (default 1000)
        shop_tz: Optional shop timezone (IANA name, e.g. "America/Los_Angeles").
                 When set AND `GMVMAX_ALIGNED_MODE != "off"` AND advertiser_tz
                 != shop_tz AND dimensions don't include stat_time_*, the
                 aligned path runs. Otherwise original path runs.
    """
    if not advertiser_id:
        raise ValueError("advertiser_id is required")
    if not start_date or not end_date:
        raise ValueError("start_date and end_date are required")
    if page < 1:
        raise ValueError("page must be >= 1")
    if page_size < 1 or page_size > 1000:
        raise ValueError("page_size must be between 1 and 1000")

    if dimensions is None:
        dimensions = GMVMAX_DEFAULT_DIMENSIONS
    if metrics is None:
        metrics = GMVMAX_DEFAULT_METRICS

    # ── Dispatcher ──────────────────────────────────────────────────────
    mode = _get_aligned_mode()
    need_align = False
    if shop_tz and mode != _ALIGNED_OFF and not (_TIME_DIMENSIONS & set(dimensions)):
        # Lazy imports keep the original path's surface area minimal and avoid
        # paying tz-cache/timezone import cost when shop_tz is not supplied.
        from zoneinfo import ZoneInfo

        from ..timezone import parse_tz as _parse_tz
        from ..tz_cache import get_ad_tz as _get_ad_tz_inner

        shop_zone = _parse_tz(shop_tz)
        # Guard: parse_tz silently returns UTC on malformed string. If caller
        # passed something other than "UTC" and we got UTC back, that's an
        # operator typo — log + skip alignment (safer than UTC drift).
        if shop_tz.upper() != "UTC" and shop_zone == ZoneInfo("UTC"):
            logger.warning(
                f"[aligned] shop_tz={shop_tz!r} parsed to UTC (silent fallback) "
                f"— skipping align for advertiser={advertiser_id}"
            )
        else:
            ad_zone = await _get_ad_tz_inner(client, advertiser_id)
            need_align = ad_zone != shop_zone

    try:
        if not need_align:
            return await _get_gmvmax_reports_original(
                client,
                advertiser_id,
                start_date,
                end_date,
                store_ids=store_ids,
                dimensions=dimensions,
                metrics=metrics,
                filtering=filtering,
                page=page,
                page_size=page_size,
            )

        # Lazy import to avoid module-load cycle (aligned imports from us)
        from .gmvmax_report_aligned import get_gmvmax_reports_aligned

        if mode == _ALIGNED_SHADOW:
            original = await _get_gmvmax_reports_original(
                client,
                advertiser_id,
                start_date,
                end_date,
                store_ids=store_ids,
                dimensions=dimensions,
                metrics=metrics,
                filtering=filtering,
                page=page,
                page_size=page_size,
            )
            try:
                aligned = await get_gmvmax_reports_aligned(
                    client,
                    advertiser_id,
                    start_date,
                    end_date,
                    store_ids or [],
                    dimensions,
                    metrics,
                    shop_tz=shop_tz,
                    filtering=filtering,
                    page_size=page_size,
                )
                _log_shadow_diff(
                    advertiser_id,
                    start_date,
                    end_date,
                    dimensions,
                    original,
                    aligned,
                )
            except Exception as e:  # noqa: BLE001 — shadow must never break request
                logger.warning(
                    f"[aligned_shadow] adv={advertiser_id[-6:]} "
                    f"aligned-path failed (shadow only, original returned): {e}"
                )
            return original

        # mode == "on"
        try:
            return await get_gmvmax_reports_aligned(
                client,
                advertiser_id,
                start_date,
                end_date,
                store_ids or [],
                dimensions,
                metrics,
                shop_tz=shop_tz,
                filtering=filtering,
                page_size=page_size,
            )
        except Exception as e:  # noqa: BLE001 — fail-soft: never break morning_brief
            logger.error(
                f"[aligned] adv={advertiser_id[-6:]} aligned-path failed, "
                f"falling back to original: {e}"
            )
            return await _get_gmvmax_reports_original(
                client,
                advertiser_id,
                start_date,
                end_date,
                store_ids=store_ids,
                dimensions=dimensions,
                metrics=metrics,
                filtering=filtering,
                page=page,
                page_size=page_size,
            )

    except Exception as e:
        logger.error(f"Failed to get GMV Max reports: {e}")
        raise
