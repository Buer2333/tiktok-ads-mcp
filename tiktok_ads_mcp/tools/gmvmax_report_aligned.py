"""GMVMAX Report with timezone alignment.

Fetches hourly GMVMAX data and re-aggregates it to match a shop-timezone day,
regardless of the ad account's native timezone setting.
"""

import json
import logging
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

from mcp_retry import api_retry

from ..client import (
    TikTokAdsClient,
    TikTokIncompleteDataError,
    TikTokRateLimitError,
)
from ..currency_cache import get_currency as _get_currency
from ..fx import get_rate_to_usd as _get_rate_to_usd
from ..timezone import day_utc_range, hour_to_utc, native_dates_for_day, parse_tz
from ..tz_cache import get_ad_tz as _get_ad_tz

logger = logging.getLogger(__name__)

ALIGNED_DEFAULT_METRICS = ["cost", "gross_revenue", "orders"]

# Metrics whose values are monetary (need FX conversion when advertiser
# currency != USD). Non-monetary metrics like `orders` pass through unchanged.
_MONETARY_METRICS = {"cost", "gross_revenue", "net_cost"}

# TikTok hourly reports typically lag 1-2h behind real time; tolerate that
# many missing hours before flagging a response as incomplete.
_HOURS_LAG_TOLERANCE = 2


def _expected_hours(date_str: str, shop_zone, now_utc) -> int:
    """How many shop-tz hours of `date_str` should have data by `now_utc`.

    Returns 24 for past days, 0 for future, hours-elapsed for today.
    """
    start_utc, end_utc = day_utc_range(date_str, shop_zone)
    if now_utc >= end_utc:
        return 24
    if now_utc <= start_utc:
        return 0
    return int((now_utc - start_utc).total_seconds() // 3600)


async def _fetch_hourly(
    client: TikTokAdsClient,
    advertiser_id: str,
    date_str: str,
    store_ids: List[str],
    metrics: List[str],
) -> List[Dict]:
    """Fetch one day of hourly GMVMAX data."""
    params = {
        "advertiser_id": advertiser_id,
        "start_date": date_str,
        "end_date": date_str,
        "dimensions": json.dumps(["advertiser_id", "stat_time_hour"]),
        "metrics": json.dumps(metrics),
        "store_ids": json.dumps(store_ids),
        "page": 1,
        "page_size": 1000,
    }
    response = await client._make_request("GET", "gmv_max/report/get/", params)
    if response.get("code") == 0:
        return response.get("data", {}).get("list", [])
    # Non-0 code: surface so caller (and @api_retry) can react instead of
    # silently treating as "no data". 2026-04-21 bug: swallowed non-0 responses
    # caused 4/5 groups to under-report today cost by $50-$1610 simultaneously.
    raise Exception(
        f"gmv_max/report/get/ returned code={response.get('code')} "
        f"msg={response.get('message')!r} for advertiser={advertiser_id} date={date_str}"
    )


# Cap pagination defensively. With dimensions=[*user_dims, "stat_time_hour"]
# worst-case row count for a single native date is items × 24h. AMSOLAR-class
# advertisers (~250 items × 24h = ~6000 rows) need ~7 pages at page_size=1000;
# 20-page ceiling gives 5× headroom before fail-fast.
_MAX_HOURLY_PAGES = 20


async def _fetch_hourly_by_dim(
    client: TikTokAdsClient,
    advertiser_id: str,
    date_str: str,
    store_ids: List[str],
    dimensions: List[str],
    metrics: List[str],
    filtering: Optional[Dict] = None,
    page_size: int = 1000,
) -> List[Dict]:
    """Fetch one native-date of GMVMAX data at hourly granularity, with arbitrary
    extra dimensions (e.g. ``["campaign_id"]`` or ``["item_id"]``).

    Appends ``stat_time_hour`` to `dimensions` so each returned row carries the
    hour it landed in — needed downstream for shop-tz UTC slicing.

    Paginates internally up to ``_MAX_HOURLY_PAGES``. With campaign/item × 24h
    expansion, single-page (1000 rows) is sometimes insufficient for high-item
    advertisers; pagination is therefore mandatory rather than caller-driven.

    Mirrors ``_fetch_hourly`` error semantics: non-zero `code` raises so
    ``@api_retry`` and ``TikTokIncompleteDataError`` flow stays intact.
    """
    full_dims = list(dimensions) + ["stat_time_hour"]
    base_params = {
        "advertiser_id": advertiser_id,
        "start_date": date_str,
        "end_date": date_str,
        "dimensions": json.dumps(full_dims),
        "metrics": json.dumps(metrics),
        "store_ids": json.dumps(store_ids),
        "page_size": page_size,
    }
    if filtering:
        base_params["filtering"] = json.dumps(filtering)

    all_rows: List[Dict] = []
    for page in range(1, _MAX_HOURLY_PAGES + 1):
        params = dict(base_params, page=page)
        response = await client._make_request("GET", "gmv_max/report/get/", params)
        if response.get("code") != 0:
            raise Exception(
                f"gmv_max/report/get/ (hourly_by_dim) returned "
                f"code={response.get('code')} msg={response.get('message')!r} "
                f"for advertiser={advertiser_id} date={date_str} page={page}"
            )
        data = response.get("data", {})
        rows = data.get("list", []) or []
        all_rows.extend(rows)

        page_info = data.get("page_info", {}) or {}
        total_page = int(page_info.get("total_page", page) or page)
        if page >= total_page or not rows:
            break
    else:
        # Hit the safety cap. Surface explicitly so operators see a truncated
        # response in logs rather than silently dropping rows.
        raise Exception(
            f"gmv_max/report/get/ (hourly_by_dim) exceeded _MAX_HOURLY_PAGES="
            f"{_MAX_HOURLY_PAGES} for advertiser={advertiser_id} date={date_str} "
            f"dims={full_dims} — investigate row explosion"
        )

    return all_rows


def _aggregate_by_dims(
    rows: List[Dict],
    dimensions: List[str],
    metrics: List[str],
    start_utc: datetime,
    end_utc: datetime,
    ad_zone,
    now_utc: datetime,
) -> tuple:
    """Filter `rows` (hourly granular) to the shop-tz UTC window then aggregate
    by tuple(`dimensions`).

    Group key = tuple of dim values (in `dimensions` order, excluding
    ``stat_time_hour`` which only exists to enable UTC slicing). Numeric metric
    values are summed across hours within the window.

    Returns ``(aggregated_rows, last_row_utc, hours_included)``:
      - ``aggregated_rows``: list of ``{"dimensions": {...}, "metrics": {...}}``
        matching the per-row shape of ``get_gmvmax_reports``. Metric values are
        stringified to match upstream serialization.
      - ``last_row_utc``: latest UTC datetime accepted into the aggregate (for
        completeness lag check downstream); ``None`` if no rows accepted.
      - ``hours_included``: count of hourly rows that landed in the window.

    Limitation: sums all metric values as floats. Additive metrics (cost,
    gross_revenue, orders, net_cost, *_impressions, *_clicks) sum correctly.
    Ratio metrics (roi, cost_per_order, *_rate) cannot be meaningfully summed
    — callers must recompute from base metrics post-aggregation.
    """
    # Per-group aggregator and dim-dict snapshot for round-trip
    aggregated: Dict[tuple, Dict[str, float]] = {}
    dim_snapshots: Dict[tuple, Dict[str, Any]] = {}
    last_row_utc: Optional[datetime] = None
    hours_included = 0

    for row in rows:
        row_dims = row.get("dimensions", {})
        hour_str = row_dims.get("stat_time_hour", "")
        # TikTok occasionally returns "-" placeholder for hours with no data
        # (2026-05-12 regression: strptime raised → @api_retry burned 4min).
        if not hour_str or hour_str == "-":
            continue

        utc_dt = hour_to_utc(hour_str, ad_zone)

        # Window check: within shop-tz UTC day AND not future-hour
        if not (start_utc <= utc_dt < end_utc and utc_dt <= now_utc):
            continue

        key = tuple(row_dims.get(d, "") for d in dimensions)
        if key not in aggregated:
            aggregated[key] = {m: 0.0 for m in metrics}
            dim_snapshots[key] = {d: row_dims.get(d, "") for d in dimensions}

        row_metrics = row.get("metrics", {})
        for m in metrics:
            val = row_metrics.get(m, "0")
            if val in (None, ""):
                continue
            try:
                aggregated[key][m] += float(val)
            except (ValueError, TypeError):
                # Non-numeric (e.g. creative_delivery_status) — skip silently
                # so the group still aggregates the numeric metrics it can.
                pass

        hours_included += 1
        if last_row_utc is None or utc_dt > last_row_utc:
            last_row_utc = utc_dt

    # Build output rows. Stringify metric values for shape parity with the
    # non-aligned `get_gmvmax_reports` path (callers parse with `float(...)`).
    out: List[Dict] = []
    for key, agg_metrics in aggregated.items():
        out.append(
            {
                "dimensions": dim_snapshots[key],
                "metrics": {m: str(round(v, 4)) for m, v in agg_metrics.items()},
            }
        )

    return out, last_row_utc, hours_included


@api_retry(
    max_attempts=3,
    min_wait=3,
    max_wait=15,
    retryable_exceptions=(TikTokRateLimitError, TikTokIncompleteDataError),
)
async def get_gmvmax_reports_aligned(
    client: TikTokAdsClient,
    advertiser_id: str,
    start_date: str,
    end_date: str,
    store_ids: List[str],
    dimensions: List[str],
    metrics: List[str],
    shop_tz: str,
    filtering: Optional[Dict] = None,
    page_size: int = 1000,
) -> Dict[str, Any]:
    """Shop-tz-aligned variant of ``get_gmvmax_reports`` for arbitrary dimensions
    (``campaign_id`` / ``item_id``).

    Solves the bug where TikTok's ``/gmv_max/report/get/`` interprets
    ``start_date``/``end_date`` in **advertiser_tz**, causing a window
    misalignment for cross-tz advertisers (e.g. THB Bangkok adv vs PT shop —
    PT-evening spend leaks into adv's next-day bucket).

    Mechanism:
      1. Resolve advertiser native TZ via cached ``/advertiser/info/``.
      2. For each shop-tz day in ``[start_date, end_date]``: compute the 1-2
         advertiser-tz native dates that overlap, fetch hourly rows
         (``stat_time_hour`` dimension) for each in parallel.
      3. Slice rows to the shop-tz day's UTC window via ``hour_to_utc``.
      4. Aggregate by ``tuple(dimensions)`` (drop ``stat_time_hour``).
      5. FX-convert monetary metrics via shared ``_apply_fx_to_rows``.

    Returns the exact same dict shape as ``get_gmvmax_reports`` so wrappers
    in ``core/tiktok_api_mcp.py`` and downstream callers stay unchanged.

    Args:
      client: TikTok API client
      advertiser_id: advertiser to query
      start_date, end_date: shop-timezone date range (YYYY-MM-DD, inclusive)
      store_ids: TikTok Shop store IDs
      dimensions: dims to aggregate by (NOT including stat_time_hour, which
                  is appended internally for slicing then dropped)
      metrics: numeric metric names to sum across hours
      shop_tz: shop timezone (IANA name) — used for UTC-window slicing
      filtering: optional dict with campaign_ids / item_group_ids
      page_size: per-page row cap for hourly fetch (default 1000)

    Raises:
      TikTokIncompleteDataError: hourly endpoint truncated mid-window (caught
        by ``@api_retry`` so transient lag self-heals).
    """
    # Lazy import to avoid circular dep between gmvmax_reports and aligned modules
    from .gmvmax_reports import _apply_fx_to_rows

    shop_zone = parse_tz(shop_tz)
    ad_zone = await _get_ad_tz(client, advertiser_id)
    now_utc = datetime.now(timezone.utc)

    # Walk each shop-tz day in the range. For single-day queries (the common
    # case for morning_brief / material_report) this loop runs once.
    start_d = datetime.strptime(start_date, "%Y-%m-%d").date()
    end_d = datetime.strptime(end_date, "%Y-%m-%d").date()
    if end_d < start_d:
        raise ValueError(f"end_date {end_date} < start_date {start_date}")

    import asyncio as _asyncio

    all_aggregated: List[Dict] = []
    last_row_utc_overall: Optional[datetime] = None
    total_cost_for_completeness = 0.0

    current = start_d
    while current <= end_d:
        d_str = current.strftime("%Y-%m-%d")
        start_utc, end_utc = day_utc_range(d_str, shop_zone)
        native_dates = native_dates_for_day(d_str, shop_zone, ad_zone)

        # Fetch all native_dates concurrently (1-2 fetches typical; client
        # Semaphore(5) caps actual concurrency under existing rate budget).
        fetched_per_native = await _asyncio.gather(
            *(
                _fetch_hourly_by_dim(
                    client,
                    advertiser_id,
                    nd,
                    store_ids,
                    dimensions,
                    metrics,
                    filtering=filtering,
                    page_size=page_size,
                )
                for nd in native_dates
            )
        )
        merged_rows: List[Dict] = []
        for rows in fetched_per_native:
            merged_rows.extend(rows)

        day_agg, day_last_utc, _ = _aggregate_by_dims(
            merged_rows,
            dimensions,
            metrics,
            start_utc,
            end_utc,
            ad_zone,
            now_utc,
        )
        all_aggregated.extend(day_agg)
        if day_last_utc is not None and (
            last_row_utc_overall is None or day_last_utc > last_row_utc_overall
        ):
            last_row_utc_overall = day_last_utc
        for r in day_agg:
            try:
                total_cost_for_completeness += float(r["metrics"].get("cost", 0) or 0)
            except (ValueError, TypeError):
                pass

        current += timedelta(days=1)

    # Completeness check: same logic as get_gmvmax_report_aligned but anchored
    # to the last shop-tz day in the range. Rate-limit truncation manifests as
    # the hourly endpoint stopping mid-window; raising
    # `TikTokIncompleteDataError` triggers @api_retry above.
    if total_cost_for_completeness > 0 and last_row_utc_overall is not None:
        last_day_end_utc = day_utc_range(end_date, shop_zone)[1]
        last_full_hour = (now_utc - timedelta(hours=1)).replace(
            minute=0, second=0, microsecond=0
        )
        expected_last = min(last_full_hour, last_day_end_utc - timedelta(hours=1))
        lag_h = (expected_last - last_row_utc_overall).total_seconds() / 3600
        if lag_h > _HOURS_LAG_TOLERANCE:
            raise TikTokIncompleteDataError(
                f"GMVMAX-aligned advertiser={advertiser_id} "
                f"dates={start_date}~{end_date} stores={store_ids} dims={dimensions}: "
                f"latest_row={last_row_utc_overall.strftime('%Y-%m-%d %H:%M UTC')} "
                f"lags {lag_h:.1f}h behind expected="
                f"{expected_last.strftime('%Y-%m-%d %H:%M UTC')} "
                f"(tol={_HOURS_LAG_TOLERANCE}, cost=${total_cost_for_completeness:.2f}) — "
                f"likely token rate-limit truncated mid-window"
            )

    # FX: convert monetary metric values via the shared helper used by the
    # non-aligned path. Stays in lockstep with non-aligned FX semantics
    # (per-row by stat_time_day when present; else fallback to end_date rate).
    converted_rows, source_currency = await _apply_fx_to_rows(
        client, advertiser_id, all_aggregated, fallback_date=end_date
    )

    return {
        "page_info": {
            "total_number": len(converted_rows),
            "total_page": 1,
            "page": 1,
            "page_size": page_size,
        },
        "list": converted_rows,
        "currency": "USD",
        "source_currency": source_currency,
    }


@api_retry(
    max_attempts=3,
    min_wait=3,
    max_wait=15,
    retryable_exceptions=(TikTokRateLimitError, TikTokIncompleteDataError),
)
async def get_gmvmax_report_aligned(
    client: TikTokAdsClient,
    advertiser_id: str,
    date: str,
    store_ids: List[str],
    shop_tz: str = "America/Los_Angeles",
    metrics: Optional[List[str]] = None,
) -> Dict[str, Any]:
    """Get GMVMAX report aligned to shop timezone.

    Fetches hourly data in ad-account native timezone, converts each hour to UTC,
    then filters to only include hours that fall within the shop-timezone day.

    Args:
        client: TikTok API client
        advertiser_id: TikTok advertiser ID
        date: Date string (YYYY-MM-DD) in shop timezone
        store_ids: TikTok Shop store IDs
        shop_tz: Shop timezone (IANA name, default America/Los_Angeles)
        metrics: Metrics to aggregate (default: cost, gross_revenue, orders)

    Returns:
        Dict with date, timezones, aggregated metrics, ROI, and hours included.
    """
    if metrics is None:
        metrics = list(ALIGNED_DEFAULT_METRICS)

    shop_zone = parse_tz(shop_tz)
    ad_zone = await _get_ad_tz(client, advertiser_id)

    start_utc, end_utc = day_utc_range(date, shop_zone)
    now_utc = datetime.now(timezone.utc)

    # Determine which native dates to query
    dates_to_query = native_dates_for_day(date, shop_zone, ad_zone)

    # Fetch hourly data for each native date concurrently. Cross-tz queries
    # usually span 2 native dates; client's asyncio.Semaphore(5) keeps real
    # concurrency inside the existing rate-limit budget.
    import asyncio as _asyncio

    fetched = await _asyncio.gather(
        *(
            _fetch_hourly(client, advertiser_id, d, store_ids, metrics)
            for d in dates_to_query
        )
    )
    all_rows: List[Dict] = []
    for rows in fetched:
        all_rows.extend(rows)

    # Filter and aggregate
    aggregated: Dict[str, float] = {m: 0.0 for m in metrics}
    hours_included = 0
    last_row_utc: Optional[datetime] = None

    for row in all_rows:
        dims = row.get("dimensions", {})
        hour_str = dims.get("stat_time_hour", "")
        # TikTok occasionally returns "-" as a placeholder when an advertiser
        # has no hourly data for the queried period. Skip those rows instead
        # of letting strptime raise (which fires transient-error retry chains
        # — empirically +4 min runtime per ~30 rows; 2026-05-12 team_lead
        # regression root cause).
        if not hour_str or hour_str == "-":
            continue

        utc_dt = hour_to_utc(hour_str, ad_zone)

        # Must be within shop-day UTC range and not in the future
        if start_utc <= utc_dt < end_utc and utc_dt <= now_utc:
            row_metrics = row.get("metrics", {})
            for m in metrics:
                val = row_metrics.get(m, "0")
                try:
                    aggregated[m] += float(val)
                except (ValueError, TypeError):
                    pass
            hours_included += 1
            if last_row_utc is None or utc_dt > last_row_utc:
                last_row_utc = utc_dt

    # FX-convert monetary metrics to USD. Done after the completeness check
    # below would have run on raw cost, so the lag detection still triggers on
    # the same magnitudes operations have historically seen. Conversion happens
    # before the response is returned so cache writers (account_manager) and
    # all downstream readers see USD.
    currency = (await _get_currency(client, advertiser_id)) or "USD"

    cost = aggregated.get("cost", 0.0)
    gmv = aggregated.get("gross_revenue", 0.0)
    roi = round(gmv / cost, 2) if cost > 0 else 0.0

    # Completeness check: rate-limit truncation manifests as the hourly
    # endpoint stopping mid-window — i.e. the latest hour we can read lags
    # noticeably behind now. Earlier counting-rows logic conflated that with
    # cross-timezone accounts whose ad-local off-hours fall inside the shop
    # window (those hours have no row by design and should not retry). See
    # ads_report_aligned for the fuller incident write-up.
    if cost > 0 and last_row_utc is not None:
        last_full_hour = (now_utc - timedelta(hours=1)).replace(
            minute=0, second=0, microsecond=0
        )
        expected_last = min(last_full_hour, end_utc - timedelta(hours=1))
        lag_h = (expected_last - last_row_utc).total_seconds() / 3600
        if lag_h > _HOURS_LAG_TOLERANCE:
            raise TikTokIncompleteDataError(
                f"GMVMAX advertiser={advertiser_id} date={date} stores={store_ids}: "
                f"latest_row={last_row_utc.strftime('%Y-%m-%d %H:%M UTC')} "
                f"lags {lag_h:.1f}h behind expected="
                f"{expected_last.strftime('%Y-%m-%d %H:%M UTC')} "
                f"(tol={_HOURS_LAG_TOLERANCE}, cost=${cost:.2f}) — "
                f"likely token rate-limit truncated mid-window"
            )

    # FX: convert monetary metrics from advertiser native currency to USD.
    # Non-USD advertisers (THB / MXN / etc.) report raw native amounts; the
    # rest of the system (cache, dashboards, ROI math) assumes USD. The
    # completeness check above already ran on raw cost so the operator-facing
    # error message magnitude is unchanged for non-USD accounts.
    fx_rate = 1.0
    if currency.upper() != "USD":
        fx_rate = await _get_rate_to_usd(currency, date)
        for k in list(aggregated.keys()):
            if k in _MONETARY_METRICS:
                aggregated[k] = aggregated[k] * fx_rate
        # ROI is dimensionless (gmv/cost) — rate cancels, but recompute against
        # converted values for consistency with downstream rounding semantics.
        roi = (
            round(aggregated.get("gross_revenue", 0.0) / aggregated["cost"], 2)
            if aggregated.get("cost", 0) > 0
            else 0.0
        )

    # Round monetary values
    for m in aggregated:
        aggregated[m] = round(aggregated[m], 2)

    return {
        "date": date,
        "shop_tz": shop_tz,
        "ad_tz": str(ad_zone),
        "currency": "USD",
        "source_currency": currency.upper(),
        "fx_rate": fx_rate,
        "metrics": aggregated,
        "roi": roi,
        "hours_included": hours_included,
    }


async def _fetch_hourly_breakdown(
    client: TikTokAdsClient,
    advertiser_id: str,
    date_str: str,
    store_ids: List[str],
    metrics: List[str],
) -> List[Dict]:
    """Fetch one day of hourly GMVMAX data with per-store breakdown.

    Same as _fetch_hourly but adds store_id to dimensions so callers can
    attribute spend to product groups via STORE_PRODUCT_GROUP without
    relying on bitable's per-row (advertiser, store) binding being correct.
    """
    params = {
        "advertiser_id": advertiser_id,
        "start_date": date_str,
        "end_date": date_str,
        "dimensions": json.dumps(["store_id", "stat_time_hour"]),
        "metrics": json.dumps(metrics),
        "store_ids": json.dumps(store_ids),
        "page": 1,
        "page_size": 1000,
    }
    response = await client._make_request("GET", "gmv_max/report/get/", params)
    if response.get("code") == 0:
        return response.get("data", {}).get("list", [])
    raise Exception(
        f"gmv_max/report/get/ returned code={response.get('code')} "
        f"msg={response.get('message')!r} for advertiser={advertiser_id} date={date_str}"
    )


@api_retry(
    max_attempts=3,
    min_wait=3,
    max_wait=15,
    retryable_exceptions=(TikTokRateLimitError, TikTokIncompleteDataError),
)
async def get_gmvmax_report_aligned_breakdown(
    client: TikTokAdsClient,
    advertiser_id: str,
    date: str,
    store_ids: List[str],
    shop_tz: str = "America/Los_Angeles",
    metrics: Optional[List[str]] = None,
) -> Dict[str, Any]:
    """Per-store GMVMAX breakdown aligned to shop timezone.

    Returns per-store metrics so callers route each store's spend to the
    correct product group via STORE_PRODUCT_GROUP — independent of any
    operator-maintained bitable (advertiser, store) binding.

    Returns:
        Dict with date, timezones, breakdown {store_id: {cost, gross_revenue,
        orders, roi}}, and hours included.
    """
    if metrics is None:
        metrics = list(ALIGNED_DEFAULT_METRICS)

    shop_zone = parse_tz(shop_tz)
    ad_zone = await _get_ad_tz(client, advertiser_id)

    start_utc, end_utc = day_utc_range(date, shop_zone)
    now_utc = datetime.now(timezone.utc)

    dates_to_query = native_dates_for_day(date, shop_zone, ad_zone)

    # Concurrent fetch — see non-breakdown variant for rate-limit reasoning.
    import asyncio as _asyncio

    fetched = await _asyncio.gather(
        *(
            _fetch_hourly_breakdown(client, advertiser_id, d, store_ids, metrics)
            for d in dates_to_query
        )
    )
    all_rows: List[Dict] = []
    for rows in fetched:
        all_rows.extend(rows)

    # Aggregate per-store
    by_store: Dict[str, Dict[str, float]] = {}
    hours_seen: Dict[str, int] = {}
    last_row_per_store: Dict[str, datetime] = {}

    for row in all_rows:
        dims = row.get("dimensions", {})
        hour_str = dims.get("stat_time_hour", "")
        store_id = str(dims.get("store_id", ""))
        # See note above re. TikTok's "-" placeholder for empty hourly buckets.
        if not hour_str or hour_str == "-" or not store_id:
            continue

        utc_dt = hour_to_utc(hour_str, ad_zone)
        if not (start_utc <= utc_dt < end_utc and utc_dt <= now_utc):
            continue

        bucket = by_store.setdefault(store_id, {m: 0.0 for m in metrics})
        row_metrics = row.get("metrics", {})
        for m in metrics:
            try:
                bucket[m] += float(row_metrics.get(m, "0"))
            except (ValueError, TypeError):
                pass
        hours_seen[store_id] = hours_seen.get(store_id, 0) + 1
        prev = last_row_per_store.get(store_id)
        if prev is None or utc_dt > prev:
            last_row_per_store[store_id] = utc_dt

    # Completeness check (per-store): use latest-row lag (see non-breakdown
    # variant). Counting rows breaks for cross-tz accounts whose ad-local
    # off-hours fall inside the shop window.
    last_full_hour = (now_utc - timedelta(hours=1)).replace(
        minute=0, second=0, microsecond=0
    )
    expected_last = min(last_full_hour, end_utc - timedelta(hours=1))
    for store_id, bucket in by_store.items():
        store_cost = bucket.get("cost", 0.0)
        store_last = last_row_per_store.get(store_id)
        if store_cost > 0 and store_last is not None:
            lag_h = (expected_last - store_last).total_seconds() / 3600
            if lag_h > _HOURS_LAG_TOLERANCE:
                raise TikTokIncompleteDataError(
                    f"GMVMAX-breakdown advertiser={advertiser_id} date={date}: "
                    f"store={store_id} latest_row="
                    f"{store_last.strftime('%Y-%m-%d %H:%M UTC')} "
                    f"lags {lag_h:.1f}h behind expected="
                    f"{expected_last.strftime('%Y-%m-%d %H:%M UTC')} "
                    f"(tol={_HOURS_LAG_TOLERANCE}, cost=${store_cost:.2f}) — "
                    f"likely token rate-limit truncated mid-window"
                )

    breakdown: Dict[str, Dict[str, Any]] = {}
    for store_id, bucket in by_store.items():
        cost = bucket.get("cost", 0.0)
        gmv = bucket.get("gross_revenue", 0.0)
        breakdown[store_id] = {
            "cost": round(cost, 2),
            "gross_revenue": round(gmv, 2),
            "orders": int(bucket.get("orders", 0)),
            "roi": round(gmv / cost, 2) if cost > 0 else 0.0,
            "hours_included": hours_seen.get(store_id, 0),
        }

    return {
        "date": date,
        "shop_tz": shop_tz,
        "ad_tz": str(ad_zone),
        "breakdown": breakdown,
    }
