# TikTok GMV Max MCP

<!-- mcp-name: io.github.Buer2333/tiktok-gmvmax-mcp -->

A read-only [Model Context Protocol](https://modelcontextprotocol.io) server for the TikTok Business (Marketing) API, with first-class **GMV Max** support. Ask your assistant how your GMV Max campaigns, regular ads, and Business Center budgets are doing, and get answers in your shop's timezone and in USD.

It runs daily in production behind a multi-shop TikTok Shop reporting stack, so most of the code beyond plain API wrappers handles things the raw API gets wrong. Report days are realigned from the ad account's timezone to the shop's. Non-USD accounts are converted at that day's rate. TikTok's transient `51010` timeouts are retried.

> Started as a fork of [ysntony/tiktok-ads-mcp](https://github.com/ysntony/tiktok-ads-mcp) (MIT), which covers the six core campaign/report tools. This project adds GMV Max, timezone-aligned and range reports, creatives, Business Center finance and trending hashtags, and is published separately as `tiktok-gmvmax-mcp`. The Python import package is still `tiktok_ads_mcp`, so don't install both distributions into the same environment.

## Tools (24, all read-only)

**GMV Max**

| Tool | What it returns |
|---|---|
| `get_gmvmax_campaigns_tool` | GMV Max campaigns with status and ROI-protection info |
| `get_gmvmax_campaign_info_tool` | Budget, bid, products and schedule for one campaign |
| `get_gmvmax_reports_tool` | GMV Max performance reports (`/gmv_max/report/get/`) |
| `get_gmvmax_report_aligned_tool` | One shop-timezone day, rebuilt from hourly data: cost, gross revenue, orders, ROI |
| `get_gmvmax_range_report_tool` | Aggregate cost / GMV / orders / ROI for a date range |
| `get_gmvmax_videos_tool` | Videos available to GMV Max for a store |
| `get_gmvmax_store_list_tool` | Stores linked to a GMV Max advertiser |

**Accounts, campaigns and ads**

| Tool | What it returns |
|---|---|
| `get_business_centers_tool` | Business Centers visible to the token |
| `get_authorized_ad_accounts_tool` | All authorized advertiser accounts |
| `get_campaigns_tool` / `get_ad_groups_tool` / `get_ads_tool` | Campaign, ad group and ad lists with filtering |
| `get_identities_tool` | TikTok identities (creator accounts) authorized to an advertiser |
| `get_video_info_tool` | Video asset URL, poster and duration by video ID |

**Reporting**

| Tool | What it returns |
|---|---|
| `get_reports_tool` | Integrated reports with custom dimensions and metrics |
| `get_ads_report_aligned_tool` | Manual-bid ads, one shop-timezone day: cost, GMV, orders, ROAS |
| `get_ads_range_report_tool` | Manual-bid ads aggregate for a date range |
| `get_creative_report_tool` | Creative-level video insights (engagement, play metrics) |

**Business Center finance**

| Tool | What it returns |
|---|---|
| `get_advertiser_balance_tool` | Balance and basic info per advertiser (≤100 IDs per call) |
| `get_bc_balance_tool` | Business Center available funds |
| `get_bc_account_cost_tool` | Spend per ad account under a BC for a date range |
| `get_bc_transactions_tool` | Top-ups and deductions at BC or advertiser level |
| `get_bc_budget_changelog_tool` | Who changed an advertiser's budget, and when |

**Discovery**

| Tool | What it returns |
|---|---|
| `get_trending_list_tool` | Trending hashtags by country (7 or 30 days), optional daily history |

## Install

Requires Python 3.10+.

```bash
uvx tiktok-gmvmax-mcp          # run without installing
# or
pip install tiktok-gmvmax-mcp  # provides the `tiktok-gmvmax-mcp` command
```

## Configure

Create an app in the [TikTok for Business developer portal](https://business-api.tiktok.com/portal/), grant it read permissions for the endpoints you need (ad accounts, campaigns and ads, reporting, GMV Max, Business Center), and generate a long-lived access token.

| Variable | Required | Meaning |
|---|---|---|
| `TIKTOK_APP_ID` | yes | App ID |
| `TIKTOK_SECRET` | yes | App secret |
| `TIKTOK_ACCESS_TOKEN` | yes | Access token |
| `TIKTOK_ACCESS_TOKEN_2` | no | Second token (e.g. another Business Center), tried on permission errors |
| `TIKTOK_ADVERTISER_ID` | no | Default advertiser |
| `TIKTOK_SANDBOX` | no | `true` to use the sandbox API |
| `TIKTOK_REQUEST_TIMEOUT` | no | Seconds, default 30 |
| `TIKTOK_ADS_PROXY` | no | Proxy URL used only for Business API calls |
| `LARK_BOT_CACHE_DIR` | no | Where FX rates are cached (default `~/.cache/lark-bot`) |

Versions before 0.2 read the token from `TIKTOK_ACCESS_TOKEN_XINCHENG` / `TIKTOK_ACCESS_TOKEN_ZECHENG`. Those names still work.

Claude Desktop / Claude Code (`mcpServers`):

```json
{
  "mcpServers": {
    "tiktok-gmvmax": {
      "command": "uvx",
      "args": ["tiktok-gmvmax-mcp"],
      "env": {
        "TIKTOK_APP_ID": "your_app_id",
        "TIKTOK_SECRET": "your_secret",
        "TIKTOK_ACCESS_TOKEN": "your_access_token"
      }
    }
  }
}
```

## Behavior worth knowing

- **Timezones.** TikTok reports days in the ad account's timezone. The `*_aligned_tool`s fetch hourly rows and re-aggregate them into a day in the shop's timezone, so ad spend lines up with shop orders.
- **Currency.** Cost and revenue from non-USD accounts are converted to USD at that day's ECB midpoint rate (Frankfurter). If the rate API is down, a small built-in table of approximate rates is used for that call only and never cached; any other currency is then returned unconverted (rate 1.0) with a logged error.
- **Retries.** Code `51010` (TikTok backend timeout) is retried with backoff at the client layer. Rate-limit and 5xx errors are retried too.
- **mcp 1.x and 2.x.** Both work: the server imports `MCPServer` (2.x) and falls back to `FastMCP` (1.x).

## Development

```bash
git clone https://github.com/Buer2333/tiktok-ads-mcp && cd tiktok-ads-mcp
python -m venv .venv && .venv/bin/pip install -e ".[dev]"
.venv/bin/python -m pytest tests -q
```

Issues and PRs are welcome. Please include a test with any behavior change.

## Changelog

### 0.2.0
- Published as `tiktok-gmvmax-mcp` (the PyPI name `tiktok-ads-mcp` belongs to the upstream project).
- Generic `TIKTOK_ACCESS_TOKEN` / `TIKTOK_ACCESS_TOKEN_2`. The legacy names still work.
- CLI logs go to stderr, so stdout stays clean for the stdio protocol.
- FX: a fallback rate used during an API outage is no longer cached permanently.
- 18 tools added since the fork: GMV Max, aligned and range reports, creatives, Business Center finance, identities, trending. Also added: account discovery, 51010 retry, USD conversion, mcp 2.x compatibility.

### 0.1.x (upstream)
- Six core tools, async `httpx` client, FastMCP server. See [ysntony/tiktok-ads-mcp](https://github.com/ysntony/tiktok-ads-mcp).

## License

MIT. See [LICENSE](https://github.com/Buer2333/tiktok-ads-mcp/blob/main/LICENSE).
