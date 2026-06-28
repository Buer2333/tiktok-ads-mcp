#!/bin/bash
# Launcher for TikTok Ads MCP — loads secrets from ~/.config/tiktok-mcp/ads.env
# Path layout assumption: project root contains .venv/ next to this script.
set -euo pipefail

CREDENTIALS_DIR="$HOME/.config/tiktok-mcp"
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

# Load env vars (TIKTOK_APP_ID / TIKTOK_SECRET / TIKTOK_ACCESS_TOKEN_XINCHENG / TIKTOK_ACCESS_TOKEN_ZECHENG)
set -a
# shellcheck disable=SC1091
source "${CREDENTIALS_DIR}/ads.env"
set +a

# business-api.tiktok.com 现仅经代理可达（直连超时 HTTP 000）。走本地 HTTP 代理
# = Clash Verge 7897 → MATCH,PROXY → 固定 LAX 45.76.78.205（TikTok allowlist 绑定）。
# 仅清 socks5 的 all_proxy（httpx 无 socksio 会坏），保留 http(s) 代理；httpx trust_env=True 自动读取。
# 端口/代理可用 TIKTOK_ADS_PROXY 覆盖。
unset ALL_PROXY all_proxy 2>/dev/null || true
export HTTP_PROXY="${TIKTOK_ADS_PROXY:-http://127.0.0.1:7897}"
export HTTPS_PROXY="${TIKTOK_ADS_PROXY:-http://127.0.0.1:7897}"
export NO_PROXY="localhost,127.0.0.1,::1"
export no_proxy="localhost,127.0.0.1,::1"

# Use venv python next to this script
PYTHON="${SCRIPT_DIR}/.venv/bin/python"

exec "$PYTHON" -m tiktok_ads_mcp
