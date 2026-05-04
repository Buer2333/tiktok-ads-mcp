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

# Clear proxy env to avoid SOCKS interference with TikTok Business API
unset ALL_PROXY all_proxy HTTP_PROXY http_proxy HTTPS_PROXY https_proxy 2>/dev/null || true

# Use venv python next to this script
PYTHON="${SCRIPT_DIR}/.venv/bin/python"

exec "$PYTHON" -m tiktok_ads_mcp
