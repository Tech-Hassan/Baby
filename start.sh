#!/usr/bin/env bash
# ─────────────────────────────────────────────────────────────────────────
# start.sh — Launch both API server AND Telegram bot in one shot
# Usage:
#   chmod +x start.sh
#   ./start.sh
#
# Env vars you MUST set before running:
#   ADMIN_TOKEN   — admin panel password (default: changeme_strong_token_here)
#   TG_API_ID     — Telegram API ID
#   TG_API_HASH   — Telegram API hash
#   TG_BOT_TOKEN  — bot token from @BotFather
#   MAX_CONCURRENT — max parallel Shopify flows (default: 2000)
#   PORT           — API port (default: 8081)
#   API_KEY        — optional: require X-API-Key header on /shopify
# ─────────────────────────────────────────────────────────────────────────
set -e

# Install deps if needed
pip install -q fastapi "uvicorn[standard]" aiohttp aiofiles telethon

echo "──────────────────────────────────────────"
echo " Starting Shopify Checker API + Bot"
echo " API → http://0.0.0.0:${PORT:-8081}/shopify"
echo " Admin → http://0.0.0.0:${PORT:-8081}/admin"
echo "──────────────────────────────────────────"

# Run API server in background
uvicorn server:app \
  --host 0.0.0.0 \
  --port "${PORT:-8081}" \
  --loop uvloop \
  --http httptools \
  --workers 1 \
  --backlog 4096 \
  --timeout-keep-alive 75 \
  --no-access-log &

API_PID=$!
echo "[+] API server PID: $API_PID"

# Give server 2s to bind
sleep 2

# Run Telegram bot (foreground)
python3 bot.py

# If bot exits, kill server too
kill $API_PID 2>/dev/null || true
