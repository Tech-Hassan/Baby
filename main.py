"""
main.py — Railway entry point
Runs FastAPI (uvicorn) + Telegram bot in a single asyncio event loop.
Railway injects $PORT automatically; bot connects outbound to Telegram MTProto
so only one port exposure is needed.
"""

import asyncio
import os

import uvicorn
from telethon import TelegramClient

# ── Config from env (set these in Railway dashboard) ──────────────────────
API_ID    = int(os.environ["TG_API_ID"])
API_HASH  = os.environ["TG_API_HASH"]
BOT_TOKEN = os.environ["TG_BOT_TOKEN"]
PORT      = int(os.getenv("PORT", "8081"))

# ── Import the FastAPI app ─────────────────────────────────────────────────
from server import app

# ── Import all bot handlers (registers @bot.on decorators) ────────────────
# We re-create the client here so bot.py can be imported without auto-starting
import bot as _bot_module

# ── Main coroutine ─────────────────────────────────────────────────────────
async def main():
    # 1. Start uvicorn (non-blocking, runs as an asyncio task)
    config = uvicorn.Config(
        app,
        host="0.0.0.0",
        port=PORT,
        loop="none",
        http="httptools",
        log_level="warning",
        access_log=False,
        backlog=65535,
        timeout_keep_alive=120,
    )
    server = uvicorn.Server(config)

    # 2. Connect the Telegram bot
    bot_client: TelegramClient = _bot_module.bot
    await bot_client.start(bot_token=BOT_TOKEN)
    print(f"[+] Bot connected — @{(await bot_client.get_me()).username}")
    print(f"[+] API server starting on port {PORT}")

    # 3. Run both concurrently; if either exits the other is cancelled
    await asyncio.gather(
        server.serve(),
        bot_client.run_until_disconnected(),
    )


if __name__ == "__main__":
    asyncio.run(main())
