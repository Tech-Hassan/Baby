"""
bot.py
────────────────────────────────────────────────────────────────────────────
Telegram bot — merged with checker_core, no bridge needed.
Calls process_card() directly (same async engine the API uses).
Supports 100k users because Telethon + asyncio handles concurrent sessions.
────────────────────────────────────────────────────────────────────────────
"""

import os
import asyncio
import aiofiles
import random
import time
import json
import re
from datetime import datetime

from telethon import TelegramClient, events, Button

from checker_core import (
    process_card,
    get_bin_info,
    fetch_products,
    parse_proxy,
    extract_clean_response,
    stats,
)

# ─── Config ── fill these or set env vars ─────────────────────────────────
API_ID    = int(os.getenv("TG_API_ID",    "21124241"))
API_HASH  = os.getenv("TG_API_HASH",      "b7ddce3d3683f54be788fddae73fa468")
BOT_TOKEN = os.getenv("TG_BOT_TOKEN",     "78401EDtSwiSOxAF5g")

PREMIUM_FILE = os.getenv("PREMIUM_FILE", "premium.txt")
SITES_FILE   = os.getenv("SITES_FILE",   "sites.txt")
PROXY_FILE   = os.getenv("PROXY_FILE",   "proxy.txt")

# Max concurrent card checks per user (fair use)
USER_CONCURRENT = int(os.getenv("USER_CONCURRENT", "10"))

# ─── Premium emoji map ────────────────────────────────────────────────────
_EMOJI_IDS = {
    "✅": "6023660820544623088", "🔥": "5999340396432333728",
    "❌": "6037570896766438989", "⚡": "6026367225466720832",
    "💳": "5971944878815317190", "💠": "5971837723676249096",
    "📝": "6023660820544623088", "🌐": "6026367225466720832",
    "🎯": "5974235702701853774", "🤖": "6057466460886799210",
    "🤵": "4949560993840629085", "💰": "5971944878815317190",
    "⏸️": "6001440193058444284", "▶️": "6285315214673975495",
    "🛑": "5420323339723881652", "📊": "5971837723676249096",
    "📦": "6066395745139824604", "📋": "5974235702701853774",
    "🔄": "5971837723676249096", "⏳": "5971837723676249096",
    "🚀": "6282977077427702833", "⚠️": "5420323339723881652",
    "💎": "6023660820544623088",
}


def pemoji(text: str) -> str:
    """Wrap emojis in <tg-emoji> tags for premium custom rendering."""
    if not text:
        return text
    placeholders = []
    result = text
    for i, (emoji, doc_id) in enumerate(_EMOJI_IDS.items()):
        ph = f"\x00PE{i:02d}\x00"
        placeholders.append((ph, doc_id, emoji))
        result = result.replace(emoji, ph)
    for ph, doc_id, emoji in placeholders:
        result = result.replace(ph, f'<tg-emoji emoji-id="{doc_id}">{emoji}</tg-emoji>')
    return result


# ─── File helpers ─────────────────────────────────────────────────────────

def _read_file(path: str) -> list[str]:
    if not os.path.exists(path):
        return []
    try:
        with open(path, "r", encoding="utf-8", errors="ignore") as f:
            return [l.strip() for l in f if l.strip()]
    except Exception:
        return []


def load_premium() -> list[str]:  return _read_file(PREMIUM_FILE)
def load_sites()   -> list[str]:  return _read_file(SITES_FILE)
def load_proxies() -> list[str]:  return _read_file(PROXY_FILE)
def is_premium(uid) -> bool:      return str(uid) in load_premium()


def extract_cc(text: str) -> list[str]:
    cards = []
    for m in re.findall(r"(\d{15,16})\|(\d{2})\|(\d{2,4})\|(\d{3,4})", text):
        card, month, year, cvv = m
        if len(year) == 2:
            year = "20" + year
        cards.append(f"{card}|{month}|{year}|{cvv}")
    return cards


_DEAD_KW = (
    "receipt id is empty", "handle is empty", "product id is empty",
    "invalid url", "error in 1st req", "cloudflare", "connection failed",
    "timed out", "access denied", "tlsv1 alert", "ssl routines",
    "could not resolve", "domain name not found", "name or service not known",
    "empty reply from server", "502", "503", "504", "bad gateway",
    "service unavailable", "gateway timeout", "network error", "connection reset",
    "failed to detect product", "failed to create checkout", "failed to tokenize",
    "failed to get proposal", "url rejected", "malformed input", "amount_too_small",
    "all products sold out", "no_session_token", "tokenize_fail",
    "captcha_required", "captcha required", "failed",
)


def is_dead_site_error(msg: str) -> bool:
    if not msg:
        return True
    m = str(msg).lower()
    return any(k in m for k in _DEAD_KW)


# ─── BIN info wrapper ─────────────────────────────────────────────────────
async def _bin(card_no: str) -> tuple:
    return await get_bin_info(card_no)


# ─── Bot init ─────────────────────────────────────────────────────────────
# Build client but DO NOT start it here — main.py starts it so Railway can
# run both uvicorn and the bot in the same asyncio loop.
bot = TelegramClient("checker_bot", API_ID, API_HASH)

# Per-user semaphores for fair queuing
_user_sems: dict[int, asyncio.Semaphore] = {}

def _user_sem(uid: int) -> asyncio.Semaphore:
    if uid not in _user_sems:
        _user_sems[uid] = asyncio.Semaphore(USER_CONCURRENT)
    return _user_sems[uid]


# active bulk sessions  {f"{uid}_{msg_id}": {paused, cancelled}}
active_sessions: dict[str, dict] = {}


# ─── Message templates ────────────────────────────────────────────────────
HDR = "<b>⚡💳 \u3164#𝒮𝒽𝑜𝓅𝒾𝒾𝒾𝒾  💳⚡</b>\n<b>━━━━━━━━━━━━━━━━━</b>\n"
BOT_CREDIT = '\n\n🤖 <b>Bot By: <a href="tg://user?id=5248903529">\u3164\u3164ＫＡＭＡＬ</a></b>'


def _result_block(result: dict, bin_info: tuple, show_bin: bool = True) -> str:
    status = result.get("status", "Dead")
    emoji = "✅" if status == "Charged" else ("🔥" if status == "Approved" else "❌")
    brand, btype, level, bank, country, flag = bin_info
    block = (
        f"{HDR}"
        f"<b>⚡💠 𝑹𝒆𝒔𝒖𝒍𝒕𝒔</b>\n"
        f"<blockquote>{emoji} Status: <b>{status}</b></blockquote>\n"
        f"<blockquote>💳 Card: <code>{result['card']}</code></blockquote>\n"
        f"<blockquote>📝 Response: {result['message'][:180]}</blockquote>\n"
        f"<blockquote>🌐 𝑮𝒂𝒕𝒆𝒘𝒂𝒚: 🔥 {result.get('gateway','?')} | 💰 {result.get('price','-')}</blockquote>\n"
        f"<b>━━━━━━━━━━━━━━━━━</b>\n"
    )
    if show_bin:
        block += (
            f"<b>🎯💠 𝑩𝑰𝑵 𝑰𝒏𝒇𝒐</b>\n"
            f"<pre>𝑩𝑰𝑵 𝑰𝒏𝒇𝒐: {brand} - {btype} - {level}\n"
            f"𝑩𝒂𝒏𝒌: {bank}\n"
            f"𝑪𝒐𝒖𝒏𝒕𝒓𝒚: {country} {flag}</pre>\n"
            f"<b>━━━━━━━━━━━━━━━━━</b>\n"
        )
    return block + BOT_CREDIT


def _progress_block(res: dict, checked: int) -> str:
    el = int(time.time() - res["start_time"])
    h, r = divmod(el, 3600)
    m, s = divmod(r, 60)
    gw = "Unknown"
    if res["charged"]:
        gw = res["charged"][0].get("gateway", "?")
    elif res["approved"]:
        gw = res["approved"][0].get("gateway", "?")
    return (
        f"{HDR}"
        f"<b>⚡💠 𝑷𝒓𝒐𝒈𝒓𝒆𝒔𝒔</b>\n"
        f"<blockquote>💳 Total: {res['total']} | ✅ {len(res['charged'])} | 🔥 {len(res['approved'])} | ❌ {len(res['dead'])}</blockquote>\n"
        f"<blockquote>📊 Checked: {checked}/{res['total']}</blockquote>\n"
        f"<blockquote>🌐 𝑮𝒂𝒕𝒆𝒘𝒂𝒚: 🔥 {gw}</blockquote>\n"
        f"<blockquote>⏳ Time: {h}h {m}m {s}s</blockquote>\n"
        f"<b>━━━━━━━━━━━━━━━━━</b>"
    )


# ─── /start ───────────────────────────────────────────────────────────────
@bot.on(events.NewMessage(pattern="/start"))
async def cmd_start(event):
    await event.reply(pemoji(
        f"{HDR}"
        "<b>⚡💠 𝑪𝑪 𝑪𝒐𝒎𝒎𝒂𝒏𝒅𝒔</b>\n"
        "<blockquote>• /cc card|mm|yy|cvv — Check single CC\n"
        "• /chk — Reply to .txt to bulk check</blockquote>\n"
        "<b>⚡💠 𝑺𝒊𝒕𝒆 𝑪𝒐𝒎𝒎𝒂𝒏𝒅𝒔</b>\n"
        "<blockquote>• /fuck — Check & clean all sites\n"
        "• /rm url — Remove a site</blockquote>\n"
        "<b>⚡💠 𝑷𝒓𝒐𝒙𝒚 𝑪𝒐𝒎𝒎𝒂𝒏𝒅𝒔</b>\n"
        "<blockquote>• /proxy — Check & clean all proxies\n"
        "• /addproxy — Add proxies (one per line)\n"
        "• /chkproxy ip:p:u:pass — Test one proxy\n"
        "• /rmproxy ip:p:u:pass — Remove one\n"
        "• /rmproxyindex 1,2,3 — Remove by index\n"
        "• /clearproxy — Nuke all proxies\n"
        "• /getproxy — Dump proxy list</blockquote>\n"
        "<b>━━━━━━━━━━━━━━━━━</b>\n"
        "<b>⚠️ Premium users only.</b>"
    ), parse_mode="html")


# ─── /cc  single card ─────────────────────────────────────────────────────
@bot.on(events.NewMessage(pattern=r"^/cc\s+"))
async def cmd_cc(event):
    uid = event.sender_id
    if not is_premium(uid):
        await event.reply(pemoji("❌ <b>Access Denied</b>"), parse_mode="html")
        return

    sites, proxies = load_sites(), load_proxies()
    if not sites:
        await event.reply(pemoji("❌ No sites — contact admin."), parse_mode="html")
        return
    if not proxies:
        await event.reply(pemoji("❌ No proxies — add some first."), parse_mode="html")
        return

    raw = event.message.text.split(" ", 1)[1].strip()
    cards = extract_cc(raw)
    if not cards:
        await event.reply(pemoji("❌ Invalid format. Use <code>/cc card|mm|yy|cvv</code>"), parse_mode="html")
        return

    card = cards[0]
    msg = await event.reply(pemoji(f"{HDR}<b>⚡💠 𝑪𝒉𝒆𝒄𝒌𝒊𝒏𝒈...</b>\n<blockquote>💳 <code>{card}</code></blockquote>"), parse_mode="html")

    async with _user_sem(uid):
        site  = random.choice(sites)
        proxy = random.choice(proxies)
        cc_parts = card.split("|")
        ok, message, gateway, price, currency = await process_card(
            cc_parts[0], cc_parts[1], cc_parts[2], cc_parts[3], site, None, proxy
        )

    status = "Charged" if ok and "ORDER_PLACED" in message else ("Approved" if ok else "Dead")
    result = {"status": status, "card": card, "message": message[:180], "gateway": gateway, "price": price}
    bin_info = await _bin(cc_parts[0])
    await msg.edit(pemoji(_result_block(result, bin_info)), parse_mode="html")


# ─── /chk  bulk check ─────────────────────────────────────────────────────
@bot.on(events.NewMessage(pattern="/chk"))
async def cmd_chk(event):
    uid = event.sender_id
    try:
        sender = await event.get_sender()
        username = sender.username or f"user_{uid}"
    except:
        username = f"user_{uid}"

    if not is_premium(uid):
        await event.reply(pemoji("❌ <b>Access Denied</b>"), parse_mode="html")
        return
    if not event.reply_to_msg_id:
        await event.reply(pemoji("❌ Reply to a .txt file."), parse_mode="html")
        return

    reply_msg = await event.get_reply_message()
    if not reply_msg.file or not reply_msg.file.name.endswith(".txt"):
        await event.reply(pemoji("❌ Must be a .txt file."), parse_mode="html")
        return
    if not load_sites():
        await event.reply(pemoji("❌ No sites available."), parse_mode="html")
        return
    if not load_proxies():
        await event.reply(pemoji("❌ No proxies available."), parse_mode="html")
        return

    status_msg = await event.reply(pemoji("🔄 Downloading file..."), parse_mode="html")
    fp = await reply_msg.download_media()

    async with aiofiles.open(fp, "r", encoding="utf-8", errors="ignore") as f:
        content = await f.read()

    cards = extract_cc(content)
    os.remove(fp)

    if not cards:
        await status_msg.edit(pemoji("❌ No valid cards found."), parse_mode="html")
        return
    if len(cards) > 5000:
        await status_msg.edit(pemoji(f"⚠️ {len(cards)} cards — capped at 5000."), parse_mode="html")
        cards = cards[:5000]

    total_cards = len(cards)
    await status_msg.edit(pemoji(f"🔄 Starting {total_cards} cards..."), parse_mode="html")

    session_key = f"{uid}_{status_msg.id}"
    active_sessions[session_key] = {"paused": False, "cancelled": False}

    all_results = {
        "charged": [], "approved": [], "dead": [],
        "total": total_cards, "checked": 0,
        "start_time": time.time(),
    }

    queue: asyncio.Queue = asyncio.Queue()
    for c in cards:
        queue.put_nowait(c)

    last_update = [time.time()]

    async def worker():
        while not queue.empty():
            sess = active_sessions.get(session_key)
            if not sess or sess.get("cancelled"):
                break
            while sess.get("paused"):
                await asyncio.sleep(1)
                sess = active_sessions.get(session_key)
                if not sess:
                    return
            try:
                card = queue.get_nowait()
            except asyncio.QueueEmpty:
                break

            cur_sites   = load_sites()
            cur_proxies = load_proxies()
            if not cur_sites or not cur_proxies:
                break

            site  = random.choice(cur_sites)
            proxy = random.choice(cur_proxies)
            parts = card.split("|")

            async with _user_sem(uid):
                ok, message, gateway, price, currency = await process_card(
                    parts[0], parts[1], parts[2], parts[3], site, None, proxy
                )

            status = "Charged" if ok and "ORDER_PLACED" in message else ("Approved" if ok else "Dead")
            res = {"status": status, "card": card, "message": message[:180],
                   "gateway": gateway, "price": price, "site": site}

            all_results["checked"] += 1
            if status == "Charged":
                all_results["charged"].append(res)
                await _send_hit(uid, res, "Charged", username)
            elif status == "Approved":
                all_results["approved"].append(res)
                await _send_hit(uid, res, "Approved", username)
            else:
                all_results["dead"].append(res)

            queue.task_done()
            now = time.time()
            if now - last_update[0] >= 1.5:
                last_update[0] = now
                if session_key in active_sessions:
                    try:
                        btns = [
                            [Button.inline("⏸️ Pause", b"pause"), Button.inline("▶️ Resume", b"resume")],
                            [Button.inline("🛑 Stop", b"stop")],
                        ]
                        await bot.edit_message(uid, status_msg.id,
                                               pemoji(_progress_block(all_results, all_results["checked"])),
                                               buttons=btns, parse_mode="html")
                    except:
                        pass

    workers = [asyncio.create_task(worker()) for _ in range(USER_CONCURRENT)]
    while workers:
        if not active_sessions.get(session_key, {}).get("cancelled", False) is False:
            for w in workers:
                w.cancel()
            break
        done, pending = await asyncio.wait(workers, timeout=1.5)
        workers = list(pending)

    active_sessions.pop(session_key, None)
    try:
        await status_msg.delete()
    except:
        pass
    await _send_final(uid, all_results)


async def _send_hit(uid: int, result: dict, hit_type: str, username: str):
    bin_info = await _bin(result["card"].split("|")[0])
    emoji = "✅" if hit_type == "Charged" else "🔥"
    brand, btype, level, bank, country, flag = bin_info
    msg = (
        f"{HDR}"
        f"<b>⚡💠 𝑯𝒊𝒕 𝑭𝒐𝒖𝒏𝒅!</b>\n"
        f"<blockquote>{emoji} Status: <b>{'𝑪𝒉𝒂𝒓𝒈𝒆𝒅' if hit_type=='Charged' else '𝑳𝒊𝒗𝒆'}</b></blockquote>\n"
        f"<blockquote>💳 Card: <code>{result['card']}</code></blockquote>\n"
        f"<blockquote>📝 Response: {result['message'][:150]}</blockquote>\n"
        f"<blockquote>🌐 𝑮𝒂𝒕𝒆𝒘𝒂𝒚: 🔥 {result.get('gateway','?')} | 💰 {result.get('price','-')}</blockquote>\n"
        f"<b>━━━━━━━━━━━━━━━━━</b>\n"
        f"<b>🎯💠 𝑩𝑰𝑵 𝑰𝒏𝒇𝒐</b>\n"
        f"<pre>𝑩𝑰𝑵: {brand} - {btype} - {level}\n𝑩𝒂𝒏𝒌: {bank}\n𝑪𝒐𝒖𝒏𝒕𝒓𝒚: {country} {flag}</pre>\n"
        f"<b>━━━━━━━━━━━━━━━━━</b>"
        + BOT_CREDIT
    )
    try:
        await bot.send_message(uid, pemoji(msg), parse_mode="html")
    except:
        pass


async def _send_final(uid: int, results: dict):
    el = int(time.time() - results["start_time"])
    h, r = divmod(el, 3600)
    m, s = divmod(r, 60)

    hits_text = ""
    for r2 in results["charged"][:5]:
        hits_text += f"✅ <code>{r2['card']}</code>\n"
    for r2 in results["approved"][:5]:
        hits_text += f"🔥 <code>{r2['card']}</code>\n"
    hits_text = hits_text or "No hits found"

    gw = "Unknown"
    if results["charged"]:
        gw = results["charged"][0].get("gateway", "?")
    elif results["approved"]:
        gw = results["approved"][0].get("gateway", "?")

    summary = (
        f"{HDR}"
        f"<b>⚡💠 𝑭𝒊𝒏𝒂𝒍 𝑹𝒆𝒔𝒖𝒍𝒕𝒔</b>\n"
        f"<blockquote>💳 Total: {results['total']} | ✅ {len(results['charged'])} | 🔥 {len(results['approved'])} | ❌ {len(results['dead'])}</blockquote>\n"
        f"<blockquote>🌐 𝑮𝒂𝒕𝒆𝒘𝒂𝒚: 🔥 {gw}</blockquote>\n"
        f"<blockquote>⏳ Time: {h}h {m}m {s}s</blockquote>\n"
        f"<b>━━━━━━━━━━━━━━━━━</b>\n"
        f"<b>🎯💠 𝑯𝒊𝒕𝒔</b>\n"
        f"<blockquote>{hits_text}</blockquote>\n"
        f"<b>━━━━━━━━━━━━━━━━━</b>"
        + BOT_CREDIT
    )

    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    fname = f"shopiii_{uid}_{ts}.txt"
    async with aiofiles.open(fname, "w") as f:
        await f.write("=" * 70 + "\n⚡💳 CC CHECKER RESULTS 💳⚡\nFormat: CC | Gateway | Price | Message | Site\n" + "=" * 70 + "\n\n")
        await f.write(f"✅ CHARGED ({len(results['charged'])}):\n" + "-" * 70 + "\n")
        for r2 in results["charged"]:
            await f.write(f"{r2['card']} | {r2.get('gateway','?')} | {r2.get('price','-')} | {r2['message'][:100]} | {r2.get('site','?')}\n")
        await f.write(f"\n🔥 APPROVED ({len(results['approved'])}):\n" + "-" * 70 + "\n")
        for r2 in results["approved"]:
            await f.write(f"{r2['card']} | {r2.get('gateway','?')} | {r2.get('price','-')} | {r2['message'][:100]} | {r2.get('site','?')}\n")
        await f.write(f"\n❌ DEAD ({len(results['dead'])}):\n" + "-" * 70 + "\n")
        for r2 in results["dead"]:
            await f.write(f"{r2['card']} | {r2.get('gateway','?')} | {r2.get('price','-')} | {r2['message'][:100]} | {r2.get('site','?')}\n")

    try:
        await bot.send_message(uid, pemoji(summary), file=fname, parse_mode="html")
    except:
        pass
    try:
        os.remove(fname)
    except:
        pass


# ─── Proxy commands ───────────────────────────────────────────────────────

@bot.on(events.NewMessage(pattern=r"^/chkproxy\s+"))
async def cmd_chkproxy(event):
    uid = event.sender_id
    if not is_premium(uid):
        await event.reply(pemoji("❌ <b>Access Denied</b>"), parse_mode="html")
        return
    proxy = event.message.text.split(" ", 1)[1].strip()
    msg = await event.reply(pemoji(f"🔄 Testing proxy <code>{proxy}</code>..."), parse_mode="html")
    # Test by fetching a test site
    sites = load_sites()
    test_site = sites[0] if sites else "https://riverbendhomedev.myshopify.com"
    try:
        import aiohttp as _ahttp
        from checker_core import _get_connector
        conn = await _get_connector()
        timeout = _ahttp.ClientTimeout(total=20)
        p_url = parse_proxy(proxy)
        async with _ahttp.ClientSession(connector=conn, connector_owner=False, timeout=timeout) as s:
            async with s.get(f"{test_site}/products.json", proxy=p_url) as r:
                ok = r.status == 200
        if ok:
            await msg.edit(pemoji(f"✅ <b>Proxy ALIVE</b>\n<code>{proxy}</code>"), parse_mode="html")
        else:
            await msg.edit(pemoji(f"❌ <b>Proxy DEAD</b>\n<code>{proxy}</code>"), parse_mode="html")
    except Exception as e:
        await msg.edit(pemoji(f"❌ Proxy dead: {e}"), parse_mode="html")


@bot.on(events.NewMessage(pattern=r"^/rmproxy\s+"))
async def cmd_rmproxy(event):
    uid = event.sender_id
    if not is_premium(uid):
        await event.reply(pemoji("❌ <b>Access Denied</b>"), parse_mode="html")
        return
    target = event.message.text.split(" ", 1)[1].strip()
    cur = load_proxies()
    if target not in cur:
        await event.reply(pemoji(f"❌ Not found: <code>{target}</code>"), parse_mode="html")
        return
    new = [p for p in cur if p != target]
    async with aiofiles.open(PROXY_FILE, "w") as f:
        await f.write("\n".join(new) + "\n")
    await event.reply(pemoji(f"✅ <b>Removed</b> <code>{target}</code>"), parse_mode="html")


@bot.on(events.NewMessage(pattern=r"^/rmproxyindex\s+"))
async def cmd_rmproxyindex(event):
    uid = event.sender_id
    if not is_premium(uid):
        await event.reply(pemoji("❌ <b>Access Denied</b>"), parse_mode="html")
        return
    raw = event.message.text.split(" ", 1)[1].strip()
    try:
        idxs = {int(i.strip()) - 1 for i in raw.split(",")}
    except ValueError:
        await event.reply(pemoji("❌ Use comma-separated numbers."), parse_mode="html")
        return
    cur = load_proxies()
    removed = [p for i, p in enumerate(cur) if i in idxs]
    if not removed:
        await event.reply(pemoji("❌ No valid indices."), parse_mode="html")
        return
    new = [p for i, p in enumerate(cur) if i not in idxs]
    async with aiofiles.open(PROXY_FILE, "w") as f:
        await f.write("\n".join(new) + "\n")
    sample = "\n".join(removed[:10]) + ("..." if len(removed) > 10 else "")
    await event.reply(pemoji(f"✅ Removed {len(removed)}:\n<code>{sample}</code>"), parse_mode="html")


@bot.on(events.NewMessage(pattern=r"^/clearproxy$"))
async def cmd_clearproxy(event):
    uid = event.sender_id
    if not is_premium(uid):
        await event.reply(pemoji("❌ <b>Access Denied</b>"), parse_mode="html")
        return
    cur = load_proxies()
    if not cur:
        await event.reply(pemoji("❌ Already empty."), parse_mode="html")
        return
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    backup = f"proxy_backup_{uid}_{ts}.txt"
    async with aiofiles.open(backup, "w") as f:
        await f.write("\n".join(cur) + "\n")
    await event.reply(pemoji(f"📦 Backup of {len(cur)} proxies:"), file=backup, parse_mode="html")
    try:
        os.remove(backup)
    except:
        pass
    async with aiofiles.open(PROXY_FILE, "w") as f:
        await f.write("")
    await event.reply(pemoji(f"✅ Cleared all {len(cur)} proxies."), parse_mode="html")


@bot.on(events.NewMessage(pattern=r"^/getproxy$"))
async def cmd_getproxy(event):
    uid = event.sender_id
    if not is_premium(uid):
        await event.reply(pemoji("❌ <b>Access Denied</b>"), parse_mode="html")
        return
    cur = load_proxies()
    if not cur:
        await event.reply(pemoji("❌ proxy.txt is empty."), parse_mode="html")
        return
    if len(cur) <= 50:
        pl = "\n".join(f"{i+1}. <code>{p}</code>" for i, p in enumerate(cur))
        await event.reply(pemoji(f"<b>📋 Proxies ({len(cur)}):</b>\n{pl}"), parse_mode="html")
    else:
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        fname = f"proxies_{uid}_{ts}.txt"
        async with aiofiles.open(fname, "w") as f:
            await f.write("\n".join(f"{i+1}. {p}" for i, p in enumerate(cur)))
        await event.reply(pemoji(f"<b>📋 Proxies ({len(cur)}) attached:</b>"), file=fname, parse_mode="html")
        try:
            os.remove(fname)
        except:
            pass


@bot.on(events.NewMessage(pattern=r"^/addproxy"))
async def cmd_addproxy(event):
    uid = event.sender_id
    if not is_premium(uid):
        await event.reply(pemoji("❌ <b>Access Denied</b>"), parse_mode="html")
        return
    lines = event.message.text.split("\n")
    new_proxies = [l.strip() for l in lines[1:] if l.strip()]
    if not new_proxies:
        await event.reply(pemoji("❌ Provide proxies after /addproxy (one per line)."), parse_mode="html")
        return
    cur = set(load_proxies())
    added = [p for p in new_proxies if p not in cur]
    if not added:
        await event.reply(pemoji("⚠️ All proxies already exist."), parse_mode="html")
        return
    async with aiofiles.open(PROXY_FILE, "a") as f:
        await f.write("\n".join(added) + "\n")
    await event.reply(pemoji(f"✅ Added {len(added)} proxies."), parse_mode="html")


# ─── Site commands ────────────────────────────────────────────────────────

@bot.on(events.NewMessage(pattern=r"^/rm"))
async def cmd_rmsite(event):
    uid = event.sender_id
    if not is_premium(uid):
        await event.reply(pemoji("❌ <b>Access Denied</b>"), parse_mode="html")
        return
    args = event.message.text.split(" ", 1)
    if len(args) < 2:
        await event.reply(pemoji("❌ Usage: <code>/rm https://site.com</code>"), parse_mode="html")
        return
    url = args[1].strip()
    cur = load_sites()
    if url not in cur:
        await event.reply(pemoji(f"❌ Not found: <code>{url}</code>"), parse_mode="html")
        return
    new = [s for s in cur if s != url]
    async with aiofiles.open(SITES_FILE, "w") as f:
        await f.write("\n".join(new) + "\n")
    await event.reply(pemoji(f"✅ Removed <code>{url}</code>"), parse_mode="html")


@bot.on(events.NewMessage(pattern="/fuck"))
async def cmd_site(event):
    """Check & clean all sites."""
    uid = event.sender_id
    if not is_premium(uid):
        await event.reply(pemoji("❌ <b>Access Denied</b>"), parse_mode="html")
        return
    sites = load_sites()
    if not sites:
        await event.reply(pemoji("❌ sites.txt is empty."), parse_mode="html")
        return
    proxies = load_proxies()
    if not proxies:
        await event.reply(pemoji("❌ No proxies available."), parse_mode="html")
        return

    msg = await event.reply(pemoji(f"🔥 Checking {len(sites)} sites..."), parse_mode="html")
    alive, dead = [], []
    batch = 10

    for i in range(0, len(sites), batch):
        chunk = sites[i:i+batch]
        tasks = [fetch_products(s, random.choice(proxies)) for s in chunk]
        results = await asyncio.gather(*tasks)
        for site, res in zip(chunk, results):
            if isinstance(res, tuple) and res[0] is False:
                dead.append(site)
            else:
                alive.append(site)
        try:
            await msg.edit(pemoji(
                f"🔥 Checking sites...\n"
                f"<b>Checked:</b> {len(alive)+len(dead)}/{len(sites)}\n"
                f"<b>Alive:</b> {len(alive)} | <b>Dead:</b> {len(dead)}"
            ), parse_mode="html")
        except:
            pass

    async with aiofiles.open(SITES_FILE, "w") as f:
        await f.write("\n".join(alive) + "\n")
    await msg.edit(pemoji(
        f"✅ <b>Site Check Done!</b>\n"
        f"Total: {len(sites)} | Alive: {len(alive)} | Removed: {len(dead)}\n"
        f"sites.txt updated."
    ), parse_mode="html")


@bot.on(events.NewMessage(pattern="/proxy"))
async def cmd_proxy(event):
    """Check & clean all proxies."""
    uid = event.sender_id
    if not is_premium(uid):
        await event.reply(pemoji("❌ <b>Access Denied</b>"), parse_mode="html")
        return
    proxies = load_proxies()
    if not proxies:
        await event.reply(pemoji("❌ proxy.txt is empty."), parse_mode="html")
        return

    msg = await event.reply(pemoji(f"🔥 Checking {len(proxies)} proxies..."), parse_mode="html")
    alive, dead = [], []
    test_url = "https://riverbendhomedev.myshopify.com/products.json"

    import aiohttp as _ahttp
    from checker_core import _get_connector
    conn = await _get_connector()
    timeout = _ahttp.ClientTimeout(total=20)
    batch = 50

    for i in range(0, len(proxies), batch):
        chunk = proxies[i:i+batch]

        async def _test(p):
            try:
                p_url = parse_proxy(p)
                async with _ahttp.ClientSession(connector=conn, connector_owner=False, timeout=timeout) as s:
                    async with s.get(test_url, proxy=p_url) as r:
                        return (p, r.status == 200)
            except:
                return (p, False)

        results = await asyncio.gather(*[_test(p) for p in chunk])
        for p, ok in results:
            (alive if ok else dead).append(p)
        try:
            await msg.edit(pemoji(
                f"🔥 Checking proxies...\n"
                f"<b>Checked:</b> {len(alive)+len(dead)}/{len(proxies)}\n"
                f"<b>Alive:</b> {len(alive)} | <b>Dead:</b> {len(dead)}"
            ), parse_mode="html")
        except:
            pass

    async with aiofiles.open(PROXY_FILE, "w") as f:
        await f.write("\n".join(alive) + "\n")
    await msg.edit(pemoji(
        f"✅ <b>Proxy Check Done!</b>\n"
        f"Total: {len(proxies)} | Alive: {len(alive)} | Removed: {len(dead)}\n"
        f"proxy.txt updated."
    ), parse_mode="html")


# ─── Callbacks ────────────────────────────────────────────────────────────

@bot.on(events.CallbackQuery(pattern=b"pause"))
async def cb_pause(event):
    key = f"{event.sender_id}_{event.message_id}"
    if key in active_sessions:
        active_sessions[key]["paused"] = True
    await event.answer(pemoji("⏸️ Paused"))


@bot.on(events.CallbackQuery(pattern=b"resume"))
async def cb_resume(event):
    key = f"{event.sender_id}_{event.message_id}"
    if key in active_sessions:
        active_sessions[key]["paused"] = False
    await event.answer(pemoji("▶️ Resumed"))


@bot.on(events.CallbackQuery(pattern=b"stop"))
async def cb_stop(event):
    key = f"{event.sender_id}_{event.message_id}"
    if key in active_sessions:
        active_sessions[key]["cancelled"] = True
        active_sessions[key]["paused"] = False
    await event.answer(pemoji("🛑 Stopped"))
    await event.edit(pemoji("❌ <b>Checking stopped.</b>"), parse_mode="html")


# ─── Entry point ──────────────────────────────────────────────────────────
print("✅ Bot handlers registered.")
# Entry point is main.py — do NOT call run_until_disconnected() here.
