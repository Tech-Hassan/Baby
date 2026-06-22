"""
server.py
────────────────────────────────────────────────────────────────────────────
FastAPI server — replaces Flask.

Why FastAPI > Flask for 100k users:
  • Fully async: one OS thread handles thousands of in-flight requests.
  • uvicorn + uvloop event loop is 3-4× faster than CPython default.
  • No asyncio.new_event_loop() hack per request (Flask legacy).
  • Connection pool is reused across ALL requests via checker_core.
  • Admin panel at /admin (JWT-protected) shows live stats.

Run:
  uvicorn server:app --host 0.0.0.0 --port 8081 \
    --workers 1 --loop uvloop --http httptools \
    --backlog 4096

  For multi-core machines (each worker gets its own pool + semaphore):
  --workers $(nproc)
────────────────────────────────────────────────────────────────────────────
"""

import os
import time
import asyncio
from datetime import datetime

from fastapi import FastAPI, Query, HTTPException, Depends, Request
from fastapi.responses import JSONResponse, HTMLResponse
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
import aiohttp

from checker_core import (
    process_card,
    parse_cc_string,
    extract_clean_response,
    get_bin_info,
    fetch_products,
    parse_proxy,
    stats,
    _get_connector,
)

# ─── Config ───────────────────────────────────────────────────────────────
ADMIN_TOKEN = os.getenv("ADMIN_TOKEN", "changeme_strong_token_here")
API_KEY = os.getenv("API_KEY", "")          # optional: require X-API-Key header
PORT = int(os.getenv("PORT", "8081"))

# ─── App ──────────────────────────────────────────────────────────────────
app = FastAPI(
    title="Shopify Checker API",
    docs_url=None,      # disable public swagger
    redoc_url=None,
)

security = HTTPBearer(auto_error=False)


# ─── Startup / Shutdown ───────────────────────────────────────────────────
@app.on_event("startup")
async def startup():
    # Warm up connector
    await _get_connector()
    stats["start_time"] = time.time()
    print(f"[+] Server ready on port {PORT}")


@app.on_event("shutdown")
async def shutdown():
    from checker_core import _connector
    if _connector and not _connector.closed:
        await _connector.close()


# ─── Optional API-key auth ────────────────────────────────────────────────
async def verify_api_key(request: Request):
    if not API_KEY:
        return
    key = request.headers.get("X-API-Key", "")
    if key != API_KEY:
        raise HTTPException(status_code=401, detail="Invalid API key")


# ─── /shopify endpoint ────────────────────────────────────────────────────
@app.get("/shopify", dependencies=[Depends(verify_api_key)])
async def shopify_checker(
    site: str = Query(..., description="Shopify store URL"),
    cc: str = Query(..., description="CC|MM|YYYY|CVV"),
    proxy: str | None = Query(None, description="ip:port:user:pass or ip:port"),
    variant: str | None = Query(None, description="Override product variant ID"),
):
    try:
        parts = parse_cc_string(cc)
    except ValueError as e:
        return JSONResponse({"error": str(e), "status": False}, status_code=400)

    success, message, gateway, price, currency = await process_card(
        parts["cc"], parts["mes"], parts["ano"], parts["cvv"],
        site, variant, proxy,
    )
    clean = extract_clean_response(message)
    try:
        price_f = float(price) if str(price).replace(".", "", 1).lstrip("-").isdigit() else 0.0
    except:
        price_f = 0.0

    status_str = "Charged" if success and "ORDER_PLACED" in message else (
        "Approved" if success else "Dead"
    )
    return {
        "Gateway": gateway,
        "Price": price_f,
        "Currency": currency,
        "Response": clean,
        "Status": status_str,
        "cc": cc,
    }


# ─── / root ───────────────────────────────────────────────────────────────
@app.get("/")
async def root():
    return {"status": "ok", "active": stats["active_requests"]}


# ─── /stats (public summary) ──────────────────────────────────────────────
@app.get("/stats")
async def get_stats():
    uptime = int(time.time() - stats["start_time"])
    return {
        "uptime_seconds": uptime,
        "total_checked": stats["total_checked"],
        "active_requests": stats["active_requests"],
        "charged": stats["charged"],
        "approved": stats["approved"],
        "dead": stats["dead"],
        "errors": stats["errors"],
    }


# ─── /admin  (token-protected HTML dashboard) ─────────────────────────────
@app.get("/admin", response_class=HTMLResponse)
async def admin_panel(
    credentials: HTTPAuthorizationCredentials = Depends(security),
):
    if not credentials or credentials.credentials != ADMIN_TOKEN:
        return HTMLResponse(
            '<html><body style="font-family:monospace;padding:2rem">'
            '<h2>🔒 Unauthorized</h2>'
            '<p>Pass <code>Authorization: Bearer &lt;ADMIN_TOKEN&gt;</code></p>'
            "</body></html>",
            status_code=401,
        )

    uptime = int(time.time() - stats["start_time"])
    h, rem = divmod(uptime, 3600)
    m, s = divmod(rem, 60)
    uptime_str = f"{h}h {m}m {s}s"
    total = stats["total_checked"] or 1
    cps = round(stats["total_checked"] / max(uptime, 1), 2)

    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<meta http-equiv="refresh" content="5">
<title>Shopify Checker – Admin</title>
<style>
  *{{box-sizing:border-box;margin:0;padding:0}}
  body{{background:#0d1117;color:#e6edf3;font-family:'Segoe UI',sans-serif;padding:1.5rem}}
  h1{{font-size:1.4rem;margin-bottom:1.5rem;color:#58a6ff}}
  .grid{{display:grid;grid-template-columns:repeat(auto-fill,minmax(200px,1fr));gap:1rem;margin-bottom:1.5rem}}
  .card{{background:#161b22;border:1px solid #30363d;border-radius:8px;padding:1rem}}
  .card .label{{font-size:.75rem;color:#8b949e;text-transform:uppercase;letter-spacing:.05em;margin-bottom:.4rem}}
  .card .value{{font-size:1.8rem;font-weight:700}}
  .charged{{color:#3fb950}}.approved{{color:#e3b341}}.dead{{color:#f85149}}.neutral{{color:#58a6ff}}
  table{{width:100%;border-collapse:collapse;background:#161b22;border-radius:8px;overflow:hidden}}
  th,td{{padding:.7rem 1rem;text-align:left;border-bottom:1px solid #30363d;font-size:.875rem}}
  th{{background:#1f2937;color:#8b949e;font-weight:600}}
  tr:last-child td{{border-bottom:none}}
  .badge{{display:inline-block;padding:.2rem .6rem;border-radius:99px;font-size:.75rem;font-weight:600}}
  .b-green{{background:#1a3b22;color:#3fb950}}
  .b-yellow{{background:#3b2f1a;color:#e3b341}}
  .b-red{{background:#3b1a1a;color:#f85149}}
  footer{{margin-top:1.5rem;color:#8b949e;font-size:.75rem}}
</style>
</head>
<body>
<h1>⚡ Shopify Checker — Admin Panel</h1>

<div class="grid">
  <div class="card">
    <div class="label">Uptime</div>
    <div class="value neutral" style="font-size:1.2rem">{uptime_str}</div>
  </div>
  <div class="card">
    <div class="label">Total Checked</div>
    <div class="value neutral">{stats['total_checked']:,}</div>
  </div>
  <div class="card">
    <div class="label">Active Now</div>
    <div class="value neutral">{stats['active_requests']:,}</div>
  </div>
  <div class="card">
    <div class="label">Checks / sec</div>
    <div class="value neutral">{cps}</div>
  </div>
  <div class="card">
    <div class="label">Charged 💎</div>
    <div class="value charged">{stats['charged']:,}</div>
  </div>
  <div class="card">
    <div class="label">Approved 🔥</div>
    <div class="value approved">{stats['approved']:,}</div>
  </div>
  <div class="card">
    <div class="label">Dead ❌</div>
    <div class="value dead">{stats['dead']:,}</div>
  </div>
  <div class="card">
    <div class="label">Errors ⚠️</div>
    <div class="value dead">{stats['errors']:,}</div>
  </div>
</div>

<table>
  <tr><th>Metric</th><th>Value</th></tr>
  <tr><td>Hit Rate (Charged)</td><td><span class="badge b-green">{stats['charged']/total*100:.1f}%</span></td></tr>
  <tr><td>Live Rate (Approved)</td><td><span class="badge b-yellow">{stats['approved']/total*100:.1f}%</span></td></tr>
  <tr><td>Dead Rate</td><td><span class="badge b-red">{stats['dead']/total*100:.1f}%</span></td></tr>
  <tr><td>Error Rate</td><td><span class="badge b-red">{stats['errors']/total*100:.1f}%</span></td></tr>
  <tr><td>Server Time</td><td>{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</td></tr>
</table>

<footer>Auto-refreshes every 5s &nbsp;·&nbsp; Shopify Checker v3</footer>
</body>
</html>"""
    return HTMLResponse(html)


# ─── /admin/reset (clear stats) ───────────────────────────────────────────
@app.post("/admin/reset")
async def reset_stats(credentials: HTTPAuthorizationCredentials = Depends(security)):
    if not credentials or credentials.credentials != ADMIN_TOKEN:
        raise HTTPException(status_code=401, detail="Unauthorized")
    for k in ("total_checked", "charged", "approved", "dead", "errors"):
        stats[k] = 0
    stats["start_time"] = time.time()
    return {"ok": True, "message": "Stats reset"}


# ─── Main ─────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        "server:app",
        host="0.0.0.0",
        port=PORT,
        loop="uvloop",
        http="httptools",
        workers=1,
        backlog=65535,       # max OS backlog queue
        timeout_keep_alive=120,
        access_log=False,
    )
