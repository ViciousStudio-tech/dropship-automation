"""
Business 3 — Order Fulfiller (CJDropshipping)
Checks Shopify for paid unfulfilled orders, routes them to CJ for fulfillment.
Runs every 4 hours via GitHub Actions.
"""

import os, json, time, sqlite3, logging, requests, smtplib
from datetime import datetime, timedelta
from pathlib import Path
from email.mime.text import MIMEText

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

# ── Env ────────────────────────────────────────────────────────────────────────
SHOPIFY_STORE        = os.environ.get("SHOPIFY_STORE", "fgtyz6-bj.myshopify.com")
SHOPIFY_ACCESS_TOKEN = os.environ["SHOPIFY_ACCESS_TOKEN"]
CJ_EMAIL             = os.environ.get("CJ_EMAIL", "")
CJ_PASSWORD          = os.environ.get("CJ_PASSWORD", "")
GMAIL_SENDER         = os.environ.get("GMAIL_SENDER", "")
GMAIL_APP_PASSWORD   = os.environ.get("GMAIL_APP_PASSWORD", "")
GMAIL_TO             = os.environ.get("GMAIL_TO", GMAIL_SENDER)
DB_PATH              = os.environ.get("DB_PATH", "data/dropship.db")

Path("data").mkdir(exist_ok=True)
HEARTBEAT = Path("b3_fulfillment_heartbeat.json")

SHOPIFY_HEADERS = {
    "X-Shopify-Access-Token": SHOPIFY_ACCESS_TOKEN,
    "Content-Type": "application/json"
}
SHOPIFY_BASE = f"https://{SHOPIFY_STORE}/admin/api/2024-10"
CJ_BASE      = "https://developers.cjdropshipping.com/api2.0/v1"

# ── CJ Auth ────────────────────────────────────────────────────────────────────
def cj_get_token() -> str | None:
    if not CJ_EMAIL or not CJ_PASSWORD:
        return None
    try:
        resp = requests.post(
            f"{CJ_BASE}/authentication/getAccessToken",
            json={"email": CJ_EMAIL, "password": CJ_PASSWORD},
            timeout=15
        )
        data = resp.json()
        if data.get("result") is True:
            return data["data"]["accessToken"]
    except Exception as e:
        log.error(f"CJ auth error: {e}")
    return None

# ── DB ─────────────────────────────────────────────────────────────────────────
def init_orders_db(conn):
    conn.execute("""
        CREATE TABLE IF NOT EXISTS orders (
            id                  INTEGER PRIMARY KEY AUTOINCREMENT,
            shopify_order_id    TEXT UNIQUE,
            shopify_order_num   TEXT,
            cj_order_id         TEXT,
            customer_email      TEXT,
            customer_name       TEXT,
            shipping_address    TEXT,
            line_items          TEXT,
            total_revenue       REAL,
            total_cost          REAL,
            profit              REAL,
            status              TEXT DEFAULT 'new',
            tracking_number     TEXT,
            fulfilled_at        TEXT,
            created_at          TEXT DEFAULT (datetime('now'))
        )
    """)
    conn.commit()

# ── Shopify: fetch unfulfilled paid orders ─────────────────────────────────────
def get_unfulfilled_orders() -> list:
    since = (datetime.now() - timedelta(days=7)).strftime("%Y-%m-%dT%H:%M:%SZ")
    try:
        resp = requests.get(
            f"{SHOPIFY_BASE}/orders.json",
            headers=SHOPIFY_HEADERS,
            params={
                "fulfillment_status": "unfulfilled",
                "financial_status": "paid",
                "created_at_min": since,
                "limit": 50,
                "fields": "id,order_number,email,shipping_address,line_items,total_price,created_at"
            },
            timeout=15
        )
        return resp.json().get("orders", [])
    except Exception as e:
        log.error(f"Failed to fetch orders: {e}")
        return []

# ── Get CJ product ID from Shopify product metafields ─────────────────────────
def get_cj_product_id_for_variant(variant_id: str) -> str | None:
    try:
        resp = requests.get(
            f"{SHOPIFY_BASE}/variants/{variant_id}.json",
            headers=SHOPIFY_HEADERS, timeout=10
        )
        product_id = resp.json()["variant"]["product_id"]

        resp2 = requests.get(
            f"{SHOPIFY_BASE}/products/{product_id}/metafields.json",
            headers=SHOPIFY_HEADERS, timeout=10
        )
        for mf in resp2.json().get("metafields", []):
            if mf.get("namespace") == "dropship" and mf.get("key") == "cj_product_id":
                return mf.get("value")
    except Exception as e:
        log.error(f"Metafield lookup failed: {e}")
    return None

# ── CJ: Place order ────────────────────────────────────────────────────────────
def place_cj_order(token: str, order: dict, cj_product_id: str) -> str | None:
    """Place order on CJDropshipping. Returns CJ order ID or None."""
    if not token or not cj_product_id:
        log.info(f"  Manual fulfillment needed — CJ product ID: {cj_product_id}")
        return "MANUAL_QUEUE"

    addr = order.get("shipping_address", {})
    try:
        payload = {
            "orderNumber": f"VF-{order.get('order_number', 'UNKNOWN')}",
            "shippingZip": addr.get("zip", ""),
            "shippingCountry": addr.get("country_code", "US"),
            "shippingCountryCode": addr.get("country_code", "US"),
            "shippingProvince": addr.get("province", ""),
            "shippingCity": addr.get("city", ""),
            "shippingAddress": addr.get("address1", ""),
            "shippingAddress2": addr.get("address2", ""),
            "shippingCustomerName": addr.get("name", ""),
            "shippingPhone": addr.get("phone", "0000000000"),
            "remark": f"VibeFinds order #{order.get('order_number')}",
            "products": [{
                "vid": cj_product_id,
                "quantity": 1
            }]
        }
        resp = requests.post(
            f"{CJ_BASE}/shopping/order/createOrderV2",
            headers={"CJ-Access-Token": token, "Content-Type": "application/json"},
            json=payload,
            timeout=20
        )
        data = resp.json()
        if data.get("result") is True:
            cj_order_id = data.get("data", {}).get("orderId") or data.get("data", {}).get("orderNum")
            log.info(f"  CJ order placed: {cj_order_id}")
            return cj_order_id
        else:
            log.warning(f"  CJ order failed: {data.get('message')} — queuing manual")
            return "MANUAL_QUEUE"
    except Exception as e:
        log.error(f"CJ order error: {e}")
        return "MANUAL_QUEUE"

# ── Shopify: mark as processing ────────────────────────────────────────────────
def note_shopify_order(shopify_order_id: str, note: str):
    """Add internal note to Shopify order."""
    try:
        requests.put(
            f"{SHOPIFY_BASE}/orders/{shopify_order_id}.json",
            headers=SHOPIFY_HEADERS,
            json={"order": {"id": shopify_order_id, "note": note}},
            timeout=10
        )
    except Exception:
        pass

# ── Email alert for manual orders ─────────────────────────────────────────────
def send_manual_alert(orders_needing_manual: list):
    if not orders_needing_manual or not GMAIL_SENDER or not GMAIL_APP_PASSWORD:
        return
    try:
        body = f"Manual fulfillment needed for {len(orders_needing_manual)} order(s):\n\n"
        for o in orders_needing_manual:
            body += f"  Order #{o['num']} — {o['customer']} — ${o['total']}\n"
        body += "\nLog in to Shopify + CJDropshipping to fulfill manually."

        msg = MIMEText(body)
        msg["Subject"] = f"ACTION NEEDED: {len(orders_needing_manual)} manual order(s) — VibeFinds"
        msg["From"] = GMAIL_SENDER
        msg["To"] = GMAIL_TO
        with smtplib.SMTP_SSL("smtp.gmail.com", 465) as smtp:
            smtp.login(GMAIL_SENDER, GMAIL_APP_PASSWORD)
            smtp.send_message(msg)
        log.info("Manual fulfillment alert sent")
    except Exception as e:
        log.error(f"Email alert failed: {e}")

# ── Heartbeat ──────────────────────────────────────────────────────────────────
def write_heartbeat(orders_processed: int, auto_fulfilled: int, revenue: float, status: str = "success"):
    HEARTBEAT.write_text(json.dumps({
        "module": "b3_order_fulfiller",
        "last_run": datetime.now().isoformat(),
        "orders_processed": orders_processed,
        "auto_fulfilled": auto_fulfilled,
        "revenue_usd": round(revenue, 2),
        "status": status
    }, indent=2))

# ── Main ───────────────────────────────────────────────────────────────────────
def main():
    log.info("=" * 60)
    log.info("B3 Order Fulfiller — CJDropshipping")
    log.info("=" * 60)

    conn = sqlite3.connect(DB_PATH)
    init_orders_db(conn)

    token  = cj_get_token()
    orders = get_unfulfilled_orders()
    log.info(f"Found {len(orders)} unfulfilled paid orders")

    fulfilled = 0
    revenue   = 0.0
    manual_needed = []

    for order in orders:
        shopify_order_id = str(order["id"])

        # Skip already processed
        if conn.execute("SELECT id FROM orders WHERE shopify_order_id=?",
                        (shopify_order_id,)).fetchone():
            continue

        log.info(f"Processing order #{order.get('order_number')} — {order.get('email')}")

        cj_order_id = None
        for item in order.get("line_items", []):
            cj_pid = get_cj_product_id_for_variant(str(item["variant_id"]))
            if cj_pid:
                cj_order_id = place_cj_order(token, order, cj_pid)
                break  # One CJ call per order

        total_revenue  = float(order.get("total_price", 0))
        estimated_cost = total_revenue * 0.35
        profit         = total_revenue - estimated_cost

        conn.execute("""
            INSERT OR IGNORE INTO orders
            (shopify_order_id, shopify_order_num, cj_order_id,
             customer_email, customer_name, shipping_address,
             line_items, total_revenue, total_cost, profit, status)
            VALUES (?,?,?,?,?,?,?,?,?,?,?)
        """, (
            shopify_order_id,
            str(order.get("order_number", "")),
            cj_order_id or "",
            order.get("email", ""),
            order.get("shipping_address", {}).get("name", ""),
            json.dumps(order.get("shipping_address", {})),
            json.dumps(order.get("line_items", [])),
            total_revenue, estimated_cost, profit,
            "processing" if cj_order_id and cj_order_id != "MANUAL_QUEUE" else "manual_needed"
        ))
        conn.commit()

        if cj_order_id == "MANUAL_QUEUE":
            manual_needed.append({
                "num": order.get("order_number"),
                "customer": order.get("shipping_address", {}).get("name", ""),
                "total": total_revenue
            })
            note_shopify_order(shopify_order_id, "MANUAL FULFILLMENT NEEDED — CJ order not placed")
        elif cj_order_id:
            fulfilled += 1
            note_shopify_order(shopify_order_id, f"CJ Order ID: {cj_order_id}")

        revenue += total_revenue
        time.sleep(0.5)

    # Alert for manual orders
    if manual_needed:
        send_manual_alert(manual_needed)

    log.info(f"Done. Processed {len(orders)} orders. Auto-fulfilled: {fulfilled}. Manual: {len(manual_needed)}. Revenue: ${revenue:.2f}")
    write_heartbeat(len(orders), fulfilled, revenue)
    conn.close()

if __name__ == "__main__":
    main()
