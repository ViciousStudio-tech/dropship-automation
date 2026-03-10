"""
Business 3 — Shopify Store Manager
Takes pending products from DB, lists them on Shopify with AI descriptions.
Runs via GitHub Actions daily.
"""

import os
import json
import time
import sqlite3
import logging
import requests
from datetime import datetime
import anthropic

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

# ── Env ────────────────────────────────────────────────────────────────────────
ANTHROPIC_API_KEY    = os.environ["ANTHROPIC_API_KEY"]
SHOPIFY_STORE        = os.environ.get("SHOPIFY_STORE", "fgtyz6-bj.myshopify.com")
SHOPIFY_ACCESS_TOKEN = os.environ["SHOPIFY_ACCESS_TOKEN"]
LISTINGS_PER_RUN     = int(os.environ.get("LISTINGS_PER_RUN", "10"))
DB_PATH              = "dropship.db"

SHOPIFY_HEADERS = {
    "X-Shopify-Access-Token": SHOPIFY_ACCESS_TOKEN,
    "Content-Type": "application/json"
}
SHOPIFY_BASE = f"https://{SHOPIFY_STORE}/admin/api/2024-10"

# ── DB helpers ─────────────────────────────────────────────────────────────────
def get_pending_products(conn, limit: int) -> list:
    rows = conn.execute("""
        SELECT id, aliexpress_id, title, niche, cost_usd, sell_price,
               image_url, product_url, ai_description, ai_tags
        FROM products
        WHERE status = 'pending'
        ORDER BY created_at ASC
        LIMIT ?
    """, (limit,)).fetchall()
    return [dict(zip(
        ["id","aliexpress_id","title","niche","cost_usd","sell_price",
         "image_url","product_url","ai_description","ai_tags"], row
    )) for row in rows]

def mark_listed(conn, product_id: int, shopify_id: str):
    conn.execute("""
        UPDATE products SET status='listed', shopify_id=?
        WHERE id=?
    """, (shopify_id, product_id))
    conn.commit()

# ── Shopify API ────────────────────────────────────────────────────────────────
def create_shopify_product(product: dict) -> str | None:
    """Create product on Shopify. Returns Shopify product ID or None."""
    tags = product.get("ai_tags", product.get("niche", ""))
    body = {
        "product": {
            "title": product["title"],
            "body_html": f"<p>{product['ai_description']}</p>",
            "vendor": "Vicious Dropship",
            "product_type": product["niche"].title(),
            "tags": tags,
            "status": "active",
            "variants": [{
                "price": str(round(product["sell_price"], 2)),
                "compare_at_price": str(round(product["sell_price"] * 1.2, 2)),
                "inventory_management": None,
                "fulfillment_service": "manual",
                "requires_shipping": True,
                "taxable": True
            }],
            "images": [{"src": product["image_url"]}] if product.get("image_url") else [],
            "metafields": [{
                "namespace": "dropship",
                "key": "aliexpress_url",
                "value": product.get("product_url", ""),
                "type": "url"
            }, {
                "namespace": "dropship",
                "key": "cost_price",
                "value": str(product["cost_usd"]),
                "type": "number_decimal"
            }]
        }
    }

    for attempt in range(3):
        try:
            resp = requests.post(
                f"{SHOPIFY_BASE}/products.json",
                headers=SHOPIFY_HEADERS,
                json=body,
                timeout=20
            )
            if resp.status_code == 201:
                shopify_id = str(resp.json()["product"]["id"])
                log.info(f"  Listed: {product['title'][:50]} → Shopify #{shopify_id}")
                return shopify_id
            elif resp.status_code == 429:
                log.warning("Rate limited — waiting 10s")
                time.sleep(10)
            else:
                log.error(f"Shopify error {resp.status_code}: {resp.text[:200]}")
                return None
        except Exception as e:
            log.error(f"Request error attempt {attempt+1}: {e}")
            time.sleep(2 ** attempt)
    return None

# ── Ensure shopify_id column exists ───────────────────────────────────────────
def migrate_db(conn):
    try:
        conn.execute("ALTER TABLE products ADD COLUMN shopify_id TEXT")
        conn.commit()
    except Exception:
        pass  # Column already exists

# ── Heartbeat ──────────────────────────────────────────────────────────────────
def write_heartbeat(listed: int):
    with open("b3_store_heartbeat.json", "w") as f:
        json.dump({"last_run": datetime.now().isoformat(), "listed": listed, "status": "ok"}, f)

# ── Main ───────────────────────────────────────────────────────────────────────
def main():
    conn = sqlite3.connect(DB_PATH)
    migrate_db(conn)
    products = get_pending_products(conn, LISTINGS_PER_RUN)
    log.info(f"Listing {len(products)} products to Shopify store: {SHOPIFY_STORE}")

    listed = 0
    for product in products:
        shopify_id = create_shopify_product(product)
        if shopify_id:
            mark_listed(conn, product["id"], shopify_id)
            listed += 1
        time.sleep(0.5)  # Shopify rate limit: 2 req/s

    log.info(f"Done. Listed {listed}/{len(products)} products.")
    write_heartbeat(listed)

if __name__ == "__main__":
    main()
