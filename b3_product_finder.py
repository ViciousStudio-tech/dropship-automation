"""
Business 3 — Dropship Product Finder
Finds trending products on AliExpress, scores them, saves to DB for listing.
Runs via GitHub Actions 2x/week.
"""

import os
import json
import time
import sqlite3
import logging
import hashlib
import hmac
import requests
from datetime import datetime
import anthropic

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

# ── Env ────────────────────────────────────────────────────────────────────────
ANTHROPIC_API_KEY   = os.environ["ANTHROPIC_API_KEY"]
ALIEXPRESS_APP_KEY  = os.environ.get("ALIEXPRESS_APP_KEY", "")
ALIEXPRESS_SECRET   = os.environ.get("ALIEXPRESS_SECRET", "")
PRODUCTS_PER_RUN    = int(os.environ.get("PRODUCTS_PER_RUN", "20"))
DB_PATH             = "dropship.db"

# ── Niches to scan ──────────────────────────────────────────────────────────────
NICHES = [
    "home gadgets", "kitchen tools", "fitness accessories", "pet supplies",
    "desk organization", "phone accessories", "outdoor camping gear",
    "beauty tools", "car accessories", "baby products", "garden tools",
    "travel accessories", "lighting decor", "storage solutions", "craft supplies"
]

# ── Database ───────────────────────────────────────────────────────────────────
def init_db():
    conn = sqlite3.connect(DB_PATH)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS products (
            id            INTEGER PRIMARY KEY AUTOINCREMENT,
            aliexpress_id TEXT UNIQUE,
            title         TEXT,
            niche         TEXT,
            cost_usd      REAL,
            sell_price    REAL,
            profit_margin REAL,
            rating        REAL,
            orders_count  INTEGER,
            image_url     TEXT,
            product_url   TEXT,
            ai_description TEXT,
            ai_tags       TEXT,
            status        TEXT DEFAULT 'pending',
            created_at    TEXT DEFAULT (datetime('now'))
        )
    """)
    conn.commit()
    return conn

# ── AliExpress API ─────────────────────────────────────────────────────────────
def aliexpress_sign(params: dict, secret: str) -> str:
    """Generate AliExpress API signature."""
    sorted_params = sorted(params.items())
    sign_str = secret + "".join(f"{k}{v}" for k, v in sorted_params) + secret
    return hmac.new(secret.encode(), sign_str.encode(), hashlib.md5).hexdigest().upper()

def search_aliexpress_products(niche: str, page: int = 1) -> list:
    """Search AliExpress for trending products in a niche."""
    if not ALIEXPRESS_APP_KEY or not ALIEXPRESS_SECRET:
        log.warning("AliExpress credentials not set — using mock data")
        return _mock_products(niche)

    params = {
        "method": "aliexpress.affiliate.product.query",
        "app_key": ALIEXPRESS_APP_KEY,
        "sign_method": "md5",
        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "format": "json",
        "v": "2.0",
        "keywords": niche,
        "sort": "SALE_PRICE_ASC",
        "min_sale_price": "100",    # cents
        "max_sale_price": "5000",   # cents ($50 max cost)
        "page_no": str(page),
        "page_size": "20",
        "target_currency": "USD",
        "target_language": "EN",
        "tracking_id": "dropship",
        "fields": "product_id,product_title,sale_price,original_price,evaluate_rate,lastest_volume,product_main_image_url,product_detail_url"
    }
    params["sign"] = aliexpress_sign(params, ALIEXPRESS_SECRET)

    try:
        resp = requests.get("https://gw.api.taobao.com/router/rest", params=params, timeout=15)
        data = resp.json()
        products = data.get("aliexpress_affiliate_product_query_response", {}) \
                       .get("resp_result", {}) \
                       .get("result", {}) \
                       .get("products", {}) \
                       .get("product", [])
        return products
    except Exception as e:
        log.error(f"AliExpress API error: {e}")
        return _mock_products(niche)

def _mock_products(niche: str) -> list:
    """Fallback mock data when API keys not configured."""
    import random
    return [{
        "product_id": f"mock_{niche[:5]}_{i}_{int(time.time())}",
        "product_title": f"Premium {niche.title()} Item {i+1} - Best Seller 2026",
        "sale_price": round(random.uniform(3.0, 25.0), 2),
        "evaluate_rate": round(random.uniform(4.2, 4.9), 1),
        "lastest_volume": random.randint(100, 5000),
        "product_main_image_url": f"https://via.placeholder.com/400?text={niche.replace(' ', '+')}",
        "product_detail_url": f"https://aliexpress.com/item/{i}.html"
    } for i in range(5)]

# ── Claude AI product scoring & description ────────────────────────────────────
def ai_score_and_describe(client, product: dict, niche: str) -> dict:
    """Use Claude to score product potential and write a store description."""
    prompt = f"""You are a dropshipping expert. Analyze this AliExpress product and create store content.

Product: {product.get('product_title', 'Unknown')}
Niche: {niche}
Cost price: ${product.get('sale_price', 0):.2f}
Rating: {product.get('evaluate_rate', 0)}/5
Orders: {product.get('lastest_volume', 0)}

Return ONLY valid JSON with these exact keys:
{{
  "score": <1-10 dropshipping potential score>,
  "sell_price": <recommended USD sell price as float, 2.5-3.5x cost>,
  "description": <compelling 150-word product description for Shopify store>,
  "tags": <comma-separated list of 5-8 SEO tags>,
  "skip": <true if product is not suitable, false otherwise>
}}"""

    for attempt in range(3):
        try:
            msg = client.messages.create(
                model="claude-opus-4-5",
                max_tokens=500,
                messages=[{"role": "user", "content": prompt}]
            )
            raw = msg.content[0].text.strip()
            if raw.startswith("```"):
                raw = raw.split("```")[1]
                if raw.startswith("json"):
                    raw = raw[4:]
            return json.loads(raw)
        except Exception as e:
            log.warning(f"Claude attempt {attempt+1} failed: {e}")
            time.sleep(2 ** attempt)
    return {"score": 0, "sell_price": 0, "description": "", "tags": "", "skip": True}

# ── Save to DB ─────────────────────────────────────────────────────────────────
def save_product(conn, product: dict, ai: dict, niche: str) -> bool:
    cost = float(product.get("sale_price", 0))
    try:
        conn.execute("""
            INSERT OR IGNORE INTO products
            (aliexpress_id, title, niche, cost_usd, sell_price, profit_margin,
             rating, orders_count, image_url, product_url, ai_description, ai_tags)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
        """, (
            str(product.get("product_id", "")),
            product.get("product_title", "")[:200],
            niche,
            cost,
            ai.get("sell_price", cost * 3),
            round((ai.get("sell_price", cost * 3) - cost) / max(ai.get("sell_price", cost * 3), 0.01) * 100, 1),
            float(product.get("evaluate_rate", 0)),
            int(product.get("lastest_volume", 0)),
            product.get("product_main_image_url", ""),
            product.get("product_detail_url", ""),
            ai.get("description", ""),
            ai.get("tags", "")
        ))
        conn.commit()
        return True
    except Exception as e:
        log.error(f"DB save error: {e}")
        return False

# ── Heartbeat ──────────────────────────────────────────────────────────────────
def write_heartbeat(products_found: int):
    with open("b3_product_heartbeat.json", "w") as f:
        json.dump({
            "last_run": datetime.now().isoformat(),
            "products_found": products_found,
            "status": "ok"
        }, f)

# ── Main ───────────────────────────────────────────────────────────────────────
def main():
    import random
    client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)
    conn = init_db()
    total_saved = 0

    selected_niches = random.sample(NICHES, 4)
    log.info(f"Scanning niches: {selected_niches}")

    for niche in selected_niches:
        if total_saved >= PRODUCTS_PER_RUN:
            break
        log.info(f"Searching: {niche}")
        products = search_aliexpress_products(niche)

        for product in products:
            if total_saved >= PRODUCTS_PER_RUN:
                break
            ai = ai_score_and_describe(client, product, niche)
            if ai.get("skip") or ai.get("score", 0) < 6:
                continue
            if save_product(conn, product, ai, niche):
                total_saved += 1
                log.info(f"  Saved: {product.get('product_title','')[:60]} | score={ai.get('score')} | sell=${ai.get('sell_price'):.2f}")
            time.sleep(1)

    pending = conn.execute("SELECT COUNT(*) FROM products WHERE status='pending'").fetchone()[0]
    log.info(f"Done. Saved {total_saved} products. Total pending: {pending}")
    write_heartbeat(total_saved)

if __name__ == "__main__":
    main()
