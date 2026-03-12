"""
Business 3 — Dropship Product Finder (CJDropshipping)
Finds trending home & lifestyle products via CJ API, scores them with Claude AI,
saves real products to SQLite DB. Runs via GitHub Actions 2x/week.
"""

import os, json, time, sqlite3, logging, requests, random
from datetime import datetime
from pathlib import Path
import anthropic

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

# ── Env ────────────────────────────────────────────────────────────────────────
ANTHROPIC_API_KEY = os.environ["ANTHROPIC_API_KEY"]
CJ_API_KEY        = os.environ.get("CJ_API_KEY", "")  # Format: CJUserNum@api@xxxx from CJ My Account > Developer
# Legacy (unused — CJ deprecated email/password auth in 2024)
CJ_EMAIL          = os.environ.get("CJ_EMAIL", "")
CJ_PASSWORD       = os.environ.get("CJ_PASSWORD", "")
PRODUCTS_PER_RUN  = int(os.environ.get("PRODUCTS_PER_RUN", "20"))
DB_PATH           = os.environ.get("DB_PATH", "data/dropship.db")

Path("data").mkdir(exist_ok=True)
HEARTBEAT = Path("b3_product_heartbeat.json")

CJ_BASE = "https://developers.cjdropshipping.com/api2.0/v1"

# ── Niches — Aesthetic Home & Lifestyle ───────────────────────────────────────
NICHES = [
    "home decor",
    "LED lighting",
    "kitchen organizer",
    "smart home gadgets",
    "wall art",
    "storage solutions",
]

# ── CJDropshipping Auth ────────────────────────────────────────────────────────
def cj_get_token() -> str | None:
    """Authenticate with CJDropshipping API using apiKey. Returns access token or None.
    Get your apiKey from: https://www.cjdropshipping.com/my.html#/developer
    Format: CJUserNum@api@xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx
    """
    if not CJ_API_KEY:
        log.warning("CJ_API_KEY not set — using mock data. Get key at cjdropshipping.com > My Account > Developer")
        return None
    try:
        resp = requests.post(
            f"{CJ_BASE}/authentication/getAccessToken",
            json={"apiKey": CJ_API_KEY},
            timeout=15
        )
        data = resp.json()
        if data.get("result") is True:
            token = data.get("data", {}).get("accessToken")
            log.info("CJ auth: token obtained")
            return token
        else:
            log.error(f"CJ auth failed: {data.get('message', 'unknown error')}")
            return None
    except Exception as e:
        log.error(f"CJ auth error: {e}")
        return None

# ── CJDropshipping Product Search ─────────────────────────────────────────────
def cj_search_products(token: str | None, keyword: str, page: int = 1) -> list:
    """Search CJ for products. Returns list of product dicts."""
    if not token:
        log.warning(f"  No CJ token — using mock data for '{keyword}'")
        return _mock_products(keyword)
    try:
        resp = requests.get(
            f"{CJ_BASE}/product/list",
            headers={"CJ-Access-Token": token},
            params={
                "keyword": keyword,
                "pageNum": page,
                "pageSize": 20,
            },
            timeout=15
        )
        data = resp.json()
        if data.get("result") is True:
            products = data.get("data", {}).get("list", [])
            log.info(f"  CJ returned {len(products)} products for '{keyword}'")
            return products
        else:
            log.warning(f"  CJ search failed for '{keyword}': {data.get('message')}")
            return []
    except Exception as e:
        log.error(f"CJ search error: {e}")
        return []

# ── CJDropshipping Product Detail ─────────────────────────────────────────────
def cj_get_product_detail(token: str, product_id: str) -> dict | None:
    """Fetch full product detail from CJ API."""
    if not token:
        return None
    try:
        resp = requests.get(
            f"{CJ_BASE}/product/query",
            headers={"CJ-Access-Token": token},
            params={"pid": product_id},
            timeout=15
        )
        data = resp.json()
        if data.get("result") is True:
            return data.get("data")
    except Exception as e:
        log.error(f"CJ product detail error: {e}")
    return None

# ── Mock data (fallback when CJ_EMAIL/CJ_PASSWORD not set) ───────────────────
# Realistic products to allow testing without a CJDropshipping account.
# Replace with real CJ credentials when ready: add CJ_EMAIL + CJ_PASSWORD secrets.
MOCK_PRODUCTS_BY_NICHE = {
    "home decor": [
        {"n": "Modern Minimalist Ceramic Vase Set (3 Pack)", "p": 12.50},
        {"n": "Boho Macrame Wall Hanging Handwoven Cotton Tapestry", "p": 8.99},
        {"n": "Aesthetic Candle Holders Set Gold Geometric Design", "p": 9.80},
        {"n": "Rattan Wicker Storage Basket with Handles Set of 2", "p": 11.20},
    ],
    "led lighting": [
        {"n": "RGB LED Strip Lights 10M Smart App Control Music Sync", "p": 14.99},
        {"n": "Aesthetic Moon Lamp 3D Print Night Light 16 Colors USB", "p": 16.50},
        {"n": "Neon Sign LED Flexible Light Bar Room Decor USB Powered", "p": 22.00},
        {"n": "Plug-in Fairy String Lights 10M 100 LEDs Warm White Indoor", "p": 6.80},
    ],
    "kitchen organizer": [
        {"n": "Bamboo Drawer Organizer Divider Set 6-Piece Adjustable", "p": 10.20},
        {"n": "Rotating Spice Rack Organizer 360 Degree Turntable 24-jar", "p": 13.50},
        {"n": "Magnetic Knife Strip Wall Mount Kitchen Storage Stainless", "p": 9.99},
        {"n": "Over Sink Dish Drying Rack Stainless Steel Adjustable", "p": 18.75},
    ],
    "smart home gadgets": [
        {"n": "WiFi Smart Plug 16A Energy Monitor Alexa Google Compatible", "p": 8.50},
        {"n": "Smart Universal IR Remote Hub WiFi App Alexa Control", "p": 12.99},
        {"n": "Mini Smart Temperature Humidity Sensor Display WiFi Zigbee", "p": 7.20},
        {"n": "Smart Door Window Sensor Alarm with App Notification Alert", "p": 6.80},
    ],
    "wall art": [
        {"n": "Abstract Canvas Wall Art Nordic Minimalist Botanical Print", "p": 7.50},
        {"n": "Retro Sunset Mountain Landscape Poster Print Set of 3", "p": 9.99},
        {"n": "Green Leaf Botanical Prints Modern Boho Gallery Wall Set", "p": 8.20},
        {"n": "Inspirational Quote Framed Wall Art Motivational Home Decor", "p": 11.00},
    ],
    "storage solutions": [
        {"n": "Clear Stackable Storage Boxes with Lids Set of 6 Acrylic", "p": 14.99},
        {"n": "Fabric Cube Storage Bins Organizer Foldable Closet Baskets", "p": 19.50},
        {"n": "Over Door Hanging Organizer 16 Pocket Shoe Rack Bag Clear", "p": 8.80},
        {"n": "Vacuum Storage Bags Space Saver Compression Bags 12-Pack", "p": 12.20},
    ],
}

def _mock_products(keyword: str) -> list:
    kw = keyword.lower()
    for niche_key, items in MOCK_PRODUCTS_BY_NICHE.items():
        if niche_key in kw or any(w in kw for w in niche_key.split()):
            return [{
                "pid": f"mock_{niche_key[:6].replace(' ','_')}_{i}",
                "productNameEn": item["n"],
                "sellPrice": item["p"],
                "categoryName": niche_key.title(),
                "productImage": f"https://placehold.co/600x600/f5f0eb/4a4a4a?text={item['n'][:20].replace(' ', '+')}",
                "isStock": "YES",
                "variants": [{"variantSellPrice": item["p"]}]
            } for i, item in enumerate(items)]
    return [{
        "pid": f"mock_generic_{i}",
        "productNameEn": f"Premium {keyword.title()} Home Lifestyle Item {i+1}",
        "sellPrice": round(9.0 + i * 3.0, 2),
        "categoryName": keyword.title(),
        "productImage": f"https://placehold.co/600x600/f5f0eb/4a4a4a?text={keyword.replace(' ', '+')}",
        "isStock": "YES",
        "variants": [{"variantSellPrice": round(9.0 + i * 3.0, 2)}]
    } for i in range(4)]

# ── Database ───────────────────────────────────────────────────────────────────
def init_db() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS products (
            id            INTEGER PRIMARY KEY AUTOINCREMENT,
            cj_id         TEXT UNIQUE,
            title         TEXT,
            niche         TEXT,
            cost_usd      REAL,
            sell_price    REAL,
            profit_margin REAL,
            image_url     TEXT,
            product_url   TEXT,
            ai_description TEXT,
            ai_tags       TEXT,
            ai_score      INTEGER DEFAULT 0,
            shopify_id    TEXT,
            status        TEXT DEFAULT 'pending',
            created_at    TEXT DEFAULT (datetime('now'))
        )
    """)
    conn.commit()
    return conn

# ── Claude AI scoring ──────────────────────────────────────────────────────────
def ai_score_and_describe(client, product: dict, niche: str) -> dict:
    title = product.get("productNameEn", "Unknown product")
    raw_price = product.get("sellPrice") or (product.get("variants") or [{}])[0].get("variantSellPrice", 10)
    cost  = float(str(raw_price).split("--")[0].strip() if "--" in str(raw_price) else raw_price)

    prompt = f"""Evaluate this dropshipping product for a home & lifestyle store called VibeFinds.

Product: {title}
Niche: {niche}
Cost price: ${cost:.2f}
Category: {product.get('categoryName', niche)}

Score it and write store content. Return ONLY valid JSON:
{{
  "score": <1-10 dropshipping viability score>,
  "sell_price": <recommended USD retail price as float, 2.5x markup on cost, minimum $15>,
  "description": <compelling 120-word Shopify product description, benefits-first>,
  "tags": <comma-separated 6 SEO tags relevant to home/lifestyle>,
  "skip": <true if unsuitable for a home decor / lifestyle store, false otherwise>
}}"""

    for attempt in range(3):
        try:
            msg = client.messages.create(
                model="claude-sonnet-4-20250514",
                max_tokens=500,
                messages=[{"role": "user", "content": prompt}]
            )
            raw = msg.content[0].text.strip()
            raw = raw.replace("```json", "").replace("```", "").strip()
            return json.loads(raw)
        except Exception as e:
            log.warning(f"Claude attempt {attempt+1} failed: {e}")
            time.sleep(2 ** attempt)
    return {"score": 0, "sell_price": cost * 2.5, "description": "", "tags": "", "skip": True}

# ── Save to DB ─────────────────────────────────────────────────────────────────
def save_product(conn, product: dict, ai: dict, niche: str) -> bool:
    raw_price = product.get("sellPrice") or (product.get("variants") or [{}])[0].get("variantSellPrice", 10)
    cost = float(str(raw_price).split("--")[0].strip() if "--" in str(raw_price) else raw_price)
    sell = float(ai.get("sell_price", cost * 2.5))
    margin = round((sell - cost) / max(sell, 0.01) * 100, 1)

    image = (product.get("productImage") or
             product.get("imageUrl") or "")

    cj_id = str(product.get("pid") or product.get("productId") or "")
    product_url = f"https://app.cjdropshipping.com/product-detail.html?id={cj_id}"

    try:
        conn.execute("""
            INSERT OR IGNORE INTO products
            (cj_id, title, niche, cost_usd, sell_price, profit_margin,
             image_url, product_url, ai_description, ai_tags, ai_score)
            VALUES (?,?,?,?,?,?,?,?,?,?,?)
        """, (
            cj_id,
            (product.get("productNameEn") or "")[:200],
            niche,
            round(cost, 2),
            round(sell, 2),
            margin,
            image,
            product_url,
            ai.get("description", ""),
            ai.get("tags", ""),
            int(ai.get("score", 0))
        ))
        conn.commit()
        return True
    except Exception as e:
        log.error(f"DB save error: {e}")
        return False

# ── Heartbeat ──────────────────────────────────────────────────────────────────
def write_heartbeat(products_found: int, status: str = "success"):
    HEARTBEAT.write_text(json.dumps({
        "module": "b3_product_finder",
        "last_run": datetime.now().isoformat(),
        "products_found": products_found,
        "status": status
    }, indent=2))

# ── Main ───────────────────────────────────────────────────────────────────────
def main():
    log.info("=" * 60)
    log.info("B3 Product Finder — CJDropshipping")
    log.info("=" * 60)

    try:
        client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)
        conn   = init_db()
        token  = cj_get_token()

        total_saved = 0
        # Search all 6 niches each run
        niches_to_search = NICHES[:]
        random.shuffle(niches_to_search)
        log.info(f"Scanning niches: {niches_to_search}")

        for niche in niches_to_search:
            if total_saved >= PRODUCTS_PER_RUN:
                break
            log.info(f"Searching niche: {niche}")
            products = cj_search_products(token, niche)

            for product in products:
                if total_saved >= PRODUCTS_PER_RUN:
                    break
                if product.get("isStock") == "NO":
                    continue

                ai = ai_score_and_describe(client, product, niche)
                if ai.get("skip") or int(ai.get("score", 0)) < 6:
                    continue

                if save_product(conn, product, ai, niche):
                    total_saved += 1
                    sell = ai.get("sell_price", 0)
                    log.info(f"  + {product.get('productNameEn','')[:55]} | score={ai.get('score')} | ${sell:.2f}")
                time.sleep(0.5)

            time.sleep(2)

        pending = conn.execute("SELECT COUNT(*) FROM products WHERE status='pending'").fetchone()[0]
        log.info(f"Done. Saved {total_saved} new products. Total pending: {pending}")
        write_heartbeat(total_saved)
        conn.close()

    except Exception as e:
        log.error(f"Product finder failed: {e}")
        write_heartbeat(0, status=f"error: {e}")
        raise

if __name__ == "__main__":
    main()
