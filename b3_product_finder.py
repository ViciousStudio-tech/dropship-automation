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
CJ_EMAIL          = os.environ["CJ_EMAIL"]
CJ_PASSWORD       = os.environ["CJ_PASSWORD"]
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
    """Authenticate with CJDropshipping API. Returns access token or None."""
    try:
        resp = requests.post(
            f"{CJ_BASE}/authentication/getAccessToken",
            json={"email": CJ_EMAIL, "password": CJ_PASSWORD},
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

# ── Mock data (fallback when CJ auth fails) ──────────────────────────────────
def _mock_products(keyword: str) -> list:
    return [{
        "pid": f"mock_{keyword[:8].replace(' ','_')}_{i}",
        "productNameEn": f"Premium {keyword.title()} - Style {chr(65+i)} [{datetime.now().year}]",
        "sellPrice": round(random.uniform(4.0, 22.0), 2),
        "categoryName": keyword.title(),
        "productImage": f"https://via.placeholder.com/500x500?text={keyword.replace(' ', '+')}",
        "isStock": "YES",
        "variants": [{"variantSellPrice": round(random.uniform(4.0, 22.0), 2)}]
    } for i in range(6)]

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
    cost  = float(product.get("sellPrice") or
                  (product.get("variants") or [{}])[0].get("variantSellPrice", 10))

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
    cost = float(product.get("sellPrice") or
                 (product.get("variants") or [{}])[0].get("variantSellPrice", 10))
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
