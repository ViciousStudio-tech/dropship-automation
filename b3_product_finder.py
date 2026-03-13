"""
Business 3 — Dropship Product Finder (CJDropshipping)
EdisonHaus theme: Warm Ambient Home Lighting & Decor. Warm, aesthetic, mood-driven home products anchored by lighting. Table lamps, pendant lights, LED strips, fairy lights, string lights, sunset lamps, galaxy projectors. Supporting products: wall art/prints, throw pillows, woven storage, candle holders, vases. Cozy, aesthetic, mood-driven home products. Edison bulbs, table lamps, LED lighting, fairy lights, throw pillows, blankets, candles, vases, wall art, woven baskets, decorative accents. All products must fit this theme.
Every product must fit the brand. Off-theme products are rejected by AI scoring.
Runs via GitHub Actions 2x/week.
"""

import os, json, time, sqlite3, logging, requests, random
from datetime import datetime
from pathlib import Path
import anthropic

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

# ── Warm Ambient Home Decor Theme (locked by phase3_pipeline_lockdown) ──
# Theme: Warm Ambient Home Decor
# Cozy, aesthetic, mood-driven home products. Edison bulbs, table lamps, LED lighting, fairy lights, throw pillows, blankets, candles, vases, wall art, woven baskets, decorative accents.
# Generated: 2026-03-13T17:42:56.533845

# -- Warm Ambient Home Lighting & Decor Theme (locked by phase3_pipeline_lockdown) --
# Theme: Warm Ambient Home Lighting & Decor
# Warm, aesthetic, mood-driven home products anchored by lighting. Table lamps, pendant lights, LED strips, fairy lights, string lights, sunset lamps, galaxy projectors. Supporting products: wall art/prints, throw pillows, woven storage, candle holders, vases.
# Generated: 2026-03-13T18:10:10.730068

NICHES = [
    {
        "name": "table & desk lamps",
        "search_terms": ["table lamp", "desk lamp", "bedside lamp", "night light"],
        "collection_id": 305043832906,
        "collection_handle": "Table & Desk Lamps",
        "reject_keywords": ["car", "bicycle", "motorcycle", "earring", "jewelry", "pet", "baby", "gym", "sport", "industrial", "commercial"],
    },
    {
        "name": "pendant & ceiling lights",
        "search_terms": ["pendant light", "chandelier", "ceiling light", "hanging lamp"],
        "collection_id": 305043865674,
        "collection_handle": "Pendant & Ceiling Lights",
        "reject_keywords": ["car", "motorcycle", "earring", "jewelry", "pet", "gym", "sport", "baby", "industrial"],
    },
    {
        "name": "LED & ambient lighting",
        "search_terms": ["LED strip light", "fairy lights", "string lights", "sunset lamp", "galaxy projector", "solar string lights"],
        "collection_id": 305043898442,
        "collection_handle": "LED & Ambient Lighting",
        "reject_keywords": ["car", "motorcycle", "earring", "jewelry", "pet", "gym", "sport", "baby", "industrial"],
    },
    {
        "name": "wall decor",
        "search_terms": ["wall art canvas", "decorative painting", "wall hanging", "tapestry", "framed print"],
        "collection_id": 305043931210,
        "collection_handle": "Wall Decor",
        "reject_keywords": ["car", "motorcycle", "earring", "jewelry", "pet", "gym", "sport", "baby", "industrial"],
    },
    {
        "name": "cozy textiles",
        "search_terms": ["throw pillow cover", "cushion cover", "decorative pillow"],
        "collection_id": 305043963978,
        "collection_handle": "Cozy Textiles",
        "reject_keywords": ["car", "motorcycle", "earring", "jewelry", "pet", "gym", "sport", "baby", "industrial"],
    },
    {
        "name": "storage & accents",
        "search_terms": ["woven basket", "candle holder", "decorative vase", "storage basket"],
        "collection_id": 305043996746,
        "collection_handle": "Storage & Accents",
        "reject_keywords": ["car", "motorcycle", "earring", "jewelry", "pet", "gym", "sport", "baby", "industrial"],
    },
]

FEATURED_COLLECTION_ID = 305043832906  # Featured collection

# ── CJDropshipping Auth ────────────────────────────────────────────────────────
def cj_get_token() -> str | None:
    if not CJ_API_KEY:
        log.warning("CJ_API_KEY not set — skipping live search")
        return None
    try:
        resp = requests.post(
            f"{CJ_BASE}/authentication/getAccessToken",
            json={"apiKey": CJ_API_KEY},
            timeout=15
        )
        data = resp.json()
        if data.get("result") is True:
            token = data["data"]["accessToken"]
            log.info("CJ auth: token obtained")
            return token
        log.error(f"CJ auth failed: {data.get('message')}")
        return None
    except Exception as e:
        log.error(f"CJ auth error: {e}")
        return None

# ── CJDropshipping Product Search ─────────────────────────────────────────────
def cj_search_products(token: str, keyword: str) -> list:
    if not token:
        return []
    try:
        resp = requests.get(
            f"{CJ_BASE}/product/list",
            headers={"CJ-Access-Token": token},
            params={"productName": keyword, "pageNum": 1, "pageSize": 30},
            timeout=15
        )
        data = resp.json()
        if data.get("result") is True:
            products = data.get("data", {}).get("list", [])
            log.info(f"  CJ returned {len(products)} products for '{keyword}'")
            return products
        log.warning(f"  CJ search failed for '{keyword}': {data.get('message')}")
        return []
    except Exception as e:
        log.error(f"CJ search error: {e}")
        return []

# ── Database ───────────────────────────────────────────────────────────────────
def init_db() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS products (
            id             INTEGER PRIMARY KEY AUTOINCREMENT,
            cj_id          TEXT UNIQUE,
            title          TEXT,
            niche          TEXT,
            collection_id  INTEGER,
            cost_usd       REAL,
            sell_price     REAL,
            profit_margin  REAL,
            image_url      TEXT,
            product_url    TEXT,
            ai_description TEXT,
            ai_tags        TEXT,
            ai_score       INTEGER DEFAULT 0,
            shopify_id     TEXT,
            status         TEXT DEFAULT 'pending',
            created_at     TEXT DEFAULT (datetime('now'))
        )
    """)
    conn.commit()
    return conn

# ── Claude AI scoring ──────────────────────────────────────────────────────────
BRAND_BRIEF = """
EdisonHaus is a Shopify store selling warm, aesthetic home lighting and decor.
The brand vibe: Edison bulb warmth, pendant light elegance, cozy ambient glow.
The customer: 25-40, rents or owns their first home, cares about how their space looks and feels.
Products must feel at home in a lifestyle Instagram post. Think warm lighting, soft textures, clean organisation.
"""

def ai_score_and_describe(client, product: dict, niche: dict) -> dict:
    title    = product.get("productNameEn") or product.get("productName") or "Unknown"
    raw      = product.get("sellPrice") or (product.get("variants") or [{}])[0].get("variantSellPrice", 10)
    cost     = float(str(raw).split("--")[0].strip() if "--" in str(raw) else raw)
    category = product.get("categoryName", niche["name"])
    rejects  = ", ".join(niche["reject_keywords"])

    prompt = f"""You are a product buyer for EdisonHaus, a home lifestyle store.

{BRAND_BRIEF}

Evaluate this product:
Title: {title}
Niche: {niche["name"]}
Category: {category}
Cost price: ${cost:.2f}

Automatic REJECT if the title or category suggests: {rejects}

Return ONLY valid JSON — no markdown, no explanation:
{{
  "score": <1-10, where 10 = perfect fit for EdisonHaus>,
  "sell_price": <recommended USD retail price, 2.5x cost minimum $14.99>,
  "description": <90-word Shopify product description, warm and benefit-focused, written for EdisonHaus>,
  "tags": <6 comma-separated SEO tags relevant to home/lifestyle>,
  "skip": <true if this product does NOT fit EdisonHaus brand, false if it fits>,
  "skip_reason": <one sentence reason if skip is true, else empty string>
}}"""

    for attempt in range(3):
        try:
            msg = client.messages.create(
                model="claude-sonnet-4-20250514",
                max_tokens=600,
                messages=[{"role": "user", "content": prompt}]
            )
            raw_text = msg.content[0].text.strip().replace("```json", "").replace("```", "").strip()
            return json.loads(raw_text)
        except Exception as e:
            log.warning(f"Claude attempt {attempt+1} failed: {e}")
            time.sleep(2 ** attempt)
    return {"score": 0, "sell_price": cost * 2.5, "description": "", "tags": "", "skip": True, "skip_reason": "AI error"}

# ── Save to DB ─────────────────────────────────────────────────────────────────
def save_product(conn, product: dict, ai: dict, niche: dict) -> bool:
    raw   = product.get("sellPrice") or (product.get("variants") or [{}])[0].get("variantSellPrice", 10)
    cost  = float(str(raw).split("--")[0].strip() if "--" in str(raw) else raw)
    sell  = float(ai.get("sell_price", cost * 2.5))
    margin = round((sell - cost) / max(sell, 0.01) * 100, 1)
    image  = product.get("productImage") or product.get("imageUrl") or ""
    cj_id  = str(product.get("pid") or product.get("productId") or "")

    try:
        conn.execute("""
            INSERT OR IGNORE INTO products
            (cj_id, title, niche, collection_id, cost_usd, sell_price, profit_margin,
             image_url, product_url, ai_description, ai_tags, ai_score)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
        """, (
            cj_id,
            (product.get("productNameEn") or "")[:200],
            niche["name"],
            niche["collection_id"],
            round(cost, 2),
            round(sell, 2),
            margin,
            image,
            f"https://app.cjdropshipping.com/product-detail.html?id={cj_id}",
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
    log.info("B3 Product Finder — EdisonHaus Theme Engine")
    log.info("=" * 60)

    try:
        client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)
        conn   = init_db()
        token  = cj_get_token()

        total_saved = 0
        skipped     = 0

        # Shuffle niches so each run hits different ones first
        niches_this_run = NICHES[:]
        random.shuffle(niches_this_run)

        for niche in niches_this_run:
            if total_saved >= PRODUCTS_PER_RUN:
                break

            log.info(f"\n── Niche: {niche['name']} → {niche['collection_handle']} ──")

            # Rotate through search terms for this niche
            search_terms = niche["search_terms"][:]
            random.shuffle(search_terms)

            for term in search_terms:
                if total_saved >= PRODUCTS_PER_RUN:
                    break

                products = cj_search_products(token, term)

                for product in products:
                    if total_saved >= PRODUCTS_PER_RUN:
                        break

                    # Skip out-of-stock
                    if product.get("isStock") == "NO":
                        continue

                    # Skip placeholder images
                    img = product.get("productImage") or ""
                    if "4a4a4a" in img or "placehold" in img or not img:
                        continue

                    # Quick title filter — reject obviously off-theme before spending AI tokens
                    title_lower = (product.get("productNameEn") or "").lower()
                    if any(kw in title_lower for kw in niche["reject_keywords"]):
                        log.info(f"  ✗ Pre-filter reject: {title_lower[:60]}")
                        skipped += 1
                        continue

                    # AI scoring
                    ai = ai_score_and_describe(client, product, niche)

                    if ai.get("skip"):
                        log.info(f"  ✗ AI reject: {title_lower[:55]} — {ai.get('skip_reason','')}")
                        skipped += 1
                        continue

                    score = int(ai.get("score", 0))
                    if score < 7:
                        log.info(f"  ✗ Low score ({score}/10): {title_lower[:55]}")
                        skipped += 1
                        continue

                    if save_product(conn, product, ai, niche):
                        total_saved += 1
                        log.info(f"  ✓ [{score}/10] {(product.get('productNameEn') or '')[:55]} | ${ai.get('sell_price', 0):.2f} → {niche['collection_handle']}")

                    time.sleep(0.5)

                time.sleep(1.5)

        pending = conn.execute("SELECT COUNT(*) FROM products WHERE status='pending'").fetchone()[0]
        log.info(f"\nDone. Saved {total_saved} new | Rejected {skipped} off-theme | Total pending: {pending}")
        write_heartbeat(total_saved)
        conn.close()

    except Exception as e:
        log.error(f"Product finder failed: {e}")
        write_heartbeat(0, status=f"error: {e}")
        raise

if __name__ == "__main__":
    main()
