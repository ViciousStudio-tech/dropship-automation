"""
Business 3 — Dropship Product Finder (CJDropshipping)
Searches CJ for on-theme products, scores them with Claude AI,
and saves REAL CJ product + variant IDs to the DB.
Runs 2x/week via GitHub Actions.
"""

import os, json, time, sqlite3, logging, requests, builtins
from datetime import datetime
from pathlib import Path

# Block any interactive prompts
def _no_input(*a, **k): raise RuntimeError("BLOCKED: interactive prompt")
builtins.input = _no_input

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

# ── Config ─────────────────────────────────────────────────────────────────────
CJ_EMAIL    = os.environ.get("CJ_EMAIL", "nicholas.jacksondesign@gmail.com")
CJ_API_KEY  = os.environ.get("CJ_API_KEY", "")
ANTHROPIC_KEY = os.environ.get("ANTHROPIC_API_KEY", "")
DB_PATH     = os.environ.get("DB_PATH", "data/dropship.db")
REPORT_DIR  = Path(os.environ.get("REPORT_DIR", "reports"))
HEARTBEAT   = Path("b3_product_heartbeat.json")
CJ_BASE     = "https://developers.cjdropshipping.com/api2.0/v1"

REPORT_DIR.mkdir(exist_ok=True)
Path("data").mkdir(exist_ok=True)

# Min score to list a product, min margin
MIN_SCORE  = 7
MIN_MARGIN = 0.40   # 40% gross margin minimum

# ── Collections ────────────────────────────────────────────────────────────────
NICHES = [
    {"name": "LED & Ambient Lighting",   "collection_id": 305043898442, "collection_handle": "led-ambient-lighting",
     "search_terms": ["LED strip light", "fairy lights", "string lights", "sunset lamp", "galaxy projector", "neon light", "ambient light"],
     "reject_keywords": ["clothing", "shoes", "food", "medicine", "weapon", "car part"]},
    {"name": "Table & Desk Lamps",       "collection_id": 305043832906, "collection_handle": "table-desk-lamps",
     "search_terms": ["table lamp", "desk lamp", "bedside lamp", "reading lamp"],
     "reject_keywords": ["clothing", "shoes", "food", "outdoor flood", "industrial"]},
    {"name": "Pendant & Ceiling Lights", "collection_id": 305043865674, "collection_handle": "pendant-ceiling-lights",
     "search_terms": ["pendant light", "chandelier", "ceiling light", "hanging lamp"],
     "reject_keywords": ["clothing", "shoes", "food", "outdoor flood"]},
    {"name": "Wall Décor",               "collection_id": 305043931210, "collection_handle": "wall-decor",
     "search_terms": ["wall art canvas", "decorative painting", "wall hanging", "tapestry", "poster print"],
     "reject_keywords": ["clothing", "shoes", "food", "medicine", "sports team", "explicit"]},
    {"name": "Cozy Textiles",            "collection_id": 305043963978, "collection_handle": "cozy-textiles",
     "search_terms": ["throw pillow cover", "cushion cover", "decorative pillow"],
     "reject_keywords": ["clothing", "shoes", "food", "medicine"]},
    {"name": "Storage & Accents",        "collection_id": 305043996746, "collection_handle": "storage-accents",
     "search_terms": ["woven basket", "candle holder", "decorative vase", "rattan basket", "storage basket"],
     "reject_keywords": ["clothing", "shoes", "food", "medicine", "industrial"]},
]

# ── CJ Auth ────────────────────────────────────────────────────────────────────
def cj_auth() -> str | None:
    try:
        resp = requests.post(f"{CJ_BASE}/authentication/getAccessToken",
            json={"email": CJ_EMAIL, "password": CJ_API_KEY}, timeout=15)
        data = resp.json()
        if data.get("result") is True:
            log.info("CJ auth: OK")
            return data["data"]["accessToken"]
        log.error(f"CJ auth failed: code={data.get('code')} msg={data.get('message')} email={CJ_EMAIL} key_len={len(CJ_API_KEY)}")
    except Exception as e:
        log.error(f"CJ auth error: {e}")
    return None

# ── CJ Product Search ──────────────────────────────────────────────────────────
def cj_search(token: str, keyword: str) -> list:
    try:
        resp = requests.get(f"{CJ_BASE}/product/list",
            headers={"CJ-Access-Token": token},
            params={"productNameEn": keyword, "pageNum": 1, "pageSize": 30},
            timeout=15)
        data = resp.json()
        if data.get("result") is True:
            products = data.get("data", {}).get("list", [])
            log.info(f"  CJ: {len(products)} results for '{keyword}'")
            return products
        log.warning(f"  CJ search failed for '{keyword}': {data.get('message')}")
    except Exception as e:
        log.error(f"CJ search error: {e}")
    return []

# ── Get real variant ID from CJ product detail ─────────────────────────────────
def cj_get_variant_id(token: str, pid: str) -> tuple[str | None, float]:
    """Returns (variant_id, cost_usd) for the cheapest variant of a CJ product."""
    try:
        resp = requests.get(f"{CJ_BASE}/product/query",
            headers={"CJ-Access-Token": token},
            params={"pid": pid}, timeout=15)
        data = resp.json()
        if data.get("result") is True:
            product = data.get("data", {})
            variants = product.get("variants", [])
            if variants:
                # Pick cheapest variant
                variants_sorted = sorted(variants, key=lambda v: float(v.get("variantSellPrice", 999)))
                v = variants_sorted[0]
                vid = v.get("vid") or v.get("variantId") or ""
                cost = float(v.get("variantSellPrice", 0) or 0)
                return str(vid), cost
            # No variants — use product-level price
            cost_raw = product.get("sellPrice", 0) or 0
            cost = float(str(cost_raw).split("--")[0].strip() if "--" in str(cost_raw) else cost_raw)
            return str(pid), cost  # fall back to pid as vid
    except Exception as e:
        log.error(f"CJ product detail error for {pid}: {e}")
    return None, 0.0

# ── DB ─────────────────────────────────────────────────────────────────────────
def init_db() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS products (
            id             INTEGER PRIMARY KEY AUTOINCREMENT,
            cj_id          TEXT UNIQUE,
            cj_vid         TEXT,
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
    # Add cj_vid column if upgrading from old schema
    try:
        conn.execute("ALTER TABLE products ADD COLUMN cj_vid TEXT")
    except Exception:
        pass
    conn.commit()
    return conn

# ── Claude AI scoring ──────────────────────────────────────────────────────────
BRAND_BRIEF = """
EdisonHaus sells warm, aesthetic home lighting and decor.
Brand vibe: Edison bulb warmth, pendant light elegance, cozy ambient glow.
Customer: 25-40, cares how their space looks and feels.
Products must fit a lifestyle Instagram post. Think warm lighting, soft textures, clean organisation.
"""

def ai_score(product: dict, niche: dict, cost: float) -> dict:
    import anthropic
    client = anthropic.Anthropic(api_key=ANTHROPIC_KEY)
    title    = product.get("productNameEn") or product.get("productName") or "Unknown"
    category = product.get("categoryName", niche["name"])
    rejects  = ", ".join(niche["reject_keywords"])

    prompt = f"""You are a product buyer for EdisonHaus, a home lifestyle store.

{BRAND_BRIEF}

Evaluate this product:
Title: {title}
Niche: {niche["name"]}
Category: {category}
CJ cost price: ${cost:.2f}

Automatic REJECT if title or category suggests: {rejects}

Return ONLY valid JSON (no markdown):
{{
  "score": <1-10, 10=perfect EdisonHaus fit>,
  "sell_price": <USD retail price, 2.5x cost minimum, $14.99 floor>,
  "description": <90-word Shopify product description, warm and benefit-focused>,
  "tags": <6 comma-separated SEO tags>,
  "skip": <true if does NOT fit EdisonHaus, false if fits>,
  "skip_reason": <one sentence if skip=true, else empty string>
}}"""

    for attempt in range(3):
        try:
            msg = client.messages.create(
                model="claude-sonnet-4-20250514", max_tokens=600,
                messages=[{"role": "user", "content": prompt}])
            raw = msg.content[0].text.strip().replace("```json","").replace("```","").strip()
            return json.loads(raw)
        except Exception as e:
            log.warning(f"AI attempt {attempt+1} failed: {e}")
            time.sleep(2 ** attempt)
    return {"score": 0, "sell_price": max(cost * 2.5, 14.99), "description": "", "tags": "", "skip": True, "skip_reason": "AI error"}

# ── Save to DB ─────────────────────────────────────────────────────────────────
def save_product(conn, product: dict, ai: dict, niche: dict, cost: float, vid: str) -> bool:
    raw_sell = ai.get("sell_price", max(cost * 2.5, 14.99)) or max(cost * 2.5, 14.99)
    sell   = float(str(raw_sell).replace("$","").replace(",","").strip())
    margin = round((sell - cost) / max(sell, 0.01) * 100, 1)
    image  = product.get("productImage") or product.get("imageUrl") or ""
    cj_id  = str(product.get("pid") or product.get("productId") or "")

    if margin < MIN_MARGIN * 100:
        log.info(f"  Skipping (margin {margin:.1f}% < {MIN_MARGIN*100:.0f}%): {product.get('productNameEn','')[:50]}")
        return False

    try:
        conn.execute("""
            INSERT OR IGNORE INTO products
            (cj_id, cj_vid, title, niche, collection_id, cost_usd, sell_price, profit_margin,
             image_url, product_url, ai_description, ai_tags, ai_score)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)
        """, (
            cj_id, vid,
            (product.get("productNameEn") or "")[:200],
            niche["name"], niche["collection_id"],
            round(cost, 2), round(sell, 2), margin,
            image,
            f"https://app.cjdropshipping.com/product-detail.html?id={cj_id}",
            ai.get("description", ""), ai.get("tags", ""),
            int(ai.get("score", 0))
        ))
        conn.commit()
        return conn.execute("SELECT changes()").fetchone()[0] > 0
    except Exception as e:
        log.error(f"DB save error: {e}")
        return False

# ── Heartbeat ──────────────────────────────────────────────────────────────────
def write_heartbeat(found: int, status: str = "success"):
    HEARTBEAT.write_text(json.dumps({
        "module": "b3_product_finder",
        "last_run": datetime.now().isoformat(),
        "products_found": found,
        "status": status
    }, indent=2))

# ── Main ───────────────────────────────────────────────────────────────────────
def main():
    log.info("=" * 60)
    log.info("B3 Product Finder — CJDropshipping")
    log.info("=" * 60)

    conn  = init_db()
    token = cj_auth()
    if not token:
        write_heartbeat(0, "error: CJ auth failed")
        return

    total_saved = 0

    for niche in NICHES:
        log.info(f"\nNiche: {niche['name']}")
        seen_pids = set()

        for keyword in niche["search_terms"]:
            products = cj_search(token, keyword)
            time.sleep(1)

            for product in products:
                pid = str(product.get("pid") or product.get("productId") or "")
                if not pid or pid in seen_pids:
                    continue
                seen_pids.add(pid)

                # Skip if already in DB
                if conn.execute("SELECT id FROM products WHERE cj_id=?", (pid,)).fetchone():
                    continue

                # Get real variant ID and cost from product detail
                vid, cost = cj_get_variant_id(token, pid)
                cost = float(str(cost).replace('$','').replace(',','').strip()) if cost else 0.0
                time.sleep(0.5)

                if not vid or cost <= 0:
                    log.info(f"  Skipping {pid} — no variant/cost data")
                    continue

                # AI score
                ai = ai_score(product, niche, cost)
                score = int(ai.get("score", 0))
                skip  = ai.get("skip", True)

                title = (product.get("productNameEn") or "")[:60]
                raw_sp = ai.get('sell_price', 0) or 0
                sell_display = float(str(raw_sp).replace('$','').replace(',','').strip() or 0)
                log.info(f"  [{score}/10] {title} | cost=${cost:.2f} sell=${sell_display:.2f} skip={skip}")

                if skip or score < MIN_SCORE:
                    continue

                saved = save_product(conn, product, ai, niche, cost, vid)
                if saved:
                    total_saved += 1
                    log.info(f"  ✓ Saved: {title} (cj_id={pid} vid={vid})")

                time.sleep(0.3)

    log.info(f"\nDone. Total new products saved: {total_saved}")
    write_heartbeat(total_saved)
    conn.close()

if __name__ == "__main__":
    main()
