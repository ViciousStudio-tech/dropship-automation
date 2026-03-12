"""
Business 3 — Shopify Store Manager
Takes pending products from DB, lists them on Shopify with AI descriptions.
Assigns products to correct collections. Runs via GitHub Actions 2x/week.
"""

import os, json, time, sqlite3, logging, requests
from datetime import datetime
from pathlib import Path
import anthropic

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

# ── Env ────────────────────────────────────────────────────────────────────────
ANTHROPIC_API_KEY    = os.environ["ANTHROPIC_API_KEY"]
SHOPIFY_STORE        = os.environ.get("SHOPIFY_STORE", "fgtyz6-bj.myshopify.com")
SHOPIFY_ACCESS_TOKEN = os.environ["SHOPIFY_ACCESS_TOKEN"]
LISTINGS_PER_RUN     = int(os.environ.get("LISTINGS_PER_RUN", "50"))
DB_PATH              = os.environ.get("DB_PATH", "data/dropship.db")

Path("data").mkdir(exist_ok=True)
HEARTBEAT = Path("b3_store_heartbeat.json")

SHOPIFY_HEADERS = {
    "X-Shopify-Access-Token": SHOPIFY_ACCESS_TOKEN,
    "Content-Type": "application/json"
}
SHOPIFY_BASE = f"https://{SHOPIFY_STORE}/admin/api/2024-10"

# ── Collection mapping: niche → Shopify collection ID ─────────────────────────
# Collections already created in VibeFinds store
# Home & Kitchen:   304927309898
# Trending Now:     304927375434
# Outdoor & Travel: 304927440970
COLLECTION_MAP = {
    "home decor":           304927309898,  # Home & Kitchen
    "kitchen organizer":    304927309898,  # Home & Kitchen
    "storage solutions":    304927309898,  # Home & Kitchen
    "wall art":             304927309898,  # Home & Kitchen
    "LED lighting":         304927375434,  # Trending Now
    "smart home gadgets":   304927375434,  # Trending Now
}

def niche_to_collection_id(niche: str) -> int | None:
    """Return Shopify collection ID for a niche string."""
    niche_lower = niche.lower()
    for keyword, collection_id in COLLECTION_MAP.items():
        if keyword in niche_lower:
            return collection_id
    return 304927309898  # Default to Home & Kitchen

# ── DB ─────────────────────────────────────────────────────────────────────────
def init_db() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    # Ensure shopify_id column exists (migration)
    try:
        conn.execute("ALTER TABLE products ADD COLUMN shopify_id TEXT")
        conn.commit()
    except Exception:
        pass
    return conn

def get_pending_products(conn, limit: int) -> list:
    rows = conn.execute("""
        SELECT id, cj_id, title, niche, cost_usd, sell_price,
               image_url, product_url, ai_description, ai_tags
        FROM products
        WHERE status = 'pending'
        ORDER BY ai_score DESC, created_at ASC
        LIMIT ?
    """, (limit,)).fetchall()
    return [dict(zip(
        ["id","cj_id","title","niche","cost_usd","sell_price",
         "image_url","product_url","ai_description","ai_tags"], row
    )) for row in rows]

def mark_listed(conn, product_id: int, shopify_id: str):
    conn.execute(
        "UPDATE products SET status='listed', shopify_id=? WHERE id=?",
        (shopify_id, product_id)
    )
    conn.commit()

# ── Shopify: Create product ────────────────────────────────────────────────────
def create_shopify_product(product: dict) -> str | None:
    """Create product on Shopify. Returns Shopify product ID or None."""
    collection_id = niche_to_collection_id(product.get("niche", ""))
    tags = product.get("ai_tags", product.get("niche", "home, lifestyle"))

    # Clean up title — remove CJ's messy formatting
    title = product["title"]
    # Remove things like "1PC" or trailing specs if too long
    if len(title) > 80:
        title = title[:77] + "..."

    body_html = f"<p>{product['ai_description']}</p>" if product.get("ai_description") else "<p>Premium quality home and lifestyle product.</p>"

    # Add supplier note as metafield (internal only)
    cj_url = product.get("product_url", "")

    payload = {
        "product": {
            "title": title,
            "body_html": body_html,
            "vendor": "EdisonHaus",
            "product_type": product["niche"].title(),
            "tags": tags,
            "status": "active",
            "variants": [{
                "price": str(round(product["sell_price"], 2)),
                "compare_at_price": str(round(product["sell_price"] * 1.25, 2)),
                "inventory_management": None,
                "fulfillment_service": "manual",
                "requires_shipping": True,
                "taxable": True
            }],
            "images": [{"src": product["image_url"]}] if product.get("image_url") and "4a4a4a" not in product.get("image_url", "") and "placehold" not in product.get("image_url", "") else []
        }
    }

    for attempt in range(3):
        try:
            resp = requests.post(
                f"{SHOPIFY_BASE}/products.json",
                headers=SHOPIFY_HEADERS,
                json=payload,
                timeout=20
            )
            if resp.status_code == 201:
                shopify_id = str(resp.json()["product"]["id"])
                log.info(f"  Created: {title[:50]} → #{shopify_id}")

                # Add to collection
                if collection_id:
                    add_to_collection(shopify_id, collection_id)

                # Store CJ URL as metafield for order fulfiller
                if cj_url:
                    add_metafield(shopify_id, cj_url, product.get("cj_id", ""))

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

def add_to_collection(shopify_product_id: str, collection_id: int):
    """Add product to a Shopify custom collection."""
    try:
        resp = requests.post(
            f"{SHOPIFY_BASE}/collects.json",
            headers=SHOPIFY_HEADERS,
            json={"collect": {"product_id": int(shopify_product_id), "collection_id": collection_id}},
            timeout=10
        )
        if resp.status_code == 201:
            log.info(f"  Added to collection {collection_id}")
        else:
            log.warning(f"  Collection add failed: {resp.status_code}")
    except Exception as e:
        log.warning(f"  Collection add error: {e}")

def add_metafield(shopify_product_id: str, cj_url: str, cj_id: str):
    """Store CJ source URL in product metafields for order routing."""
    try:
        requests.post(
            f"{SHOPIFY_BASE}/products/{shopify_product_id}/metafields.json",
            headers=SHOPIFY_HEADERS,
            json={"metafield": {
                "namespace": "dropship",
                "key": "cj_product_url",
                "value": cj_url,
                "type": "url"
            }},
            timeout=10
        )
        requests.post(
            f"{SHOPIFY_BASE}/products/{shopify_product_id}/metafields.json",
            headers=SHOPIFY_HEADERS,
            json={"metafield": {
                "namespace": "dropship",
                "key": "cj_product_id",
                "value": cj_id,
                "type": "single_line_text_field"
            }},
            timeout=10
        )
    except Exception as e:
        log.warning(f"  Metafield error: {e}")

# ── Heartbeat ──────────────────────────────────────────────────────────────────
def write_heartbeat(listed: int, status: str = "success"):
    HEARTBEAT.write_text(json.dumps({
        "module": "b3_store_manager",
        "last_run": datetime.now().isoformat(),
        "listed": listed,
        "status": status
    }, indent=2))

# ── Main ───────────────────────────────────────────────────────────────────────
def main():
    log.info("=" * 60)
    log.info("B3 Store Manager — Listing products to VibeFinds")
    log.info("=" * 60)

    try:
        conn = init_db()
        products = get_pending_products(conn, LISTINGS_PER_RUN)
        log.info(f"Listing {len(products)} products to {SHOPIFY_STORE}")

        if not products:
            log.info("No pending products. Run b3_product_finder.py first.")
            write_heartbeat(0, "no_products")
            return

        listed = 0
        for product in products:
            shopify_id = create_shopify_product(product)
            if shopify_id:
                mark_listed(conn, product["id"], shopify_id)
                listed += 1
            time.sleep(0.6)  # Shopify rate limit: 2 req/s

        log.info(f"Done. Listed {listed}/{len(products)} products.")
        write_heartbeat(listed)
        conn.close()

    except Exception as e:
        log.error(f"Store manager failed: {e}")
        write_heartbeat(0, status=f"error: {e}")
        raise

if __name__ == "__main__":
    main()
