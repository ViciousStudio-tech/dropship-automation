"""
Business 3 — Store Manager (CJDropshipping → Shopify)
Lists approved products from DB onto Shopify with:
  - Real CJ product/variant IDs stored as metafields for auto-fulfillment
  - Prices from CJ cost × AI markup (no fake sale badges)
  - Correct collection assignment
Runs daily via GitHub Actions.
"""

import os, json, time, sqlite3, logging, requests, builtins
from datetime import datetime
from pathlib import Path

def _no_input(*a, **k): raise RuntimeError("BLOCKED: interactive prompt")
builtins.input = _no_input

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

# ── Config ─────────────────────────────────────────────────────────────────────
SHOPIFY_STORE  = os.environ.get("SHOPIFY_STORE", "fgtyz6-bj.myshopify.com")
SHOPIFY_TOKEN  = os.environ["SHOPIFY_ACCESS_TOKEN"]
DB_PATH        = os.environ.get("DB_PATH", "data/dropship.db")
REPORT_DIR     = Path(os.environ.get("REPORT_DIR", "reports"))
HEARTBEAT      = Path("b3_store_heartbeat.json")
MAX_PER_RUN    = 10   # list up to 10 new products per run

REPORT_DIR.mkdir(exist_ok=True)
SHOPIFY_BASE   = f"https://{SHOPIFY_STORE}/admin/api/2024-01"
SHOPIFY_HEADERS = {"X-Shopify-Access-Token": SHOPIFY_TOKEN, "Content-Type": "application/json"}

# ── DB ─────────────────────────────────────────────────────────────────────────
def get_pending(conn) -> list:
    rows = conn.execute("""
        SELECT id, cj_id, cj_vid, title, niche, collection_id,
               cost_usd, sell_price, profit_margin,
               image_url, ai_description, ai_tags, ai_score
        FROM products
        WHERE status = 'pending' AND cj_vid IS NOT NULL AND cj_vid != ''
        ORDER BY ai_score DESC
        LIMIT ?
    """, (MAX_PER_RUN,)).fetchall()
    cols = ["id","cj_id","cj_vid","title","niche","collection_id",
            "cost_usd","sell_price","profit_margin",
            "image_url","ai_description","ai_tags","ai_score"]
    return [dict(zip(cols, r)) for r in rows]

# ── Shopify: create product ─────────────────────────────────────────────────────
def create_shopify_product(product: dict) -> str | None:
    """Creates product on Shopify. Returns Shopify product ID or None."""
    tags = product.get("ai_tags") or product["niche"]
    if "EdisonHaus" not in tags:
        tags = "EdisonHaus, " + tags

    payload = {
        "product": {
            "title": product["title"],
            "body_html": product.get("ai_description") or f"<p>{product['title']}.</p>",
            "vendor": "EdisonHaus",
            "product_type": product["niche"],
            "tags": tags,
            "status": "active",
            "images": [{"src": product["image_url"]}] if product.get("image_url") else [],
            "variants": [{
                "price": str(round(product["sell_price"], 2)),
                "inventory_management": None,
                "fulfillment_service": "manual",
                "requires_shipping": True,
                "taxable": True,
                "sku": f"CJ-{product['cj_id']}"
            }]
        }
    }

    try:
        r = requests.post(f"{SHOPIFY_BASE}/products.json",
            headers=SHOPIFY_HEADERS, json=payload, timeout=20)
        if r.ok:
            shopify_id = str(r.json()["product"]["id"])
            log.info(f"  ✓ Created Shopify product {shopify_id}: {product['title'][:50]}")
            return shopify_id
        log.error(f"  Shopify create failed: {r.status_code} {r.text[:200]}")
    except Exception as e:
        log.error(f"  Shopify create error: {e}")
    return None

# ── Shopify: assign collection ──────────────────────────────────────────────────
def assign_collection(shopify_product_id: str, collection_id: int):
    try:
        r = requests.post(f"{SHOPIFY_BASE}/collects.json",
            headers=SHOPIFY_HEADERS,
            json={"collect": {"product_id": shopify_product_id, "collection_id": collection_id}},
            timeout=10)
        if r.ok:
            log.info(f"  ✓ Assigned to collection {collection_id}")
        else:
            log.warning(f"  Collection assign failed: {r.status_code}")
    except Exception as e:
        log.error(f"  Collection assign error: {e}")

# ── Shopify: write CJ metafields for fulfillment ───────────────────────────────
def write_cj_metafields(shopify_product_id: str, cj_id: str, cj_vid: str):
    """
    Writes cj_product_id and cj_variant_id as metafields so the order
    fulfiller can route orders to CJ automatically.
    """
    metafields = [
        {"namespace": "dropship", "key": "cj_product_id", "value": cj_id,  "type": "single_line_text_field"},
        {"namespace": "dropship", "key": "cj_variant_id", "value": cj_vid, "type": "single_line_text_field"},
        {"namespace": "dropship", "key": "supplier",      "value": "CJDropshipping", "type": "single_line_text_field"},
    ]
    ok = 0
    for mf in metafields:
        try:
            r = requests.post(f"{SHOPIFY_BASE}/products/{shopify_product_id}/metafields.json",
                headers=SHOPIFY_HEADERS, json={"metafield": mf}, timeout=10)
            if r.ok:
                ok += 1
            else:
                log.warning(f"  Metafield {mf['key']} failed: {r.status_code}")
        except Exception as e:
            log.error(f"  Metafield error: {e}")
    log.info(f"  ✓ Wrote {ok}/3 metafields (cj_id={cj_id} vid={cj_vid})")

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
    log.info("B3 Store Manager — CJDropshipping → Shopify")
    log.info("=" * 60)

    conn = sqlite3.connect(DB_PATH)
    pending = get_pending(conn)
    log.info(f"Pending products to list: {len(pending)}")

    listed = 0
    for product in pending:
        log.info(f"\nListing: {product['title'][:60]}")
        log.info(f"  cj_id={product['cj_id']} cj_vid={product['cj_vid']} cost=${product['cost_usd']:.2f} sell=${product['sell_price']:.2f}")

        # 1. Create on Shopify
        shopify_id = create_shopify_product(product)
        if not shopify_id:
            continue

        # 2. Assign to collection
        assign_collection(shopify_id, product["collection_id"])
        time.sleep(0.3)

        # 3. Write CJ metafields — this is what enables auto-fulfillment
        write_cj_metafields(shopify_id, product["cj_id"], product["cj_vid"])
        time.sleep(0.3)

        # 4. Mark as listed in DB
        conn.execute(
            "UPDATE products SET status='listed', shopify_id=? WHERE id=?",
            (shopify_id, product["id"])
        )
        conn.commit()
        listed += 1
        time.sleep(0.5)

    log.info(f"\nDone. Listed {listed} products.")
    write_heartbeat(listed)
    conn.close()

if __name__ == "__main__":
    main()
