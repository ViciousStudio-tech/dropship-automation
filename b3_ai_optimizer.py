"""
Business 3 — AI Optimizer
Rewrites weak product listings, generates ad copy, monitors store performance.
Runs weekly via GitHub Actions.
"""

import os
import json
import time
import sqlite3
import logging
import requests
from datetime import datetime, timedelta
from pathlib import Path
import anthropic

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

ANTHROPIC_API_KEY    = os.environ["ANTHROPIC_API_KEY"]
SHOPIFY_STORE        = os.environ.get("SHOPIFY_STORE", "fgtyz6-bj.myshopify.com")
SHOPIFY_ACCESS_TOKEN = os.environ["SHOPIFY_ACCESS_TOKEN"]
GMAIL_SENDER         = os.environ.get("GMAIL_SENDER", "")
GMAIL_TO             = os.environ.get("GMAIL_TO", GMAIL_SENDER)
DB_PATH              = os.environ.get("DB_PATH", "data/dropship.db")

SHOPIFY_HEADERS = {
    "X-Shopify-Access-Token": SHOPIFY_ACCESS_TOKEN,
    "Content-Type": "application/json"
}
SHOPIFY_BASE = f"https://{SHOPIFY_STORE}/admin/api/2024-10"

HEARTBEAT = Path("b3_optimizer_heartbeat.json")

def get_store_stats() -> dict:
    """Fetch this week's orders and revenue from Shopify."""
    try:
        since = (datetime.now() - timedelta(days=7)).strftime("%Y-%m-%dT%H:%M:%SZ")
        resp = requests.get(
            f"{SHOPIFY_BASE}/orders/count.json",
            headers=SHOPIFY_HEADERS,
            params={"financial_status": "paid", "created_at_min": since},
            timeout=10
        )
        order_count = resp.json().get("count", 0)
        return {"weekly_orders": order_count}
    except Exception as e:
        log.error(f"Stats fetch failed: {e}")
        return {}

def get_low_performing_products(conn) -> list:
    """Find listed products with 0 sales to refresh descriptions."""
    try:
        rows = conn.execute("""
            SELECT p.id, p.shopify_id, p.title, p.niche, p.ai_description
            FROM products p
            LEFT JOIN orders o ON o.line_items LIKE '%' || p.shopify_id || '%'
            WHERE p.status = 'listed' AND o.id IS NULL
            LIMIT 10
        """).fetchall()
        return [{"id": r[0], "shopify_id": r[1], "title": r[2],
                 "niche": r[3], "description": r[4]} for r in rows]
    except Exception:
        return []

def refresh_product_description(client, product: dict) -> str:
    """Generate a fresh, more compelling product description."""
    prompt = f"""Rewrite this product description for EdisonHaus, a warm ambient home lighting and decor store.
Use cozy, inviting language that evokes warmth, glow, and comfort. Focus on how this product creates atmosphere
and transforms a space with light and texture. Include benefits-first copy and a clear call-to-action.
Keep it under 200 words. Format as HTML paragraphs.

Product: {product['title']}
Current description: {product['description']}
Niche: {product['niche']}
Store theme: Warm Ambient Home Lighting & Decor — table lamps, pendant lights, fairy lights, wall art, throw pillows, woven storage."""

    try:
        msg = client.messages.create(
            model="claude-sonnet-4-20250514",
            max_tokens=400,
            messages=[{"role": "user", "content": prompt}]
        )
        return msg.content[0].text.strip()
    except Exception as e:
        log.error(f"Description refresh failed: {e}")
        return product["description"]

def update_shopify_description(shopify_id: str, new_description: str) -> bool:
    """Update product description on Shopify."""
    try:
        resp = requests.put(
            f"{SHOPIFY_BASE}/products/{shopify_id}.json",
            headers=SHOPIFY_HEADERS,
            json={"product": {"id": shopify_id, "body_html": new_description}},
            timeout=15
        )
        return resp.status_code == 200
    except Exception:
        return False

def generate_weekly_report(client, conn, stats: dict) -> str:
    """Have Claude write a weekly performance summary."""
    try:
        total_orders = conn.execute("SELECT COUNT(*) FROM orders").fetchone()[0]
        total_revenue = conn.execute("SELECT SUM(total_revenue) FROM orders").fetchone()[0] or 0
        total_profit = conn.execute("SELECT SUM(profit) FROM orders").fetchone()[0] or 0
        total_listed = conn.execute("SELECT COUNT(*) FROM products WHERE status='listed'").fetchone()[0]

        prompt = f"""Write a brief weekly dropshipping business report (3-4 sentences).

Stats:
- Products listed: {total_listed}
- Total orders ever: {total_orders}
- Total revenue: ${total_revenue:.2f}
- Total profit: ${total_profit:.2f}
- This week's orders: {stats.get('weekly_orders', 0)}

Be encouraging but honest. Suggest 1 action item."""

        msg = client.messages.create(
            model="claude-sonnet-4-20250514",
            max_tokens=200,
            messages=[{"role": "user", "content": prompt}]
        )
        return msg.content[0].text.strip()
    except Exception as e:
        return f"Report generation failed: {e}"

def send_email_report(report: str, stats: dict):
    """Send weekly report via Gmail SMTP."""
    if not GMAIL_SENDER:
        log.info(f"Weekly Report:\n{report}")
        return
    try:
        import smtplib
        from email.mime.text import MIMEText
        gmail_pass = os.environ.get("GMAIL_APP_PASSWORD", "")
        msg = MIMEText(f"{report}\n\nFull stats: {json.dumps(stats, indent=2)}")
        msg["Subject"] = f"📦 Dropship Weekly Report — {datetime.now().strftime('%b %d')}"
        msg["From"] = GMAIL_SENDER
        msg["To"] = GMAIL_TO
        with smtplib.SMTP_SSL("smtp.gmail.com", 465) as smtp:
            smtp.login(GMAIL_SENDER, gmail_pass)
            smtp.send_message(msg)
        log.info("Weekly report emailed.")
    except Exception as e:
        log.error(f"Email failed: {e}")

def write_heartbeat(refreshed: int, status: str = "success"):
    HEARTBEAT.write_text(json.dumps({
        "module": "b3_ai_optimizer",
        "last_run": datetime.now().isoformat(),
        "refreshed": refreshed,
        "status": status
    }, indent=2))

def main():
    log.info("=" * 60)
    log.info("B3 AI Optimizer — EdisonHaus")
    log.info("=" * 60)

    try:
        client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)
        conn = sqlite3.connect(DB_PATH)

        # 1. Get store stats
        stats = get_store_stats()
        log.info(f"Store stats: {stats}")

        # 2. Refresh low-performing product descriptions
        low_performers = get_low_performing_products(conn)
        log.info(f"Refreshing {len(low_performers)} low-performing listings")
        refreshed = 0
        for product in low_performers:
            new_desc = refresh_product_description(client, product)
            if product.get("shopify_id") and update_shopify_description(product["shopify_id"], new_desc):
                conn.execute("UPDATE products SET ai_description=? WHERE id=?",
                            (new_desc, product["id"]))
                conn.commit()
                refreshed += 1
                log.info(f"  Refreshed: {product['title'][:50]}")
            time.sleep(1)

        # 3. Generate and send weekly report
        report = generate_weekly_report(client, conn, stats)
        log.info(f"\n{'='*50}\nWEEKLY REPORT:\n{report}\n{'='*50}")
        send_email_report(report, stats)

        write_heartbeat(refreshed)
        conn.close()

    except Exception as e:
        log.error(f"AI optimizer failed: {e}")
        write_heartbeat(0, status=f"error: {e}")
        raise

if __name__ == "__main__":
    main()
