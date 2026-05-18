import csv
import os
import re
from datetime import datetime
from pathlib import Path

from bs4 import BeautifulSoup
from zenrows import ZenRowsClient

from .step00_config import BESTBUY_BASE_URL, DEFAULT_BESTBUY_RUN_ROOT, has_target_url, load_initial_urls


RUN_DATE = os.getenv("BESTBUY_RUN_DATE", datetime.now().strftime("%Y%m%d"))
INPUT_HTML = Path(os.getenv("BESTBUY_TRENDING_HTML", "references/bestbuy_tv_trending_page_sample.html"))
OUTPUT_CSV = Path(
    os.getenv(
        "BESTBUY_TRENDING_OUTPUT",
        DEFAULT_BESTBUY_RUN_ROOT / "trending" / "parsed" / "trending_products.csv",
    )
)
LIMIT = int(os.getenv("BESTBUY_TRENDING_LIMIT", "6"))
REQUEST_TIMEOUT = int(os.getenv("ZENROWS_TIMEOUT", "180"))


def clean_text(value):
    return " ".join(str(value or "").split())


def absolute_url(path):
    if not path:
        return ""
    if path.startswith("http"):
        return path
    return f"{BESTBUY_BASE_URL}{path}"


def extract_sku_from_card(card):
    html = str(card)
    for pattern in [r"plp-add-to-cart-(\d+)", r"/site/reviews/[^/]+/(\d+)", r"skuId[=:\"']+(\d+)"]:
        match = re.search(pattern, html)
        if match:
            return match.group(1)
    return ""


def parse_trending_cards(html_text, limit=6):
    soup = BeautifulSoup(html_text, "html.parser")
    cards = []
    seen = set()

    selectors = [
        '[data-testid^="product-carousel-card-"]',
        '[data-testid^="product-grid-card-"]',
    ]
    for selector in selectors:
        for card in soup.select(selector):
            sku_id = extract_sku_from_card(card)
            product_link = card.find("a", href=lambda href: href and href.startswith("/product/"))
            product_url = absolute_url(product_link.get("href") if product_link else "")
            name = ""
            h3 = card.find("h3")
            if h3:
                name = clean_text(h3.get_text(" ", strip=True))
            elif product_link:
                name = clean_text(product_link.get("aria-label") or product_link.get_text(" ", strip=True))
            key = sku_id or product_url
            if not key or key in seen:
                continue
            seen.add(key)
            cards.append(
                {
                    "trend_rank": len(cards) + 1,
                    "sku_id": sku_id,
                    "retailer_sku_name": name,
                    "product_url": product_url,
                    "source_card_id": card.get("data-testid", ""),
                }
            )
            if len(cards) >= limit:
                return cards
    return cards


def fetch_trending_html():
    api_key = os.getenv("ZENROWS_API_KEY")
    if not api_key:
        raise RuntimeError("Set ZENROWS_API_KEY in .env")
    url = load_initial_urls().get("trending_tvs_projectors", "")
    if not url:
        raise RuntimeError("Missing BestBuy trend URL")
    INPUT_HTML.parent.mkdir(parents=True, exist_ok=True)
    response = ZenRowsClient(api_key).get(
        url,
        params={
            "custom_headers": "true",
            "premium_proxy": "true",
            "proxy_country": "us",
            "js_render": "true",
        },
        timeout=REQUEST_TIMEOUT,
    )
    INPUT_HTML.write_text(response.text or "", encoding="utf-8", errors="replace")
    if response.status_code != 200:
        raise RuntimeError(f"Failed to fetch trending HTML: status={response.status_code} path={INPUT_HTML}")
    return response.text or ""


def main():
    if not has_target_url("trend"):
        OUTPUT_CSV.parent.mkdir(parents=True, exist_ok=True)
        with OUTPUT_CSV.open("w", newline="", encoding="utf-8-sig") as f:
            writer = csv.DictWriter(
                f,
                fieldnames=["trend_rank", "sku_id", "retailer_sku_name", "product_url", "source_card_id"],
            )
            writer.writeheader()
        print(f"skipped trending: no trend URL for category -> {OUTPUT_CSV}")
        return
    if INPUT_HTML.exists():
        html_text = INPUT_HTML.read_text(encoding="utf-8", errors="ignore")
    else:
        print(f"trending_source_html_missing={INPUT_HTML}; fetching with ZenRows")
        html_text = fetch_trending_html()
    rows = parse_trending_cards(html_text, LIMIT)
    OUTPUT_CSV.parent.mkdir(parents=True, exist_ok=True)
    with OUTPUT_CSV.open("w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=["trend_rank", "sku_id", "retailer_sku_name", "product_url", "source_card_id"],
        )
        writer.writeheader()
        writer.writerows(rows)
    print(f"wrote {len(rows)} rows -> {OUTPUT_CSV}")
    for row in rows:
        print(f"{row['trend_rank']}. {row['sku_id']} {row['retailer_sku_name']}")


if __name__ == "__main__":
    main()
