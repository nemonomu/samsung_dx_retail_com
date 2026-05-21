import csv
import json
import os
import re
import time
from datetime import datetime
from pathlib import Path

from bs4 import BeautifulSoup
from zenrows import ZenRowsClient

from .step00_config import BESTBUY_BASE_URL, DEFAULT_BESTBUY_RUN_ROOT, has_target_url, rel_path, target_url


RUN_DATE = os.getenv("BESTBUY_RUN_DATE", datetime.now().strftime("%Y%m%d"))
RUN_ROOT = Path(os.getenv("BESTBUY_TRENDING_RUN_ROOT", DEFAULT_BESTBUY_RUN_ROOT / "trending"))
INPUT_HTML = os.getenv("BESTBUY_TRENDING_HTML", "").strip()
OUTPUT_CSV = Path(
    os.getenv(
        "BESTBUY_TRENDING_OUTPUT",
        RUN_ROOT / "parsed" / "trending_products.csv",
    )
)
LIMIT = int(os.getenv("BESTBUY_TRENDING_LIMIT", "10"))
REQUEST_TIMEOUT = int(os.getenv("ZENROWS_TIMEOUT", "180"))
FORCE_REFRESH = os.getenv("BESTBUY_FORCE_REFRESH", "0").lower() in {"1", "true", "yes", "y"}


def now():
    return datetime.now().isoformat(timespec="seconds")


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


def extract_sku_from_url(url):
    match = re.search(r"/sku/(\d+)", str(url or ""))
    if match:
        return match.group(1)
    match = re.search(r"/site/[^/?#]+/(\d+)(?:[/?#]|$)", str(url or ""))
    if match:
        return match.group(1)
    return ""


def append_card(cards, seen, sku_id, name, product_url, source_card_id):
    key = sku_id or product_url
    if not key or key in seen:
        return False
    seen.add(key)
    cards.append(
        {
            "trend_rank": len(cards) + 1,
            "sku_id": sku_id,
            "retailer_sku_name": name,
            "product_url": product_url,
            "source_card_id": source_card_id,
        }
    )
    return True


def parse_trending_cards(html_text, limit=10):
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
            append_card(cards, seen, sku_id, name, product_url, card.get("data-testid", ""))
            if len(cards) >= limit:
                return cards
    for link in soup.find_all("a", href=True):
        href = link.get("href") or ""
        if not href.startswith("/product/"):
            continue
        product_url = absolute_url(href)
        sku_id = extract_sku_from_url(product_url)
        name = clean_text(link.get("aria-label") or link.get_text(" ", strip=True))
        append_card(cards, seen, sku_id, name, product_url, "product_link_fallback")
        if len(cards) >= limit:
            return cards
    return cards


def raw_paths(status=None):
    raw_root = RUN_ROOT / "raw"
    if status:
        folder = raw_root / f"trending_{status}"
        folder.mkdir(parents=True, exist_ok=True)
        return {
            "folder": folder,
            "html": folder / "trending_response.html",
            "headers": folder / "trending_headers.json",
            "meta": folder / "trending_meta.json",
        }
    for suffix in ("success", "fail"):
        folder = raw_root / f"trending_{suffix}"
        if folder.exists():
            return {
                "folder": folder,
                "html": folder / "trending_response.html",
                "headers": folder / "trending_headers.json",
                "meta": folder / "trending_meta.json",
            }
    return {
        "folder": raw_root,
        "html": raw_root / "trending_response.html",
        "headers": raw_root / "trending_headers.json",
        "meta": raw_root / "trending_meta.json",
    }


def read_json(path):
    path = Path(path)
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except ValueError:
        return {}


def load_cached_html():
    if FORCE_REFRESH or INPUT_HTML:
        return "", {}
    paths = raw_paths()
    meta = read_json(paths["meta"])
    if int(meta.get("status_code") or 0) != 200 or not paths["html"].exists():
        return "", {}
    html_text = paths["html"].read_text(encoding="utf-8", errors="ignore")
    if not html_text:
        return "", {}
    return html_text, meta


def fetch_trending_html(url):
    api_key = os.getenv("ZENROWS_API_KEY")
    if not api_key:
        raise RuntimeError("Set ZENROWS_API_KEY in .env")
    client = ZenRowsClient(api_key)
    started_at = now()
    start = time.perf_counter()
    response = client.get(
        url,
        params={
            "premium_proxy": "true",
            "proxy_country": "us",
            "js_render": "true",
        },
        timeout=REQUEST_TIMEOUT,
    )
    elapsed = round(time.perf_counter() - start, 3)
    status = "success" if response.status_code == 200 else "fail"
    paths = raw_paths(status)
    paths["html"].write_text(response.text or "", encoding="utf-8", errors="replace")
    paths["headers"].write_text(json.dumps(dict(response.headers), indent=2, ensure_ascii=False), encoding="utf-8")
    meta = {
        "started_at": started_at,
        "finished_at": now(),
        "url": url,
        "status_code": response.status_code,
        "elapsed_seconds": elapsed,
        "x_request_cost": response.headers.get("x-request-cost", ""),
        "bytes": len(response.text or ""),
        "artifact_folder": rel_path(paths["folder"]),
        "html_path": rel_path(paths["html"]),
        "headers_path": rel_path(paths["headers"]),
    }
    paths["meta"].write_text(json.dumps(meta, indent=2, ensure_ascii=False), encoding="utf-8")
    return response.text or "", meta


def load_trending_html(url):
    if INPUT_HTML:
        path = Path(INPUT_HTML)
        return path.read_text(encoding="utf-8", errors="ignore"), {
            "url": str(path),
            "status_code": "local",
            "x_request_cost": 0,
            "bytes": path.stat().st_size if path.exists() else 0,
            "html_path": rel_path(path),
        }
    cached_html, cached_meta = load_cached_html()
    if cached_html:
        return cached_html, cached_meta
    return fetch_trending_html(url)


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
    url = target_url("trend")
    if not url:
        raise RuntimeError("trend URL is enabled but target_url('trend') is blank")
    html_text, source_meta = load_trending_html(url)
    rows = parse_trending_cards(html_text, LIMIT)
    OUTPUT_CSV.parent.mkdir(parents=True, exist_ok=True)
    with OUTPUT_CSV.open("w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=["trend_rank", "sku_id", "retailer_sku_name", "product_url", "source_card_id"],
        )
        writer.writeheader()
        writer.writerows(rows)
    summary = {
        "run_type": "step06_trending_deals",
        "started_at": source_meta.get("started_at", now()),
        "finished_at": now(),
        "url": url,
        "input_html_override": INPUT_HTML,
        "status_code": source_meta.get("status_code", ""),
        "x_request_cost": source_meta.get("x_request_cost", ""),
        "bytes": source_meta.get("bytes", len(html_text or "")),
        "row_count": len(rows),
        "limit": LIMIT,
        "shortfall": max(0, LIMIT - len(rows)),
        "unique_sku_count": len({row.get("sku_id") for row in rows if row.get("sku_id")}),
        "csv": rel_path(OUTPUT_CSV),
        "html_path": source_meta.get("html_path", ""),
    }
    RUN_ROOT.mkdir(parents=True, exist_ok=True)
    (RUN_ROOT / "summary.json").write_text(json.dumps(summary, indent=2, ensure_ascii=False), encoding="utf-8")
    print(json.dumps(summary, indent=2, ensure_ascii=False))
    print(f"wrote {len(rows)} rows -> {OUTPUT_CSV}")
    for row in rows:
        print(f"{row['trend_rank']}. {row['sku_id']} {row['retailer_sku_name']}")


if __name__ == "__main__":
    main()
