from __future__ import annotations

import argparse
import csv
import json
import os
import re
from datetime import datetime
from pathlib import Path


PRODUCT_LOWERS = ("hhp", "tv", "ref", "ldy")

COMMON_COLS = [
    "country", "product", "item", "sku", "account_name", "page_type",
    "retailer_sku_name", "product_url", "calendar_week", "crawl_datetime", "batch_id",
    "star_rating", "count_of_star_ratings",
    "detailed_review_content", "retailer_sku_name_similar",
    "final_sku_price", "original_sku_price", "discount_type",
    "delivery_availability",
    "sku_popularity", "sku_status", "main_rank", "bsr_rank",
]
AMAZON_ONLY_COLS = [
    "summarized_review_content", "fastest_delivery", "inventory_status",
    "sku_assurance", "number_of_units_purchased_past_month",
]
FLIPKART_ONLY_COLS = [
    "count_of_reviews", "available_quantity_for_purchase", "savings",
]
PRODUCT_SPECIFIC = {
    "hhp": ["hhp_storage", "hhp_color", "trade_in"],
    "tv": ["screen_size", "model_year", "estimated_annual_electricity_use"],
    "ref": ["ref_refrigerator_type", "ref_capacity"],
    "ldy": ["ldy_loading_type", "ldy_capacity"],
}
COLUMNS_BY_PRODUCT = {
    product: COMMON_COLS + AMAZON_ONLY_COLS + FLIPKART_ONLY_COLS + PRODUCT_SPECIFIC[product]
    for product in PRODUCT_LOWERS
}
COLUMNS_LIST = [
    "country", "product", "item", "account_name", "page_type",
    "retailer_sku_name", "product_url", "calendar_week", "crawl_datetime", "batch_id",
    "star_rating", "count_of_star_ratings", "count_of_reviews",
    "final_sku_price", "original_sku_price", "savings", "discount_type",
    "available_quantity_for_purchase",
    "sku_popularity", "sku_status", "main_rank", "bsr_rank",
    "number_of_units_purchased_past_month",
]

ASIN_RE = re.compile(r"/(?:dp|gp/product)/([A-Z0-9]{10})")
FPKT_PID_RE = re.compile(r"[?&]pid=([A-Z0-9]+)")
PRICE_RE = re.compile(r"^(₹)([\d,]+)(.*)$")


def now() -> str:
    return datetime.now().isoformat(timespec="seconds")


def normalize_price(value):
    if not value or "₹" not in str(value):
        return value
    match = PRICE_RE.match(str(value))
    if not match:
        return value
    prefix, digits, suffix = match.groups()
    raw = digits.replace(",", "")
    return f"{prefix}{int(raw):,}{suffix}" if raw.isdigit() else value


def normalize_count(value):
    if value in (None, ""):
        return None
    raw = str(value).strip().replace(",", "")
    return f"{int(raw):,}" if raw.isdigit() else value


def parse_int_safe(value):
    if value in (None, ""):
        return None
    try:
        return int(str(value).replace(",", ""))
    except (TypeError, ValueError):
        return None


def calendar_week_iso(value):
    if not value:
        return None
    try:
        dt = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None
    iso_year, iso_week, _ = dt.isocalendar()
    return f"{iso_year}-W{iso_week:02d}"


def url_path(url: str) -> str:
    return (url or "").split("?", 1)[0].rstrip("/")


def listing_key(rec: dict) -> str:
    item = rec.get("asin") or rec.get("fsn")
    if item:
        return item
    url = rec.get("product_url") or rec.get("source_url") or ""
    match = ASIN_RE.search(url)
    if match:
        return match.group(1)
    match = FPKT_PID_RE.search(url)
    if match:
        return match.group(1)
    return url_path(url)


def read_jsonl(path: Path):
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                yield json.loads(line)
            except json.JSONDecodeError:
                continue


def merge(listing: dict, detail: dict, max_n: int = 0) -> list[dict]:
    rows = []
    items = list(listing.items())
    if max_n and max_n > 0:
        items = items[:max_n]
    for key, entry in items:
        main = entry.get("main") or {}
        bsr = entry.get("bsr") or {}
        primary = main or bsr
        if not primary:
            continue
        det = detail.get(key, {})
        account = (primary.get("account_name") or det.get("account_name") or "").capitalize()
        product = (primary.get("product") or det.get("product") or "").upper()
        item = det.get("fsn") or det.get("asin") or primary.get("fsn") or primary.get("asin")
        crawl_datetime = det.get("crawl_datetime") or primary.get("crawl_datetime")
        rows.append({
            "country": "siel",
            "product": product or None,
            "item": item,
            "sku": det.get("sku") or primary.get("sku"),
            "account_name": account or None,
            "page_type": "main" if main else "bsr",
            "retailer_sku_name": primary.get("retailer_sku_name") or det.get("retailer_sku_name"),
            "product_url": primary.get("product_url") or det.get("source_url"),
            "calendar_week": calendar_week_iso(crawl_datetime),
            "crawl_datetime": crawl_datetime,
            "batch_id": primary.get("batch_id") or det.get("batch_id"),
            "star_rating": det.get("star_rating") or primary.get("star_rating"),
            "count_of_star_ratings": normalize_count(det.get("count_of_star_ratings") or primary.get("count_of_star_ratings")),
            "count_of_reviews": normalize_count(det.get("count_of_reviews") or primary.get("count_of_reviews")),
            "detailed_review_content": det.get("detailed_review_content"),
            "retailer_sku_name_similar": det.get("retailer_sku_name_similar"),
            "final_sku_price": normalize_price(primary.get("final_sku_price") or det.get("final_sku_price")),
            "original_sku_price": normalize_price(primary.get("original_sku_price") or det.get("original_sku_price")),
            "savings": primary.get("savings") or det.get("savings"),
            "discount_type": primary.get("discount_type") or det.get("discount_type"),
            "delivery_availability": det.get("delivery_availability") or primary.get("delivery_availability"),
            "available_quantity_for_purchase": primary.get("available_quantity_for_purchase"),
            "sku_popularity": primary.get("sku_popularity") or det.get("sku_popularity"),
            "sku_status": primary.get("sku_status"),
            "main_rank": parse_int_safe(main.get("main_rank")) if main else None,
            "bsr_rank": parse_int_safe(bsr.get("bsr_rank")) if bsr else None,
            "screen_size": det.get("screen_size"),
            "model_year": det.get("model_year"),
            "estimated_annual_electricity_use": det.get("estimated_annual_electricity_use"),
            "hhp_storage": det.get("hhp_storage"),
            "hhp_color": det.get("hhp_color"),
            "trade_in": det.get("trade_in"),
            "ref_refrigerator_type": det.get("ref_refrigerator_type"),
            "ref_capacity": det.get("ref_capacity"),
            "ldy_loading_type": det.get("ldy_loading_type"),
            "ldy_capacity": det.get("ldy_capacity"),
            "summarized_review_content": det.get("summarized_review_content"),
            "fastest_delivery": det.get("fastest_delivery"),
            "inventory_status": det.get("inventory_status"),
            "sku_assurance": det.get("sku_assurance"),
            "number_of_units_purchased_past_month": primary.get("number_of_units_purchased_past_month"),
        })
    return rows


def build_rows(jsonl_path: Path, max_n: int = 0):
    listing = {}
    detail = {}
    counts = {"main": 0, "bsr": 0, "detail": 0, "other": 0}
    for rec in read_jsonl(jsonl_path):
        stage = rec.get("stage")
        key = listing_key(rec)
        if not key:
            counts["other"] += 1
            continue
        if stage in {"main", "bsr"}:
            listing.setdefault(key, {"main": None, "bsr": None})[stage] = rec
            counts[stage] += 1
        elif stage == "detail":
            detail[key] = rec
            counts["detail"] += 1
        else:
            counts["other"] += 1
    return merge(listing, detail, max_n=max_n), merge(listing, {}, max_n=max_n), counts


def write_csv(path: Path, rows: list[dict], columns: list[str]):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=columns, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def export(jsonl_path: Path, output_dir: Path, max_n: int = 0):
    rows_full, rows_listing, counts = build_rows(jsonl_path, max_n=max_n)
    product = next((str(row.get("product") or "").lower() for row in rows_full + rows_listing if row.get("product")), "")
    columns = COLUMNS_BY_PRODUCT.get(product, COMMON_COLS + AMAZON_ONLY_COLS + FLIPKART_ONLY_COLS)
    final_path = output_dir / "final_output.csv"
    list_path = output_dir / "product_list.csv"
    write_csv(final_path, rows_full, columns)
    write_csv(list_path, rows_listing, COLUMNS_LIST)
    manifest = {
        "run_type": "siel_csv_export",
        "started_at": now(),
        "finished_at": now(),
        "jsonl_path": str(jsonl_path),
        "output_dir": str(output_dir),
        "final_output_csv": str(final_path),
        "product_list_csv": str(list_path),
        "input_counts": counts,
        "final_rows": len(rows_full),
        "product_list_rows": len(rows_listing),
        "success": True,
    }
    (output_dir / "csv_export_manifest.json").write_text(
        json.dumps(manifest, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )
    return manifest


def main() -> int:
    parser = argparse.ArgumentParser(description="Export SIEL JSONL crawl records to CSV artifacts.")
    parser.add_argument("jsonl_path")
    parser.add_argument("--output-dir", default="")
    parser.add_argument("--max-n", type=int, default=0)
    args = parser.parse_args()
    jsonl_path = Path(args.jsonl_path)
    output_dir = Path(args.output_dir) if args.output_dir else jsonl_path.parent / (jsonl_path.stem + "_output")
    manifest = export(jsonl_path, output_dir, max_n=args.max_n)
    print(json.dumps(manifest, indent=2, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
