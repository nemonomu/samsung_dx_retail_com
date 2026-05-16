"""Run a bounded main/BSR listing test and feed the filtered result to dt1.

Flow:
1. Crawl main listing for 3 pages.
2. Crawl BSR listing for 3 pages.
3. Remove Open Box rows and duplicate numeric sku/item rows across both CSVs.
4. Clear promo/trend listing CSVs so dt1 only consumes main+BSR test rows.
5. Write detail CSV through GraphQL replay without opening each PDP.
"""

from __future__ import annotations

import csv
import importlib.util
import os
import re
import sys
from datetime import datetime
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent
BBY_VPN_DIR = BASE_DIR.parent
PROJECT_DIR = BBY_VPN_DIR.parent

def add_import_path(path):
    if path and path.exists() and str(path) not in sys.path:
        sys.path.insert(0, str(path))


for path in (BASE_DIR, BBY_VPN_DIR, PROJECT_DIR):
    add_import_path(path)

for ancestor in (BASE_DIR, *BASE_DIR.parents):
    for path in (
        ancestor,
        ancestor / "running",
        ancestor / "bby_vpn",
        ancestor / "bby_vpn" / "running",
    ):
        if (path / "common" / "setup.py").exists():
            add_import_path(path)

for path in (
    BASE_DIR / "running",
    BBY_VPN_DIR / "running",
    PROJECT_DIR / "running",
):
    if str(path) not in sys.path:
        add_import_path(path)

from bby_listing_sku import extract_numeric_sku_from_text
from collectors.graphql_collector import BrowserFetchGraphQLCollector, load_graphql_cookies, load_graphql_registry, load_sku_map
from core.retry import ExponentialBackoff


DETAIL_OPERATION_NAMES = (
    "CustomerRatingCard_Init",
    "Ai_Review_Summary_Init",
    "CustomerReviewList_Init",
    "Reviews_Pros_Cons_Init",
    "ReviewStats_Init",
    "getPDPProductBySkuId",
    "getProduct",
    "ProductSchema_init",
    "ProductSpecification_Init",
    "MediaGalleryImagesAndDetails_Init",
    "ProductHeader_Init",
    "GetCompareProduct",
    "ProductCarousel_Recommendations",
)


def load_v2_class(module_filename, class_name):
    module_path = BASE_DIR / module_filename
    module_name = f"_v2_{module_path.stem}"
    spec = importlib.util.spec_from_file_location(module_name, module_path)
    if not spec or not spec.loader:
        raise ImportError(f"Cannot load {module_path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return getattr(module, class_name)


BestBuyDetailCrawler = load_v2_class("bby_tv_dt1.py", "BestBuyDetailCrawler")
BestBuyTVBSRCrawler = load_v2_class("bby_tv_bsr1.py", "BestBuyTVBSRCrawler")
BestBuyTVMainCrawler = load_v2_class("bby_tv_main1.py", "BestBuyTVMainCrawler")


LISTING_FILES = {
    "main": BASE_DIR / "bby_tv_main1_vpn_test.csv",
    "bsr": BASE_DIR / "bby_tv_bsr1_vpn_test.csv",
    "promotion": BASE_DIR / "bby_tv_pmt1_vpn_test.csv",
    "trend": BASE_DIR / "bby_tv_trend_crawl_vpn_test.csv",
}


def extract_item_from_url(product_url):
    if not product_url:
        return None
    cleaned = product_url.split("?")[0].rstrip("/")
    if "/sku/" in cleaned:
        cleaned = cleaned.split("/sku/")[0]
    item = cleaned.split("/")[-1]
    return item[:-2] if item.endswith(".p") else item


def read_rows(path):
    if not path.exists() or path.stat().st_size == 0:
        return [], []
    with path.open(newline="", encoding="utf-8-sig") as csvfile:
        reader = csv.DictReader(csvfile)
        return list(reader), list(reader.fieldnames or [])


def write_rows(path, rows, fieldnames):
    fieldnames = list(fieldnames or [])
    if rows:
        for key in rows[0].keys():
            if key not in fieldnames:
                fieldnames.append(key)
    with path.open("w", newline="", encoding="utf-8-sig") as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow({field: row.get(field) for field in fieldnames})


def clear_file(path, fieldnames):
    with path.open("w", newline="", encoding="utf-8-sig") as csvfile:
        csv.DictWriter(csvfile, fieldnames=fieldnames).writeheader()


def row_dedupe_key(row):
    product_url = row.get("product_url") or ""
    numeric_sku = row.get("numeric_sku") or extract_numeric_sku_from_text(product_url)
    item = extract_item_from_url(product_url)
    if numeric_sku:
        return f"sku:{numeric_sku}"
    if item:
        return f"item:{item}"
    return f"url:{product_url}"


def resolve_numeric_sku(row, sku_map=None):
    product_url = row.get("product_url") or ""
    item = extract_item_from_url(product_url)
    sku = row.get("numeric_sku") or extract_numeric_sku_from_text(product_url)
    if sku:
        return sku
    for key in (product_url, item):
        if not key:
            continue
        value = (sku_map or {}).get(key)
        if isinstance(value, dict) and value.get("skuId"):
            return str(value.get("skuId"))
        if value:
            return str(value)
    return None


def filter_listing_csvs():
    seen = set()
    stats = {
        "input": 0,
        "kept": 0,
        "openbox": 0,
        "duplicate": 0,
    }

    for source in ("main", "bsr"):
        path = LISTING_FILES[source]
        rows, fieldnames = read_rows(path)
        filtered = []
        for row in rows:
            stats["input"] += 1
            product_url = row.get("product_url") or ""
            if "openbox" in product_url.lower():
                stats["openbox"] += 1
                continue
            key = row_dedupe_key(row)
            if key in seen:
                stats["duplicate"] += 1
                continue
            seen.add(key)
            filtered.append(row)
        stats["kept"] += len(filtered)
        write_rows(path, filtered, fieldnames)
        print(f"[FILTER] {source}: {len(rows)} -> {len(filtered)} rows")

    clear_file(
        LISTING_FILES["promotion"],
        ["account_name", "batch_id", "page_type", "retailer_sku_name", "promotion_rank", "offer",
         "promotion_type", "product_url", "numeric_sku", "crawl_datetime", "calendar_week"],
    )
    clear_file(
        LISTING_FILES["trend"],
        ["account_name", "batch_id", "page_type", "rank", "product_name", "product_url",
         "numeric_sku", "crawl_strdatetime", "calendar_week"],
    )
    print(
        "[FILTER] total: "
        f"input={stats['input']} kept={stats['kept']} "
        f"openbox={stats['openbox']} duplicate={stats['duplicate']}"
    )
    return stats


def filtered_listing_rows():
    rows = []
    for source in ("main", "bsr"):
        source_rows, _ = read_rows(LISTING_FILES[source])
        for row in source_rows:
            row["_source"] = source
            rows.append(row)
    return rows


def run_listing(crawler_cls, label, batch_id, pages):
    print("\n" + "=" * 80)
    print(f"[RUN] {label}: {pages} pages")
    print("=" * 80)
    crawler = crawler_cls(test_mode=False, batch_id=batch_id)
    crawler.max_pages = pages
    crawler.max_products = 10000
    crawler.run()


def configure_dt_env():
    defaults = {
        "BBY_GRAPHQL_REGISTRY_DIR": str(BASE_DIR / "mapping_run"),
        "BBY_DT_CORE_ONLY": "0",
        "BBY_DT_SKIP_REVIEWS": "0",
        "BBY_DT_GRAPHQL_REPLAY": "1",
        "BBY_DT_PDP_GRAPHQL_DISCOVERY": "1",
        "BBY_DT_PDP_GRAPHQL_DISCOVERY_SECONDS": "8",
        "BBY_GRAPHQL_REVIEW_PAGE_SIZE": "20",
        "BBY_DT_SKIP_SIMILAR": "1",
        "BBY_DT_REVIEW_DOM_FALLBACK": "0",
        "BBY_DT_REACTIVE_REFRESH_ON_GRAPHQL_ERROR": "1",
        "BBY_DT_RESTART_EVERY": "6",
        "BBY_DT_COOLDOWN_EVERY": "6",
        "BBY_DT_COOLDOWN_MIN": "240",
        "BBY_DT_COOLDOWN_MAX": "480",
        "BBY_RATE_MIN_DELAY": "16",
        "BBY_RATE_MAX_DELAY": "34",
        "BBY_RATE_MAX_PER_MINUTE": "3",
        "BBY_RATE_MAX_PER_HOUR": "90",
        "BBY_DT_CLEAR_OUTPUT": "1",
    }
    for key, value in defaults.items():
        os.environ.setdefault(key, value)


def format_recommendation(value):
    if value in (None, ""):
        return None
    try:
        return f"{int(value)}% would recommend to a friend"
    except Exception:
        return str(value)


def money(value):
    if value in (None, ""):
        return None
    try:
        return f"${float(value):,.2f}"
    except Exception:
        text = str(value).strip()
        return text if text.startswith("$") else text


def first_value(payload, keys):
    found = None

    def walk(value):
        nonlocal found
        if found is not None:
            return
        if isinstance(value, dict):
            for key in keys:
                if key in value and value[key] not in (None, "", []):
                    found = value[key]
                    return
            for child in value.values():
                walk(child)
        elif isinstance(value, list):
            for child in value:
                walk(child)

    walk(payload)
    return found


def contains_external_review_marker(payload):
    found = False

    def walk(value):
        nonlocal found
        if found:
            return
        if isinstance(value, str):
            text = value.lower()
            if "reviews from" in text or "review from" in text:
                found = True
                return
        elif isinstance(value, dict):
            for child in value.values():
                walk(child)
        elif isinstance(value, list):
            for child in value:
                walk(child)

    walk(payload)
    return found


def contains_text(payload, patterns):
    found = None

    def walk(value):
        nonlocal found
        if found is not None:
            return
        if isinstance(value, str):
            lower = value.lower()
            for pattern, label in patterns:
                if pattern in lower:
                    found = label
                    return
        elif isinstance(value, dict):
            for child in value.values():
                walk(child)
        elif isinstance(value, list):
            for child in value:
                walk(child)

    walk(payload)
    return found


def normalize_review_state(parsed, bundle):
    parsed = dict(parsed or {})
    star_rating = parsed.get("star_rating")
    count = parsed.get("count_of_reviews")
    external_reviews = contains_external_review_marker(bundle)
    not_yet_reviewed = "not yet reviewed" in str(star_rating or "").lower()

    if external_reviews or not_yet_reviewed:
        parsed["star_rating"] = "Not yet reviewed"
        parsed["count_of_reviews"] = 0
        parsed["detailed_review_content"] = None
        parsed["summarized_review_content"] = None
        parsed["recommendation_intent"] = None
        return parsed

    if count in (None, ""):
        parsed["count_of_reviews"] = 0
    return parsed


def parse_price(bundle):
    price_state = contains_text(
        bundle,
        (
            ("no longer available", "no longer available"),
            ("see price in cart", "See price in cart"),
            ("see details in checkout", "See details in checkout"),
        ),
    )
    if price_state == "no longer available":
        return {"final_sku_price": price_state, "original_sku_price": None, "savings": None}

    for operation in ("getProduct", "getPDPProductBySkuId"):
        payload = bundle.get(operation) or {}
        price_payload = first_value(payload, ("price",))
        if not isinstance(price_payload, dict):
            continue
        final_price = (
            price_payload.get("displayableCustomerPrice")
            or price_payload.get("currentPrice")
            or price_payload.get("customerPrice")
            or price_payload.get("salePrice")
        )
        restricted_message = price_payload.get("restrictedPriceDisplayMessage") or price_payload.get("priceWithCart")
        if not final_price and restricted_message:
            marker = contains_text(restricted_message, (("see price in cart", "See price in cart"), ("see details in checkout", "See details in checkout")))
            if marker:
                final_price = marker
        if not final_price and price_state:
            final_price = price_state
        original_price = price_payload.get("displayableRegularPrice") or price_payload.get("regularPrice")
        savings = price_payload.get("totalSavings")
        return {
            "final_sku_price": money(final_price),
            "original_sku_price": money(original_price),
            "savings": money(savings),
        }
    if price_state:
        return {"final_sku_price": price_state, "original_sku_price": None, "savings": None}
    return {"final_sku_price": None, "original_sku_price": None, "savings": None}


def spec_text(payload):
    parts = []

    def walk(value):
        if isinstance(value, dict):
            label = (
                value.get("name")
                or value.get("displayName")
                or value.get("specName")
                or value.get("label")
                or value.get("key")
            )
            val = (
                value.get("value")
                or value.get("displayValue")
                or value.get("values")
                or value.get("description")
                or value.get("text")
            )
            if label is not None or val is not None:
                parts.append(f"{label or ''}: {val or ''}")
            for child in value.values():
                walk(child)
        elif isinstance(value, list):
            for child in value:
                walk(child)

    walk(payload)
    return " | ".join(str(part) for part in parts)


def parse_specs(bundle, product_name=""):
    text = " | ".join(
        spec_text(bundle.get(operation) or {})
        for operation in ("ProductSpecification_Init", "ProductSchema_init", "GetCompareProduct")
    )
    search_text = f"{product_name or ''} | {text}"

    def find_labeled_number(labels):
        for label in labels:
            pattern = rf"{label}[^|:]*[:| ]+[^|]*?(\d+(?:\.\d+)?)"
            match = re.search(pattern, search_text, re.IGNORECASE)
            if match:
                return match.group(1)
        return None

    screen_size = find_labeled_number(("screen size", "class size", "display size", "diagonal screen"))
    if not screen_size:
        match = re.search(r'(\d+(?:\.\d+)?)\s*(?:"|inches|inch|\u201d)\s+class', search_text, re.IGNORECASE)
        if match:
            screen_size = match.group(1)
    if screen_size and "inch" not in screen_size.lower():
        screen_size = f"{screen_size} inches"

    electricity_use = find_labeled_number(("estimated annual electricity use", "annual energy consumption", "electricity use"))
    model_year = first_value(bundle.get("ProductSchema_init") or {}, ("modelYear",))
    if not model_year:
        match = re.search(r"\b(20[2-4]\d)\b", search_text)
        model_year = match.group(1) if match else None
    model_number = first_value(bundle.get("ProductSchema_init") or {}, ("modelNumber",))
    if not model_number:
        model_number = first_value(bundle.get("ProductSpecification_Init") or {}, ("modelNumber", "model"))
    if not model_number:
        match = re.search(r"(?:manufacturer\s+)?model\s+(?:number|no\.?)\s*:\s*([^|]+)", search_text, re.IGNORECASE)
        model_number = match.group(1).strip() if match else None

    return {
        "screen_size": screen_size,
        "estimated_annual_electricity_use": electricity_use,
        "model_year": model_year,
        "model_number": model_number,
    }


def parse_similar_products(bundle, current_name=None, limit=4):
    names = []
    seen = set()

    def add_name(value):
        if not value:
            return
        text = str(value).strip()
        if len(text) < 8:
            return
        if current_name and text == current_name:
            return
        key = text.lower()
        if key in seen:
            return
        seen.add(key)
        names.append(text)

    def walk(value):
        if len(names) >= limit:
            return
        if isinstance(value, dict):
            name_obj = value.get("name")
            if isinstance(name_obj, dict):
                add_name(name_obj.get("short") or name_obj.get("title"))
            elif isinstance(name_obj, str):
                add_name(name_obj)
            add_name(value.get("productName") or value.get("title"))
            for child in value.values():
                walk(child)
        elif isinstance(value, list):
            for child in value:
                walk(child)

    walk(bundle.get("GetCompareProduct") or {})
    walk(bundle.get("ProductCarousel_Recommendations") or {})
    return [{"product_name": name} for name in names[:limit]]


def detail_operation_names(registry):
    return [name for name in DETAIL_OPERATION_NAMES if name in registry]


def run_api_only_detail(rows):
    print("\n" + "=" * 80)
    print(f"[RUN] API-only detail CSV for {len(rows)} filtered listing rows")
    print("=" * 80)

    configure_dt_env()
    registry_dir = os.environ.get("BBY_GRAPHQL_REGISTRY_DIR") or str(BASE_DIR / "mapping_run")
    registry = load_graphql_registry(registry_dir)
    cookies = load_graphql_cookies(registry_dir)
    saved_sku_map = load_sku_map(registry_dir)
    if not registry:
        print(f"[WARNING] GraphQL registry not found: {registry_dir}")
    operation_names = detail_operation_names(registry)
    if operation_names:
        print(f"[INFO] Detail GraphQL operations: {', '.join(operation_names)}")

    crawler = BestBuyDetailCrawler()
    collector = None
    try:
        collector = BrowserFetchGraphQLCollector(
            audit_log=crawler.audit_log,
            timeout=int(os.environ.get("BBY_GRAPHQL_REPLAY_TIMEOUT", "45")),
            concurrency=1,
            retry_policy=ExponentialBackoff(
                max_attempts=int(os.environ.get("BBY_GRAPHQL_REPLAY_MAX_ATTEMPTS", "1")),
                base_delay=1.0,
                max_delay=10.0,
            ),
        )
        try:
            page = collector._ensure_page()
            page.get("https://www.bestbuy.com/")
        except Exception as exc:
            print(f"[WARNING] Browser origin warmup failed; GraphQL may fail: {exc}")

        for order, row in enumerate(rows, 1):
            product_url = row.get("product_url")
            item = extract_item_from_url(product_url)
            numeric_sku = resolve_numeric_sku(row, saved_sku_map)
            parsed = {}
            errors = None
            if registry and product_url and numeric_sku:
                bundle = collector.collect_review_bundle_sync(
                    product_url,
                    registry,
                    cookies=cookies,
                    sku_map={product_url: numeric_sku, item: numeric_sku},
                    operation_names=operation_names,
                )
                parsed = bundle.get("parsed") or {}
                if parsed.get("count_of_reviews") is None:
                    parsed["count_of_reviews"] = first_value(bundle, ("reviewCount", "totalReviewCount", "numberOfReviews"))
                if parsed.get("star_rating") is None:
                    parsed["star_rating"] = first_value(bundle, ("averageRating", "ratingValue", "starRating"))
                parsed = normalize_review_state(parsed, bundle)
                price_data = parse_price(bundle)
                spec_data = parse_specs(bundle, row.get("retailer_sku_name") or row.get("product_name"))
                similar_products = parse_similar_products(bundle, row.get("retailer_sku_name") or row.get("product_name"))
                errors = bundle.get("errors")
                crawler.record_graphql_sku_map(product_url, numeric_sku)
            elif not numeric_sku:
                errors = {"skuId": "numeric_sku missing from listing"}
                price_data = {}
                spec_data = {}
                similar_products = []
            else:
                price_data = {}
                spec_data = {}
                similar_products = []

            if errors:
                print(f"[API-ONLY] {order}/{len(rows)} {item}: GraphQL partial/failed: {errors}")
            else:
                print(f"[API-ONLY] {order}/{len(rows)} {item}: GraphQL ok")

            crawler.save_to_db(
                page_type=row.get("page_type") or row.get("_source") or "listing_api",
                order=order,
                retailer_sku_name=row.get("retailer_sku_name") or row.get("product_name"),
                item=item,
                electricity_use=spec_data.get("estimated_annual_electricity_use"),
                screen_size=spec_data.get("screen_size"),
                count_of_reviews=parsed.get("count_of_reviews"),
                count_of_star_ratings=parsed.get("count_of_reviews"),
                top_mentions=None,
                detailed_reviews=parsed.get("detailed_review_content"),
                summarized_review_content=parsed.get("summarized_review_content"),
                recommendation_intent=format_recommendation(parsed.get("recommendation_intent")),
                product_url=product_url,
                final_sku_price=price_data.get("final_sku_price"),
                savings=price_data.get("savings"),
                original_sku_price=price_data.get("original_sku_price"),
                offer=row.get("offer"),
                pick_up_availability=row.get("pick_up_availability"),
                shipping_availability=row.get("shipping_availability") or row.get("fastest_delivery"),
                delivery_availability=row.get("delivery_availability"),
                sku_status=row.get("sku_status"),
                star_rating_source=parsed.get("star_rating"),
                promotion_type=row.get("promotion_type"),
                promotion_position=row.get("promotion_rank"),
                bsr_rank=row.get("bsr_rank"),
                main_rank=row.get("main_rank"),
                trend_rank=row.get("trend_rank"),
                model_year=spec_data.get("model_year"),
                sku=spec_data.get("model_number") or numeric_sku or "no sku",
                similar_products=similar_products or None,
            )
            crawler.total_collected += 1
    finally:
        if collector:
            collector.close()


def main():
    pages = int(os.environ.get("BBY_LISTING_TEST_PAGES", "3"))
    batch_id = datetime.now().strftime("%Y%m%d_%H%M%S")
    os.chdir(BASE_DIR)

    for path in LISTING_FILES.values():
        if path.exists():
            path.unlink()

    run_listing(BestBuyTVMainCrawler, "main listing", batch_id, pages)
    run_listing(BestBuyTVBSRCrawler, "bsr listing", batch_id, pages)
    stats = filter_listing_csvs()
    if stats["kept"] <= 0:
        print("[ERROR] No listing rows left after filtering. Detail crawl skipped.")
        return 1

    run_api_only_detail(filtered_listing_rows())
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
