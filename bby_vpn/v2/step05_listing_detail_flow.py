"""Shared listing/detail step implementation used by the V2 orchestrator.

The public execution order lives in ``pipeline.steps``. This module keeps the
older helper functions in one importable place while the crawler classes are
being split further.
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

from listing_sku import extract_numeric_sku_from_text
from graphql_collector import BrowserFetchGraphQLCollector, load_graphql_cookies, load_graphql_registry, load_sku_map
from config import DB_CONFIG
from db_readonly import connect_readonly
from retry_policy import ExponentialBackoff
from data_paths import ensure_data_layout, graphql_registry_dir, listing_csv_path


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
    "URE_FetchRecommendations",
)

DETAIL_FIELD_OPERATION_HINTS = {
    "ProductSpecification_Init": "screen_size / estimated_annual_electricity_use / model_year",
    "ProductSchema_init": "model_year / model_number",
    "ProductHeader_Init": "title / price / fulfillment header fields",
    "getPDPProductBySkuId": "price / product status / fulfillment fields",
    "getProduct": "price / product status / fulfillment fields",
    "GetCompareProduct": "retailer_sku_name_similar / compare specs",
    "ProductCarousel_Recommendations": "retailer_sku_name_similar",
    "URE_FetchRecommendations": "retailer_sku_name_similar",
}


def load_v2_class(module_filename, class_name):
    module_path = BASE_DIR / module_filename
    module_name = f"_v2_{module_path.stem}"
    spec = importlib.util.spec_from_file_location(module_name, module_path)
    if not spec or not spec.loader:
        raise ImportError(f"Cannot load {module_path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return getattr(module, class_name)


BestBuyDetailCrawler = load_v2_class("step06_detail_crawler.py", "BestBuyDetailCrawler")
BestBuyTVBSRCrawler = load_v2_class("step02_bsr_listing.py", "BestBuyTVBSRCrawler")
BestBuyTVMainCrawler = load_v2_class("step01_main_listing.py", "BestBuyTVMainCrawler")
BestBuyTVPromotionCrawler = load_v2_class("step03_promotion_listing.py", "BestBuyTVPromotionCrawler")
BestBuyTVTrendCrawler = load_v2_class("step04_trend_listing.py", "BestBuyTVTrendCrawler")


ensure_data_layout()

LISTING_FILES = {
    "main": listing_csv_path("main"),
    "bsr": listing_csv_path("bsr"),
    "promotion": listing_csv_path("promotion"),
    "trend": listing_csv_path("trend"),
}

RAW_TARGET_FILES = {
    "main": LISTING_FILES["main"].parent / "bby_tv_main_raw_target_list.csv",
    "bsr": LISTING_FILES["bsr"].parent / "bby_tv_bsr_raw_target_list.csv",
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


def load_listing_numeric_sku_map():
    """Use listing CSVs as the first local source of URL/item -> numeric skuId."""
    result = {}
    for path in LISTING_FILES.values():
        rows, _ = read_rows(path)
        for row in rows:
            product_url = row.get("product_url") or ""
            item = extract_item_from_url(product_url)
            sku = row.get("numeric_sku") or extract_numeric_sku_from_text(product_url)
            if not sku:
                continue
            if product_url:
                result.setdefault(product_url, str(sku))
            if item:
                result.setdefault(item, str(sku))
    if result:
        print(f"[INFO] Loaded listing numeric sku map: {len(result)} keys")
    return result


def merge_missing_values(row, enrichment):
    if not enrichment:
        return row
    for key, value in enrichment.items():
        if value not in (None, "") and row.get(key) in (None, ""):
            row[key] = value
    return row


def normalized_sku_status(value):
    text = str(value or "").strip()
    if text.lower() == "sponsored":
        return "Sponsored"
    if text.lower() == "active":
        return ""
    return text


def missing_detail_operation_hints(registry):
    if not isinstance(registry, dict):
        registry = {}
    missing = []
    for operation, fields in DETAIL_FIELD_OPERATION_HINTS.items():
        if operation not in registry:
            missing.append(f"{operation} ({fields})")
    return missing


def summarize_graphql_errors(errors, limit=3):
    if not errors:
        return ""
    if not isinstance(errors, dict):
        return str(errors)[:500]
    summaries = []
    for operation, operation_errors in errors.items():
        for error in operation_errors or []:
            if isinstance(error, dict):
                path = error.get("path")
                path_text = ".".join(str(part) for part in path) if isinstance(path, list) else str(path or "")
                message = str(error.get("message") or "")
                summaries.append(f"{operation}:{path_text or '-'}:{message[:120]}")
            else:
                summaries.append(f"{operation}:{str(error)[:120]}")
            if len(summaries) >= limit:
                return " | ".join(summaries)
    return " | ".join(summaries)


def load_listing_enrichment_map():
    """Load product identity fields only.

    Prices, Sponsored labels, offers, pickup, delivery, and shipping are
    occurrence fields. They must never be copied by SKU into another row.
    """
    result = {}
    useful_fields = (
        "sku_id",
        "numeric_sku",
        "bsin",
    )

    def add(row):
        sku = row.get("numeric_sku") or row.get("sku_id") or extract_numeric_sku_from_text(row.get("product_url") or "")
        product_url = row.get("product_url") or ""
        item = row.get("bsin") or extract_item_from_url(product_url)
        values = {field: row.get(field) for field in useful_fields if row.get(field) not in (None, "")}
        if row.get("bsin"):
            values["item"] = row.get("bsin")
        if sku:
            values["sku"] = str(sku)
        for key in (sku, product_url, item):
            if not key:
                continue
            current = result.setdefault(str(key), {})
            merge_missing_values(current, values)

    for path in RAW_TARGET_FILES.values():
        rows, _ = read_rows(path)
        for row in rows:
            add(row)
    for path in LISTING_FILES.values():
        rows, _ = read_rows(path)
        for row in rows:
            add(row)
    if result:
        print(f"[INFO] Loaded listing enrichment map: {len(result)} keys")
    return result


def exact_occurrence_keys(row, source):
    if not source:
        return []
    keys = []
    rank = (
        row.get("main_rank")
        or row.get("bsr_rank")
        or row.get("trend_rank")
        or row.get("promotion_position")
        or row.get("promotion_rank")
    )
    if rank not in (None, ""):
        keys.append(f"{source}|rank:{str(rank).strip()}")

    page_number = str(row.get("page_number") or row.get("page") or "").strip()
    if not page_number:
        return keys
    product_url = str(row.get("product_url") or "").strip()
    sku = str(row.get("numeric_sku") or row.get("sku_id") or extract_numeric_sku_from_text(product_url) or "").strip()
    if product_url:
        keys.append(f"{source}|page:{page_number}|url:{product_url}")
    if sku:
        keys.append(f"{source}|page:{page_number}|sku:{sku}")
    return keys


def load_listing_occurrence_map():
    """Load exact listing occurrence fields from raw target CSVs only.

    A value is exposed only when the source/page/url or source/page/sku key maps
    to a single raw row. Ambiguous duplicated cards are intentionally skipped.
    """
    buckets = {}
    occurrence_fields = (
        "sku_id",
        "numeric_sku",
        "bsin",
        "retailer_sku_name",
        "product_name",
        "product_url",
        "source_product_url",
        "final_sku_price",
        "original_sku_price",
        "savings",
        "pick_up_availability",
        "shipping_availability",
        "fastest_delivery",
        "delivery_availability",
        "sku_status",
    )
    for source, path in RAW_TARGET_FILES.items():
        rows, _ = read_rows(path)
        seen_rank_keys = set()
        rank = 0
        for row in rows:
            values = {field: row.get(field) for field in occurrence_fields if row.get(field) not in (None, "")}
            if values.get("sku_status"):
                values["sku_status"] = normalized_sku_status(values.get("sku_status"))
            if row.get("bsin"):
                values["item"] = row.get("bsin")

            dedupe_key = row_dedupe_key(row)
            if dedupe_key and dedupe_key not in seen_rank_keys:
                seen_rank_keys.add(dedupe_key)
                rank += 1
                buckets.setdefault(f"{source}|rank:{rank}", []).append(values)

            for key in exact_occurrence_keys(row, source):
                buckets.setdefault(key, []).append(values)

    result = {}
    for key, values in buckets.items():
        if len(values) == 1:
            result[key] = values[0]
    if result:
        print(f"[INFO] Loaded exact listing occurrence map: {len(result)} keys")
    return result


def listing_enrichment_for(row, enrichment_map):
    if not enrichment_map:
        return {}
    product_url = row.get("product_url") or ""
    numeric_sku = row.get("numeric_sku") or extract_numeric_sku_from_text(product_url)
    item = row.get("bsin") or extract_item_from_url(product_url)
    merged = {}
    for key in (numeric_sku, product_url, item):
        if key and str(key) in enrichment_map:
            merge_missing_values(merged, enrichment_map[str(key)])
    return merged


def listing_occurrence_for(row, occurrence_map):
    if not occurrence_map:
        return {}
    source = row.get("_source") or row.get("page_type")
    merged = {}
    for key in exact_occurrence_keys(row, source):
        if key in occurrence_map:
            merge_missing_values(merged, occurrence_map[key])
    return merged


def load_db_numeric_sku_url_map():
    """Load item -> numeric skuId from existing DB URLs that already include /sku/<id>."""
    result = {}
    try:
        conn = connect_readonly({**DB_CONFIG, "database": "postgres"})
        cursor = conn.cursor()
        cursor.execute(
            """
            SELECT item, product_url FROM tv_item_mst
            WHERE item IS NOT NULL AND product_url IS NOT NULL
            UNION
            SELECT substring(product_url from '/product/[^/]+/([^/?]+)') AS item, product_url
            FROM bby_tv_product_list
            WHERE product_url IS NOT NULL
            """
        )
        for item, product_url in cursor.fetchall():
            sku = extract_numeric_sku_from_text(product_url)
            if item and sku:
                result.setdefault(str(item), str(sku))
        cursor.close()
        conn.close()
    except Exception as exc:
        print(f"[WARNING] DB numeric sku URL map unavailable: {exc}")
    if result:
        print(f"[INFO] Loaded DB numeric sku URL map: {len(result)} items")
    return result


def discover_numeric_sku_from_pdp(collector, product_url, item):
    """Last-mile skuId recovery for rows whose listing card did not expose numeric skuId."""
    if os.environ.get("BBY_API_ONLY_SKU_DISCOVERY_ON_MISSING", "1") != "1":
        return None
    if not product_url:
        return None
    try:
        page = collector._ensure_page()
        page.get(product_url)
        candidates = [str(getattr(page, "url", "") or "")]
        try:
            candidates.append(page.run_js("return document.documentElement.outerHTML || '';", timeout=8) or "")
        except Exception:
            pass
        try:
            candidates.append(page.run_js("return document.body ? document.body.innerText : '';", timeout=8) or "")
        except Exception:
            pass
        for text in candidates:
            sku = extract_numeric_sku_from_text(text)
            if sku:
                print(f"[INFO] Recovered numeric skuId via minimal PDP discovery: item={item} skuId={sku}")
                return sku
    except Exception as exc:
        print(f"[WARNING] Minimal PDP sku discovery failed for {item}: {exc}")
    return None


def filter_noncritical_graphql_errors(errors):
    """Ignore optional GraphQL field misses that do not block detail collection."""
    if not isinstance(errors, dict):
        return errors
    filtered = {}
    for operation, operation_errors in errors.items():
        kept = []
        for error in operation_errors or []:
            path = error.get("path") if isinstance(error, dict) else None
            message = str(error.get("message") if isinstance(error, dict) else error)
            path_text = ".".join(str(part) for part in path) if isinstance(path, list) else str(path or "")
            optional_feature_missing = (
                "not found" in message.lower()
                and (
                    "reviewInfo.conFeatures" in path_text
                    or "reviewInfo.proFeatures" in path_text
                )
            )
            if not optional_feature_missing:
                kept.append(error)
        if kept:
            filtered[operation] = kept
    return filtered or None


def filter_listing_csvs():
    seen = set()
    stats = {
        "input": 0,
        "kept": 0,
        "openbox": 0,
        "duplicate": 0,
    }

    for source in ("main", "bsr", "promotion", "trend"):
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

    print(
        "[FILTER] total: "
        f"input={stats['input']} kept={stats['kept']} "
        f"openbox={stats['openbox']} duplicate={stats['duplicate']}"
    )
    return stats


def filtered_listing_rows():
    rows = []
    for source in ("main", "bsr", "promotion", "trend"):
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


def run_single_listing(crawler_cls, label, batch_id):
    print("\n" + "=" * 80)
    print(f"[RUN] {label}")
    print("=" * 80)
    crawler = crawler_cls(test_mode=False, batch_id=batch_id)
    crawler.run()


def configure_dt_env():
    defaults = {
        "BBY_GRAPHQL_REGISTRY_DIR": str(graphql_registry_dir()),
        "BBY_DT_CORE_ONLY": "0",
        "BBY_DT_SKIP_REVIEWS": "0",
        "BBY_DT_GRAPHQL_REPLAY": "1",
        "BBY_DT_PDP_GRAPHQL_DISCOVERY": "0",
        "BBY_DT_PDP_GRAPHQL_DISCOVERY_SECONDS": "0",
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
    text = str(value).strip()
    normalized = re.sub(r"[^0-9.\-]", "", text)
    if not normalized:
        return text
    try:
        return f"${float(normalized):,.2f}"
    except Exception:
        return text if text.startswith("$") else text


def money_optional_cents(value):
    if value in (None, ""):
        return None
    text = str(value).strip()
    normalized = re.sub(r"[^0-9.\-]", "", text)
    if not normalized:
        return text
    try:
        number = float(normalized)
    except Exception:
        return text
    if number.is_integer():
        return f"${int(number):,}"
    return f"${number:,.2f}"


def numeric_money_value(value):
    if value in (None, ""):
        return None
    text = str(value).strip()
    normalized = re.sub(r"[^0-9.\-]", "", text)
    if not normalized:
        return None
    try:
        return float(normalized)
    except Exception:
        return None


def normalize_detail_prices(row, price_data):
    final_raw = row.get("final_sku_price") or price_data.get("final_sku_price")
    original_raw = row.get("original_sku_price") or price_data.get("original_sku_price")
    savings_raw = row.get("savings") or price_data.get("savings")

    final_value = numeric_money_value(final_raw)
    original_value = numeric_money_value(original_raw)
    savings_value = numeric_money_value(savings_raw)

    if final_value is None:
        return {
            "final_sku_price": money(final_raw),
            "original_sku_price": None,
            "savings": None,
        }

    result = {
        "final_sku_price": money(final_value),
        "original_sku_price": None,
        "savings": None,
    }

    if original_value is None or original_value <= final_value:
        return result

    result["original_sku_price"] = money(original_value)

    if savings_value is None or savings_value <= 0:
        return result

    expected_savings = original_value - final_value
    if abs(savings_value - expected_savings) <= 1.0:
        result["savings"] = money_optional_cents(savings_value)
    return result


def format_count(value):
    if value in (None, ""):
        return None
    try:
        return f"{int(str(value).replace(',', '').strip()):,}"
    except Exception:
        return str(value).strip()


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
        restricted_message = price_payload.get("restrictedPriceDisplayMessage") or price_payload.get("priceWithCart")
        restricted_marker = contains_text(
            restricted_message,
            (
                ("see price in cart", "See price in cart"),
                ("see details in checkout", "See details in checkout"),
            ),
        )
        if restricted_marker:
            return {"final_sku_price": restricted_marker, "original_sku_price": None, "savings": None}

        final_price = (
            price_payload.get("displayableCustomerPrice")
            or price_payload.get("currentPrice")
            or price_payload.get("customerPrice")
            or price_payload.get("salePrice")
        )
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
    spec_search_text = text

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
        match = re.search(r"\b(20[2-4]\d)\b", spec_search_text)
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
        lower = text.lower()
        blocked_labels = {
            "picture quality",
            "key specs",
            "dimensions",
            "features",
            "specifications",
            "pros",
            "cons",
        }
        if lower in blocked_labels:
            return
        key = text.lower()
        if key in seen:
            return
        seen.add(key)
        names.append(text)

    def walk_recommendation_node(value):
        if len(names) >= limit:
            return
        if isinstance(value, dict):
            name_obj = value.get("name")
            if isinstance(name_obj, dict):
                add_name(name_obj.get("short") or name_obj.get("title"))
            elif isinstance(name_obj, str):
                add_name(name_obj)
            add_name(
                value.get("productName")
                or value.get("title")
                or value.get("shortName")
                or value.get("displayName")
            )
            product = value.get("product")
            if isinstance(product, dict):
                walk_recommendation_node(product)
        elif isinstance(value, list):
            for child in value:
                walk_recommendation_node(child)

    def walk_recommendation_containers(value):
        if len(names) >= limit:
            return
        if isinstance(value, dict):
            for key in ("recommendations", "items", "products", "results"):
                child = value.get(key)
                if isinstance(child, list):
                    walk_recommendation_node(child)
            for key in ("subPlacements", "placements", "recommendationsV2", "recommendations"):
                child = value.get(key)
                if isinstance(child, (dict, list)):
                    walk_recommendation_containers(child)
        elif isinstance(value, list):
            for child in value:
                walk_recommendation_containers(child)

    for operation in ("GetCompareProduct", "ProductCarousel_Recommendations", "URE_FetchRecommendations"):
        walk_recommendation_containers(bundle.get(operation) or {})
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
    listing_sku_map = load_listing_numeric_sku_map()
    saved_sku_map.update({key: value for key, value in listing_sku_map.items() if key not in saved_sku_map})
    listing_enrichment_map = load_listing_enrichment_map()
    listing_occurrence_map = load_listing_occurrence_map()
    db_numeric_sku_map = load_db_numeric_sku_url_map()
    if not registry:
        print(f"[WARNING] GraphQL registry not found: {registry_dir}")
    operation_names = detail_operation_names(registry)
    if operation_names:
        print(f"[INFO] Detail GraphQL operations: {', '.join(operation_names)}")
    missing_hints = missing_detail_operation_hints(registry)
    if missing_hints:
        print("[INFO] Missing optional detail GraphQL operations:")
        for hint in missing_hints:
            print(f"       - {hint}")

    crawler = BestBuyDetailCrawler()
    collector = None
    try:
        allow_page_access = os.environ.get("BBY_API_ONLY_ALLOW_PAGE_ACCESS", "0").strip().lower() in {"1", "true", "yes"}
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
        if allow_page_access:
            try:
                page = collector._ensure_page()
                page.get("https://www.bestbuy.com/")
            except Exception as exc:
                print(f"[WARNING] Browser origin warmup failed; GraphQL may fail: {exc}")

        for order, row in enumerate(rows, 1):
            row = dict(row)
            occurrence = listing_occurrence_for(row, listing_occurrence_map)
            merge_missing_values(row, occurrence)
            enrichment = listing_enrichment_for(row, listing_enrichment_map)
            merge_missing_values(row, enrichment)
            product_url = row.get("product_url")
            item = row.get("bsin") or enrichment.get("item") or extract_item_from_url(product_url)
            numeric_sku = resolve_numeric_sku(row, saved_sku_map)
            if not numeric_sku and item:
                numeric_sku = db_numeric_sku_map.get(item)
            parsed = {}
            errors = None
            ignored_errors = None
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
                raw_errors = bundle.get("errors")
                errors = filter_noncritical_graphql_errors(raw_errors)
                ignored_errors = raw_errors if raw_errors and not errors else None
                crawler.record_graphql_sku_map(product_url, numeric_sku)
            elif not numeric_sku:
                errors = {"skuId": "numeric_sku missing from listing; PDP discovery disabled in API-only mode"}
                price_data = {}
                spec_data = {}
                similar_products = []
            else:
                price_data = {}
                spec_data = {}
                similar_products = []

            if errors:
                print(f"[API-ONLY] {order}/{len(rows)} {item}: GraphQL partial/failed: {summarize_graphql_errors(errors)}")
            elif ignored_errors:
                print(f"[API-ONLY] {order}/{len(rows)} {item}: GraphQL ok (ignored optional feature errors: {summarize_graphql_errors(ignored_errors)})")
            else:
                print(f"[API-ONLY] {order}/{len(rows)} {item}: GraphQL ok")

            price_values = normalize_detail_prices(row, price_data)

            crawler.save_to_db(
                page_type=row.get("page_type") or row.get("_source") or "listing_api",
                order=order,
                retailer_sku_name=row.get("retailer_sku_name") or row.get("product_name") or enrichment.get("retailer_sku_name") or enrichment.get("product_name"),
                item=item,
                electricity_use=row.get("estimated_annual_electricity_use") or spec_data.get("estimated_annual_electricity_use"),
                screen_size=row.get("screen_size") or spec_data.get("screen_size"),
                count_of_reviews=format_count(parsed.get("count_of_reviews")),
                count_of_star_ratings=format_count(parsed.get("count_of_reviews")),
                top_mentions=None,
                detailed_reviews=parsed.get("detailed_review_content"),
                summarized_review_content=None,
                recommendation_intent=format_recommendation(parsed.get("recommendation_intent")),
                product_url=product_url,
                final_sku_price=price_values.get("final_sku_price"),
                savings=price_values.get("savings"),
                original_sku_price=price_values.get("original_sku_price"),
                offer=row.get("offer"),
                pick_up_availability=row.get("pick_up_availability"),
                shipping_availability=row.get("fastest_delivery") or row.get("shipping_availability"),
                delivery_availability=row.get("delivery_availability"),
                sku_status=normalized_sku_status(row.get("sku_status")),
                star_rating_source=parsed.get("star_rating"),
                promotion_type=row.get("promotion_type"),
                promotion_position=row.get("promotion_position") or row.get("promotion_rank"),
                bsr_rank=row.get("bsr_rank"),
                main_rank=row.get("main_rank"),
                trend_rank=row.get("trend_rank"),
                model_year=row.get("model_year") or spec_data.get("model_year"),
                sku=numeric_sku or row.get("sku") or spec_data.get("model_number") or "no sku",
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
    run_single_listing(BestBuyTVPromotionCrawler, "promotion listing", batch_id)
    run_single_listing(BestBuyTVTrendCrawler, "trend listing", batch_id)
    stats = filter_listing_csvs()
    if stats["kept"] <= 0:
        print("[ERROR] No listing rows left after filtering. Detail crawl skipped.")
        return 1

    run_api_only_detail(filtered_listing_rows())
    return 0


if __name__ == "__main__":
    raise SystemExit(main())


