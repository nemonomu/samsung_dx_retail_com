import csv
import html
import json
import os
import re
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from functools import lru_cache
from pathlib import Path
from urllib.parse import urlencode

from bs4 import BeautifulSoup
from lxml import html as lxml_html
from requests import RequestException
from zenrows import ZenRowsClient

from .step00_config import (
    DEFAULT_BESTBUY_RUN_ROOT,
    KRW_PER_USD,
    bestbuy_batch_id,
    bestbuy_category,
    bestbuy_output_table,
    db_config,
    eastern_now,
    old_pdp_url,
    rel_path,
)
from .step00_detail_benchmarks import write_detail_benchmarks
from .step00_parse_pdp import event_data, extract_apollo_payloads

RUN_DATE = os.getenv("BESTBUY_RUN_DATE", datetime.now().strftime("%Y%m%d"))
CATEGORY = bestbuy_category()
BATCH_ID = bestbuy_batch_id(CATEGORY)
DETAIL_ROOT = Path(os.getenv("BESTBUY_DETAIL_RUN_ROOT", DEFAULT_BESTBUY_RUN_ROOT / "detail"))
OUTPUT_ROOT = Path(os.getenv("BESTBUY_OUTPUT_ROOT", DEFAULT_BESTBUY_RUN_ROOT / "output"))
TARGET_CSV = Path(os.getenv("BESTBUY_DETAIL_TARGET_CSV", OUTPUT_ROOT / "bestbuy_final_targets.csv"))
SAMPLE_SCHEMA_CSV = Path(os.getenv("BESTBUY_OUTPUT_SCHEMA_CSV", "references/tv_retail_com_202605170513.csv"))
SELECTOR_TABLE = os.getenv("BESTBUY_SELECTOR_TABLE", "dx_xpath_selectors")
LIMIT = int(os.getenv("BESTBUY_DETAIL_LIMIT", "0"))
MAX_ATTEMPTS = int(os.getenv("BESTBUY_DETAIL_MAX_ATTEMPTS", "3"))
RETRY_ONLY = os.getenv("BESTBUY_DETAIL_RETRY_ONLY", "0").lower() in {"1", "true", "yes", "y"}
REBUILD_ONLY = os.getenv("BESTBUY_DETAIL_REBUILD_ONLY", "0").lower() in {"1", "true", "yes", "y"}
FORCE_REFRESH = os.getenv("BESTBUY_DETAIL_FORCE_REFRESH", "0").lower() in {"1", "true", "yes", "y"}
TARGET_SKUS = {
    value.strip().lower()
    for value in re.split(r"[\s,;]+", os.getenv("BESTBUY_DETAIL_SKUS", ""))
    if value.strip()
}
REQUEST_TIMEOUT = int(os.getenv("ZENROWS_TIMEOUT", "240"))
WORKERS = int(os.getenv("BESTBUY_DETAIL_WORKERS", "1"))
STAGE = os.getenv("BESTBUY_DETAIL_STAGE", "detail").lower()
SAVE_HTML_MODE = os.getenv("BESTBUY_SAVE_HTML_MODE", "slim").lower()

RAW_DETAIL_DIR = DETAIL_ROOT / "raw" / "detail_html"
RAW_REVIEW_DIR = DETAIL_ROOT / "raw" / "review20"
PARSED_DIR = DETAIL_ROOT / "parsed"
BENCHMARKS_DIR = DETAIL_ROOT / "benchmarks"
DETAIL_ROWS_CSV = PARSED_DIR / "detail_enriched_rows.csv"
FAILURES_CSV = PARSED_DIR / "detail_failures.csv"
DETAIL_BENCHMARKS_CSV = BENCHMARKS_DIR / "detail_benchmarks.csv"
FINAL_OUTPUT_CSV = Path(os.getenv("BESTBUY_FINAL_OUTPUT_CSV", OUTPUT_ROOT / "final_output.csv"))
PRODUCT_LIST_CSV = Path(os.getenv("BESTBUY_PRODUCT_LIST_OUTPUT", OUTPUT_ROOT / "bestbuy_product_list.csv"))
MANIFEST_PATH = DETAIL_ROOT / "manifest_detail_enrichment.json"

TV_FINAL_FIELDS = [
    "id",
    "item",
    "account_name",
    "page_type",
    "count_of_reviews",
    "retailer_sku_name",
    "product_url",
    "star_rating",
    "count_of_star_ratings",
    "screen_size",
    "sku_popularity",
    "final_sku_price",
    "original_sku_price",
    "savings",
    "discount_type",
    "offer",
    "pick_up_availability",
    "fastest_delivery",
    "delivery_availability",
    "shipping_info",
    "available_quantity_for_purchase",
    "inventory_status",
    "sku_status",
    "retailer_membership_discounts",
    "detailed_review_content",
    "summarized_review_content",
    "top_mentions",
    "recommendation_intent",
    "main_rank",
    "bsr_rank",
    "rank_1",
    "rank_2",
    "promotion_position",
    "trend_rank",
    "number_of_ppl_purchased_yesterday",
    "number_of_ppl_added_to_carts",
    "retailer_sku_name_similar",
    "estimated_annual_electricity_use",
    "promotion_type",
    "calendar_week",
    "crawl_datetime",
    "number_of_units_purchased_past_month",
    "model_year",
    "batch_id",
    "country",
]

HHP_FINAL_FIELDS = [
    "id",
    "country",
    "product",
    "item",
    "account_name",
    "page_type",
    "count_of_reviews",
    "retailer_sku_name",
    "product_url",
    "star_rating",
    "count_of_star_ratings",
    "sku_popularity",
    "final_sku_price",
    "original_sku_price",
    "savings",
    "discount_type",
    "offer",
    "bundle",
    "pick_up_availability",
    "fastest_delivery",
    "delivery_availability",
    "shipping_info",
    "available_quantity_for_purchase",
    "inventory_status",
    "sku_status",
    "retailer_membership_discounts",
    "trade_in",
    "hhp_storage",
    "hhp_color",
    "hhp_carrier",
    "detailed_review_content",
    "summarized_review_content",
    "top_mentions",
    "recommendation_intent",
    "main_rank",
    "bsr_rank",
    "rank_1",
    "rank_2",
    "trend_rank",
    "number_of_ppl_purchased_yesterday",
    "number_of_ppl_added_to_carts",
    "number_of_units_purchased_past_month",
    "retailer_sku_name_similar",
    "promotion_type",
    "calendar_week",
    "crawl_strdatetime",
    "batch_id",
]

REF_FINAL_FIELDS = [
    "id",
    "country",
    "product",
    "item",
    "sku",
    "account_name",
    "page_type",
    "count_of_reviews",
    "retailer_sku_name",
    "product_url",
    "star_rating",
    "count_of_star_ratings",
    "sku_popularity",
    "final_sku_price",
    "original_sku_price",
    "savings",
    "discount_type",
    "offer",
    "pick_up_availability",
    "fastest_delivery",
    "delivery_availability",
    "shipping_info",
    "available_quantity_for_purchase",
    "inventory_status",
    "sku_status",
    "retailer_membership_discounts",
    "ref_capacity",
    "ref_refrigerator_type",
    "detailed_review_content",
    "summarized_review_content",
    "top_mentions",
    "recommendation_intent",
    "main_rank",
    "bsr_rank",
    "rank_1",
    "rank_2",
    "trend_rank",
    "number_of_ppl_purchased_yesterday",
    "number_of_ppl_added_to_carts",
    "number_of_units_purchased_past_month",
    "retailer_sku_name_similar",
    "promotion_type",
    "calendar_week",
    "crawl_datetime",
    "batch_id",
]

LDY_FINAL_FIELDS = [
    "id",
    "country",
    "product",
    "item",
    "sku",
    "account_name",
    "page_type",
    "count_of_reviews",
    "retailer_sku_name",
    "product_url",
    "star_rating",
    "count_of_star_ratings",
    "sku_popularity",
    "final_sku_price",
    "original_sku_price",
    "savings",
    "discount_type",
    "offer",
    "pick_up_availability",
    "fastest_delivery",
    "delivery_availability",
    "shipping_info",
    "available_quantity_for_purchase",
    "inventory_status",
    "sku_status",
    "retailer_membership_discounts",
    "ldy_capacity",
    "ldy_loading_type",
    "detailed_review_content",
    "summarized_review_content",
    "top_mentions",
    "recommendation_intent",
    "main_rank",
    "bsr_rank",
    "rank_1",
    "rank_2",
    "trend_rank",
    "number_of_ppl_purchased_yesterday",
    "number_of_ppl_added_to_carts",
    "number_of_units_purchased_past_month",
    "retailer_sku_name_similar",
    "promotion_type",
    "calendar_week",
    "crawl_datetime",
    "batch_id",
]

FALLBACK_FINAL_FIELDS = {
    "TV": TV_FINAL_FIELDS,
    "HHP": HHP_FINAL_FIELDS,
    "REF": REF_FINAL_FIELDS,
    "LDY": LDY_FINAL_FIELDS,
}


def now():
    return datetime.now().isoformat(timespec="seconds")


def compact_text(value):
    if value is None:
        value = ""
    return re.sub(r"\s+", " ", html.unescape(str(value))).strip()


def first_non_empty(*values):
    for value in values:
        if value not in ("", None, [], {}):
            return value
    return ""


def clean_hhp_carrier(value):
    text = compact_text(value)
    if not text:
        return ""
    lowered = text.lower()
    carriers = [
        ("Total by Verizon", ["total by verizon"]),
        ("Unlocked", ["unlocked", "fully unlocked"]),
        ("AT&T", ["at&t", "att"]),
        ("Verizon", ["verizon"]),
        ("T-Mobile", ["t-mobile", "tmobile"]),
        ("Sprint", ["sprint"]),
        ("Boost Mobile", ["boost mobile"]),
        ("Cricket", ["cricket"]),
        ("Tracfone", ["tracfone"]),
        ("Google Fi", ["google fi"]),
        ("Metro by T-Mobile", ["metro by t-mobile", "metropcs", "metro pcs", "metro"]),
        ("Consumer Cellular", ["consumer cellular"]),
        ("Mint Mobile", ["mint mobile", "mint"]),
        ("Ultra Mobile", ["ultra mobile"]),
        ("H2O Wireless", ["h2o wireless", "h2o"]),
        ("Ting Mobile", ["ting mobile", "ting"]),
        ("US Cellular", ["us cellular", "u.s. cellular"]),
        ("Simple Mobile", ["simple mobile"]),
        ("Straight Talk", ["straight talk"]),
        ("Total Wireless", ["total wireless"]),
        ("Visible", ["visible"]),
        ("Lively", ["lively sim", "lively mobile"]),
    ]
    found = []
    parts = [part.strip() for part in re.split(r"[,;/|]+", text) if part.strip()]
    scan_values = parts if len(parts) > 1 else [text]
    for scan_value in scan_values:
        scan_lowered = scan_value.lower()
        matches = []
        for canonical, needles in carriers:
            positions = [scan_lowered.find(needle) for needle in needles if needle in scan_lowered]
            if positions:
                matches.append((min(positions), canonical))
        for _, canonical in sorted(matches, key=lambda item: item[0]):
            if canonical == "Unlocked" and len(scan_values) > 1:
                continue
            if canonical == "Verizon" and "Total by Verizon" in found and "total by verizon" in scan_lowered:
                continue
            if canonical not in found:
                found.append(canonical)
    if found:
        if "Unlocked" in found and len(found) > 1:
            found = [carrier for carrier in found if carrier != "Unlocked"]
        return ", ".join(found)
    return ""


def clean_hhp_color(value):
    text = compact_text(value)
    if not text:
        return ""
    text = re.sub(
        r"(?i)\s*\((?:unlocked|verizon|at&t|att|t-mobile|tmobile|total wireless|tracfone|lively)\)\s*$",
        "",
        text,
    ).strip(" ,-")
    text = re.sub(
        r"(?i)\b(?:carrier\s+)?(?:unlocked|verizon|at&t|att|t-mobile|tmobile)\b\s*$",
        "",
        text,
    ).strip(" ,-")
    return text


def clean_hhp_storage(value):
    text = compact_text(value)
    if not text:
        return ""
    match = re.search(r"(?i)\b(\d+(?:\.\d+)?)\s*(TB|GB|terabytes?|gigabytes?)\b", text)
    if not match:
        return text
    number = match.group(1)
    unit = match.group(2).lower()
    if unit.startswith("tb") or unit.startswith("tera"):
        return f"{number} terabytes"
    return f"{number} gigabytes"


def hhp_attributes_from_name(name, include_carrier=False):
    text = compact_text(name)
    attrs = {"hhp_storage": "", "hhp_color": "", "hhp_carrier": ""}
    if not text:
        return attrs

    storage_match = re.search(r"(?i)\b(\d+(?:\.\d+)?)\s*(TB|GB)\b", text)
    if storage_match:
        number = storage_match.group(1)
        unit = storage_match.group(2).upper()
        attrs["hhp_storage"] = clean_hhp_storage(f"{number}{unit}")

    if include_carrier:
        paren_values = re.findall(r"\(([^()]*)\)", text)
        for value in reversed(paren_values):
            carrier = clean_hhp_carrier(value)
            if carrier:
                attrs["hhp_carrier"] = carrier
                break

    # Best Buy HHP titles usually end with "- Color" after carrier/storage.
    parts = [part.strip() for part in re.split(r"\s+-\s+", text) if part.strip()]
    if len(parts) >= 2:
        color = clean_hhp_color(parts[-1])
        if not re.search(r"(?i)\b(class|series|gb|tb|unlocked|verizon|at&t|t-mobile)\b", color):
            attrs["hhp_color"] = color
        elif color and len(color.split()) <= 4:
            attrs["hhp_color"] = color
    return attrs


def hhp_attributes_from_product(product, product_name):
    attrs = hhp_attributes_from_name(product_name, include_carrier=False)
    color = first_path([product], ["color", "displayName"])
    if color:
        attrs["hhp_color"] = clean_hhp_color(color)
    spec_candidates = {
        "hhp_storage": ["Internal Storage", "Storage Capacity", "Built-In Storage", "Total Storage Capacity"],
        "hhp_color": ["Color", "Color Category"],
        "hhp_carrier": ["Carrier Compatibility", "Carrier", "Wireless Carrier"],
    }
    for field, names in spec_candidates.items():
        for name in names:
            value = spec_value([product], name)
            if value:
                if field == "hhp_carrier":
                    attrs[field] = clean_hhp_carrier(value)
                elif field == "hhp_storage":
                    attrs[field] = clean_hhp_storage(value)
                else:
                    attrs[field] = clean_hhp_color(value)
                break
    return attrs


def money(value):
    if value in ("", None):
        return ""
    try:
        return f"${float(value):,.2f}"
    except (TypeError, ValueError):
        return str(value)


def price_policy_message(price):
    if not isinstance(price, dict):
        return ""
    return first_non_empty(
        price.get("restrictedPriceDisplayMessage"),
        price.get("priceDisplayMessage"),
    )


def money_int(value):
    if value in ("", None):
        return ""
    try:
        return f"${int(round(float(value))):,}"
    except (TypeError, ValueError):
        return str(value)


def normalize_savings(value):
    text = compact_text(value)
    if not text:
        return ""
    match = re.search(r"\$?\s*([0-9][0-9,]*(?:\.\d+)?)", text)
    if not match:
        return text
    try:
        amount = float(match.group(1).replace(",", ""))
    except ValueError:
        return text
    if amount <= 0:
        return ""
    return f"${int(round(amount)):,}"


def int_commas(value):
    if value in ("", None):
        return ""
    try:
        return f"{int(value):,}"
    except (TypeError, ValueError):
        return str(value)


def date_to_phrase(prefix, date_value):
    if not date_value:
        return ""
    try:
        dt = datetime.fromisoformat(str(date_value)[:10])
    except ValueError:
        return ""
    return f"{prefix} {dt.strftime('%a, %b')} {dt.day}"


def html_match(pattern, html_text, flags=re.I | re.S):
    match = re.search(pattern, html_text, flags)
    return compact_text(match.group(1)) if match else ""


def recommendation_from_html(html_text):
    match = re.search(
        r"<span[^>]*>\s*(\d+%)\s*</span>\s*&nbsp;\s*would recommend to a friend",
        html_text,
        re.I | re.S,
    )
    if match:
        return f"{match.group(1)} would recommend to a friend"
    return html_match(r"(\d+%\s*would recommend to a friend)", html_text)


def fastest_delivery_from_html(html_text):
    for pattern in (
        r'aria-label="(Get it[^"]*)"',
        r'aria-label="(Shipping[^"]+)"',
        r"(Get it[^<]{0,80}(?:FREE|Free|free))",
        r">(Get it[^<]*)<",
    ):
        value = html_match(pattern, html_text)
        if value:
            return compact_text(value)
    return ""


def normalize_fastest_delivery(value):
    text = compact_text(value)
    if not text:
        return ""
    text = re.sub(r"\s*(?:&bull;|•)\s*", " • ", text)
    text = re.sub(r"^(Shipping|Delivery|Pickup)\s+", "", text, flags=re.I).strip()
    if re.match(r"(?i)^get it tomorrow\b", text):
        return re.sub(r"(?i)^get it tomorrow\b", "Get it tomorrow", text, count=1)
    if re.match(r"(?i)^get it\b", text):
        return re.sub(r"(?i)^get it\b", "Get it", text, count=1)
    if re.match(r"(?i)^unavailable\b", text):
        return ""
    return text


def delivery_from_html(html_text):
    for pattern in (
        r'aria-label="(Delivery\s+As soon as[^"]+)"',
        r"(Delivery\s+as soon as[^<]{0,120}(?:FREE|Free|free))",
        r"(Delivery\s+As soon as[^<]{0,120}(?:FREE|Free|free))",
    ):
        value = html_match(pattern, html_text)
        if value:
            return normalize_delivery_availability(value)
    return ""


def shipping_date_from_products(products):
    shipping = best_path(
        products,
        ["fulfillmentOptions", "shippingDetails", 0, "shippingAvailability", 0],
        ("customerLOSGroup",),
    )
    group = shipping.get("customerLOSGroup") if isinstance(shipping, dict) else {}
    if isinstance(group, list):
        free_groups = [item for item in group if isinstance(item, dict) and item.get("price") in (0, 0.0, "0")]
        dated_groups = free_groups or [item for item in group if isinstance(item, dict)]
        if not dated_groups:
            return ""
        group = sorted(
            dated_groups,
            key=lambda item: item.get("maxLineItemMaxDate")
            or item.get("minLineItemMaxDate")
            or item.get("maxDate")
            or item.get("minDate")
            or "",
        )[0]
    if not isinstance(group, dict):
        return ""
    return (
        group.get("maxLineItemMaxDate")
        or group.get("minLineItemMaxDate")
        or group.get("maxDate")
        or group.get("minDate")
    )


def normalize_delivery_availability(value):
    text = compact_text(value)
    if not text:
        return ""
    text = re.sub(r"\s*(?:&bull;|•)\s*", " • ", text)
    return re.sub(r"(?i)^Delivery\s+As soon as", "Delivery as soon as", text, count=1)


def visible_shipping_value(*values, normalize_func=compact_text):
    normalized_values = [normalize_func(value) for value in values]
    for value in normalized_values:
        if re.search(r"(?i)(?:^|\s)(FREE|free)(?:\s|$)|•", value):
            return value
    return first_non_empty(*normalized_values)


def normalize_fastest_delivery_output(value):
    text = normalize_fastest_delivery(value)
    if re.match(r"(?i)^get\b", text) and not re.search(r"(?i)(?:^|\s)FREE(?:\s|$)|•", text):
        return f"{text} • FREE"
    return text


def price_policy_from_html(html_text):
    for pattern in (
        r"(See price in cart)",
        r"(No longer available)",
        r"(Sold Out)",
        r"(Coming Soon)",
        r"(Unavailable)",
    ):
        value = html_match(pattern, html_text)
        if value:
            return value
    return ""


def price_policy_value(price, selector_values, target, html_text):
    candidates = [
        selector_values.get("final_sku_price_see_price_in_cart"),
        selector_values.get("final_sku_price_no_longer_available"),
        selector_values.get("price_policy_message"),
        price_policy_message(price),
        target.get("restricted_price_message"),
        price_policy_from_html(html_text),
    ]
    for value in candidates:
        text = compact_text(value)
        if not text:
            continue
        lowered = text.lower()
        if any(needle in lowered for needle in ("see price in cart", "no longer available", "sold out", "coming soon", "unavailable")):
            return text
    return ""


def is_policy_price(value):
    lowered = compact_text(value).lower()
    return any(needle in lowered for needle in ("see price in cart", "no longer available", "sold out", "coming soon", "unavailable"))


def trade_in_from_html(html_text):
    if not html_text:
        return ""
    text = compact_text(BeautifulSoup(html_text, "html.parser").get_text(" "))
    match = re.search(
        r"(Check your trade-in value(?:\.\s*Save(?: up to)?[^.]{0,100}\.)?)",
        text,
        re.I,
    )
    return compact_text(match.group(1)) if match else ""


def trade_in_from_products(products):
    for product in reversed(products):
        if isinstance(product, dict) and product.get("isPurchaseWithTradeInEligible") is True:
            return "Check your trade-in value."
    return ""


def clean_energy(value):
    match = re.search(r"\d+(?:\.\d+)?", str(value or ""))
    return match.group(0) if match else ""


def quote_ident(value):
    return '"' + str(value).replace('"', '""') + '"'


@lru_cache(maxsize=32)
def detail_selectors(category):
    config = db_config()
    if not config:
        return {}
    try:
        import psycopg2

        conn = psycopg2.connect(
            host=config.get("host"),
            port=int(config.get("port") or 5432),
            user=config.get("user"),
            password=config.get("password"),
            dbname=config.get("database"),
            connect_timeout=10,
        )
        with conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    SELECT data_field, xpath
                    FROM public.{quote_ident(SELECTOR_TABLE)}
                    WHERE product_line = %s
                      AND account_name ILIKE %s
                      AND page_type = %s
                      AND is_active IS TRUE
                    ORDER BY id
                    """,
                    (str(category or "").upper(), "Bestbuy", "detail"),
                )
                rows = cur.fetchall()
        conn.close()
    except Exception:
        return {}

    selectors = {}
    for field, xpath in rows:
        field = str(field or "").strip()
        xpath = str(xpath or "").strip()
        if field and xpath:
            selectors.setdefault(field, []).append(xpath)
    return selectors


def xpath_text(node):
    if isinstance(node, str):
        return compact_text(node)
    if isinstance(node, bytes):
        return compact_text(node.decode("utf-8", errors="ignore"))
    if hasattr(node, "text_content"):
        return compact_text(node.text_content())
    return compact_text(node)


def eval_selector(document, xpath_expr):
    values = []
    for part in str(xpath_expr or "").split("|||"):
        expr = part.strip()
        if not expr:
            continue
        try:
            matches = document.xpath(expr)
        except Exception:
            continue
        if not isinstance(matches, list):
            matches = [matches]
        for match in matches:
            text = xpath_text(match)
            if text:
                values.append(text)
    return " ".join(dict.fromkeys(values))


def detail_selector_values(html_text):
    if not html_text:
        return {}
    selectors = detail_selectors(bestbuy_category())
    if not selectors:
        return {}
    try:
        document = lxml_html.fromstring(html_text)
    except Exception:
        return {}
    values = {}
    for field, xpaths in selectors.items():
        for xpath_expr in xpaths:
            value = eval_selector(document, xpath_expr)
            if value:
                values[field] = value
                break
    return values


def recommendation_phrase(value):
    value = compact_text(value)
    if not value:
        return ""
    if "would recommend" in value:
        return value
    match = re.search(r"\d+%", value)
    return f"{match.group(0)} would recommend to a friend" if match else value


def request_cost(headers):
    raw = headers.get("X-Request-Cost") or headers.get("x-request-cost") or "0"
    try:
        return float(raw)
    except (TypeError, ValueError):
        return 0.0


def detail_params():
    return {
        "js_render": "true",
        "premium_proxy": "true",
        "proxy_country": "us",
        "js_instructions": json.dumps(
            [
                {"wait": 2000},
                {"scroll_y": 1800},
                {"wait": 800},
                {"scroll_y": 1800},
                {"wait": 800},
                {"scroll_y": 1800},
                {"wait": 800},
                {"wait": 1500},
            ]
        ),
    }


def graphql_params():
    return {
        "custom_headers": "true",
        "premium_proxy": "true",
        "proxy_country": "us",
        "js_render": "true",
    }


def load_csv(path):
    if not path.exists():
        return []
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        return list(csv.DictReader(f))


def write_csv(path, rows, preferred=None):
    path.parent.mkdir(parents=True, exist_ok=True)
    keys = set()
    for row in rows:
        keys.update(row)
    fieldnames = [key for key in (preferred or []) if key in keys]
    fieldnames.extend(sorted(keys - set(fieldnames)))
    with path.open("w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def sync_product_list(final_rows):
    if not PRODUCT_LIST_CSV.exists():
        return 0
    with PRODUCT_LIST_CSV.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        rows = list(reader)
        fields = reader.fieldnames or []
    if not rows or not fields:
        return 0

    def canonical_product_url(row):
        url = compact_text(row.get("product_url"))
        if "/sku/" in url:
            url = url.split("/sku/", 1)[0]
        return url.rstrip("/")

    def product_list_keys(row):
        keys = []
        item = compact_text(row.get("item") or row.get("bsin"))
        if item:
            keys.append(item)
        url = canonical_product_url(row)
        if url:
            keys.append(url)
        return keys

    by_url = {}
    for row in final_rows:
        for key in product_list_keys(row):
            by_url[key] = row
    sync_fields = [
        "offer",
        "pick_up_availability",
        "fastest_delivery",
        "delivery_availability",
        "sku_status",
        "promotion_type",
        "trend_rank",
        "main_rank",
        "bsr_rank",
        "promotion_position",
        "calendar_week",
        "crawl_datetime",
        "crawl_strdatetime",
        "batch_id",
    ]
    changed = 0
    for row in rows:
        source = None
        for key in product_list_keys(row):
            source = by_url.get(key)
            if source:
                break
        if not source:
            continue
        for field in sync_fields:
            if field in fields and field in source:
                new_value = source.get(field, "")
                if row.get(field, "") != new_value:
                    row[field] = new_value
                    changed += 1
    write_csv(PRODUCT_LIST_CSV, rows, fields)
    return changed


def read_json(path):
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8-sig"))
    except ValueError:
        return {}


def safe_part(value, default="na"):
    value = re.sub(r"[^0-9A-Za-z_-]+", "_", str(value or "").strip()).strip("_")
    return value or default


def detail_rank(target):
    if isinstance(target, dict):
        return safe_part(target.get("main_rank") or target.get("final_target_rank") or target.get("bsr_rank") or target.get("rank") or "na")
    return "na"


def existing_detail_dirs(sku):
    pattern = f"*_{safe_part(sku)}_*"
    dirs = []
    for path in RAW_DETAIL_DIR.glob(pattern):
        if path.is_dir() and (path / f"{sku}_meta.json").exists():
            dirs.append(path)
    return sorted(
        dirs,
        key=lambda path: (
            0 if path.name.endswith("_success") else 1 if path.name.endswith("_fail") else 2,
            path.name,
        ),
    )


def legacy_detail_paths(sku):
    return {
        "html": RAW_DETAIL_DIR / f"{sku}.html",
        "headers": RAW_DETAIL_DIR / f"{sku}_headers.json",
        "apollo": RAW_DETAIL_DIR / f"{sku}_apollo.json",
        "meta": RAW_DETAIL_DIR / f"{sku}_meta.json",
    }


def detail_folder(sku, target=None, status=None):
    sku_part = safe_part(sku)
    desired = None
    if status:
        desired = RAW_DETAIL_DIR / f"{detail_rank(target)}_{sku_part}_{safe_part(status)}"

    existing = existing_detail_dirs(sku)
    if desired:
        if existing and desired not in existing:
            if not desired.exists():
                existing[0].rename(desired)
            else:
                for old_dir in existing:
                    if old_dir == desired:
                        continue
                    for old_file in old_dir.iterdir():
                        new_file = desired / old_file.name
                        if not new_file.exists():
                            old_file.rename(new_file)
        desired.mkdir(parents=True, exist_ok=True)
        return desired
    if existing:
        return existing[0]
    return None


def existing_review_dirs(sku):
    pattern = f"*_{safe_part(sku)}_*"
    dirs = []
    for path in RAW_REVIEW_DIR.glob(pattern):
        if path.is_dir() and (path / f"{sku}_meta.json").exists():
            dirs.append(path)
    return sorted(
        dirs,
        key=lambda path: (
            0 if path.name.endswith("_success") else 1 if path.name.endswith("_fail") else 2,
            path.name,
        ),
    )


def review_folder(sku, target=None, status=None):
    sku_part = safe_part(sku)
    desired = None
    if status:
        desired = RAW_REVIEW_DIR / f"{detail_rank(target)}_{sku_part}_{safe_part(status)}"

    existing = existing_review_dirs(sku)
    if desired:
        if existing and desired not in existing:
            if not desired.exists():
                existing[0].rename(desired)
            else:
                for old_dir in existing:
                    if old_dir == desired:
                        continue
                    for old_file in old_dir.iterdir():
                        new_file = desired / old_file.name
                        if not new_file.exists():
                            old_file.rename(new_file)
        desired.mkdir(parents=True, exist_ok=True)
        return desired
    if existing:
        return existing[0]
    return None


def detail_paths(sku):
    folder = detail_folder(sku)
    if folder:
        return {
            "html": folder / f"{sku}.html",
            "headers": folder / f"{sku}_headers.json",
            "apollo": folder / f"{sku}_apollo.json",
            "meta": folder / f"{sku}_meta.json",
        }
    legacy = legacy_detail_paths(sku)
    if any(path.exists() for path in legacy.values()):
        return legacy
    folder = RAW_DETAIL_DIR / f"na_{safe_part(sku)}_pending"
    return {
        "html": folder / f"{sku}.html",
        "headers": folder / f"{sku}_headers.json",
        "apollo": folder / f"{sku}_apollo.json",
        "meta": folder / f"{sku}_meta.json",
    }


def detail_paths_for_status(sku, target, success):
    folder = detail_folder(sku, target, "success" if success else "fail")
    return {
        "html": folder / f"{sku}.html",
        "headers": folder / f"{sku}_headers.json",
        "apollo": folder / f"{sku}_apollo.json",
        "meta": folder / f"{sku}_meta.json",
    }


def review_paths(sku):
    folder = review_folder(sku)
    if folder:
        return {
            "request": folder / f"{sku}_request.json",
            "response_txt": folder / f"{sku}_response.txt",
            "response_json": folder / f"{sku}_response.json",
            "headers": folder / f"{sku}_headers.json",
            "meta": folder / f"{sku}_meta.json",
        }
    legacy = {
        "request": RAW_REVIEW_DIR / f"{sku}_request.json",
        "response_txt": RAW_REVIEW_DIR / f"{sku}_response.txt",
        "response_json": RAW_REVIEW_DIR / f"{sku}_response.json",
        "headers": RAW_REVIEW_DIR / f"{sku}_headers.json",
        "meta": RAW_REVIEW_DIR / f"{sku}_meta.json",
    }
    if any(path.exists() for path in legacy.values()):
        return legacy
    folder = RAW_REVIEW_DIR / f"na_{safe_part(sku)}_pending"
    return {
        "request": folder / f"{sku}_request.json",
        "response_txt": folder / f"{sku}_response.txt",
        "response_json": folder / f"{sku}_response.json",
        "headers": folder / f"{sku}_headers.json",
        "meta": folder / f"{sku}_meta.json",
    }


def review_paths_for_status(sku, target, success):
    folder = review_folder(sku, target, "success" if success else "fail")
    return {
        "request": folder / f"{sku}_request.json",
        "response_txt": folder / f"{sku}_response.txt",
        "response_json": folder / f"{sku}_response.json",
        "headers": folder / f"{sku}_headers.json",
        "meta": folder / f"{sku}_meta.json",
    }


def target_url(target, sku):
    url = str(target.get("product_url") or "").strip()
    return url or old_pdp_url(sku)


def old_pdp_bsin(url):
    match = re.search(r"/product/[^/]+/([^/?#]+)", str(url or ""))
    return match.group(1) if match else ""


def has_product_schema(html_text):
    return "ProductSchema_init" in html_text and "productBySkuId" in html_text


def apollo_payloads_json(html_text):
    try:
        return extract_apollo_payloads(html_text)
    except Exception:
        return []


def slim_html(html_text):
    soup = BeautifulSoup(html_text or "", "html.parser")
    for tag in soup.find_all(["style", "link", "svg", "noscript", "iframe"]):
        tag.decompose()
    for script in list(soup.find_all("script")):
        script_type = (script.get("type") or "").lower()
        text = script.string or script.get_text() or ""
        keep = "ApolloSSRDataTransport" not in text and script_type == "application/ld+json"
        if not keep:
            script.decompose()
            continue
        for attr in list(script.attrs):
            if attr not in {"type", "id"}:
                del script.attrs[attr]
    return str(soup)


def stored_html(html_text):
    if SAVE_HTML_MODE == "none":
        return ""
    if SAVE_HTML_MODE == "full":
        return html_text
    return slim_html(html_text)


def write_detail_artifacts(paths, html_text, headers):
    payloads = apollo_payloads_json(html_text)
    paths["apollo"].write_text(json.dumps(payloads, ensure_ascii=False, separators=(",", ":")), encoding="utf-8")
    stored = stored_html(html_text)
    if SAVE_HTML_MODE == "none":
        if paths["html"].exists():
            paths["html"].unlink()
    else:
        paths["html"].write_text(stored, encoding="utf-8", errors="replace")
    paths["headers"].write_text(json.dumps(headers, indent=2, ensure_ascii=False), encoding="utf-8")
    return {
        "html_mode": SAVE_HTML_MODE,
        "full_bytes": len(html_text or ""),
        "stored_bytes": len(stored or ""),
        "apollo_payload_count": len(payloads),
    }


def detail_success(sku):
    paths = detail_paths(sku)
    meta = read_json(paths["meta"])
    if meta.get("success") is True and paths["html"].exists():
        return True
    return False


def review_success(sku):
    paths = review_paths(sku)
    meta = read_json(paths["meta"])
    if meta.get("success") is True and paths["response_json"].exists():
        return True
    if review_result_count(paths["response_json"]) is not None:
        return True
    return False


def expected_review_count(target):
    value = str(target.get("review_count") or "").replace(",", "").strip()
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return 0


def review_needs_retry(target):
    sku = str(target.get("sku_id") or "").strip()
    if not review_success(sku):
        return True
    if expected_review_count(target) <= 0:
        return False
    if CATEGORY == "HHP" and not hhp_review_has_recommended_percent(sku):
        return True
    return not bool(review20_content(sku))


def review_result_count(path):
    data = read_json(path)
    return review_result_count_from_json(data)


def review_result_count_from_json(data):
    product = ((data.get("data") or {}).get("productBySkuId") or {})
    reviews = (product.get("reviews") or {}).get("results")
    if isinstance(reviews, list):
        return len(reviews)
    review_count = (product.get("reviewInfo") or {}).get("reviewCount")
    try:
        if int(str(review_count).replace(",", "").strip()) == 0:
            return 0
    except (TypeError, ValueError):
        pass
    return None


def attempts(meta_path):
    return int(read_json(meta_path).get("attempt", 0) or 0)


def next_attempt(meta_path, url):
    meta = read_json(meta_path)
    previous_url = str(meta.get("url") or "").strip()
    if previous_url and previous_url != str(url or "").strip():
        return 1
    return int(meta.get("attempt", 0) or 0) + 1


def target_rows(apply_filters=True):
    rows = load_csv(TARGET_CSV)
    unique = []
    seen = set()
    for row in rows:
        sku = str(row.get("sku_id") or "").strip()
        if not sku or sku in seen:
            continue
        seen.add(sku)
        unique.append(row)
    if apply_filters and TARGET_SKUS:
        unique = [
            row
            for row in unique
            if str(row.get("sku_id") or "").strip().lower() in TARGET_SKUS
            or str(row.get("bsin") or "").strip().lower() in TARGET_SKUS
            or str(row.get("item") or "").strip().lower() in TARGET_SKUS
        ]
    if apply_filters and RETRY_ONLY:
        if STAGE == "detail":
            unique = [row for row in unique if not detail_success(row["sku_id"])]
        elif STAGE == "review":
            unique = [
                row
                for row in unique
                if detail_success(row["sku_id"]) and review_needs_retry(row)
            ]
        else:
            unique = [
                row
                for row in unique
                if not detail_success(row["sku_id"]) or review_needs_retry(row)
            ]
    if apply_filters and LIMIT:
        unique = unique[:LIMIT]
    return unique


def find_started_operation(html_text, operation_name):
    for payload in extract_apollo_payloads(html_text):
        for event in payload.get("events", []):
            if event.get("type") != "started":
                continue
            options = event.get("options", {})
            query = options.get("query") or ""
            if query.startswith(f"query {operation_name}") or f"query {operation_name}(" in query:
                return {
                    "operationName": operation_name,
                    "variables": options.get("variables", {}),
                    "query": query,
                }
    return None


def find_started_operation_from_payloads(payloads, operation_name):
    for payload in payloads:
        for event in payload.get("events", []):
            if event.get("type") != "started":
                continue
            options = event.get("options", {})
            query = options.get("query") or ""
            if query.startswith(f"query {operation_name}") or f"query {operation_name}(" in query:
                return {
                    "operationName": operation_name,
                    "variables": options.get("variables", {}),
                    "query": query,
                }
    return None


def review20_payload(html_text):
    payload = find_started_operation(html_text, "ProductSchema_init")
    if not payload:
        return None
    payload["query"] = payload["query"].replace("reviews(filter:{pageSize:5})", "reviews(filter:{pageSize:20})")
    if CATEGORY == "HHP":
        payload["query"] = payload["query"].replace(
            "reviewInfo{averageRating reviewCount}",
            "reviewInfo{averageRating reviewCount recommendedPercent}",
        )
    return payload


def detail_payloads(sku):
    paths = detail_paths(sku)
    if paths.get("apollo") and paths["apollo"].exists():
        try:
            data = json.loads(paths["apollo"].read_text(encoding="utf-8-sig"))
            if isinstance(data, list):
                return data
        except ValueError:
            pass
    html_path = paths["html"]
    html_text = html_path.read_text(encoding="utf-8", errors="replace") if html_path.exists() else ""
    return apollo_payloads_json(html_text)


def review20_payload_for_sku(sku):
    payload = find_started_operation_from_payloads(detail_payloads(sku), "ProductSchema_init")
    if not payload:
        return None
    payload["query"] = payload["query"].replace("reviews(filter:{pageSize:5})", "reviews(filter:{pageSize:20})")
    if CATEGORY == "HHP":
        payload["query"] = payload["query"].replace(
            "reviewInfo{averageRating reviewCount}",
            "reviewInfo{averageRating reviewCount recommendedPercent}",
        )
    return payload


def fetch_detail(client, target):
    sku = str(target.get("sku_id") or "").strip()
    pdp_url = target_url(target, sku)
    current_paths = detail_paths(sku)
    if not FORCE_REFRESH and detail_success(sku):
        return read_json(current_paths["meta"])
    attempt = next_attempt(current_paths["meta"], pdp_url)
    meta = {"sku_id": sku, "stage": "detail", "url": pdp_url, "attempt": attempt, "started_at": now()}
    if not FORCE_REFRESH and attempt > MAX_ATTEMPTS:
        paths = detail_paths_for_status(sku, target, False)
        meta.update({"success": False, "error": "max_attempts_exceeded"})
        paths["meta"].write_text(json.dumps(meta, indent=2, ensure_ascii=False), encoding="utf-8")
        return meta

    start = time.perf_counter()
    response = None
    try:
        response = client.get(pdp_url, params=detail_params(), timeout=REQUEST_TIMEOUT)
        html_text = response.text
        status = response.status_code
        success = status == 200 and has_product_schema(html_text)
        paths = detail_paths_for_status(sku, target, success)
        artifact_meta = write_detail_artifacts(paths, html_text, dict(response.headers))
        meta.update(
            {
                "success": success,
                "status_code": status,
                "elapsed_seconds": round(time.perf_counter() - start, 3),
                "x_request_cost": request_cost(response.headers),
                "bytes": artifact_meta["full_bytes"],
                "stored_bytes": artifact_meta["stored_bytes"],
                "html_mode": artifact_meta["html_mode"],
                "apollo_payload_count": artifact_meta["apollo_payload_count"],
                "finished_at": now(),
                "error": "" if success else "detail_html_missing_product_schema",
            }
        )
    except RequestException as exc:
        paths = detail_paths_for_status(sku, target, False)
        meta.update(
            {
                "success": False,
                "status_code": "ERR",
                "elapsed_seconds": round(time.perf_counter() - start, 3),
                "x_request_cost": 0,
                "finished_at": now(),
                "error": str(exc),
            }
        )
    paths["meta"].write_text(json.dumps(meta, indent=2, ensure_ascii=False), encoding="utf-8")
    return meta


def fetch_review20(client, target):
    sku = str(target.get("sku_id") or "").strip()
    pdp_url = target_url(target, sku)
    current_paths = review_paths(sku)
    if not FORCE_REFRESH and not review_needs_retry(target):
        return read_json(current_paths["meta"])
    attempt = next_attempt(current_paths["meta"], pdp_url)
    meta = {"sku_id": sku, "stage": "review20", "url": pdp_url, "attempt": attempt, "started_at": now()}
    if not FORCE_REFRESH and attempt > MAX_ATTEMPTS:
        paths = review_paths_for_status(sku, target, False)
        meta.update({"success": False, "error": "max_attempts_exceeded"})
        paths["meta"].write_text(json.dumps(meta, indent=2, ensure_ascii=False), encoding="utf-8")
        return meta

    payload = review20_payload_for_sku(sku)
    if not payload:
        paths = review_paths_for_status(sku, target, False)
        meta.update({"success": False, "error": "ProductSchema_init not found", "finished_at": now()})
        paths["meta"].write_text(json.dumps(meta, indent=2, ensure_ascii=False), encoding="utf-8")
        return meta
    paths = review_paths_for_status(sku, target, False)
    paths["request"].write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")

    start = time.perf_counter()
    try:
        response = client.post(
            "https://www.bestbuy.com/gateway/graphql",
            params=graphql_params(),
            headers={
                "accept": "application/json, text/plain, */*",
                "content-type": "application/json",
                "origin": "https://www.bestbuy.com",
                "referer": pdp_url,
            },
            data=json.dumps(payload),
            timeout=REQUEST_TIMEOUT,
        )
        text = response.text
        review_count = 0
        error = ""
        response_json = {}
        try:
            response_json = response.json()
            count = review_result_count_from_json(response_json)
            review_count = count if count is not None else 0
            if response_json.get("errors"):
                error = json.dumps(response_json.get("errors"), ensure_ascii=False, separators=(",", ":"))
        except ValueError as exc:
            error = str(exc)
        success = response.status_code == 200 and review_result_count_from_json(response_json) is not None
        paths = review_paths_for_status(sku, target, success)
        if response_json:
            paths["response_json"].write_text(
                json.dumps(response_json, indent=2, ensure_ascii=False), encoding="utf-8"
            )
        paths["response_txt"].write_text(text, encoding="utf-8", errors="replace")
        paths["headers"].write_text(
            json.dumps(dict(response.headers), indent=2, ensure_ascii=False), encoding="utf-8"
        )
        paths["request"].write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")
        meta.update(
            {
                "success": success,
                "status_code": response.status_code,
                "elapsed_seconds": round(time.perf_counter() - start, 3),
                "x_request_cost": request_cost(response.headers),
                "bytes": len(text or ""),
                "review_count_returned": review_count,
                "finished_at": now(),
                "error": error if not success else "",
            }
        )
    except RequestException as exc:
        paths = review_paths_for_status(sku, target, False)
        meta.update(
            {
                "success": False,
                "status_code": "ERR",
                "elapsed_seconds": round(time.perf_counter() - start, 3),
                "x_request_cost": 0,
                "finished_at": now(),
                "error": str(exc),
            }
        )
    paths["meta"].write_text(json.dumps(meta, indent=2, ensure_ascii=False), encoding="utf-8")
    return meta


def products_from_detail(sku):
    products = []
    variations = []
    seen_products = set()

    def add_product(product):
        if not isinstance(product, dict) or str(product.get("skuId")) != str(sku):
            return
        if product.get("openBoxCondition") not in (None, "", [], {}):
            return
        condition_type = ((product.get("condition") or {}).get("type") or "").lower()
        if condition_type and condition_type != "new":
            return
        marker = id(product)
        if marker in seen_products:
            return
        seen_products.add(marker)
        products.append(product)

    def add_variations(value):
        if not isinstance(value, dict):
            return
        variation_display = value.get("productVariationDetailDisplay") or {}
        items = variation_display.get("productBsinVariations", []) or []
        if not isinstance(items, list):
            return
        for item in items:
            if not isinstance(item, dict):
                continue
            name = (
                (((item.get("bsinProduct") or {}).get("featuredSKU") or {}).get("product") or {})
                .get("name", {})
                .get("short")
            )
            if name and name not in variations:
                variations.append(name)

    def walk(value):
        if isinstance(value, dict):
            add_product(value)
            add_variations(value)
            for child in value.values():
                walk(child)
        elif isinstance(value, list):
            for child in value:
                walk(child)

    for payload in detail_payloads(sku):
        for event in payload.get("events", []):
            data = event_data(event)
            product = data.get("productBySkuId") if isinstance(data, dict) else None
            add_product(product)
            walk(data)
            bsin_product = data.get("bsinProduct") if isinstance(data, dict) else None
            add_variations(bsin_product)
    return products, variations


def similar_products_from_html(html_text):
    if not html_text or "Compare similar products" not in html_text:
        return []
    soup = BeautifulSoup(html_text, "lxml")
    heading_text = soup.find(string=re.compile(r"Compare\s+similar\s+products", re.I))
    if not heading_text:
        return []

    heading = heading_text.parent
    section = heading.find_parent(attrs={"data-component-name": re.compile(r"^Compare$", re.I)})
    if section is None:
        section = heading
        for _ in range(8):
            if not section or not getattr(section, "parent", None):
                break
            section = section.parent
            links = section.find_all("a", href=re.compile(r"/product/", re.I))
            images = section.find_all("img", alt=True)
            if len(links) + len(images) >= 4:
                break
        else:
            section = heading.parent if heading else soup

    names = []
    if CATEGORY == "HHP":
        product_name_re = re.compile(
            r"iphone|galaxy|pixel|motorola|moto|razr|nokia|oneplus|phone|cell|"
            r"unlocked|verizon|at&t|t-mobile|mint mobile|tracfone|cricket|boost",
            re.I,
        )
    else:
        product_name_re = re.compile(r"\bclass\b|smart|tv|television|oled|qled|uhd|led|roku|fire tv", re.I)
    skip_re = re.compile(
        r"compare similar products|shop now|learn more|add to cart|stars?|reviews?|"
        r"home delivery|mounting|installation|haul-away|recycling|soundbar|wall mount",
        re.I,
    )
    if CATEGORY == "HHP":
        for element in section.find_all(class_=re.compile(r"\bproduct-title\b", re.I)):
            text = compact_text(element.get_text(" "))
            if (
                text
                and product_name_re.search(text)
                and not skip_re.search(text)
                and text not in names
            ):
                names.append(text)
    for element in section.find_all(["img", "a"]):
        if element.name == "img":
            text = compact_text(element.get("alt"))
        else:
            href = element.get("href") or ""
            if not re.search(r"/product/", href, re.I):
                continue
            text = compact_text(element.get_text(" ", strip=True))
        if not text:
            continue
        if len(text) < 20 or skip_re.search(text):
            continue
        if re.search(r"\$\d|^\d+(\.\d+)?$|stars?|reviews?", text, re.I):
            continue
        if not product_name_re.search(text):
            continue
        if text not in names:
            names.append(text)
        if len(names) >= 4:
            break
    return names


def target_page_type(target):
    source = target.get("target_source")
    if source == "bsr_only_backfill":
        return "bsr"
    if source == "promotion_backfill":
        return "promotion"
    if source == "trending_backfill":
        return "trend"
    return "main"


def first_value(products, key):
    for product in reversed(products):
        value = product.get(key)
        if value not in (None, "", [], {}):
            return value
    return ""


def first_path(products, path):
    for product in reversed(products):
        current = product
        ok = True
        for part in path:
            if isinstance(part, int):
                if isinstance(current, list) and len(current) > part:
                    current = current[part]
                else:
                    ok = False
                    break
            elif isinstance(current, dict) and part in current:
                current = current[part]
            else:
                ok = False
                break
        if ok and current not in (None, "", [], {}):
            return current
    return ""


def best_path(products, path, required_keys=()):
    values = []
    for product in products:
        current = product
        ok = True
        for part in path:
            if isinstance(part, int):
                if isinstance(current, list) and len(current) > part:
                    current = current[part]
                else:
                    ok = False
                    break
            elif isinstance(current, dict) and part in current:
                current = current[part]
            else:
                ok = False
                break
        if ok and isinstance(current, dict):
            score = sum(1 for key in required_keys if current.get(key) not in (None, "", [], {}))
            values.append((score, current))
    return sorted(values, key=lambda item: item[0], reverse=True)[0][1] if values else {}


def best_price(products):
    best = {}
    best_score = -1
    for product in products:
        embedded_price = product.get("price")
        if isinstance(embedded_price, dict):
            price = embedded_price
        elif any(
            key in product
            for key in (
                "displayableCustomerPrice",
                "customerPrice",
                "displayableRegularPrice",
                "regularPrice",
                "totalSavings",
                "restrictedPriceDisplayMessage",
                "financeOption",
            )
        ):
            price = product
        else:
            continue
        score = 0
        open_box_condition = first_non_empty(product.get("openBoxCondition"), price.get("openBoxCondition"))
        if open_box_condition in (None, "", [], {}):
            score += 1000
        else:
            score -= 1000
        condition_type = ((product.get("condition") or {}).get("type") or "").lower()
        if condition_type == "new":
            score += 100
        if price.get("displayableCustomerPrice") not in (None, "", [], {}):
            score += 100
        if price.get("customerPrice") not in (None, "", [], {}):
            score += 90
        finance_option = price.get("financeOption") if isinstance(price.get("financeOption"), dict) else {}
        if finance_option.get("totalCost") not in (None, "", [], {}):
            score += 80
        if price.get("displayableRegularPrice") not in (None, "", [], {}):
            score += 30
        if price.get("regularPrice") not in (None, "", [], {}):
            score += 25
        if price.get("totalSavings") not in (None, "", [], {}):
            score += 10
        if price_policy_message(price):
            score += 1
        if score > best_score:
            best = price
            best_score = score
    return best


def spec_value(products, display_name):
    for product in reversed(products):
        for group in product.get("specificationGroups") or []:
            for spec in group.get("specifications") or []:
                if (spec.get("displayName") or "").lower() == display_name.lower():
                    return spec.get("value", "")
    return ""


def first_spec_value(products, names):
    for name in names:
        value = spec_value(products, name)
        if value:
            return compact_text(value)
    return ""


def offer_count_value(value):
    text = compact_text(value)
    if not text:
        return ""
    if re.fullmatch(r"\d+", text):
        return "" if text == "0" else text
    match = re.search(r"\+?\s*(\d+)\s+offers?\s+for\s+you\b", text, re.I)
    return match.group(1) if match else ""


def offer_count_from_html(html_text):
    if not html_text:
        return ""
    match = re.search(r"\+?\s*(\d+)\s+offers?\s+for\s+you\b", html.unescape(html_text), re.I)
    return match.group(1) if match else ""


def offer_count_from_products(products):
    counts = []
    for product in products:
        offers = ((product.get("offers") or {}).get("offers") or []) if isinstance(product.get("offers"), dict) else []
        hot_offer_count = sum(1 for offer in offers if isinstance(offer, dict) and offer.get("hotOffer"))
        price = product.get("price") if isinstance(product.get("price"), dict) else {}
        gift_skus = price.get("giftSkus") if isinstance(price, dict) else []
        gift_count = len(gift_skus) if isinstance(gift_skus, list) else 0
        total = hot_offer_count + gift_count
        if total > 0:
            counts.append(total)
    return str(max(counts)) if counts else ""


def offer_value(selector_values, products, html_text):
    for key in ("offer", "special_offer", "retailer_offer"):
        value = offer_count_value(selector_values.get(key))
        if value:
            return value
    value = offer_count_from_html(html_text)
    if value:
        return value
    return offer_count_from_products(products)


HHP_PROMOTION_TYPES = {
    "best selling",
    "bundle and save",
    "overall pick",
    "pre-owned",
    "top rated",
    "trade-in offer",
    "trending deal",
}


def hhp_promotion_type(products, html_text):
    if CATEGORY != "HHP":
        return ""
    names = []
    for product in products:
        for badge in product.get("badges") or []:
            if not isinstance(badge, dict):
                continue
            name = compact_text(badge.get("displayName"))
            if name and name.lower() in HHP_PROMOTION_TYPES and name not in names:
                names.append(name)
    if not names and html_text:
        for value in re.findall(
            r'data-component-name="Badge"[^>]*>.*?data-testid="button-label"[^>]*>(.*?)</span>',
            html_text,
            re.I | re.S,
        ):
            name = compact_text(html.unescape(re.sub(r"<[^>]+>", " ", value)))
            if name and name.lower() in HHP_PROMOTION_TYPES and name not in names:
                names.append(name)
    return " ||| ".join(names)


def recommendation(products, target):
    target_reviews = review_count_value({"reviewCount": target.get("review_count")})
    target_rating = usable_rating(target.get("rating"))
    candidates = []
    for index, product in enumerate(products):
        review_info = product.get("reviewInfo") if isinstance(product, dict) else {}
        if not isinstance(review_info, dict):
            continue
        value = review_info.get("recommendedPercent")
        if value in (None, "", [], {}):
            continue
        score = index
        product_reviews = review_count_value(review_info)
        product_rating = usable_rating(review_info.get("averageRating"))
        if target_reviews is not None and product_reviews == target_reviews:
            score += 1000
        if target_rating and product_rating == target_rating:
            score += 500
        try:
            if float(str(value).strip()) > 0:
                score += 100
        except ValueError:
            pass
        candidates.append((score, value))
    if not candidates:
        return ""
    value = sorted(candidates, key=lambda item: item[0], reverse=True)[0][1]
    return f"{value}% would recommend to a friend"


def review20_review_info(sku):
    path = review_paths(sku)["response_json"]
    if not path.exists():
        return {}
    data = read_json(path)
    review_info = ((data.get("data") or {}).get("productBySkuId") or {}).get("reviewInfo") or {}
    return review_info if isinstance(review_info, dict) else {}


def recommendation_from_review20(sku):
    value = review20_review_info(sku).get("recommendedPercent")
    if value in (None, "", [], {}):
        return ""
    return f"{value}% would recommend to a friend"


def hhp_review_has_recommended_percent(sku):
    if CATEGORY != "HHP":
        return True
    value = review20_review_info(sku).get("recommendedPercent")
    return value not in (None, "", [], {})


def _has_non_empty_syndicated_summary(value):
    if value in (None, "", [], {}):
        return False
    if isinstance(value, list):
        return any(item not in (None, "", [], {}) for item in value)
    if isinstance(value, dict):
        return True
    if isinstance(value, str):
        text = value.strip()
        if not text or text.lower() in {"null", "none", "[]", "{}"}:
            return False
        try:
            return _has_non_empty_syndicated_summary(json.loads(text))
        except ValueError:
            return "reviews from " in text.lower()
    return False


def review_count_value(review_info):
    if not isinstance(review_info, dict):
        return None
    value = review_info.get("reviewCount")
    if value in (None, ""):
        return None
    try:
        return int(float(str(value).replace(",", "").strip()))
    except ValueError:
        return None


def has_syndicated_reviews(products, target):
    for product in products:
        review_info = product.get("reviewInfo") if isinstance(product, dict) else {}
        if isinstance(review_info, dict) and _has_non_empty_syndicated_summary(
            review_info.get("syndicatedReviewSummary")
        ) and review_count_value(review_info) == 0:
            return True
    for key in ("syndicated_review_summary_json", "syndicatedReviewSummary"):
        if _has_non_empty_syndicated_summary(target.get(key)):
            target_count = str(target.get("review_count") or "").replace(",", "").strip()
            if target_count in {"", "0", "0.0"}:
                return True
    return False


def has_external_review_text(html_text, selector_values):
    values = list(selector_values.values())
    if html_text:
        values.append(html_text)
    for value in values:
        text = compact_text(value)
        if re.search(r"\(?\s*[\d,]+\s+reviews?\s+from\s+[^)]{2,80}\)?", text, re.I):
            return True
    return False


def has_external_reviews(products, target, html_text, selector_values):
    return has_syndicated_reviews(products, target) or has_external_review_text(html_text, selector_values)


def is_zero_review_value(value):
    text = compact_text(value).replace(",", "")
    if text == "":
        return False
    try:
        return int(float(text)) == 0
    except ValueError:
        return False


def usable_rating(value):
    text = compact_text(value)
    if not text:
        return ""
    if text.lower() == "not yet reviewed":
        return text
    try:
        return "" if float(text) <= 0 else text
    except ValueError:
        return text


def should_mark_not_yet_reviewed(row, external_reviews):
    rating = compact_text(row.get("star_rating")).lower()
    if external_reviews:
        return True
    if rating == "not yet reviewed":
        return True
    return rating in {"0", "0.0"} and is_zero_review_value(row.get("count_of_reviews"))


def review20_content(sku):
    path = review_paths(sku)["response_json"]
    if not path.exists():
        return ""
    data = read_json(path)
    reviews = (((data.get("data") or {}).get("productBySkuId") or {}).get("reviews") or {}).get("results") or []
    chunks = []
    for index, review in enumerate(reviews[:20], 1):
        text = compact_text(review.get("text"))
        if text:
            chunks.append(f"review{index} - {text}")
    return " ||| ".join(chunks)


def sample_fields():
    if CATEGORY in FALLBACK_FINAL_FIELDS:
        return FALLBACK_FINAL_FIELDS[CATEGORY]
    config = db_config()
    table_name = bestbuy_output_table()
    if config and table_name:
        try:
            import psycopg2

            conn = psycopg2.connect(
                host=config.get("host"),
                port=int(config.get("port") or 5432),
                user=config.get("user"),
                password=config.get("password"),
                dbname=config.get("database"),
                connect_timeout=10,
            )
            with conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        SELECT column_name
                        FROM information_schema.columns
                        WHERE table_schema = 'public' AND table_name = %s
                        ORDER BY ordinal_position
                        """,
                        (table_name,),
                    )
                    fields = [row[0] for row in cur.fetchall()]
                    if fields:
                        return fields
        except Exception:
            pass
    with SAMPLE_SCHEMA_CSV.open("r", encoding="utf-8-sig", newline="") as f:
        return next(csv.reader(f))


def output_row(target):
    sku = str(target.get("sku_id") or "").strip()
    detail_html_path = detail_paths(sku)["html"]
    html_text = detail_html_path.read_text(encoding="utf-8", errors="replace") if detail_html_path.exists() else ""
    selector_values = detail_selector_values(html_text)
    products, variations = products_from_detail(sku)
    similar_names = similar_products_from_html(html_text) or variations
    price = best_price(products)
    policy_price = price_policy_value(price, selector_values, target, html_text)
    selector_final_price = selector_values.get("final_sku_price")
    finance_option = price.get("financeOption") if isinstance(price.get("financeOption"), dict) else {}
    numeric_final_price = money(
        price.get("displayableCustomerPrice")
        or price.get("customerPrice")
        or finance_option.get("totalCost")
        or target.get("customer_price")
    )
    final_price = first_non_empty(
        "" if is_policy_price(selector_final_price) else selector_final_price,
        numeric_final_price,
        policy_price,
        selector_final_price,
    )
    original_price = "" if is_policy_price(final_price) else first_non_empty(
        selector_values.get("original_sku_price"),
        money(price.get("displayableRegularPrice") or price.get("regularPrice") or target.get("regular_price")),
    )
    savings = ""
    if final_price and original_price and not is_policy_price(final_price):
        savings = first_non_empty(
            selector_values.get("savings"),
            money_int(price.get("totalSavings") or target.get("total_savings")),
        )
        if final_price == original_price:
            savings = ""
        else:
            savings = normalize_savings(savings)
    if final_price and original_price and final_price == original_price:
        original_price = ""
        savings = ""
    review_info = first_value(products, "reviewInfo") or {}
    pickup = best_path(products, ["fulfillmentOptions", "ispuDetails", 0, "ispuAvailability", 0], ("maxDate",))
    delivery = best_path(
        products,
        ["fulfillmentOptions", "deliveryDetails", 0, "deliveryAvailability", 0],
        ("deliverySlots",),
    )
    delivery_slot = (delivery.get("deliverySlots") or [{}])[0].get("date") if isinstance(delivery, dict) else ""
    shipping_date = shipping_date_from_products(products) if CATEGORY == "HHP" else ""
    screen = spec_value(products, "Screen Size Class") or spec_value(products, "Screen Size")
    energy = spec_value(products, "Estimated Annual Electricity Use")
    model_year = spec_value(products, "Model Year")
    model_number = first_spec_value(products, ["Model Number"]) or first_path(products, ["manufacturer", "modelNumber"])
    ref_capacity = first_spec_value(products, ["Capacity", "Total Capacity", "Refrigerator Capacity"])
    ref_refrigerator_type = first_spec_value(products, ["Refrigerator Style", "Refrigerator Type", "Configuration"])
    ldy_capacity = first_spec_value(
        products,
        ["Washer Capacity", "Dryer Capacity", "Capacity", "Total Capacity"],
    )
    ldy_loading_type = first_spec_value(products, ["Load Type", "Washer Load Type", "Loading Type"])
    product_name = first_path(products, ["name", "short"]) or target.get("product_name", "")
    product_url = first_path(products, ["url", "pdp"]) or target.get("product_url", "")
    bsin = first_value(products, "bsin") or target.get("bsin") or old_pdp_bsin(product_url) or ""
    primary_product = products[-1] if products else {}
    hhp_attrs = hhp_attributes_from_product(primary_product, product_name) if CATEGORY == "HHP" else {}

    crawl_dt = eastern_now()
    category_key = (target.get("category_key") or CATEGORY).upper()
    row = {
        "id": "",
        "product": category_key,
        "item": bsin,
        "sku": model_number,
        "account_name": "Bestbuy",
        "page_type": target_page_type(target),
        "count_of_reviews": int_commas(review_info.get("reviewCount") or target.get("review_count")),
        "retailer_sku_name": first_non_empty(product_name, selector_values.get("retailer_sku_name")),
        "product_url": product_url,
        "star_rating": first_non_empty(
            usable_rating(review_info.get("averageRating")),
            usable_rating(target.get("rating")),
            selector_values.get("top_star_rating"),
            selector_values.get("star_rating"),
            "Not yet reviewed",
        ),
        "count_of_star_ratings": int_commas(review_info.get("reviewCount") or target.get("review_count")),
        "screen_size": first_non_empty(screen, selector_values.get("screen_size")),
        "final_sku_price": final_price,
        "original_sku_price": original_price,
        "savings": savings,
        "offer": first_non_empty(
            offer_value(selector_values, products, html_text),
            target.get("offer_count"),
        ),
        "pick_up_availability": first_non_empty(
            selector_values.get("pick_up_availability"),
            date_to_phrase("Pick up", pickup.get("maxDate") if isinstance(pickup, dict) else ""),
        ),
        "fastest_delivery": visible_shipping_value(
            selector_values.get("fastest_delivery"),
            fastest_delivery_from_html(html_text),
            date_to_phrase("Get it by", shipping_date),
            normalize_func=normalize_fastest_delivery,
        ),
        "delivery_availability": visible_shipping_value(
            "" if CATEGORY == "HHP" else selector_values.get("delivery_availability"),
            "" if CATEGORY == "HHP" else delivery_from_html(html_text),
            "" if CATEGORY == "HHP" else date_to_phrase("Delivery as soon as", delivery_slot),
            normalize_func=normalize_delivery_availability,
        ),
        "shipping_info": "",
        "sku_status": "Sponsored" if target.get("is_sponsored") in {"1", "true", "True"} else "",
        "trade_in": first_non_empty(
            selector_values.get("trade_in"),
            trade_in_from_html(html_text),
            trade_in_from_products(products),
        ),
        "hhp_storage": hhp_attrs.get("hhp_storage", ""),
        "hhp_color": hhp_attrs.get("hhp_color", ""),
        "hhp_carrier": hhp_attrs.get("hhp_carrier", ""),
        "ref_capacity": ref_capacity,
        "ref_refrigerator_type": ref_refrigerator_type,
        "ldy_capacity": ldy_capacity,
        "ldy_loading_type": ldy_loading_type,
        "detailed_review_content": review20_content(sku),
        "summarized_review_content": "",
        "top_mentions": "",
        "recommendation_intent": first_non_empty(
            recommendation_phrase(selector_values.get("recommendation_intent")),
            recommendation_phrase(selector_values.get("reviewpage_recommendation_intent_fallback")),
            recommendation_phrase(selector_values.get("reviewpage_recommendation_intent_fallback2")),
            recommendation_phrase(selector_values.get("reviewpage_recommendation_intent_fallback3")),
            recommendation_phrase(selector_values.get("reviewpage_recommendation_intent_fallback4")),
            recommendation_from_html(html_text),
            recommendation_from_review20(sku) if CATEGORY == "HHP" else "",
            recommendation(products, target),
        ),
        "main_rank": target.get("main_rank", ""),
        "bsr_rank": target.get("bsr_rank", ""),
        "promotion_position": target.get("promotion_position", ""),
        "trend_rank": target.get("trend_rank", ""),
        "retailer_sku_name_similar": " ||| ".join(similar_names[:4]),
        "estimated_annual_electricity_use": clean_energy(energy),
        "promotion_type": first_non_empty(hhp_promotion_type(products, html_text), target.get("promotion_type", "")),
        "calendar_week": f"w{crawl_dt.isocalendar().week}",
        "crawl_datetime": crawl_dt.strftime("%Y-%m-%d %H:%M:%S"),
        "crawl_strdatetime": crawl_dt.strftime("%Y-%m-%d %H:%M:%S"),
        "model_year": model_year,
        "batch_id": BATCH_ID,
        "country": "SEA",
    }
    row["fastest_delivery"] = normalize_fastest_delivery_output(row["fastest_delivery"])
    external_reviews = has_external_reviews(products, target, html_text, selector_values)
    if should_mark_not_yet_reviewed(row, external_reviews):
        row["star_rating"] = "Not yet reviewed"
        row["count_of_reviews"] = "0"
        row["count_of_star_ratings"] = "0"
        row["recommendation_intent"] = ""
    for field, value in selector_values.items():
        row.setdefault(field, value)
    return row


def build_outputs(targets):
    rows = []
    failures = []
    for target in targets:
        sku = str(target.get("sku_id") or "").strip()
        dmeta = read_json(detail_paths(sku)["meta"])
        rmeta = read_json(review_paths(sku)["meta"])
        rows.append(output_row(target))
        if not dmeta.get("success"):
            failures.append(
                {
                    "sku_id": sku,
                    "stage": "detail",
                    "attempt": dmeta.get("attempt", 0),
                    "status_code": dmeta.get("status_code", ""),
                    "error": dmeta.get("error", "missing_detail"),
                    "retryable": str(int(int(dmeta.get("attempt", 0) or 0) < MAX_ATTEMPTS)),
                }
            )
        if not rmeta.get("success"):
            failures.append(
                {
                    "sku_id": sku,
                    "stage": "review20",
                    "attempt": rmeta.get("attempt", 0),
                    "status_code": rmeta.get("status_code", ""),
                    "error": rmeta.get("error", "missing_review20"),
                    "retryable": str(int(int(rmeta.get("attempt", 0) or 0) < MAX_ATTEMPTS)),
                }
            )
    return rows, failures


def main():
    started_at = now()
    targets = target_rows(apply_filters=True)
    output_targets = target_rows(apply_filters=False)
    api_key = "" if REBUILD_ONLY else os.getenv("ZENROWS_API_KEY")
    client = ZenRowsClient(api_key) if api_key else None

    RAW_DETAIL_DIR.mkdir(parents=True, exist_ok=True)
    RAW_REVIEW_DIR.mkdir(parents=True, exist_ok=True)
    PARSED_DIR.mkdir(parents=True, exist_ok=True)

    if STAGE not in {"all", "detail", "review"}:
        raise RuntimeError("BESTBUY_DETAIL_STAGE must be one of: all, detail, review")

    if not client and not REBUILD_ONLY:
        # Cached parse-only mode is useful during local development.
        if STAGE == "detail":
            missing = [row.get("sku_id") for row in targets if not detail_success(row.get("sku_id"))]
        elif STAGE == "review":
            missing = [row.get("sku_id") for row in targets if not review_success(row.get("sku_id"))]
        else:
            missing = [
                row.get("sku_id")
                for row in targets
                if not detail_success(row.get("sku_id")) or not review_success(row.get("sku_id"))
            ]
        if missing:
            raise RuntimeError("Set ZENROWS_API_KEY or provide cached detail/review files for all selected SKUs")

    def process_target(index, target):
        sku = str(target.get("sku_id") or "").strip()
        fetched_detail = False
        fetched_review = False
        if STAGE in {"all", "detail"}:
            should_fetch_detail = client and (FORCE_REFRESH or not detail_success(sku))
            dmeta = fetch_detail(client, target) if should_fetch_detail else read_json(detail_paths(sku)["meta"])
            fetched_detail = bool(should_fetch_detail)
        else:
            dmeta = read_json(detail_paths(sku)["meta"])
        if STAGE in {"all", "review"}:
            should_fetch_review = client and dmeta.get("success") and (FORCE_REFRESH or not review_success(sku))
            rmeta = fetch_review20(client, target) if should_fetch_review else read_json(review_paths(sku)["meta"])
            fetched_review = bool(should_fetch_review)
        else:
            rmeta = read_json(review_paths(sku)["meta"])
        return index, sku, dmeta, rmeta, fetched_detail, fetched_review

    detail_cost = 0.0
    review_cost = 0.0
    if REBUILD_ONLY:
        print(f"rebuild_only=1 output_targets={len(output_targets)}")
    elif WORKERS > 1 and len(targets) > 1:
        with ThreadPoolExecutor(max_workers=WORKERS) as executor:
            futures = [executor.submit(process_target, index, target) for index, target in enumerate(targets, 1)]
            for future in as_completed(futures):
                index, sku, dmeta, rmeta, fetched_detail, fetched_review = future.result()
                if fetched_detail:
                    detail_cost += float(dmeta.get("x_request_cost") or 0)
                if fetched_review:
                    review_cost += float(rmeta.get("x_request_cost") or 0)
                print(
                    f"[{index}/{len(targets)}] sku={sku} "
                    f"detail={dmeta.get('success')} attempt={dmeta.get('attempt')} "
                    f"review={rmeta.get('success')} attempt={rmeta.get('attempt')} "
                    f"reviews={rmeta.get('review_count_returned', '')}"
                )
    else:
        for index, target in enumerate(targets, 1):
            index, sku, dmeta, rmeta, fetched_detail, fetched_review = process_target(index, target)
            if fetched_detail:
                detail_cost += float(dmeta.get("x_request_cost") or 0)
            if fetched_review:
                review_cost += float(rmeta.get("x_request_cost") or 0)
            print(
                f"[{index}/{len(targets)}] sku={sku} "
                f"detail={dmeta.get('success')} attempt={dmeta.get('attempt')} "
                f"review={rmeta.get('success')} attempt={rmeta.get('attempt')} "
                f"reviews={rmeta.get('review_count_returned', '')}"
            )

    enriched_rows, failures = build_outputs(output_targets)
    write_csv(DETAIL_ROWS_CSV, enriched_rows)
    write_csv(FAILURES_CSV, failures, ["sku_id", "stage", "attempt", "status_code", "error", "retryable"])
    fields = sample_fields()
    for row in enriched_rows:
        for field in fields:
            row.setdefault(field, "")
    final_rows = [{field: row.get(field, "") for field in fields} for row in enriched_rows]
    write_csv(FINAL_OUTPUT_CSV, final_rows, fields)
    product_list_updates = sync_product_list(final_rows)
    benchmark_rows = write_detail_benchmarks(TARGET_CSV, DETAIL_ROOT, DETAIL_BENCHMARKS_CSV)

    manifest = {
        "run_type": "step08_detail_enrichment",
        "started_at": started_at,
        "finished_at": now(),
        "target_csv": rel_path(TARGET_CSV),
        "limit": LIMIT,
        "retry_only": RETRY_ONLY,
        "rebuild_only": REBUILD_ONLY,
        "force_refresh": FORCE_REFRESH,
        "stage": STAGE,
        "workers": WORKERS,
        "max_attempts": MAX_ATTEMPTS,
        "target_count": len(output_targets),
        "processed_count": len(targets),
        "success_count": len(enriched_rows),
        "failure_count": len(failures),
        "detail_cost_usd_this_run": detail_cost,
        "review_cost_usd_this_run": review_cost,
        "total_cost_usd_this_run": detail_cost + review_cost,
        "total_cost_krw_1550_this_run": round((detail_cost + review_cost) * KRW_PER_USD, 2),
        "detail_rows_csv": rel_path(DETAIL_ROWS_CSV),
        "failures_csv": rel_path(FAILURES_CSV),
        "detail_benchmarks_csv": rel_path(DETAIL_BENCHMARKS_CSV),
        "detail_benchmark_rows": len(benchmark_rows),
        "final_output_csv": rel_path(FINAL_OUTPUT_CSV),
        "product_list_csv": rel_path(PRODUCT_LIST_CSV),
        "product_list_updates": product_list_updates,
    }
    MANIFEST_PATH.write_text(json.dumps(manifest, indent=2, ensure_ascii=False), encoding="utf-8")
    print(json.dumps(manifest, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()
