"""Best Buy listing GraphQL helpers.

The listing crawlers still use the existing DOM selectors as the canonical
fallback, but Best Buy listing responses often carry the internal numeric
``skuId`` before the product card DOM exposes it.  This module listens to
GraphQL traffic during listing page loads and builds an item -> skuId map.
"""

from __future__ import annotations

import json
import html as html_lib
import copy
import os
import re
import socket
import time
import urllib.error
import urllib.request
from urllib.parse import urlencode


SKU_KEYS = {"skuId", "skuID", "sku_id"}
URL_KEYS = {
    "pdpUrl",
    "productUrl",
    "product_url",
    "relativePdp",
    "pdp",
    "url",
    "href",
}


def extract_item_from_url(product_url):
    if not product_url:
        return None
    text = str(product_url).split("?", 1)[0].rstrip("/")
    if "/sku/" in text:
        text = text.split("/sku/", 1)[0].rstrip("/")
    parts = text.split("/")
    if "product" in parts:
        idx = parts.index("product")
        if len(parts) > idx + 2:
            return parts[idx + 2] or None
    if "site" in parts:
        tail = parts[-1]
        return tail[:-2] if tail.endswith(".p") else tail
    tail = parts[-1] if parts else None
    return tail[:-2] if tail and tail.endswith(".p") else tail


def extract_sku_from_text(text):
    if not text:
        return None
    patterns = (
        r"/sku/(\d{5,})(?:/|$)",
        r"[?&]skuId=(\d{5,})\b",
        r'"skuId"\s*:\s*"?(\d{5,})"?',
        r"\bskuId[=:]\s*'?\"?(\d{5,})\b",
    )
    for pattern in patterns:
        match = re.search(pattern, str(text), re.IGNORECASE)
        if match:
            return match.group(1)
    return None


def normalize_url(value):
    if not value:
        return None
    text = html_lib.unescape(str(value)).replace("\\/", "/")
    if text.startswith("/"):
        return "https://www.bestbuy.com" + text
    return text


def value_is_product_url(value):
    if not isinstance(value, str):
        return False
    text = str(value or "")
    return "/product/" in text or ("/site/" in text and text.endswith(".p"))


def extract_sku_map_from_payload(payload):
    item_to_sku = {}
    url_to_sku = {}

    def direct_sku_values(node):
        values = []
        if not isinstance(node, dict):
            return values
        for key, value in node.items():
            if key in SKU_KEYS and value:
                match = re.fullmatch(r"\s*(\d{5,})\s*", str(value))
                if match:
                    values.append(match.group(1))
            elif key == "sku" and isinstance(value, str):
                match = re.fullmatch(r"\s*(\d{5,})\s*", value)
                if match:
                    values.append(match.group(1))
        return values

    def product_urls(node):
        urls = []
        if isinstance(node, dict):
            for key, value in node.items():
                if key in URL_KEYS and value_is_product_url(value):
                    urls.append(normalize_url(value))
                urls.extend(product_urls(value))
        elif isinstance(node, list):
            for child in node:
                urls.extend(product_urls(child))
        return urls

    def walk(node):
        if isinstance(node, dict):
            # Pair only records that expose a skuId directly. Pairing all skuIds
            # and all URLs from a large parent response can assign the first sku
            # to every product in the listing.
            skus = direct_sku_values(node)
            urls = product_urls(node) if skus else []
            if skus and urls:
                sku = skus[0]
                for url in urls:
                    item = extract_item_from_url(url)
                    if item:
                        item_to_sku.setdefault(item, sku)
                    url_to_sku.setdefault(url, sku)
            for child in node.values():
                walk(child)
        elif isinstance(node, list):
            for child in node:
                walk(child)

    walk(payload)
    return item_to_sku, url_to_sku


def _direct_text_value(node, keys):
    if not isinstance(node, dict):
        return None
    for key in keys:
        value = node.get(key)
        if isinstance(value, str) and value.strip():
            return value.strip()
        if isinstance(value, dict):
            for nested_key in ("short", "title", "text", "displayName", "name"):
                nested = value.get(nested_key)
                if isinstance(nested, str) and nested.strip():
                    return nested.strip()
    return None


def _direct_price_value(node):
    if not isinstance(node, dict):
        return None
    price = node.get("price")
    if isinstance(price, dict):
        for key in ("displayableCustomerPrice", "currentPrice", "customerPrice", "salePrice"):
            value = price.get(key)
            if value not in (None, ""):
                try:
                    return f"${float(value):,.2f}"
                except Exception:
                    return str(value)
    return None


def _direct_savings_value(node):
    if not isinstance(node, dict):
        return None
    price = node.get("price")
    if isinstance(price, dict):
        value = price.get("totalSavings")
        if value not in (None, ""):
            try:
                return f"${float(value):,.2f}"
            except Exception:
                return str(value)
    return None


def extract_listing_products_from_payload(payload, page_type, page_number=None):
    """Extract listing rows from GraphQL payload product records.

    A row is emitted only from a dict that directly exposes a numeric skuId and
    contains a PDP URL under that same record. This avoids cross-product pairing
    from large response containers.
    """
    rows = []
    seen = set()

    def add_record(node):
        skus = []
        for key, value in node.items():
            if key in SKU_KEYS and value:
                match = re.fullmatch(r"\s*(\d{5,})\s*", str(value))
                if match:
                    skus.append(match.group(1))
            elif key == "sku" and isinstance(value, str):
                match = re.fullmatch(r"\s*(\d{5,})\s*", value)
                if match:
                    skus.append(match.group(1))
        if not skus:
            return
        urls = []
        for key, value in node.items():
            if key in URL_KEYS and value_is_product_url(value):
                urls.append(normalize_url(value))
            elif key in URL_KEYS and isinstance(value, dict):
                urls.extend(product_urls(value))
        if not urls:
            return
        product_url = urls[0]
        item = extract_item_from_url(product_url)
        key = item or skus[0] or product_url
        if not key or key in seen:
            return
        seen.add(key)
        row = {
            "page_type": page_type,
            "retailer_sku_name": _direct_text_value(
                node,
                ("name", "title", "productName", "shortName", "displayName"),
            ),
            "offer": None,
            "pick_up_availability": None,
            "fastest_delivery": None,
            "delivery_availability": None,
            "sku_status": None,
            "product_url": product_url,
            "numeric_sku": skus[0],
            "page_number": page_number,
            "final_sku_price": _direct_price_value(node),
            "savings": _direct_savings_value(node),
        }
        rows.append(row)

    def walk(node):
        if isinstance(node, dict):
            add_record(node)
            for child in node.values():
                walk(child)
        elif isinstance(node, list):
            for child in node:
                walk(child)

    def product_urls(node):
        urls = []
        if isinstance(node, dict):
            for key, value in node.items():
                if key in URL_KEYS and value_is_product_url(value):
                    urls.append(normalize_url(value))
                elif isinstance(value, (dict, list)):
                    urls.extend(product_urls(value))
        elif isinstance(node, list):
            for child in node:
                urls.extend(product_urls(child))
        return urls

    walk(payload)
    return rows


def nested_get(value, path, default=None):
    current = value
    for key in path:
        if not isinstance(current, dict):
            return default
        current = current.get(key)
    return default if current is None else current


def product_row_from_graphql_product(product, page_type, page_number=None):
    if not isinstance(product, dict) or not product.get("skuId"):
        return None
    price = product.get("price") if isinstance(product.get("price"), dict) else {}
    review_info = product.get("reviewInfo") if isinstance(product.get("reviewInfo"), dict) else {}
    product_url = (
        nested_get(product, ["url", "skuSpecificUrl"])
        or nested_get(product, ["url", "pdp"])
        or nested_get(product, ["url", "relativePdp"])
    )
    product_url = normalize_url(product_url)
    if not product_url:
        return None
    return {
        "page_type": page_type,
        "retailer_sku_name": nested_get(product, ["name", "short"]) or product.get("name") or product.get("title"),
        "offer": None,
        "pick_up_availability": nested_get(product, ["fulfillmentOptions", "ispuDetails", "ispuAvailability", "pickupEligible"]),
        "fastest_delivery": None,
        "delivery_availability": nested_get(product, ["fulfillmentOptions", "shippingDetails", "shippingAvailability", "shippingEligible"]),
        "sku_status": product.get("dotComDisplayStatus"),
        "product_url": product_url,
        "numeric_sku": str(product.get("skuId")),
        "page_number": page_number,
        "final_sku_price": price.get("displayableCustomerPrice") or price.get("customerPrice") or price.get("currentPrice"),
        "savings": price.get("totalSavings"),
        "star_rating": normalize_listing_rating(review_info.get("averageRating")),
        "review_count": review_info.get("reviewCount"),
    }


def normalize_listing_rating(value):
    if value in (None, ""):
        return None
    try:
        number = float(value)
    except Exception:
        return value
    if number <= 0:
        return "Not yet reviewed"
    return number


def extract_product_list_rows(payload, page_type, page_number=None):
    data = payload.get("data", {}) if isinstance(payload, dict) else {}
    rows = []
    seen = set()

    def add_product(product):
        row = product_row_from_graphql_product(product, page_type, page_number=page_number)
        if not row:
            return
        key = row.get("numeric_sku") or extract_item_from_url(row.get("product_url"))
        if not key or key in seen:
            return
        seen.add(key)
        rows.append(row)

    documents = nested_get(data, ["detailedProductSearch", "documents"], [])
    if isinstance(documents, list):
        for document in documents:
            product = document.get("product") if isinstance(document, dict) else None
            add_product(product)

    placements = nested_get(data, ["search", "withBestMedia", "placements"], [])
    if isinstance(placements, list):
        for placement in placements:
            if not isinstance(placement, dict):
                continue
            sponsored_documents = nested_get(placement, ["documentsGridView", "sponsoredDocuments"], [])
            if isinstance(sponsored_documents, list):
                for document in sponsored_documents:
                    product = document.get("product") if isinstance(document, dict) else None
                    add_product(product)
            documents = placement.get("documents", [])
            if isinstance(documents, list):
                for document in documents:
                    product = document.get("product") if isinstance(document, dict) else None
                    add_product(product)
    return rows


def extract_listing_products_from_html(html_text, page_type, page_number=None):
    """Extract listing rows from embedded HTML/JS payloads.

    Best Buy listing pages do not always expose anchors in the first rendered
    product-card DOM. The initial HTML/JS payload commonly still contains PDP
    URLs and nearby numeric skuIds, so this is used before scroll-heavy DOM
    fallback.
    """
    if not html_text:
        return []
    text = html_lib.unescape(str(html_text)).replace("\\u002F", "/").replace("\\/", "/")
    url_pattern = re.compile(
        r"(https?://www\.bestbuy\.com)?/(product|site)/[^\"'<>\\\s]+",
        re.IGNORECASE,
    )
    rows = []
    seen = set()
    for match in url_pattern.finditer(text):
        raw_url = match.group(0)
        if "/openbox" in raw_url.lower():
            continue
        product_url = normalize_url(raw_url)
        item = extract_item_from_url(product_url)
        if not item or item.lower() in {"product", "site"}:
            continue
        if item in seen:
            continue
        window = text[max(0, match.start() - 2500): min(len(text), match.end() + 2500)]
        sku = extract_sku_from_text(product_url) or extract_sku_from_text(window)
        if not sku:
            continue
        seen.add(item)
        rows.append({
            "page_type": page_type,
            "retailer_sku_name": extract_name_from_text_window(window),
            "offer": None,
            "pick_up_availability": None,
            "fastest_delivery": None,
            "delivery_availability": None,
            "sku_status": None,
            "product_url": product_url,
            "numeric_sku": sku,
            "page_number": page_number,
            "final_sku_price": None,
            "savings": None,
        })
    return rows


def extract_name_from_text_window(text):
    if not text:
        return None
    patterns = (
        r'"short"\s*:\s*"([^"]{8,220})"',
        r'"title"\s*:\s*"([^"]{8,220})"',
        r'"productName"\s*:\s*"([^"]{8,220})"',
        r'"name"\s*:\s*"([^"]{8,220})"',
    )
    for pattern in patterns:
        match = re.search(pattern, text)
        if match:
            value = html_lib.unescape(match.group(1)).replace('\\"', '"').strip()
            if value and "best buy" not in value.lower():
                return value
    return None


def _listing_registry_path(base_dir):
    return os.path.join(base_dir or ".", "listing_graphql_operation.json")


def _listing_candidates_path(base_dir):
    return os.path.join(base_dir or ".", "listing_graphql_candidates.jsonl")


def _load_dotenv_once():
    if getattr(_load_dotenv_once, "_loaded", False):
        return
    setattr(_load_dotenv_once, "_loaded", True)
    candidates = [
        os.path.join(os.getcwd(), ".env"),
        os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env"),
        os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), ".env"),
    ]
    for path in candidates:
        if not os.path.exists(path):
            continue
        try:
            with open(path, encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if not line or line.startswith("#") or "=" not in line:
                        continue
                    key, value = line.split("=", 1)
                    key = key.strip()
                    value = value.strip().strip('"').strip("'")
                    if key and key not in os.environ:
                        os.environ[key] = value
        except Exception:
            continue


def source_html_candidates(base_dir):
    configured = os.environ.get("BESTBUY_MAIN_SOURCE_HTML") or os.environ.get("BBY_LISTING_SOURCE_HTML")
    candidates = []
    if configured:
        candidates.append(configured)
    candidates.extend([
        os.path.join(base_dir or ".", "bestbuy_main_search_page_sample.html"),
        os.path.join(base_dir or ".", "references", "bestbuy_main_search_page_sample.html"),
        os.path.join(os.path.dirname(os.path.abspath(__file__)), "bestbuy_main_search_page_sample.html"),
        os.path.join(os.path.dirname(os.path.abspath(__file__)), "references", "bestbuy_main_search_page_sample.html"),
    ])
    return candidates


def ensure_listing_operation_from_source_html(base_dir):
    for path in source_html_candidates(base_dir):
        if not path or not os.path.exists(path):
            continue
        try:
            with open(path, encoding="utf-8", errors="replace") as f:
                html_text = f.read()
            saved = save_listing_operation_from_html(base_dir, html_text, cookies={}, headers={})
            if saved:
                print(f"[INFO] Saved Apollo listing GraphQL operation from source HTML {path} -> {saved}")
                return load_listing_operation(base_dir)
        except Exception as exc:
            print(f"[WARNING] Source HTML listing operation load failed: {path}: {exc}")
    return None


def build_search_url(page_number, search_term="tv", sort=""):
    query = {"id": "pcat17071", "st": search_term, "intl": "nosplash"}
    if sort:
        query["sp"] = sort
    if int(page_number or 1) > 1:
        query["cp"] = int(page_number)
    return "https://www.bestbuy.com/site/searchpage.jsp?" + urlencode(query)


def load_listing_operation(base_dir):
    candidates = [
        _listing_registry_path(base_dir),
        _listing_registry_path(os.path.dirname(os.path.abspath(__file__))),
    ]
    for path in candidates:
        if not os.path.exists(path):
            continue
        try:
            with open(path, encoding="utf-8") as f:
                payload = json.load(f)
            if (
                payload.get("endpoint_url")
                and payload.get("request_payload")
                and is_reusable_listing_operation(
                    payload.get("request_payload"),
                    payload.get("sample_response_shape"),
                )
            ):
                return payload
        except Exception:
            continue
    return None


def remove_listing_operation(base_dir):
    removed = []
    for path in {
        _listing_registry_path(base_dir),
        _listing_registry_path(os.path.dirname(os.path.abspath(__file__))),
    }:
        if not path or not os.path.exists(path):
            continue
        try:
            os.remove(path)
            removed.append(path)
        except Exception:
            continue
    return removed


def save_listing_operation(base_dir, endpoint_url, request_payload, request_headers, cookies, sample_response=None):
    if not endpoint_url or not isinstance(request_payload, dict):
        return None
    sample_shape = _shape(sample_response)
    if not is_reusable_listing_operation(request_payload, sample_shape):
        return None
    operation = {
        "endpoint_url": endpoint_url,
        "request_payload": request_payload,
        "request_headers": normalize_header_mapping(request_headers),
        "cookies": normalize_cookie_mapping(cookies),
        "sample_response_shape": sample_shape,
        "updated_at": int(time.time()),
    }
    written = None
    for folder in {base_dir, os.path.dirname(os.path.abspath(__file__))}:
        if not folder:
            continue
        try:
            os.makedirs(folder, exist_ok=True)
            path = _listing_registry_path(folder)
            with open(path, "w", encoding="utf-8") as f:
                json.dump(operation, f, ensure_ascii=False, indent=2, default=str)
            written = path
        except Exception:
            continue
    return written


def save_listing_operation_from_html(base_dir, html_text, cookies=None, headers=None):
    operation = find_started_operation(html_text, "PlpView_ProductList_Init")
    if not operation:
        write_apollo_operation_diagnostics(base_dir, html_text, "PlpView_ProductList_Init")
        return None
    return save_listing_operation(
        base_dir,
        os.environ.get("BBY_GRAPHQL_ENDPOINT", "https://www.bestbuy.com/gateway/graphql"),
        operation,
        headers or {},
        cookies or {},
        {"data": {"detailedProductSearch": {"documents": []}}},
    )


def operation_name(query):
    if not isinstance(query, str):
        return ""
    match = re.search(r"\bquery\s+([A-Za-z0-9_]+)", query)
    return match.group(1) if match else ""


def extract_apollo_payloads(html_text):
    html_text = str(html_text or "")
    payloads = []
    if not html_text or "ApolloSSRDataTransport" not in html_text:
        return payloads
    pos = 0
    while True:
        marker = html_text.find(".push(", pos)
        if marker < 0:
            break
        if "ApolloSSRDataTransport" not in html_text[max(0, marker - 120): marker + 120]:
            pos = marker + 6
            continue
        start = marker + 6
        depth = 0
        in_string = False
        escape = False
        end = None
        for idx in range(start, len(html_text)):
            char = html_text[idx]
            if in_string:
                if escape:
                    escape = False
                elif char == "\\":
                    escape = True
                elif char == '"':
                    in_string = False
            else:
                if char == '"':
                    in_string = True
                elif char in "[{":
                    depth += 1
                elif char in "]}":
                    depth -= 1
                    if depth == 0:
                        end = idx + 1
                        break
        if end is None:
            break
        raw_payload = html_text[start:end]
        normalized = re.sub(r":\s*undefined(?=[,}])", ":null", raw_payload).replace("undefined", "null")
        try:
            payloads.append(json.loads(normalized))
        except Exception:
            pass
        pos = end
    return payloads


def write_apollo_operation_diagnostics(base_dir, html_text, target_name):
    payloads = extract_apollo_payloads(html_text)
    operations = []
    for payload in payloads:
        events = payload.get("events", []) if isinstance(payload, dict) else []
        for event in events:
            if not isinstance(event, dict):
                continue
            options = event.get("options", {})
            query = options.get("query", "") if isinstance(options, dict) else ""
            name = operation_name(query)
            if name:
                operations.append(name)
    record = {
        "ts": int(time.time()),
        "target": target_name,
        "html_length": len(str(html_text or "")),
        "has_apollo_marker": "ApolloSSRDataTransport" in str(html_text or ""),
        "payload_count": len(payloads),
        "operations": sorted(set(operations)),
    }
    written = None
    for folder in {base_dir, os.path.dirname(os.path.abspath(__file__))}:
        if not folder:
            continue
        try:
            os.makedirs(folder, exist_ok=True)
            path = os.path.join(folder, "listing_apollo_diagnostics.jsonl")
            with open(path, "a", encoding="utf-8") as f:
                f.write(json.dumps(record, ensure_ascii=False, default=str) + "\n")
            written = path
        except Exception:
            continue
    if written:
        print(
            "[INFO] Apollo listing operation not found: "
            f"target={target_name} marker={record['has_apollo_marker']} "
            f"payloads={record['payload_count']} operations={record['operations'][:10]} -> {written}"
        )
    return written


def find_started_operation(html_text, target_name):
    for payload in extract_apollo_payloads(html_text):
        events = payload.get("events", []) if isinstance(payload, dict) else []
        for event in events:
            if not isinstance(event, dict) or event.get("type") != "started":
                continue
            options = event.get("options", {})
            if not isinstance(options, dict):
                continue
            query = options.get("query", "")
            if operation_name(query) != target_name:
                continue
            return {
                "operationName": target_name,
                "query": query,
                "variables": options.get("variables", {}),
            }
    return None


def append_listing_candidate(base_dir, endpoint_url, request_payload, response_payload, request_headers=None):
    if not isinstance(request_payload, dict):
        return None
    variables = request_payload.get("variables")
    response_shape = _shape(response_payload)
    candidate = {
        "ts": int(time.time()),
        "endpoint_url": endpoint_url,
        "operationName": request_payload.get("operationName"),
        "variable_keys": sorted(variables.keys()) if isinstance(variables, dict) else [],
        "has_paging_key": _has_paging_key(variables),
        "reusable_listing_operation": is_reusable_listing_operation(request_payload, response_shape),
        "product_url_count": count_product_urls(response_payload),
        "sku_count": count_sku_values(response_payload),
        "response_root_keys": sorted(response_payload.keys()) if isinstance(response_payload, dict) else [],
        "request_header_keys": sorted(normalize_header_mapping(request_headers).keys()),
        "sample_response_shape": response_shape,
    }
    written = None
    for folder in {base_dir, os.path.dirname(os.path.abspath(__file__))}:
        if not folder:
            continue
        try:
            os.makedirs(folder, exist_ok=True)
            path = _listing_candidates_path(folder)
            with open(path, "a", encoding="utf-8") as f:
                f.write(json.dumps(candidate, ensure_ascii=False, default=str) + "\n")
            written = path
        except Exception:
            continue
    return written


def is_reusable_listing_operation(request_payload, response_shape=None):
    if not isinstance(request_payload, dict):
        return False
    operation_name = str(request_payload.get("operationName") or "")
    # Only the Apollo product-list operation is reusable for page-level listing
    # replay. Header/footer/config/intent/recommendation/card/price operations
    # can expose paging-like keys or product URLs, but they are not the 24-item
    # search listing contract.
    if operation_name == "PlpView_ProductList_Init":
        return True
    return False


def count_product_urls(value):
    seen = set()

    def walk(node):
        if isinstance(node, dict):
            for key, child in node.items():
                if key in URL_KEYS and value_is_product_url(child):
                    seen.add(normalize_url(child))
                else:
                    walk(child)
        elif isinstance(node, list):
            for child in node:
                walk(child)

    walk(value)
    return len(seen)


def count_sku_values(value):
    seen = set()

    def walk(node):
        if isinstance(node, dict):
            for key, child in node.items():
                if key in SKU_KEYS or key == "sku":
                    match = re.fullmatch(r"\s*(\d{5,})\s*", str(child or ""))
                    if match:
                        seen.add(match.group(1))
                else:
                    walk(child)
        elif isinstance(node, list):
            for child in node:
                walk(child)

    walk(value)
    return len(seen)


def _has_paging_key(value):
    if isinstance(value, dict):
        for key, child in value.items():
            lower = str(key).lower()
            if lower in {
                "page",
                "pagenumber",
                "currentpage",
                "cp",
                "pagesize",
                "page_size",
                "nrp",
                "rows",
                "count",
                "limit",
                "offset",
            }:
                return True
            if _has_paging_key(child):
                return True
    elif isinstance(value, list):
        return any(_has_paging_key(child) for child in value)
    return False


def _shape(value, depth=0):
    if depth >= 4:
        return type(value).__name__
    if isinstance(value, dict):
        return {str(k): _shape(v, depth + 1) for k, v in list(value.items())[:30]}
    if isinstance(value, list):
        return [_shape(value[0], depth + 1)] if value else []
    return type(value).__name__


def build_listing_payload(template, page_number, page_size=24):
    payload = copy.deepcopy(template or {})
    variables = payload.get("variables")
    if isinstance(variables, dict):
        search_term = os.environ.get("BESTBUY_SEARCH_TERM", os.environ.get("BBY_LISTING_SEARCH_TERM", "tv"))
        sort = os.environ.get("BESTBUY_SEARCH_SORT", os.environ.get("BBY_LISTING_SEARCH_SORT", ""))
        organic_offset = int(os.environ.get("BESTBUY_MAIN_ORGANIC_OFFSET", os.environ.get("BBY_LISTING_ORGANIC_OFFSET", "18")))
        for key in ("input", "detailedSearchInput"):
            if isinstance(variables.get(key), dict):
                variables[key]["query"] = search_term
                variables[key]["queryType"] = "SEARCH"
                variables[key]["site"] = "WWW"
        variables["categoryId"] = search_term
        variables["isBrowse"] = False
        if isinstance(variables.get("sort"), dict):
            variables["sort"]["sort"] = sort
        elif "sort" in variables:
            variables["sort"] = {"sort": sort}
        for key in ("pagination", "paginationForDetailedProductSearch"):
            if isinstance(variables.get(key), dict):
                variables[key]["pageNumber"] = page_number
                variables[key]["offset"] = organic_offset
        _set_first_existing(variables, ("page", "pageNumber", "currentPage", "cp"), page_number)
        _set_first_existing(variables, ("pageSize", "page_size", "nrp", "rows", "count", "limit"), page_size)
        _walk_mutate_paging(variables, page_number, page_size)
    query = payload.get("query")
    if isinstance(query, str):
        query = re.sub(r"pageSize:\s*\d+", f"pageSize: {page_size}", query)
        query = re.sub(r"first:\s*\d+", f"first: {page_size}", query)
        payload["query"] = query
    return payload


def _set_first_existing(mapping, keys, value):
    for key in keys:
        if key in mapping:
            mapping[key] = value
            return True
    return False


def _walk_mutate_paging(value, page_number, page_size):
    if isinstance(value, dict):
        for key in list(value.keys()):
            lower = str(key).lower()
            if lower in {"page", "pagenumber", "currentpage", "cp"}:
                value[key] = page_number
            elif lower in {"pagesize", "page_size", "nrp", "rows", "count", "limit"}:
                value[key] = page_size
            else:
                _walk_mutate_paging(value[key], page_number, page_size)
    elif isinstance(value, list):
        for child in value:
            _walk_mutate_paging(child, page_number, page_size)


def direct_listing_products(base_dir, page_type, page_number, defaults=None, page_size=24, timeout=None):
    _load_dotenv_once()
    operation = load_listing_operation(base_dir)
    if not operation:
        operation = ensure_listing_operation_from_source_html(base_dir)
    if not operation:
        return []
    if timeout is None:
        timeout = int(os.environ.get("BBY_LISTING_DIRECT_TIMEOUT", "12"))
    endpoint_url = operation.get("endpoint_url")
    payload = build_listing_payload(operation.get("request_payload"), page_number, page_size)
    headers = _sanitize_headers(operation.get("request_headers") or {})
    headers.setdefault("content-type", "application/json")
    headers.setdefault("accept", "application/graphql-response+json,application/json;q=0.9")
    headers.setdefault("origin", "https://www.bestbuy.com")
    headers.setdefault(
        "referer",
        build_search_url(
            page_number,
            os.environ.get("BESTBUY_SEARCH_TERM", os.environ.get("BBY_LISTING_SEARCH_TERM", "tv")),
            os.environ.get("BESTBUY_SEARCH_SORT", os.environ.get("BBY_LISTING_SEARCH_SORT", "")),
        ),
    )
    cookies = normalize_cookie_mapping(operation.get("cookies") or {})
    if cookies:
        headers["Cookie"] = "; ".join(f"{k}={v}" for k, v in cookies.items())
    if os.environ.get("ZENROWS_API_KEY") and os.environ.get("BBY_LISTING_USE_ZENROWS", "1").strip().lower() not in {"0", "false", "no"}:
        parsed = post_listing_graphql_via_zenrows(endpoint_url, payload, headers, timeout)
    else:
        parsed = post_listing_graphql_direct(endpoint_url, payload, headers, timeout, base_dir)
    rows = extract_product_list_rows(parsed, page_type, page_number=page_number)
    if not rows:
        rows = extract_listing_products_from_payload(parsed, page_type, page_number=page_number)
    merged_rows = []
    defaults = dict(defaults or {})
    for row in rows:
        merged = dict(defaults)
        merged.update(row)
        merged["page_type"] = page_type
        merged["page_number"] = page_number
        merged_rows.append(merged)
    return merged_rows


def post_listing_graphql_direct(endpoint_url, payload, headers, timeout, base_dir):
    body = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(endpoint_url, data=body, headers=headers, method="POST")
    try:
        with urllib.request.urlopen(req, timeout=timeout) as response:
            raw = response.read().decode("utf-8", errors="replace")
            return json.loads(raw) if raw else {}
    except urllib.error.HTTPError as exc:
        raw = exc.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"listing GraphQL HTTP {exc.code}: {raw[:300]}") from exc
    except (socket.timeout, TimeoutError) as exc:
        if os.environ.get("BBY_LISTING_REMOVE_STALE_OPERATION", "0").strip().lower() in {"1", "true", "yes"}:
            removed = remove_listing_operation(base_dir)
            raise RuntimeError(f"listing GraphQL timeout; removed stale operation files={removed}") from exc
        raise RuntimeError(f"listing GraphQL timeout after {timeout}s; operation file kept") from exc
    except Exception as exc:
        if os.environ.get("BBY_LISTING_REMOVE_STALE_OPERATION", "0").strip().lower() in {"1", "true", "yes"}:
            removed = remove_listing_operation(base_dir)
            raise RuntimeError(f"listing GraphQL request failed; removed stale operation files={removed}: {exc}") from exc
        raise RuntimeError(f"listing GraphQL request failed; operation file kept: {exc}") from exc


def post_listing_graphql_via_zenrows(endpoint_url, payload, headers, timeout):
    try:
        from zenrows import ZenRowsClient
    except Exception as exc:
        raise RuntimeError("ZENROWS_API_KEY is set but zenrows package is not installed") from exc
    api_key = os.environ.get("ZENROWS_API_KEY")
    if not api_key:
        raise RuntimeError("ZENROWS_API_KEY is required for ZenRows listing GraphQL replay")
    params = {"custom_headers": "true"}
    if os.environ.get("BESTBUY_GRAPHQL_PREMIUM_PROXY", "1").strip().lower() in {"1", "true", "yes"}:
        params["premium_proxy"] = "true"
        params["proxy_country"] = "us"
    if os.environ.get("BESTBUY_GRAPHQL_JS_RENDER", "1").strip().lower() in {"1", "true", "yes"}:
        params["js_render"] = "true"
    if os.environ.get("BESTBUY_GRAPHQL_MODE_AUTO", "0").strip().lower() in {"1", "true", "yes"}:
        params["mode"] = "auto"
        params["proxy_country"] = "us"
    response = ZenRowsClient(api_key).post(
        endpoint_url,
        params=params,
        headers=headers,
        data=json.dumps(payload),
        timeout=timeout,
    )
    if getattr(response, "status_code", 0) != 200:
        raise RuntimeError(f"ZenRows listing GraphQL HTTP {response.status_code}: {getattr(response, 'text', '')[:300]}")
    try:
        return response.json()
    except Exception as exc:
        raise RuntimeError(f"ZenRows listing GraphQL returned non-JSON: {getattr(response, 'text', '')[:300]}") from exc


def _sanitize_headers(headers):
    skipped = {"accept-encoding", "content-length", "cookie", "host", "connection"}
    clean = {}
    for key, value in normalize_header_mapping(headers).items():
        if not key or value in (None, ""):
            continue
        if str(key).lower() in skipped or str(key).startswith(":"):
            continue
        clean[str(key)] = str(value)
    return clean


def normalize_header_mapping(headers):
    if isinstance(headers, dict):
        return headers
    if isinstance(headers, list):
        result = {}
        for item in headers:
            if isinstance(item, dict):
                key = item.get("name") or item.get("key")
                value = item.get("value")
                if key and value is not None:
                    result[key] = value
        return result
    if isinstance(headers, str):
        result = {}
        for line in headers.splitlines():
            if ":" not in line:
                continue
            key, value = line.split(":", 1)
            if key.strip():
                result[key.strip()] = value.strip()
        return result
    return {}


def normalize_cookie_mapping(cookies):
    if isinstance(cookies, dict):
        return cookies
    if isinstance(cookies, list):
        result = {}
        for item in cookies:
            if isinstance(item, dict):
                key = item.get("name") or item.get("key")
                value = item.get("value")
                if key and value is not None:
                    result[key] = value
        return result
    if isinstance(cookies, str):
        result = {}
        for part in cookies.split(";"):
            if "=" not in part:
                continue
            key, value = part.split("=", 1)
            if key.strip():
                result[key.strip()] = value.strip()
        return result
    return {}


class ListingGraphQLSkuCollector:
    def __init__(self, page=None, output_dir=None):
        self.page = page
        self.output_dir = output_dir or os.environ.get("BBY_OUTPUT_DIR") or os.path.dirname(os.path.abspath(__file__))
        self.item_to_sku = {}
        self.url_to_sku = {}
        self.sku_to_url = {}
        self.products = []
        self._product_keys = set()
        self.packet_count = 0

    def _remember_product_row(self, row):
        key = extract_item_from_url(row.get("product_url")) or row.get("numeric_sku")
        if key and key not in self._product_keys:
            self._product_keys.add(key)
            self.products.append(row)
            return True
        return False

    def start(self, page=None):
        self.page = page or self.page
        if not self.page:
            return False
        try:
            targets = [
                target.strip()
                for target in os.environ.get("BBY_LISTING_LISTEN_TARGETS", "graphql").split(",")
                if target.strip()
            ]
            self.page.listen.start(targets[0] if len(targets) == 1 else targets)
            return True
        except Exception as exc:
            print(f"[WARNING] Listing GraphQL listen start failed: {exc}")
            return False

    def stop(self):
        try:
            if self.page:
                self.page.listen.stop()
        except Exception:
            pass

    def drain(self, seconds=3.0):
        if not self.page:
            return 0
        captured = 0
        deadline = time.time() + seconds
        while time.time() < deadline:
            try:
                packet = self.page.listen.wait(timeout=0.4)
            except Exception:
                break
            if not packet:
                continue
            if self.record_packet(packet):
                captured += 1
        if captured:
            print(f"[INFO] Listing GraphQL sku map captured packets={captured} items={len(self.item_to_sku)}")
        return captured

    def record_packet(self, packet):
        try:
            body = getattr(packet.response, "body", None)
            if not body:
                return False
            if isinstance(body, str):
                payload = json.loads(body)
            else:
                payload = body
            req_body = None
            try:
                for attr in ("body", "postData", "data"):
                    value = getattr(packet.request, attr, None)
                    if value:
                        req_body = value
                        break
                req_payload = json.loads(req_body) if isinstance(req_body, str) else req_body
            except Exception:
                req_payload = None
            endpoint_url = getattr(packet.request, "url", None) or getattr(packet, "url", None)
            headers = getattr(packet.request, "headers", None) or {}
            if isinstance(req_payload, dict):
                append_listing_candidate(self.output_dir, endpoint_url, req_payload, payload, headers)
            item_map, url_map = extract_sku_map_from_payload(payload)
            product_rows = extract_listing_products_from_payload(payload, page_type="listing")
            if isinstance(req_payload, dict):
                cookies = {}
                try:
                    for cookie in self.page.cookies():
                        if cookie.get("name"):
                            cookies[cookie.get("name")] = cookie.get("value")
                except Exception:
                    pass
                path = save_listing_operation(self.output_dir, endpoint_url, req_payload, headers, cookies, payload)
                if path:
                    print(
                        "[INFO] Saved reusable listing GraphQL operation: "
                        f"{req_payload.get('operationName')} urls={count_product_urls(payload)} "
                        f"skus={count_sku_values(payload)} -> {path}"
                    )
            for row in product_rows:
                self._remember_product_row(row)
            for url, sku in url_map.items():
                self._remember_product_row({
                    "page_type": "listing",
                    "retailer_sku_name": None,
                    "offer": None,
                    "pick_up_availability": None,
                    "fastest_delivery": None,
                    "delivery_availability": None,
                    "sku_status": None,
                    "product_url": url,
                    "numeric_sku": str(sku),
                    "page_number": None,
                    "final_sku_price": None,
                    "savings": None,
                })
            if not item_map and not url_map and not product_rows:
                return False
            self.item_to_sku.update({k: str(v) for k, v in item_map.items() if v})
            self.url_to_sku.update({k: str(v) for k, v in url_map.items() if v})
            for url, sku in url_map.items():
                if url and sku:
                    self.sku_to_url.setdefault(str(sku), url)
            self.packet_count += 1
            return True
        except Exception:
            return False

    def resolve(self, product_url):
        item = extract_item_from_url(product_url)
        if item and item in self.item_to_sku:
            return self.item_to_sku[item]
        normalized = normalize_url(product_url)
        if normalized in self.url_to_sku:
            return self.url_to_sku[normalized]
        return extract_sku_from_text(product_url)

    def resolve_url(self, numeric_sku):
        if not numeric_sku:
            return None
        return self.sku_to_url.get(str(numeric_sku))

    def listing_products(self, page_type, page_number=None, defaults=None):
        rows = []
        defaults = dict(defaults or {})
        for row in self.products:
            merged = dict(defaults)
            merged.update(row)
            merged["page_type"] = page_type
            merged["page_number"] = page_number
            rows.append(merged)
        return rows

    def apply(self, products):
        filled = 0
        for product in products or []:
            if product.get("numeric_sku"):
                continue
            sku = self.resolve(product.get("product_url"))
            if sku:
                product["numeric_sku"] = sku
                filled += 1
        if filled:
            print(f"[INFO] Filled numeric_sku from listing GraphQL: {filled}")
        return filled

    def apply_by_order(self, products):
        filled = 0
        rows = self.products
        row_index = 0
        used_items = {
            extract_item_from_url(product.get("product_url"))
            for product in (products or [])
            if product.get("product_url")
        }
        used_items.discard(None)
        for idx, product in enumerate(products or []):
            if product.get("product_url"):
                if not product.get("numeric_sku"):
                    sku = self.resolve(product.get("product_url"))
                    if sku:
                        product["numeric_sku"] = sku
                continue
            while row_index < len(rows):
                candidate = rows[row_index]
                row_index += 1
                candidate_item = extract_item_from_url(candidate.get("product_url"))
                if candidate_item and candidate_item in used_items:
                    continue
                row = candidate
                break
            else:
                break
            if row.get("product_url"):
                product["product_url"] = row.get("product_url")
                candidate_item = extract_item_from_url(row.get("product_url"))
                if candidate_item:
                    used_items.add(candidate_item)
                filled += 1
            if not product.get("numeric_sku") and row.get("numeric_sku"):
                product["numeric_sku"] = row.get("numeric_sku")
            if not product.get("retailer_sku_name") and row.get("retailer_sku_name"):
                product["retailer_sku_name"] = row.get("retailer_sku_name")
        if filled:
            print(f"[INFO] Filled product_url from listing GraphQL order: {filled}")
        return filled
