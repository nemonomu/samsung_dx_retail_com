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
            if payload.get("endpoint_url") and payload.get("request_payload"):
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
    operation = {
        "endpoint_url": endpoint_url,
        "request_payload": request_payload,
        "request_headers": normalize_header_mapping(request_headers),
        "cookies": normalize_cookie_mapping(cookies),
        "sample_response_shape": _shape(sample_response),
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
    operation = load_listing_operation(base_dir)
    if not operation:
        return []
    if timeout is None:
        timeout = int(os.environ.get("BBY_LISTING_DIRECT_TIMEOUT", "12"))
    endpoint_url = operation.get("endpoint_url")
    payload = build_listing_payload(operation.get("request_payload"), page_number, page_size)
    headers = _sanitize_headers(operation.get("request_headers") or {})
    headers.setdefault("content-type", "application/json")
    headers.setdefault("accept", "application/graphql-response+json,application/json;q=0.9")
    cookies = normalize_cookie_mapping(operation.get("cookies") or {})
    if cookies:
        headers["Cookie"] = "; ".join(f"{k}={v}" for k, v in cookies.items())
    body = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(endpoint_url, data=body, headers=headers, method="POST")
    try:
        with urllib.request.urlopen(req, timeout=timeout) as response:
            raw = response.read().decode("utf-8", errors="replace")
            parsed = json.loads(raw) if raw else {}
    except urllib.error.HTTPError as exc:
        raw = exc.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"listing GraphQL HTTP {exc.code}: {raw[:300]}") from exc
    except (socket.timeout, TimeoutError) as exc:
        removed = remove_listing_operation(base_dir)
        raise RuntimeError(f"listing GraphQL timeout; removed stale operation files={removed}") from exc
    except Exception as exc:
        removed = remove_listing_operation(base_dir)
        raise RuntimeError(f"listing GraphQL request failed; removed stale operation files={removed}: {exc}") from exc
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
            self.page.listen.start("graphql")
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
            item_map, url_map = extract_sku_map_from_payload(payload)
            product_rows = extract_listing_products_from_payload(payload, page_type="listing")
            if product_rows and isinstance(req_payload, dict):
                endpoint_url = getattr(packet.request, "url", None) or getattr(packet, "url", None)
                headers = getattr(packet.request, "headers", None) or {}
                cookies = {}
                try:
                    for cookie in self.page.cookies():
                        if cookie.get("name"):
                            cookies[cookie.get("name")] = cookie.get("value")
                except Exception:
                    pass
                path = save_listing_operation(self.output_dir, endpoint_url, req_payload, headers, cookies, payload)
                if path:
                    print(f"[INFO] Saved listing GraphQL operation: {req_payload.get('operationName')} -> {path}")
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
