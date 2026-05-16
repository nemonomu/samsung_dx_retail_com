"""Best Buy listing GraphQL helpers.

The listing crawlers still use the existing DOM selectors as the canonical
fallback, but Best Buy listing responses often carry the internal numeric
``skuId`` before the product card DOM exposes it.  This module listens to
GraphQL traffic during listing page loads and builds an item -> skuId map.
"""

from __future__ import annotations

import json
import re
import time


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
    text = str(value)
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


class ListingGraphQLSkuCollector:
    def __init__(self, page=None):
        self.page = page
        self.item_to_sku = {}
        self.url_to_sku = {}
        self.packet_count = 0

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
            item_map, url_map = extract_sku_map_from_payload(payload)
            if not item_map and not url_map:
                return False
            self.item_to_sku.update({k: str(v) for k, v in item_map.items() if v})
            self.url_to_sku.update({k: str(v) for k, v in url_map.items() if v})
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
