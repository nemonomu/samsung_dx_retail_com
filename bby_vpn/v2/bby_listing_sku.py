"""Helpers for extracting Best Buy numeric skuId from listing cards."""

import re


NUMERIC_SKU_PATTERNS = (
    r"[?&]skuId=(\d{5,})\b",
    r"/sku/(\d{5,})(?:/|$)",
    r"/(\d{5,})\.p(?:[?#]|$)",
    r'"skuId"\s*:\s*"?(\d{5,})"?',
    r"\bskuId[=:]\s*'?\"?(\d{5,})\b",
)


def extract_numeric_sku_from_text(text):
    if not text:
        return None
    for pattern in NUMERIC_SKU_PATTERNS:
        match = re.search(pattern, str(text), re.IGNORECASE)
        if match:
            return match.group(1)
    return None


def extract_numeric_sku(card=None, product_url=None):
    sku = extract_numeric_sku_from_text(product_url)
    if sku:
        return sku
    if card is None:
        return None

    attr_xpaths = (
        ".//@data-sku-id",
        ".//@data-skuid",
        ".//@data-sku",
        ".//@sku-id",
        ".//@sku",
        ".//@href",
        ".//@data-url",
    )
    for xpath in attr_xpaths:
        try:
            for value in card.xpath(xpath):
                sku = extract_numeric_sku_from_text(value)
                if sku:
                    return sku
                if xpath.lower().endswith(("sku", "sku-id", "skuid")):
                    digits = re.fullmatch(r"\s*(\d{5,})\s*", str(value))
                    if digits:
                        return digits.group(1)
        except Exception:
            continue
    return None


def extract_sponsored_status(card=None, raw_status=None):
    texts = [raw_status or ""]
    if card is not None:
        try:
            texts.append(card.text_content())
        except Exception:
            pass
        for xpath in (".//@aria-label", ".//@title", ".//@data-testid", ".//@class"):
            try:
                texts.extend(str(value) for value in card.xpath(xpath))
            except Exception:
                continue
    combined = " ".join(texts).lower()
    return "Sponsored" if "sponsored" in combined else None
