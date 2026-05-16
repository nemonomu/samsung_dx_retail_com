"""Browser fallback decision helpers."""


CRITICAL_FIELDS = ("retailer_sku_name", "product_url", "item", "final_sku_price")


def needs_browser_fallback(row, api_errors=None, schema_mismatch=False):
    if schema_mismatch:
        return True
    if api_errors:
        return True
    for field in CRITICAL_FIELDS:
        if not row.get(field):
            return True
    return False

