"""Product fact parsing from GraphQL payloads."""


def first_value(payload, candidate_keys):
    found = None

    def walk(value):
        nonlocal found
        if found is not None:
            return
        if isinstance(value, dict):
            for key in candidate_keys:
                if key in value and value[key] not in (None, ""):
                    found = value[key]
                    return
            for child in value.values():
                walk(child)
        elif isinstance(value, list):
            for child in value:
                walk(child)

    walk(payload)
    return found


def parse_product_facts(payload):
    price = first_value(payload, ("currentPrice", "customerPrice", "price", "salePrice"))
    if price and not str(price).startswith("$"):
        price = f"${price}"
    return {
        "retailer_sku_name": first_value(payload, ("name", "title", "productName")),
        "final_sku_price": price,
        "star_rating": first_value(payload, ("ratingValue", "averageRating", "starRating")),
        "count_of_reviews": first_value(payload, ("reviewCount", "totalReviewCount", "reviewsCount")),
        "sku": first_value(payload, ("modelNumber", "model", "sku")),
    }

