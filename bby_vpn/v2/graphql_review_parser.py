"""Review parsing from GraphQL payloads."""

import re


def is_valid_review_text(text):
    if not text:
        return False
    clean = re.sub(r"\s+", " ", str(text)).strip()
    if len(clean) < 40:
        return False
    lower = clean.lower()
    banned = (
        "rating ",
        "out of 5 stars with",
        "would recommend to a friend",
        "verified that this content was written",
    )
    return not any(item in lower for item in banned)


def collect_reviews(payload, max_reviews=20):
    reviews = []
    seen = set()
    cursors = []

    def walk(value):
        if isinstance(value, dict):
            cursor = value.get("cursor") or value.get("endCursor")
            if cursor:
                cursors.append(cursor)
            for key in ("reviewText", "text", "comment", "body", "content", "description"):
                text = value.get(key)
                if isinstance(text, str):
                    clean = re.sub(r"\s+", " ", text).strip()
                    if is_valid_review_text(clean) and clean not in seen:
                        seen.add(clean)
                        reviews.append(clean)
            for child in value.values():
                if len(reviews) >= max_reviews:
                    break
                walk(child)
        elif isinstance(value, list):
            for child in value:
                if len(reviews) >= max_reviews:
                    break
                walk(child)

    walk(payload)
    formatted = " ||| ".join(f"review{i} - {text}" for i, text in enumerate(reviews[:max_reviews], 1))
    return {"reviews": formatted or None, "count": len(reviews[:max_reviews]), "cursors": cursors}

