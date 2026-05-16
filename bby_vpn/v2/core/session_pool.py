"""Session cookie/header helpers shared by browser discovery and API collectors."""


def cookies_from_drission_page(page):
    cookies = {}
    try:
        for cookie in page.cookies():
            name = cookie.get("name")
            value = cookie.get("value")
            if name and value is not None:
                cookies[name] = value
    except Exception:
        pass
    return cookies


def minimal_headers_from_packet(packet):
    headers = {}
    try:
        source = getattr(packet.request, "headers", None) or {}
        for key, value in dict(source).items():
            lower = str(key).lower()
            if lower in ("accept", "content-type", "origin", "referer", "user-agent"):
                headers[key] = value
    except Exception:
        pass
    return headers

