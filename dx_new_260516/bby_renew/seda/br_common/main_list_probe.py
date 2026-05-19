import csv
import html
import json
import os
import re
import time
from datetime import datetime
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

from .config import rel_path, url_for_page


DEFAULT_HEADERS = {
    "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "accept-language": "pt-BR,pt;q=0.9,en-US;q=0.7,en;q=0.6",
    "cache-control": "no-cache",
    "pragma": "no-cache",
    "sec-ch-ua": '"Chromium";v="125", "Google Chrome";v="125", "Not.A/Brand";v="24"',
    "sec-ch-ua-mobile": "?0",
    "sec-ch-ua-platform": '"Windows"',
    "sec-fetch-dest": "document",
    "sec-fetch-mode": "navigate",
    "sec-fetch-site": "none",
    "sec-fetch-user": "?1",
    "upgrade-insecure-requests": "1",
    "user-agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/125.0.0.0 Safari/537.36"
    ),
}


def now():
    return datetime.now().isoformat(timespec="seconds")


def ensure_dirs(subject_root):
    for child in ("raw", "parsed", "logs"):
        (subject_root / child).mkdir(parents=True, exist_ok=True)


def env_headers(retailer_key):
    headers = dict(DEFAULT_HEADERS)
    variant = os.getenv(f"{retailer_key}_REQUEST_VARIANT", "default").strip().lower()
    if variant in {"magalu_browser", "browser", "browser_headers"}:
        headers.update(
            {
                "accept": (
                    "text/html,application/xhtml+xml,application/xml;q=0.9,"
                    "image/avif,image/webp,image/apng,*/*;q=0.8,"
                    "application/signed-exchange;v=b3;q=0.7"
                ),
                "priority": "u=0, i",
            }
        )
    referer = os.getenv(f"{retailer_key}_REFERER", "").strip()
    if referer:
        headers["referer"] = referer
        headers["sec-fetch-site"] = "same-origin"
    cookie = os.getenv(f"{retailer_key}_COOKIE", "").strip()
    if cookie:
        headers["cookie"] = cookie
    extra_raw = os.getenv(f"{retailer_key}_HEADERS_JSON", "").strip()
    if extra_raw:
        try:
            extra = json.loads(extra_raw)
            if isinstance(extra, dict):
                headers.update({str(key): str(value) for key, value in extra.items()})
        except ValueError:
            pass
    return headers


def fetch_url(url, timeout, retailer_key):
    started = time.monotonic()
    request = Request(url, headers=env_headers(retailer_key))
    try:
        with urlopen(request, timeout=timeout) as response:
            body = response.read()
            return {
                "success": True,
                "status_code": getattr(response, "status", 0),
                "elapsed_seconds": round(time.monotonic() - started, 3),
                "body": body,
                "headers": dict(response.headers.items()),
                "error": "",
            }
    except HTTPError as exc:
        body = exc.read()
        return {
            "success": False,
            "status_code": exc.code,
            "elapsed_seconds": round(time.monotonic() - started, 3),
            "body": body,
            "headers": dict(exc.headers.items()) if exc.headers else {},
            "error": body[:1000].decode("utf-8", errors="replace"),
        }
    except (TimeoutError, URLError, OSError) as exc:
        return {
            "success": False,
            "status_code": 0,
            "elapsed_seconds": round(time.monotonic() - started, 3),
            "body": b"",
            "headers": {},
            "error": f"{type(exc).__name__}: {exc}",
        }


def text_from_body(body, headers):
    content_type = str(headers.get("Content-Type") or headers.get("content-type") or "")
    match = re.search(r"charset=([^;\s]+)", content_type, re.I)
    encoding = match.group(1) if match else "utf-8"
    return body.decode(encoding, errors="replace")


def compact_text(value):
    return re.sub(r"\s+", " ", html.unescape(str(value or ""))).strip()


def first_value(obj, keys):
    lowered = {str(key).lower(): value for key, value in obj.items()}
    for key in keys:
        value = lowered.get(key.lower())
        if value not in (None, "", [], {}):
            return value
    return ""


def walk_json(value):
    if isinstance(value, dict):
        yield value
        for child in value.values():
            yield from walk_json(child)
    elif isinstance(value, list):
        for child in value:
            yield from walk_json(child)


def normalize_price(value):
    if isinstance(value, dict):
        value = first_value(value, ["price", "value", "amount", "bestPrice", "salePrice"])
    return compact_text(value)


def product_from_dict(obj, page, rank):
    name = first_value(obj, ["name", "title", "productName", "description"])
    url = first_value(obj, ["url", "href", "link", "productUrl", "canonical"])
    sku = first_value(obj, ["sku", "id", "productId", "itemId", "sellerSku"])
    price = first_value(obj, ["price", "priceSpecification", "offers", "bestPrice", "salePrice"])
    if not name or not (url or sku):
        return None
    return {
        "page_number": page,
        "rank": rank,
        "sku_id": compact_text(sku),
        "retailer_sku_name": compact_text(name),
        "product_url": compact_text(url),
        "final_sku_price": normalize_price(price),
        "source": "json",
    }


def parse_json_candidates(text, page):
    candidates = []
    scripts = re.findall(
        r'<script[^>]+type=["\']application/ld\+json["\'][^>]*>(.*?)</script>',
        text,
        flags=re.I | re.S,
    )
    next_data = re.search(r'<script[^>]+id=["\']__NEXT_DATA__["\'][^>]*>(.*?)</script>', text, flags=re.I | re.S)
    if next_data:
        scripts.append(next_data.group(1))
    for script in scripts:
        try:
            payload = json.loads(html.unescape(script).strip())
        except ValueError:
            continue
        for obj in walk_json(payload):
            row = product_from_dict(obj, page, len(candidates) + 1)
            if row:
                candidates.append(row)
    return dedupe(candidates)


def parse_anchor_candidates(text, page):
    rows = []
    pattern = re.compile(r'<a\b[^>]*href=["\']([^"\']+)["\'][^>]*>(.*?)</a>', re.I | re.S)
    for href, inner in pattern.findall(text):
        label = compact_text(re.sub(r"<[^>]+>", " ", inner))
        if len(label) < 12:
            continue
        lower_href = href.lower()
        if not any(token in lower_href for token in ("/p/", "/produto/", "product")):
            continue
        rows.append(
            {
                "page_number": page,
                "rank": len(rows) + 1,
                "sku_id": "",
                "retailer_sku_name": label[:500],
                "product_url": href,
                "final_sku_price": "",
                "source": "anchor",
            }
        )
    return dedupe(rows)


def dedupe(rows):
    seen = set()
    output = []
    for row in rows:
        key = (row.get("product_url") or "", row.get("sku_id") or "", row.get("retailer_sku_name") or "")
        if key in seen:
            continue
        seen.add(key)
        row = dict(row)
        row["rank"] = len(output) + 1
        output.append(row)
    return output


def parse_products(text, page):
    rows = parse_json_candidates(text, page)
    if rows:
        return rows
    return parse_anchor_candidates(text, page)


def write_csv(path, rows):
    fields = [
        "page_number",
        "rank",
        "sku_id",
        "retailer_sku_name",
        "product_url",
        "final_sku_price",
        "source",
    ]
    with Path(path).open("w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        for row in rows:
            writer.writerow({field: row.get(field, "") for field in fields})


def run_main_list(
    retailer_key,
    product_type,
    run_date,
    run_root,
    run_id,
    url_template,
    pages=None,
    timeout=None,
):
    started_at = now()
    run_root = Path(run_root)
    subject_root = run_root / run_id
    ensure_dirs(subject_root)
    page_count = int(pages or os.getenv(f"{retailer_key}_MAIN_PAGES", "1"))
    timeout = int(timeout or os.getenv(f"{retailer_key}_HTTP_TIMEOUT", "30"))
    all_rows = []
    fetches = []

    for page in range(1, page_count + 1):
        url = url_for_page(url_template, page)
        result = fetch_url(url, timeout, retailer_key)
        raw_path = subject_root / "raw" / f"main_page_{page:03d}.html"
        raw_path.write_bytes(result["body"])
        text = text_from_body(result["body"], result["headers"]) if result["body"] else ""
        rows = parse_products(text, page) if result["success"] else []
        all_rows.extend(rows)
        fetches.append(
            {
                "page": page,
                "url": url,
                "status_code": result["status_code"],
                "success": result["success"],
                "elapsed_seconds": result["elapsed_seconds"],
                "raw_path": rel_path(raw_path),
                "bytes": len(result["body"]),
                "parsed_rows": len(rows),
                "error": result["error"],
            }
        )

    parsed_path = subject_root / "parsed" / "main_occurrences.csv"
    write_csv(parsed_path, all_rows)
    manifest = {
        "run_type": "main_list_probe",
        "started_at": started_at,
        "finished_at": now(),
        "retailer_key": retailer_key,
        "product_type": product_type.upper(),
        "run_date": run_date,
        "run_root": rel_path(run_root),
        "subject_root": rel_path(subject_root),
        "url_template": url_template,
        "pages": page_count,
        "success": bool(all_rows),
        "row_count": len(all_rows),
        "parsed_path": rel_path(parsed_path),
        "fetches": fetches,
    }
    manifest_path = subject_root / "manifest.json"
    manifest_path.write_text(json.dumps(manifest, indent=2, ensure_ascii=False), encoding="utf-8")
    print(json.dumps(manifest, indent=2, ensure_ascii=False))
    return manifest
