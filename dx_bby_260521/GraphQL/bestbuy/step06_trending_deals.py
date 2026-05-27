import csv
import html
import json
import os
import re
import time
from urllib.parse import unquote
from datetime import datetime
from pathlib import Path

from zenrows import ZenRowsClient

from .step00_config import DEFAULT_BESTBUY_RUN_ROOT, bestbuy_category, has_target_url, load_initial_urls, rel_path


RUN_DATE = os.getenv("BESTBUY_RUN_DATE", datetime.now().strftime("%Y%m%d"))
INPUT_HTML = Path(os.getenv("BESTBUY_TRENDING_HTML", "references/bestbuy_tv_trending_page_sample.html"))
RUN_ROOT = Path(os.getenv("BESTBUY_TRENDING_RUN_ROOT", DEFAULT_BESTBUY_RUN_ROOT / "trending"))
OUTPUT_CSV = Path(
    os.getenv(
        "BESTBUY_TRENDING_OUTPUT",
        DEFAULT_BESTBUY_RUN_ROOT / "trending" / "parsed" / "trending_products.csv",
    )
)
LIVE_FETCH = os.getenv("BESTBUY_TRENDING_LIVE", "1").lower() in {"1", "true", "yes", "y"}
REQUEST_TIMEOUT = int(os.getenv("ZENROWS_TIMEOUT", "180"))
TRENDING_URL = os.getenv("BESTBUY_TRENDING_URL", load_initial_urls().get("trending_tvs_projectors", ""))
LIMIT = int(os.getenv("BESTBUY_TRENDING_LIMIT", "10"))
WAIT_MS = os.getenv("ZENROWS_WAIT_MS") or os.getenv("BESTBUY_TRENDING_WAIT_MS") or "8000"
WAIT_MS_SEQUENCE = os.getenv("BESTBUY_TRENDING_WAIT_MS_SEQUENCE", "").strip()
REQUIRE_ROWS = os.getenv(
    "BESTBUY_TRENDING_REQUIRE_ROWS",
    "1",
).lower() in {"1", "true", "yes", "y"}
ALLOW_NETWORK_SKU_FALLBACK = os.getenv(
    "BESTBUY_TRENDING_ALLOW_NETWORK_SKUS",
    "1",
).lower() in {"1", "true", "yes", "y"}
DEFAULT_TREND_SECTION = (
    "Trending Deals in Cell Phones & Accessories"
    if bestbuy_category() == "HHP"
    else "Trending Deals in TVs & Projectors"
)
TREND_SECTION = os.getenv("BESTBUY_TRENDING_SECTION", DEFAULT_TREND_SECTION)
SKU_WINDOW = os.getenv("BESTBUY_TRENDING_SKU_WINDOW", "tail").strip().lower()
BESTBUY_BASE_URL = "https://www.bestbuy.com"


def now():
    return datetime.now().isoformat(timespec="seconds")


def decode_capture_text(text):
    decoded = unquote(str(text or "").replace("^%^", "%"))
    decoded = decoded.replace("^\\^\"", '"').replace("^\"", '"').replace("^", "")
    decoded = decoded.replace('\\"', '"')
    return html.unescape(decoded)


def clean_text(value):
    return " ".join(str(value or "").split())


def absolute_url(path):
    if not path:
        return ""
    if path.startswith("http"):
        return path
    if path.startswith("/"):
        return f"{BESTBUY_BASE_URL}{path}"
    return path


def extract_analytics_sku_sequences(text):
    sequences = []
    seen = set()
    for match in re.finditer(r"\bskus\b[^\n\r]{0,8000}", text, flags=re.IGNORECASE):
        snippet = decode_capture_text(match.group(0))
        skus = re.findall(r"\b\d{7}\b", snippet)
        if len(skus) < 3:
            continue
        key = tuple(skus)
        if key in seen:
            continue
        seen.add(key)
        sequences.append(skus)
    return sequences


def choose_trending_skus(text, limit=10):
    sequences = extract_analytics_sku_sequences(text)
    if not sequences:
        return []
    sequence = max(sequences, key=len)
    if not limit:
        return sequence
    if SKU_WINDOW in {"head", "first"}:
        return sequence[:limit]
    return sequence[-limit:]


def extract_structured_product_metadata(text):
    decoded = decode_capture_text(text)
    metadata = {}

    patterns = [
        re.compile(
            r'"skuId"\s*:\s*"(?P<sku>\d{7})".{0,2500?}'
            r'"name"\s*:\s*\{[^{}]*"short"\s*:\s*"(?P<name>[^"]+)"[^{}]*\}.{0,2500?}'
            r'"url"\s*:\s*\{[^{}]*(?:"pdp"|"relativePdp"|"skuSpecificUrl")\s*:\s*"(?P<url>[^"]+)"',
            re.DOTALL,
        ),
        re.compile(
            r'"skuId"\s*:\s*"(?P<sku>\d{7})".{0,2500?}'
            r'"url"\s*:\s*\{[^{}]*(?:"pdp"|"relativePdp"|"skuSpecificUrl")\s*:\s*"(?P<url>[^"]+)"[^{}]*\}.{0,2500?}'
            r'"name"\s*:\s*\{[^{}]*"short"\s*:\s*"(?P<name>[^"]+)"',
            re.DOTALL,
        ),
    ]
    for pattern in patterns:
        for match in pattern.finditer(decoded):
            sku = match.group("sku")
            metadata.setdefault(sku, {})
            metadata[sku].update(
                {
                    "retailer_sku_name": clean_text(match.group("name")),
                    "product_url": absolute_url(match.group("url")),
                }
            )

    return metadata


def clean_graphql_value(value):
    raw = str(value or "").replace('\\\\"', '\\"')
    try:
        decoded = json.loads(f'"{raw}"')
    except ValueError:
        decoded = raw
    return clean_text(html.unescape(str(decoded).replace("\\u0026", "&").replace("\\/", "/").replace('\\"', '"')))


def extract_spotlight_product_rows(text, limit=10):
    decoded = decode_capture_text(text).replace('\\\\"', '\\"')
    connection_pos = decoded.find('"__typename":"SpotlightProductConnection"')
    if connection_pos < 0:
        return []

    block = decoded[connection_pos : connection_pos + 250000]
    header_match = re.search(r'"storyHeader":"(?P<header>(?:\\.|[^"])*)"', block)
    trend_section = clean_graphql_value(header_match.group("header")) if header_match else TREND_SECTION
    pattern = re.compile(
        r'"__typename":"SpotlightProduct","sku":"(?P<sku>\d{7})"'
        r'(?P<body>.*?)'
        r'"bsin":"(?P<bsin>[A-Z0-9]+)","originalSkuId":"(?P<original_sku>\d{7})"',
        re.DOTALL,
    )
    rows = []
    seen = set()
    for match in pattern.finditer(block):
        sku = match.group("sku")
        if sku in seen:
            continue
        seen.add(sku)
        body = match.group("body")
        name_match = re.search(r'"short":"(?P<name>(?:\\.|[^"])*)"', body)
        url_match = re.search(r'"pdp":"(?P<url>(?:\\.|[^"])*)"', body)
        if not url_match:
            url_match = re.search(r'"relativePdp":"(?P<url>(?:\\.|[^"])*)"', body)
        rows.append(
            {
                "trend_section": trend_section,
                "trend_rank": len(rows) + 1,
                "sku_id": sku,
                "bsin": match.group("bsin"),
                "retailer_sku_name": clean_graphql_value(name_match.group("name")) if name_match else "",
                "product_url": absolute_url(clean_graphql_value(url_match.group("url"))) if url_match else "",
                "source_card_id": "",
                "source": "spotlight_product_connection",
            }
        )
        if limit and len(rows) >= limit:
            break
    return rows


def parse_trending_products(html_text, limit=10):
    spotlight_rows = extract_spotlight_product_rows(html_text, limit=limit)
    if spotlight_rows:
        return spotlight_rows
    if not ALLOW_NETWORK_SKU_FALLBACK:
        return []

    trend_skus = choose_trending_skus(html_text, limit=limit)
    metadata = extract_structured_product_metadata(html_text)
    rows = []
    for rank, sku in enumerate(trend_skus, 1):
        product = metadata.get(sku, {})
        rows.append(
            {
                "trend_section": TREND_SECTION,
                "trend_rank": rank,
                "sku_id": sku,
                "retailer_sku_name": product.get("retailer_sku_name", ""),
                "product_url": product.get("product_url", ""),
                "source_card_id": "",
                "source": "network_skus_with_structured_product_metadata" if product else "network_skus",
            }
        )
    return rows


def write_rows(path, rows):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=[
                "trend_section",
                "trend_rank",
                "sku_id",
                "bsin",
                "retailer_sku_name",
                "product_url",
                "source_card_id",
                "source",
            ],
        )
        writer.writeheader()
        writer.writerows(rows)


def trending_wait_sequence():
    raw_values = WAIT_MS_SEQUENCE or ",".join([WAIT_MS, "20000", "35000"])
    values = []
    seen = set()
    for raw in raw_values.split(","):
        value = str(raw or "").strip()
        if not value or value in seen:
            continue
        seen.add(value)
        values.append(value)
    return values or [WAIT_MS]


def live_html(wait_ms=None, attempt=1):
    api_key = os.getenv("ZENROWS_API_KEY")
    if not api_key:
        raise RuntimeError("Set ZENROWS_API_KEY in .env")
    if not TRENDING_URL:
        raise RuntimeError("Set BESTBUY_TRENDING_URL or target_urls.trend before live trending collection")

    raw_dir = RUN_ROOT / "raw" / "live_page"
    raw_dir.mkdir(parents=True, exist_ok=True)
    client = ZenRowsClient(api_key)
    wait_ms = str(wait_ms or WAIT_MS or "8000")
    start = time.perf_counter()
    response = client.get(
        TRENDING_URL,
        params={
            "js_render": "true",
            "premium_proxy": "true",
            "proxy_country": "us",
            **({"wait": wait_ms} if wait_ms else {}),
        },
        timeout=REQUEST_TIMEOUT,
    )
    elapsed = round(time.perf_counter() - start, 3)
    text = response.text
    html_path = raw_dir / "trending_page.html"
    attempt_html_path = raw_dir / f"trending_page_attempt{attempt}.html"
    headers_path = raw_dir / "trending_page_headers.json"
    attempt_headers_path = raw_dir / f"trending_page_attempt{attempt}_headers.json"
    html_path.write_text(text, encoding="utf-8", errors="replace")
    attempt_html_path.write_text(text, encoding="utf-8", errors="replace")
    headers_path.write_text(json.dumps(dict(response.headers), indent=2, ensure_ascii=False), encoding="utf-8")
    attempt_headers_path.write_text(
        json.dumps(dict(response.headers), indent=2, ensure_ascii=False), encoding="utf-8"
    )
    summary = {
        "started_at": now(),
        "live": True,
        "attempt": attempt,
        "url": TRENDING_URL,
        "status_code": response.status_code,
        "elapsed_seconds": elapsed,
        "x_request_cost": response.headers.get("x-request-cost", ""),
        "wait_ms": wait_ms,
        "bytes": len(text or ""),
        "html": rel_path(html_path),
        "attempt_html": rel_path(attempt_html_path),
        "headers": rel_path(headers_path),
        "attempt_headers": rel_path(attempt_headers_path),
        "success": response.status_code == 200,
    }
    (RUN_ROOT / "summary_live_fetch.json").write_text(
        json.dumps(summary, indent=2, ensure_ascii=False), encoding="utf-8"
    )
    if response.status_code != 200:
        raise RuntimeError(f"Trending live fetch failed: status={response.status_code}")
    return text


def main():
    if not has_target_url("trend"):
        write_rows(OUTPUT_CSV, [])
        print(f"skipped trending: no trend URL for category -> {OUTPUT_CSV}")
        return
    rows = []
    attempted_waits = []
    if LIVE_FETCH:
        for attempt, wait_ms in enumerate(trending_wait_sequence(), 1):
            attempted_waits.append(wait_ms)
            html_text = live_html(wait_ms=wait_ms, attempt=attempt)
            rows = parse_trending_products(html_text, LIMIT)
            if rows:
                break
            print(
                f"[trending:retry] attempt={attempt} wait_ms={wait_ms} rows=0 "
                "reason=no SpotlightProduct/network SKU rows",
                flush=True,
            )
    else:
        html_text = INPUT_HTML.read_text(encoding="utf-8", errors="ignore")
        rows = parse_trending_products(html_text, LIMIT)
    if LIVE_FETCH and REQUIRE_ROWS and not rows:
        raise RuntimeError(
            "Trending live fetch returned 0 GraphQL SpotlightProduct rows after waits="
            + ",".join(attempted_waits)
            + "; retry with a larger BESTBUY_TRENDING_WAIT_MS_SEQUENCE"
        )
    write_rows(OUTPUT_CSV, rows)
    print(f"wrote {len(rows)} rows -> {OUTPUT_CSV}")
    for row in rows:
        print(f"{row['trend_rank']}. {row['sku_id']} {row['retailer_sku_name']}")


if __name__ == "__main__":
    main()
