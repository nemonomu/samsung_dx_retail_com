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

from .step00_apollo import iter_apollo_push_payloads
from .step00_browser_session import (
    add_intl_nosplash,
    browser_fetch_graphql,
    browser_outer_html,
    close_browser_page,
    create_browser_page,
    env_bool,
    env_int,
)
from .step00_config import DEFAULT_BESTBUY_RUN_ROOT, bestbuy_category, has_target_url, load_initial_urls, rel_path


RUN_DATE = os.getenv("BESTBUY_RUN_DATE", datetime.now().strftime("%Y%m%d"))
CATEGORY = bestbuy_category()
INPUT_HTML = Path(os.getenv("BESTBUY_TRENDING_HTML", "references/bestbuy_tv_trending_page_sample.html"))
RUN_ROOT = Path(os.getenv("BESTBUY_TRENDING_RUN_ROOT", DEFAULT_BESTBUY_RUN_ROOT / "trending"))
GRAPHQL_ENDPOINT = os.getenv("BESTBUY_GRAPHQL_ENDPOINT", "https://www.bestbuy.com/gateway/graphql")
FETCH_MODE = os.getenv("BESTBUY_TRENDING_FETCH_MODE", "auto").strip().lower()
PAGE_PAYLOAD_FETCH_MODES = {"html", "page", "live_html", "legacy_html", "page_payload", "rsc_payload", "doc_payload"}
SOURCE_PAYLOAD_ENV = os.getenv("BESTBUY_TRENDING_SOURCE_PAYLOAD", "").strip()
TRENDING_URL_ENV = os.getenv("BESTBUY_TRENDING_URL", "").strip()
SOURCE_PAYLOAD_PATH = Path(
    SOURCE_PAYLOAD_ENV or f"references/bestbuy_trending_{CATEGORY.lower()}_request.json"
)
SOURCE_PAYLOAD_FALLBACK_PATH = Path("references/bestbuy_trending_request.json")
OUTPUT_CSV = Path(
    os.getenv(
        "BESTBUY_TRENDING_OUTPUT",
        DEFAULT_BESTBUY_RUN_ROOT / "trending" / "parsed" / "trending_products.csv",
    )
)
LIVE_FETCH = os.getenv("BESTBUY_TRENDING_LIVE", "1").lower() in {"1", "true", "yes", "y"}
ALLOW_RENDER_FALLBACK = os.getenv(
    "BESTBUY_TRENDING_ALLOW_RENDER_FALLBACK",
    "0",
).lower() in {"1", "true", "yes", "y"}
REQUEST_TIMEOUT = int(os.getenv("ZENROWS_TIMEOUT", "180"))
TRENDING_URL = TRENDING_URL_ENV or load_initial_urls().get("trending_tvs_projectors", "")
LIMIT = int(os.getenv("BESTBUY_TRENDING_LIMIT", "10"))
WAIT_MS = os.getenv("ZENROWS_WAIT_MS") or os.getenv("BESTBUY_TRENDING_WAIT_MS") or "8000"
WAIT_MS_SEQUENCE = os.getenv("BESTBUY_TRENDING_WAIT_MS_SEQUENCE", "").strip()
SKIP_IF_NO_SOURCE = os.getenv("BESTBUY_TRENDING_SKIP_IF_NO_SOURCE", "1").lower() in {"1", "true", "yes", "y"}
BROWSER_WAIT_SECONDS = max(0, int(os.getenv("BESTBUY_TRENDING_BROWSER_WAIT_SECONDS", "8")))
BROWSER_JS_TIMEOUT = max(1, int(os.getenv("BESTBUY_TRENDING_BROWSER_JS_TIMEOUT", "120")))
BROWSER_HEADLESS = env_bool("BESTBUY_TRENDING_BROWSER_HEADLESS", "1")
BROWSER_LOCAL_PORT = env_int("BESTBUY_TRENDING_BROWSER_LOCAL_PORT", "0")
JSON_RESPONSE = os.getenv("BESTBUY_TRENDING_JSON_RESPONSE", "1").lower() in {"1", "true", "yes", "y"}
REQUIRE_ROWS = os.getenv(
    "BESTBUY_TRENDING_REQUIRE_ROWS",
    "1",
).lower() in {"1", "true", "yes", "y"}
ALLOW_NETWORK_SKU_FALLBACK = os.getenv(
    "BESTBUY_TRENDING_ALLOW_NETWORK_SKUS",
    "1",
).lower() in {"1", "true", "yes", "y"}
GRAPHQL_JS_RENDER = os.getenv(
    "BESTBUY_TRENDING_GRAPHQL_JS_RENDER",
    os.getenv("BESTBUY_GRAPHQL_JS_RENDER", "1"),
).lower() in {"1", "true", "yes", "y"}
DEFAULT_TREND_SECTION = (
    "Trending Deals in Cell Phones & Accessories"
    if CATEGORY == "HHP"
    else "Trending Deals in TVs & Projectors"
)
TREND_SECTION = os.getenv("BESTBUY_TRENDING_SECTION", DEFAULT_TREND_SECTION)
SKU_WINDOW = os.getenv("BESTBUY_TRENDING_SKU_WINDOW", "tail").strip().lower()
BESTBUY_BASE_URL = "https://www.bestbuy.com"


def now():
    return datetime.now().isoformat(timespec="seconds")


def cost_float(value):
    try:
        return float(value or 0)
    except (TypeError, ValueError):
        return 0.0


def decode_capture_text(text):
    decoded = unquote(str(text or "").replace("^%^", "%"))
    decoded = decoded.replace("^\\^\"", '"').replace("^\"", '"').replace("^", "")
    decoded = decoded.replace('\\"', '"')
    return html.unescape(decoded)


def clean_text(value):
    return " ".join(str(value or "").split())


def walk_nodes(value):
    if isinstance(value, dict):
        yield value
        for item in value.values():
            yield from walk_nodes(item)
    elif isinstance(value, list):
        for item in value:
            yield from walk_nodes(item)


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


def parse_json_value(text):
    try:
        return json.loads(text)
    except (TypeError, ValueError):
        return {}


def json_response_xhr_items(json_data):
    if not isinstance(json_data, dict):
        return []
    xhr = json_data.get("xhr") or []
    return xhr if isinstance(xhr, list) else []


def json_response_capture_texts(json_data):
    if not isinstance(json_data, (dict, list)):
        return []

    texts = []
    seen = set()

    def add_text(value):
        if value is None:
            return
        if isinstance(value, (dict, list)):
            value = json.dumps(value, ensure_ascii=False, separators=(",", ":"))
        value = str(value or "")
        if not value:
            return
        key = (len(value), value[:500])
        if key in seen:
            return
        seen.add(key)
        texts.append(value)

    if isinstance(json_data, dict):
        for key in ("html", "content", "body", "response", "responseText", "text"):
            add_text(json_data.get(key))
        for request in json_response_xhr_items(json_data):
            if isinstance(request, dict):
                for key in ("body", "response", "responseText", "content", "text", "html"):
                    add_text(request.get(key))
                add_text(request)
            else:
                add_text(request)
    add_text(json_data)
    return texts


def parse_trending_products_from_capture(html_text, json_data=None, limit=10):
    rows = parse_trending_products(html_text or "", limit=limit)
    if rows:
        return rows
    for capture_text in json_response_capture_texts(json_data):
        rows = parse_trending_products(capture_text, limit=limit)
        if rows:
            for row in rows:
                row["source"] = f"json_response_{row.get('source') or 'capture'}"
            return rows
    return []


def product_name(product):
    name = (product or {}).get("name") if isinstance(product, dict) else {}
    if isinstance(name, dict):
        return clean_text(name.get("short") or name.get("title") or name.get("display") or name.get("rawShort") or "")
    return clean_text(name)


def product_url(product):
    url = (product or {}).get("url") if isinstance(product, dict) else {}
    if isinstance(url, dict):
        return absolute_url(
            clean_text(url.get("pdp") or url.get("relativePdp") or url.get("skuSpecificUrl") or "")
        )
    return absolute_url(clean_text(url))


def spotlight_product_items(connection):
    items = []
    for key in ("items", "products", "nodes"):
        value = connection.get(key)
        if isinstance(value, list):
            items.extend(item for item in value if isinstance(item, dict))
    edges = connection.get("edges")
    if isinstance(edges, list):
        for edge in edges:
            node = edge.get("node") if isinstance(edge, dict) else None
            if isinstance(node, dict):
                items.append(node)
    if items:
        return items
    return [
        node
        for node in walk_nodes(connection)
        if node is not connection and node.get("__typename") == "SpotlightProduct"
    ]


def parse_trending_products_from_graphql(response_json, limit=10):
    rows = []
    seen = set()
    for node in walk_nodes(response_json):
        if node.get("__typename") != "SpotlightProductConnection":
            continue
        trend_section = clean_text(node.get("storyHeader") or TREND_SECTION)
        for item in spotlight_product_items(node):
            if not isinstance(item, dict):
                continue
            sku = clean_text(item.get("sku") or item.get("skuId") or item.get("originalSkuId") or "")
            if not sku or sku in seen:
                continue
            product = item.get("product") if isinstance(item.get("product"), dict) else item
            seen.add(sku)
            rows.append(
                {
                    "trend_section": trend_section,
                    "trend_rank": len(rows) + 1,
                    "sku_id": sku,
                    "bsin": clean_text(item.get("bsin") or product.get("bsin") or ""),
                    "retailer_sku_name": product_name(product),
                    "product_url": product_url(product),
                    "source_card_id": "",
                    "source": "direct_graphql_spotlight_product_connection",
                }
            )
            if limit and len(rows) >= limit:
                return rows
    return rows


def source_payload_candidates(path=SOURCE_PAYLOAD_PATH):
    candidates = [Path(path)]
    if not SOURCE_PAYLOAD_ENV and SOURCE_PAYLOAD_FALLBACK_PATH not in candidates:
        candidates.append(SOURCE_PAYLOAD_FALLBACK_PATH)
    return candidates


def existing_source_payload_path(path=SOURCE_PAYLOAD_PATH):
    for candidate in source_payload_candidates(path):
        if candidate.exists():
            return candidate
    return None


def no_exposed_trending_source():
    if not SKIP_IF_NO_SOURCE:
        return False
    if CATEGORY != "TV":
        return False
    if TRENDING_URL_ENV:
        return False
    if existing_source_payload_path() is not None:
        return False
    return True


def load_graphql_payload(path=SOURCE_PAYLOAD_PATH):
    path = existing_source_payload_path(path)
    if not path:
        searched = ", ".join(str(candidate) for candidate in source_payload_candidates())
        raise FileNotFoundError(
            f"Trending direct GraphQL source payload not found. searched={searched}. "
            "Set BESTBUY_TRENDING_SOURCE_PAYLOAD to a saved /gateway/graphql request body."
        )
    payload = json.loads(path.read_text(encoding="utf-8"))
    if isinstance(payload, dict):
        for key in ("payload", "body", "request"):
            nested = payload.get(key)
            if isinstance(nested, (dict, list)):
                payload = nested
                break
    if not isinstance(payload, (dict, list)):
        raise ValueError(f"Trending direct GraphQL payload must be a JSON object or list: {path}")
    return payload


def graphql_params():
    params = {
        "custom_headers": "true",
        "premium_proxy": "true",
        "proxy_country": "us",
    }
    if GRAPHQL_JS_RENDER:
        params["js_render"] = "true"
    return params


def direct_graphql(payload):
    api_key = os.getenv("ZENROWS_API_KEY")
    if not api_key:
        raise RuntimeError("Set ZENROWS_API_KEY in .env")
    if not TRENDING_URL:
        raise RuntimeError("Set BESTBUY_TRENDING_URL or target_urls.trend before direct trending collection")

    raw_dir = RUN_ROOT / "raw" / "graphql"
    raw_dir.mkdir(parents=True, exist_ok=True)
    client = ZenRowsClient(api_key)
    start = time.perf_counter()
    response = client.post(
        GRAPHQL_ENDPOINT,
        params=graphql_params(),
        headers={
            "accept": "application/json, text/plain, */*",
            "content-type": "application/json",
            "origin": "https://www.bestbuy.com",
            "referer": TRENDING_URL,
        },
        data=json.dumps(payload),
        timeout=REQUEST_TIMEOUT,
    )
    elapsed = round(time.perf_counter() - start, 3)
    text = response.text
    request_path = raw_dir / "trending_request.json"
    response_path = raw_dir / "trending_response.txt"
    json_path = raw_dir / "trending_response.json"
    headers_path = raw_dir / "trending_headers.json"
    request_path.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")
    response_path.write_text(text, encoding="utf-8", errors="replace")
    headers_path.write_text(json.dumps(dict(response.headers), indent=2, ensure_ascii=False), encoding="utf-8")
    response_json = parse_json_value(text)
    if response_json:
        json_path.write_text(json.dumps(response_json, indent=2, ensure_ascii=False), encoding="utf-8")
    summary = {
        "started_at": now(),
        "live": True,
        "fetch_mode": "direct_graphql",
        "url": TRENDING_URL,
        "endpoint": GRAPHQL_ENDPOINT,
        "status_code": response.status_code,
        "elapsed_seconds": elapsed,
        "x_request_cost": response.headers.get("x-request-cost", ""),
        "js_render": GRAPHQL_JS_RENDER,
        "bytes": len(text or ""),
        "request": rel_path(request_path),
        "response": rel_path(json_path if response_json else response_path),
        "headers": rel_path(headers_path),
        "success": response.status_code == 200 and bool(response_json),
    }
    (RUN_ROOT / "summary_direct_graphql.json").write_text(
        json.dumps(summary, indent=2, ensure_ascii=False), encoding="utf-8"
    )
    if response.status_code != 200:
        raise RuntimeError(f"Trending direct GraphQL fetch failed: status={response.status_code}")
    if not response_json:
        raise RuntimeError("Trending direct GraphQL fetch returned non-JSON response")
    return response_json


def operation_name_from_query(query):
    query = str(query or "")
    return query.split("{", 1)[0].replace("query", "", 1).strip().split("(", 1)[0]


def find_trending_started_operation(html_text):
    for payload in iter_apollo_push_payloads(html_text or ""):
        for event in payload.get("events", []):
            if event.get("type") != "started":
                continue
            options = event.get("options") or {}
            query = options.get("query") or ""
            if "SpotlightProductConnection" not in query and "SpotlightProduct" not in query:
                continue
            result = {
                "operationName": options.get("operationName") or operation_name_from_query(query),
                "variables": options.get("variables") or {},
                "query": query,
            }
            extensions = options.get("extensions")
            if isinstance(extensions, dict):
                result["extensions"] = extensions
            return result
    return None


def browser_graphql():
    if not TRENDING_URL:
        raise RuntimeError("Set BESTBUY_TRENDING_URL or target_urls.trend before browser trending collection")

    raw_dir = RUN_ROOT / "raw" / "browser_graphql"
    raw_dir.mkdir(parents=True, exist_ok=True)
    page = None
    browser_meta = {}
    try:
        page, browser_meta = create_browser_page(
            run_root=RUN_ROOT,
            name="trending_browser",
            headless=BROWSER_HEADLESS,
            local_port=BROWSER_LOCAL_PORT,
        )
        browser_url = add_intl_nosplash(TRENDING_URL)
        page.get(browser_url)
        if BROWSER_WAIT_SECONDS:
            time.sleep(BROWSER_WAIT_SECONDS)
        html_text = browser_outer_html(page, timeout=BROWSER_JS_TIMEOUT)
        html_path = raw_dir / "trending_browser_page.html"
        html_path.write_text(html_text, encoding="utf-8", errors="replace")

        payload = find_trending_started_operation(html_text)
        payload_source = "browser_apollo_started"
        if payload is None:
            payload_path = existing_source_payload_path()
            if payload_path:
                payload = load_graphql_payload(payload_path)
                payload_source = rel_path(payload_path)
        if payload is None:
            raise RuntimeError(
                "Trending browser page did not expose a SpotlightProduct GraphQL payload. "
                "Save a captured request JSON and set BESTBUY_TRENDING_SOURCE_PAYLOAD."
            )

        start = time.perf_counter()
        envelope = browser_fetch_graphql(page, payload, timeout=BROWSER_JS_TIMEOUT)
        elapsed = round(time.perf_counter() - start, 3)
    finally:
        close_browser_page(page)

    status_code = int(envelope.get("status") or 0)
    text = str(envelope.get("body") or "")
    request_path = raw_dir / "trending_request.json"
    response_path = raw_dir / "trending_response.txt"
    json_path = raw_dir / "trending_response.json"
    envelope_path = raw_dir / "trending_envelope.json"
    request_path.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")
    response_path.write_text(text, encoding="utf-8", errors="replace")
    envelope_path.write_text(json.dumps(envelope, indent=2, ensure_ascii=False), encoding="utf-8")

    response_json = parse_json_value(text)
    if response_json:
        json_path.write_text(json.dumps(response_json, indent=2, ensure_ascii=False), encoding="utf-8")
    summary = {
        "started_at": now(),
        "live": True,
        "fetch_mode": "browser_graphql",
        "url": TRENDING_URL,
        "browser_url": browser_url,
        "endpoint": "/gateway/graphql",
        "status_code": status_code,
        "elapsed_seconds": elapsed,
        "x_request_cost": "0",
        "bytes": len(text or ""),
        "payload_source": payload_source,
        "request": rel_path(request_path),
        "response": rel_path(json_path if response_json else response_path),
        "envelope": rel_path(envelope_path),
        "browser": browser_meta,
        "success": status_code == 200 and bool(response_json),
    }
    (RUN_ROOT / "summary_browser_graphql.json").write_text(
        json.dumps(summary, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )
    if status_code != 200:
        raise RuntimeError(f"Trending browser GraphQL fetch failed: status={status_code}")
    if not response_json:
        raise RuntimeError("Trending browser GraphQL fetch returned non-JSON response")
    return response_json


def use_direct_graphql():
    if FETCH_MODE in {"graphql", "direct_graphql"}:
        return True
    if FETCH_MODE == "auto":
        return existing_source_payload_path() is not None
    if FETCH_MODE in PAGE_PAYLOAD_FETCH_MODES:
        return False
    raise ValueError(
        "BESTBUY_TRENDING_FETCH_MODE must be one of: auto, graphql, direct_graphql, "
        "html, page_payload"
    )


def use_render_fallback():
    if FETCH_MODE in PAGE_PAYLOAD_FETCH_MODES:
        return True
    return FETCH_MODE == "auto" and ALLOW_RENDER_FALLBACK


def write_skip_summary(reason):
    RUN_ROOT.mkdir(parents=True, exist_ok=True)
    summary = {
        "started_at": now(),
        "live": LIVE_FETCH,
        "fetch_mode": FETCH_MODE,
        "skipped": True,
        "reason": reason,
        "source_payload": rel_path(SOURCE_PAYLOAD_PATH),
        "source_payload_searched": [rel_path(path) for path in source_payload_candidates()],
        "render_fallback_allowed": ALLOW_RENDER_FALLBACK,
        "row_count": 0,
        "total_x_request_cost": 0,
    }
    (RUN_ROOT / "summary_skip.json").write_text(json.dumps(summary, indent=2, ensure_ascii=False), encoding="utf-8")
    return summary


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
    params = {
        "js_render": "true",
        "premium_proxy": "true",
        "proxy_country": "us",
        **({"wait": wait_ms} if wait_ms else {}),
    }
    if JSON_RESPONSE:
        params["json_response"] = "true"
    response = client.get(TRENDING_URL, params=params, timeout=REQUEST_TIMEOUT)
    elapsed = round(time.perf_counter() - start, 3)
    text = response.text
    json_data = parse_json_value(text) if JSON_RESPONSE else {}
    html_text = text
    if isinstance(json_data, dict):
        html_text = str(json_data.get("html") or json_data.get("content") or "")
    html_path = raw_dir / "trending_page.html"
    attempt_html_path = raw_dir / f"trending_page_attempt{attempt}.html"
    json_path = raw_dir / "trending_page_json_response.json"
    attempt_json_path = raw_dir / f"trending_page_attempt{attempt}_json_response.json"
    headers_path = raw_dir / "trending_page_headers.json"
    attempt_headers_path = raw_dir / f"trending_page_attempt{attempt}_headers.json"
    html_path.write_text(html_text, encoding="utf-8", errors="replace")
    attempt_html_path.write_text(html_text, encoding="utf-8", errors="replace")
    if JSON_RESPONSE:
        json_path.write_text(text, encoding="utf-8", errors="replace")
        attempt_json_path.write_text(text, encoding="utf-8", errors="replace")
    headers_path.write_text(json.dumps(dict(response.headers), indent=2, ensure_ascii=False), encoding="utf-8")
    attempt_headers_path.write_text(
        json.dumps(dict(response.headers), indent=2, ensure_ascii=False), encoding="utf-8"
    )
    xhr_items = json_response_xhr_items(json_data)
    summary = {
        "started_at": now(),
        "live": True,
        "attempt": attempt,
        "url": TRENDING_URL,
        "status_code": response.status_code,
        "elapsed_seconds": elapsed,
        "x_request_cost": response.headers.get("x-request-cost", ""),
        "wait_ms": wait_ms,
        "json_response": JSON_RESPONSE,
        "bytes": len(text or ""),
        "html_bytes": len(html_text or ""),
        "json_xhr_count": len(xhr_items),
        "html": rel_path(html_path),
        "attempt_html": rel_path(attempt_html_path),
        "json": rel_path(json_path) if JSON_RESPONSE else "",
        "attempt_json": rel_path(attempt_json_path) if JSON_RESPONSE else "",
        "headers": rel_path(headers_path),
        "attempt_headers": rel_path(attempt_headers_path),
        "success": response.status_code == 200,
    }
    (RUN_ROOT / "summary_live_fetch.json").write_text(
        json.dumps(summary, indent=2, ensure_ascii=False), encoding="utf-8"
    )
    if response.status_code != 200:
        raise RuntimeError(f"Trending live fetch failed: status={response.status_code}")
    return html_text, json_data, summary


def rewrite_live_fetch_summary(attempt_summaries, row_count, attempted_waits):
    if not attempt_summaries:
        return
    final_summary = dict(attempt_summaries[-1])
    final_summary["call_count"] = len(attempt_summaries)
    final_summary["total_x_request_cost"] = round(
        sum(cost_float(summary.get("x_request_cost")) for summary in attempt_summaries),
        7,
    )
    final_summary["attempts"] = attempt_summaries
    final_summary["attempted_waits"] = attempted_waits
    final_summary["row_count"] = row_count
    (RUN_ROOT / "summary_live_fetch.json").write_text(
        json.dumps(final_summary, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )


def main():
    if not has_target_url("trend"):
        write_rows(OUTPUT_CSV, [])
        print(f"skipped trending: no trend URL for category -> {OUTPUT_CSV}")
        return
    if no_exposed_trending_source():
        write_rows(OUTPUT_CSV, [])
        summary = write_skip_summary(
            "TV trending source is not exposed/configured; skipping step06 without collection"
        )
        print(json.dumps(summary, indent=2, ensure_ascii=False))
        return
    rows = []
    attempted_waits = []
    live_attempt_summaries = []
    if FETCH_MODE == "browser_graphql":
        response_json = browser_graphql()
        rows = parse_trending_products_from_graphql(response_json, LIMIT)
        if REQUIRE_ROWS and not rows:
            raise RuntimeError(
                "Trending browser GraphQL returned 0 SpotlightProductConnection rows; "
                "verify the captured browser payload contains product data"
            )
    elif use_direct_graphql():
        payload = load_graphql_payload()
        response_json = direct_graphql(payload)
        rows = parse_trending_products_from_graphql(response_json, LIMIT)
        if REQUIRE_ROWS and not rows:
            raise RuntimeError(
                "Trending direct GraphQL returned 0 SpotlightProductConnection rows; "
                "verify BESTBUY_TRENDING_SOURCE_PAYLOAD captures the product data request"
            )
    elif LIVE_FETCH and use_render_fallback():
        for attempt, wait_ms in enumerate(trending_wait_sequence(), 1):
            attempted_waits.append(wait_ms)
            html_text, json_data, summary = live_html(wait_ms=wait_ms, attempt=attempt)
            live_attempt_summaries.append(summary)
            rows = parse_trending_products_from_capture(html_text, json_data, LIMIT)
            if rows:
                break
            print(
                f"[trending:retry] attempt={attempt} wait_ms={wait_ms} rows=0 "
                "reason=no SpotlightProduct/network SKU rows in html/json_response",
                flush=True,
            )
    elif LIVE_FETCH:
        summary = write_skip_summary(
            "direct GraphQL payload is not configured and HTML render fallback is disabled"
        )
        write_rows(OUTPUT_CSV, [])
        print(json.dumps(summary, indent=2, ensure_ascii=False))
        if REQUIRE_ROWS:
            raise RuntimeError(
                "Trending direct GraphQL payload is required. "
                f"Set BESTBUY_TRENDING_SOURCE_PAYLOAD to a saved /gateway/graphql request body: {SOURCE_PAYLOAD_PATH}"
            )
        return
    else:
        html_text = INPUT_HTML.read_text(encoding="utf-8", errors="ignore")
        rows = parse_trending_products_from_capture(html_text, {}, LIMIT)
    if LIVE_FETCH and REQUIRE_ROWS and not rows:
        raise RuntimeError(
            "Trending live fetch returned 0 SpotlightProduct rows after waits="
            + ",".join(attempted_waits)
            + "; retry with a larger BESTBUY_TRENDING_WAIT_MS_SEQUENCE"
        )
    write_rows(OUTPUT_CSV, rows)
    rewrite_live_fetch_summary(live_attempt_summaries, len(rows), attempted_waits)
    print(f"wrote {len(rows)} rows -> {OUTPUT_CSV}")
    for row in rows:
        print(f"{row['trend_rank']}. {row['sku_id']} {row['retailer_sku_name']}")


if __name__ == "__main__":
    main()
