"""Fetch Best Buy listing seed HTML through ZenRows.

The API-only listing replay needs the Apollo ``PlpView_ProductList_Init``
operation embedded in a rendered Best Buy search page. This script follows the
reference implementation's source-HTML approach: fetch one rendered search page,
save it as ``bestbuy_main_search_page_sample.html``, and validate that the
operation can be extracted before the crawler uses it.
"""

from __future__ import annotations

import argparse
import html as html_lib
import json
import os
import re
import ssl
import sys
import time
import urllib.error
import urllib.parse
import urllib.request

from listing_graphql import extract_apollo_payloads, find_started_operation, operation_name


BESTBUY_SEARCH_URL = "https://www.bestbuy.com/site/searchpage.jsp?id=pcat17071&st=tv&intl=nosplash"
ZENROWS_ENDPOINT = "https://api.zenrows.com/v1/"
TARGET_OPERATION = "PlpView_ProductList_Init"


def load_dotenv(path=".env"):
    if not os.path.exists(path):
        return
    with open(path, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, value = line.split("=", 1)
            key = key.strip()
            value = value.strip().strip('"').strip("'")
            if key and key not in os.environ:
                os.environ[key] = value


def zenrows_params(target_url):
    api_key = os.environ.get("ZENROWS_API_KEY")
    if not api_key:
        raise RuntimeError("ZENROWS_API_KEY is missing. Put it in .env or set it in the cmd session.")
    params = {
        "apikey": api_key,
        "url": target_url,
        "custom_headers": "true",
    }
    if env_enabled("BESTBUY_GRAPHQL_PREMIUM_PROXY", default=True):
        params["premium_proxy"] = "true"
        params["proxy_country"] = "us"
    if env_enabled("BESTBUY_GRAPHQL_JS_RENDER", default=True):
        params["js_render"] = "true"
        wait_ms = os.environ.get("ZENROWS_WAIT_MS") or os.environ.get("BESTBUY_GRAPHQL_ZENROWS_WAIT_MS") or "12000"
        if wait_ms and wait_ms.strip() not in {"0", "false", "False"}:
            params["wait"] = wait_ms.strip()
        wait_for = os.environ.get("ZENROWS_WAIT_FOR") or os.environ.get("BESTBUY_GRAPHQL_ZENROWS_WAIT_FOR")
        if wait_for:
            params["wait_for"] = wait_for.strip()
    if env_enabled("BESTBUY_GRAPHQL_MODE_AUTO", default=False):
        params["mode"] = "auto"
        params["proxy_country"] = "us"
    for env_name, param_name in (
        ("ZENROWS_ANTIBOT", "antibot"),
        ("ZENROWS_BLOCK_RESOURCES", "block_resources"),
        ("ZENROWS_DEVICE", "device"),
        ("ZENROWS_ORIGINAL_STATUS", "original_status"),
    ):
        value = os.environ.get(env_name)
        if value:
            params[param_name] = value.strip()
    return params


def env_enabled(name, default=False):
    value = os.environ.get(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "y"}


def fetch_html(target_url, timeout):
    url = ZENROWS_ENDPOINT + "?" + urllib.parse.urlencode(zenrows_params(target_url))
    req = urllib.request.Request(
        url,
        headers={
            "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "user-agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125 Safari/537.36"
            ),
        },
        method="GET",
    )
    def read_response(context=None):
        with urllib.request.urlopen(req, timeout=timeout, context=context) as response:
            return response.read().decode("utf-8", errors="replace")

    if not env_enabled("ZENROWS_SSL_VERIFY", default=True):
        return read_response(ssl._create_unverified_context())

    try:
        return read_response()
    except urllib.error.URLError as exc:
        reason = getattr(exc, "reason", None)
        message = str(exc)
        cert_failed = isinstance(reason, ssl.SSLCertVerificationError) or "CERTIFICATE_VERIFY_FAILED" in message
        if cert_failed:
            print("[WARNING] ZenRows SSL certificate verification failed; retrying without local CA verification")
            return read_response(ssl._create_unverified_context())
        raise RuntimeError(f"ZenRows seed HTML fetch failed: {exc}") from exc
    except Exception as exc:
        raise RuntimeError(f"ZenRows seed HTML fetch failed: {exc}") from exc


def summarize_html(html):
    text = str(html or "")
    lower = text.lower()
    title_match = re.search(r"<title[^>]*>(.*?)</title>", text, re.IGNORECASE | re.DOTALL)
    title = html_lib.unescape(re.sub(r"\s+", " ", title_match.group(1)).strip()) if title_match else ""
    hints = []
    checks = [
        ("akamai", "_abck" in lower or "akamai" in lower or "bm_sz" in lower),
        ("captcha", "captcha" in lower or "verify you are human" in lower),
        ("consent", "privacy" in lower and ("consent" in lower or "cookie" in lower)),
        ("access_denied", "access denied" in lower or "forbidden" in lower),
        ("bestbuy_shell", "bestbuy" in lower or "bby" in lower),
    ]
    for name, matched in checks:
        if matched:
            hints.append(name)
    snippet = html_lib.unescape(re.sub(r"\s+", " ", text[:700])).strip()
    return {
        "bytes": len(text),
        "title": title,
        "hints": hints,
        "has_body_tag": "<body" in lower,
        "has_next_flight": "self.__next_f" in text,
        "has_apollo_marker": "ApolloSSRDataTransport" in text,
        "has_operation_literal": TARGET_OPERATION in text,
        "looks_like_next_head_shell": (
            "/shop/plp/_next/static/" in text
            and "<title>tv - Best Buy</title>" in text
            and "<body" not in lower
            and "ApolloSSRDataTransport" not in text
        ),
        "snippet": snippet,
    }


def list_apollo_operations(html):
    names = []
    for payload in extract_apollo_payloads(html):
        events = payload.get("events", []) if isinstance(payload, dict) else []
        for event in events:
            if not isinstance(event, dict):
                continue
            options = event.get("options", {})
            query = options.get("query", "") if isinstance(options, dict) else ""
            name = operation_name(query)
            if name:
                names.append(name)
    return sorted(set(names))


def write_seed_diagnostics(out_path, html, operations):
    summary = summarize_html(html)
    summary["operations"] = operations
    summary["ts"] = int(time.time())
    diag_path = os.path.splitext(os.path.abspath(out_path))[0] + ".diagnostics.json"
    with open(diag_path, "w", encoding="utf-8") as f:
        json.dump(summary, f, ensure_ascii=False, indent=2)
    return diag_path, summary


def main(argv=None):
    parser = argparse.ArgumentParser(description="Fetch Best Buy listing seed HTML via ZenRows.")
    parser.add_argument("--url", default=os.environ.get("BESTBUY_MAIN_URL", BESTBUY_SEARCH_URL))
    parser.add_argument("--input", help="Analyze an existing HTML file instead of fetching ZenRows.")
    parser.add_argument(
        "--out",
        default=os.environ.get("BESTBUY_MAIN_SOURCE_HTML", "bestbuy_main_search_page_sample.html"),
    )
    parser.add_argument("--timeout", type=int, default=int(os.environ.get("ZENROWS_TIMEOUT", "120")))
    args = parser.parse_args(argv)

    load_dotenv(".env")
    if args.input:
        with open(args.input, encoding="utf-8", errors="replace") as f:
            html = f.read()
        args.out = args.input
    else:
        html = fetch_html(args.url, args.timeout)
        with open(args.out, "w", encoding="utf-8", errors="replace") as f:
            f.write(html)

    operations = list_apollo_operations(html)
    marker = "ApolloSSRDataTransport" in html
    operation = find_started_operation(html, TARGET_OPERATION)
    print(f"saved={os.path.abspath(args.out)} bytes={len(html)} apollo_marker={marker}")
    if not operation:
        diag_path, summary = write_seed_diagnostics(args.out, html, operations)
        print(f"[ERROR] {TARGET_OPERATION} not found in fetched HTML")
        print(
            "[INFO] seed_html_summary "
            f"title={summary['title']!r} hints={summary['hints']} "
            f"body={summary['has_body_tag']} next_flight={summary['has_next_flight']} "
            f"next_head_shell={summary['looks_like_next_head_shell']} "
            f"operation_literal={summary['has_operation_literal']} operations={operations[:20]} "
            f"diagnostics={diag_path}"
        )
        if summary["looks_like_next_head_shell"]:
            print(
                "[INFO] fetched HTML is a Best Buy Next PLP head-only shell. "
                "It does not contain Apollo startup payloads; capture listing GraphQL from browser network instead."
            )
        return 2
    variables = operation.get("variables") if isinstance(operation, dict) else {}
    print(f"operation={TARGET_OPERATION} variable_keys={sorted((variables or {}).keys())}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

