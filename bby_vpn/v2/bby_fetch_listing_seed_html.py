"""Fetch Best Buy listing seed HTML through ZenRows.

The API-only listing replay needs the Apollo ``PlpView_ProductList_Init``
operation embedded in a rendered Best Buy search page. This script follows the
reference implementation's source-HTML approach: fetch one rendered search page,
save it as ``bestbuy_main_search_page_sample.html``, and validate that the
operation can be extracted before the crawler uses it.
"""

from __future__ import annotations

import argparse
import os
import ssl
import sys
import urllib.error
import urllib.parse
import urllib.request

from bby_listing_graphql import find_started_operation


BESTBUY_SEARCH_URL = "https://www.bestbuy.com/site/searchpage.jsp?id=pcat17071&st=tv&intl=nosplash"
ZENROWS_ENDPOINT = "https://api.zenrows.com/v1/"


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
    if env_enabled("BESTBUY_GRAPHQL_MODE_AUTO", default=False):
        params["mode"] = "auto"
        params["proxy_country"] = "us"
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


def main(argv=None):
    parser = argparse.ArgumentParser(description="Fetch Best Buy listing seed HTML via ZenRows.")
    parser.add_argument("--url", default=os.environ.get("BESTBUY_MAIN_URL", BESTBUY_SEARCH_URL))
    parser.add_argument(
        "--out",
        default=os.environ.get("BESTBUY_MAIN_SOURCE_HTML", "bestbuy_main_search_page_sample.html"),
    )
    parser.add_argument("--timeout", type=int, default=int(os.environ.get("ZENROWS_TIMEOUT", "120")))
    args = parser.parse_args(argv)

    load_dotenv(".env")
    html = fetch_html(args.url, args.timeout)
    with open(args.out, "w", encoding="utf-8", errors="replace") as f:
        f.write(html)

    marker = "ApolloSSRDataTransport" in html
    operation = find_started_operation(html, "PlpView_ProductList_Init")
    print(f"saved={os.path.abspath(args.out)} bytes={len(html)} apollo_marker={marker}")
    if not operation:
        print("[ERROR] PlpView_ProductList_Init not found in fetched HTML")
        return 2
    variables = operation.get("variables") if isinstance(operation, dict) else {}
    print(f"operation=PlpView_ProductList_Init variable_keys={sorted((variables or {}).keys())}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
