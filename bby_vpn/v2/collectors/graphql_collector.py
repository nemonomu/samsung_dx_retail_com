"""API-first GraphQL collector using httpx when available."""

import asyncio
import copy
import json
import os
import re
import tempfile
import time
import urllib.error
import urllib.request

from core.rate_limit import AsyncHostRateLimiter
from core.retry import ExponentialBackoff
from parsers.graphql_product_parser import parse_product_facts
from parsers.graphql_review_parser import collect_reviews


REVIEW_OPERATIONS = (
    "CustomerRatingCard_Init",
    "Ai_Review_Summary_Init",
    "CustomerReviewList_Init",
)


def extract_bestbuy_sku_id(product_url):
    """Extract numeric Best Buy skuId from legacy URLs when available."""
    if not product_url:
        return None
    clean = str(product_url).split("?", 1)[0].rstrip("/")
    match = re.search(r"/sku/(\d+)(?:/|$)", clean)
    if match:
        return match.group(1)
    match = re.search(r"/(\d+)\.p$", clean)
    if match:
        return match.group(1)
    return None


class GraphQLCollector:
    def __init__(self, audit_log=None, timeout=20, concurrency=3, rate_limiter=None, retry_policy=None):
        self.audit_log = audit_log
        self.timeout = timeout
        self.semaphore = asyncio.Semaphore(concurrency)
        self.rate_limiter = rate_limiter or AsyncHostRateLimiter()
        self.retry_policy = retry_policy or ExponentialBackoff()

    async def execute(self, endpoint_url, payload, headers=None, cookies=None):
        try:
            import httpx
        except ImportError:
            try:
                import requests  # noqa: F401
            except ImportError:
                return await self._execute_with_urllib(endpoint_url, payload, headers=headers, cookies=cookies)
            return await self._execute_with_requests(endpoint_url, payload, headers=headers, cookies=cookies)

        headers = headers or {}
        cookies = cookies or {}
        async with self.semaphore:
            client_kwargs = {
                "timeout": self.timeout,
                "headers": headers,
                "cookies": cookies,
                "follow_redirects": True,
            }
            try:
                import h2  # noqa: F401
                client_kwargs["http2"] = True
            except ImportError:
                pass
            async with httpx.AsyncClient(**client_kwargs) as client:
                attempt = 1
                while True:
                    await self.rate_limiter.wait(endpoint_url)
                    started = time.time()
                    try:
                        response = await client.post(endpoint_url, json=payload)
                        elapsed_ms = int((time.time() - started) * 1000)
                        self._log("graphql_request", {
                            "endpoint_url": endpoint_url,
                            "operationName": payload.get("operationName") if isinstance(payload, dict) else None,
                            "status_code": response.status_code,
                            "elapsed_ms": elapsed_ms,
                            "attempt": attempt,
                            "client": "httpx",
                        })
                        if response.status_code == 200:
                            body = response.json()
                            if isinstance(body, dict) and body.get("errors"):
                                self._log("graphql_errors", {"errors": body.get("errors"), "operationName": payload.get("operationName")})
                            return body

                        decision = self.retry_policy.decide(attempt, status_code=response.status_code, error_kind="http_status")
                    except Exception as exc:
                        self._log("graphql_exception", {"error": str(exc), "attempt": attempt, "client": "httpx"})
                        decision = self.retry_policy.decide(attempt, error_kind="exception")

                    if not decision.retry:
                        return {"errors": [{"message": decision.reason, "terminal": decision.terminal}]}
                    await asyncio.sleep(decision.delay_seconds)
                    attempt += 1

    async def _execute_with_requests(self, endpoint_url, payload, headers=None, cookies=None):
        async with self.semaphore:
            attempt = 1
            while True:
                await self.rate_limiter.wait(endpoint_url)
                started = time.time()
                try:
                    status_code, body = await asyncio.to_thread(
                        _post_json_with_requests,
                        endpoint_url,
                        payload,
                        headers or {},
                        cookies or {},
                        self.timeout,
                    )
                    elapsed_ms = int((time.time() - started) * 1000)
                    self._log("graphql_request", {
                        "endpoint_url": endpoint_url,
                        "operationName": payload.get("operationName") if isinstance(payload, dict) else None,
                        "status_code": status_code,
                        "elapsed_ms": elapsed_ms,
                        "attempt": attempt,
                        "client": "requests",
                    })
                    if status_code == 200:
                        if isinstance(body, dict) and body.get("errors"):
                            self._log("graphql_errors", {"errors": body.get("errors"), "operationName": payload.get("operationName")})
                        return body
                    decision = self.retry_policy.decide(attempt, status_code=status_code, error_kind="http_status")
                except Exception as exc:
                    self._log("graphql_exception", {"error": str(exc), "attempt": attempt, "client": "requests"})
                    decision = self.retry_policy.decide(attempt, error_kind="exception")

                if not decision.retry:
                    return {"errors": [{"message": decision.reason, "terminal": decision.terminal}]}
                await asyncio.sleep(decision.delay_seconds)
                attempt += 1

    async def _execute_with_urllib(self, endpoint_url, payload, headers=None, cookies=None):
        async with self.semaphore:
            attempt = 1
            while True:
                await self.rate_limiter.wait(endpoint_url)
                started = time.time()
                try:
                    status_code, body = await asyncio.to_thread(
                        _post_json_with_urllib,
                        endpoint_url,
                        payload,
                        headers or {},
                        cookies or {},
                        self.timeout,
                    )
                    elapsed_ms = int((time.time() - started) * 1000)
                    self._log("graphql_request", {
                        "endpoint_url": endpoint_url,
                        "operationName": payload.get("operationName") if isinstance(payload, dict) else None,
                        "status_code": status_code,
                        "elapsed_ms": elapsed_ms,
                        "attempt": attempt,
                        "client": "urllib",
                    })
                    if status_code == 200:
                        if isinstance(body, dict) and body.get("errors"):
                            self._log("graphql_errors", {"errors": body.get("errors"), "operationName": payload.get("operationName")})
                        return body
                    decision = self.retry_policy.decide(attempt, status_code=status_code, error_kind="http_status")
                except Exception as exc:
                    self._log("graphql_exception", {"error": str(exc), "attempt": attempt, "client": "urllib"})
                    decision = self.retry_policy.decide(attempt, error_kind="exception")

                if not decision.retry:
                    return {"errors": [{"message": decision.reason, "terminal": decision.terminal}]}
                await asyncio.sleep(decision.delay_seconds)
                attempt += 1

    def execute_sync(self, endpoint_url, payload, headers=None, cookies=None):
        return asyncio.run(self.execute(endpoint_url, payload, headers=headers, cookies=cookies))

    async def collect_review_bundle(self, product_url, registry, cookies=None, sku_map=None, operation_names=None):
        """Run mapped review/rating operations for the product URL's skuId."""
        sku_id = lookup_sku_id(product_url, sku_map)
        if not sku_id:
            sku_id = extract_bestbuy_sku_id(product_url)
        if not sku_id:
            sku_id = await asyncio.to_thread(resolve_sku_id_from_product_page, product_url, registry)
        if not sku_id:
            return {"errors": [{"message": "skuId not found in product_url"}]}

        requested_operations = tuple(operation_names or REVIEW_OPERATIONS)
        responses_by_operation = {}
        for operation_name in requested_operations:
            operation = registry.get(operation_name)
            if not operation:
                self._log("graphql_operation_missing", {"operationName": operation_name})
                continue
            endpoint_url = operation.get("endpoint_url")
            payload = build_payload_for_sku(operation, sku_id)
            headers = build_headers_for_url(operation, product_url)
            self._log("graphql_operation_start", {"operationName": operation_name, "skuId": sku_id})
            responses_by_operation[operation_name] = await self.execute(endpoint_url, payload, headers=headers, cookies=cookies)

        operation_errors = {
            operation_name: response.get("errors")
            for operation_name, response in responses_by_operation.items()
            if isinstance(response, dict) and response.get("errors")
        }
        bundle = dict(responses_by_operation)
        bundle["skuId"] = sku_id
        bundle["product_url"] = product_url
        if operation_errors:
            bundle["errors"] = operation_errors
        bundle["parsed"] = parse_review_bundle(bundle)
        return bundle

    def collect_review_bundle_sync(self, product_url, registry, cookies=None, sku_map=None, operation_names=None):
        return asyncio.run(self.collect_review_bundle(
            product_url,
            registry,
            cookies=cookies,
            sku_map=sku_map,
            operation_names=operation_names,
        ))

    def _log(self, event_type, payload):
        if self.audit_log:
            self.audit_log.write(event_type, payload)
        else:
            message = json.dumps({"event_type": event_type, **payload}, ensure_ascii=False)
            try:
                print(message)
            except UnicodeEncodeError:
                print(message.encode("ascii", errors="backslashreplace").decode("ascii"))


class BrowserFetchGraphQLCollector(GraphQLCollector):
    """Run GraphQL POST through Chromium fetch when direct Python HTTP is reset."""

    def __init__(self, *args, page=None, **kwargs):
        super().__init__(*args, **kwargs)
        self.page = page
        self._owns_page = page is None
        self._user_data_path = None

    async def execute(self, endpoint_url, payload, headers=None, cookies=None):
        async with self.semaphore:
            attempt = 1
            while True:
                await self.rate_limiter.wait(endpoint_url)
                started = time.time()
                try:
                    status_code, body = await asyncio.to_thread(
                        self._execute_fetch_sync,
                        endpoint_url,
                        payload,
                        headers or {},
                    )
                    elapsed_ms = int((time.time() - started) * 1000)
                    self._log("graphql_request", {
                        "endpoint_url": endpoint_url,
                        "operationName": payload.get("operationName") if isinstance(payload, dict) else None,
                        "status_code": status_code,
                        "elapsed_ms": elapsed_ms,
                        "attempt": attempt,
                        "client": "browser_fetch",
                    })
                    if status_code == 200:
                        if isinstance(body, dict) and body.get("errors"):
                            self._log("graphql_errors", {"errors": body.get("errors"), "operationName": payload.get("operationName")})
                        return body
                    decision = self.retry_policy.decide(attempt, status_code=status_code, error_kind="http_status")
                except Exception as exc:
                    self._log("graphql_exception", {"error": str(exc), "attempt": attempt, "client": "browser_fetch"})
                    decision = self.retry_policy.decide(attempt, error_kind="exception")

                if not decision.retry:
                    return {"errors": [{"message": decision.reason, "terminal": decision.terminal}]}
                await asyncio.sleep(decision.delay_seconds)
                attempt += 1

    def close(self):
        if self._owns_page and self.page:
            try:
                self.page.quit()
            except Exception:
                try:
                    self.page.close()
                except Exception:
                    pass

    def _ensure_page(self):
        if self.page:
            return self.page
        from DrissionPage import ChromiumOptions, ChromiumPage

        options = ChromiumOptions()
        options.auto_port()
        options.no_imgs(True)
        user_data_path = os.environ.get("BBY_BROWSER_FETCH_USER_DATA_DIR")
        if not user_data_path:
            temp_root = "C:\\tmp" if os.path.isdir("C:\\tmp") else None
            user_data_path = tempfile.mkdtemp(prefix="bby_graphql_fetch_", dir=temp_root)
        self._user_data_path = user_data_path
        options.set_user_data_path(user_data_path)
        if os.environ.get("BBY_BROWSER_FETCH_HEADLESS", "1") == "1":
            options.set_argument("--headless=new")
        self.page = ChromiumPage(options)
        return self.page

    def _execute_fetch_sync(self, endpoint_url, payload, headers):
        page = self._ensure_page()
        referer = headers.get("Referer") or headers.get("referer") or "https://www.bestbuy.com/"
        if not str(getattr(page, "url", "") or "").startswith("https://www.bestbuy.com"):
            page.get(referer)

        result_key = f"__bbyGraphqlFetchResult_{int(time.time() * 1000)}"
        fetch_headers = _sanitize_request_headers(headers)
        fetch_headers.setdefault("content-type", "application/json")
        fetch_headers.setdefault("accept", "application/graphql-response+json,application/json;q=0.9")

        js = f"""
        window[{json.dumps(result_key)}] = null;
        fetch({json.dumps(endpoint_url)}, {{
            method: 'POST',
            credentials: 'include',
            headers: {json.dumps(fetch_headers)},
            body: JSON.stringify({json.dumps(payload)})
        }}).then(async response => {{
            const text = await response.text();
            let body = {{}};
            try {{ body = text ? JSON.parse(text) : {{}}; }}
            catch (error) {{ body = {{errors: [{{message: text.slice(0, 500)}}]}}; }}
            window[{json.dumps(result_key)}] = {{ok: true, status: response.status, body}};
        }}).catch(error => {{
            window[{json.dumps(result_key)}] = {{ok: false, status: 0, error: String(error)}};
        }});
        return true;
        """
        page.run_js(js)

        deadline = time.time() + self.timeout
        while time.time() < deadline:
            result = page.run_js(f"return window[{json.dumps(result_key)}] || null;")
            if result:
                page.run_js(f"delete window[{json.dumps(result_key)}];")
                if not result.get("ok"):
                    raise RuntimeError(result.get("error") or "browser fetch failed")
                return int(result.get("status") or 0), result.get("body") or {}
            time.sleep(0.25)
        raise TimeoutError(f"browser fetch timed out after {self.timeout}s")


def load_graphql_registry(base_dir):
    """Load registry from either mapping output root or crawler/discovery dir."""
    candidates = [
        os.path.join(base_dir, "graphql_registry.json"),
        os.path.join(base_dir, "crawler", "discovery", "graphql_registry.json"),
    ]
    for path in candidates:
        if os.path.exists(path):
            with open(path, encoding="utf-8") as f:
                return json.load(f)
    return {}


def load_sku_map(base_dir):
    """Load URL -> numeric skuId cache produced during GraphQL discovery."""
    candidates = [
        os.path.join(base_dir, "graphql_sku_map.json"),
        os.path.join(base_dir, "crawler", "discovery", "graphql_sku_map.json"),
    ]
    sku_map = {}
    for path in candidates:
        if os.path.exists(path):
            try:
                with open(path, encoding="utf-8") as f:
                    sku_map.update(json.load(f))
            except Exception:
                pass

    for path in _operation_files(base_dir):
        try:
            with open(path, encoding="utf-8") as f:
                operation = json.load(f)
            payload = operation.get("request_payload") or operation.get("request_template") or {}
            headers = operation.get("request_headers") or {}
            referer = headers.get("Referer") or headers.get("referer")
            variables = payload.get("variables") if isinstance(payload, dict) else {}
            sku_id = variables.get("skuId") if isinstance(variables, dict) else None
            if referer and sku_id:
                sku_map.setdefault(referer, {"skuId": str(sku_id)})
        except Exception:
            continue
    return sku_map


def load_graphql_cookies(base_dir):
    """Load browser session cookies captured during GraphQL discovery."""
    candidates = [
        os.path.join(base_dir, "graphql_cookies.json"),
        os.path.join(base_dir, "crawler", "discovery", "graphql_cookies.json"),
    ]
    for path in candidates:
        if not os.path.exists(path):
            continue
        try:
            with open(path, encoding="utf-8") as f:
                payload = json.load(f)
            cookies = payload.get("cookies") if isinstance(payload, dict) else payload
            return cookies if isinstance(cookies, dict) else {}
        except Exception:
            continue
    return {}


def lookup_sku_id(product_url, sku_map):
    if not product_url or not sku_map:
        return None
    normalized = _normalize_url(product_url)
    item_key = _url_tail(normalized)
    for url, value in sku_map.items():
        mapped_url = _normalize_url(url)
        if mapped_url == normalized or (_url_tail(mapped_url) and _url_tail(mapped_url) == item_key):
            if isinstance(value, dict):
                return value.get("skuId")
            return value
    return None


def _operation_files(base_dir):
    candidates = [
        base_dir,
        os.path.join(base_dir, "graphql_map"),
        os.path.join(base_dir, "crawler", "discovery", "graphql_map"),
    ]
    for folder in candidates:
        if not os.path.isdir(folder):
            continue
        for name in os.listdir(folder):
            if name.startswith("graphql_operation_") and name.endswith(".json"):
                yield os.path.join(folder, name)


def _normalize_url(url):
    return str(url or "").replace("\u00a0", " ").split("?", 1)[0].rstrip("/").lower()


def _url_tail(url):
    clean = _normalize_url(url)
    return clean.rsplit("/", 1)[-1] if clean else None


def build_payload_for_sku(operation, sku_id):
    payload = copy.deepcopy(operation.get("request_template") or operation.get("request_payload") or {})
    variables = payload.setdefault("variables", {})
    variables["skuId"] = str(sku_id)
    if payload.get("operationName") == "CustomerReviewList_Init":
        variables.setdefault("onlyIfRelated", True)
        page_size = _review_page_size()
        query = payload.get("query")
        if isinstance(query, str):
            payload["query"] = re.sub(r"pageSize:\s*\d+", f"pageSize: {page_size}", query)
    return payload


def _review_page_size():
    try:
        value = int(os.environ.get("BBY_GRAPHQL_REVIEW_PAGE_SIZE", "20"))
    except ValueError:
        value = 20
    return max(1, min(value, 20))


def build_headers_for_url(operation, product_url):
    headers = dict(operation.get("request_headers") or {})
    headers["Referer"] = product_url
    headers.setdefault("origin", "https://www.bestbuy.com")
    headers.setdefault("content-type", "application/json")
    headers.setdefault("accept", "application/graphql-response+json,application/json;q=0.9")
    return headers


def parse_review_bundle(bundle):
    rating_payload = bundle.get("CustomerRatingCard_Init") or {}
    summary_payload = bundle.get("Ai_Review_Summary_Init") or {}
    review_payload = bundle.get("CustomerReviewList_Init") or {}

    rating_facts = parse_product_facts(rating_payload)
    review_result = collect_reviews(review_payload, max_reviews=20)
    summary = _first_value(summary_payload, ("reviewSummary",))

    return {
        "skuId": bundle.get("skuId"),
        "star_rating": rating_facts.get("star_rating"),
        "count_of_reviews": rating_facts.get("count_of_reviews"),
        "recommendation_intent": _first_value(rating_payload, ("recommendedPercent",)),
        "summarized_review_content": summary,
        "detailed_review_content": review_result.get("reviews"),
        "review_count_collected": review_result.get("count"),
    }


def _first_value(payload, keys):
    found = None

    def walk(value):
        nonlocal found
        if found is not None:
            return
        if isinstance(value, dict):
            for key in keys:
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


def _post_json_with_requests(endpoint_url, payload, headers, cookies, timeout):
    import requests

    request_headers = _sanitize_request_headers(headers)
    request_headers.setdefault("Content-Type", "application/json")
    request_headers.setdefault("Accept", "application/graphql-response+json,application/json;q=0.9")
    request_headers["Accept-Encoding"] = "identity"
    request_headers["Connection"] = "close"

    response = requests.post(
        endpoint_url,
        json=payload,
        headers=request_headers,
        cookies=cookies or {},
        timeout=(10, timeout),
    )
    try:
        body = response.json() if response.content else {}
    except Exception:
        body = {"errors": [{"message": response.text[:500]}]}
    return response.status_code, body


def _post_json_with_urllib(endpoint_url, payload, headers, cookies, timeout):
    request_headers = _sanitize_request_headers(headers)
    request_headers.setdefault("Content-Type", "application/json")
    request_headers.setdefault("Accept", "application/graphql-response+json,application/json;q=0.9")
    request_headers["Accept-Encoding"] = "identity"
    request_headers["Connection"] = "close"
    if cookies:
        request_headers["Cookie"] = "; ".join(f"{key}={value}" for key, value in cookies.items())

    data = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(endpoint_url, data=data, headers=request_headers, method="POST")
    try:
        with urllib.request.urlopen(req, timeout=timeout) as response:
            raw = response.read().decode("utf-8", errors="replace")
            return response.status, json.loads(raw) if raw else {}
    except urllib.error.HTTPError as exc:
        raw = exc.read().decode("utf-8", errors="replace")
        try:
            body = json.loads(raw) if raw else {}
        except Exception:
            body = {"errors": [{"message": raw[:500]}]}
        return exc.code, body


def _sanitize_request_headers(headers):
    skipped = {
        "accept-encoding",
        "connection",
        "content-length",
        "cookie",
        "host",
        "sec-fetch-dest",
        "sec-fetch-mode",
        "sec-fetch-site",
    }
    normalized = {}
    for key, value in (headers or {}).items():
        if not key or value in (None, ""):
            continue
        key_text = str(key)
        if key_text.lower().startswith(":") or key_text.lower() in skipped:
            continue
        normalized[key_text] = str(value)
    return normalized


def resolve_sku_id_from_product_page(product_url, registry=None, timeout=20):
    """Resolve numeric skuId from PDP HTML for opaque /product/... URLs."""
    if not product_url:
        return None

    headers = _headers_for_page_resolve(product_url, registry)
    req = urllib.request.Request(product_url, headers=headers, method="GET")
    try:
        with urllib.request.urlopen(req, timeout=timeout) as response:
            html = response.read().decode("utf-8", errors="replace")
    except Exception:
        return None

    patterns = (
        r'\bSKU\s*[:#]?\s*(\d{5,})\b',
        r'\bSku\s*[:#]?\s*(\d{5,})\b',
        r'"skuId"\s*:\s*"(\d+)"',
        r'"skuId"\s*:\s*(\d+)',
        r'"sku"\s*:\s*\{\s*"skuId"\s*:\s*"(\d+)"',
        r'"productId"\s*:\s*"(\d{6,})"',
        r'"sku_id"\s*:\s*"(\d+)"',
    )
    for pattern in patterns:
        match = re.search(pattern, html)
        if match:
            return match.group(1)
    return None


def _headers_for_page_resolve(product_url, registry=None):
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/148.0.0.0 Safari/537.36",
        "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Referer": product_url,
    }
    if registry:
        for operation in registry.values():
            request_headers = operation.get("request_headers") or {}
            if request_headers.get("User-Agent"):
                headers["User-Agent"] = request_headers["User-Agent"]
                break
    return headers
