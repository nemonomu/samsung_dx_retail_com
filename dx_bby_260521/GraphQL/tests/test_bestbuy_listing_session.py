import json
import sys
import tempfile
import types
import unittest
from unittest.mock import patch
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

try:
    import requests  # noqa: F401
except ModuleNotFoundError:
    requests_module = types.ModuleType("requests")

    class RequestException(Exception):
        pass

    requests_module.RequestException = RequestException
    sys.modules["requests"] = requests_module

try:
    import zenrows  # noqa: F401
except ModuleNotFoundError:
    zenrows_module = types.ModuleType("zenrows")

    class ZenRowsClient:
        pass

    zenrows_module.ZenRowsClient = ZenRowsClient
    sys.modules["zenrows"] = zenrows_module

try:
    import bs4  # noqa: F401
except ModuleNotFoundError:
    bs4_module = types.ModuleType("bs4")

    class BeautifulSoup:
        pass

    bs4_module.BeautifulSoup = BeautifulSoup
    sys.modules["bs4"] = bs4_module

import bestbuy.step01_main_list as listing_step  # noqa: E402


class FakeResponse:
    def __init__(self, status_code=200, headers=None, text=""):
        self.status_code = status_code
        self.headers = headers or {}
        self.text = text


class FakeClient:
    def __init__(self, response):
        self.response = response
        self.calls = []

    def get(self, url, params=None, headers=None, timeout=None):
        self.calls.append(
            {
                "url": url,
                "params": params or {},
                "headers": headers or {},
                "timeout": timeout,
            }
        )
        return self.response

    def post(self, url, params=None, headers=None, data=None, timeout=None):
        self.calls.append(
            {
                "url": url,
                "params": params or {},
                "headers": headers or {},
                "data": data,
                "timeout": timeout,
            }
        )
        return self.response


class BestBuyListingSessionTests(unittest.TestCase):
    def test_default_listing_collection_mode_is_dom(self):
        self.assertEqual(listing_step.LISTING_COLLECTION_MODE, "dom")

    def test_dom_manifest_transport_does_not_require_graphql_fetch_mode(self):
        with patch.object(listing_step, "LISTING_COLLECTION_MODE", "dom"):
            self.assertEqual(listing_step.manifest_fetch_transports(), ["zenrows_html_dom"])

    def test_dom_listing_collection_uses_get_not_graphql_post(self):
        html = """
        <html><body>
          <ul class="product-grid-view-container">
            <li class="product-list-item" data-product-id="123">
              <div class="skeleton-product-grid-view"></div>
            </li>
          </ul>
          <script>
          (window[Symbol.for("ApolloSSRDataTransport")] ??= []).push({"events":[
            {"type":"data","result":{"data":{"productBySkuId":{
              "__typename":"Product",
              "skuId":"123",
              "bsin":"JDOM123"
            }}}}
          ]})
          </script>
        </body></html>
        """
        client = FakeClient(FakeResponse(200, {"X-Request-Cost": "0.001"}, html))

        with (
            tempfile.TemporaryDirectory() as tmpdir,
            patch.object(listing_step, "RUN_ROOT", Path(tmpdir)),
            patch.object(listing_step, "LISTING_COLLECTION_MODE", "dom"),
            patch.object(listing_step, "LISTING_HTML_FALLBACK_MIN_ROWS", 1),
            patch.object(listing_step, "LISTING_MAX_ATTEMPTS", 1),
        ):
            listing_step.make_dirs()
            _response_json, meta, rows = listing_step.collect_listing_page(
                1,
                {"source_type": "dom_html"},
                client,
                listing_step.ListingSessionState(),
                [],
            )

        self.assertEqual(meta["transport"], "zenrows_html_dom")
        self.assertEqual([call for call in client.calls if "data" in call], [])
        self.assertEqual(len(client.calls), 1)
        self.assertEqual(client.calls[0]["url"], listing_step.build_search_url(1))
        self.assertEqual([row["sku_id"] for row in rows], ["123"])

    def test_listing_payload_keeps_fulfillment_by_default(self):
        operation = {
            "operationName": "PlpView_ProductList_Init",
            "variables": {},
            "query": "query Q { product { skuId fulfillmentOptions { shippingDetails } name } }",
        }

        payload = listing_step.prepare_product_list_payload(operation, 1)

        self.assertIn("fulfillmentOptions", payload["query"])
        self.assertIn("skuId", payload["query"])
        self.assertIn("name", payload["query"])

    def test_listing_payload_can_strip_fulfillment_when_explicitly_enabled(self):
        operation = {
            "operationName": "PlpView_ProductList_Init",
            "variables": {},
            "query": "query Q { product { skuId fulfillmentOptions { shippingDetails } name } }",
        }

        with patch.object(listing_step, "SANITIZE_PRODUCT_LIST_QUERY", True), patch.object(
            listing_step, "STRIP_PRODUCT_LIST_FULFILLMENT", True
        ):
            payload = listing_step.prepare_product_list_payload(operation, 1)

        self.assertNotIn("fulfillmentOptions", payload["query"])
        self.assertIn("skuId", payload["query"])
        self.assertIn("name", payload["query"])

    def test_default_listing_mode_is_stateless_and_does_not_reuse_cookies(self):
        state = listing_step.ListingSessionState()

        received = state.update_from_headers(
            {
                "Zr-Cookies": "SID=session-a; _abck=akamai-a; bm_sz=bot-a",
            }
        )

        self.assertEqual(received, 0)
        self.assertIsNone(state.session_id)
        self.assertEqual(state.cookies, {})
        self.assertEqual(state.cookie_header(), "")
        self.assertNotIn("session_id", listing_step.zenrows_params(state.session_id))

    def test_optional_session_keeps_cookie_values_in_memory_and_builds_cookie_header(self):
        with patch.object(listing_step, "LISTING_SESSION_ENABLED", True):
            state = listing_step.ListingSessionState()

            received = state.update_from_headers(
                {
                    "Zr-Cookies": "SID=session-a; _abck=akamai-a; bm_sz=bot-a",
                }
            )

            self.assertEqual(received, 3)
            self.assertEqual(state.cookies["SID"], "session-a")
            self.assertIn("SID=session-a", state.cookie_header())
            self.assertIn("_abck=akamai-a", state.cookie_header())

    def test_bootstrap_uses_session_id_and_captures_response_cookies(self):
        with patch.object(listing_step, "LISTING_SESSION_ENABLED", True):
            state = listing_step.ListingSessionState()
            client = FakeClient(
                FakeResponse(
                    headers={
                        "Zr-Cookies": "SID=session-a; _abck=akamai-a",
                        "X-Request-Cost": "0.001",
                        "X-Request-Id": "request-a",
                        "Zr-Gatewaystatus": "200",
                    }
                )
            )

            summary = listing_step.bootstrap_listing_session(client, state, page=1)

            self.assertEqual(client.calls[0]["params"]["session_id"], str(state.session_id))
            self.assertEqual(client.calls[0]["headers"]["referer"], "https://www.bestbuy.com/")
            self.assertEqual(state.cookies["SID"], "session-a")
            self.assertEqual(summary["received_cookie_count"], 2)
            self.assertTrue(state.bootstrapped)

    def test_graphql_headers_reuse_bootstrap_cookies(self):
        with patch.object(listing_step, "LISTING_SESSION_ENABLED", True):
            state = listing_step.ListingSessionState()
            state.update_from_headers({"Zr-Cookies": "SID=session-a; _abck=akamai-a"})

            headers = listing_step.listing_headers(2, state, graphql=True)

            self.assertIn("SID=session-a", headers["cookie"])
            self.assertEqual(headers["content-type"], "application/json")
            self.assertTrue(headers["referer"].startswith("https://www.bestbuy.com/"))

    def test_cookie_headers_are_redacted_before_artifact_logging(self):
        headers = listing_step.redacted_response_headers(
            {
                "Zr-Cookies": "SID=secret",
                "Zr-Set-Cookie": "SID=secret; path=/",
                "X-Request-Id": "request-a",
            }
        )

        self.assertEqual(headers["Zr-Cookies"], "[REDACTED]")
        self.assertEqual(headers["Zr-Set-Cookie"], "[REDACTED]")
        self.assertEqual(headers["X-Request-Id"], "request-a")

    def test_resp001_requires_a_new_session_but_other_422_does_not_loop(self):
        self.assertEqual(
            listing_step.listing_retry_reason(
                [],
                {"status_code": 422, "zenrows_error_code": "RESP001"},
                {"code": "RESP001"},
            ),
            "resp001",
        )
        self.assertEqual(
            listing_step.listing_retry_reason(
                [],
                {"status_code": 422, "zenrows_error_code": "REQ_INVALID_PARAMS"},
                {"code": "REQ_INVALID_PARAMS"},
            ),
            "",
        )

    def test_transient_status_uses_exponential_backoff(self):
        self.assertEqual(
            listing_step.listing_retry_reason([], {"status_code": 429}, {}),
            "http_429",
        )
        self.assertEqual(
            [listing_step.listing_retry_delay(attempt) for attempt in range(1, 5)],
            [2.0, 4.0, 8.0, 8.0],
        )

    def test_session_reset_discards_old_cookies(self):
        with patch.object(listing_step, "LISTING_SESSION_ENABLED", True):
            state = listing_step.ListingSessionState()
            old_generation = state.generation
            state.update_from_headers({"Zr-Cookies": "SID=session-a; _abck=akamai-a"})

            state.reset("resp001")

            self.assertEqual(state.generation, old_generation + 1)
            self.assertIsNotNone(state.session_id)
            self.assertEqual(state.cookies, {})
            self.assertFalse(state.bootstrapped)
            self.assertEqual(state.last_reset_reason, "resp001")

    def test_manual_mode_keeps_explicit_protection_and_session_parameters(self):
        with patch.object(listing_step, "LISTING_SESSION_ENABLED", True):
            state = listing_step.ListingSessionState()
            with patch.dict(
                listing_step.os.environ,
                {
                    "BESTBUY_GRAPHQL_MODE_AUTO": "0",
                    "BESTBUY_GRAPHQL_PREMIUM_PROXY": "1",
                    "BESTBUY_GRAPHQL_JS_RENDER": "1",
                },
            ):
                params = listing_step.zenrows_params(state.session_id)

            self.assertNotIn("mode", params)
            self.assertEqual(params["premium_proxy"], "true")
            self.assertEqual(params["js_render"], "true")
            self.assertEqual(params["wait"], str(listing_step.LISTING_WAIT_MS))
            self.assertEqual(params["proxy_country"], "us")
            self.assertEqual(params["session_id"], str(state.session_id))

    def test_manual_graphql_post_reuses_cookie_and_refreshes_cookie_jar(self):
        with patch.object(listing_step, "LISTING_SESSION_ENABLED", True):
            state = listing_step.ListingSessionState()
            state.update_from_headers({"Zr-Cookies": "SID=session-a; _abck=akamai-a"})
            client = FakeClient(
                FakeResponse(
                    headers={
                        "Zr-Cookies": "SID=session-a; _abck=akamai-b",
                    }
                )
            )
            payload = {"operationName": "Q", "variables": {}, "query": "query Q { ok }"}

            listing_step.post_graphql(client, payload, 1, "zenrows", state)

            self.assertIn("SID=session-a", client.calls[0]["headers"]["cookie"])
            self.assertEqual(client.calls[0]["params"]["session_id"], str(state.session_id))
            self.assertEqual(state.cookies["_abck"], "akamai-b")

    def test_auto_mode_does_not_send_conflicting_manual_parameters(self):
        with patch.object(listing_step, "LISTING_SESSION_ENABLED", True):
            state = listing_step.ListingSessionState()
            with patch.dict(
                listing_step.os.environ,
                {
                    "BESTBUY_GRAPHQL_MODE_AUTO": "1",
                    "BESTBUY_GRAPHQL_PREMIUM_PROXY": "1",
                    "BESTBUY_GRAPHQL_JS_RENDER": "1",
                },
            ):
                params = listing_step.zenrows_params(state.session_id)

            self.assertEqual(params["mode"], "auto")
            self.assertEqual(params["proxy_country"], "us")
            self.assertEqual(params["wait"], str(listing_step.LISTING_WAIT_MS))
            self.assertEqual(params["session_id"], str(state.session_id))
            self.assertNotIn("premium_proxy", params)
            self.assertNotIn("js_render", params)
            self.assertNotIn("js_instructions", params)

    def test_listing_country_click_instruction_is_sent_for_manual_render(self):
        with patch.object(listing_step, "LISTING_COUNTRY_CLICK_ENABLED", True), patch.object(
            listing_step,
            "LISTING_COUNTRY_CLICK_SELECTOR",
            ".us-link",
        ):
            params = listing_step.zenrows_params()

        self.assertEqual(params["js_render"], "true")
        self.assertEqual(json.loads(params["js_instructions"]), [{"click": ".us-link"}])

    def test_bestbuy_nosplash_url_adds_intl_without_dropping_query(self):
        url = "https://www.bestbuy.com/site/searchpage.jsp?id=pcat17071&st=tv&cp=3"

        result = listing_step.bestbuy_nosplash_url(url)

        self.assertIn("id=pcat17071", result)
        self.assertIn("st=tv", result)
        self.assertIn("cp=3", result)
        self.assertIn("intl=nosplash", result)

    def test_listing_recovery_profiles_follow_official_zenrows_options(self):
        with patch.object(listing_step, "LISTING_RECOVERY_PROFILE_NAMES", ["wait", "session_wait", "auto"]):
            profiles = listing_step.listing_recovery_profiles()

        self.assertEqual([profile["name"] for profile in profiles], ["wait", "session_wait", "auto"])
        wait_params = listing_step.zenrows_params(request_profile=profiles[0])
        session_wait_params = listing_step.zenrows_params(request_profile=profiles[1])
        auto_params = listing_step.zenrows_params(request_profile=profiles[2])

        self.assertEqual(wait_params["wait"], str(listing_step.LISTING_RECOVERY_WAIT_MS))
        self.assertEqual(wait_params["js_render"], "true")
        self.assertIn("session_id", session_wait_params)
        self.assertEqual(session_wait_params["wait"], str(listing_step.LISTING_RECOVERY_WAIT_MS))
        self.assertEqual(session_wait_params["js_render"], "true")
        self.assertEqual(auto_params["mode"], "auto")
        self.assertEqual(auto_params["proxy_country"], "us")

    def test_html_dom_parser_only_reads_scoped_product_list_cards(self):
        html = """
        <html><body>
          <div data-product-id="999">
            <a href="/product/not-a-plp-card/J999/sku/999">Wrong placement</a>
            <h3 class="product-title">Wrong - Should Not Parse</h3>
          </div>
          <ul class="product-grid-view-container">
            <li class="product-list-item" data-product-id="111">
              <a class="product-list-item-link" href="/product/samsung-tv/JVALID111">
                <h3 class="product-title" title="Samsung - Valid TV">
                  <span class="first-title">Samsung</span>
                </h3>
              </a>
              <p class="visually-hidden">Rating 4.7 out of 5 stars with 1,234 reviews</p>
              <div data-testid="price-block-customer-price">$399.99</div>
              <div data-testid="price-block-regular-price">$499.99</div>
              <div data-testid="price-block-total-savings-text">Save $100</div>
            </li>
            <li class="product-list-item" data-product-id="222">
              <a class="product-list-item-link" href="/product/mismatch/JMISMATCH/sku/999">
                <h3 class="product-title" title="Mismatch - Should Not Parse"></h3>
              </a>
            </li>
          </ul>
          <div class="sponsored-content product-list-sponsored-wrapper-grid-view">
            <div class="product-list-item" data-product-id="333">
              <span class="sponsored">Sponsored</span>
              <a class="product-list-item-link" href="/product/lg-tv/JVALID333/sku/333">
                <h3 class="product-title" title="LG - Sponsored TV">
                  <span class="first-title">LG</span>
                </h3>
              </a>
              <div data-testid="price-block-customer-price">$299</div>
            </div>
          </div>
        </body></html>
        """

        rows = listing_step.parse_html_dom_rows(1, html, "raw/html_dom_fallback/page_001.html")

        self.assertEqual([row["sku_id"] for row in rows], ["111", "333"])
        self.assertEqual(rows[0]["container_type"], "organic_product")
        self.assertEqual(rows[0]["organic_rank"], 1)
        self.assertEqual(rows[0]["global_organic_rank"], 1)
        self.assertEqual(rows[0]["rating"], "4.7")
        self.assertEqual(rows[0]["review_count"], "1234")
        self.assertEqual(rows[0]["customer_price"], "$399.99")
        self.assertEqual(rows[0]["regular_price"], "$499.99")
        self.assertEqual(rows[0]["total_savings"], "$100")
        self.assertEqual(rows[1]["container_type"], "sponsored_ingrid")
        self.assertEqual(rows[1]["organic_rank"], "")
        self.assertEqual(rows[1]["sponsored_rank"], 1)

    def test_html_dom_parser_joins_apollo_data_only_for_scoped_cards(self):
        html = """
        <html><body>
          <ul class="product-grid-view-container">
            <li class="product-list-item" data-product-id="444">
              <div class="skeleton-product-grid-view"></div>
            </li>
          </ul>
          <script>
          (window[Symbol.for("ApolloSSRDataTransport")] ??= []).push({"events":[
            {"type":"data","result":{"data":{"productBySkuId":{
              "__typename":"Product",
              "skuId":"444",
              "bsin":"JVALID444",
              "brand":"Sony",
              "name":{"short":"Sony - Scoped Product"},
              "url":{"pdp":"https://www.bestbuy.com/product/sony-scoped-product/JVALID444"},
              "primaryImage":{"piscesHref":"https://pisces.bbystatic.com/image.jpg"},
              "reviewInfo":{"averageRating":4.5,"reviewCount":12},
              "price":{"displayableCustomerPrice":599.99,"displayableRegularPrice":699.99,"totalSavings":100}
            }}}},
            {"type":"data","result":{"data":{"productBySkuId":{
              "__typename":"Product",
              "skuId":"555",
              "bsin":"JWRONG555",
              "name":{"short":"Wrong Unscoped Product"},
              "url":{"pdp":"https://www.bestbuy.com/product/wrong/JWRONG555"}
            }}}}
          ]})
          </script>
        </body></html>
        """

        rows = listing_step.parse_html_dom_rows(1, html, "raw/html_dom_fallback/page_001.html")

        self.assertEqual([row["sku_id"] for row in rows], ["444"])
        self.assertEqual(rows[0]["product_name"], "Sony - Scoped Product")
        self.assertEqual(rows[0]["product_url"], "https://www.bestbuy.com/product/sony-scoped-product/JVALID444/sku/444")
        self.assertEqual(rows[0]["customer_price"], 599.99)

    def test_html_dom_parser_adds_sku_fallback_url_for_skeleton_cards(self):
        html = """
        <html><body>
          <ul class="product-grid-view-container">
            <li class="product-list-item" data-product-id="666">
              <div class="skeleton-product-grid-view"></div>
            </li>
          </ul>
          <script>
          (window[Symbol.for("ApolloSSRDataTransport")] ??= []).push({"events":[
            {"type":"data","result":{"data":{"productBySkuId":{
              "__typename":"Product",
              "skuId":"666",
              "bsin":"JSKELETON666"
            }}}}
          ]})
          </script>
        </body></html>
        """

        rows = listing_step.parse_html_dom_rows(1, html, "raw/html_dom_fallback/page_001.html")

        self.assertEqual([row["sku_id"] for row in rows], ["666"])
        self.assertEqual(rows[0]["product_url"], "https://www.bestbuy.com/site/-/666.p?skuId=666&intl=nosplash")

    def test_html_dom_fallback_params_include_optional_scroll_instructions(self):
        with (
            patch.object(listing_step, "LISTING_HTML_FALLBACK_SCROLL_ENABLED", True),
            patch.object(listing_step, "LISTING_HTML_FALLBACK_SCROLL_STEPS", 2),
            patch.object(listing_step, "LISTING_HTML_FALLBACK_SCROLL_Y", 1500),
            patch.object(listing_step, "LISTING_HTML_FALLBACK_SCROLL_WAIT_MS", 900),
            patch.object(listing_step, "LISTING_HTML_FALLBACK_SCROLL_FINAL_WAIT_MS", 1100),
            patch.object(listing_step, "LISTING_HTML_FALLBACK_SCROLL_RESET_TOP", True),
        ):
            params = listing_step.html_dom_fallback_params()

        instructions = json.loads(params["js_instructions"])
        self.assertEqual(
            instructions,
            [
                {"wait": 900},
                {"scroll_y": 1500},
                {"wait": 900},
                {"scroll_y": 1500},
                {"wait": 900},
                {"wait": 1100},
                {"evaluate": "window.scrollTo(0, 0);"},
                {"wait": 900},
            ],
        )

    def test_html_dom_fallback_params_can_disable_scroll_instructions(self):
        with patch.object(listing_step, "LISTING_HTML_FALLBACK_SCROLL_ENABLED", False):
            params = listing_step.html_dom_fallback_params()

        self.assertNotIn("js_instructions", params)

    def test_delayed_retry_preserves_prior_attempt_costs(self):
        previous = {
            "attempt_count": 2,
            "attempt_status_codes": "422,422",
            "attempt_costs": "0,0",
            "attempt_retry_reasons": "resp001,resp001",
            "attempt_retry_delays": "2,4",
            "attempt_profiles": "base,base",
            "attempt_errors": "RESP001",
            "recovery_attempt_count": 1,
        }
        current = {
            "attempt_count": 1,
            "attempt_status_codes": "200",
            "attempt_costs": "0.0027996",
            "attempt_retry_reasons": "",
            "attempt_retry_delays": "",
            "attempt_profiles": "dom",
            "attempt_errors": "",
            "recovery_attempt_count": 0,
            "status_code": 200,
            "total_occurrence_count": 24,
        }

        merged = listing_step.merge_listing_attempt_history(previous, current)

        self.assertEqual(merged["status_code"], 200)
        self.assertEqual(merged["total_occurrence_count"], 24)
        self.assertEqual(merged["attempt_count"], 3)
        self.assertEqual(merged["attempt_status_codes"], "422,422,200")
        self.assertEqual(merged["attempt_costs"], "0,0,0.0027996")
        self.assertEqual(merged["recovery_attempt_count"], 1)

    def test_html_dom_parser_keeps_organic_and_sponsored_occurrences_for_same_sku(self):
        html = """
        <html><body>
          <ul class="product-grid-view-container">
            <li class="product-list-item" data-product-id="777">
              <a class="product-list-item-link" href="/product/roku-tv/JVALID777/sku/777">
                <h3 class="product-title" title="Roku - Organic TV"></h3>
              </a>
            </li>
          </ul>
          <div class="sponsored-content product-list-sponsored-wrapper-grid-view">
            <div class="product-list-item" data-product-id="777">
              <div class="sponsored">Sponsored</div>
              <a class="product-list-item-link" href="/product/roku-tv/JVALID777/sku/777">
                <h3 class="product-title" title="Roku - Sponsored TV"></h3>
              </a>
            </div>
          </div>
        </body></html>
        """

        rows = listing_step.parse_html_dom_rows(1, html, "raw/html_dom_fallback/page_001.html")

        self.assertEqual([row["sku_id"] for row in rows], ["777", "777"])
        self.assertEqual([row["container_type"] for row in rows], ["organic_product", "sponsored_ingrid"])
        self.assertEqual(rows[0]["organic_rank"], 1)
        self.assertEqual(rows[1]["organic_rank"], "")
        self.assertEqual(rows[1]["sku_status"], "Sponsored")

    def test_status_200_with_zero_rows_is_retryable(self):
        self.assertEqual(
            listing_step.listing_retry_reason([], {"status_code": 200}, {"data": {"products": []}}),
            "empty_rows",
        )


if __name__ == "__main__":
    unittest.main()
