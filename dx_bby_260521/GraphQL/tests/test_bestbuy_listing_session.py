import sys
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
    def test_listing_payload_strips_fulfillment(self):
        operation = {
            "operationName": "PlpView_ProductList_Init",
            "variables": {},
            "query": "query Q { product { skuId fulfillmentOptions { shippingDetails } name } }",
        }

        payload = listing_step.prepare_product_list_payload(operation, 1)

        self.assertNotIn("fulfillmentOptions", payload["query"])
        self.assertIn("skuId", payload["query"])
        self.assertIn("name", payload["query"])

    def test_session_keeps_cookie_values_in_memory_and_builds_cookie_header(self):
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
        self.assertEqual(params["proxy_country"], "us")
        self.assertEqual(params["session_id"], str(state.session_id))

    def test_manual_graphql_post_reuses_cookie_and_refreshes_cookie_jar(self):
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
        self.assertEqual(params["session_id"], str(state.session_id))
        self.assertNotIn("premium_proxy", params)
        self.assertNotIn("js_render", params)


if __name__ == "__main__":
    unittest.main()
