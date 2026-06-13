import sys
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from bestbuy import step08_detail_enrichment as step08  # noqa: E402


class Step08LogTests(unittest.TestCase):
    def test_item_log_is_concise(self):
        old_fetch_compare = step08.FETCH_COMPARE
        step08.FETCH_COMPARE = True
        try:
            line = step08.process_log_line(
                1,
                1,
                "6623791",
                {"success": True, "final_compare_name_count": 4},
                {"success": True, "review_count_returned": 20},
                {"success": True, "recommendation_count": 3},
                fetched_detail=True,
                fetched_review=False,
                fetched_compare=False,
            )
        finally:
            step08.FETCH_COMPARE = old_fetch_compare

        self.assertEqual(
            line,
            "[detail:item] 1/1 sku=6623791 status=ok detail=ok(fetch) "
            "review=ok(cache,reviews:20) compare=ok(cache,recs:3) similar=4",
        )
        self.assertNotIn("run_attempts", line)
        self.assertNotIn("fulfillment_batch", line)
        self.assertNotIn("json_response", line)

    def test_item_log_shows_failure_context_only_when_needed(self):
        old_fetch_compare = step08.FETCH_COMPARE
        step08.FETCH_COMPARE = False
        try:
            line = step08.process_log_line(
                1,
                1,
                "6623791",
                {
                    "success": False,
                    "status_code": 422,
                    "error": "http_422_RESP001: Could not get content (RESP001)",
                },
                {"success": False, "status_code": 422, "error": "review20_missing"},
                {},
                fetched_detail=True,
                fetched_review=False,
                fetched_compare=False,
            )
        finally:
            step08.FETCH_COMPARE = old_fetch_compare

        self.assertIn("status=fail", line)
        self.assertIn("detail=fail(fetch,http:422,http_422_RESP001", line)
        self.assertIn("review=fail(cache,http:422,review20_missing)", line)
        self.assertIn("compare=off", line)

    def test_direct_detail_payload_excludes_fulfillment(self):
        payload = step08.fallback_review20_payload("6623791")
        self.assertNotIn("fulfillmentInput", payload["variables"])
        self.assertNotIn("ProductFulfillmentInput", payload["query"])
        self.assertNotIn("fulfillmentOptions", payload["query"])

    def test_review_attention_detects_partial_review_texts(self):
        row = {
            "star_rating": "4.8",
            "count_of_reviews": "22",
            "detailed_review_content": " ||| ".join(f"review{i} - good" for i in range(1, 17)),
            "recommendation_intent": "95% would recommend to a friend",
        }

        self.assertTrue(step08.review_output_needs_attention(row))
        self.assertEqual(step08.review_output_attention_reason(row), "review20_partial_16_of_20")

    def test_review_attention_accepts_expected_review_texts(self):
        row = {
            "star_rating": "4.8",
            "count_of_reviews": "2",
            "detailed_review_content": "review1 - good ||| review2 - also good",
            "recommendation_intent": "100% would recommend to a friend",
        }

        self.assertFalse(step08.review_output_needs_attention(row))

    def test_get_it_fast_payload_is_same_batch_operation(self):
        payload = step08.get_it_fast_payload("6623791")

        self.assertEqual(payload["operationName"], "ProductSchemaGetItFastProbe")
        self.assertEqual(payload["variables"]["skuId"], "6623791")
        self.assertIn("fulfillmentGetItFastOptions", payload["query"])
        self.assertNotIn("ProductFulfillmentInput", payload["query"])
        self.assertNotIn("fulfillmentOptions(input:$fulfillmentInput)", payload["query"])
        self.assertNotIsInstance(payload, list)

    def test_fulfillment_dynamic_payload_is_same_batch_operation(self):
        payload = step08.fulfillment_dynamic_payload("6623791")

        self.assertEqual(payload["operationName"], "FulfillmentOptionHook_FulfillmentDynamicQuery")
        self.assertEqual(payload["variables"]["skuId"], "6623791")
        self.assertIn("fulfillmentInput", payload["variables"])
        self.assertIn("productPriceInput", payload["variables"])
        self.assertIn("fulfillmentOption", payload["variables"]["fulfillmentInput"]["buttonState"])
        self.assertIsNone(payload["variables"]["fulfillmentInput"]["buttonState"]["fulfillmentOption"])
        self.assertNotIn("usePriceWithCart", payload["variables"]["productPriceInput"])
        self.assertIn("ProductFulfillmentInput", payload["query"])
        self.assertIn("fulfillmentOptions(input:$fulfillmentInput)", payload["query"])
        self.assertIn("shippingAvailability", payload["query"])
        self.assertIn("deliveryAvailability", payload["query"])
        self.assertIn("ispuAvailability", payload["query"])
        self.assertNotIsInstance(payload, list)

    def test_get_it_fast_values_fill_pickup_and_fastest_only(self):
        item = {
            "data": {
                "fulfillmentGetItFastOptions": {
                    "shippingCutOffDetails": {"getItBy": "tomorrow", "getItByDate": "2026-05-27"},
                    "storeCutOffDetails": [{"getItBy": "today", "getItByDate": "2026-05-25"}],
                }
            }
        }

        values = step08.get_it_fast_availability_values(item)

        self.assertEqual(values["pick_up_availability"], "Pick up today")
        self.assertEqual(values["fastest_delivery"], "Get it tomorrow \u2022 FREE")
        self.assertEqual(values["delivery_availability"], "")

    def test_detail_batch_request_entries_maps_multiple_skus(self):
        old_fetch_compare = step08.FETCH_COMPARE
        old_fetch_get_it_fast = step08.FETCH_GET_IT_FAST
        old_fetch_fulfillment_dynamic = step08.FETCH_FULFILLMENT_DYNAMIC
        step08.FETCH_COMPARE = True
        step08.FETCH_GET_IT_FAST = True
        step08.FETCH_FULFILLMENT_DYNAMIC = False
        try:
            payloads, entries = step08.detail_batch_request_entries(
                [
                    {"sku_id": "6639210", "product_url": "https://www.bestbuy.com/site/-/6639210.p?skuId=6639210"},
                    {"sku_id": "6670264", "product_url": "https://www.bestbuy.com/site/-/6670264.p?skuId=6670264"},
                ]
            )
        finally:
            step08.FETCH_COMPARE = old_fetch_compare
            step08.FETCH_GET_IT_FAST = old_fetch_get_it_fast
            step08.FETCH_FULFILLMENT_DYNAMIC = old_fetch_fulfillment_dynamic

        self.assertEqual(len(entries), 2)
        self.assertEqual(len(payloads), 8)
        self.assertEqual(entries[0]["indices"], {"detail": 0, "review": 1, "compare": 2, "get_it_fast": 3})
        self.assertEqual(entries[1]["indices"], {"detail": 4, "review": 5, "compare": 6, "get_it_fast": 7})
        self.assertEqual(payloads[0]["variables"]["skuId"], "6639210")
        self.assertEqual(payloads[4]["variables"]["skuId"], "6670264")

    def test_detail_batch_request_entries_can_use_fulfillment_dynamic_instead_of_get_it_fast(self):
        old_fetch_compare = step08.FETCH_COMPARE
        old_fetch_get_it_fast = step08.FETCH_GET_IT_FAST
        old_fetch_fulfillment_dynamic = step08.FETCH_FULFILLMENT_DYNAMIC
        step08.FETCH_COMPARE = True
        step08.FETCH_GET_IT_FAST = True
        step08.FETCH_FULFILLMENT_DYNAMIC = True
        try:
            payloads, entries = step08.detail_batch_request_entries(
                [
                    {"sku_id": "6639210", "product_url": "https://www.bestbuy.com/site/-/6639210.p?skuId=6639210"},
                ]
            )
        finally:
            step08.FETCH_COMPARE = old_fetch_compare
            step08.FETCH_GET_IT_FAST = old_fetch_get_it_fast
            step08.FETCH_FULFILLMENT_DYNAMIC = old_fetch_fulfillment_dynamic

        self.assertEqual(entries[0]["indices"], {"detail": 0, "review": 1, "compare": 2, "fulfillment_dynamic": 3})
        self.assertEqual([payload["operationName"] for payload in payloads], [
            "ProductSchema_init",
            "ProductSchema_init",
            "GetCompareProduct",
            "FulfillmentOptionHook_FulfillmentDynamicQuery",
        ])


if __name__ == "__main__":
    unittest.main()
