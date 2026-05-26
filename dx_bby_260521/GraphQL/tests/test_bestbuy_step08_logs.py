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

    def test_get_it_fast_payload_is_same_batch_operation(self):
        payload = step08.get_it_fast_payload("6623791")

        self.assertEqual(payload["operationName"], "ProductSchemaGetItFastProbe")
        self.assertEqual(payload["variables"]["skuId"], "6623791")
        self.assertIn("fulfillmentGetItFastOptions", payload["query"])
        self.assertNotIn("ProductFulfillmentInput", payload["query"])
        self.assertNotIn("fulfillmentOptions(input:$fulfillmentInput)", payload["query"])
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
        self.assertEqual(values["fastest_delivery"], "Get it by tomorrow")
        self.assertEqual(values["delivery_availability"], "")

    def test_detail_batch_request_entries_maps_multiple_skus(self):
        old_fetch_compare = step08.FETCH_COMPARE
        old_fetch_get_it_fast = step08.FETCH_GET_IT_FAST
        step08.FETCH_COMPARE = True
        step08.FETCH_GET_IT_FAST = True
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

        self.assertEqual(len(entries), 2)
        self.assertEqual(len(payloads), 8)
        self.assertEqual(entries[0]["indices"], {"detail": 0, "review": 1, "compare": 2, "get_it_fast": 3})
        self.assertEqual(entries[1]["indices"], {"detail": 4, "review": 5, "compare": 6, "get_it_fast": 7})
        self.assertEqual(payloads[0]["variables"]["skuId"], "6639210")
        self.assertEqual(payloads[4]["variables"]["skuId"], "6670264")


if __name__ == "__main__":
    unittest.main()
