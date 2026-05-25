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


if __name__ == "__main__":
    unittest.main()
