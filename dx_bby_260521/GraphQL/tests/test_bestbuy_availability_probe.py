import sys
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from bestbuy.step00_availability_graphql_probe import (  # noqa: E402
    analyze_curl_reference_text,
    detail_with_fulfillment_payload,
)


class AvailabilityProbeReferenceTests(unittest.TestCase):
    def test_reference_audit_separates_graphql_posts_from_fulfillment_endpoint(self):
        sample = r"""
curl ^"https://www.bestbuy.com/gateway/graphql^" ^
  -H ^"x-requested-for-operation-name: PageLoadAnalyticsData_Init_Pdp^" ^
  --data-raw ^"^{^\^"operationName^\^":^\^"PageLoadAnalyticsData_Init_Pdp^\^",^\^"query^\^":^\^"fragment AnalyticsFulfillmentOptionsFragment on Product { skuId }^\^"^}^" &
curl ^"https://www.bestbuy.com/gateway/graphql/fulfillment?variables=^%^7B^%^22fulfillmentOptionsInput^%^22^%^3A^%^7B^%^22sku^%^22^%^3A^%^226623791^%^22^%^2C^%^22shipping^%^22^%^3A^%^7B^%^22destinationZipCode^%^22^%^3A^%^2210010^%^22^%^7D^%^2C^%^22inStorePickup^%^22^%^3A^%^7B^%^22storeId^%^22^%^3A^%^22482^%^22^%^7D^%^2C^%^22buttonState^%^22^%^3A^%^7B^%^22context^%^22^%^3A^%^22PDP^%^22^%^2C^%^22fulfillmentOption^%^22^%^3A^%^22PICKUP^%^22^%^7D^%^7D^%^7D^" ^
  -H ^"x-requested-for-operation-name: PageLoadAnalyticsData_Init_Pdp^" &
"""
        summary = analyze_curl_reference_text(sample)

        self.assertEqual(summary["graphql_post_count"], 1)
        self.assertEqual(summary["fulfillment_endpoint_count"], 1)
        self.assertEqual(summary["graphql_posts_with_availability_fields"], [])

        endpoint = summary["fulfillment_endpoint_examples"][0]["fulfillment_input"]
        self.assertEqual(endpoint["sku"], "6623791")
        self.assertEqual(endpoint["context"], "PDP")
        self.assertEqual(endpoint["button"], "PICKUP")
        self.assertTrue(endpoint["has_shipping"])
        self.assertTrue(endpoint["has_pickup"])

    def test_reference_audit_detects_real_graphql_availability_fields(self):
        sample = r"""
curl ^"https://www.bestbuy.com/gateway/graphql^" ^
  -H ^"x-requested-for-operation-name: PlpView_ProductList_Init^" ^
  --data-raw ^"query X($fulfillmentInput:ProductFulfillmentInput!){ productBySkuId(skuId: ^\\^"1^\\^"){ fulfillmentOptions(input:$fulfillmentInput){ buttonStates shippingDetails deliveryDetails ispuDetails } } }^" &
"""
        summary = analyze_curl_reference_text(sample)

        self.assertEqual(summary["graphql_post_count"], 1)
        self.assertEqual(len(summary["graphql_posts_with_availability_fields"]), 1)

    def test_detail_with_fulfillment_payload_is_single_operation(self):
        payload = detail_with_fulfillment_payload("6623791")

        self.assertEqual(payload["operationName"], "ProductSchema_init")
        self.assertEqual(payload["variables"]["skuId"], "6623791")
        self.assertIn("fulfillmentInput", payload["variables"])
        self.assertIn("ProductFulfillmentInput", payload["query"])
        self.assertIn("fulfillmentOptions(input:$fulfillmentInput)", payload["query"])
        self.assertNotIsInstance(payload, list)


if __name__ == "__main__":
    unittest.main()
