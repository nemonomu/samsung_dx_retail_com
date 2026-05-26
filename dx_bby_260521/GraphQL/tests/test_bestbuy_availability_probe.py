import sys
import unittest
from datetime import datetime
from pathlib import Path
from unittest.mock import patch


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from bestbuy.step00_availability_graphql_probe import (  # noqa: E402
    analyze_curl_reference_text,
    detail_with_fulfillment_payload,
    detail_with_get_it_fast_payload,
    digital_event_availability_values,
    fulfillment_dynamic_exact_payload,
    get_it_fast_availability_values,
)
import bestbuy.step08_detail_enrichment as detail_enrichment  # noqa: E402


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

    def test_detail_with_get_it_fast_payload_is_single_operation(self):
        payload = detail_with_get_it_fast_payload("6623791")

        self.assertEqual(payload["operationName"], "ProductSchemaGetItFastProbe")
        self.assertEqual(payload["variables"]["skuId"], "6623791")
        self.assertIn("fulfillmentGetItFastOptions", payload["query"])
        self.assertNotIn("fulfillmentOptions(input:$fulfillmentInput)", payload["query"])
        self.assertNotIsInstance(payload, list)

    def test_fulfillment_dynamic_exact_payload_matches_pdp_render_shape(self):
        payload = fulfillment_dynamic_exact_payload("6623791")

        self.assertEqual(payload["operationName"], "FulfillmentOptionHook_FulfillmentDynamicQuery")
        self.assertEqual(payload["variables"]["skuId"], "6623791")
        self.assertIn("productPriceInput", payload["variables"])
        self.assertIn("openBoxCondition", payload["variables"])
        self.assertIn("...FullfillmentProductBySkuIdFragment", payload["query"])
        self.assertIn("deliverySlots{date}", payload["query"])
        self.assertIn("installationSlots{date}", payload["query"])
        self.assertGreater(len(payload["query"]), 3000)
        self.assertNotIsInstance(payload, list)

    def test_get_it_fast_values_map_pickup_and_shipping_dates(self):
        class FrozenDateTime(datetime):
            @classmethod
            def now(cls, tz=None):
                return cls(2026, 5, 25, 12, 0, 0, tzinfo=tz)

        item = {
            "data": {
                "fulfillmentGetItFastOptions": {
                    "shippingCutOffDetails": {"getItByDate": "2026-05-27"},
                    "storeCutOffDetails": [{"getItBy": "today", "getItByDate": "2026-05-25"}],
                }
            }
        }
        with patch.object(detail_enrichment, "datetime", FrozenDateTime):
            values = get_it_fast_availability_values(item)

        self.assertEqual(values["pick_up_availability"], "Pick up today")
        self.assertEqual(values["fastest_delivery"], "Get it by Wed, May 27 \u2022 FREE")
        self.assertEqual(values["delivery_availability"], "")

    def test_reference_audit_maps_digital_fulfillment_event(self):
        sample = r"""
curl ^"https://streams.bestbuy.com/customer/web-streams/v1/events/digital-experience-event^" ^
  --data-raw ^"^{^\^"device^\^":^{^\^"time^\^":^\^"2026-05-25T11:44:26.578Z^\^",^\^"timeZone^\^":^\^"UTC-04:00^\^"^},^\^"interaction^\^":^{^\^"name^\^":^\^"Fulfillment Impression^\^"^},^\^"skus^\^":^[^{^\^"id^\^":^\^"6623791^\^",^\^"fulfillment^\^":^{^\^"type^\^":^\^"pickup^\^",^\^"daysOut^\^":0,^\^"cost^\^":0,^\^"isSelected^\^":true^}^},^{^\^"id^\^":^\^"6623791^\^",^\^"fulfillment^\^":^{^\^"type^\^":^\^"shipping^\^",^\^"daysOut^\^":2,^\^"cost^\^":0,^\^"isSelected^\^":false^}^},^{^\^"id^\^":^\^"6623791^\^",^\^"fulfillment^\^":^{^\^"type^\^":^\^"delivery^\^",^\^"daysOut^\^":3^}^}^]^}^" &
"""
        summary = analyze_curl_reference_text(sample)
        values = digital_event_availability_values(summary["digital_fulfillment_examples"], "6623791")

        self.assertEqual(summary["digital_event_count"], 1)
        self.assertEqual(summary["digital_event_parsed_count"], 1)
        self.assertEqual(summary["digital_fulfillment_event_count"], 3)
        self.assertEqual(values["pick_up_availability"], "Pick up today")
        self.assertEqual(values["fastest_delivery"], "Get it by Wed, May 27 \u2022 FREE")
        self.assertEqual(values["delivery_availability"], "Delivery as soon as Thu, May 28")


if __name__ == "__main__":
    unittest.main()
