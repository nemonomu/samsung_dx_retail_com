import json
import sys
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from bestbuy.step00_parse_search import listing_availability_values, parse_product  # noqa: E402
from bestbuy.step02_main_targets import normalize_existing_listing_row  # noqa: E402


def product_with_listing_fulfillment():
    return {
        "skuId": "6623791",
        "price": {"giftSkus": ["a", "b"]},
        "fulfillmentOptions": {
            "shippingDetails": {
                "shippingAvailability": {
                    "shippingEligible": True,
                    "customerLOSGroup": [
                        {
                            "customerLosGroupId": "standard",
                            "displayText": "Get it Wed, May 27",
                            "price": 0,
                        }
                    ],
                    "defaultCustomerLosGroupId": "standard",
                }
            },
            "deliveryDetails": {
                "deliveryAvailability": {
                    "deliveryEligible": True,
                    "displayText": "Delivery as soon as Thu, May 28",
                }
            },
            "ispuDetails": {
                "ispuAvailability": {
                    "pickupEligible": True,
                    "displayText": "Pick up today",
                    "quantity": 3,
                }
            },
        },
    }


class ListingAvailabilityTests(unittest.TestCase):
    def test_listing_availability_values_from_product_fulfillment(self):
        values = listing_availability_values(product_with_listing_fulfillment())

        self.assertEqual(values["pick_up_availability"], "Pick up today")
        self.assertEqual(values["fastest_delivery"], "Get it by Wed, May 27 \u2022 FREE")
        self.assertEqual(values["delivery_availability"], "Delivery as soon as Thu, May 28")
        self.assertTrue(values["shipping_eligible"])
        self.assertTrue(values["pickup_eligible"])
        self.assertEqual(values["pickup_quantity"], 3)

    def test_parse_product_writes_listing_availability_columns(self):
        row = parse_product(product_with_listing_fulfillment(), {"page": 1})

        self.assertEqual(row["pick_up_availability"], "Pick up today")
        self.assertEqual(row["fastest_delivery"], "Get it by Wed, May 27 \u2022 FREE")
        self.assertEqual(row["delivery_availability"], "Delivery as soon as Thu, May 28")
        self.assertEqual(row["offer_count"], 2)

    def test_main_target_normalization_backfills_availability_from_raw_product_json(self):
        row = normalize_existing_listing_row(
            {
                "sku_id": "6623791",
                "raw_product_json": json.dumps(product_with_listing_fulfillment()),
            }
        )

        self.assertEqual(row["pick_up_availability"], "Pick up today")
        self.assertEqual(row["fastest_delivery"], "Get it by Wed, May 27 \u2022 FREE")
        self.assertEqual(row["delivery_availability"], "Delivery as soon as Thu, May 28")
        self.assertEqual(row["offer_count"], 2)

    def test_main_target_normalization_falls_back_to_sku_pdp_url(self):
        row = normalize_existing_listing_row({"sku_id": "6623791"})

        self.assertIn("skuId=6623791", row["product_url"])


if __name__ == "__main__":
    unittest.main()
