import sys
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from bestbuy.step00_fulfillment_graphql import parse_fulfillment_response  # noqa: E402
from bestbuy.step08_availability_backfill import (  # noqa: E402
    all_availability_blank,
    apply_values,
    build_sku_lookup,
    filter_row_to_sku_for_selected_skus,
    item_from_product_url,
    sku_for_row,
)


class AvailabilityBackfillTests(unittest.TestCase):
    def test_candidate_default_missing_any_requires_batch_and_any_blank_field(self):
        import bestbuy.step08_availability_backfill as backfill

        old_mode = backfill.CANDIDATE_MODE
        row = {
            "batch_id": "b_20260525_040458",
            "pick_up_availability": "",
            "fastest_delivery": "",
            "delivery_availability": "",
        }

        backfill.CANDIDATE_MODE = "missing_any"
        try:
            self.assertTrue(all_availability_blank(row))
            self.assertTrue(backfill.backfill_candidate(row, "b_20260525_040458"))

            row["fastest_delivery"] = "Get it Wed, May 27 \u2022 FREE"
            self.assertFalse(all_availability_blank(row))
            self.assertTrue(backfill.backfill_candidate(row, "b_20260525_040458"))

            row["pick_up_availability"] = "Pick up Thu, May 28"
            row["delivery_availability"] = "Delivery as soon as Fri, May 29"
            self.assertFalse(backfill.backfill_candidate(row, "b_20260525_040458"))
            self.assertFalse(backfill.backfill_candidate(row, "b_other"))
        finally:
            backfill.CANDIDATE_MODE = old_mode

    def test_candidate_all_rows_reprocesses_existing_availability(self):
        import bestbuy.step08_availability_backfill as backfill

        old_mode = backfill.CANDIDATE_MODE
        row = {
            "batch_id": "b_20260525_040458",
            "pick_up_availability": "Pick up today",
            "fastest_delivery": "Get it today",
            "delivery_availability": "Delivery as soon as Sat, May 30",
        }

        backfill.CANDIDATE_MODE = "all_rows"
        try:
            self.assertTrue(backfill.backfill_candidate(row, "b_20260525_040458"))
            self.assertFalse(backfill.backfill_candidate(row, "b_other"))
        finally:
            backfill.CANDIDATE_MODE = old_mode

    def test_sku_lookup_maps_final_output_item_to_target_sku(self):
        lookup = build_sku_lookup(
            [
                {
                    "sku_id": "6623791",
                    "bsin": "J2FPJK9P43",
                    "product_url": "https://www.bestbuy.com/site/example/6623791.p?skuId=6623791",
                }
            ]
        )

        self.assertEqual(sku_for_row({"item": "J2FPJK9P43"}, lookup), "6623791")
        self.assertEqual(sku_for_row({"sku_id": "6623791"}, lookup), "6623791")

    def test_item_can_be_derived_from_bestbuy_product_url(self):
        self.assertEqual(
            item_from_product_url("https://www.bestbuy.com/product/name/J3ZYG2V5VV/sku/6639210"),
            "J3ZYG2V5VV",
        )

    def test_limited_backfill_only_maps_selected_skus(self):
        self.assertEqual(
            filter_row_to_sku_for_selected_skus({0: "6639210", 1: "6670264", 2: "6607832"}, ["6639210"]),
            {0: "6639210"},
        )

    def test_parse_fulfillment_does_not_invent_generic_pickup_available(self):
        response = {
            "data": {
                "fulfillmentOptions": {
                    "ispuDetails": [
                        {
                            "sku": "6632331",
                            "ispuAvailability": [
                                {
                                    "condition": "NEW",
                                    "pickupEligible": True,
                                    "instoreInventoryAvailable": False,
                                    "maxDate": None,
                                    "fulfillDate": None,
                                    "promiseByStreetDate": None,
                                    "minPickupInHours": None,
                                }
                            ],
                        }
                    ]
                }
            }
        }

        values = parse_fulfillment_response(response)

        self.assertEqual(values, {})

    def test_parse_batch_fulfillment_response_groups_values_by_sku(self):
        response = {
            "data": {
                "fulfillmentOptions": {
                    "deliveryDetails": [
                        {
                            "sku": "6635801",
                            "deliveryAvailability": [
                                {
                                    "condition": "NEW",
                                    "deliveryEligible": True,
                                    "deliverySlots": [
                                        {
                                            "date": "2026-05-29",
                                            "displayText": "Delivery as soon as Fri, May 29",
                                        }
                                    ],
                                }
                            ],
                        }
                    ],
                    "ispuDetails": [
                        {
                            "sku": "6635801",
                            "ispuAvailability": [
                                {
                                    "condition": "NEW",
                                    "pickupEligible": True,
                                    "instoreInventoryAvailable": True,
                                    "maxDate": "2026-05-25",
                                    "displayText": "Pick up today",
                                }
                            ],
                        }
                    ],
                    "shippingDetails": [
                        {
                            "sku": "6623791",
                            "shippingAvailability": [
                                {
                                    "condition": "NEW",
                                    "shippingEligible": True,
                                    "defaultCustomerLosGroupId": "standard",
                                    "customerLOSGroup": [
                                        {
                                            "customerLosGroupId": "standard",
                                            "displayText": "Get it Wed, May 27",
                                            "price": 0,
                                        }
                                    ],
                                }
                            ],
                        }
                    ],
                }
            }
        }

        values = parse_fulfillment_response(response)

        self.assertEqual(values["6635801"]["pick_up_availability"], "Pick up today")
        self.assertEqual(values["6635801"]["delivery_availability"], "Delivery as soon as Fri, May 29")
        self.assertEqual(values["6623791"]["fastest_delivery"], "Get it by Wed, May 27 \u2022 FREE")

    def test_apply_values_only_fills_blank_availability_fields(self):
        rows = [
            {
                "batch_id": "b_20260525_040458",
                "pick_up_availability": "",
                "fastest_delivery": "",
                "delivery_availability": "",
            }
        ]
        values = {
            "6623791": {
                "pick_up_availability": "Pick up today",
                "fastest_delivery": "Get it Wed, May 27 \u2022 FREE",
                "delivery_availability": "",
            }
        }

        updated, fields = apply_values(rows, {0: "6623791"}, values)

        self.assertEqual(updated, 1)
        self.assertEqual(fields, 2)
        self.assertEqual(rows[0]["pick_up_availability"], "Pick up today")
        self.assertEqual(rows[0]["fastest_delivery"], "Get it Wed, May 27 \u2022 FREE")
        self.assertEqual(rows[0]["delivery_availability"], "")

    def test_apply_values_can_overwrite_existing_availability_when_enabled(self):
        old_overwrite = __import__("bestbuy.step08_availability_backfill", fromlist=["OVERWRITE"]).OVERWRITE
        import bestbuy.step08_availability_backfill as backfill

        rows = [
            {
                "pick_up_availability": "Pick up today",
                "fastest_delivery": "Get it today",
                "delivery_availability": "",
            }
        ]
        values = {
            "6670831": {
                "pick_up_availability": "Pick up today",
                "fastest_delivery": "Get it tomorrow \u2022 FREE",
                "delivery_availability": "",
            }
        }
        backfill.OVERWRITE = True
        try:
            updated, fields = backfill.apply_values(rows, {0: "6670831"}, values)
        finally:
            backfill.OVERWRITE = old_overwrite

        self.assertEqual(updated, 1)
        self.assertEqual(fields, 2)
        self.assertEqual(rows[0]["fastest_delivery"], "Get it tomorrow \u2022 FREE")

    def test_apply_values_can_clear_existing_availability_before_authoritative_backfill(self):
        import bestbuy.step08_availability_backfill as backfill

        old_clear = backfill.CLEAR_EXISTING_FIELDS
        rows = [
            {
                "pick_up_availability": "Pick up today",
                "fastest_delivery": "Get it today",
                "delivery_availability": "Delivery as soon as Sat, May 30",
            }
        ]
        values = {
            "6670831": {
                "pick_up_availability": "Pick up Thu, May 28",
                "fastest_delivery": "Get it Fri, May 29 \u2022 FREE",
                "delivery_availability": "",
            }
        }
        backfill.CLEAR_EXISTING_FIELDS = True
        try:
            updated, fields = backfill.apply_values(rows, {0: "6670831"}, values)
        finally:
            backfill.CLEAR_EXISTING_FIELDS = old_clear

        self.assertEqual(updated, 1)
        self.assertEqual(fields, 3)
        self.assertEqual(rows[0]["pick_up_availability"], "Pick up Thu, May 28")
        self.assertEqual(rows[0]["fastest_delivery"], "Get it Fri, May 29 \u2022 FREE")
        self.assertEqual(rows[0]["delivery_availability"], "")


if __name__ == "__main__":
    unittest.main()
