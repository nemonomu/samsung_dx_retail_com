import sys
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from bestbuy.step00_config import BESTBUY_OUTPUT_TABLES  # noqa: E402
from bestbuy.step00_availability_policy import ALL_AVAILABILITY_FIELDS  # noqa: E402
from bestbuy.step08_detail_enrichment import HHP_FINAL_FIELDS, PRODUCT_LIST_DETAIL_FIELD_SOURCES  # noqa: E402
from bestbuy.step13_db_prepare import HHP_COLUMNS, LDY_COLUMNS, REF_COLUMNS  # noqa: E402


def column_names(columns):
    return [name for name, _ in columns]


class BestBuyCategorySchemaTests(unittest.TestCase):
    def test_output_table_names_match_confirmed_targets(self):
        self.assertEqual(BESTBUY_OUTPUT_TABLES["HHP"], "hhp_retail_com")
        self.assertEqual(BESTBUY_OUTPUT_TABLES["LDY"], "ldy_retail_com")
        self.assertEqual(BESTBUY_OUTPUT_TABLES["REF"], "ref_retail_com")

    def test_hhp_final_output_uses_confirmed_insert_columns_only(self):
        expected = [
            "id",
            "country",
            "product",
            "item",
            "account_name",
            "page_type",
            "count_of_reviews",
            "retailer_sku_name",
            "product_url",
            "star_rating",
            "count_of_star_ratings",
            "final_sku_price",
            "original_sku_price",
            "savings",
            "offer",
            "pick_up_availability",
            "fastest_delivery",
            "sku_status",
            "trade_in",
            "hhp_storage",
            "hhp_color",
            "hhp_carrier",
            "detailed_review_content",
            "recommendation_intent",
            "main_rank",
            "bsr_rank",
            "trend_rank",
            "retailer_sku_name_similar",
            "promotion_type",
            "calendar_week",
            "crawl_strdatetime",
            "batch_id",
        ]

        self.assertEqual(HHP_FINAL_FIELDS, expected)

    def test_ref_and_ldy_schemas_exclude_non_target_availability_and_promo_fields(self):
        for names in (column_names(REF_COLUMNS), column_names(LDY_COLUMNS)):
            self.assertIn("pick_up_availability", names)
            self.assertIn("delivery_availability", names)
            self.assertNotIn("fastest_delivery", names)
            self.assertNotIn("trend_rank", names)
            self.assertNotIn("promotion_type", names)
        self.assertIn("ref_capacity", column_names(REF_COLUMNS))
        self.assertIn("ref_refrigerator_type", column_names(REF_COLUMNS))
        self.assertIn("ldy_capacity", column_names(LDY_COLUMNS))
        self.assertIn("ldy_loading_type", column_names(LDY_COLUMNS))

    def test_hhp_test10_runner_keeps_run_limited_and_dry(self):
        script = (ROOT / "bby_hhp_test10_task.bat").read_text(encoding="utf-8")

        self.assertIn('set "BESTBUY_FORCE_STEP_ENV=0"', script)
        self.assertIn('set "BESTBUY_FINAL_TARGET_SIZE=7"', script)
        self.assertIn('set "BESTBUY_BSR_RANK_LIMIT=20"', script)
        self.assertIn('set "BESTBUY_FINAL_ROW_LIMIT=10"', script)
        self.assertIn('set "BESTBUY_DB_LOAD_DRY_RUN=1"', script)
        self.assertIn('call "%~dp0_bby_daily_task.bat" HHP', script)

    def test_detail_promotion_type_can_backfill_hhp_product_list(self):
        self.assertEqual(PRODUCT_LIST_DETAIL_FIELD_SOURCES["promotion_type"], ("promotion_type",))

    def test_tv_detail_output_uses_defined_availability_fields(self):
        step08 = (ROOT / "bestbuy" / "step08_detail_enrichment.py").read_text(encoding="utf-8")

        self.assertIn("ALL_AVAILABILITY_FIELDS", step08)
        self.assertEqual(
            ALL_AVAILABILITY_FIELDS,
            ["pick_up_availability", "fastest_delivery", "delivery_availability"],
        )
        self.assertNotIn("for field in AVAILABILITY_FIELDS", step08)

    def test_hhp_promotion_listing_is_explicitly_skipped(self):
        orchestrator = (ROOT / "bestbuy" / "bestbuy_orchestrator.py").read_text(encoding="utf-8")
        final_targets = (ROOT / "bestbuy" / "step07_final_targets.py").read_text(encoding="utf-8")

        self.assertIn("HHP promotion page is not collected", orchestrator)
        self.assertIn('if CATEGORY == "HHP":', final_targets)
        self.assertIn("promotion_rows = []", final_targets)


if __name__ == "__main__":
    unittest.main()
