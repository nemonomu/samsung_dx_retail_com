import sys
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from bestbuy.step00_config import BESTBUY_OUTPUT_TABLES, BESTBUY_PRODUCT_LIST_TABLES  # noqa: E402
from bestbuy.step00_availability_policy import ALL_AVAILABILITY_FIELDS  # noqa: E402
from bestbuy.bestbuy_orchestrator import CATEGORY_SEARCH_TERMS, HHP_TRENDING_PAGE_PAYLOAD_ENV  # noqa: E402
import bestbuy.step07_final_targets as final_targets_step  # noqa: E402
import bestbuy.step06_trending_deals as trending_step  # noqa: E402
import bestbuy.step14_db_load as db_load_step  # noqa: E402
import bestbuy.step08_detail_enrichment as detail_step  # noqa: E402
import bestbuy.step15_item_mst_load as item_mst_step  # noqa: E402
import bestbuy.step16_email_notify as email_notify_step  # noqa: E402
from bestbuy.step08_detail_enrichment import (  # noqa: E402
    COMPARE_RECOMMENDATIONS_V2_QUERY,
    HHP_FINAL_FIELDS,
    LDY_FINAL_FIELDS,
    PRODUCT_LIST_DETAIL_FIELD_SOURCES,
    PRODUCT_SCHEMA_REVIEW20_QUERY,
    REF_FINAL_FIELDS,
    TRADE_IN_DATA_QUERY,
    compare_recommendation_names,
    hhp_attributes_from_product,
    ldy_attributes_from_product,
    ref_attributes_from_product,
    trade_in_from_offer_data,
    trade_in_from_html,
    trade_in_from_products,
)
from bestbuy.step13_db_prepare import HHP_COLUMNS, LDY_COLUMNS, REF_COLUMNS, TV_PRODUCT_LIST_COLUMNS  # noqa: E402


def column_names(columns):
    return [name for name, _ in columns]


class BestBuyCategorySchemaTests(unittest.TestCase):
    def test_output_table_names_match_confirmed_targets(self):
        self.assertEqual(BESTBUY_OUTPUT_TABLES["TV"], "tv_retail_com")
        self.assertEqual(BESTBUY_OUTPUT_TABLES["HHP"], "hhp_retail_com")
        self.assertEqual(BESTBUY_OUTPUT_TABLES["LDY"], "ldy_retail_com")
        self.assertEqual(BESTBUY_OUTPUT_TABLES["REF"], "ref_retail_com")
        self.assertEqual(BESTBUY_PRODUCT_LIST_TABLES["TV"], "bby_tv_product_list")
        self.assertEqual(BESTBUY_PRODUCT_LIST_TABLES["HHP"], "bby_hhp_product_list")

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

    def test_ldy_final_output_uses_confirmed_insert_columns_only(self):
        expected = [
            "id",
            "country",
            "product",
            "item",
            "sku",
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
            "delivery_availability",
            "sku_status",
            "detailed_review_content",
            "recommendation_intent",
            "main_rank",
            "bsr_rank",
            "retailer_sku_name_similar",
            "ldy_capacity",
            "ldy_loading_type",
            "calendar_week",
            "crawl_strdatetime",
            "batch_id",
        ]

        self.assertEqual(LDY_FINAL_FIELDS, expected)

    def test_ref_final_output_uses_confirmed_insert_columns_only(self):
        expected = [
            "id",
            "country",
            "product",
            "item",
            "sku",
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
            "delivery_availability",
            "sku_status",
            "detailed_review_content",
            "recommendation_intent",
            "main_rank",
            "bsr_rank",
            "retailer_sku_name_similar",
            "ref_capacity",
            "ref_refrigerator_type",
            "calendar_week",
            "crawl_strdatetime",
            "batch_id",
        ]

        self.assertEqual(REF_FINAL_FIELDS, expected)

    def test_db_load_fails_when_csv_columns_are_missing_from_table(self):
        rows = [{"id": "", "batch_id": "b_test", "hhp_storage": "128 gigabytes"}]

        with self.assertRaisesRegex(RuntimeError, "hhp_storage"):
            db_load_step.plan_rows([("id", "serial"), ("batch_id", "text")], rows, "hhp_retail_com")

        result = db_load_step.plan_rows(
            [("id", "serial"), ("batch_id", "text"), ("hhp_storage", "text")],
            rows,
            "hhp_retail_com",
        )

        self.assertEqual(result["missing_table_columns"], [])
        self.assertEqual(result["columns"], ["batch_id", "hhp_storage"])

    def test_db_row_upsert_filters_by_sku_and_requires_filter_by_default(self):
        old_skus = db_load_step.ROW_UPSERT_SKUS
        old_allow_all = db_load_step.ROW_UPSERT_ALLOW_ALL
        old_nonblank = db_load_step.ROW_UPSERT_NONBLANK_ONLY
        try:
            rows = [
                {
                    "batch_id": "b_test",
                    "sku_id": "111",
                    "retailer_sku_name": "keep",
                    "product_url": "https://www.bestbuy.com/product/a/A/sku/111",
                },
                {
                    "batch_id": "b_test",
                    "sku_id": "222",
                    "retailer_sku_name": "retry",
                    "product_url": "https://www.bestbuy.com/product/b/B/sku/222",
                },
            ]
            columns = [
                ("id", "serial"),
                ("batch_id", "text"),
                ("sku_id", "text"),
                ("retailer_sku_name", "text"),
                ("product_url", "text"),
            ]
            db_load_step.ROW_UPSERT_SKUS = set()
            db_load_step.ROW_UPSERT_ALLOW_ALL = False
            result = db_load_step.row_upsert_rows(None, "bby_ref_product_list", columns, rows, dry_run=True)
            self.assertEqual(result["candidate_rows"], 0)

            db_load_step.ROW_UPSERT_SKUS = {"222"}
            db_load_step.ROW_UPSERT_NONBLANK_ONLY = True
            result = db_load_step.row_upsert_rows(None, "bby_ref_product_list", columns, rows, dry_run=True)
            self.assertEqual(result["candidate_rows"], 1)
            self.assertEqual(result["updated"], 1)
        finally:
            db_load_step.ROW_UPSERT_SKUS = old_skus
            db_load_step.ROW_UPSERT_ALLOW_ALL = old_allow_all
            db_load_step.ROW_UPSERT_NONBLANK_ONLY = old_nonblank

    def test_db_load_extracts_sku_id_from_bestbuy_urls(self):
        self.assertEqual(
            db_load_step.sku_id_from_product_url("https://www.bestbuy.com/product/x/ABC/sku/6467055"),
            "6467055",
        )
        self.assertEqual(
            db_load_step.sku_id_from_product_url("https://www.bestbuy.com/site/-/6671361.p?skuId=6671361&intl=nosplash"),
            "6671361",
        )

    def test_db_row_upsert_blank_existing_row_does_not_insert_duplicate(self):
        class FakeCursor:
            def __init__(self):
                self.rowcount = 0
                self.insert_calls = 0

            def execute(self, sql, params=()):
                if sql.strip().upper().startswith("SELECT"):
                    self._fetchone = (1,)
                    self.rowcount = 1
                    return
                if sql.strip().upper().startswith("INSERT"):
                    self.insert_calls += 1
                    self.rowcount = 1

            def fetchone(self):
                return self._fetchone

        old_skus = db_load_step.ROW_UPSERT_SKUS
        old_allow_all = db_load_step.ROW_UPSERT_ALLOW_ALL
        old_nonblank = db_load_step.ROW_UPSERT_NONBLANK_ONLY
        try:
            db_load_step.ROW_UPSERT_SKUS = {"111"}
            db_load_step.ROW_UPSERT_ALLOW_ALL = False
            db_load_step.ROW_UPSERT_NONBLANK_ONLY = True
            cur = FakeCursor()
            result = db_load_step.row_upsert_rows(
                cur,
                "bby_ref_product_list",
                [("id", "serial"), ("batch_id", "text"), ("sku_id", "text"), ("retailer_sku_name", "text")],
                [{"batch_id": "b_test", "sku_id": "111", "retailer_sku_name": ""}],
            )
            self.assertEqual(result["inserted"], 0)
            self.assertEqual(result["skipped_blank_update"], 1)
            self.assertEqual(cur.insert_calls, 0)
        finally:
            db_load_step.ROW_UPSERT_SKUS = old_skus
            db_load_step.ROW_UPSERT_ALLOW_ALL = old_allow_all
            db_load_step.ROW_UPSERT_NONBLANK_ONLY = old_nonblank

    def test_db_prepare_adds_missing_columns_for_existing_tables(self):
        step13 = (ROOT / "bestbuy" / "step13_db_prepare.py").read_text(encoding="utf-8")

        self.assertIn("BESTBUY_DB_PREPARE_ADD_MISSING_COLUMNS", step13)
        self.assertIn("ALTER TABLE", step13)
        self.assertIn("ADD COLUMN", step13)
        self.assertIn("bestbuy_product_list_table(category)", step13)
        self.assertNotIn("BESTBUY_PRODUCT_LIST_TABLES", step13)

    def test_tv_product_list_schema_accepts_generated_price_columns(self):
        names = column_names(TV_PRODUCT_LIST_COLUMNS)

        self.assertIn("final_sku_price", names)
        self.assertIn("savings", names)
        self.assertIn("comparable_pricing", names)

    def test_hhp_test10_runner_keeps_run_limited_and_dry(self):
        script = (ROOT / "bby_hhp_test10_task.bat").read_text(encoding="utf-8")

        self.assertIn('set "BESTBUY_FORCE_STEP_ENV=0"', script)
        self.assertIn('set "BESTBUY_FINAL_TARGET_SIZE=7"', script)
        self.assertIn('set "BESTBUY_BSR_RANK_LIMIT=20"', script)
        self.assertIn('set "BESTBUY_FINAL_ROW_LIMIT=10"', script)
        self.assertIn('set "BESTBUY_DB_LOAD_DRY_RUN=1"', script)
        self.assertIn('set "BESTBUY_PRESERVE_RUN_ENV=1"', script)
        self.assertIn('call "%~dp0_bby_daily_task.bat" HHP', script)

    def test_daily_task_cleans_residual_test_env_by_default(self):
        script = (ROOT / "_bby_daily_task.bat").read_text(encoding="utf-8")

        self.assertIn('if not "%BESTBUY_PRESERVE_RUN_ENV%"=="1"', script)
        self.assertIn('set "BESTBUY_FORCE_STEP_ENV=1"', script)
        self.assertIn('set "BESTBUY_FINAL_TARGET_SIZE="', script)
        self.assertIn('set "BESTBUY_FINAL_ROW_LIMIT="', script)
        self.assertIn('set "BESTBUY_DETAIL_SKUS="', script)
        self.assertIn('set "BESTBUY_DB_PREPARE_ADD_MISSING_COLUMNS=1"', script)
        self.assertIn('set "BESTBUY_DB_LOAD_DRY_RUN=0"', script)
        self.assertIn('set "BESTBUY_DB_UPDATE_SIMILAR_ONLY=0"', script)
        self.assertIn('set "BESTBUY_FORCE_RUN_PATH_ENV="', script)
        self.assertIn('set "BESTBUY_RUN_ROOT="', script)
        self.assertIn('set "BESTBUY_ITEM_MST_OUTPUT_CSV="', script)

    def test_daily_task_sends_preflight_failure_email_for_lock_errors(self):
        script = (ROOT / "_bby_daily_task.bat").read_text(encoding="utf-8")

        self.assertIn('call :notify_preflight "daily_lock failed" "%LOCK_STATUS%"', script)
        self.assertIn('set "BESTBUY_NOTIFY_PREFLIGHT_ISSUE=%~1 exit_code=%~2"', script)
        self.assertIn("python -m bestbuy.step16_email_notify", script)

    def test_tv_hhp_daily_task_runs_only_tv_then_hhp_with_clean_env(self):
        script = (ROOT / "bby_tv_hhp_daily_task.bat").read_text(encoding="utf-8")

        self.assertIn('set "CHAIN_ORDER=TV HHP"', script)
        self.assertIn("call :run_category TV", script)
        self.assertIn("call :run_category HHP", script)
        self.assertNotIn("call :run_category LDY", script)
        self.assertNotIn("call :run_category REF", script)
        self.assertIn("call :clear_operational_env", script)
        self.assertIn('set "BESTBUY_PRESERVE_RUN_ENV="', script)
        self.assertIn('set "BESTBUY_RUN_ROOT="', script)
        self.assertIn('call "%~dp0_bby_daily_task.bat" %CATEGORY%', script)

    def test_ref_ldy_tv_hhp_daily_task_continues_after_category_failure(self):
        script = (ROOT / "bby_ref_ldy_tv_hhp_daily_task.bat").read_text(encoding="utf-8")

        self.assertIn('set "CHAIN_ORDER=REF LDY TV HHP"', script)
        self.assertIn("call :run_category REF", script)
        self.assertIn("call :record_category REF", script)
        self.assertIn("call :run_category HHP", script)
        self.assertIn("call :record_category HHP", script)
        self.assertIn("completed with failures", script)
        self.assertNotIn("if errorlevel 1 goto :fail", script)

    def test_sos_refill_preserves_operational_call_units(self):
        script = (ROOT / "bestbuy" / "sos_refill.py").read_text(encoding="utf-8")
        bat = (ROOT / "bby_sos_refill.bat").read_text(encoding="utf-8")

        self.assertIn('"BESTBUY_LISTING_MAX_ATTEMPTS": "3"', script)
        self.assertIn('"BESTBUY_DETAIL_SKU_BATCH_SIZE": "5"', script)
        self.assertIn('"BESTBUY_AVAILABILITY_BACKFILL_CHUNK_SIZE": "1"', script)
        self.assertIn('candidate_mode = "all_rows" if refresh_all_availability else "blank_all"', script)
        self.assertIn('"BESTBUY_AVAILABILITY_BACKFILL_CLEAR_EXISTING_FIELDS": "0"', script)
        self.assertIn('"BESTBUY_DB_UPDATE_SIMILAR_ONLY": "0"', script)
        self.assertIn("SOS safety check blocked DB load", script)
        self.assertIn("detail_refill_skus", script)
        self.assertIn("[sos:scope] detail_skus=", script)
        self.assertIn("merge_existing_nonblank_values", script)
        self.assertIn("TIMESTAMP_FIELDS", script)
        self.assertIn("duplicate_numeric_values(final_rows, field)", script)
        self.assertIn('"main_rank": 300', script)
        self.assertIn('"bsr_rank": 100', script)
        self.assertIn("missing_numeric_rank_values(final_rows, field, expected_max)", script)
        self.assertIn("apply_cached_availability_values(run_root, log_handle)", script)
        self.assertIn("parse_fulfillment_response", script)
        self.assertIn("python -m bestbuy.sos_refill", bat)

    def test_tv_and_hhp_daily_wrappers_force_production_env(self):
        tv_script = (ROOT / "bby_tv_daily_task.bat").read_text(encoding="utf-8")
        hhp_script = (ROOT / "bby_hhp_daily_task.bat").read_text(encoding="utf-8")

        self.assertIn('set "BESTBUY_PRESERVE_RUN_ENV="', tv_script)
        self.assertIn('set "BESTBUY_PRESERVE_RUN_ENV="', hhp_script)
        self.assertIn('call "%~dp0_bby_daily_task.bat" TV', tv_script)
        self.assertIn('call "%~dp0_bby_daily_task.bat" HHP', hhp_script)

    def test_daily_lock_falls_back_to_wmi_when_cim_fails(self):
        script = (ROOT / "bestbuy" / "step00_daily_lock.py").read_text(encoding="utf-8")

        self.assertIn("Get-CimInstance Win32_Process", script)
        self.assertIn("Get-WmiObject Win32_Process", script)

    def test_daily_lock_uses_parent_pid_to_clear_stale_locks_when_process_inspection_fails(self):
        script = (ROOT / "bestbuy" / "step00_daily_lock.py").read_text(encoding="utf-8")

        self.assertIn('"parent_pid": os.getppid()', script)
        self.assertIn("def lock_owner_active", script)
        self.assertIn("lock owner process ended; treating lock as stale", script)
        self.assertIn("lock owner process is still active; keeping", script)

    def test_fullrun_resets_db_update_only_modes(self):
        script = (ROOT / "run_bestbuy_fullrun.bat").read_text(encoding="utf-8")

        self.assertIn('set "BESTBUY_DB_UPDATE_SIMILAR_ONLY=0"', script)
        self.assertIn('set "BESTBUY_DB_UPDATE_AVAILABILITY_ONLY=0"', script)
        self.assertIn('if not defined BESTBUY_DB_LOAD_DRY_RUN set "BESTBUY_DB_LOAD_DRY_RUN=0"', script)

    def test_optional_s3_and_cleanup_steps_are_skippable(self):
        orchestrator = (ROOT / "bestbuy" / "bestbuy_orchestrator.py").read_text(encoding="utf-8")

        self.assertIn('step.name == "s3_sync"', orchestrator)
        self.assertIn("BESTBUY_S3_SYNC_SKIP", orchestrator)
        self.assertIn("S3_BUCKET is missing", orchestrator)
        self.assertIn("BESTBUY_LOCAL_CLEANUP_SKIP", orchestrator)

    def test_fullrun_runs_item_master_after_db_load(self):
        script = (ROOT / "run_bestbuy_fullrun.bat").read_text(encoding="utf-8")
        orchestrator = (ROOT / "bestbuy" / "bestbuy_orchestrator.py").read_text(encoding="utf-8")

        self.assertIn('set "BESTBUY_ITEM_MST_OUTPUT_CSV=%BESTBUY_OUTPUT_ROOT%\\item_mst.csv"', script)
        self.assertIn('call :run_step 14 14 "item_mst_load" 16', script)
        self.assertIn('Step(16, "item_mst_load", "bestbuy.step15_item_mst_load")', orchestrator)

    def test_fullrun_notifies_after_success_or_failure(self):
        script = (ROOT / "run_bestbuy_fullrun.bat").read_text(encoding="utf-8")

        self.assertIn('call :notify "success" "" ""', script)
        self.assertIn('call :notify "failed" "%FAILED_STEP_NAME%" "%FAILED_STEP%"', script)
        self.assertIn('set "BESTBUY_NOTIFY_FAILED_STEP_NAME=%~2"', script)
        self.assertIn("python -m bestbuy.step16_email_notify", script)

    def test_email_notification_subject_body_and_warnings(self):
        notifier = (ROOT / "bestbuy" / "step16_email_notify.py").read_text(encoding="utf-8")

        self.assertNotIn("samsung_ds_retail_com", notifier)
        self.assertNotIn("EMAIL_CONFIG", notifier)
        self.assertEqual(
            email_notify_step.build_subject("TV", []),
            "[SEA] BBY TV crawled",
        )
        self.assertEqual(
            email_notify_step.build_subject("HHP", ["main_rank 299/300"]),
            "[SEA] [Warning] BBY HHP crawled",
        )
        sample_call_counts = {
            "total": 343,
            "listing": 19,
            "detail": 3,
            "availability": 321,
        }
        body = email_notify_step.build_body(303, 3800, sample_call_counts, [])
        self.assertIn("특이사항 없음", body)
        self.assertIn("총 호출 수 343회", body)
        self.assertIn("listing - 19회", body)
        self.assertIn("detail/review/compare - 3회", body)
        self.assertIn("3종 availability - 321회", body)
        self.assertIn("평균 호출 비용 11원", body)

    def test_email_preflight_notification_uses_zero_counts_and_warning(self):
        notification = email_notify_step.build_preflight_notification("REF", "daily_lock failed exit_code=2")

        self.assertEqual(notification["subject"], "[SEA] [Warning] BBY REF crawled")
        self.assertEqual(notification["metrics"]["collected_count"], 0)
        self.assertEqual(notification["metrics"]["call_counts"]["total"], 0)
        self.assertIn("daily_lock failed exit_code=2", notification["issues"][0])
        self.assertIn("listing - 0", notification["body"])
        self.assertIn("detail/review/compare - 0", notification["body"])
        self.assertIn("availability - 0", notification["body"])

    def test_email_notification_detects_null_columns_and_short_counts(self):
        rows = [
            {
                "retailer_sku_name": "TV",
                "item": "SKU1",
                "final_sku_price": "$1.00",
                "screen_size": "",
                "product_url": "https://example.test/1",
            },
            {
                "retailer_sku_name": "TV2",
                "item": "SKU2",
                "final_sku_price": "",
                "screen_size": "",
                "product_url": "https://example.test/2",
            },
        ]
        columns = ["retailer_sku_name", "item", "final_sku_price", "screen_size"]

        self.assertIn(
            "screen_size",
            email_notify_step.all_null_column_issues("TV", rows, columns)[0],
        )
        self.assertEqual(
            email_notify_step.all_null_column_issues(
                "TV",
                [{"sku_popularity": "", "item": "x"}, {"sku_popularity": "", "item": "y"}],
                ["sku_popularity", "item"],
            ),
            [],
        )
        self.assertTrue(
            any("final_sku_price 1 rows null" in issue for issue in email_notify_step.critical_null_issues(rows, columns))
        )
        self.assertEqual(
            email_notify_step.db_count_issue({"final_output": {"inserted": 250, "csv_rows": 310}}, 310),
            "DB insert rows 미달: 250/310 success",
        )
        listing_issues = email_notify_step.listing_count_issues(
            "TV",
            ROOT,
            [{"main_rank": "299", "bsr_rank": "99"}],
            {"trending_unique_count": 9, "promotion_unique_count": 17},
        )
        self.assertIn("main_rank 299/300", listing_issues)
        self.assertIn("bsr_rank 99/100", listing_issues)
        self.assertIn("trend listing sku 9/10", listing_issues)
        self.assertIn("promotion listing sku 17/18", listing_issues)

    def test_email_notification_recovers_detail_batch_calls_from_raw_meta(self):
        run_root = Path("unit_run_root")
        fake_paths = [Path(f"sku{index}_meta.json") for index in range(1, 6)]
        fake_metas = {
            path: {
                "success": True,
                "transport": "zenrows",
                "stage": "detail",
                "fetched_this_run": True,
                "detail_mode": "direct_graphql_sku_batch",
                "sku_batch_index": index,
                "x_request_cost_total": 0.00055992,
                "batch_x_request_cost": 0.0027996,
            }
            for index, path in enumerate(fake_paths, 1)
        }
        original_read_json = email_notify_step.read_json
        original_detail_meta_paths = email_notify_step.detail_meta_paths

        def fake_read_json(path):
            text = str(path).replace("\\", "/")
            if text.endswith("main/manifest.json"):
                return {"actual_post_calls": 1, "total_x_request_cost": 0.0027996}
            return fake_metas.get(Path(path), {})

        def fake_detail_meta_paths(_run_root, folder):
            return fake_paths if folder == "detail_html" else []

        email_notify_step.read_json = fake_read_json
        email_notify_step.detail_meta_paths = fake_detail_meta_paths
        try:
            call_counts = email_notify_step.manifest_call_counts(run_root)
            detail_cost, sources = email_notify_step.manifest_costs(run_root)
        finally:
            email_notify_step.read_json = original_read_json
            email_notify_step.detail_meta_paths = original_detail_meta_paths

        self.assertEqual(call_counts["detail"], 1)
        self.assertEqual(call_counts["detail_breakdown"][0]["source"], "raw_batch_meta")
        self.assertAlmostEqual(detail_cost, 0.0055992)
        self.assertTrue(any(source["key"] == "raw_meta_cost" for source in sources))

    def test_ref_test10_runner_keeps_run_limited_and_dry(self):
        script = (ROOT / "bby_ref_test10_task.bat").read_text(encoding="utf-8")

        self.assertIn('set "BESTBUY_FORCE_STEP_ENV=0"', script)
        self.assertIn('set "BESTBUY_FINAL_TARGET_SIZE=7"', script)
        self.assertIn('set "BESTBUY_BSR_RANK_LIMIT=20"', script)
        self.assertIn('set "BESTBUY_FINAL_ROW_LIMIT=10"', script)
        self.assertIn('set "BESTBUY_DB_LOAD_DRY_RUN=1"', script)
        self.assertIn('set "BESTBUY_PRESERVE_RUN_ENV=1"', script)
        self.assertIn('call "%~dp0_bby_daily_task.bat" REF', script)

    def test_ldy_test10_runner_preserves_test_env(self):
        script = (ROOT / "bby_ldy_test10_task.bat").read_text(encoding="utf-8")

        self.assertIn('set "BESTBUY_FORCE_STEP_ENV=0"', script)
        self.assertIn('set "BESTBUY_DB_LOAD_DRY_RUN=1"', script)
        self.assertIn('set "BESTBUY_PRESERVE_RUN_ENV=1"', script)
        self.assertIn('call "%~dp0_bby_daily_task.bat" LDY', script)

    def test_detail_promotion_type_can_backfill_hhp_product_list(self):
        self.assertEqual(PRODUCT_LIST_DETAIL_FIELD_SOURCES["promotion_type"], ("promotion_type",))

    def test_no_longer_available_detail_price_becomes_status_text(self):
        self.assertEqual(
            detail_step.no_longer_available_price_fields("", "$499.99", "$70", unavailable=True),
            ("no longer available", "", ""),
        )
        self.assertEqual(
            detail_step.no_longer_available_price_fields(
                "This item is no longer available in new condition.",
                "$499.99",
                "$70",
                unavailable=True,
            ),
            ("no longer available", "", ""),
        )
        self.assertEqual(
            detail_step.no_longer_available_price_fields("$429.99", "$499.99", "$70", unavailable=True),
            ("no longer available", "", ""),
        )
        self.assertTrue(detail_step.is_detail_no_longer_available_product({"dotComDisplayStatus": "inactive"}))
        self.assertFalse(detail_step.is_detail_no_longer_available_product({"dotComDisplayStatus": "active"}))

    def test_no_longer_available_detector_ignores_generic_review_text(self):
        self.assertTrue(
            detail_step.is_detail_no_longer_available_text(
                "This item is no longer available in new condition. See similar items below"
            )
        )
        self.assertFalse(
            detail_step.is_detail_no_longer_available_text(
                "I considered a 60-inch TV but discovered that size was no longer available."
            )
        )
        self.assertNotIn("no longer available", detail_step.NO_LONGER_AVAILABLE_PHRASES)

    def test_product_schema_query_includes_dotcom_display_status(self):
        self.assertIn("dotComDisplayStatus", detail_step.PRODUCT_SCHEMA_REVIEW20_QUERY)
        query = "query X($skuId:String!){productBySkuId(skuId:$skuId){skuId name{short}}}"
        self.assertIn("dotComDisplayStatus", detail_step.ensure_dotcom_display_status_query(query))

    def test_item_mst_model_uses_top_pdp_model_only(self):
        payload = {
            "data": {
                "productBySkuId": {
                    "skuId": "6619254",
                    "manufacturer": {"modelNumber": "QN65Q7FAAFXZA"},
                    "specificationGroups": [
                        {
                            "specifications": [
                                {"displayName": "Model Number", "value": "SPEC-SHOULD-NOT-BE-USED"},
                            ]
                        }
                    ],
                }
            }
        }
        spec_only_payload = {
            "data": {
                "productBySkuId": {
                    "skuId": "6619254",
                    "specificationGroups": [
                        {"specifications": [{"displayName": "Model Number", "value": "SPEC-ONLY"}]}
                    ],
                }
            }
        }

        self.assertEqual(
            item_mst_step.product_top_model_from_data(payload, "6619254"),
            "QN65Q7FAAFXZA",
        )
        self.assertEqual(item_mst_step.product_top_model_from_data(spec_only_payload, "6619254"), "")

    def test_tv_item_mst_rows_use_final_output_and_pdp_top_model(self):
        original = item_mst_step.model_from_detail_top
        item_mst_step.model_from_detail_top = lambda sku_id: {"6619254": "QN65Q7FAAFXZA"}.get(sku_id, "")
        try:
            rows = item_mst_step.item_mst_rows(
                "TV",
                [
                    {
                        "item": "ABC123",
                        "product_url": "https://www.bestbuy.com/site/samsung-tv/6619254.p?skuId=6619254",
                        "screen_size": '65"',
                        "estimated_annual_electricity_use": "140 kilowatt hours",
                    }
                ],
                timestamp="2026-05-28T00:00:00",
            )
        finally:
            item_mst_step.model_from_detail_top = original

        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["item"], "ABC123")
        self.assertEqual(rows[0]["sku"], "QN65Q7FAAFXZA")
        self.assertEqual(rows[0]["account_name"], "Bestbuy")
        self.assertEqual(rows[0]["screen_size"], '65"')
        self.assertEqual(rows[0]["estimated_annual_electricity_use"], "140 kilowatt hours")

    def test_hhp_item_mst_rows_use_hhp_specific_fields(self):
        original = item_mst_step.model_from_detail_top
        item_mst_step.model_from_detail_top = lambda sku_id: {"6665489": "SM-A176UZKAXAA"}.get(sku_id, "")
        try:
            rows = item_mst_step.item_mst_rows(
                "HHP",
                [
                    {
                        "item": "PHONE123",
                        "product_url": "https://www.bestbuy.com/site/galaxy-a17/6665489.p?skuId=6665489",
                        "hhp_carrier": "Unlocked",
                        "hhp_color": "Black",
                        "hhp_storage": "128 gigabytes",
                    }
                ],
                timestamp="2026-05-28T00:00:00",
            )
        finally:
            item_mst_step.model_from_detail_top = original

        self.assertEqual(rows[0]["sku"], "SM-A176UZKAXAA")
        self.assertEqual(rows[0]["hhp_carrier"], "Unlocked")
        self.assertEqual(rows[0]["hhp_color"], "Black")
        self.assertEqual(rows[0]["hhp_storage"], "128 gigabytes")
        self.assertNotIn("screen_size", rows[0])

    def test_item_mst_insert_uses_no_sku_when_top_model_is_missing(self):
        self.assertEqual(
            item_mst_step.value_for_insert({"sku": ""}, "sku", "text"),
            "no sku",
        )
        self.assertEqual(
            item_mst_step.value_for_insert({"account_name": ""}, "account_name", "text"),
            "Bestbuy",
        )

    def test_item_mst_existing_sku_is_not_overwritten(self):
        updates = item_mst_step.missing_only_updates(
            {
                "sku": "NEW-MODEL",
                "product_url": "https://new.example",
                "screen_size": "65 inches",
                "estimated_annual_electricity_use": "140",
            },
            {
                "sku": "EXISTING-MODEL",
                "product_url": "https://old.example",
                "screen_size": "65 inches",
                "estimated_annual_electricity_use": "",
            },
            item_mst_step.table_columns("TV"),
        )

        self.assertNotIn("sku", [name for name, _, _ in updates])
        self.assertNotIn("product_url", [name for name, _, _ in updates])
        self.assertIn("estimated_annual_electricity_use", [name for name, _, _ in updates])

    def test_item_mst_missing_existing_sku_can_be_filled_once(self):
        updates = item_mst_step.missing_only_updates(
            {
                "sku": "QN65Q7FAAFXZA",
                "product_url": "https://www.bestbuy.com/product/tv/ABC123",
                "screen_size": "65 inches",
            },
            {"sku": "no sku", "product_url": "", "screen_size": ""},
            item_mst_step.table_columns("TV"),
        )

        by_name = {name: value for name, _, value in updates}
        self.assertEqual(by_name["sku"], "QN65Q7FAAFXZA")
        self.assertEqual(by_name["product_url"], "https://www.bestbuy.com/product/tv/ABC123")
        self.assertEqual(by_name["screen_size"], "65 inches")

    def test_tv_detail_output_uses_defined_availability_fields(self):
        step08 = (ROOT / "bestbuy" / "step08_detail_enrichment.py").read_text(encoding="utf-8")

        self.assertIn("ALL_AVAILABILITY_FIELDS", step08)
        self.assertEqual(
            ALL_AVAILABILITY_FIELDS,
            ["pick_up_availability", "fastest_delivery", "delivery_availability"],
        )
        self.assertNotIn("for field in AVAILABILITY_FIELDS", step08)
        self.assertNotIn("fulfillment_button_text(products)", step08)
        self.assertNotIn("screen_size_from_name(product_name)", step08)

    def test_final_targets_propagate_sponsored_status_after_sku_dedupe(self):
        rows = [
            {
                "sku_id": "sku-duplicate",
                "page": "1",
                "visual_rank": "1",
                "global_visual_rank": "1",
                "container_type": "organic_product",
                "is_sponsored": "False",
                "sku_status": "",
                "product_name": "Organic Selected",
                "product_url": "https://example.test/organic",
            },
            {
                "sku_id": "sku-sponsored",
                "page": "1",
                "visual_rank": "19",
                "global_visual_rank": "19",
                "container_type": "sponsored_ingrid",
                "is_sponsored": "True",
                "sku_status": "Sponsored",
                "product_name": "Sponsored Selected",
                "product_url": "https://example.test/sponsored",
            },
            {
                "sku_id": "sku-duplicate",
                "page": "1",
                "visual_rank": "20",
                "global_visual_rank": "20",
                "container_type": "sponsored_ingrid",
                "is_sponsored": "True",
                "sku_status": "Sponsored",
                "product_name": "Sponsored Duplicate",
                "product_url": "https://example.test/duplicate-sponsored",
            },
        ]

        main_rows = final_targets_step.unique_main_rows(rows)
        main_attrs = final_targets_step.main_attribute_map(rows)
        enriched = final_targets_step.enrich_rows(main_rows, {}, {}, {}, main_attrs)
        by_sku = {row["sku_id"]: row for row in enriched}

        self.assertEqual(by_sku["sku-duplicate"]["container_type"], "organic_product")
        self.assertEqual(by_sku["sku-duplicate"]["sku_status"], "Sponsored")
        self.assertEqual(by_sku["sku-sponsored"]["sku_status"], "Sponsored")

        product_rows = final_targets_step.product_list_rows(enriched, {})
        product_by_sku = {row["sku_id"]: row for row in product_rows}
        self.assertEqual(product_by_sku["sku-duplicate"]["sku_status"], "Sponsored")
        self.assertEqual(product_by_sku["sku-sponsored"]["sku_status"], "Sponsored")

    def test_final_targets_dedupe_shared_item_across_different_skus(self):
        rows = [
            {
                "sku_id": "10214293",
                "bsin": "JJ8VPZTRG6",
                "page": "1",
                "visual_rank": "1",
                "global_visual_rank": "1",
                "container_type": "organic_product",
                "product_name": "LG TV",
                "product_url": "https://www.bestbuy.com/product/lg-tv/JJ8VPZTRG6/sku/10214293",
            },
            {
                "sku_id": "6621824",
                "bsin": "JJ8VPZTRG6",
                "page": "7",
                "visual_rank": "2",
                "global_visual_rank": "146",
                "container_type": "organic_product",
                "product_name": "LG TV",
                "product_url": "https://www.bestbuy.com/site/-/6621824.p?skuId=6621824&intl=nosplash",
            },
        ]

        main_rows = final_targets_step.unique_main_rows(rows)

        self.assertEqual(len(main_rows), 1)
        self.assertEqual(main_rows[0]["sku_id"], "10214293")

    def test_final_targets_dedupe_backfill_shared_product_item_url(self):
        main_rows = [
            {
                "sku_id": "10214293",
                "bsin": "JJ8VPZTRG6",
                "main_rank": "1",
                "product_name": "LG TV",
                "product_url": "https://www.bestbuy.com/product/lg-tv/JJ8VPZTRG6/sku/10214293",
            }
        ]
        trending_rows = [
            {
                "sku_id": "6621824",
                "trend_rank": "2",
                "retailer_sku_name": "LG TV",
                "product_url": "https://www.bestbuy.com/product/lg-tv/JJ8VPZTRG6",
            }
        ]

        selected, _ = final_targets_step.choose_final_rows(
            main_rows,
            [],
            target_size=300,
            promotion_rows=[],
            trending_rows=trending_rows,
            main_attrs={},
        )

        self.assertEqual(len(selected), 1)

    def test_final_targets_merge_bsr_rank_by_shared_bsin_when_sku_differs(self):
        main_rows = [
            {
                "sku_id": "11961800",
                "bsin": "J3P3229L6X",
                "main_rank": "13",
                "page": "1",
                "product_name": "Bosch refrigerator",
                "product_url": "https://www.bestbuy.com/product/bosch/J3P3229L6X/sku/11961800",
            }
        ]
        bsr_rows = [
            {
                "sku_id": "6590343",
                "bsin": "J3P3229L6X",
                "bsr_rank": "64",
                "source_page": "1",
                "product_name": "Bosch refrigerator",
                "product_url": "https://www.bestbuy.com/product/bosch/J3P3229L6X/sku/6590343",
            }
        ]

        bsr = final_targets_step.build_bsr_map(bsr_rows)
        enriched = final_targets_step.enrich_rows(main_rows, bsr, {}, {}, {})

        self.assertEqual(enriched[0]["sku_id"], "11961800")
        self.assertEqual(enriched[0]["bsr_rank"], "64")

        product_rows = final_targets_step.product_list_rows(
            enriched,
            final_targets_step.bsr_page_map(bsr_rows),
        )
        self.assertEqual(product_rows[0]["bsr_rank"], "64")
        self.assertEqual(product_rows[0]["bsr_page_number"], "1")

    def test_product_list_price_normalization_handles_money_text(self):
        self.assertEqual(
            final_targets_step.normalized_product_list_prices("999.99", "1,199.99", ""),
            ("$999.99", "$1,199.99", "$200.00"),
        )
        self.assertEqual(
            final_targets_step.normalized_product_list_prices("$149.99", "$229.98", "$80"),
            ("$149.99", "$229.98", "$79.99"),
        )
        self.assertEqual(
            final_targets_step.normalized_product_list_prices("$999.99", "$999.99", "$0"),
            ("$999.99", "", ""),
        )

    def test_detail_price_normalization_keeps_exact_savings_cents(self):
        self.assertEqual(
            detail_step.normalized_price_fields("$149.99", "$229.98", "$80"),
            ("$149.99", "$229.98", "$79.99"),
        )

    def test_hhp_promotion_listing_is_explicitly_skipped(self):
        orchestrator = (ROOT / "bestbuy" / "bestbuy_orchestrator.py").read_text(encoding="utf-8")
        final_targets = (ROOT / "bestbuy" / "step07_final_targets.py").read_text(encoding="utf-8")

        self.assertIn("HHP promotion page is not collected", orchestrator)
        self.assertIn('if CATEGORY == "HHP":', final_targets)
        self.assertIn("promotion_rows = []", final_targets)
        self.assertEqual(CATEGORY_SEARCH_TERMS["HHP"], "cellphone")
        self.assertEqual(CATEGORY_SEARCH_TERMS["REF"], "refrigerator")
        self.assertEqual(CATEGORY_SEARCH_TERMS["LDY"], "washing machine")

    def test_trending_parser_reads_json_response_xhr_when_html_is_shell(self):
        xhr_body = (
            '{"__typename":"SpotlightProductConnection","storyHeader":"Trending Deals in TVs & Projectors",'
            '"__typename":"SpotlightProduct","sku":"1234567","name":{"short":"Example TV"},'
            '"url":{"pdp":"/site/example-tv/1234567.p"},"bsin":"ABCD1234",'
            '"originalSkuId":"1234567"}'
        )

        rows = trending_step.parse_trending_products_from_capture(
            "<html><head><title>Trending</title></head></html>",
            {"xhr": [{"url": "https://www.bestbuy.com/gateway/graphql", "body": xhr_body}]},
            limit=10,
        )

        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["sku_id"], "1234567")
        self.assertEqual(rows[0]["retailer_sku_name"], "Example TV")
        self.assertEqual(rows[0]["source"], "json_response_spotlight_product_connection")

    def test_trending_parser_reads_direct_graphql_spotlight_connection(self):
        rows = trending_step.parse_trending_products_from_graphql(
            {
                "data": {
                    "story": {
                        "__typename": "SpotlightProductConnection",
                        "storyHeader": "Trending Deals in Cell Phones & Accessories",
                        "edges": [
                            {
                                "node": {
                                    "__typename": "SpotlightProduct",
                                    "sku": "7654321",
                                    "bsin": "BSIN1234",
                                    "product": {
                                        "name": {"short": "Example Phone"},
                                        "url": {"relativePdp": "/site/example-phone/7654321.p"},
                                    },
                                },
                            }
                        ],
                    }
                }
            },
            limit=10,
        )

        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["sku_id"], "7654321")
        self.assertEqual(rows[0]["bsin"], "BSIN1234")
        self.assertEqual(rows[0]["retailer_sku_name"], "Example Phone")
        self.assertEqual(rows[0]["product_url"], "https://www.bestbuy.com/site/example-phone/7654321.p")
        self.assertEqual(rows[0]["source"], "direct_graphql_spotlight_product_connection")

    def test_trending_step_uses_page_payload_by_default(self):
        orchestrator = (ROOT / "bestbuy" / "bestbuy_orchestrator.py").read_text(encoding="utf-8")

        self.assertIn('"BESTBUY_TRENDING_FETCH_MODE": "page_payload"', orchestrator)
        self.assertIn('"BESTBUY_TRENDING_ALLOW_RENDER_FALLBACK": "1"', orchestrator)
        self.assertIn('"BESTBUY_TRENDING_WAIT_MS_SEQUENCE": "30000"', orchestrator)
        self.assertEqual(HHP_TRENDING_PAGE_PAYLOAD_ENV["BESTBUY_TRENDING_FETCH_MODE"], "page_payload")
        self.assertEqual(HHP_TRENDING_PAGE_PAYLOAD_ENV["BESTBUY_TRENDING_ALLOW_NETWORK_SKUS"], "0")
        self.assertEqual(HHP_TRENDING_PAGE_PAYLOAD_ENV["BESTBUY_TRENDING_WAIT_MS_SEQUENCE"], "30000")

    def test_listing_step_requires_saved_graphql_payload_by_default(self):
        step01 = (ROOT / "bestbuy" / "step01_main_list.py").read_text(encoding="utf-8")
        orchestrator = (ROOT / "bestbuy" / "bestbuy_orchestrator.py").read_text(encoding="utf-8")

        self.assertIn('BESTBUY_MAIN_ALLOW_HTML_TEMPLATE", "0"', step01)
        self.assertIn("if ALLOW_HTML_TEMPLATE:", step01)
        self.assertIn('"BESTBUY_MAIN_ALLOW_HTML_TEMPLATE": "0"', orchestrator)

    def test_hhp_promotion_type_comes_from_detail_badge(self):
        old_category = detail_step.CATEGORY
        detail_step.CATEGORY = "HHP"
        try:
            self.assertEqual(
                detail_step.hhp_promotion_type(
                    [
                        {
                            "badgesV2": [
                                {"label": "Best Selling"},
                                {"label": "Best Selling"},
                                {"label": "Trending Deal"},
                            ]
                        }
                    ],
                    "",
                ),
                "Best Selling",
            )
        finally:
            detail_step.CATEGORY = old_category

    def test_hhp_carrier_prefers_selected_carrier_over_compatibility_list(self):
        product = {
            "skuId": "6665489",
            "productVariationDetailDisplay": {
                "productVariations": [
                    {
                        "sku": "6665489",
                        "variations": [
                            {"rawName": "Cell_Phones:Carrier", "value": "Unlocked"},
                            {"rawName": "Communications:Color", "value": "Black"},
                        ],
                    }
                ]
            },
            "specificationGroups": [
                {
                    "specifications": [
                        {
                            "displayName": "Carrier Compatibility",
                            "value": "AT&T, Boost Mobile, Cricket, Verizon, Visible",
                        },
                        {"displayName": "Built-in Storage", "value": "128 gigabytes"},
                        {"displayName": "Color", "value": "Black"},
                    ]
                }
            ],
            "color": {"displayName": "Black"},
        }

        attrs = hhp_attributes_from_product(
            product,
            "Samsung - Galaxy A17 5G 128GB - Black",
            "6665489",
        )

        self.assertEqual(attrs["hhp_carrier"], "Unlocked")
        self.assertEqual(attrs["hhp_storage"], "128 gigabytes")
        self.assertEqual(attrs["hhp_color"], "Black")

        no_product_name_fallback_attrs = hhp_attributes_from_product(
            {},
            "Samsung - Galaxy A17 5G 128GB (Unlocked) - Black",
            "6665489",
        )
        self.assertEqual(no_product_name_fallback_attrs["hhp_carrier"], "")
        self.assertEqual(no_product_name_fallback_attrs["hhp_storage"], "")
        self.assertEqual(no_product_name_fallback_attrs["hhp_color"], "")

    def test_hhp_carrier_uses_unlocked_fallback_only_when_title_and_spec_match(self):
        product = {
            "specificationGroups": [
                {
                    "specifications": [
                        {"displayName": "Carrier Compatibility", "value": "AT&T, Verizon, T-Mobile"},
                        {"displayName": "Unlocked", "value": "Yes"},
                    ]
                }
            ]
        }

        attrs = hhp_attributes_from_product(
            product,
            "Nokia - 2780 Flip Phone (Unlocked) - Black",
            "6584894",
        )
        self.assertEqual(attrs["hhp_carrier"], "Unlocked")

        title_only_attrs = hhp_attributes_from_product(
            {
                "specificationGroups": [
                    {
                        "specifications": [
                            {"displayName": "Carrier Compatibility", "value": "AT&T, Verizon, T-Mobile"},
                        ]
                    }
                ]
            },
            "Nokia - 2780 Flip Phone (Unlocked) - Black",
            "6584894",
        )
        self.assertEqual(title_only_attrs["hhp_carrier"], "")

        spec_only_attrs = hhp_attributes_from_product(
            product,
            "Nokia - 2780 Flip Phone - Black",
            "6584894",
        )
        self.assertEqual(spec_only_attrs["hhp_carrier"], "")

    def test_hhp_carrier_does_not_use_carrier_compatibility_as_carrier(self):
        attrs = hhp_attributes_from_product(
            {
                "specificationGroups": [
                    {
                        "specifications": [
                            {"displayName": "Carrier Compatibility", "value": "AT&T, Verizon, T-Mobile"},
                        ]
                    }
                ]
            },
            "Nokia - 2780 Flip Phone - Black",
            "6584894",
        )

        self.assertEqual(attrs["hhp_carrier"], "")

    def test_ldy_capacity_and_loading_type_from_specs_only(self):
        attrs = ldy_attributes_from_product(
            [
                {
                    "specificationGroups": [
                        {
                            "specifications": [
                                {"displayName": "Capacity", "value": "4.5 cubic feet"},
                                {"displayName": "Washer Load Type", "value": "Front Load"},
                            ]
                        }
                    ]
                }
            ],
            "Samsung washer",
        )

        self.assertEqual(attrs["ldy_capacity"], "4.5 cubic feet")
        self.assertEqual(attrs["ldy_loading_type"], "Front Load")

        no_product_name_fallback_attrs = ldy_attributes_from_product(
            [],
            "LG 5.0 Cu. Ft. Top Load Washer",
        )
        self.assertEqual(no_product_name_fallback_attrs["ldy_capacity"], "")
        self.assertEqual(no_product_name_fallback_attrs["ldy_loading_type"], "")

        non_target_spec_attrs = ldy_attributes_from_product(
            [
                {
                    "specificationGroups": [
                        {
                            "specifications": [
                                {"displayName": "Washer Capacity", "value": "4.5 cubic feet"},
                                {"displayName": "Load Type", "value": "Front Load"},
                            ]
                        }
                    ]
                }
            ],
            "Samsung washer",
        )
        self.assertEqual(non_target_spec_attrs["ldy_capacity"], "")
        self.assertEqual(non_target_spec_attrs["ldy_loading_type"], "")

        capacity_section_attrs = ldy_attributes_from_product(
            [
                {
                    "specificationGroups": [
                        {
                            "name": "Capacity",
                            "specifications": [
                                {"displayName": "Washer Capacity", "value": "5.3 cubic feet"},
                            ],
                        }
                    ]
                }
            ],
            "Samsung washer",
        )
        self.assertEqual(capacity_section_attrs["ldy_capacity"], "5.3 cubic feet")

        washer_dryer_capacity_attrs = ldy_attributes_from_product(
            [
                {
                    "specificationGroups": [
                        {
                            "name": "Capacity",
                            "specifications": [
                                {"displayName": "Washer Dryer Capacity", "value": "2.8 cubic feet"},
                            ],
                        }
                    ]
                }
            ],
            "LG washer dryer combo",
        )
        self.assertEqual(washer_dryer_capacity_attrs["ldy_capacity"], "2.8 cubic feet")

        spec_value_attrs = ldy_attributes_from_product(
            [
                {
                    "specificationGroups": [
                        {
                            "specifications": [
                                {"displayName": "Capacity", "value": "0 cubic feet"},
                            ]
                        }
                    ]
                }
            ],
            "Portable washer",
        )
        self.assertEqual(spec_value_attrs["ldy_capacity"], "0 cubic feet")

    def test_ref_capacity_and_type_from_specs_only(self):
        attrs = ref_attributes_from_product(
            [
                {
                    "specificationGroups": [
                        {
                            "specifications": [
                                {"displayName": "Total Capacity", "value": "28 cubic feet"},
                                {"displayName": "Refrigerator Style", "value": "French Door"},
                            ]
                        }
                    ]
                }
            ],
            "Samsung refrigerator",
        )

        self.assertEqual(attrs["ref_capacity"], "28 cubic feet")
        self.assertEqual(attrs["ref_refrigerator_type"], "French Door")

        no_product_name_fallback_attrs = ref_attributes_from_product(
            [],
            "LG 26 Cu. Ft. Side-by-Side Refrigerator",
        )
        self.assertEqual(no_product_name_fallback_attrs["ref_capacity"], "26 cubic feet")
        self.assertEqual(no_product_name_fallback_attrs["ref_refrigerator_type"], "")

        non_total_spec_attrs = ref_attributes_from_product(
            [
                {
                    "specificationGroups": [
                        {
                            "specifications": [
                                {"displayName": "Capacity", "value": "7.8 cubic feet"},
                                {"displayName": "Freezer Capacity", "value": "7.8 cubic feet"},
                            ]
                        }
                    ]
                }
            ],
            "KitchenAid - 36 in. Wide 26 cu. ft. Multi-Door French Door Refrigerator",
        )
        self.assertEqual(non_total_spec_attrs["ref_capacity"], "26 cubic feet")

        refrigerator_capacity_attrs = ref_attributes_from_product(
            [
                {
                    "specificationGroups": [
                        {
                            "name": "Capacity",
                            "specifications": [
                                {"displayName": "Refrigerator Capacity", "value": "2.3 cubic feet"},
                                {"displayName": "Freezer Capacity", "value": "1 cubic feet"},
                            ],
                        }
                    ]
                }
            ],
            "Compact refrigerator",
        )
        self.assertEqual(refrigerator_capacity_attrs["ref_capacity"], "2.3 cubic feet")

        freezer_only_attrs = ref_attributes_from_product(
            [
                {
                    "specificationGroups": [
                        {
                            "name": "Capacity",
                            "specifications": [
                                {"displayName": "Freezer Capacity", "value": "1 cubic feet"},
                            ],
                        }
                    ]
                }
            ],
            "Compact refrigerator",
        )
        self.assertEqual(freezer_only_attrs["ref_capacity"], "")

        drawer_attrs = ref_attributes_from_product(
            [],
            "VEVOR - 24 inch Undercounter Refrigerator, 2 Drawer Refrigerator with Different Temperature, 4.87 Cu.ft. Capacity",
        )
        self.assertEqual(drawer_attrs["ref_capacity"], "4.87 cubic feet")
        self.assertEqual(drawer_attrs["ref_refrigerator_type"], "Drawer")

        box_contents_attrs = ref_attributes_from_product(
            [
                {
                    "operationalAttributes": [
                        {
                            "displayName": "Box_Contents",
                            "values": ["KitchenAid 26 cu. ft. French Door Refrigerator"],
                        }
                    ],
                    "specificationGroups": [
                        {
                            "specifications": [
                                {"displayName": "Total Capacity", "value": "7.8 cubic feet"},
                                {"displayName": "Refrigerator Style", "value": "French Door"},
                            ]
                        }
                    ]
                }
            ],
            "KitchenAid refrigerator",
        )
        self.assertEqual(box_contents_attrs["ref_capacity"], "7.8 cubic feet")
        self.assertEqual(box_contents_attrs["ref_refrigerator_type"], "French Door")
        self.assertIn("operationalAttributes", PRODUCT_SCHEMA_REVIEW20_QUERY)

        j3_attrs = ref_attributes_from_product(
            [
                {
                    "operationalAttributes": [
                        {
                            "displayName": "Box_Contents",
                            "values": ["KitchenAid 26 cu. ft. French Door Refrigerator"],
                        }
                    ],
                    "specificationGroups": [
                        {
                            "specifications": [
                                {"displayName": "Total Capacity", "value": "7.8 cubic feet"},
                                {"displayName": "Refrigerator Capacity", "value": "18.4 cubic feet"},
                                {"displayName": "Freezer Capacity", "value": "7.8 cubic feet"},
                                {"displayName": "Refrigerator Style", "value": "French Door"},
                            ]
                        }
                    ],
                }
            ],
            "KitchenAid - 36 in. Wide 26 cu. ft. Multi-Door French Door Refrigerator",
        )
        self.assertEqual(j3_attrs["ref_capacity"], "7.8 cubic feet")
        self.assertEqual(j3_attrs["ref_refrigerator_type"], "French Door")

        style_attrs = ref_attributes_from_product(
            [
                {
                    "operationalAttributes": [
                        {
                            "displayName": "Box_Contents",
                            "values": ["Insignia 18.6 cu. ft. Bottom Freezer Refrigerator"],
                        }
                    ],
                    "specificationGroups": [
                        {
                            "specifications": [
                                {"displayName": "Refrigerator Style", "value": "Bottom Freezer"},
                            ]
                        }
                    ],
                }
            ],
            "",
        )
        self.assertEqual(style_attrs["ref_refrigerator_type"], "Bottom Freezer")

    def test_hhp_trade_in_text_from_html_and_detail_product(self):
        html = (
            '<span data-testid="trade-in-check-your-value">Check your trade-in value.</span>'
            '<span data-testid="trade-in-save-when-you-trade">Save when you trade in a similar device.</span>'
        )
        amount_html = (
            '<span data-testid="trade-in-check-your-value">Check your trade-in value.</span>'
            '<span data-testid="trade-in-save-up-to-830"><strong>Save up to $830.00</strong> '
            "when you trade in a similar device.</span>"
        )

        self.assertEqual(
            trade_in_from_html(html),
            "Check your trade-in value. Save when you trade in a similar device.",
        )
        self.assertEqual(
            trade_in_from_html(amount_html),
            "Check your trade-in value. Save up to $830.00 when you trade in a similar device.",
        )
        self.assertEqual(
            trade_in_from_products([{"isPurchaseWithTradeInEligible": True}]),
            "Check your trade-in value. Save when you trade in a similar device.",
        )
        self.assertEqual(
            trade_in_from_offer_data(
                {
                    "productBySkuId": {
                        "tradeInOffer": {
                            "offerCarrierValue": [
                                {
                                    "carrierCode": "VEZ",
                                    "carrierUpToValue": "830",
                                }
                            ]
                        }
                    }
                }
            ),
            "Check your trade-in value. Save up to $830.00 when you trade in a similar device.",
        )
        self.assertEqual(
            trade_in_from_offer_data(
                {
                    "productBySkuId": {
                        "tradeInOffer": {
                            "offerCarrierValue": [
                                {
                                    "carrierCode": "ATT",
                                    "carrierUpToValue": "415.5",
                                }
                            ]
                        }
                    }
                }
            ),
            "Check your trade-in value. Save up to $415.50 when you trade in a similar device.",
        )
        self.assertIn("isPurchaseWithTradeInEligible", PRODUCT_SCHEMA_REVIEW20_QUERY)
        self.assertIn("productVariationDetailDisplay", PRODUCT_SCHEMA_REVIEW20_QUERY)
        self.assertIn("badgesV2", PRODUCT_SCHEMA_REVIEW20_QUERY)
        self.assertIn("tradeInOffer", TRADE_IN_DATA_QUERY)

    def test_hhp_trade_in_reads_direct_apollo_without_html(self):
        old_detail_payloads = detail_step.detail_payloads

        def fake_detail_payloads(_sku):
            return [
                {
                    "events": [
                        {
                            "type": "next",
                            "value": {
                                "data": {
                                    "productBySkuId": {
                                        "tradeInOffer": {
                                            "offerCarrierValue": [
                                                {"carrierCode": "VEZ", "carrierUpToValue": "830"}
                                            ]
                                        }
                                    }
                                }
                            },
                            "id": "direct-graphql-trade-in",
                        }
                    ]
                }
            ]

        detail_step.detail_payloads = fake_detail_payloads
        try:
            self.assertEqual(
                detail_step.trade_in_from_detail_payloads("6487351"),
                "Check your trade-in value. Save up to $830.00 when you trade in a similar device.",
            )
        finally:
            detail_step.detail_payloads = old_detail_payloads

    def test_hhp_compare_v2_payload_stays_inside_detail_batch(self):
        old_category = detail_step.CATEGORY
        old_fetch_compare = detail_step.FETCH_COMPARE
        detail_step.CATEGORY = "HHP"
        detail_step.FETCH_COMPARE = True
        try:
            payloads, indices = detail_step.detail_batch_payloads_for_sku("6578951")
        finally:
            detail_step.CATEGORY = old_category
            detail_step.FETCH_COMPARE = old_fetch_compare

        self.assertIn("compare", indices)
        self.assertIn("compare_v2", indices)
        self.assertIn("trade_in", indices)
        self.assertEqual(payloads[indices["compare_v2"]]["operationName"], "ProductCarousel_Recommendations")
        self.assertEqual(payloads[indices["compare_v2"]]["variables"]["placement"], "pdp-compare")
        self.assertEqual(payloads[indices["trade_in"]]["operationName"], "GetTradeInData")
        self.assertIn("recommendationsV2", COMPARE_RECOMMENDATIONS_V2_QUERY)

    def test_compare_parser_accepts_recommendations_v2(self):
        data = {
            "recommendationsV2": {
                "subPlacements": [
                    {
                        "recommendations": [
                            {"item": {"name": {"short": "Motorola - moto g - 2025 128GB"}}},
                            {"item": {"name": {"title": "Motorola - edge 2024 256GB"}}},
                        ]
                    }
                ]
            }
        }

        self.assertEqual(
            compare_recommendation_names(data),
            ["Motorola - moto g - 2025 128GB", "Motorola - edge 2024 256GB"],
        )


if __name__ == "__main__":
    unittest.main()
