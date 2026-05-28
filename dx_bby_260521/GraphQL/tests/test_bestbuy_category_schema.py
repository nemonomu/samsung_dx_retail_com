import sys
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from bestbuy.step00_config import BESTBUY_OUTPUT_TABLES  # noqa: E402
from bestbuy.step00_availability_policy import ALL_AVAILABILITY_FIELDS  # noqa: E402
from bestbuy.bestbuy_orchestrator import CATEGORY_SEARCH_TERMS, HHP_TRENDING_PAGE_PAYLOAD_ENV  # noqa: E402
import bestbuy.step07_final_targets as final_targets_step  # noqa: E402
import bestbuy.step06_trending_deals as trending_step  # noqa: E402
import bestbuy.step14_db_load as db_load_step  # noqa: E402
import bestbuy.step08_detail_enrichment as detail_step  # noqa: E402
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
from bestbuy.step13_db_prepare import HHP_COLUMNS, LDY_COLUMNS, REF_COLUMNS  # noqa: E402


def column_names(columns):
    return [name for name, _ in columns]


class BestBuyCategorySchemaTests(unittest.TestCase):
    def test_output_table_names_match_confirmed_targets(self):
        self.assertEqual(BESTBUY_OUTPUT_TABLES["HHP"], "hhp_retail_com_bby_v2_test")
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

    def test_ldy_final_output_uses_confirmed_insert_columns_only(self):
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

    def test_db_prepare_adds_missing_columns_for_existing_tables(self):
        step13 = (ROOT / "bestbuy" / "step13_db_prepare.py").read_text(encoding="utf-8")

        self.assertIn("BESTBUY_DB_PREPARE_ADD_MISSING_COLUMNS", step13)
        self.assertIn("ALTER TABLE", step13)
        self.assertIn("ADD COLUMN", step13)

    def test_hhp_test10_runner_keeps_run_limited_and_dry(self):
        script = (ROOT / "bby_hhp_test10_task.bat").read_text(encoding="utf-8")

        self.assertIn('set "BESTBUY_FORCE_STEP_ENV=0"', script)
        self.assertIn('set "BESTBUY_FINAL_TARGET_SIZE=7"', script)
        self.assertIn('set "BESTBUY_BSR_RANK_LIMIT=20"', script)
        self.assertIn('set "BESTBUY_FINAL_ROW_LIMIT=10"', script)
        self.assertIn('set "BESTBUY_DB_LOAD_DRY_RUN=1"', script)
        self.assertIn('call "%~dp0_bby_daily_task.bat" HHP', script)

    def test_ref_test10_runner_keeps_run_limited_and_dry(self):
        script = (ROOT / "bby_ref_test10_task.bat").read_text(encoding="utf-8")

        self.assertIn('set "BESTBUY_FORCE_STEP_ENV=0"', script)
        self.assertIn('set "BESTBUY_FINAL_TARGET_SIZE=7"', script)
        self.assertIn('set "BESTBUY_BSR_RANK_LIMIT=20"', script)
        self.assertIn('set "BESTBUY_FINAL_ROW_LIMIT=10"', script)
        self.assertIn('set "BESTBUY_DB_LOAD_DRY_RUN=1"', script)
        self.assertIn('call "%~dp0_bby_daily_task.bat" REF', script)

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
