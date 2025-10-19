import psycopg2

DB_CONFIG = {
    'host': 'samsung-dx-crawl.csnixzmkuppn.ap-northeast-2.rds.amazonaws.com',
    'port': 5432,
    'database': 'postgres',
    'user': 'postgres',
    'password': 'admin2025!'
}

def insert_amazon_detail_xpath():
    """Insert Amazon detail page XPath selectors into xpath_selectors table"""
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()

        print("="*80)
        print("Inserting Amazon Detail Page XPath Selectors")
        print("="*80)

        # Amazon detail page XPath selectors
        amazon_detail_xpaths = [
            # Star_Rating
            {
                'mall_name': 'Amazon',
                'page_type': 'detail_page',
                'data_field': 'star_rating',
                'xpath': '//*[@id="acrPopover"]/span[1]/a/span',
                'description': 'Product star rating (e.g., "4.5 out of 5 stars")'
            },
            # Retailer_SKU_Name
            {
                'mall_name': 'Amazon',
                'page_type': 'detail_page',
                'data_field': 'product_name',
                'xpath': '//*[@id="productTitle"]',
                'description': 'Product title/name on detail page'
            },
            # SKU_Popularity (Amazon's Choice badge)
            {
                'mall_name': 'Amazon',
                'page_type': 'detail_page',
                'data_field': 'sku_popularity',
                'xpath': '//*[@id="acBadge_feature_div"]/div/span[1]/span/span',
                'description': 'SKU popularity badge (collect only if "Amazon\'s Choice")'
            },
            # Retailer_Membership_Discounts
            {
                'mall_name': 'Amazon',
                'page_type': 'detail_page',
                'data_field': 'membership_discount',
                'xpath': '//*[@id="mir-layout-DELIVERY_BLOCK-slot-SECONDARY_DELIVERY_MESSAGE_LARGE"]/span',
                'description': 'Prime membership discount info (from "Prime members get FREE delivery" to before "Join Prime")'
            },
            # Samsung_SKU_Name
            {
                'mall_name': 'Amazon',
                'page_type': 'detail_page',
                'data_field': 'samsung_sku_name',
                'xpath': '//*[@id="productDetails_techSpec_section_1"]/tbody/tr[4]/td',
                'description': 'Samsung model number from technical specifications'
            },
            # Rank_1
            {
                'mall_name': 'Amazon',
                'page_type': 'detail_page',
                'data_field': 'rank_1',
                'xpath': '//*[@id="productDetails_detailBullets_sections1"]/tbody/tr[3]/td/span/ul/li[1]/span/span',
                'description': 'Primary best seller rank (remove parentheses content)'
            },
            # Rank_2
            {
                'mall_name': 'Amazon',
                'page_type': 'detail_page',
                'data_field': 'rank_2',
                'xpath': '//*[@id="productDetails_detailBullets_sections1"]/tbody/tr[3]/td/span/ul/li[2]/span/span',
                'description': 'Secondary best seller rank (remove parentheses content)'
            },
            # Count_of_Star_Ratings (placeholder - not collecting yet)
            {
                'mall_name': 'Amazon',
                'page_type': 'detail_page',
                'data_field': 'count_of_star_ratings',
                'xpath': '',
                'description': 'Total number of customer ratings (TBD)'
            },
            # Summarized_Review_Content (placeholder - not collecting yet)
            {
                'mall_name': 'Amazon',
                'page_type': 'detail_page',
                'data_field': 'summarized_review',
                'xpath': '',
                'description': 'AI-summarized review content (TBD)'
            },
            # Detailed_Review_Content (placeholder - not collecting yet)
            {
                'mall_name': 'Amazon',
                'page_type': 'detail_page',
                'data_field': 'detailed_review',
                'xpath': '',
                'description': 'Detailed review content (TBD)'
            }
        ]

        print(f"\n[INFO] Inserting {len(amazon_detail_xpaths)} Amazon detail page XPath selectors...")

        for xpath_data in amazon_detail_xpaths:
            cursor.execute("""
                INSERT INTO xpath_selectors
                (mall_name, page_type, data_field, xpath, description, is_active, updated_by)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (mall_name, page_type, data_field)
                DO UPDATE SET
                    xpath = EXCLUDED.xpath,
                    description = EXCLUDED.description,
                    updated_at = CURRENT_TIMESTAMP,
                    updated_by = EXCLUDED.updated_by
            """, (
                xpath_data['mall_name'],
                xpath_data['page_type'],
                xpath_data['data_field'],
                xpath_data['xpath'],
                xpath_data['description'],
                True,
                'claude_code'
            ))

            print(f"  [OK] {xpath_data['data_field']}: {xpath_data['xpath'][:60] if xpath_data['xpath'] else '[EMPTY - TBD]'}...")

        conn.commit()
        cursor.close()
        conn.close()

        print("\n" + "="*80)
        print(f"SUCCESS: {len(amazon_detail_xpaths)} Amazon detail page XPath selectors inserted!")
        print("="*80)

    except Exception as e:
        print(f"\n[ERROR] {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    insert_amazon_detail_xpath()
    print("\n[INFO] Script completed. Exiting...")
