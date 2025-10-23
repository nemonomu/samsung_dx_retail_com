import psycopg2

DB_CONFIG = {
    'host': 'samsung-dx-crawl.csnixzmkuppn.ap-northeast-2.rds.amazonaws.com',
    'port': 5432,
    'database': 'postgres',
    'user': 'postgres',
    'password': 'admin2025!'
}

def insert_walmart_detail_xpath():
    """Insert Walmart detail page XPath selectors into xpath_selectors table"""
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()

        print("="*80)
        print("Inserting Walmart Detail Page XPath Selectors")
        print("="*80)

        # Walmart detail page XPath selectors (placeholders - need actual XPaths)
        walmart_detail_xpaths = [
            # Retailer_SKU_Name
            {
                'mall_name': 'Walmart',
                'page_type': 'detail_page',
                'data_field': 'product_name',
                'xpath': '//*[@id="main-title"]',
                'description': 'Product title/name on detail page'
            },
            # Star_Rating
            {
                'mall_name': 'Walmart',
                'page_type': 'detail_page',
                'data_field': 'star_rating',
                'xpath': '',
                'description': 'Product star rating (e.g., "4.5 out of 5")'
            },
            # Number_of_ppl_purchased_yesterday
            {
                'mall_name': 'Walmart',
                'page_type': 'detail_page',
                'data_field': 'purchased_yesterday',
                'xpath': '',
                'description': 'Number of people who purchased yesterday'
            },
            # Number_of_ppl_added_to_carts
            {
                'mall_name': 'Walmart',
                'page_type': 'detail_page',
                'data_field': 'added_to_carts',
                'xpath': '',
                'description': 'Number of people who added to carts'
            },
            # SKU_Popularity
            {
                'mall_name': 'Walmart',
                'page_type': 'detail_page',
                'data_field': 'sku_popularity',
                'xpath': '',
                'description': 'SKU popularity badge or indicator'
            },
            # Savings
            {
                'mall_name': 'Walmart',
                'page_type': 'detail_page',
                'data_field': 'savings',
                'xpath': '',
                'description': 'Savings amount (discount price)'
            },
            # Discount_Type
            {
                'mall_name': 'Walmart',
                'page_type': 'detail_page',
                'data_field': 'discount_type',
                'xpath': '',
                'description': 'Type of discount (rollback, clearance, etc.)'
            },
            # Shipping_Info
            {
                'mall_name': 'Walmart',
                'page_type': 'detail_page',
                'data_field': 'shipping_info',
                'xpath': '',
                'description': 'Shipping availability and delivery info'
            },
            # Count_of_Star_Ratings
            {
                'mall_name': 'Walmart',
                'page_type': 'detail_page',
                'data_field': 'count_of_star_ratings',
                'xpath': '',
                'description': 'Total number of customer ratings'
            },
            # Detailed_Review_Content
            {
                'mall_name': 'Walmart',
                'page_type': 'detail_page',
                'data_field': 'detailed_review',
                'xpath': '',
                'description': 'Detailed review content (up to 20 reviews)'
            }
        ]

        print(f"\n[INFO] Inserting {len(walmart_detail_xpaths)} Walmart detail page XPath selectors...")

        for xpath_data in walmart_detail_xpaths:
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
        print(f"SUCCESS: {len(walmart_detail_xpaths)} Walmart detail page XPath selectors inserted!")
        print("="*80)

    except Exception as e:
        print(f"\n[ERROR] {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    insert_walmart_detail_xpath()
    print("\n[INFO] Script completed. Exiting...")
