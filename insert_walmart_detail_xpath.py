import psycopg2

# Import database configuration
from config import DB_CONFIG

def insert_walmart_detail_xpath():
    """Insert Walmart detail page XPath selectors into xpath_selectors table"""
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()

        print("="*80)
        print("Inserting Walmart Detail Page XPath Selectors")
        print("="*80)

        # Walmart detail page XPath selectors
        walmart_detail_xpaths = [
            # Retailer_SKU_Name
            {
                'mall_name': 'Walmart',
                'page_type': 'detail_page',
                'data_field': 'product_name',
                'xpath': '//*[@id="main-title"]',
                'description': 'Product title/name on detail page'
            },
            # Sku (Model)
            {
                'mall_name': 'Walmart',
                'page_type': 'detail_page',
                'data_field': 'sku_model',
                'xpath': '/html/body/div[2]/div/div[2]/div[1]/div/div[2]/div/div/div[8]/div/span',
                'description': 'Model number from Specifications > More details > Model'
            },
            # Star_Rating
            {
                'mall_name': 'Walmart',
                'page_type': 'detail_page',
                'data_field': 'star_rating',
                'xpath': '//*[@id="item-review-section"]/div[2]/div[1]/div[1]/span[1]',
                'description': 'Product star rating (extract number from "4.4 out of 5")'
            },
            # Number_of_ppl_purchased_yesterday
            {
                'mall_name': 'Walmart',
                'page_type': 'detail_page',
                'data_field': 'purchased_yesterday',
                'xpath': '//*[@id="maincontent"]/section/main/div[2]/div[2]/div/div[2]/div/div[2]/div/div/div[1]/div/span[1]/span',
                'description': 'Number of people who purchased yesterday (extract number from "100+ bought since yesterday")'
            },
            # Number_of_ppl_added_to_carts
            {
                'mall_name': 'Walmart',
                'page_type': 'detail_page',
                'data_field': 'added_to_carts',
                'xpath': '//*[@id="maincontent"]/section/main/div[2]/div[2]/div/div[2]/div/div[2]/div/div/div[1]/div/span/span',
                'description': 'Number of people who added to carts (extract number from "In 200+ people\'s carts")'
            },
            # SKU_Popularity
            {
                'mall_name': 'Walmart',
                'page_type': 'detail_page',
                'data_field': 'sku_popularity',
                'xpath': '//*[@id="maincontent"]/section/main/div[2]/div[2]/div/div[2]/div/div[2]/div/div/div[1]/div/span/span',
                'description': 'SKU popularity badge (Best seller, Popular pick, Reduced price, etc.)'
            },
            # Savings
            {
                'mall_name': 'Walmart',
                'page_type': 'detail_page',
                'data_field': 'savings',
                'xpath': '//*[@id="maincontent"]/section/main/div[2]/div[2]/div/div[3]/div/div[1]/div/div[2]/div/div/div[1]/div[2]/span[2]',
                'description': 'Savings amount (e.g., "$700.00")'
            },
            # Discount_Type
            {
                'mall_name': 'Walmart',
                'page_type': 'detail_page',
                'data_field': 'discount_type',
                'xpath': '//*[@id="maincontent"]/section/main/div[2]/div[2]/div/div[3]/div/div[1]/div/div[2]/div/div/div[2]/span[1]',
                'description': 'Type of discount (Price when purchased online, etc.)'
            },
            # Shipping_Info (first part)
            {
                'mall_name': 'Walmart',
                'page_type': 'detail_page',
                'data_field': 'shipping_info_1',
                'xpath': '//*[@id="maincontent"]/section/main/div[2]/div[2]/div/div[3]/div/div[1]/div/div[2]/div/div/div[6]/section/div/div/section[1]/div/fieldset/div/div[1]/span/label/div[3]',
                'description': 'Shipping info part 1 (Arrives by tomorrow)'
            },
            # Shipping_Info (second part)
            {
                'mall_name': 'Walmart',
                'page_type': 'detail_page',
                'data_field': 'shipping_info_2',
                'xpath': '//*[@id="maincontent"]/section/main/div[2]/div[2]/div/div[3]/div/div[1]/div/div[2]/div/div/div[6]/section/div/div/section[1]/div/fieldset/div/div[1]/span/label/div[4]',
                'description': 'Shipping info part 2 (order within 12hr 49min)'
            },
            # Count_of_Star_Ratings (total ratings)
            {
                'mall_name': 'Walmart',
                'page_type': 'detail_page',
                'data_field': 'total_ratings',
                'xpath': '//*[@id="item-review-section"]/div[2]/div[1]/div[1]/div/span[2]',
                'description': 'Total number of ratings (e.g., "685 ratings")'
            },
            # Retailer_SKU_Name_similar (base container)
            {
                'mall_name': 'Walmart',
                'page_type': 'detail_page',
                'data_field': 'similar_products',
                'xpath': '//*[@id="ip-carousel-Similar items you might like"]/section/section/div/ul/li',
                'description': 'Similar product container (collect all product names)'
            },
            # View all reviews button
            {
                'mall_name': 'Walmart',
                'page_type': 'detail_page',
                'data_field': 'view_all_reviews_button',
                'xpath': '//*[@id="item-review-section"]/div[8]/button',
                'description': 'View all reviews button'
            },
            # Review content (base container)
            {
                'mall_name': 'Walmart',
                'page_type': 'detail_page',
                'data_field': 'review_container',
                'xpath': '//*[@id="maincontent"]/main/section/div[3]/section[2]/div[1]/div',
                'description': 'Review container on reviews page'
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
