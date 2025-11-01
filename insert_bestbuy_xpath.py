import psycopg2

# Import database configuration
from config import DB_CONFIG

def insert_bestbuy_xpath():
    """Insert Best Buy XPath selectors into xpath_selectors table"""
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()

        print("="*80)
        print("Inserting Best Buy XPath Selectors")
        print("="*80)

        # First, check table structure
        print("\n[INFO] Checking table structure...")
        cursor.execute("""
            SELECT column_name, data_type
            FROM information_schema.columns
            WHERE table_name = 'xpath_selectors'
            ORDER BY ordinal_position;
        """)

        columns = cursor.fetchall()
        print(f"[OK] Table columns:")
        for col_name, col_type in columns:
            print(f"  - {col_name}: {col_type}")

        # Best Buy XPath selectors from bestbuy_tv_main_crawl.py
        bestbuy_xpaths = [
            {
                'mall_name': 'Best Buy',
                'page_type': 'main',
                'data_field': 'product_container',
                'xpath': '//li[contains(@class, "product-list-item") and contains(@class, "product-list-item-gridView")]',
                'description': 'Main product container for each TV listing'
            },
            {
                'mall_name': 'Best Buy',
                'page_type': 'main',
                'data_field': 'product_name',
                'xpath': './/h2[contains(@class, "product-title")]',
                'description': 'Product name/title (primary selector)'
            },
            {
                'mall_name': 'Best Buy',
                'page_type': 'main',
                'data_field': 'product_name_fallback1',
                'xpath': './/a[@class="product-list-item-link"]//h2',
                'description': 'Product name/title (fallback 1)'
            },
            {
                'mall_name': 'Best Buy',
                'page_type': 'main',
                'data_field': 'product_name_fallback2',
                'xpath': './/div[@class="sku-block-content-title"]//h2',
                'description': 'Product name/title (fallback 2)'
            },
            {
                'mall_name': 'Best Buy',
                'page_type': 'main',
                'data_field': 'product_url',
                'xpath': './/a[@class="product-list-item-link"]/@href',
                'description': 'Product detail page URL'
            },
            {
                'mall_name': 'Best Buy',
                'page_type': 'main',
                'data_field': 'final_price',
                'xpath': './/span[contains(@class, "text-6") and contains(@class, "leading-6")]',
                'description': 'Final SKU price (primary selector)'
            },
            {
                'mall_name': 'Best Buy',
                'page_type': 'main',
                'data_field': 'final_price_fallback1',
                'xpath': './/span[@data-testid="price-block-customer-price"]//span',
                'description': 'Final SKU price (fallback 1)'
            },
            {
                'mall_name': 'Best Buy',
                'page_type': 'main',
                'data_field': 'final_price_fallback2',
                'xpath': './/div[@class="pricing"]//span[contains(@class, "font-500")]',
                'description': 'Final SKU price (fallback 2)'
            },
            {
                'mall_name': 'Best Buy',
                'page_type': 'main',
                'data_field': 'savings',
                'xpath': './/span[@data-testid="price-block-total-savings-text"]',
                'description': 'Savings amount'
            },
            {
                'mall_name': 'Best Buy',
                'page_type': 'main',
                'data_field': 'comparable_pricing',
                'xpath': './/span[@class="font-sans text-default text-style-body-md-400" and contains(@style, "color: rgb(108, 111, 117)")]',
                'description': 'Comparable pricing (primary selector)'
            },
            {
                'mall_name': 'Best Buy',
                'page_type': 'main',
                'data_field': 'comparable_pricing_fallback',
                'xpath': './/span[@data-testid="price-block-regular-price-message-text"]//span',
                'description': 'Comparable pricing (fallback)'
            },
            {
                'mall_name': 'Best Buy',
                'page_type': 'main',
                'data_field': 'offer',
                'xpath': './/div[@data-testid="plus-x-offers"]//span[@class="font-sans text-default text-style-body-md-400"]',
                'description': 'Additional offers (e.g., "+ X offers")'
            },
            {
                'mall_name': 'Best Buy',
                'page_type': 'main',
                'data_field': 'pickup_availability',
                'xpath': './/div[@class="fulfillment"]//p[contains(., "Pick up")]',
                'description': 'Store pickup availability'
            },
            {
                'mall_name': 'Best Buy',
                'page_type': 'main',
                'data_field': 'shipping_availability',
                'xpath': './/div[@class="fulfillment"]//p[contains(., "Get it") or contains(., "FREE")]',
                'description': 'Shipping availability'
            },
            {
                'mall_name': 'Best Buy',
                'page_type': 'main',
                'data_field': 'delivery_availability',
                'xpath': './/div[@class="fulfillment"]//p[contains(., "Delivery")]',
                'description': 'Delivery availability (excludes Installation)'
            },
            {
                'mall_name': 'Best Buy',
                'page_type': 'main',
                'data_field': 'star_rating',
                'xpath': './/span[@aria-hidden="true" and contains(@class, "font-weight-bold")]',
                'description': 'Customer star rating'
            },
            {
                'mall_name': 'Best Buy',
                'page_type': 'main',
                'data_field': 'sku_status',
                'xpath': './/div[@class="sponsored"]',
                'description': 'SKU status (Sponsored/Regular)'
            }
        ]

        print(f"\n[INFO] Inserting {len(bestbuy_xpaths)} Best Buy XPath selectors...")

        for xpath_data in bestbuy_xpaths:
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

            print(f"  [OK] {xpath_data['data_field']}: {xpath_data['xpath'][:60]}...")

        conn.commit()
        cursor.close()
        conn.close()

        print("\n" + "="*80)
        print(f"SUCCESS: {len(bestbuy_xpaths)} Best Buy XPath selectors inserted!")
        print("="*80)

    except Exception as e:
        print(f"\n[ERROR] {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    insert_bestbuy_xpath()
    print("\n[INFO] Script completed. Exiting...")
