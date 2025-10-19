import psycopg2

DB_CONFIG = {
    'host': 'samsung-dx-crawl.csnixzmkuppn.ap-northeast-2.rds.amazonaws.com',
    'port': 5432,
    'database': 'postgres',
    'user': 'postgres',
    'password': 'admin2025!'
}

def insert_walmart_xpath():
    """Insert Walmart XPath selectors into xpath_selectors table"""
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()

        print("="*80)
        print("Inserting Walmart XPath Selectors")
        print("="*80)

        # Walmart XPath selectors from wmart_crawl_base.txt
        walmart_xpaths = [
            # Base container
            {
                'mall_name': 'Walmart',
                'page_type': 'main',
                'data_field': 'base_container',
                'xpath': '//div[contains(@class, "mb0 ph0-xl pt0-xl bb b--near-white w-25 pb3-m ph1")]',
                'description': 'Main product container for each TV listing'
            },
            # Retailer_SKU_Name
            {
                'mall_name': 'Walmart',
                'page_type': 'main',
                'data_field': 'product_name',
                'xpath': './/span[@data-automation-id="product-title"]',
                'description': 'Product name/title'
            },
            # Final_SKU_Price
            {
                'mall_name': 'Walmart',
                'page_type': 'main',
                'data_field': 'final_price',
                'xpath': './/div[@data-automation-id="product-price"]//div[contains(@class, "mr1 mr2-xl b black")]',
                'description': 'Final SKU price (current price)'
            },
            # Original_SKU_Price
            {
                'mall_name': 'Walmart',
                'page_type': 'main',
                'data_field': 'original_price',
                'xpath': './/div[contains(@class, "gray mr1 f6 f4-l")]//span[@class="strike"]',
                'description': 'Original SKU price (strikethrough price)'
            },
            # Offer
            {
                'mall_name': 'Walmart',
                'page_type': 'main',
                'data_field': 'offer',
                'xpath': './/span[@class="f7 normal lh-solid"]',
                'description': 'Special offers (e.g., "4 free offers from Apple")'
            },
            # Pick-Up_Availability
            {
                'mall_name': 'Walmart',
                'page_type': 'main',
                'data_field': 'pickup_availability',
                'xpath': './/div[@class="ff-text-wrapper" and contains(., "Free pickup")]//span[@class="b"]',
                'description': 'Store pickup time (e.g., "7am")'
            },
            # Shipping_Availability
            {
                'mall_name': 'Walmart',
                'page_type': 'main',
                'data_field': 'shipping_availability',
                'xpath': './/div[@class="ff-text-wrapper" and contains(., "Free shipping")]//span[@class="b"]',
                'description': 'Shipping time (e.g., "today", "tomorrow", "in 3+ days")'
            },
            # Delivery_Availability
            {
                'mall_name': 'Walmart',
                'page_type': 'main',
                'data_field': 'delivery_availability',
                'xpath': './/div[contains(., "Delivery as soon as")]//span[@class="b"]',
                'description': 'Delivery time (e.g., "6am")'
            },
            # SKU_Status - Rollback
            {
                'mall_name': 'Walmart',
                'page_type': 'main',
                'data_field': 'sku_status_rollback',
                'xpath': './/span[contains(@class, "w_yTSq w_0aYG w_MwbK") and text()="Rollback"]',
                'description': 'SKU status - Rollback badge'
            },
            # SKU_Status - Sponsored
            {
                'mall_name': 'Walmart',
                'page_type': 'main',
                'data_field': 'sku_status_sponsored',
                'xpath': './/div[text()="Sponsored"]',
                'description': 'SKU status - Sponsored badge'
            },
            # Retailer_Membership_Discounts
            {
                'mall_name': 'Walmart',
                'page_type': 'main',
                'data_field': 'membership_discount',
                'xpath': './/div[@data-testid="save-with-walmart-plus-badge"]',
                'description': 'Walmart Plus membership discount badge'
            },
            # Available_Quantity_for_Purchase
            {
                'mall_name': 'Walmart',
                'page_type': 'main',
                'data_field': 'available_quantity',
                'xpath': './/span[contains(@class, "w_yTSq") and contains(@class, "dark-red") and contains(., "Only") and contains(., "left")]',
                'description': 'Available quantity (e.g., "Only 4 left")'
            },
            # Inventory_Status
            {
                'mall_name': 'Walmart',
                'page_type': 'main',
                'data_field': 'inventory_status',
                'xpath': './/span[contains(@class, "w_yTSq") and contains(@class, "dark-red") and text()="Low stock"]',
                'description': 'Inventory status (e.g., "Low stock")'
            },
            # Product_URL
            {
                'mall_name': 'Walmart',
                'page_type': 'main',
                'data_field': 'product_url',
                'xpath': './/a[contains(@class, "w-100 h-100 z-1")]/@href',
                'description': 'Product detail page URL'
            }
        ]

        print(f"\n[INFO] Inserting {len(walmart_xpaths)} Walmart XPath selectors...")

        for xpath_data in walmart_xpaths:
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
        print(f"SUCCESS: {len(walmart_xpaths)} Walmart XPath selectors inserted!")
        print("="*80)

    except Exception as e:
        print(f"\n[ERROR] {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    insert_walmart_xpath()
    print("\n[INFO] Script completed. Exiting...")
