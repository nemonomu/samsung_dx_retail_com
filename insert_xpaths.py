import psycopg2

# Import database configuration
from config import DB_CONFIG

def insert_xpaths():
    """Insert Amazon Main Page XPath selectors into database"""
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()

        # XPath definitions for Amazon Main Page
        xpaths = [
            {
                'mall_name': 'Amazon',
                'page_type': 'main_page',
                'data_field': 'product_name',
                'xpath': './/h2//span',
                'css_selector': 'h2 span',
                'description': 'Product name/title - NO. 47',
                'updated_by': 'system'
            },
            {
                'mall_name': 'Amazon',
                'page_type': 'main_page',
                'data_field': 'purchase_history',
                'xpath': ".//span[@class='a-size-base a-color-secondary' and contains(text(), 'bought in past')]",
                'css_selector': 'span.a-size-base.a-color-secondary',
                'description': 'Purchase history (e.g., 10K+ bought in past month) - NO. 49',
                'updated_by': 'system'
            },
            {
                'mall_name': 'Amazon',
                'page_type': 'main_page',
                'data_field': 'final_price',
                'xpath': ".//span[@class='a-price' and @data-a-color='base']//span[@class='a-offscreen']",
                'css_selector': 'span.a-price[data-a-color="base"] span.a-offscreen',
                'description': 'Final selling price - NO. 50',
                'updated_by': 'system'
            },
            {
                'mall_name': 'Amazon',
                'page_type': 'main_page',
                'data_field': 'original_price',
                'xpath': ".//span[@class='a-price a-text-price' and @data-a-strike='true']//span[@class='a-offscreen']",
                'css_selector': 'span.a-price.a-text-price[data-a-strike="true"] span.a-offscreen',
                'description': 'Original price before discount - NO. 51',
                'updated_by': 'system'
            },
            {
                'mall_name': 'Amazon',
                'page_type': 'main_page',
                'data_field': 'shipping_info',
                'xpath': ".//div[@data-cy='delivery-recipe']//div[contains(@class, 'udm-primary-delivery-message')]",
                'css_selector': 'div[data-cy="delivery-recipe"] div.udm-primary-delivery-message',
                'description': 'Shipping/delivery information - NO. 52',
                'updated_by': 'system'
            },
            {
                'mall_name': 'Amazon',
                'page_type': 'main_page',
                'data_field': 'stock_availability',
                'xpath': ".//span[@class='a-size-base a-color-price' and (contains(text(), 'left in stock') or contains(text(), 'Only'))]",
                'css_selector': 'span.a-size-base.a-color-price',
                'description': 'Stock availability (e.g., Only 1 left in stock) - NO. 53',
                'updated_by': 'system'
            },
            {
                'mall_name': 'Amazon',
                'page_type': 'main_page',
                'data_field': 'deal_badge',
                'xpath': ".//span[@class='a-badge-text']",
                'css_selector': 'span.a-badge-text',
                'description': 'Deal/badge information (Limited time deal, Best Seller, etc) - NO. 54',
                'updated_by': 'system'
            },
            {
                'mall_name': 'Amazon',
                'page_type': 'main_page',
                'data_field': 'product_url',
                'xpath': './/h2/a/@href',
                'css_selector': 'h2 a',
                'description': 'Product page URL',
                'updated_by': 'system'
            },
            {
                'mall_name': 'Amazon',
                'page_type': 'main_page',
                'data_field': 'base_container',
                'xpath': "//div[@data-component-type='s-search-result']",
                'css_selector': 'div[data-component-type="s-search-result"]',
                'description': 'Base container for each product item',
                'updated_by': 'system'
            }
        ]

        # Insert xpaths
        for xpath_data in xpaths:
            cursor.execute("""
                INSERT INTO xpath_selectors
                (mall_name, page_type, data_field, xpath, css_selector, description, updated_by)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (mall_name, page_type, data_field)
                DO UPDATE SET
                    xpath = EXCLUDED.xpath,
                    css_selector = EXCLUDED.css_selector,
                    description = EXCLUDED.description,
                    updated_at = CURRENT_TIMESTAMP,
                    updated_by = EXCLUDED.updated_by;
            """, (
                xpath_data['mall_name'],
                xpath_data['page_type'],
                xpath_data['data_field'],
                xpath_data['xpath'],
                xpath_data['css_selector'],
                xpath_data['description'],
                xpath_data['updated_by']
            ))
            print(f"[OK] Inserted/Updated: {xpath_data['data_field']}")

        conn.commit()

        # Verify inserted data
        cursor.execute("""
            SELECT data_field, xpath, description
            FROM xpath_selectors
            WHERE mall_name = 'Amazon' AND page_type = 'main_page'
            ORDER BY id;
        """)

        print("\n" + "="*80)
        print("Inserted XPath Selectors:")
        print("="*80)
        for row in cursor.fetchall():
            print(f"\n{row[0]}:")
            print(f"  XPath: {row[1]}")
            print(f"  Description: {row[2]}")

        cursor.close()
        conn.close()

        print("\n" + "="*80)
        print("All XPaths successfully inserted into database!")
        print("="*80)

        return True

    except Exception as e:
        print(f"Error inserting xpaths: {e}")
        return False

if __name__ == "__main__":
    insert_xpaths()
