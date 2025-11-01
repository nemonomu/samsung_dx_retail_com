import psycopg2

# Import database configuration
from config import DB_CONFIG

def insert_bfd_xpath():
    """Insert BFD (BlackFriday.com) XPath selectors into xpath_selectors table"""
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()

        print("="*80)
        print("Inserting BFD XPath Selectors")
        print("="*80)

        # BFD XPath selectors from bfd_event_crawl.py
        bfd_xpaths = [
            # Main page (blackfriday.com)
            {
                'mall_name': 'BlackFriday.com',
                'page_type': 'main',
                'data_field': 'retailer_container',
                'xpath': '//div[@class="flex flex-wrap text-left justify-center"]',
                'description': 'Main container for retailer cards on homepage'
            },
            {
                'mall_name': 'BlackFriday.com',
                'page_type': 'main',
                'data_field': 'retailer_name',
                'xpath': './/span[@class="font-bold"]',
                'description': 'Retailer name within container'
            },
            {
                'mall_name': 'BlackFriday.com',
                'page_type': 'main',
                'data_field': 'retailer_url',
                'xpath': './/a[@class="block relative h-60 md:h-63 lg:h-66"]/@href',
                'description': 'Retailer detail page URL'
            },

            # Retailer event page
            {
                'mall_name': 'BlackFriday.com',
                'page_type': 'event',
                'data_field': 'event_container',
                'xpath': '//li[contains(@class, "ad-scan-nav-slide")]',
                'description': 'Event container for each promotion/sale'
            },
            {
                'mall_name': 'BlackFriday.com',
                'page_type': 'event',
                'data_field': 'event_name',
                'xpath': './/span[@class="text-xs leading-1 my-px font-bold line-clamp-2 overflow-hidden"]',
                'description': 'Event/promotion name'
            },
            {
                'mall_name': 'BlackFriday.com',
                'page_type': 'event',
                'data_field': 'event_dates',
                'xpath': './/time/@datetime',
                'description': 'Event start and end datetime attributes'
            }
        ]

        print(f"\n[INFO] Inserting {len(bfd_xpaths)} BFD XPath selectors...")

        for xpath_data in bfd_xpaths:
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

            print(f"  [OK] {xpath_data['page_type']}/{xpath_data['data_field']}: {xpath_data['xpath'][:60]}...")

        conn.commit()
        cursor.close()
        conn.close()

        print("\n" + "="*80)
        print(f"SUCCESS: {len(bfd_xpaths)} BFD XPath selectors inserted!")
        print("="*80)

    except Exception as e:
        print(f"\n[ERROR] {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    insert_bfd_xpath()
    print("\n[INFO] Script completed. Exiting...")
