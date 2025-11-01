import psycopg2

# Import database configuration
from config import DB_CONFIG

def setup_bsr_configuration():
    """Setup BSR crawler configuration: add column, insert XPaths and URLs"""
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()

        print("="*80)
        print("Setting up BSR Crawler Configuration")
        print("="*80)

        # 1. Add product_url column to amazon_tv_bsr table
        print("\n[1/3] Adding product_url column to amazon_tv_bsr table...")
        cursor.execute("""
            SELECT column_name
            FROM information_schema.columns
            WHERE table_name = 'amazon_tv_bsr' AND column_name = 'product_url'
        """)

        if cursor.fetchone():
            print("[INFO] product_url column already exists, skipping...")
        else:
            cursor.execute("""
                ALTER TABLE amazon_tv_bsr
                ADD COLUMN product_url TEXT;
            """)
            print("[OK] product_url column added")

        # 2. Insert XPath selectors for BSR page
        print("\n[2/3] Inserting XPath selectors for BSR page...")

        xpaths = [
            ('base_container', '//li[@class="zg-no-numbers"]', None),
            ('rank', './/div[@class="a-section zg-bdg-body zg-bdg-clr-body aok-float-left"]/span[@class="zg-bdg-text"]', 'span.zg-bdg-text'),
            ('product_name', './/div[contains(@class, "_cDEzb_p13n-sc-css-line-clamp")]', 'div[class*="_cDEzb_p13n-sc-css-line-clamp"]'),
            ('product_url', './/a[@class="a-link-normal aok-block"]/@href', 'a.a-link-normal.aok-block')
        ]

        for data_field, xpath, css_selector in xpaths:
            cursor.execute("""
                INSERT INTO xpath_selectors
                (mall_name, page_type, data_field, xpath, css_selector, is_active)
                VALUES ('Amazon', 'bsr_page', %s, %s, %s, TRUE)
                ON CONFLICT (mall_name, page_type, data_field) DO UPDATE
                SET xpath = EXCLUDED.xpath,
                    css_selector = EXCLUDED.css_selector,
                    is_active = EXCLUDED.is_active
            """, (data_field, xpath, css_selector))

        print(f"[OK] Inserted {len(xpaths)} XPath selectors")

        # 3. Insert page 2 URL
        print("\n[3/3] Inserting BSR page URLs...")

        urls = [
            (1, 'https://www.amazon.com/Best-Sellers-Electronics-Televisions/zgbs/electronics/172659/ref=zg_bs_nav_electronics_2_1266092011'),
            (2, 'https://www.amazon.com/Best-Sellers-Electronics-Televisions/zgbs/electronics/172659/ref=zg_bs_pg_2_electronics?_encoding=UTF8&pg=2')
        ]

        for page_num, url in urls:
            cursor.execute("""
                INSERT INTO bsr_page_urls (page_number, url, is_active)
                VALUES (%s, %s, TRUE)
                ON CONFLICT (page_number) DO UPDATE
                SET url = EXCLUDED.url,
                    is_active = EXCLUDED.is_active
            """, (page_num, url))

        print(f"[OK] Inserted {len(urls)} page URLs")

        conn.commit()
        cursor.close()
        conn.close()

        print("\n" + "="*80)
        print("SUCCESS: BSR Configuration Complete!")
        print("="*80)
        print("\nConfiguration Summary:")
        print("  - product_url column added to amazon_tv_bsr")
        print("  - 4 XPath selectors added (base_container, rank, product_name, product_url)")
        print("  - 2 page URLs added (page 1 and 2)")
        print("\nReady to run: python Amazon_tv_bsr_crawl.py")

    except Exception as e:
        print(f"\n[ERROR] {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    setup_bsr_configuration()
    print("\n[INFO] Script completed. Exiting...")
