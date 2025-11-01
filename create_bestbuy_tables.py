import psycopg2

# Import database configuration
from config import DB_CONFIG

def create_bestbuy_tables():
    """Create tables for Best Buy TV main page crawler"""
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()

        print("="*80)
        print("Creating Best Buy TV Main Crawler Tables")
        print("="*80)

        # 1. Create bestbuy_tv_main_crawl table
        print("\n[1/2] Creating bestbuy_tv_main_crawl table...")
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS bestbuy_tv_main_crawl (
                id SERIAL PRIMARY KEY,
                page_type VARCHAR(50),
                Retailer_SKU_Name TEXT,
                Final_SKU_Price VARCHAR(50),
                Savings VARCHAR(50),
                Comparable_Pricing VARCHAR(50),
                Offer TEXT,
                Pick_Up_Availability TEXT,
                Shipping_Availability TEXT,
                Delivery_Availability TEXT,
                Star_Rating VARCHAR(10),
                SKU_Status VARCHAR(50),
                Product_url TEXT,
                crawl_at_local_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """)
        print("[OK] bestbuy_tv_main_crawl table created")

        # 2. Create bby_page_url table
        print("\n[2/2] Creating bby_page_url table...")
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS bby_page_url (
                id SERIAL PRIMARY KEY,
                page_number INTEGER NOT NULL,
                url TEXT NOT NULL,
                is_active BOOLEAN DEFAULT TRUE,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(page_number)
            );
        """)
        print("[OK] bby_page_url table created")

        # Insert page URLs (1-13 pages)
        print("\n[INFO] Inserting page URLs...")

        # Page 1 (main page)
        cursor.execute("""
            INSERT INTO bby_page_url (page_number, url)
            VALUES (1, 'https://www.bestbuy.com/site/searchpage.jsp?id=pcat17071&st=tv')
            ON CONFLICT (page_number) DO NOTHING
        """)

        # Pages 2-13
        for page_num in range(2, 14):
            url = f'https://www.bestbuy.com/site/searchpage.jsp?cp={page_num}&id=pcat17071&st=tv'
            cursor.execute("""
                INSERT INTO bby_page_url (page_number, url)
                VALUES (%s, %s)
                ON CONFLICT (page_number) DO NOTHING
            """, (page_num, url))

        print(f"[OK] Inserted 13 page URLs")

        conn.commit()
        cursor.close()
        conn.close()

        print("\n" + "="*80)
        print("SUCCESS: Best Buy tables created!")
        print("="*80)
        print("\nTables created:")
        print("  1. bestbuy_tv_main_crawl - Stores product data")
        print("  2. bby_page_url - Stores page URLs (13 pages)")
        print("\nReady to run: python bestbuy_tv_main_crawl.py")

    except Exception as e:
        print(f"\n[ERROR] {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    create_bestbuy_tables()
    print("\n[INFO] Script completed. Exiting...")
