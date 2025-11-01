import psycopg2

# Import database configuration
from config import DB_CONFIG

def create_bsr_tables():
    """Create tables for BSR (Best Sellers Rank) crawling"""
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()

        print("="*80)
        print("Creating BSR Tables")
        print("="*80)

        # 1. Create amazon_tv_bsr table
        print("\n[1/2] Creating amazon_tv_bsr table...")
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS amazon_tv_bsr (
                id SERIAL PRIMARY KEY,
                Rank INTEGER,
                Retailer_SKU_Name TEXT,
                crawl_at_local_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """)
        print("[OK] amazon_tv_bsr table created")

        # 2. Create bsr_page_urls table
        print("\n[2/2] Creating bsr_page_urls table...")
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS bsr_page_urls (
                id SERIAL PRIMARY KEY,
                page_number INTEGER NOT NULL,
                url TEXT NOT NULL,
                is_active BOOLEAN DEFAULT TRUE,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(page_number)
            );
        """)
        print("[OK] bsr_page_urls table created")

        # Insert initial URL
        print("\n[INFO] Inserting initial BSR page URL...")
        cursor.execute("""
            INSERT INTO bsr_page_urls (page_number, url)
            VALUES (1, 'https://www.amazon.com/Best-Sellers-Electronics-Televisions/zgbs/electronics/172659/ref=zg_bs_nav_electronics_2_1266092011')
            ON CONFLICT (page_number) DO NOTHING
        """)

        if cursor.rowcount > 0:
            print("[OK] Initial URL inserted (page 1)")
        else:
            print("[INFO] Page 1 URL already exists, skipping")

        conn.commit()
        cursor.close()
        conn.close()

        print("\n" + "="*80)
        print("SUCCESS: BSR tables created!")
        print("="*80)
        print("\nTables created:")
        print("  1. amazon_tv_bsr - Stores BSR ranking data")
        print("  2. bsr_page_urls - Stores BSR page URLs")
        print("\nNext steps:")
        print("  1. Add page 2 URL to bsr_page_urls table")
        print("  2. Add XPath selectors to xpath_selectors table (page_type='bsr_page')")
        print("  3. Run Amazon_tv_bsr_crawl.py")

    except Exception as e:
        print(f"\n[ERROR] {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    create_bsr_tables()
    print("\n[INFO] Script completed. Exiting...")
