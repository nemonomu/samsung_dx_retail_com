import psycopg2

# Import database configuration
from config import DB_CONFIG

def add_asin_column():
    """Add ASIN column to raw_data_ununique table after Discount_Type"""
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()

        print("="*80)
        print("Adding ASIN column to raw_data_ununique table")
        print("="*80)

        # Check if ASIN column already exists
        cursor.execute("""
            SELECT column_name
            FROM information_schema.columns
            WHERE table_name = 'raw_data_ununique' AND column_name = 'asin'
        """)

        if cursor.fetchone():
            print("\n[INFO] ASIN column already exists, skipping...")
        else:
            # Add ASIN column (PostgreSQL doesn't support AFTER, column will be added at end)
            cursor.execute("""
                ALTER TABLE raw_data_ununique
                ADD COLUMN ASIN VARCHAR(50);
            """)
            print("\n[OK] ASIN column added successfully!")

        # Also add to Amazon_tv_main_crawled_ununique table
        cursor.execute("""
            SELECT column_name
            FROM information_schema.columns
            WHERE table_name = 'amazon_tv_main_crawled_ununique' AND column_name = 'asin'
        """)

        if cursor.fetchone():
            print("[INFO] ASIN column already exists in amazon_tv_main_crawled_ununique, skipping...")
        else:
            cursor.execute("""
                ALTER TABLE amazon_tv_main_crawled_ununique
                ADD COLUMN ASIN VARCHAR(50);
            """)
            print("[OK] ASIN column added to amazon_tv_main_crawled_ununique table!")

        conn.commit()
        cursor.close()
        conn.close()

        print("\n" + "="*80)
        print("SUCCESS: ASIN column added to both tables!")
        print("="*80)

    except Exception as e:
        print(f"\n[ERROR] {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    add_asin_column()
    print("\n[INFO] Script completed. Exiting...")
