import psycopg2

# Import database configuration
from config import DB_CONFIG

def add_order_column():
    """Add 'order' column after 'id' in both ununique tables"""
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()

        print("="*80)
        print("Adding 'order' column to ununique tables")
        print("="*80)

        # Check if order column already exists in raw_data_ununique
        cursor.execute("""
            SELECT column_name
            FROM information_schema.columns
            WHERE table_name = 'raw_data_ununique' AND column_name = 'order'
        """)

        if cursor.fetchone():
            print("\n[INFO] 'order' column already exists in raw_data_ununique, skipping...")
        else:
            cursor.execute("""
                ALTER TABLE raw_data_ununique
                ADD COLUMN "order" INTEGER;
            """)
            print("\n[OK] 'order' column added to raw_data_ununique table!")

        # Check if order column already exists in Amazon_tv_main_crawled_ununique
        cursor.execute("""
            SELECT column_name
            FROM information_schema.columns
            WHERE table_name = 'amazon_tv_main_crawled_ununique' AND column_name = 'order'
        """)

        if cursor.fetchone():
            print("[INFO] 'order' column already exists in amazon_tv_main_crawled_ununique, skipping...")
        else:
            cursor.execute("""
                ALTER TABLE amazon_tv_main_crawled_ununique
                ADD COLUMN "order" INTEGER;
            """)
            print("[OK] 'order' column added to amazon_tv_main_crawled_ununique table!")

        conn.commit()
        cursor.close()
        conn.close()

        print("\n" + "="*80)
        print("SUCCESS: 'order' column added to both tables!")
        print("="*80)
        print("\nNote: 'order' column will store collection sequence (1-300) per run")

    except Exception as e:
        print(f"\n[ERROR] {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    add_order_column()
    print("\n[INFO] Script completed. Exiting...")
