import psycopg2

# Import database configuration
from config import DB_CONFIG

def add_batch_id_column():
    """Add batch_id column to raw_data and amazon_tv_bsr tables"""
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        conn.autocommit = True
        cursor = conn.cursor()

        print("="*80)
        print("Adding batch_id Column to Tables")
        print("="*80)

        # Add batch_id to raw_data
        print("\n[1/2] Adding batch_id to raw_data table...")
        try:
            cursor.execute("""
                ALTER TABLE raw_data
                ADD COLUMN IF NOT EXISTS batch_id VARCHAR(20)
            """)
            print("  [OK] batch_id column added to raw_data")
        except Exception as e:
            print(f"  [INFO] Column may already exist: {e}")

        # Add batch_id to amazon_tv_bsr
        print("\n[2/2] Adding batch_id to amazon_tv_bsr table...")
        try:
            cursor.execute("""
                ALTER TABLE amazon_tv_bsr
                ADD COLUMN IF NOT EXISTS batch_id VARCHAR(20)
            """)
            print("  [OK] batch_id column added to amazon_tv_bsr")
        except Exception as e:
            print(f"  [INFO] Column may already exist: {e}")

        # Verify columns were added
        print("\n[VERIFY] Checking raw_data columns...")
        cursor.execute("""
            SELECT column_name
            FROM information_schema.columns
            WHERE table_name = 'raw_data' AND column_name = 'batch_id'
        """)
        if cursor.fetchone():
            print("  [OK] batch_id exists in raw_data")
        else:
            print("  [ERROR] batch_id NOT found in raw_data")

        print("\n[VERIFY] Checking amazon_tv_bsr columns...")
        cursor.execute("""
            SELECT column_name
            FROM information_schema.columns
            WHERE table_name = 'amazon_tv_bsr' AND column_name = 'batch_id'
        """)
        if cursor.fetchone():
            print("  [OK] batch_id exists in amazon_tv_bsr")
        else:
            print("  [ERROR] batch_id NOT found in amazon_tv_bsr")

        cursor.close()
        conn.close()

        print("\n" + "="*80)
        print("SUCCESS: batch_id columns added!")
        print("="*80)
        print("\n[INFO] Now main/bsr crawlers will track batch_id for each session")

    except Exception as e:
        print(f"\n[ERROR] {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    add_batch_id_column()
    print("\n[INFO] Script completed. Exiting...")
