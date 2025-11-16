"""
Fix Walmart table batch_id column type from INTEGER to VARCHAR

Changes:
- wmart_tv_main_1: batch_id INTEGER -> VARCHAR(50)
- wmart_tv_main_2: batch_id INTEGER -> VARCHAR(50)
- wmart_tv_main_crawl: batch_id INTEGER -> VARCHAR(50)
- wmart_tv_bsr_crawl: batch_id INTEGER -> VARCHAR(50)
"""
import psycopg2
from config import DB_CONFIG

def fix_batch_id_schema():
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()

        walmart_tables = [
            'wmart_tv_main_1',
            'wmart_tv_main_2',
            'wmart_tv_main_crawl',
            'wmart_tv_bsr_crawl'
        ]

        print("="*80)
        print("Fixing Walmart batch_id Column Type: INTEGER -> VARCHAR(50)")
        print("="*80)

        for table in walmart_tables:
            print(f"\n[{table}]")

            # Check current type
            cursor.execute(f"""
                SELECT data_type
                FROM information_schema.columns
                WHERE table_name = '{table}' AND column_name = 'batch_id'
            """)

            current_type = cursor.fetchone()
            if current_type:
                print(f"  Current type: {current_type[0]}")

                if current_type[0] == 'integer':
                    # Alter column type
                    cursor.execute(f"""
                        ALTER TABLE {table}
                        ALTER COLUMN batch_id TYPE VARCHAR(50)
                        USING batch_id::VARCHAR
                    """)

                    conn.commit()
                    print(f"  [OK] Changed to VARCHAR(50)")
                else:
                    print(f"  [SKIP] Already VARCHAR")
            else:
                print(f"  [WARNING] batch_id column not found")

        print("\n" + "="*80)
        print("Verification:")
        print("="*80)

        cursor.execute("""
            SELECT table_name, data_type
            FROM information_schema.columns
            WHERE table_name LIKE 'wmart%' AND column_name = 'batch_id'
            ORDER BY table_name
        """)

        for row in cursor.fetchall():
            print(f"  {row[0]}: {row[1]}")

        cursor.close()
        conn.close()

        print("\n[SUCCESS] Migration completed")

    except Exception as e:
        print(f"\n[ERROR] Migration failed: {e}")
        import traceback
        traceback.print_exc()
        if conn:
            conn.rollback()
            conn.close()

if __name__ == "__main__":
    fix_batch_id_schema()
