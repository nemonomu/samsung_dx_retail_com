import psycopg2

DB_CONFIG = {
    'host': 'samsung-dx-crawl.csnixzmkuppn.ap-northeast-2.rds.amazonaws.com',
    'port': 5432,
    'database': 'postgres',
    'user': 'postgres',
    'password': 'admin2025!'
}

def remove_unique_constraints():
    """Remove unique constraints from raw_data and Amazon_tv_main_crawled tables"""
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        conn.autocommit = True
        cursor = conn.cursor()

        print("="*80)
        print("Removing Unique Constraints")
        print("="*80)

        # Find and drop constraint on raw_data table
        print("\n[1/2] Checking raw_data table...")
        cursor.execute("""
            SELECT constraint_name
            FROM information_schema.table_constraints
            WHERE table_name = 'raw_data'
            AND constraint_type = 'UNIQUE'
        """)

        constraints = cursor.fetchall()
        if constraints:
            for constraint in constraints:
                constraint_name = constraint[0]
                print(f"  Found constraint: {constraint_name}")
                cursor.execute(f"ALTER TABLE raw_data DROP CONSTRAINT {constraint_name}")
                print(f"  [OK] Dropped constraint: {constraint_name}")
        else:
            print("  [INFO] No unique constraints found on raw_data")

        # Find and drop constraint on Amazon_tv_main_crawled table
        print("\n[2/2] Checking Amazon_tv_main_crawled table...")
        cursor.execute("""
            SELECT constraint_name
            FROM information_schema.table_constraints
            WHERE table_name = 'amazon_tv_main_crawled'
            AND constraint_type = 'UNIQUE'
        """)

        constraints = cursor.fetchall()
        if constraints:
            for constraint in constraints:
                constraint_name = constraint[0]
                print(f"  Found constraint: {constraint_name}")
                cursor.execute(f"ALTER TABLE Amazon_tv_main_crawled DROP CONSTRAINT {constraint_name}")
                print(f"  [OK] Dropped constraint: {constraint_name}")
        else:
            print("  [INFO] No unique constraints found on Amazon_tv_main_crawled")

        cursor.close()
        conn.close()

        print("\n" + "="*80)
        print("SUCCESS: Unique constraints removed!")
        print("="*80)
        print("\n[INFO] Now amazon_tv_main_crawl.py will insert new records every session")

    except Exception as e:
        print(f"\n[ERROR] {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    remove_unique_constraints()
    print("\n[INFO] Script completed. Exiting...")
