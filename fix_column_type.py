import psycopg2

# Database configuration
DB_CONFIG = {
    'host': 'samsung-dx-crawl.csnixzmkuppn.ap-northeast-2.rds.amazonaws.com',
    'port': 5432,
    'database': 'postgres',
    'user': 'postgres',
    'password': 'admin2025!'
}

def fix_column_types():
    """Fix Count_of_Star_Ratings column type from integer to TEXT"""
    try:
        # Connect to database
        conn = psycopg2.connect(**DB_CONFIG)
        conn.autocommit = True
        cursor = conn.cursor()

        print("=" * 80)
        print("Fixing Column Types for Amazon_tv_detail_crawled")
        print("=" * 80)

        # Check ALL column types first
        print("\n[1] Checking ALL columns in amazon_tv_detail_crawled...")
        cursor.execute("""
            SELECT column_name, data_type, character_maximum_length
            FROM information_schema.columns
            WHERE table_name = 'amazon_tv_detail_crawled'
            ORDER BY ordinal_position
        """)

        print("\nAll columns:")
        for row in cursor.fetchall():
            print(f"  - {row[0]}: {row[1]} {f'({row[2]})' if row[2] else ''}")

        # Check current column types
        print("\n[2] Checking target column types...")
        cursor.execute("""
            SELECT column_name, data_type, character_maximum_length
            FROM information_schema.columns
            WHERE table_name = 'amazon_tv_detail_crawled'
            AND column_name ILIKE '%star%rating%'
            ORDER BY column_name
        """)

        print("\nTarget columns:")
        for row in cursor.fetchall():
            print(f"  - {row[0]}: {row[1]} {f'({row[2]})' if row[2] else ''}")

        # Alter count_of_star_ratings to TEXT
        print("\n[3] Altering count_of_star_ratings from integer to TEXT...")
        cursor.execute("""
            ALTER TABLE amazon_tv_detail_crawled
            ALTER COLUMN count_of_star_ratings TYPE TEXT
        """)
        print("  [OK] count_of_star_ratings changed to TEXT")

        # Verify changes
        print("\n[4] Verifying changes...")
        cursor.execute("""
            SELECT column_name, data_type, character_maximum_length
            FROM information_schema.columns
            WHERE table_name = 'amazon_tv_detail_crawled'
            AND column_name = 'count_of_star_ratings'
        """)

        print("\nUpdated schema:")
        for row in cursor.fetchall():
            print(f"  - {row[0]}: {row[1]} {f'({row[2]})' if row[2] else ''}")

        cursor.close()
        conn.close()

        print("\n" + "=" * 80)
        print("Column type fix completed successfully!")
        print("=" * 80)

    except Exception as e:
        print(f"\n[ERROR] Failed to fix column types: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    fix_column_types()
