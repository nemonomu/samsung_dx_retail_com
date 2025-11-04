"""
Best Buy Tables Migration Script

변경사항:
1. bestbuy_tv_main_crawl: Retailer_SKU_Name → item
2. bby_tv_promotion_crawl: Retailer_SKU_Name → item, 4개 컬럼 추가, 순서 변경
3. bby_tv_detail_crawled: Retailer_SKU_Name → item, screen_size 컬럼 추가
"""
import psycopg2
from config import DB_CONFIG

def migrate_bestbuy_tables():
    """Migrate 3 Bestbuy tables"""
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()

        print("="*80)
        print("Best Buy Tables Migration")
        print("="*80)

        # ===================================================================
        # 1. bestbuy_tv_main_crawl - 컬럼명만 변경
        # ===================================================================
        print("\n[TABLE 1/3] bestbuy_tv_main_crawl")
        print("-"*80)

        # Check if column exists
        cursor.execute("""
            SELECT column_name
            FROM information_schema.columns
            WHERE table_name = 'bestbuy_tv_main_crawl'
            AND column_name = 'retailer_sku_name'
        """)

        if cursor.fetchone():
            print("[STEP 1] Renaming Retailer_SKU_Name to item...")
            cursor.execute("""
                ALTER TABLE bestbuy_tv_main_crawl
                RENAME COLUMN Retailer_SKU_Name TO item
            """)
            print("[OK] Column renamed: Retailer_SKU_Name → item")
        else:
            print("[SKIP] Column already renamed or doesn't exist")

        # ===================================================================
        # 2. bby_tv_detail_crawled - 컬럼명 변경 + screen_size 추가
        # ===================================================================
        print("\n[TABLE 2/3] bby_tv_detail_crawled")
        print("-"*80)

        # Check if Retailer_SKU_Name exists
        cursor.execute("""
            SELECT column_name
            FROM information_schema.columns
            WHERE table_name = 'bby_tv_detail_crawled'
            AND column_name = 'retailer_sku_name'
        """)

        if cursor.fetchone():
            print("[STEP 1] Renaming Retailer_SKU_Name to item...")
            cursor.execute("""
                ALTER TABLE bby_tv_detail_crawled
                RENAME COLUMN Retailer_SKU_Name TO item
            """)
            print("[OK] Column renamed: Retailer_SKU_Name → item")
        else:
            print("[SKIP] Column already renamed or doesn't exist")

        # Add screen_size column
        print("[STEP 2] Adding screen_size column...")
        cursor.execute("""
            ALTER TABLE bby_tv_detail_crawled
            ADD COLUMN IF NOT EXISTS screen_size TEXT
        """)
        print("[OK] Column added: screen_size")

        # ===================================================================
        # 3. bby_tv_promotion_crawl - 재생성 (컬럼 순서 변경 필요)
        # ===================================================================
        print("\n[TABLE 3/3] bby_tv_promotion_crawl")
        print("-"*80)

        # Check current row count
        cursor.execute("SELECT COUNT(*) FROM bby_tv_promotion_crawl")
        row_count = cursor.fetchone()[0]
        print(f"[INFO] Current rows: {row_count}")

        if row_count > 0:
            # Backup existing data
            print("[STEP 1] Creating backup...")
            cursor.execute("""
                DROP TABLE IF EXISTS bby_tv_promotion_crawl_backup;
                CREATE TABLE bby_tv_promotion_crawl_backup AS
                SELECT * FROM bby_tv_promotion_crawl;
            """)
            print(f"[OK] Backed up {row_count} rows to bby_tv_promotion_crawl_backup")

        # Drop and recreate table with new structure
        print("[STEP 2] Recreating table with new structure...")
        cursor.execute("DROP TABLE bby_tv_promotion_crawl")

        cursor.execute("""
            CREATE TABLE bby_tv_promotion_crawl (
                id SERIAL PRIMARY KEY,
                page_type VARCHAR(50),
                item TEXT,
                rank INTEGER,
                final_sku_price VARCHAR(50),
                original_sku_price VARCHAR(50),
                offer VARCHAR(50),
                savings VARCHAR(50),
                promotion_type TEXT,
                product_url TEXT,
                crawl_datetime TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                calendar_week VARCHAR(10),
                batch_id VARCHAR(50)
            )
        """)
        print("[OK] Table recreated with new column order")

        # Migrate old data if exists
        if row_count > 0:
            print("[STEP 3] Migrating data from backup...")

            # Check if backup has Retailer_SKU_Name or item
            cursor.execute("""
                SELECT column_name
                FROM information_schema.columns
                WHERE table_name = 'bby_tv_promotion_crawl_backup'
            """)
            backup_columns = [row[0] for row in cursor.fetchall()]

            # Determine the item column name in backup
            item_column = 'item' if 'item' in backup_columns else 'retailer_sku_name'

            cursor.execute(f"""
                INSERT INTO bby_tv_promotion_crawl
                (page_type, item, rank, promotion_type, product_url, crawl_datetime, calendar_week, batch_id)
                SELECT
                    page_type,
                    {item_column},
                    rank,
                    promotion_type,
                    product_url,
                    crawl_datetime,
                    calendar_week,
                    batch_id
                FROM bby_tv_promotion_crawl_backup
            """)

            cursor.execute("SELECT COUNT(*) FROM bby_tv_promotion_crawl")
            migrated_count = cursor.fetchone()[0]
            print(f"[OK] Migrated {migrated_count} rows (new columns will be NULL)")
            print("[INFO] New columns (final_sku_price, original_sku_price, offer, savings) need to be populated by new crawler")

        conn.commit()

        # ===================================================================
        # Verification
        # ===================================================================
        print("\n" + "="*80)
        print("VERIFICATION")
        print("="*80)

        tables = [
            ('bestbuy_tv_main_crawl', ['item']),
            ('bby_tv_detail_crawled', ['item', 'screen_size']),
            ('bby_tv_promotion_crawl', ['item', 'final_sku_price', 'original_sku_price', 'offer', 'savings'])
        ]

        for table_name, check_columns in tables:
            print(f"\n{table_name}:")
            cursor.execute(f"""
                SELECT column_name, data_type
                FROM information_schema.columns
                WHERE table_name = '{table_name}'
                ORDER BY ordinal_position
            """)
            columns = cursor.fetchall()
            for col_name, col_type in columns:
                marker = " ✓" if col_name in check_columns else ""
                print(f"  - {col_name} ({col_type}){marker}")

        cursor.close()
        conn.close()

        print("\n" + "="*80)
        print("SUCCESS: Migration completed!")
        print("="*80)
        print("\nNotes:")
        print("  1. bestbuy_tv_main_crawl: Retailer_SKU_Name → item")
        print("  2. bby_tv_detail_crawled: Retailer_SKU_Name → item, screen_size added")
        print("  3. bby_tv_promotion_crawl: Recreated with new columns and order")
        print("\nOld data:")
        print("  - bby_tv_promotion_crawl_backup: Contains old data")
        print("\nReady to run:")
        print("  - python bby_tv_main.py")
        print("  - python bby_tv_dt.py")
        print("  - python bby_tv_pmt.py")

    except Exception as e:
        print(f"\n[ERROR] {e}")
        import traceback
        traceback.print_exc()
        if conn:
            conn.rollback()

if __name__ == "__main__":
    print("\n[WARNING] This will modify Best Buy table structures")
    print("[INFO] Data will be backed up before migration")

    response = input("\nContinue? (yes/no): ")
    if response.lower() == 'yes':
        migrate_bestbuy_tables()
    else:
        print("[INFO] Migration cancelled")

    print("\n[INFO] Script completed. Exiting...")
