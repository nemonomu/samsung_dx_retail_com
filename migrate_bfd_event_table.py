import psycopg2
from config import DB_CONFIG

def migrate_bfd_event_table():
    """Migrate bfd_event_crawl table to new schema"""
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()

        print("="*80)
        print("Migrating BFD Event Table Schema")
        print("="*80)

        # Step 1: Backup existing data (optional - create backup table)
        print("\n[STEP 1] Creating backup table...")
        cursor.execute("""
            DROP TABLE IF EXISTS bfd_event_crawl_backup;
            CREATE TABLE bfd_event_crawl_backup AS
            SELECT * FROM bfd_event_crawl;
        """)
        cursor.execute("SELECT COUNT(*) FROM bfd_event_crawl_backup")
        backup_count = cursor.fetchone()[0]
        print(f"[OK] Backed up {backup_count} rows to bfd_event_crawl_backup")

        # Step 2: Drop old columns
        print("\n[STEP 2] Dropping old columns...")
        cursor.execute("""
            ALTER TABLE bfd_event_crawl
            DROP COLUMN IF EXISTS Bestbuy_event_schedule,
            DROP COLUMN IF EXISTS Walmart_event_schedule,
            DROP COLUMN IF EXISTS Amazon_event_schedule;
        """)
        print("[OK] Old columns dropped")

        # Step 3: Add new columns
        print("\n[STEP 3] Adding new columns...")
        cursor.execute("""
            ALTER TABLE bfd_event_crawl
            ADD COLUMN IF NOT EXISTS Event_channel VARCHAR(50),
            ADD COLUMN IF NOT EXISTS Event_name TEXT,
            ADD COLUMN IF NOT EXISTS Event_start_date DATE,
            ADD COLUMN IF NOT EXISTS Event_end_date DATE,
            ADD COLUMN IF NOT EXISTS calendar_week VARCHAR(10);
        """)
        print("[OK] New columns added")

        # Step 4: Add CHECK constraint
        print("\n[STEP 4] Adding CHECK constraint...")
        cursor.execute("""
            ALTER TABLE bfd_event_crawl
            DROP CONSTRAINT IF EXISTS bfd_event_crawl_event_channel_check;

            ALTER TABLE bfd_event_crawl
            ADD CONSTRAINT bfd_event_crawl_event_channel_check
            CHECK (Event_channel IN ('Amazon', 'Walmart', 'Bestbuy'));
        """)
        print("[OK] CHECK constraint added")

        # Step 5: Migrate data from backup (if needed)
        print("\n[STEP 5] Checking if data migration is needed...")
        cursor.execute("SELECT COUNT(*) FROM bfd_event_crawl_backup")
        backup_count = cursor.fetchone()[0]

        if backup_count > 0:
            print(f"[INFO] Found {backup_count} rows in backup")
            print("[WARNING] Old data format cannot be automatically migrated to new schema")
            print("[INFO] Old data preserved in bfd_event_crawl_backup table")
            print("[INFO] New crawls will use the new schema")

            # Clear main table since old format is incompatible
            cursor.execute("TRUNCATE TABLE bfd_event_crawl RESTART IDENTITY;")
            print("[OK] Main table cleared, ready for new format data")
        else:
            print("[INFO] No data to migrate")

        conn.commit()

        # Verify new schema
        print("\n[STEP 6] Verifying new schema...")
        cursor.execute("""
            SELECT column_name, data_type, character_maximum_length
            FROM information_schema.columns
            WHERE table_name = 'bfd_event_crawl'
            ORDER BY ordinal_position;
        """)

        columns = cursor.fetchall()
        print("\nNew table structure:")
        for col in columns:
            col_name, data_type, max_length = col
            if max_length:
                print(f"  - {col_name} ({data_type}({max_length}))")
            else:
                print(f"  - {col_name} ({data_type})")

        cursor.close()
        conn.close()

        print("\n" + "="*80)
        print("SUCCESS: Migration completed!")
        print("="*80)
        print("\nNotes:")
        print("  - Old data backed up in: bfd_event_crawl_backup")
        print("  - Main table ready for new schema")
        print("  - Run: python bfd_event_crawl.py to collect new data")

    except Exception as e:
        print(f"\n[ERROR] {e}")
        import traceback
        traceback.print_exc()
        if conn:
            conn.rollback()

if __name__ == "__main__":
    print("\n[WARNING] This will modify the bfd_event_crawl table structure")
    print("[INFO] Old data will be backed up to bfd_event_crawl_backup")

    response = input("\nContinue? (yes/no): ")
    if response.lower() == 'yes':
        migrate_bfd_event_table()
    else:
        print("[INFO] Migration cancelled")

    print("\n[INFO] Script completed. Exiting...")
