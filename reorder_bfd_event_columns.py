import psycopg2
from config import DB_CONFIG

def reorder_columns():
    """Reorder columns in bfd_event_crawl table"""
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()

        print("="*80)
        print("Reordering BFD Event Table Columns")
        print("="*80)

        # Step 1: Check if table has data
        print("\n[STEP 1] Checking existing data...")
        cursor.execute("SELECT COUNT(*) FROM bfd_event_crawl")
        row_count = cursor.fetchone()[0]
        print(f"[INFO] Found {row_count} rows in current table")

        # Step 2: Create backup with data
        print("\n[STEP 2] Creating backup...")
        cursor.execute("""
            DROP TABLE IF EXISTS bfd_event_crawl_old;
            CREATE TABLE bfd_event_crawl_old AS
            SELECT * FROM bfd_event_crawl;
        """)
        print(f"[OK] Backed up {row_count} rows to bfd_event_crawl_old")

        # Step 3: Drop current table
        print("\n[STEP 3] Dropping current table...")
        cursor.execute("DROP TABLE bfd_event_crawl;")
        print("[OK] Table dropped")

        # Step 4: Create new table with correct column order
        print("\n[STEP 4] Creating table with new column order...")
        cursor.execute("""
            CREATE TABLE bfd_event_crawl (
                id SERIAL PRIMARY KEY,
                Event_channel VARCHAR(50) CHECK (Event_channel IN ('Amazon', 'Walmart', 'Bestbuy')),
                Event_name TEXT,
                Event_start_date DATE,
                Event_end_date DATE,
                crawl_at_local_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                calendar_week VARCHAR(10)
            );
        """)
        print("[OK] Table created with new column order")

        # Step 5: Restore data if any existed
        if row_count > 0:
            print("\n[STEP 5] Restoring data...")
            cursor.execute("""
                INSERT INTO bfd_event_crawl
                (Event_channel, Event_name, Event_start_date, Event_end_date, crawl_at_local_time, calendar_week)
                SELECT Event_channel, Event_name, Event_start_date, Event_end_date, crawl_at_local_time, calendar_week
                FROM bfd_event_crawl_old;
            """)
            cursor.execute("SELECT COUNT(*) FROM bfd_event_crawl")
            restored_count = cursor.fetchone()[0]
            print(f"[OK] Restored {restored_count} rows")
        else:
            print("\n[STEP 5] No data to restore")

        conn.commit()

        # Step 6: Verify new structure
        print("\n[STEP 6] Verifying new column order...")
        cursor.execute("""
            SELECT column_name, data_type, character_maximum_length
            FROM information_schema.columns
            WHERE table_name = 'bfd_event_crawl'
            ORDER BY ordinal_position;
        """)

        columns = cursor.fetchall()
        print("\nNew table structure:")
        for idx, col in enumerate(columns, 1):
            col_name, data_type, max_length = col
            if max_length:
                print(f"  {idx}. {col_name} ({data_type}({max_length}))")
            else:
                print(f"  {idx}. {col_name} ({data_type})")

        cursor.close()
        conn.close()

        print("\n" + "="*80)
        print("SUCCESS: Column reordering completed!")
        print("="*80)
        print("\nNotes:")
        print("  - Old table backed up as: bfd_event_crawl_old")
        print("  - New column order: id, Event_channel, Event_name, Event_start_date,")
        print("                      Event_end_date, crawl_at_local_time, calendar_week")

    except Exception as e:
        print(f"\n[ERROR] {e}")
        import traceback
        traceback.print_exc()
        if conn:
            conn.rollback()

if __name__ == "__main__":
    print("\n[WARNING] This will recreate the bfd_event_crawl table")
    print("[INFO] Existing data will be preserved and restored")

    response = input("\nContinue? (yes/no): ")
    if response.lower() == 'yes':
        reorder_columns()
    else:
        print("[INFO] Operation cancelled")

    print("\n[INFO] Script completed. Exiting...")
