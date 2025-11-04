import psycopg2

DB_CONFIG = {
    'host': 'samsung-dx-crawl.csnixzmkuppn.ap-northeast-2.rds.amazonaws.com',
    'port': 5432,
    'database': 'postgres',
    'user': 'postgres',
    'password': 'admin2025!'
}

def create_bfd_event_table():
    """Create table for Black Friday event schedule"""
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()

        print("="*80)
        print("Creating BFD Event Schedule Table")
        print("="*80)

        print("\nCreating bfd_event_crawl table...")
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS bfd_event_crawl (
                id SERIAL PRIMARY KEY,
                Event_channel VARCHAR(50) CHECK (Event_channel IN ('Amazon', 'Walmart', 'Bestbuy')),
                Event_name TEXT,
                Event_start_date DATE,
                Event_end_date DATE,
                crawl_at_local_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                calendar_week VARCHAR(10)
            );
        """)
        print("[OK] bfd_event_crawl table created")

        conn.commit()
        cursor.close()
        conn.close()

        print("\n" + "="*80)
        print("SUCCESS: BFD Event table created!")
        print("="*80)
        print("\nTable structure:")
        print("  - id (SERIAL PRIMARY KEY)")
        print("  - Event_channel (VARCHAR(50)) - only: Amazon, Walmart, Bestbuy")
        print("  - Event_name (TEXT)")
        print("  - Event_start_date (DATE)")
        print("  - Event_end_date (DATE)")
        print("  - crawl_at_local_time (TIMESTAMP)")
        print("  - calendar_week (VARCHAR(10))")
        print("\nReady to run: python bfd_event_crawl.py")

    except Exception as e:
        print(f"\n[ERROR] {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    create_bfd_event_table()
    print("\n[INFO] Script completed. Exiting...")
