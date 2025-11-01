import psycopg2

# Import database configuration
from config import DB_CONFIG

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
                Bestbuy_event_schedule TEXT,
                Walmart_event_schedule TEXT,
                Amazon_event_schedule TEXT,
                crawl_at_local_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
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
        print("  - Bestbuy_event_schedule (TEXT)")
        print("  - Walmart_event_schedule (TEXT)")
        print("  - Amazon_event_schedule (TEXT)")
        print("  - crawl_at_local_time (TIMESTAMP)")
        print("\nReady to run: python bfd_event_crawl.py")

    except Exception as e:
        print(f"\n[ERROR] {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    create_bfd_event_table()
    print("\n[INFO] Script completed. Exiting...")
