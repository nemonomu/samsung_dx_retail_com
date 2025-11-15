"""
Best Buy TV v1 Tables Deletion Script
기존 테이블 삭제 (재생성 전)
"""
import psycopg2
from config import DB_CONFIG

def drop_tables():
    """Drop all v1 tables"""
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()
        print("[INFO] Database connected")

        tables = ['bby_tv_main1', 'bby_tv_bsr1', 'bby_tv_pmt1', 'bby_tv_crawl']

        for table in tables:
            print(f"\n[INFO] Dropping {table}...")
            cursor.execute(f"DROP TABLE IF EXISTS {table} CASCADE")
            print(f"  [OK] {table} dropped")

        conn.commit()
        cursor.close()
        conn.close()

        print("\n" + "="*80)
        print("[SUCCESS] All tables dropped successfully!")
        print("="*80)

    except Exception as e:
        print(f"\n[ERROR] Failed to drop tables: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    drop_tables()
