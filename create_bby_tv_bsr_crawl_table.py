"""
bby_tv_bsr_crawl 테이블 생성
bestbuy_tv_main_crawl과 동일한 구조
"""
import psycopg2
from config import DB_CONFIG

def create_bby_tv_bsr_crawl_table():
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        conn.autocommit = False
        cursor = conn.cursor()

        print("="*80)
        print("bby_tv_bsr_crawl 테이블 생성")
        print("="*80)

        # 테이블 생성
        print("\n[1] 테이블 생성 중...")
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS bby_tv_bsr_crawl (
                id SERIAL PRIMARY KEY,
                account_name VARCHAR(50),
                page_type VARCHAR(50),
                retailer_sku_name TEXT,
                final_sku_price VARCHAR(50),
                savings VARCHAR(50),
                comparable_pricing VARCHAR(50),
                offer TEXT,
                pick_up_availability TEXT,
                shipping_availability TEXT,
                delivery_availability TEXT,
                star_rating VARCHAR(50),
                sku_status VARCHAR(50),
                product_url TEXT,
                batch_id VARCHAR(50),
                calendar_week VARCHAR(10),
                crawl_strdatetime VARCHAR(20)
            )
        """)
        print("  [OK] bby_tv_bsr_crawl 테이블 생성 완료")

        # 커밋
        conn.commit()
        print("\n" + "="*80)
        print("[SUCCESS] 테이블 생성 완료!")
        print("="*80)

        # 결과 확인
        print("\n[결과 확인]")
        cursor.execute("""
            SELECT column_name, data_type
            FROM information_schema.columns
            WHERE table_name = 'bby_tv_bsr_crawl'
            ORDER BY ordinal_position
        """)

        columns = cursor.fetchall()
        print("\nbby_tv_bsr_crawl 테이블 컬럼:")
        for col_name, col_type in columns:
            print(f"  {col_name}: {col_type}")

        cursor.close()
        conn.close()

    except Exception as e:
        print(f"\n[ERROR] 테이블 생성 실패: {e}")
        if conn:
            conn.rollback()
            conn.close()
        import traceback
        traceback.print_exc()
        raise

if __name__ == "__main__":
    create_bby_tv_bsr_crawl_table()
