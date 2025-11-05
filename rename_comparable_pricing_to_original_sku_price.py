"""
comparable_pricing 컬럼을 original_sku_price로 이름 변경
- bestbuy_tv_main_crawl
- bby_tv_bsr_crawl
"""
import psycopg2
from config import DB_CONFIG

def rename_comparable_pricing_columns():
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        conn.autocommit = False
        cursor = conn.cursor()

        print("="*80)
        print("comparable_pricing → original_sku_price 컬럼명 변경")
        print("="*80)

        # 1. bestbuy_tv_main_crawl
        print("\n[1] bestbuy_tv_main_crawl 테이블 처리 중...")
        cursor.execute("""
            SELECT column_name
            FROM information_schema.columns
            WHERE table_name = 'bestbuy_tv_main_crawl'
            AND column_name = 'comparable_pricing'
        """)

        if cursor.fetchone():
            cursor.execute("""
                ALTER TABLE bestbuy_tv_main_crawl
                RENAME COLUMN comparable_pricing TO original_sku_price
            """)
            print("  [OK] comparable_pricing → original_sku_price 변경 완료")
        else:
            print("  [INFO] comparable_pricing 컬럼이 이미 변경되었거나 존재하지 않습니다")

        # 2. bby_tv_bsr_crawl
        print("\n[2] bby_tv_bsr_crawl 테이블 처리 중...")
        cursor.execute("""
            SELECT column_name
            FROM information_schema.columns
            WHERE table_name = 'bby_tv_bsr_crawl'
            AND column_name = 'comparable_pricing'
        """)

        if cursor.fetchone():
            cursor.execute("""
                ALTER TABLE bby_tv_bsr_crawl
                RENAME COLUMN comparable_pricing TO original_sku_price
            """)
            print("  [OK] comparable_pricing → original_sku_price 변경 완료")
        else:
            print("  [INFO] comparable_pricing 컬럼이 이미 변경되었거나 존재하지 않습니다")

        # 커밋
        conn.commit()
        print("\n" + "="*80)
        print("[SUCCESS] 모든 테이블 컬럼명 변경 완료!")
        print("="*80)

        # 변경 결과 확인
        print("\n[변경 결과 확인]")
        tables = ['bestbuy_tv_main_crawl', 'bby_tv_bsr_crawl']

        for table_name in tables:
            cursor.execute("""
                SELECT column_name, data_type
                FROM information_schema.columns
                WHERE table_name = %s
                ORDER BY ordinal_position
            """, (table_name,))

            columns = cursor.fetchall()
            print(f"\n{table_name}:")
            for col_name, col_type in columns:
                print(f"  {col_name}: {col_type}")

        cursor.close()
        conn.close()

    except Exception as e:
        print(f"\n[ERROR] 컬럼명 변경 실패: {e}")
        if conn:
            conn.rollback()
            conn.close()
        import traceback
        traceback.print_exc()
        raise

if __name__ == "__main__":
    rename_comparable_pricing_columns()
