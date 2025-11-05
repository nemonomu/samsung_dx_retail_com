"""
crawl_datetime/crawl_at_local_time을 crawl_strdatetime으로 변경
- bby_tv_promotion_crawl: crawl_datetime → crawl_strdatetime
- bby_tv_trend_crawl: crawl_datetime → crawl_strdatetime
- bestbuy_tv_main_crawl: crawl_at_local_time → crawl_strdatetime

기존 데이터는 유지하고 새 컬럼만 추가
"""
import psycopg2
from config import DB_CONFIG

def add_crawl_strdatetime_columns():
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        conn.autocommit = False
        cursor = conn.cursor()

        print("="*80)
        print("crawl_strdatetime 컬럼 추가 마이그레이션 시작")
        print("="*80)

        # 1. bby_tv_promotion_crawl
        print("\n[1] bby_tv_promotion_crawl 테이블 처리 중...")
        cursor.execute("""
            SELECT column_name
            FROM information_schema.columns
            WHERE table_name = 'bby_tv_promotion_crawl'
            AND column_name = 'crawl_strdatetime'
        """)

        if cursor.fetchone():
            print("  [INFO] crawl_strdatetime 컬럼이 이미 존재합니다")
        else:
            cursor.execute("""
                ALTER TABLE bby_tv_promotion_crawl
                ADD COLUMN crawl_strdatetime VARCHAR(20)
            """)
            print("  [OK] crawl_strdatetime 컬럼 추가 완료")

        # 2. bby_tv_trend_crawl
        print("\n[2] bby_tv_trend_crawl 테이블 처리 중...")
        cursor.execute("""
            SELECT column_name
            FROM information_schema.columns
            WHERE table_name = 'bby_tv_trend_crawl'
            AND column_name = 'crawl_strdatetime'
        """)

        if cursor.fetchone():
            print("  [INFO] crawl_strdatetime 컬럼이 이미 존재합니다")
        else:
            cursor.execute("""
                ALTER TABLE bby_tv_trend_crawl
                ADD COLUMN crawl_strdatetime VARCHAR(20)
            """)
            print("  [OK] crawl_strdatetime 컬럼 추가 완료")

        # 3. bestbuy_tv_main_crawl
        print("\n[3] bestbuy_tv_main_crawl 테이블 처리 중...")
        cursor.execute("""
            SELECT column_name
            FROM information_schema.columns
            WHERE table_name = 'bestbuy_tv_main_crawl'
            AND column_name = 'crawl_strdatetime'
        """)

        if cursor.fetchone():
            print("  [INFO] crawl_strdatetime 컬럼이 이미 존재합니다")
        else:
            cursor.execute("""
                ALTER TABLE bestbuy_tv_main_crawl
                ADD COLUMN crawl_strdatetime VARCHAR(20)
            """)
            print("  [OK] crawl_strdatetime 컬럼 추가 완료")

        # 커밋
        conn.commit()
        print("\n" + "="*80)
        print("[SUCCESS] 모든 테이블에 crawl_strdatetime 컬럼 추가 완료!")
        print("="*80)

        # 변경 결과 확인
        print("\n[변경 결과 확인]")
        tables = [
            'bby_tv_promotion_crawl',
            'bby_tv_trend_crawl',
            'bestbuy_tv_main_crawl'
        ]

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
        print(f"\n[ERROR] 마이그레이션 실패: {e}")
        if conn:
            conn.rollback()
            conn.close()
        import traceback
        traceback.print_exc()
        raise

if __name__ == "__main__":
    add_crawl_strdatetime_columns()
