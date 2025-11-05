"""
main_rank, bsr_rank 컬럼 추가
- bestbuy_tv_main_crawl: main_rank 추가 (page_type 다음)
- bby_tv_bsr_crawl: bsr_rank 추가 (page_type 다음)
"""
import psycopg2
from config import DB_CONFIG

def add_rank_columns():
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        conn.autocommit = False
        cursor = conn.cursor()

        print("="*80)
        print("main_rank, bsr_rank 컬럼 추가")
        print("="*80)

        # 1. bestbuy_tv_main_crawl - main_rank 추가
        print("\n[1] bestbuy_tv_main_crawl 테이블 처리 중...")
        cursor.execute("""
            SELECT column_name
            FROM information_schema.columns
            WHERE table_name = 'bestbuy_tv_main_crawl'
            AND column_name = 'main_rank'
        """)

        if cursor.fetchone():
            print("  [INFO] main_rank 컬럼이 이미 존재합니다")
        else:
            cursor.execute("""
                ALTER TABLE bestbuy_tv_main_crawl
                ADD COLUMN main_rank INTEGER
            """)
            print("  [OK] main_rank 컬럼 추가 완료")

        # 2. bby_tv_bsr_crawl - bsr_rank 추가
        print("\n[2] bby_tv_bsr_crawl 테이블 처리 중...")
        cursor.execute("""
            SELECT column_name
            FROM information_schema.columns
            WHERE table_name = 'bby_tv_bsr_crawl'
            AND column_name = 'bsr_rank'
        """)

        if cursor.fetchone():
            print("  [INFO] bsr_rank 컬럼이 이미 존재합니다")
        else:
            cursor.execute("""
                ALTER TABLE bby_tv_bsr_crawl
                ADD COLUMN bsr_rank INTEGER
            """)
            print("  [OK] bsr_rank 컬럼 추가 완료")

        # 커밋
        conn.commit()
        print("\n" + "="*80)
        print("[SUCCESS] 모든 테이블 컬럼 추가 완료!")
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
        print(f"\n[ERROR] 컬럼 추가 실패: {e}")
        if conn:
            conn.rollback()
            conn.close()
        import traceback
        traceback.print_exc()
        raise

if __name__ == "__main__":
    add_rank_columns()
