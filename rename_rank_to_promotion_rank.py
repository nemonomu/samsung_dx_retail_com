"""
bby_tv_promotion_crawl 테이블의 rank 컬럼을 promotion_rank로 이름 변경
"""
import psycopg2
from config import DB_CONFIG

def rename_rank_to_promotion_rank():
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        conn.autocommit = False
        cursor = conn.cursor()

        print("="*80)
        print("rank → promotion_rank 컬럼명 변경")
        print("="*80)

        # bby_tv_promotion_crawl
        print("\n[1] bby_tv_promotion_crawl 테이블 처리 중...")
        cursor.execute("""
            SELECT column_name
            FROM information_schema.columns
            WHERE table_name = 'bby_tv_promotion_crawl'
            AND column_name = 'rank'
        """)

        if cursor.fetchone():
            cursor.execute("""
                ALTER TABLE bby_tv_promotion_crawl
                RENAME COLUMN rank TO promotion_rank
            """)
            print("  [OK] rank → promotion_rank 변경 완료")
        else:
            print("  [INFO] rank 컬럼이 이미 변경되었거나 존재하지 않습니다")

        # 커밋
        conn.commit()
        print("\n" + "="*80)
        print("[SUCCESS] 컬럼명 변경 완료!")
        print("="*80)

        # 변경 결과 확인
        print("\n[변경 결과 확인]")
        cursor.execute("""
            SELECT column_name, data_type
            FROM information_schema.columns
            WHERE table_name = 'bby_tv_promotion_crawl'
            ORDER BY ordinal_position
        """)

        columns = cursor.fetchall()
        print("\nbby_tv_promotion_crawl 테이블 컬럼:")
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
    rename_rank_to_promotion_rank()
