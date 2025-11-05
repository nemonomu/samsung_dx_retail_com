"""
bby_tv_detail_crawled 테이블에 count_of_reviews 컬럼 추가
"""
import psycopg2
from config import DB_CONFIG

def add_count_of_reviews_column():
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        conn.autocommit = False
        cursor = conn.cursor()

        print("="*80)
        print("count_of_reviews 컬럼 추가 마이그레이션 시작")
        print("="*80)

        # count_of_reviews 컬럼이 이미 있는지 확인
        cursor.execute("""
            SELECT column_name
            FROM information_schema.columns
            WHERE table_name = 'bby_tv_detail_crawled'
            AND column_name = 'count_of_reviews'
        """)

        if cursor.fetchone():
            print("\n[INFO] count_of_reviews 컬럼이 이미 존재합니다")
        else:
            print("\n[1] count_of_reviews 컬럼 추가 중...")

            # count_of_reviews 컬럼 추가 (screen_size 다음, Count_of_Star_Ratings 전)
            cursor.execute("""
                ALTER TABLE bby_tv_detail_crawled
                ADD COLUMN count_of_reviews TEXT
            """)
            print("  [OK] count_of_reviews 컬럼 추가 완료")

        # 커밋
        conn.commit()
        print("\n" + "="*80)
        print("[SUCCESS] 마이그레이션 완료!")
        print("="*80)

        # 변경 결과 확인
        print("\n[변경 결과 확인]")
        cursor.execute("""
            SELECT column_name, data_type
            FROM information_schema.columns
            WHERE table_name = 'bby_tv_detail_crawled'
            ORDER BY ordinal_position
        """)

        columns = cursor.fetchall()
        print("\nbby_tv_detail_crawled 테이블 컬럼:")
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
    add_count_of_reviews_column()
