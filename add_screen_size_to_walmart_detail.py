"""
walmart_tv_detail_crawled 테이블에 screen_size 컬럼 추가
"""
import psycopg2
from config import DB_CONFIG

def add_screen_size_column():
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        conn.autocommit = False
        cursor = conn.cursor()

        print("="*80)
        print("walmart_tv_detail_crawled 테이블에 screen_size 컬럼 추가")
        print("="*80)

        # screen_size 컬럼 추가
        print("\n[1] screen_size 컬럼 추가 중...")
        cursor.execute("""
            SELECT column_name
            FROM information_schema.columns
            WHERE table_name = 'walmart_tv_detail_crawled'
            AND column_name = 'screen_size'
        """)

        if not cursor.fetchone():
            cursor.execute("""
                ALTER TABLE walmart_tv_detail_crawled
                ADD COLUMN screen_size VARCHAR(50)
            """)
            print("  [OK] screen_size 컬럼 추가 완료")
        else:
            print("  [INFO] screen_size 컬럼이 이미 존재합니다")

        # 커밋
        conn.commit()
        print("\n" + "="*80)
        print("[SUCCESS] screen_size 컬럼 추가 완료!")
        print("="*80)

        # 변경 결과 확인
        print("\n[확인] walmart_tv_detail_crawled 테이블 컬럼:")
        cursor.execute("""
            SELECT column_name, data_type
            FROM information_schema.columns
            WHERE table_name = 'walmart_tv_detail_crawled'
            ORDER BY ordinal_position
        """)

        columns = cursor.fetchall()
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
    add_screen_size_column()
