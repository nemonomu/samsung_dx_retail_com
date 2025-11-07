"""
walmart_tv_detail_crawled 테이블에서 order 컬럼 삭제
"""
import psycopg2
from config import DB_CONFIG

def remove_order_column():
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        conn.autocommit = False
        cursor = conn.cursor()

        print("="*80)
        print("walmart_tv_detail_crawled 테이블에서 order 컬럼 삭제")
        print("="*80)

        # order 컬럼 확인
        print("\n[1] order 컬럼 확인 중...")
        cursor.execute("""
            SELECT column_name
            FROM information_schema.columns
            WHERE table_name = 'walmart_tv_detail_crawled'
            AND column_name = 'order'
        """)

        if cursor.fetchone():
            # order 컬럼 삭제
            cursor.execute("""
                ALTER TABLE walmart_tv_detail_crawled
                DROP COLUMN "order"
            """)
            print("  [OK] order 컬럼 삭제 완료")
        else:
            print("  [INFO] order 컬럼이 존재하지 않습니다")

        # 커밋
        conn.commit()
        print("\n" + "="*80)
        print("[SUCCESS] order 컬럼 삭제 완료!")
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
        print(f"\n[ERROR] 컬럼 삭제 실패: {e}")
        if conn:
            conn.rollback()
            conn.close()
        import traceback
        traceback.print_exc()
        raise

if __name__ == "__main__":
    remove_order_column()
