"""
amazon_tv_detail_crawled 테이블에 account_name 컬럼 추가
"""
import psycopg2
from config import DB_CONFIG

def add_account_name_column():
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        conn.autocommit = False
        cursor = conn.cursor()

        print("="*80)
        print("amazon_tv_detail_crawled 테이블에 account_name 컬럼 추가")
        print("="*80)

        # account_name 컬럼 추가
        cursor.execute("""
            SELECT column_name
            FROM information_schema.columns
            WHERE table_name = 'amazon_tv_detail_crawled'
            AND column_name = 'account_name'
        """)

        if not cursor.fetchone():
            cursor.execute("""
                ALTER TABLE amazon_tv_detail_crawled
                ADD COLUMN account_name VARCHAR(50)
            """)
            print("  [OK] account_name 컬럼 추가 완료")
        else:
            print("  [INFO] account_name 컬럼이 이미 존재합니다")

        # 커밋
        conn.commit()
        print("\n" + "="*80)
        print("[SUCCESS] 컬럼 추가 완료!")
        print("="*80)

        # 변경 결과 확인
        print("\n[변경 결과 확인]")
        cursor.execute("""
            SELECT column_name, data_type
            FROM information_schema.columns
            WHERE table_name = 'amazon_tv_detail_crawled'
            ORDER BY ordinal_position
        """)

        columns = cursor.fetchall()
        print("\namazon_tv_detail_crawled 테이블 컬럼:")
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
    add_account_name_column()
