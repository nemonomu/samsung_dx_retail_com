"""
wmart_tv_bsr_crawl 테이블 마이그레이션
1. account_name 컬럼 추가 (VARCHAR(50))
2. created_at → crawl_strdatetime 이름 변경 (형식: 202511051100000000)
3. order → bsr_rank 이름 변경
"""
import psycopg2
from config import DB_CONFIG

def migrate_wmart_tv_bsr_crawl():
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        conn.autocommit = False
        cursor = conn.cursor()

        print("="*80)
        print("wmart_tv_bsr_crawl 테이블 마이그레이션")
        print("="*80)

        # 1. account_name 컬럼 추가
        print("\n[1] account_name 컬럼 추가 중...")
        cursor.execute("""
            SELECT column_name
            FROM information_schema.columns
            WHERE table_name = 'wmart_tv_bsr_crawl'
            AND column_name = 'account_name'
        """)

        if not cursor.fetchone():
            cursor.execute("""
                ALTER TABLE wmart_tv_bsr_crawl
                ADD COLUMN account_name VARCHAR(50)
            """)
            print("  [OK] account_name 컬럼 추가 완료")
        else:
            print("  [INFO] account_name 컬럼이 이미 존재합니다")

        # 2. order → bsr_rank 이름 변경
        print("\n[2] order → bsr_rank 컬럼 이름 변경 중...")
        cursor.execute("""
            SELECT column_name
            FROM information_schema.columns
            WHERE table_name = 'wmart_tv_bsr_crawl'
            AND column_name = 'order'
        """)

        if cursor.fetchone():
            cursor.execute("""
                ALTER TABLE wmart_tv_bsr_crawl
                RENAME COLUMN "order" TO bsr_rank
            """)
            print("  [OK] order → bsr_rank 변경 완료")
        else:
            print("  [INFO] order 컬럼이 없거나 이미 변경되었습니다")

        # 3. created_at → crawl_strdatetime 이름 변경 및 형식 변환
        print("\n[3] created_at → crawl_strdatetime 변경 중...")
        cursor.execute("""
            SELECT column_name
            FROM information_schema.columns
            WHERE table_name = 'wmart_tv_bsr_crawl'
            AND column_name = 'created_at'
        """)

        if cursor.fetchone():
            # 임시 컬럼 생성 (VARCHAR)
            cursor.execute("""
                ALTER TABLE wmart_tv_bsr_crawl
                ADD COLUMN crawl_strdatetime VARCHAR(50)
            """)
            print("  [OK] crawl_strdatetime 컬럼 생성")

            # created_at 데이터를 새 형식으로 변환하여 crawl_strdatetime에 복사
            cursor.execute("""
                UPDATE wmart_tv_bsr_crawl
                SET crawl_strdatetime = TO_CHAR(created_at, 'YYYYMMDDHH24MISS') || '0000'
                WHERE created_at IS NOT NULL
            """)
            print("  [OK] 데이터 변환 완료 (형식: 202511051100000000)")

            # created_at 컬럼 삭제
            cursor.execute("""
                ALTER TABLE wmart_tv_bsr_crawl
                DROP COLUMN created_at
            """)
            print("  [OK] created_at 컬럼 삭제")
        else:
            print("  [INFO] created_at 컬럼이 없거나 이미 변경되었습니다")

        # 커밋
        conn.commit()
        print("\n" + "="*80)
        print("[SUCCESS] wmart_tv_bsr_crawl 테이블 마이그레이션 완료!")
        print("="*80)

        # 변경 결과 확인
        print("\n[확인] wmart_tv_bsr_crawl 테이블 컬럼:")
        cursor.execute("""
            SELECT column_name, data_type
            FROM information_schema.columns
            WHERE table_name = 'wmart_tv_bsr_crawl'
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
    migrate_wmart_tv_bsr_crawl()
