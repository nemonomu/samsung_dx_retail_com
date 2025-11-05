"""
bby_tv_detail_crawled 테이블의 crawl_datetime을 crawl_strdatetime으로 변경
TIMESTAMP 타입을 VARCHAR(20) 타입으로 변경하고 형식 변환
"""
import psycopg2
from config import DB_CONFIG

def rename_crawl_datetime():
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        conn.autocommit = False
        cursor = conn.cursor()

        print("="*80)
        print("crawl_datetime -> crawl_strdatetime 마이그레이션 시작")
        print("="*80)

        # crawl_datetime 컬럼이 있는지 확인
        cursor.execute("""
            SELECT column_name, data_type
            FROM information_schema.columns
            WHERE table_name = 'bby_tv_detail_crawled'
            AND column_name IN ('crawl_datetime', 'crawl_strdatetime')
        """)

        existing_columns = {row[0]: row[1] for row in cursor.fetchall()}
        print(f"\n[INFO] 현재 컬럼: {existing_columns}")

        if 'crawl_strdatetime' in existing_columns:
            print("[INFO] crawl_strdatetime 컬럼이 이미 존재합니다")
        elif 'crawl_datetime' in existing_columns:
            print("\n[1] crawl_datetime 컬럼을 crawl_strdatetime으로 변경 중...")

            # 임시 컬럼 추가
            cursor.execute("""
                ALTER TABLE bby_tv_detail_crawled
                ADD COLUMN IF NOT EXISTS crawl_strdatetime VARCHAR(20)
            """)
            print("  [OK] crawl_strdatetime 컬럼 추가")

            # 기존 데이터를 새 형식으로 변환
            cursor.execute("""
                UPDATE bby_tv_detail_crawled
                SET crawl_strdatetime = TO_CHAR(crawl_datetime, 'YYYYMMDDHH24MISS') ||
                    SUBSTRING(TO_CHAR(crawl_datetime, 'US'), 1, 4)
                WHERE crawl_datetime IS NOT NULL AND crawl_strdatetime IS NULL
            """)
            updated_count = cursor.rowcount
            print(f"  [OK] {updated_count}개 레코드 변환 완료")

            # 기존 컬럼 삭제
            cursor.execute("""
                ALTER TABLE bby_tv_detail_crawled
                DROP COLUMN crawl_datetime
            """)
            print("  [OK] crawl_datetime 컬럼 삭제")
        else:
            print("\n[INFO] crawl_datetime 컬럼이 존재하지 않습니다. 새 테이블로 추정됩니다.")

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

        # 샘플 데이터 확인
        cursor.execute("""
            SELECT crawl_strdatetime
            FROM bby_tv_detail_crawled
            WHERE crawl_strdatetime IS NOT NULL
            ORDER BY id DESC
            LIMIT 3
        """)

        samples = cursor.fetchall()
        if samples:
            print("\n[샘플 데이터]")
            for sample in samples:
                print(f"  crawl_strdatetime: {sample[0]}")

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
    rename_crawl_datetime()
