"""
crawl_datetime/crawl_at_local_time 데이터를 crawl_strdatetime으로 변환 후 삭제
- bby_tv_promotion_crawl: crawl_datetime → crawl_strdatetime 변환 후 삭제
- bby_tv_trend_crawl: crawl_datetime → crawl_strdatetime 변환 후 삭제
- bestbuy_tv_main_crawl: crawl_at_local_time → crawl_strdatetime 변환 후 삭제

형식: YYYYMMDDHHMISS + 마이크로초 4자리
"""
import psycopg2
from config import DB_CONFIG

def migrate_crawl_datetime_to_strdatetime():
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        conn.autocommit = False
        cursor = conn.cursor()

        print("="*80)
        print("crawl_datetime → crawl_strdatetime 데이터 변환 및 삭제 시작")
        print("="*80)

        # 1. bby_tv_promotion_crawl
        print("\n[1] bby_tv_promotion_crawl 테이블 처리 중...")

        # 데이터 변환
        cursor.execute("""
            UPDATE bby_tv_promotion_crawl
            SET crawl_strdatetime = TO_CHAR(crawl_datetime, 'YYYYMMDDHH24MISS') ||
                SUBSTRING(TO_CHAR(crawl_datetime, 'US'), 1, 4)
            WHERE crawl_datetime IS NOT NULL AND crawl_strdatetime IS NULL
        """)
        updated_count = cursor.rowcount
        print(f"  [OK] {updated_count}개 레코드 변환 완료")

        # crawl_datetime 컬럼 삭제
        cursor.execute("""
            ALTER TABLE bby_tv_promotion_crawl
            DROP COLUMN crawl_datetime
        """)
        print("  [OK] crawl_datetime 컬럼 삭제 완료")

        # 2. bby_tv_trend_crawl
        print("\n[2] bby_tv_trend_crawl 테이블 처리 중...")

        # 데이터 변환
        cursor.execute("""
            UPDATE bby_tv_trend_crawl
            SET crawl_strdatetime = TO_CHAR(crawl_datetime, 'YYYYMMDDHH24MISS') ||
                SUBSTRING(TO_CHAR(crawl_datetime, 'US'), 1, 4)
            WHERE crawl_datetime IS NOT NULL AND crawl_strdatetime IS NULL
        """)
        updated_count = cursor.rowcount
        print(f"  [OK] {updated_count}개 레코드 변환 완료")

        # crawl_datetime 컬럼 삭제
        cursor.execute("""
            ALTER TABLE bby_tv_trend_crawl
            DROP COLUMN crawl_datetime
        """)
        print("  [OK] crawl_datetime 컬럼 삭제 완료")

        # 3. bestbuy_tv_main_crawl
        print("\n[3] bestbuy_tv_main_crawl 테이블 처리 중...")

        # 데이터 변환
        cursor.execute("""
            UPDATE bestbuy_tv_main_crawl
            SET crawl_strdatetime = TO_CHAR(crawl_at_local_time, 'YYYYMMDDHH24MISS') ||
                SUBSTRING(TO_CHAR(crawl_at_local_time, 'US'), 1, 4)
            WHERE crawl_at_local_time IS NOT NULL AND crawl_strdatetime IS NULL
        """)
        updated_count = cursor.rowcount
        print(f"  [OK] {updated_count}개 레코드 변환 완료")

        # crawl_at_local_time 컬럼 삭제
        cursor.execute("""
            ALTER TABLE bestbuy_tv_main_crawl
            DROP COLUMN crawl_at_local_time
        """)
        print("  [OK] crawl_at_local_time 컬럼 삭제 완료")

        # 커밋
        conn.commit()
        print("\n" + "="*80)
        print("[SUCCESS] 모든 테이블 변환 및 삭제 완료!")
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

        # 샘플 데이터 확인
        print("\n[샘플 데이터 확인]")

        cursor.execute("""
            SELECT crawl_strdatetime
            FROM bby_tv_promotion_crawl
            WHERE crawl_strdatetime IS NOT NULL
            ORDER BY id DESC
            LIMIT 3
        """)
        samples = cursor.fetchall()
        if samples:
            print("\nbby_tv_promotion_crawl 샘플:")
            for sample in samples:
                print(f"  crawl_strdatetime: {sample[0]}")

        cursor.execute("""
            SELECT crawl_strdatetime
            FROM bby_tv_trend_crawl
            WHERE crawl_strdatetime IS NOT NULL
            ORDER BY id DESC
            LIMIT 3
        """)
        samples = cursor.fetchall()
        if samples:
            print("\nbby_tv_trend_crawl 샘플:")
            for sample in samples:
                print(f"  crawl_strdatetime: {sample[0]}")

        cursor.execute("""
            SELECT crawl_strdatetime
            FROM bestbuy_tv_main_crawl
            WHERE crawl_strdatetime IS NOT NULL
            ORDER BY id DESC
            LIMIT 3
        """)
        samples = cursor.fetchall()
        if samples:
            print("\nbestbuy_tv_main_crawl 샘플:")
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
    migrate_crawl_datetime_to_strdatetime()
