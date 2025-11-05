"""
데이터베이스 account_name 컬럼 추가 마이그레이션
5개 테이블에 account_name 컬럼 추가 (맨 앞 위치)
- bestbuy_tv_main_crawl
- bby_tv_promotion_crawl
- bby_tv_mst
- bby_tv_detail_crawled
- bby_tv_trend_crawl
"""
import psycopg2
from config import DB_CONFIG

def add_account_name_column():
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        conn.autocommit = False
        cursor = conn.cursor()

        print("="*80)
        print("account_name 컬럼 추가 마이그레이션 시작")
        print("="*80)

        tables = [
            'bestbuy_tv_main_crawl',
            'bby_tv_promotion_crawl',
            'bby_tv_mst',
            'bby_tv_detail_crawled',
            'bby_tv_trend_crawl'
        ]

        for idx, table_name in enumerate(tables, 1):
            print(f"\n[{idx}] {table_name} 테이블 처리 중...")

            try:
                # account_name 컬럼이 이미 있는지 확인
                cursor.execute("""
                    SELECT column_name
                    FROM information_schema.columns
                    WHERE table_name = %s
                    AND column_name = 'account_name'
                """, (table_name,))

                if cursor.fetchone():
                    print(f"  - account_name 컬럼이 이미 존재합니다")
                    continue

                # account_name 컬럼 추가
                cursor.execute(f"""
                    ALTER TABLE {table_name}
                    ADD COLUMN account_name VARCHAR(50)
                """)
                print(f"  [OK] account_name 컬럼 추가 완료")

                # 기존 데이터에 'Bestbuy' 값 채우기
                cursor.execute(f"""
                    UPDATE {table_name}
                    SET account_name = 'Bestbuy'
                    WHERE account_name IS NULL
                """)
                updated_count = cursor.rowcount
                print(f"  [OK] 기존 데이터 {updated_count}개에 'Bestbuy' 값 설정 완료")

                # 컬럼 위치를 맨 앞으로 이동 (PostgreSQL에서는 순서 변경이 복잡하므로 스킵)
                # 새로 생성되는 테이블에서는 CREATE TABLE 문에서 순서가 정의됨

            except Exception as e:
                print(f"  [ERROR] {table_name} 테이블 처리 실패: {e}")
                raise

        # 커밋
        conn.commit()
        print("\n" + "="*80)
        print("[SUCCESS] 모든 테이블에 account_name 컬럼 추가 완료!")
        print("="*80)

        # 변경 결과 확인
        print("\n[변경 결과 확인]")

        for table_name in tables:
            cursor.execute("""
                SELECT column_name
                FROM information_schema.columns
                WHERE table_name = %s
                ORDER BY ordinal_position
            """, (table_name,))

            columns = [row[0] for row in cursor.fetchall()]
            print(f"\n{table_name}:")
            print(f"  컬럼: {', '.join(columns[:10])}...")  # 처음 10개만 출력

        cursor.close()
        conn.close()

    except Exception as e:
        print(f"\n[ERROR] 마이그레이션 실패: {e}")
        if conn:
            conn.rollback()
            conn.close()
        raise

if __name__ == "__main__":
    add_account_name_column()
