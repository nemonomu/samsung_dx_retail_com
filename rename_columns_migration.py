"""
데이터베이스 컬럼명 변경 마이그레이션
- bby_tv_detail_crawled: Samsung_SKU_Name → item, item 컬럼 추가 시 retailer_sku_name으로 추가
- bestbuy_tv_main_crawl: item → retailer_sku_name
- bby_tv_promotion_crawl: item → retailer_sku_name
"""
import psycopg2
from config import DB_CONFIG

def rename_columns():
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        conn.autocommit = False
        cursor = conn.cursor()

        print("="*80)
        print("컬럼명 변경 마이그레이션 시작")
        print("="*80)

        # 1. bby_tv_detail_crawled 테이블
        print("\n[1] bby_tv_detail_crawled 테이블 변경 중...")
        try:
            # retailer_sku_name 컬럼이 없으면 item을 retailer_sku_name으로 변경
            cursor.execute("""
                SELECT column_name
                FROM information_schema.columns
                WHERE table_name = 'bby_tv_detail_crawled'
                AND column_name = 'retailer_sku_name'
            """)

            if not cursor.fetchone():
                # retailer_sku_name 컬럼이 없으면 item을 변경
                cursor.execute("""
                    ALTER TABLE bby_tv_detail_crawled
                    RENAME COLUMN item TO retailer_sku_name
                """)
                print("  [OK] item -> retailer_sku_name 변경 완료")
            else:
                print("  - retailer_sku_name 컬럼이 이미 존재합니다")

            # Samsung_SKU_Name을 item으로 변경
            cursor.execute("""
                SELECT column_name
                FROM information_schema.columns
                WHERE table_name = 'bby_tv_detail_crawled'
                AND column_name = 'samsung_sku_name'
            """)

            if cursor.fetchone():
                cursor.execute("""
                    ALTER TABLE bby_tv_detail_crawled
                    RENAME COLUMN samsung_sku_name TO item
                """)
                print("  [OK] Samsung_SKU_Name -> item 변경 완료")
            else:
                # 대소문자 구분해서 다시 시도
                cursor.execute("""
                    SELECT column_name
                    FROM information_schema.columns
                    WHERE table_name = 'bby_tv_detail_crawled'
                    AND column_name = 'Samsung_SKU_Name'
                """)

                if cursor.fetchone():
                    cursor.execute("""
                        ALTER TABLE bby_tv_detail_crawled
                        RENAME COLUMN "Samsung_SKU_Name" TO item
                    """)
                    print("  [OK] Samsung_SKU_Name -> item 변경 완료")
                else:
                    print("  - Samsung_SKU_Name 컬럼이 이미 item으로 변경되었습니다")

        except Exception as e:
            print(f"  [ERROR] bby_tv_detail_crawled 테이블 변경 실패: {e}")
            raise

        # 2. bestbuy_tv_main_crawl 테이블
        print("\n[2] bestbuy_tv_main_crawl 테이블 변경 중...")
        try:
            cursor.execute("""
                SELECT column_name
                FROM information_schema.columns
                WHERE table_name = 'bestbuy_tv_main_crawl'
                AND column_name = 'item'
            """)

            if cursor.fetchone():
                cursor.execute("""
                    ALTER TABLE bestbuy_tv_main_crawl
                    RENAME COLUMN item TO retailer_sku_name
                """)
                print("  [OK] item -> retailer_sku_name 변경 완료")
            else:
                print("  - item 컬럼이 이미 retailer_sku_name으로 변경되었습니다")

        except Exception as e:
            print(f"  [ERROR] bestbuy_tv_main_crawl 테이블 변경 실패: {e}")
            raise

        # 3. bby_tv_promotion_crawl 테이블
        print("\n[3] bby_tv_promotion_crawl 테이블 변경 중...")
        try:
            cursor.execute("""
                SELECT column_name
                FROM information_schema.columns
                WHERE table_name = 'bby_tv_promotion_crawl'
                AND column_name = 'item'
            """)

            if cursor.fetchone():
                cursor.execute("""
                    ALTER TABLE bby_tv_promotion_crawl
                    RENAME COLUMN item TO retailer_sku_name
                """)
                print("  [OK] item -> retailer_sku_name 변경 완료")
            else:
                print("  - item 컬럼이 이미 retailer_sku_name으로 변경되었습니다")

        except Exception as e:
            print(f"  [ERROR] bby_tv_promotion_crawl 테이블 변경 실패: {e}")
            raise

        # 커밋
        conn.commit()
        print("\n" + "="*80)
        print("[SUCCESS] 모든 컬럼명 변경 완료!")
        print("="*80)

        # 변경 결과 확인
        print("\n[변경 결과 확인]")

        for table_name in ['bby_tv_detail_crawled', 'bestbuy_tv_main_crawl', 'bby_tv_promotion_crawl']:
            cursor.execute("""
                SELECT column_name
                FROM information_schema.columns
                WHERE table_name = %s
                ORDER BY ordinal_position
            """, (table_name,))

            columns = [row[0] for row in cursor.fetchall()]
            print(f"\n{table_name}:")
            print(f"  컬럼: {', '.join(columns)}")

        cursor.close()
        conn.close()

    except Exception as e:
        print(f"\n[ERROR] 마이그레이션 실패: {e}")
        if conn:
            conn.rollback()
            conn.close()
        raise

if __name__ == "__main__":
    rename_columns()
