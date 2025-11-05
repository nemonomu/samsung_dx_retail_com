"""
Amazon 테이블 마이그레이션
1. amazon_tv_detail_crawled: screen_size, count_of_reviews 추가, 컬럼명 변경
2. amazon_tv_main_crawled: 컬럼명 변경, page_type 추가, asin 삭제
3. amazon_tv_bsr: 컬럼명 변경, page_type 추가, 6개 컬럼 추가
"""
import psycopg2
from config import DB_CONFIG

def migrate_amazon_tables():
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        conn.autocommit = False
        cursor = conn.cursor()

        print("="*80)
        print("Amazon 테이블 마이그레이션")
        print("="*80)

        # ========================================================================
        # 1. amazon_tv_detail_crawled 테이블
        # ========================================================================
        print("\n[1] amazon_tv_detail_crawled 테이블 처리 중...")

        # 1-1. screen_size 컬럼 추가
        cursor.execute("""
            SELECT column_name
            FROM information_schema.columns
            WHERE table_name = 'amazon_tv_detail_crawled'
            AND column_name = 'screen_size'
        """)
        if not cursor.fetchone():
            cursor.execute("""
                ALTER TABLE amazon_tv_detail_crawled
                ADD COLUMN screen_size TEXT
            """)
            print("  [OK] screen_size 컬럼 추가 완료")
        else:
            print("  [INFO] screen_size 컬럼이 이미 존재합니다")

        # 1-2. count_of_reviews 컬럼 추가
        cursor.execute("""
            SELECT column_name
            FROM information_schema.columns
            WHERE table_name = 'amazon_tv_detail_crawled'
            AND column_name = 'count_of_reviews'
        """)
        if not cursor.fetchone():
            cursor.execute("""
                ALTER TABLE amazon_tv_detail_crawled
                ADD COLUMN count_of_reviews TEXT
            """)
            print("  [OK] count_of_reviews 컬럼 추가 완료")
        else:
            print("  [INFO] count_of_reviews 컬럼이 이미 존재합니다")

        # 1-3. samsung_sku_name → item
        cursor.execute("""
            SELECT column_name
            FROM information_schema.columns
            WHERE table_name = 'amazon_tv_detail_crawled'
            AND column_name = 'samsung_sku_name'
        """)
        if cursor.fetchone():
            cursor.execute("""
                ALTER TABLE amazon_tv_detail_crawled
                RENAME COLUMN samsung_sku_name TO item
            """)
            print("  [OK] samsung_sku_name → item 변경 완료")
        else:
            print("  [INFO] samsung_sku_name 컬럼이 이미 변경되었거나 존재하지 않습니다")

        # 1-4. mother → page_type
        cursor.execute("""
            SELECT column_name
            FROM information_schema.columns
            WHERE table_name = 'amazon_tv_detail_crawled'
            AND column_name = 'mother'
        """)
        if cursor.fetchone():
            cursor.execute("""
                ALTER TABLE amazon_tv_detail_crawled
                RENAME COLUMN mother TO page_type
            """)
            print("  [OK] mother → page_type 변경 완료")
        else:
            print("  [INFO] mother 컬럼이 이미 변경되었거나 존재하지 않습니다")

        # 1-5. order 컬럼 삭제
        cursor.execute("""
            SELECT column_name
            FROM information_schema.columns
            WHERE table_name = 'amazon_tv_detail_crawled'
            AND column_name = 'order'
        """)
        if cursor.fetchone():
            cursor.execute("""
                ALTER TABLE amazon_tv_detail_crawled
                DROP COLUMN "order"
            """)
            print("  [OK] order 컬럼 삭제 완료")
        else:
            print("  [INFO] order 컬럼이 이미 삭제되었거나 존재하지 않습니다")

        # 1-6. crawl_at_local_time → crawl_strdatetime
        cursor.execute("""
            SELECT column_name
            FROM information_schema.columns
            WHERE table_name = 'amazon_tv_detail_crawled'
            AND column_name = 'crawl_at_local_time'
        """)
        if cursor.fetchone():
            # 새 컬럼 추가
            cursor.execute("""
                ALTER TABLE amazon_tv_detail_crawled
                ADD COLUMN crawl_strdatetime VARCHAR(20)
            """)
            # 기존 데이터 변환
            cursor.execute("""
                UPDATE amazon_tv_detail_crawled
                SET crawl_strdatetime = TO_CHAR(crawl_at_local_time, 'YYYYMMDDHH24MISS') ||
                    SUBSTRING(TO_CHAR(crawl_at_local_time, 'US'), 1, 4)
                WHERE crawl_at_local_time IS NOT NULL AND crawl_strdatetime IS NULL
            """)
            # 기존 컬럼 삭제
            cursor.execute("""
                ALTER TABLE amazon_tv_detail_crawled
                DROP COLUMN crawl_at_local_time
            """)
            print("  [OK] crawl_at_local_time → crawl_strdatetime 변경 완료")
        else:
            print("  [INFO] crawl_at_local_time 컬럼이 이미 변경되었거나 존재하지 않습니다")

        # ========================================================================
        # 2. amazon_tv_main_crawled 테이블
        # ========================================================================
        print("\n[2] amazon_tv_main_crawled 테이블 처리 중...")

        # 2-1. collected_at_local_time → crawl_strdatetime
        cursor.execute("""
            SELECT column_name
            FROM information_schema.columns
            WHERE table_name = 'amazon_tv_main_crawled'
            AND column_name = 'collected_at_local_time'
        """)
        if cursor.fetchone():
            # 새 컬럼 추가
            cursor.execute("""
                ALTER TABLE amazon_tv_main_crawled
                ADD COLUMN crawl_strdatetime VARCHAR(20)
            """)
            # 기존 데이터 변환
            cursor.execute("""
                UPDATE amazon_tv_main_crawled
                SET crawl_strdatetime = TO_CHAR(collected_at_local_time, 'YYYYMMDDHH24MISS') ||
                    SUBSTRING(TO_CHAR(collected_at_local_time, 'US'), 1, 4)
                WHERE collected_at_local_time IS NOT NULL AND crawl_strdatetime IS NULL
            """)
            # 기존 컬럼 삭제
            cursor.execute("""
                ALTER TABLE amazon_tv_main_crawled
                DROP COLUMN collected_at_local_time
            """)
            print("  [OK] collected_at_local_time → crawl_strdatetime 변경 완료")
        else:
            print("  [INFO] collected_at_local_time 컬럼이 이미 변경되었거나 존재하지 않습니다")

        # 2-2. page_type 컬럼 추가
        cursor.execute("""
            SELECT column_name
            FROM information_schema.columns
            WHERE table_name = 'amazon_tv_main_crawled'
            AND column_name = 'page_type'
        """)
        if not cursor.fetchone():
            cursor.execute("""
                ALTER TABLE amazon_tv_main_crawled
                ADD COLUMN page_type VARCHAR(50)
            """)
            # 기존 데이터는 'main'으로 설정
            cursor.execute("""
                UPDATE amazon_tv_main_crawled
                SET page_type = 'main'
                WHERE page_type IS NULL
            """)
            print("  [OK] page_type 컬럼 추가 완료 (기존 데이터는 'main'으로 설정)")
        else:
            print("  [INFO] page_type 컬럼이 이미 존재합니다")

        # 2-3. mall_name → account_name
        cursor.execute("""
            SELECT column_name
            FROM information_schema.columns
            WHERE table_name = 'amazon_tv_main_crawled'
            AND column_name = 'mall_name'
        """)
        if cursor.fetchone():
            cursor.execute("""
                ALTER TABLE amazon_tv_main_crawled
                RENAME COLUMN mall_name TO account_name
            """)
            print("  [OK] mall_name → account_name 변경 완료")
        else:
            print("  [INFO] mall_name 컬럼이 이미 변경되었거나 존재하지 않습니다")

        # 2-4. asin 컬럼 삭제
        cursor.execute("""
            SELECT column_name
            FROM information_schema.columns
            WHERE table_name = 'amazon_tv_main_crawled'
            AND column_name = 'asin'
        """)
        if cursor.fetchone():
            cursor.execute("""
                ALTER TABLE amazon_tv_main_crawled
                DROP COLUMN asin
            """)
            print("  [OK] asin 컬럼 삭제 완료")
        else:
            print("  [INFO] asin 컬럼이 이미 삭제되었거나 존재하지 않습니다")

        # 2-5. order → main_rank
        cursor.execute("""
            SELECT column_name
            FROM information_schema.columns
            WHERE table_name = 'amazon_tv_main_crawled'
            AND column_name = 'order'
        """)
        if cursor.fetchone():
            cursor.execute("""
                ALTER TABLE amazon_tv_main_crawled
                RENAME COLUMN "order" TO main_rank
            """)
            print("  [OK] order → main_rank 변경 완료")
        else:
            print("  [INFO] order 컬럼이 이미 변경되었거나 존재하지 않습니다")

        # ========================================================================
        # 3. amazon_tv_bsr 테이블
        # ========================================================================
        print("\n[3] amazon_tv_bsr 테이블 처리 중...")

        # 3-1. crawl_at_local_time → crawl_strdatetime
        cursor.execute("""
            SELECT column_name
            FROM information_schema.columns
            WHERE table_name = 'amazon_tv_bsr'
            AND column_name = 'crawl_at_local_time'
        """)
        if cursor.fetchone():
            # 새 컬럼 추가
            cursor.execute("""
                ALTER TABLE amazon_tv_bsr
                ADD COLUMN crawl_strdatetime VARCHAR(20)
            """)
            # 기존 데이터 변환
            cursor.execute("""
                UPDATE amazon_tv_bsr
                SET crawl_strdatetime = TO_CHAR(crawl_at_local_time, 'YYYYMMDDHH24MISS') ||
                    SUBSTRING(TO_CHAR(crawl_at_local_time, 'US'), 1, 4)
                WHERE crawl_at_local_time IS NOT NULL AND crawl_strdatetime IS NULL
            """)
            # 기존 컬럼 삭제
            cursor.execute("""
                ALTER TABLE amazon_tv_bsr
                DROP COLUMN crawl_at_local_time
            """)
            print("  [OK] crawl_at_local_time → crawl_strdatetime 변경 완료")
        else:
            print("  [INFO] crawl_at_local_time 컬럼이 이미 변경되었거나 존재하지 않습니다")

        # 3-2. rank → bsr_rank
        cursor.execute("""
            SELECT column_name
            FROM information_schema.columns
            WHERE table_name = 'amazon_tv_bsr'
            AND column_name = 'rank'
        """)
        if cursor.fetchone():
            cursor.execute("""
                ALTER TABLE amazon_tv_bsr
                RENAME COLUMN rank TO bsr_rank
            """)
            print("  [OK] rank → bsr_rank 변경 완료")
        else:
            print("  [INFO] rank 컬럼이 이미 변경되었거나 존재하지 않습니다")

        # 3-3. page_type 컬럼 추가
        cursor.execute("""
            SELECT column_name
            FROM information_schema.columns
            WHERE table_name = 'amazon_tv_bsr'
            AND column_name = 'page_type'
        """)
        if not cursor.fetchone():
            cursor.execute("""
                ALTER TABLE amazon_tv_bsr
                ADD COLUMN page_type VARCHAR(50)
            """)
            # 기존 데이터는 'bsr'로 설정
            cursor.execute("""
                UPDATE amazon_tv_bsr
                SET page_type = 'bsr'
                WHERE page_type IS NULL
            """)
            print("  [OK] page_type 컬럼 추가 완료 (기존 데이터는 'bsr'로 설정)")
        else:
            print("  [INFO] page_type 컬럼이 이미 존재합니다")

        # 3-4. 6개 컬럼 추가
        new_columns = [
            ('final_sku_price', 'TEXT'),
            ('original_sku_price', 'TEXT'),
            ('number_of_units_purchased_past_month', 'TEXT'),
            ('shipping_info', 'TEXT'),
            ('available_quantity_for_purchase', 'TEXT'),
            ('discount_type', 'TEXT')
        ]

        for col_name, col_type in new_columns:
            cursor.execute("""
                SELECT column_name
                FROM information_schema.columns
                WHERE table_name = 'amazon_tv_bsr'
                AND column_name = %s
            """, (col_name,))
            if not cursor.fetchone():
                cursor.execute(f"""
                    ALTER TABLE amazon_tv_bsr
                    ADD COLUMN {col_name} {col_type}
                """)
                print(f"  [OK] {col_name} 컬럼 추가 완료")
            else:
                print(f"  [INFO] {col_name} 컬럼이 이미 존재합니다")

        # 커밋
        conn.commit()
        print("\n" + "="*80)
        print("[SUCCESS] 모든 테이블 마이그레이션 완료!")
        print("="*80)

        # 변경 결과 확인
        print("\n[변경 결과 확인]")
        tables = ['amazon_tv_detail_crawled', 'amazon_tv_main_crawled', 'amazon_tv_bsr']

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
    migrate_amazon_tables()
