"""
account_name 컬럼을 맨 앞으로 이동하는 마이그레이션
테이블을 재생성하여 컬럼 순서 변경
"""
import psycopg2
from config import DB_CONFIG

def reorder_account_name_column():
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        conn.autocommit = False
        cursor = conn.cursor()

        print("="*80)
        print("account_name 컬럼 순서 변경 마이그레이션 시작")
        print("="*80)

        # 1. bestbuy_tv_main_crawl
        print("\n[1] bestbuy_tv_main_crawl 테이블 재구성 중...")
        cursor.execute("""
            -- 임시 테이블 생성
            CREATE TABLE bestbuy_tv_main_crawl_new AS
            SELECT
                id,
                account_name,
                page_type,
                retailer_sku_name,
                final_sku_price,
                savings,
                comparable_pricing,
                offer,
                pick_up_availability,
                shipping_availability,
                delivery_availability,
                star_rating,
                sku_status,
                product_url,
                crawl_at_local_time,
                batch_id,
                calendar_week
            FROM bestbuy_tv_main_crawl;

            -- 기존 테이블 삭제
            DROP TABLE bestbuy_tv_main_crawl;

            -- 새 테이블 이름 변경
            ALTER TABLE bestbuy_tv_main_crawl_new RENAME TO bestbuy_tv_main_crawl;

            -- 시퀀스 재생성
            CREATE SEQUENCE IF NOT EXISTS bestbuy_tv_main_crawl_id_seq;
            ALTER TABLE bestbuy_tv_main_crawl ALTER COLUMN id SET DEFAULT nextval('bestbuy_tv_main_crawl_id_seq');
            SELECT setval('bestbuy_tv_main_crawl_id_seq', (SELECT MAX(id) FROM bestbuy_tv_main_crawl));
        """)
        print("  [OK] bestbuy_tv_main_crawl 완료")

        # 2. bby_tv_promotion_crawl
        print("\n[2] bby_tv_promotion_crawl 테이블 재구성 중...")
        cursor.execute("""
            CREATE TABLE bby_tv_promotion_crawl_new AS
            SELECT
                id,
                account_name,
                page_type,
                retailer_sku_name,
                rank,
                final_sku_price,
                original_sku_price,
                offer,
                savings,
                promotion_type,
                product_url,
                crawl_datetime,
                calendar_week,
                batch_id
            FROM bby_tv_promotion_crawl;

            DROP TABLE bby_tv_promotion_crawl;
            ALTER TABLE bby_tv_promotion_crawl_new RENAME TO bby_tv_promotion_crawl;

            CREATE SEQUENCE IF NOT EXISTS bby_tv_promotion_crawl_id_seq;
            ALTER TABLE bby_tv_promotion_crawl ALTER COLUMN id SET DEFAULT nextval('bby_tv_promotion_crawl_id_seq');
            SELECT setval('bby_tv_promotion_crawl_id_seq', (SELECT MAX(id) FROM bby_tv_promotion_crawl));
        """)
        print("  [OK] bby_tv_promotion_crawl 완료")

        # 3. bby_tv_mst
        print("\n[3] bby_tv_mst 테이블 재구성 중...")
        cursor.execute("""
            CREATE TABLE bby_tv_mst_new AS
            SELECT
                id,
                account_name,
                sku,
                product_url,
                pros,
                cons,
                product_name,
                update_date,
                calendar_week
            FROM bby_tv_mst;

            DROP TABLE bby_tv_mst;
            ALTER TABLE bby_tv_mst_new RENAME TO bby_tv_mst;

            CREATE SEQUENCE IF NOT EXISTS bby_tv_mst_id_seq;
            ALTER TABLE bby_tv_mst ALTER COLUMN id SET DEFAULT nextval('bby_tv_mst_id_seq');
            SELECT setval('bby_tv_mst_id_seq', (SELECT MAX(id) FROM bby_tv_mst));
        """)
        print("  [OK] bby_tv_mst 완료")

        # 4. bby_tv_detail_crawled
        print("\n[4] bby_tv_detail_crawled 테이블 재구성 중...")
        cursor.execute("""
            CREATE TABLE bby_tv_detail_crawled_new AS
            SELECT
                id,
                account_name,
                batch_id,
                page_type,
                "order",
                retailer_sku_name,
                item,
                estimated_annual_electricity_use,
                screen_size,
                count_of_star_ratings,
                top_mentions,
                detailed_review_content,
                recommendation_intent,
                product_url,
                crawl_datetime,
                calendar_week
            FROM bby_tv_detail_crawled;

            DROP TABLE bby_tv_detail_crawled;
            ALTER TABLE bby_tv_detail_crawled_new RENAME TO bby_tv_detail_crawled;

            CREATE SEQUENCE IF NOT EXISTS bby_tv_detail_crawled_id_seq;
            ALTER TABLE bby_tv_detail_crawled ALTER COLUMN id SET DEFAULT nextval('bby_tv_detail_crawled_id_seq');
            SELECT setval('bby_tv_detail_crawled_id_seq', (SELECT MAX(id) FROM bby_tv_detail_crawled));
        """)
        print("  [OK] bby_tv_detail_crawled 완료")

        # 5. bby_tv_trend_crawl
        print("\n[5] bby_tv_trend_crawl 테이블 재구성 중...")
        cursor.execute("""
            CREATE TABLE bby_tv_trend_crawl_new AS
            SELECT
                id,
                account_name,
                page_type,
                rank,
                product_name,
                product_url,
                crawl_datetime,
                batch_id,
                calendar_week
            FROM bby_tv_trend_crawl;

            DROP TABLE bby_tv_trend_crawl;
            ALTER TABLE bby_tv_trend_crawl_new RENAME TO bby_tv_trend_crawl;

            CREATE SEQUENCE IF NOT EXISTS bby_tv_trend_crawl_id_seq;
            ALTER TABLE bby_tv_trend_crawl ALTER COLUMN id SET DEFAULT nextval('bby_tv_trend_crawl_id_seq');
            SELECT setval('bby_tv_trend_crawl_id_seq', (SELECT MAX(id) FROM bby_tv_trend_crawl));
        """)
        print("  [OK] bby_tv_trend_crawl 완료")

        # 커밋
        conn.commit()
        print("\n" + "="*80)
        print("[SUCCESS] 모든 테이블의 컬럼 순서 변경 완료!")
        print("="*80)

        # 변경 결과 확인
        print("\n[변경 결과 확인]")
        tables = [
            'bestbuy_tv_main_crawl',
            'bby_tv_promotion_crawl',
            'bby_tv_mst',
            'bby_tv_detail_crawled',
            'bby_tv_trend_crawl'
        ]

        for table_name in tables:
            cursor.execute("""
                SELECT column_name
                FROM information_schema.columns
                WHERE table_name = %s
                ORDER BY ordinal_position
            """, (table_name,))

            columns = [row[0] for row in cursor.fetchall()]
            print(f"\n{table_name}:")
            print(f"  처음 5개 컬럼: {', '.join(columns[:5])}")

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
    reorder_account_name_column()
