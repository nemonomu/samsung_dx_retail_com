"""
Best Buy TV v1 Tables Creation Script
bby_tv_main1, bby_tv_bsr1, bby_tv_pmt1, bby_tv_crawl 테이블 생성

한 번만 실행하면 됩니다.
"""
import psycopg2
from config import DB_CONFIG

def create_tables():
    """Create all v1 tables"""
    try:
        # DB 연결
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()
        print("[INFO] Database connected")

        # 1. bby_tv_main1 테이블 생성 (bestbuy_tv_main_crawl 구조 복사)
        print("\n[1/4] Creating bby_tv_main1 table...")
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS bby_tv_main1 (
                id SERIAL PRIMARY KEY,
                account_name VARCHAR(50),
                batch_id VARCHAR(50),
                page_type VARCHAR(50),
                main_rank INTEGER,
                retailer_sku_name TEXT,
                Offer VARCHAR(50),
                Pick_Up_Availability TEXT,
                Shipping_Availability TEXT,
                Delivery_Availability TEXT,
                SKU_Status VARCHAR(50),
                Product_url TEXT,
                crawl_datetime VARCHAR(50),
                calendar_week VARCHAR(10)
            )
        """)
        print("  [OK] bby_tv_main1 created")

        # 2. bby_tv_bsr1 테이블 생성 (bby_tv_bsr_crawl 구조 복사)
        print("\n[2/4] Creating bby_tv_bsr1 table...")
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS bby_tv_bsr1 (
                id SERIAL PRIMARY KEY,
                account_name VARCHAR(50),
                batch_id VARCHAR(50),
                page_type VARCHAR(50),
                bsr_rank INTEGER,
                retailer_sku_name TEXT,
                Offer VARCHAR(50),
                Pick_Up_Availability TEXT,
                Shipping_Availability TEXT,
                Delivery_Availability TEXT,
                SKU_Status VARCHAR(50),
                Product_url TEXT,
                crawl_datetime VARCHAR(50),
                calendar_week VARCHAR(10)
            )
        """)
        print("  [OK] bby_tv_bsr1 created")

        # 3. bby_tv_pmt1 테이블 생성 (bby_tv_promotion_crawl 구조 복사)
        print("\n[3/4] Creating bby_tv_pmt1 table...")
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS bby_tv_pmt1 (
                id SERIAL PRIMARY KEY,
                account_name VARCHAR(50),
                page_type VARCHAR(50),
                retailer_sku_name TEXT,
                promotion_rank INTEGER,
                final_sku_price VARCHAR(50),
                original_sku_price VARCHAR(50),
                offer VARCHAR(50),
                savings VARCHAR(50),
                promotion_type TEXT,
                product_url TEXT,
                crawl_datetime VARCHAR(50),
                calendar_week VARCHAR(10),
                batch_id VARCHAR(50)
            )
        """)
        print("  [OK] bby_tv_pmt1 created")

        # 4. bby_tv_crawl 테이블 생성 (bby_tv_detail_crawled 구조 복사)
        print("\n[4/4] Creating bby_tv_crawl table...")
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS bby_tv_crawl (
                id SERIAL PRIMARY KEY,
                account_name VARCHAR(50),
                batch_id VARCHAR(50),
                page_type VARCHAR(50),
                "order" INTEGER,
                retailer_sku_name TEXT,
                item VARCHAR(50),
                Estimated_Annual_Electricity_Use VARCHAR(50),
                screen_size VARCHAR(50),
                count_of_reviews VARCHAR(50),
                Count_of_Star_Ratings VARCHAR(50),
                Top_Mentions TEXT,
                Detailed_Review_Content TEXT,
                Recommendation_Intent TEXT,
                product_url TEXT,
                crawl_datetime VARCHAR(50),
                calendar_week VARCHAR(10),
                final_sku_price VARCHAR(50),
                savings VARCHAR(50),
                original_sku_price VARCHAR(50),
                offer VARCHAR(50),
                pick_up_availability TEXT,
                shipping_availability TEXT,
                delivery_availability TEXT,
                sku_status VARCHAR(50),
                star_rating VARCHAR(50),
                promotion_type TEXT,
                promotion_rank INTEGER,
                bsr_rank INTEGER,
                main_rank INTEGER,
                trend_rank INTEGER
            )
        """)
        print("  [OK] bby_tv_crawl created")

        # Commit
        conn.commit()
        cursor.close()
        conn.close()

        print("\n" + "="*80)
        print("[SUCCESS] All tables created successfully!")
        print("="*80)
        print("\nCreated tables:")
        print("  1. bby_tv_main1 (13 columns)")
        print("  2. bby_tv_bsr1 (13 columns)")
        print("  3. bby_tv_pmt1 (14 columns)")
        print("  4. bby_tv_crawl (30 columns)")

    except Exception as e:
        print(f"\n[ERROR] Failed to create tables: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    create_tables()
