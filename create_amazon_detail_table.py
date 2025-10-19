import psycopg2

DB_CONFIG = {
    'host': 'samsung-dx-crawl.csnixzmkuppn.ap-northeast-2.rds.amazonaws.com',
    'port': 5432,
    'database': 'postgres',
    'user': 'postgres',
    'password': 'admin2025!'
}

def create_amazon_detail_table():
    """Create Amazon_tv_detail_crawled table"""
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        conn.autocommit = True
        cursor = conn.cursor()

        print("="*80)
        print("Creating Amazon_tv_detail_crawled Table")
        print("="*80)

        # Create table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS Amazon_tv_detail_crawled (
                id SERIAL PRIMARY KEY,
                mother VARCHAR(10),
                "order" INTEGER,
                product_url TEXT,
                Retailer_SKU_Name TEXT,
                Star_Rating VARCHAR(50),
                SKU_Popularity VARCHAR(100),
                Retailer_Membership_Discounts TEXT,
                Samsung_SKU_Name VARCHAR(100),
                Rank_1 VARCHAR(200),
                Rank_2 VARCHAR(200),
                Count_of_Star_Ratings INTEGER,
                Summarized_Review_Content TEXT,
                Detailed_Review_Content TEXT,
                crawl_at_local_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)

        print("[OK] Table 'Amazon_tv_detail_crawled' created successfully")

        # Check if table was created
        cursor.execute("""
            SELECT column_name, data_type
            FROM information_schema.columns
            WHERE table_name = 'amazon_tv_detail_crawled'
            ORDER BY ordinal_position
        """)

        columns = cursor.fetchall()
        print("\n[INFO] Table structure:")
        for col in columns:
            print(f"  - {col[0]}: {col[1]}")

        cursor.close()
        conn.close()

        print("\n" + "="*80)
        print("SUCCESS: Amazon_tv_detail_crawled table ready!")
        print("="*80)

    except Exception as e:
        print(f"\n[ERROR] {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    create_amazon_detail_table()
    print("\n[INFO] Script completed. Exiting...")
