import psycopg2

# Import database configuration
from config import DB_CONFIG

def create_walmart_detail_table():
    """Create Walmart_tv_detail_crawled table"""
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        conn.autocommit = True
        cursor = conn.cursor()

        print("="*80)
        print("Creating Walmart_tv_detail_crawled Table")
        print("="*80)

        # Create table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS Walmart_tv_detail_crawled (
                id SERIAL PRIMARY KEY,
                mother VARCHAR(10),
                "order" INTEGER,
                product_url TEXT,
                Retailer_SKU_Name TEXT,
                Star_Rating VARCHAR(50),
                Number_of_ppl_purchased_yesterday VARCHAR(100),
                Number_of_ppl_added_to_carts VARCHAR(100),
                SKU_Popularity VARCHAR(100),
                Savings VARCHAR(50),
                Discount_Type VARCHAR(50),
                Shipping_Info TEXT,
                Count_of_Star_Ratings VARCHAR(50),
                Detailed_Review_Content TEXT,
                crawl_at_local_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)

        print("[OK] Table 'Walmart_tv_detail_crawled' created successfully")

        # Check if table was created
        cursor.execute("""
            SELECT column_name, data_type
            FROM information_schema.columns
            WHERE table_name = 'walmart_tv_detail_crawled'
            ORDER BY ordinal_position
        """)

        columns = cursor.fetchall()
        print("\n[INFO] Table structure:")
        for col in columns:
            print(f"  - {col[0]}: {col[1]}")

        cursor.close()
        conn.close()

        print("\n" + "="*80)
        print("SUCCESS: Walmart_tv_detail_crawled table ready!")
        print("="*80)

    except Exception as e:
        print(f"\n[ERROR] {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    create_walmart_detail_table()
    print("\n[INFO] Script completed. Exiting...")
