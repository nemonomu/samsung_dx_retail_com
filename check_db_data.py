import psycopg2

# Database configuration
DB_CONFIG = {
    'host': 'samsung-dx-crawl.csnixzmkuppn.ap-northeast-2.rds.amazonaws.com',
    'port': 5432,
    'database': 'postgres',
    'user': 'postgres',
    'password': 'admin2025!'
}

def check_latest_data():
    """Check the latest crawled data from database"""
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()

        print("=" * 80)
        print("Checking Latest Data in Amazon_tv_detail_crawled")
        print("=" * 80)

        # Check data type first
        cursor.execute("""
            SELECT column_name, data_type
            FROM information_schema.columns
            WHERE table_name = 'amazon_tv_detail_crawled'
            AND column_name = 'count_of_star_ratings'
        """)
        col_info = cursor.fetchone()
        print(f"\n[INFO] Column type: {col_info[0]} = {col_info[1]}\n")

        # Get latest 10 records with explicit column selection
        cursor.execute("""
            SELECT
                id,
                mother,
                "order",
                retailer_sku_name,
                star_rating,
                count_of_star_ratings,
                length(count_of_star_ratings::text) as count_length,
                summarized_review_content,
                crawl_at_local_time
            FROM amazon_tv_detail_crawled
            ORDER BY id DESC
            LIMIT 10
        """)

        records = cursor.fetchall()

        if not records:
            print("\n[INFO] No data found")
        else:
            print(f"\n[INFO] Found {len(records)} latest records:\n")
            for idx, row in enumerate(records, 1):
                print(f"--- Record {idx} (ID: {row[0]}) ---")
                print(f"  Mother: {row[1]}")
                print(f"  Order: {row[2]}")
                print(f"  Product: {row[3][:50] if row[3] else 'N/A'}...")
                print(f"  Star Rating: {row[4]}")
                print(f"  Count of Star Ratings: '{row[5]}' (length: {row[6]})")
                print(f"  Count type: {type(row[5])}")
                print(f"  Review Summary: {row[7][:80] + '...' if row[7] and len(row[7]) > 80 else row[7] or 'N/A'}")
                print(f"  Crawled At: {row[8]}")
                print()

        cursor.close()
        conn.close()

        print("=" * 80)

    except Exception as e:
        print(f"\n[ERROR] Failed to check data: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    check_latest_data()
