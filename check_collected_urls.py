import psycopg2

# Import database configuration
from config import DB_CONFIG

def check_collected_urls():
    """Check collected product URLs"""
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()

        # Get first 5 products with their URLs
        cursor.execute("""
            SELECT
                Retailer_SKU_Name,
                Product_URL
            FROM collected_data
            WHERE mall_name = 'Amazon'
            ORDER BY page_number, id
            LIMIT 10
        """)

        results = cursor.fetchall()

        print("="*80)
        print("Collected Product URLs (First 10)")
        print("="*80)

        for idx, (name, url) in enumerate(results, 1):
            print(f"\n[{idx}] {name[:60]}")
            print(f"    URL: {url if url else '[NULL]'}")

        # Count how many have NULL URLs
        cursor.execute("""
            SELECT
                COUNT(*) as total,
                COUNT(Product_URL) as with_url,
                COUNT(*) - COUNT(Product_URL) as null_url
            FROM collected_data
            WHERE mall_name = 'Amazon'
        """)

        total, with_url, null_url = cursor.fetchone()

        print("\n" + "="*80)
        print("Summary:")
        print("="*80)
        print(f"Total products: {total}")
        print(f"With URL: {with_url}")
        print(f"NULL URL: {null_url}")

        cursor.close()
        conn.close()

    except Exception as e:
        print(f"[ERROR] {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    check_collected_urls()
    input("\nPress Enter to exit...")
