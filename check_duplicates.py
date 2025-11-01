import psycopg2

# Import database configuration
from config import DB_CONFIG

def check_duplicates():
    """Check for duplicate products across pages"""
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()

        print("="*80)
        print("Checking for Duplicate Products Across Pages")
        print("="*80)

        # Find products that appear on multiple pages
        cursor.execute("""
            SELECT
                Retailer_SKU_Name,
                COUNT(*) as page_count,
                STRING_AGG(page_number::text, ', ' ORDER BY page_number) as pages
            FROM raw_data
            WHERE mall_name = 'Amazon'
            GROUP BY Retailer_SKU_Name
            HAVING COUNT(*) > 1
            ORDER BY page_count DESC
            LIMIT 20
        """)

        duplicates = cursor.fetchall()

        if duplicates:
            print(f"\nFound {len(duplicates)} products appearing on multiple pages:")
            print("-"*80)
            for sku, count, pages in duplicates:
                print(f"{sku[:60]}")
                print(f"  Appears on {count} pages: {pages}")
                print()
        else:
            print("\nNo duplicate products found!")

        # Total attempted inserts per page (including duplicates)
        print("\n" + "="*80)
        print("Analyzing Total Containers vs Collected Products")
        print("="*80)

        cursor.execute("""
            SELECT page_number, COUNT(*) as collected
            FROM raw_data
            WHERE mall_name = 'Amazon'
            GROUP BY page_number
            ORDER BY page_number
        """)

        page_data = cursor.fetchall()

        print("\nPage | Collected | Expected | Difference")
        print("-"*50)
        for page_num, collected in page_data:
            expected = 16
            diff = expected - collected
            status = "OK" if diff == 0 else f"Missing {diff}"
            print(f"{page_num:4d} | {collected:9d} | {expected:8d} | {status}")

        cursor.close()
        conn.close()

    except Exception as e:
        print(f"[ERROR] {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    check_duplicates()
    input("\nPress Enter to exit...")
