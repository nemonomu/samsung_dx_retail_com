import psycopg2

# Import database configuration
from config import DB_CONFIG

def check_status():
    """Check collection status by page"""
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()

        # Total count
        cursor.execute("""
            SELECT COUNT(*) FROM raw_data WHERE mall_name = 'Amazon'
        """)
        total_raw = cursor.fetchone()[0]

        cursor.execute("""
            SELECT COUNT(*) FROM Amazon_tv_main_crawled WHERE mall_name = 'Amazon'
        """)
        total_main = cursor.fetchone()[0]

        print("="*80)
        print("Collection Status Summary")
        print("="*80)
        print(f"Total in raw_data: {total_raw}")
        print(f"Total in Amazon_tv_main_crawled: {total_main}")

        # Count by page
        print("\n" + "="*80)
        print("Products Collected by Page:")
        print("="*80)
        cursor.execute("""
            SELECT page_number, COUNT(*) as count
            FROM raw_data
            WHERE mall_name = 'Amazon'
            GROUP BY page_number
            ORDER BY page_number
        """)

        page_results = cursor.fetchall()
        for page_num, count in page_results:
            print(f"Page {page_num:2d}: {count:2d} products")

        # Check which pages are missing
        print("\n" + "="*80)
        print("Page URL Status:")
        print("="*80)
        cursor.execute("""
            SELECT page_number, url
            FROM page_urls
            WHERE mall_name = 'Amazon' AND is_active = TRUE
            ORDER BY page_number
        """)

        all_pages = cursor.fetchall()
        collected_pages = [p[0] for p in page_results]

        print(f"Total pages configured: {len(all_pages)}")
        print(f"Pages with data: {len(collected_pages)}")

        missing_pages = [p[0] for p in all_pages if p[0] not in collected_pages]
        if missing_pages:
            print(f"\nPages NOT crawled yet: {missing_pages}")
        else:
            print("\nAll configured pages have been crawled!")

        # Expected vs Actual
        print("\n" + "="*80)
        print("Analysis:")
        print("="*80)
        expected_per_page = 16
        pages_crawled = len(collected_pages)
        expected_total = min(300, pages_crawled * expected_per_page)

        print(f"Pages crawled: {pages_crawled}")
        print(f"Expected (16 per page): {expected_total}")
        print(f"Actual collected: {total_main}")
        print(f"Difference: {expected_total - total_main}")

        if total_main < expected_total:
            print("\nPossible reasons for fewer products:")
            print("- Some pages had fewer than 16 valid products")
            print("- Products were filtered out (ads, widgets, etc.)")
            print("- Duplicate products across pages")

        cursor.close()
        conn.close()

    except Exception as e:
        print(f"[ERROR] {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    check_status()
    input("\nPress Enter to exit...")
