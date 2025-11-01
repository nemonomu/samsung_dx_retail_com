"""
Add Walmart BSR pages 2 and 3 to database
"""
import psycopg2

# Import database configuration
from config import DB_CONFIG

def add_pages():
    """Add page 2 and 3 URLs to wmart_tv_bsr_page_url table"""
    try:
        # Connect to database
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()

        print("[INFO] Connected to database")

        # Insert page 2 and 3
        urls_to_add = [
            (2, 'https://www.walmart.com/search?q=TV&sort=best_seller&page=2&affinityOverride=default', 'bsr', True),
            (3, 'https://www.walmart.com/search?q=TV&sort=best_seller&page=3&affinityOverride=default', 'bsr', True)
        ]

        for page_number, url, page_type, is_active in urls_to_add:
            cursor.execute("""
                INSERT INTO wmart_tv_bsr_page_url (page_number, url, page_type, is_active)
                VALUES (%s, %s, %s, %s)
            """, (page_number, url, page_type, is_active))

            print(f"[OK] Added page {page_number}: {url[:80]}...")

        conn.commit()
        print(f"\n[SUCCESS] Successfully added 2 pages to wmart_tv_bsr_page_url")

        # Verify
        cursor.execute("""
            SELECT page_number, url, page_type, is_active
            FROM wmart_tv_bsr_page_url
            ORDER BY page_number
        """)

        results = cursor.fetchall()
        print(f"\n[INFO] Current pages in wmart_tv_bsr_page_url:")
        for row in results:
            print(f"  Page {row[0]}: {row[1][:80]}... (type: {row[2]}, active: {row[3]})")

        cursor.close()
        conn.close()

    except Exception as e:
        print(f"[ERROR] Failed to add pages: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    add_pages()
