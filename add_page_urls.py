import psycopg2

# Import database configuration
from config import DB_CONFIG

def add_page_urls():
    """Add page URLs from url_pages.txt to database"""
    try:
        # Read URLs from file
        urls = []
        with open('url_pages.txt', 'r', encoding='utf-8') as f:
            for line in f:
                line = line.strip()
                if line:
                    parts = line.split(',', 1)
                    if len(parts) == 2:
                        page_num = int(parts[0])
                        url = parts[1]
                        urls.append((page_num, url))

        print(f"[INFO] Read {len(urls)} URLs from file")

        # Connect to database
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()

        # Insert URLs
        for page_num, url in urls:
            cursor.execute("""
                INSERT INTO page_urls (mall_name, page_number, url, updated_by)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (mall_name, page_number)
                DO UPDATE SET
                    url = EXCLUDED.url,
                    updated_at = CURRENT_TIMESTAMP,
                    updated_by = EXCLUDED.updated_by;
            """, ('Amazon', page_num, url, 'system'))
            print(f"[OK] Added page {page_num}")

        conn.commit()

        # Verify
        cursor.execute("""
            SELECT page_number, url
            FROM page_urls
            WHERE mall_name = 'Amazon'
            ORDER BY page_number;
        """)

        all_urls = cursor.fetchall()
        print(f"\n[INFO] Total pages in database: {len(all_urls)}")

        cursor.close()
        conn.close()

        print("\n" + "="*80)
        print("Successfully added all page URLs to database!")
        print("="*80)

        return True

    except Exception as e:
        print(f"[ERROR] Failed to add URLs: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    add_page_urls()
    input("\nPress Enter to exit...")
