"""
Amazon TV Detail2 Crawler - Resume from specific index
Usage: python amazon_tv_dt2_resume.py [start_index] [batch_id]
Example: python amazon_tv_dt2_resume.py 148 20250122_143052
"""
import sys
from amazon_tv_dt2 import AmazonDetailCrawler
import random
import time


def run_from_index(start_idx=1, batch_id=None):
    """Run crawler starting from specific index with optional batch_id"""
    crawler = AmazonDetailCrawler()

    # Override batch_id if provided
    if batch_id:
        crawler.batch_id = batch_id
        print(f"[INFO] Using existing batch_id: {batch_id}")

    try:
        print("="*80)
        print(f"Amazon TV Detail2 Crawler - RESUME from index {start_idx}")
        print("="*80)

        # Step 1: Connect to database
        print("\n[STEP 1/5] Connecting to database...")
        if not crawler.connect_db():
            print("[ERROR] Failed to connect to database. Stopping.")
            return

        # Step 2: Load XPaths
        print("\n[STEP 2/5] Loading XPath selectors...")
        if not crawler.load_xpaths():
            print("[ERROR] Failed to load XPath selectors. Stopping.")
            return

        # Step 3: Load product URLs
        print("\n[STEP 3/5] Loading product URLs...")
        product_urls = crawler.load_product_urls()
        if not product_urls:
            print("[ERROR] No product URLs found. Stopping.")
            return

        # Step 4: Setup WebDriver
        print("\n[STEP 4/5] Setting up WebDriver...")
        crawler.setup_driver()
        print("[OK] WebDriver ready")

        # Step 5: Scrape each detail page - START FROM SPECIFIED INDEX
        print("\n[STEP 5/5] Starting to scrape detail pages...")
        print(f"[INFO] Total pages: {len(product_urls)}, Starting from: {start_idx}")

        # Skip already processed URLs
        remaining_urls = product_urls[start_idx - 1:]
        print(f"[INFO] Remaining pages to scrape: {len(remaining_urls)}")

        for idx, url_data in enumerate(remaining_urls, start_idx):
            # Check if we've reached the maximum SKU limit
            if crawler.total_collected >= crawler.max_skus:
                print(f"\n{'='*80}")
                print(f"[INFO] Reached maximum SKU limit ({crawler.max_skus})")
                print(f"[INFO] Stopping collection. Total collected: {crawler.total_collected}")
                break

            print(f"\n{'='*80}")
            print(f"Processing {idx}/{len(product_urls)}")

            crawler.scrape_detail_page(url_data)

            # Random delay between requests
            delay = random.uniform(2, 4)
            print(f"[INFO] Waiting {delay:.1f} seconds before next request...")
            time.sleep(delay)

        print("\n" + "="*80)
        print(f"Detail Crawling completed!")
        print(f"Total collected: {crawler.total_collected}")
        print("="*80)

    except KeyboardInterrupt:
        print("\n[INFO] Crawler stopped by user")
    except Exception as e:
        print(f"[ERROR] Crawler failed: {e}")
        import traceback
        traceback.print_exc()
    finally:
        if crawler.driver:
            crawler.driver.quit()
        if crawler.db_conn:
            crawler.db_conn.close()


def find_recent_batch_ids():
    """Find recent batch_ids from DB"""
    import psycopg2
    from config import DB_CONFIG

    conn = psycopg2.connect(**DB_CONFIG)
    cursor = conn.cursor()
    cursor.execute('''
        SELECT batch_id, COUNT(*) as cnt
        FROM amazon_tv_detail_crawled
        WHERE batch_id IS NOT NULL
        GROUP BY batch_id
        ORDER BY batch_id DESC
        LIMIT 10
    ''')
    results = cursor.fetchall()
    cursor.close()
    conn.close()
    return results


if __name__ == '__main__':
    start_index = 1
    batch_id = None

    if len(sys.argv) > 1:
        try:
            start_index = int(sys.argv[1])
        except ValueError:
            print(f"Invalid index: {sys.argv[1]}, using default: 1")

    if len(sys.argv) > 2:
        batch_id = sys.argv[2]

    print(f"Starting from index: {start_index}")

    if not batch_id:
        print("\n[INFO] No batch_id provided. Recent batch_ids:")
        print("-" * 45)
        recent = find_recent_batch_ids()
        for i, (bid, cnt) in enumerate(recent, 1):
            print(f"  {i}. {bid} | {cnt} rows")
        print("-" * 45)
        choice = input("Enter number to select, or paste batch_id (or 'n' for new): ")

        if choice.lower() == 'n':
            print("[INFO] Will create new batch_id")
        elif choice.isdigit() and 1 <= int(choice) <= len(recent):
            batch_id = recent[int(choice) - 1][0]
            print(f"[INFO] Selected: {batch_id}")
        elif len(choice) > 10:
            batch_id = choice
            print(f"[INFO] Using: {batch_id}")
        else:
            print("Invalid choice. Aborted.")
            sys.exit(0)

    if batch_id:
        print(f"Using batch_id: {batch_id}")

    run_from_index(start_index, batch_id)
