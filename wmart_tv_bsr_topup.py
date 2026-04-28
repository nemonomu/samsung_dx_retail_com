"""
기존 BSR 배치 부족분 채우기 (page 4, 5 만 추가 크롤).
사용 예: python wmart_tv_bsr_topup.py 20260428_131328 96
"""
import sys
import time
import random

from wmart_tv_bsr import WalmartTVBSRCrawler

PAGES_TO_RUN = (4, 5)


def main():
    if len(sys.argv) < 3:
        print("Usage: python wmart_tv_bsr_topup.py <existing_batch_id> <current_total>")
        return 1

    existing_batch_id = sys.argv[1]
    current_total = int(sys.argv[2])

    crawler = WalmartTVBSRCrawler()
    crawler.batch_id = existing_batch_id
    crawler.total_collected = current_total
    crawler.sequential_id = current_total + 1

    print("=" * 80)
    print(f"BSR Top-up: batch_id={crawler.batch_id}")
    print(f"  start total={crawler.total_collected}, seq={crawler.sequential_id}, target={crawler.max_skus}")
    print(f"  pages to run: {PAGES_TO_RUN}")
    print("=" * 80)

    if not crawler.connect_db():
        return 1
    if not crawler.load_xpaths():
        return 1
    crawler.load_excluded_urls()

    page_urls_all = crawler.load_page_urls()
    page_urls = [(p, u) for p, u in page_urls_all if p in PAGES_TO_RUN]
    print(f"[OK] Filtered page URLs: {len(page_urls)} (pages {[p for p, _ in page_urls]})")
    if not page_urls:
        print(f"[ERROR] No active page URLs found for pages {PAGES_TO_RUN}")
        return 1

    crawler.setup_driver()
    try:
        if not crawler.initialize_session():
            print("[WARNING] Session init had issues, continuing anyway")

        between = crawler.config.get_timing_range("between_pages", "wmart_tv_bsr") or (5, 8)

        for page_number, url in page_urls:
            if crawler.total_collected >= crawler.max_skus:
                print(f"[INFO] Already at max ({crawler.max_skus}), stopping")
                break
            if not crawler.scrape_page(url, page_number):
                break
            time.sleep(random.uniform(*between))

        print("\n" + "=" * 80)
        print(f"Top-up completed! Total collected: {crawler.total_collected} SKUs")
        print("=" * 80)
    finally:
        try:
            crawler.page.quit()
        except Exception:
            pass
        try:
            crawler.db_conn.close()
        except Exception:
            pass

    return 0


if __name__ == "__main__":
    sys.exit(main())
