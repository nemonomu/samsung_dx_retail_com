"""
Best Buy TV Integrated Crawler
통합 크롤러: main1 → bsr1 → pmt1 → dt1 순차 실행

실행 순서:
1. bby_tv_main1.py: Main page crawling → bby_tv_main1 table
2. bby_tv_bsr1.py: Best-selling page crawling → bby_tv_bsr1 table
3. bby_tv_pmt1.py: Promotion page crawling → bby_tv_pmt1 table
4. bby_tv_dt1.py: Detail page crawling → bby_tv_crawl + tv_retail_com tables
   - Uses URLs from: bby_tv_main1, bby_tv_bsr1, bby_tv_pmt1

저장 테이블:
- bby_tv_main1: Main listing data
- bby_tv_bsr1: Best-selling listing data
- bby_tv_pmt1: Promotion listing data
- bby_tv_crawl: Detail page data (copy structure from bby_tv_detail_crawled)
- tv_retail_com: Unified retail data
"""
import subprocess
import sys
import time
from datetime import datetime

class IntegratedCrawler:
    def __init__(self):
        self.batch_id = datetime.now().strftime('%Y%m%d_%H%M%S')
        self.results = {
            'main1': None,
            'bsr1': None,
            'pmt1': None,
            'dt1': None
        }

    def run_crawler(self, script_name, description):
        """Run a crawler script and return success status"""
        print("\n" + "="*80)
        print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Starting: {description}")
        print("="*80)

        try:
            result = subprocess.run(
                [sys.executable, script_name],
                capture_output=True,
                text=True,
                timeout=3600  # 1 hour timeout
            )

            if result.returncode == 0:
                print(f"[✓] {description} completed successfully")
                print(result.stdout)
                return True
            else:
                print(f"[✗] {description} failed with exit code {result.returncode}")
                print("STDOUT:", result.stdout)
                print("STDERR:", result.stderr)
                return False

        except subprocess.TimeoutExpired:
            print(f"[✗] {description} timed out after 1 hour")
            return False
        except Exception as e:
            print(f"[✗] {description} failed with error: {e}")
            return False

    def run(self):
        """Run all crawlers in sequence"""
        print("="*80)
        print(f"Best Buy TV Integrated Crawler")
        print(f"Batch ID: {self.batch_id}")
        print(f"Start Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("="*80)

        # Step 1: Main page crawler
        self.results['main1'] = self.run_crawler(
            'bby_tv_main1.py',
            'Main Page Crawler (bby_tv_main1.py)'
        )

        if not self.results['main1']:
            print("\n[WARNING] Main crawler failed, but continuing with other crawlers...")

        time.sleep(5)  # Brief pause between crawlers

        # Step 2: Best-selling page crawler
        self.results['bsr1'] = self.run_crawler(
            'bby_tv_bsr1.py',
            'Best-Selling Page Crawler (bby_tv_bsr1.py)'
        )

        if not self.results['bsr1']:
            print("\n[WARNING] BSR crawler failed, but continuing with other crawlers...")

        time.sleep(5)

        # Step 3: Promotion page crawler
        self.results['pmt1'] = self.run_crawler(
            'bby_tv_pmt1.py',
            'Promotion Page Crawler (bby_tv_pmt1.py)'
        )

        if not self.results['pmt1']:
            print("\n[WARNING] Promotion crawler failed, but continuing with detail crawler...")

        time.sleep(5)

        # Step 4: Detail page crawler (uses URLs from above)
        self.results['dt1'] = self.run_crawler(
            'bby_tv_dt1.py',
            'Detail Page Crawler (bby_tv_dt1.py)'
        )

        # Final summary
        print("\n" + "="*80)
        print("FINAL SUMMARY")
        print("="*80)
        print(f"Batch ID: {self.batch_id}")
        print(f"End Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("\nResults:")
        print(f"  Main Crawler (bby_tv_main1):      {'✓ SUCCESS' if self.results['main1'] else '✗ FAILED'}")
        print(f"  BSR Crawler (bby_tv_bsr1):        {'✓ SUCCESS' if self.results['bsr1'] else '✗ FAILED'}")
        print(f"  Promotion Crawler (bby_tv_pmt1):  {'✓ SUCCESS' if self.results['pmt1'] else '✗ FAILED'}")
        print(f"  Detail Crawler (bby_tv_dt1):      {'✓ SUCCESS' if self.results['dt1'] else '✗ FAILED'}")
        print("="*80)

        # Return overall success status
        return all(self.results.values())

def main():
    """Main execution"""
    try:
        crawler = IntegratedCrawler()
        success = crawler.run()

        if success:
            print("\n[✓] All crawlers completed successfully")
            sys.exit(0)
        else:
            print("\n[✗] Some crawlers failed - check logs above")
            sys.exit(1)

    except KeyboardInterrupt:
        print("\n[!] Interrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"\n[!] Fatal error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    main()
