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
import os
from datetime import datetime

class IntegratedCrawler:
    def __init__(self):
        self.batch_id = datetime.now().strftime('%Y%m%d_%H%M%S')
        self.session_start_time = datetime.now().strftime('%Y%m%d%H%M')  # YYYYMMDDHHMM
        self.overall_start_time = datetime.now()
        self.results = {
            'main1': {'success': None, 'duration': None},
            'bsr1': {'success': None, 'duration': None},
            'pmt1': {'success': None, 'duration': None},
            'dt1': {'success': None, 'duration': None}
        }

        # 환경변수 설정 (각 크롤러가 사용)
        os.environ['SESSION_START_TIME'] = self.session_start_time

    def run_crawler(self, script_name, description):
        """Run a crawler script and return success status with timing"""
        start_time = datetime.now()

        print("\n" + "="*80)
        print(f"Starting: {description}")
        print(f"Start Time: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
        print("="*80)

        try:
            result = subprocess.run(
                [sys.executable, script_name],
                capture_output=True,
                text=True,
                timeout=3600  # 1 hour timeout
            )

            end_time = datetime.now()
            duration = (end_time - start_time).total_seconds()

            if result.returncode == 0:
                print(result.stdout)
                print("\n" + "-"*80)
                print(f"[SUCCESS] {description}")
                print(f"End Time: {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
                print(f"Duration: {duration:.2f} seconds ({duration/60:.2f} minutes)")
                print("-"*80)
                return True, duration
            else:
                print(f"[FAILED] {description} - Exit code {result.returncode}")
                print("STDOUT:", result.stdout)
                print("STDERR:", result.stderr)
                print(f"Duration: {duration:.2f} seconds")
                return False, duration

        except subprocess.TimeoutExpired:
            print(f"[FAILED] {description} - Timed out after 1 hour")
            return False, 3600
        except Exception as e:
            print(f"[FAILED] {description} - Error: {e}")
            return False, 0

    def run(self):
        """Run all crawlers in sequence"""
        print("="*80)
        print(f"Best Buy TV Integrated Crawler")
        print(f"Batch ID: {self.batch_id}")
        print(f"Session ID: {self.session_start_time}")
        print(f"Overall Start Time: {self.overall_start_time.strftime('%Y-%m-%d %H:%M:%S')}")
        print("="*80)

        # Step 1: Main page crawler
        success, duration = self.run_crawler(
            'bby_tv_main1.py',
            'Main Page Crawler (bby_tv_main1.py)'
        )
        self.results['main1']['success'] = success
        self.results['main1']['duration'] = duration

        if not success:
            print("\n[WARNING] Main crawler failed, but continuing with other crawlers...")

        time.sleep(5)  # Brief pause between crawlers

        # Step 2: Best-selling page crawler
        success, duration = self.run_crawler(
            'bby_tv_bsr1.py',
            'Best-Selling Page Crawler (bby_tv_bsr1.py)'
        )
        self.results['bsr1']['success'] = success
        self.results['bsr1']['duration'] = duration

        if not success:
            print("\n[WARNING] BSR crawler failed, but continuing with other crawlers...")

        time.sleep(5)

        # Step 3: Promotion page crawler
        success, duration = self.run_crawler(
            'bby_tv_pmt1.py',
            'Promotion Page Crawler (bby_tv_pmt1.py)'
        )
        self.results['pmt1']['success'] = success
        self.results['pmt1']['duration'] = duration

        if not success:
            print("\n[WARNING] Promotion crawler failed, but continuing with detail crawler...")

        time.sleep(5)

        # Step 4: Detail page crawler (uses URLs from above)
        success, duration = self.run_crawler(
            'bby_tv_dt1.py',
            'Detail Page Crawler (bby_tv_dt1.py)'
        )
        self.results['dt1']['success'] = success
        self.results['dt1']['duration'] = duration

        # Calculate overall duration
        overall_end_time = datetime.now()
        total_duration = (overall_end_time - self.overall_start_time).total_seconds()

        # Final summary
        print("\n" + "="*80)
        print("FINAL SUMMARY")
        print("="*80)
        print(f"Batch ID: {self.batch_id}")
        print(f"Session ID: {self.session_start_time}")
        print(f"Overall Start Time: {self.overall_start_time.strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"Overall End Time: {overall_end_time.strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"Total Duration: {total_duration:.2f} seconds ({total_duration/60:.2f} minutes)")
        print("\nCrawler Results:")
        print(f"  Main Crawler (bby_tv_main1):      {'SUCCESS' if self.results['main1']['success'] else 'FAILED':8s} ({self.results['main1']['duration']:.2f}s)")
        print(f"  BSR Crawler (bby_tv_bsr1):        {'SUCCESS' if self.results['bsr1']['success'] else 'FAILED':8s} ({self.results['bsr1']['duration']:.2f}s)")
        print(f"  Promotion Crawler (bby_tv_pmt1):  {'SUCCESS' if self.results['pmt1']['success'] else 'FAILED':8s} ({self.results['pmt1']['duration']:.2f}s)")
        print(f"  Detail Crawler (bby_tv_dt1):      {'SUCCESS' if self.results['dt1']['success'] else 'FAILED':8s} ({self.results['dt1']['duration']:.2f}s)")
        print("="*80)

        # Return overall success status
        return all(r['success'] for r in self.results.values())

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
