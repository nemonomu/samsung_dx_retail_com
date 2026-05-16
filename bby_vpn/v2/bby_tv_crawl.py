"""
Best Buy TV Integrated Crawler
통합 크롤러: main1 → bsr1 → pmt1 → trend_crawl → dt1 순차 실행

실행 순서:
1. bby_tv_main1.py: Main page crawling → bby_tv_main1_vpn_test.csv
2. bby_tv_bsr1.py: Best-selling page crawling → bby_tv_bsr1_vpn_test.csv
3. bby_tv_pmt1.py: Promotion page crawling → bby_tv_pmt1_vpn_test.csv
4. bby_tv_trend_crawl.py: Trending deals crawling → bby_tv_trend_crawl_vpn_test.csv
5. bby_tv_dt1.py: Detail page crawling → bby_tv_vpn_test.csv
   - Uses URLs from listing CSV files above.

VPN 테스트 모드:
- 수집 결과는 DB에 저장하지 않는다.
- listing CSV를 통해 URL과 rank/type 메타데이터를 dt1으로 전달한다.

Sends email notification on completion or failure.
"""
import subprocess
import sys
import time
import os
from datetime import datetime, timedelta

ROOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT_DIR not in sys.path:
    sys.path.insert(0, ROOT_DIR)

from alert_monitor import send_crawl_alert
from bby_config_loader import get_config

class IntegratedCrawler:
    def __init__(self, max_runtime_seconds=None):
        self.batch_id = datetime.now().strftime('%Y%m%d_%H%M%S')
        self.base_dir = os.path.dirname(os.path.abspath(__file__))
        self.session_start_time = datetime.now().strftime('%Y%m%d%H%M')  # YYYYMMDDHHMM
        self.overall_start_time = datetime.now()
        self.max_runtime_seconds = max_runtime_seconds
        self.stop_at = (
            self.overall_start_time + timedelta(seconds=max_runtime_seconds)
            if max_runtime_seconds
            else None
        )
        self.results = {
            'main1': {'success': None, 'duration': None},
            'bsr1': {'success': None, 'duration': None},
            'pmt1': {'success': None, 'duration': None},
            'trend': {'success': None, 'duration': None},
            'dt1': {'success': None, 'duration': None}
        }

        # Config loader 초기화
        self.config = get_config()
        self.file_name = 'bby_tv_crawl'
        self.retailer_name = self.config.get_constant('retailer_name', self.file_name) or 'BestBuy'

        # 환경변수 설정 (각 크롤러가 사용)
        os.environ['SESSION_START_TIME'] = self.session_start_time

    def _remaining_seconds(self):
        if not self.stop_at:
            return None
        remaining = (self.stop_at - datetime.now()).total_seconds()
        return max(0, int(remaining))

    def _time_limit_reached(self):
        return self.stop_at is not None and datetime.now() >= self.stop_at

    def _sleep_between_stages(self, seconds):
        remaining = self._remaining_seconds()
        if remaining is None:
            time.sleep(seconds)
        elif remaining > 0:
            time.sleep(min(seconds, remaining))

    def run_crawler(self, script_name, description):
        """Run a crawler script and return success status with timing"""
        start_time = datetime.now()

        print("\n" + "="*80)
        print(f"Starting: {description}")
        print(f"Start Time: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
        print("="*80)

        try:
            # Config에서 timeout 가져오기
            configured_timeout = self.config.get_int('timing', 'subprocess_timeout', self.file_name, 21600)
            remaining_seconds = self._remaining_seconds()
            if remaining_seconds is not None:
                if remaining_seconds <= 0:
                    print(f"[INFO] Max runtime reached before starting {description}. Skipping.")
                    return False, 0
                subprocess_timeout = min(configured_timeout, remaining_seconds)
                print(f"[INFO] Max runtime stop time: {self.stop_at.strftime('%Y-%m-%d %H:%M:%S')}")
                print(f"[INFO] Remaining runtime for this stage: {subprocess_timeout} seconds")
            else:
                subprocess_timeout = configured_timeout

            script_path = os.path.join(self.base_dir, script_name)
            command = [sys.executable, '-u', script_path]
            child_env = os.environ.copy()
            child_env['PYTHONPATH'] = (
                ROOT_DIR + os.pathsep + child_env['PYTHONPATH']
                if child_env.get('PYTHONPATH')
                else ROOT_DIR
            )
            if script_name == 'bby_tv_dt1.py' and self.stop_at:
                command.extend(['until', self.stop_at.strftime('%Y%m%d%H%M%S')])
            if script_name == 'bby_tv_dt1.py':
                # V2 default: browser-minimized, API/GraphQL discovery first, defer heavy actions.
                child_env.setdefault('BBY_BROWSER_MIN_MODE', '1')
                child_env.setdefault('BBY_DT_CORE_ONLY', '1')
                child_env.setdefault('BBY_DT_SKIP_REVIEWS', '1')
                child_env.setdefault('BBY_DT_SKIP_SIMILAR', '1')
                child_env.setdefault('BBY_DT_DISCOVERY_REFRESH_EVERY', '4')
                child_env.setdefault('BBY_DT_RESTART_EVERY', '8')
                child_env.setdefault('BBY_DT_COOLDOWN_EVERY', '8')
                child_env.setdefault('BBY_DT_COOLDOWN_MIN', '180')
                child_env.setdefault('BBY_DT_COOLDOWN_MAX', '600')
                print("[INFO] DT V2 API-first/browser-minimized defaults:")
                print(f"       BBY_BROWSER_MIN_MODE={child_env.get('BBY_BROWSER_MIN_MODE')}")
                print(f"       BBY_DT_CORE_ONLY={child_env.get('BBY_DT_CORE_ONLY')}")
                print(f"       BBY_DT_SKIP_REVIEWS={child_env.get('BBY_DT_SKIP_REVIEWS')}")
                print(f"       BBY_DT_SKIP_SIMILAR={child_env.get('BBY_DT_SKIP_SIMILAR')}")
                print(f"       BBY_DT_DISCOVERY_REFRESH_EVERY={child_env.get('BBY_DT_DISCOVERY_REFRESH_EVERY')}")
                print(f"       BBY_DT_RESTART_EVERY={child_env.get('BBY_DT_RESTART_EVERY')}")
                print(f"       BBY_DT_COOLDOWN_EVERY={child_env.get('BBY_DT_COOLDOWN_EVERY')}")
                print(f"       BBY_DT_COOLDOWN_MIN={child_env.get('BBY_DT_COOLDOWN_MIN')}")
                print(f"       BBY_DT_COOLDOWN_MAX={child_env.get('BBY_DT_COOLDOWN_MAX')}")

            # Run with real-time output (no buffering)
            result = subprocess.run(
                command,  # -u: unbuffered output
                stdout=None,  # Inherit parent's stdout for real-time output
                stderr=None,  # Inherit parent's stderr
                text=True,
                timeout=subprocess_timeout,
                env=child_env,
                cwd=self.base_dir
            )

            end_time = datetime.now()
            duration = (end_time - start_time).total_seconds()

            if result.returncode == 0:
                print("\n" + "-"*80)
                print(f"[SUCCESS] {description}")
                print(f"End Time: {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
                print(f"Duration: {duration:.2f} seconds ({duration/60:.2f} minutes)")
                print("-"*80)
                return True, duration
            else:
                print(f"\n[FAILED] {description} - Exit code {result.returncode}")
                print(f"Duration: {duration:.2f} seconds")
                return False, duration

        except subprocess.TimeoutExpired:
            print(f"[FAILED] {description} - Timed out after {subprocess_timeout} seconds")
            return False, subprocess_timeout
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
        if self.stop_at:
            print(f"Max Runtime: {self.max_runtime_seconds} seconds ({self.max_runtime_seconds / 3600:.2f} hours)")
            print(f"Stop At: {self.stop_at.strftime('%Y-%m-%d %H:%M:%S')}")
        print("="*80)

        # Config에서 크롤러 간 대기 시간 가져오기
        between_crawlers_wait = self.config.get_int('timing', 'between_crawlers_wait', self.file_name, 5)

        # Step 1: Main page crawler
        success, duration = self.run_crawler(
            'bby_tv_main1.py',
            'Main Page Crawler (bby_tv_main1.py)'
        )
        self.results['main1']['success'] = success
        self.results['main1']['duration'] = duration

        if not success:
            print("\n[WARNING] Main crawler failed, but continuing with other crawlers...")

        if self._time_limit_reached():
            print("\n[INFO] Max runtime reached after main crawler. Stopping sequence.")
            return False
        self._sleep_between_stages(between_crawlers_wait)  # Brief pause between crawlers

        # Step 2: Best-selling page crawler
        success, duration = self.run_crawler(
            'bby_tv_bsr1.py',
            'Best-Selling Page Crawler (bby_tv_bsr1.py)'
        )
        self.results['bsr1']['success'] = success
        self.results['bsr1']['duration'] = duration

        if not success:
            print("\n[WARNING] BSR crawler failed, but continuing with other crawlers...")

        if self._time_limit_reached():
            print("\n[INFO] Max runtime reached after BSR crawler. Stopping sequence.")
            return False
        self._sleep_between_stages(between_crawlers_wait)

        # Step 3: Promotion page crawler
        success, duration = self.run_crawler(
            'bby_tv_pmt1.py',
            'Promotion Page Crawler (bby_tv_pmt1.py)'
        )
        self.results['pmt1']['success'] = success
        self.results['pmt1']['duration'] = duration

        if not success:
            print("\n[WARNING] Promotion crawler failed, but continuing with trend crawler...")

        if self._time_limit_reached():
            print("\n[INFO] Max runtime reached after promotion crawler. Stopping sequence.")
            return False
        self._sleep_between_stages(between_crawlers_wait)

        # Step 4: Trending deals crawler
        success, duration = self.run_crawler(
            'bby_tv_trend_crawl.py',
            'Trending Deals Crawler (bby_tv_trend_crawl.py)'
        )
        self.results['trend']['success'] = success
        self.results['trend']['duration'] = duration

        if not success:
            print("\n[WARNING] Trend crawler failed, but continuing with detail crawler...")

        if self._time_limit_reached():
            print("\n[INFO] Max runtime reached after trend crawler. Stopping sequence.")
            return False
        self._sleep_between_stages(between_crawlers_wait)

        # Step 5: Detail page crawler (uses URLs from above)
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
        print(f"  Trend Crawler (bby_tv_trend):     {'SUCCESS' if self.results['trend']['success'] else 'FAILED':8s} ({self.results['trend']['duration']:.2f}s)")
        print(f"  Detail Crawler (bby_tv_dt1):      {'SUCCESS' if self.results['dt1']['success'] else 'FAILED':8s} ({self.results['dt1']['duration']:.2f}s)")
        print("="*80)

        # Return overall success status
        return all(r['success'] for r in self.results.values())

def parse_runtime_limit(args):
    """Parse runtime limit args: 6 hours, 6 hour, 6h, --max-hours 6."""
    if not args:
        return None

    normalized = [arg.strip().lower() for arg in args if arg.strip()]
    if not normalized:
        return None

    try:
        if normalized[0] in ('--max-hours', '--max-runtime-hours', '--hours') and len(normalized) >= 2:
            return int(float(normalized[1]) * 3600)
        if normalized[0].endswith('h') and normalized[0][:-1]:
            return int(float(normalized[0][:-1]) * 3600)
        if len(normalized) >= 2 and normalized[1] in ('hour', 'hours', 'hr', 'hrs', 'h'):
            return int(float(normalized[0]) * 3600)
    except ValueError:
        print(f"[WARNING] Invalid max runtime arguments: {' '.join(args)}")

    print("[INFO] No max runtime limit applied. Usage: python bby_tv_crawl.py 6 hours")
    return None

def main():
    """Main execution"""
    crawler = None
    try:
        max_runtime_seconds = parse_runtime_limit(sys.argv[1:])
        crawler = IntegratedCrawler(max_runtime_seconds=max_runtime_seconds)
        success = crawler.run()

        # Calculate total duration
        total_duration = (datetime.now() - crawler.overall_start_time).total_seconds()

        # Collect failed stages
        failed_stages = [name for name, result in crawler.results.items() if not result['success']]

        # Email notification disabled during VPN test.
        print("[INFO] Email alert disabled for VPN test")
        # send_crawl_alert(
        #     retailer=crawler.retailer_name,
        #     results=crawler.results,
        #     failed_stages=failed_stages,
        #     elapsed_time=total_duration
        # )

        if success:
            print("\n[✓] All crawlers completed successfully")
            sys.exit(0)
        else:
            print("\n[✗] Some crawlers failed - check logs above")
            sys.exit(1)

    except KeyboardInterrupt:
        print("\n[!] Interrupted by user")
        print("[INFO] Email alert disabled for VPN test")
        # send_crawl_alert(
        #     retailer=crawler.retailer_name if crawler else 'BestBuy',
        #     results=crawler.results if crawler else {},
        #     failed_stages=['Interrupted by user'],
        #     elapsed_time=0,
        #     error_message='Crawler interrupted by user'
        # )
        sys.exit(1)
    except Exception as e:
        print(f"\n[!] Fatal error: {e}")
        import traceback
        traceback.print_exc()
        print("[INFO] Email alert disabled for VPN test")
        # send_crawl_alert(
        #     retailer=crawler.retailer_name if crawler else 'BestBuy',
        #     results=crawler.results if crawler else {},
        #     failed_stages=['Fatal error'],
        #     elapsed_time=0,
        #     error_message=str(e)
        # )
        sys.exit(1)

if __name__ == "__main__":
    main()
