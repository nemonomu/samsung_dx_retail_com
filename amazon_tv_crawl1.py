"""
Amazon TV Crawler 1 - Integrated Sequential Execution with Account Switching
Executes main1 -> bsr1 -> dt2 in sequence

dt2 uses account switching: unsandev0002 (first 100) -> unsandev0003 (remaining)
If at least one of (main1, bsr1) succeeds, dt2 will run.
If any step fails, creates a failure log at C:\\samsung_dx_retail_com\\failed_amazon\\
Sends email notification on completion or failure.
"""

import subprocess
import sys
import time
import os
import json
import threading
from datetime import datetime
from alert_monitor import send_tv_crawl_report


RESULT_DIR = r"C:\samsung_dx_retail_com\stage_results"
STAGE_TIMEOUT = 39600  # 11 hours
INTERIM_REPORT_SECONDS = 21600  # 6 hours

STAGE_ORDER = ['amazon_tv_main1', 'amazon_tv_bsr1', 'amazon_tv_dt2']


def print_separator():
    """Print separator line"""
    print("=" * 80)


def print_stage_header(stage_name, stage_num, total_stages):
    """Print stage header"""
    print_separator()
    print(f"[STAGE {stage_num}/{total_stages}] {stage_name}")
    print_separator()


def read_stage_result(script_name):
    """Read stage result JSON file"""
    try:
        result_file = os.path.join(RESULT_DIR, script_name.replace('.py', '.json'))
        if os.path.exists(result_file):
            with open(result_file, 'r') as f:
                return json.load(f)
    except Exception as e:
        print(f"[WARNING] Failed to read result JSON for {script_name}: {e}")
    return None


def clean_stage_results():
    """Clean up previous stage result JSON files"""
    try:
        if os.path.exists(RESULT_DIR):
            for f in os.listdir(RESULT_DIR):
                if f.endswith('.json') and 'amazon_tv' in f:
                    os.remove(os.path.join(RESULT_DIR, f))
        else:
            os.makedirs(RESULT_DIR, exist_ok=True)
    except Exception as e:
        print(f"[WARNING] Failed to clean stage results: {e}")


def run_crawler(script_name, stage_name):
    """Run a crawler script and return structured result"""
    start_time = time.time()
    print(f"\n[INFO] Starting {stage_name}...")
    print(f"[INFO] Command: python {script_name}")
    print(f"[INFO] Start time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")

    result = {
        "success": False,
        "elapsed": 0,
        "timeout": False,
        "collected_count": None,
        "target_count": None
    }

    try:
        proc = subprocess.run(
            [sys.executable, '-u', script_name],
            stdout=None,
            stderr=None,
            text=True,
            encoding='utf-8',
            errors='replace',
            timeout=STAGE_TIMEOUT
        )

        result["elapsed"] = time.time() - start_time
        result["success"] = proc.returncode == 0

        if result["success"]:
            print(f"\n[OK] {stage_name} completed successfully")
        else:
            print(f"\n[ERROR] {stage_name} failed with return code {proc.returncode}")
        print(f"[INFO] Elapsed time: {result['elapsed']:.1f} seconds")

    except subprocess.TimeoutExpired:
        result["elapsed"] = time.time() - start_time
        result["timeout"] = True
        print(f"\n[ERROR] {stage_name} timed out after {STAGE_TIMEOUT/3600:.0f} hours")
        print(f"[INFO] Elapsed time: {result['elapsed']:.1f} seconds")
    except Exception as e:
        result["elapsed"] = time.time() - start_time
        print(f"\n[ERROR] {stage_name} failed with exception: {e}")
        print(f"[INFO] Elapsed time: {result['elapsed']:.1f} seconds")

    # Read result JSON from subprocess
    stage_data = read_stage_result(script_name)
    if stage_data:
        result["collected_count"] = stage_data.get("collected_count")
        result["target_count"] = stage_data.get("target_count")


    return result


def create_failure_log(failed_stages):
    """Create failure log file"""
    try:
        log_dir = r"C:\samsung_dx_retail_com\failed_amazon"
        os.makedirs(log_dir, exist_ok=True)

        timestamp = datetime.now().strftime("%Y%m%d%H%M")
        log_file = os.path.join(log_dir, f"{timestamp}.txt")

        with open(log_file, 'w', encoding='utf-8') as f:
            for stage in failed_stages:
                f.write(f"{stage}\n")

        print(f"\n[INFO] Failure log created: {log_file}")
        return log_file

    except Exception as e:
        print(f"\n[ERROR] Failed to create failure log: {e}")
        return None


def main():
    """Main execution function"""
    stage_results = {}
    failed_stages = []
    overall_start_time = time.time()
    interim_sent = False

    # 6시간 중간보고 타이머
    def send_interim():
        nonlocal interim_sent
        if not interim_sent:
            interim_sent = True
            print(f"\n[INFO] 6시간 경과 - 중간 보고 발송")
            try:
                send_tv_crawl_report('Amazon',
                    stage_results=stage_results,
                    failed_stages=failed_stages,
                    overall_elapsed=time.time() - overall_start_time,
                    stage_order=STAGE_ORDER,
                    is_interim=True
                )
            except Exception as e:
                print(f"[WARNING] 중간 보고 발송 실패: {e}")

    timer = threading.Timer(INTERIM_REPORT_SECONDS, send_interim)
    timer.daemon = True

    try:
        batch_id = datetime.now().strftime('%Y%m%d_%H%M%S')
        session_start_time = datetime.now().strftime('%Y%m%d%H%M')

        print_separator()
        print("Amazon TV Crawler 1 - Integrated Sequential Execution")
        print(f"Batch ID: {batch_id}")
        print(f"Session ID: {session_start_time}")
        print(f"Start time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print_separator()

        # 이전 결과 파일 정리
        clean_stage_results()

        # 6시간 타이머 시작
        timer.start()

        # Stage definitions
        stages = [
            ("amazon_tv_main1.py", "amazon_tv_main1"),
            ("amazon_tv_bsr1.py", "amazon_tv_bsr1"),
            ("amazon_tv_dt2.py", "amazon_tv_dt2")
        ]

        MAIN1_MIN = 300  # main1 최소 수집 기준
        BSR_MIN = 100    # bsr 최소 수집 기준

        # Execute main1
        print_stage_header("amazon_tv_main1", 1, 3)
        result = run_crawler("amazon_tv_main1.py", "amazon_tv_main1")
        stage_results["amazon_tv_main1"] = result
        main1_collected = result.get("collected_count") or 0

        # main1 성공 판정: 300개 이상 수집
        if main1_collected >= MAIN1_MIN:
            result["success"] = True
        else:
            result["success"] = False
            failed_stages.append("amazon_tv_main1")
        print(f"\n[INFO] main1 수집: {main1_collected}개 (기준: {MAIN1_MIN}개)")

        print(f"\n[INFO] Waiting 5 seconds for driver cleanup...")
        time.sleep(5)

        # Execute bsr1
        print_stage_header("amazon_tv_bsr1", 2, 3)
        result = run_crawler("amazon_tv_bsr1.py", "amazon_tv_bsr1")
        stage_results["amazon_tv_bsr1"] = result
        bsr_collected = result.get("collected_count") or 0

        # bsr 성공 판정: 100개 이상 수집
        if bsr_collected >= BSR_MIN:
            result["success"] = True
        else:
            result["success"] = False
            failed_stages.append("amazon_tv_bsr1")
        print(f"\n[INFO] bsr1 수집: {bsr_collected}개 (기준: {BSR_MIN}개)")

        # Check if at least one of main1/bsr1 succeeded
        main_stages_success = any([
            stage_results["amazon_tv_main1"]["success"],
            stage_results["amazon_tv_bsr1"]["success"]
        ])

        # Execute dt2 only if at least one main stage succeeded
        if main_stages_success:
            print(f"\n[INFO] At least one main stage succeeded. Proceeding to detail crawler...")
            time.sleep(5)

            print_stage_header(stages[2][1], 3, 3)
            result = run_crawler(stages[2][0], stages[2][1])
            stage_results["amazon_tv_dt2"] = result

            # dt2 failed 판정
            dt2_target = result.get("target_count") or 0
            dt2_collected = result.get("collected_count") or 0
            if not result["success"] or dt2_collected < dt2_target:
                if "amazon_tv_dt2" not in failed_stages:
                    failed_stages.append("amazon_tv_dt2")
        else:
            print(f"\n[WARNING] All main stages (main1, bsr1) failed. Skipping detail crawler.")
            stage_results["amazon_tv_dt2"] = {
                "success": None, "elapsed": 0, "timeout": False,
                "collected_count": None, "target_count": None
            }

        # 6시간 타이머 취소
        timer.cancel()

        # Create failure log if any stage failed
        if failed_stages:
            create_failure_log(failed_stages)

        # Print final summary
        overall_elapsed = time.time() - overall_start_time
        print_separator()
        print("EXECUTION SUMMARY")
        print_separator()

        for script, name in stages:
            sr = stage_results.get(name, {})
            status_str = "성공" if sr.get("success") is True else ("실패" if sr.get("success") is False else "미실행")
            print(f"  {name}: {status_str}")

        print(f"\nTotal elapsed: {overall_elapsed:.1f}s ({overall_elapsed/60:.1f}min)")
        print_separator()

        # 최종 리포트 발송
        send_tv_crawl_report('Amazon',
            stage_results=stage_results,
            failed_stages=failed_stages,
            overall_elapsed=overall_elapsed,
            stage_order=STAGE_ORDER
        )

        return 0 if not failed_stages else 1

    except KeyboardInterrupt:
        print("\n[!] Interrupted by user")
        timer.cancel()
        overall_elapsed = time.time() - overall_start_time
        send_tv_crawl_report('Amazon',
            stage_results=stage_results,
            failed_stages=['Interrupted by user'],
            overall_elapsed=overall_elapsed,
            stage_order=STAGE_ORDER,
            error_message='Crawler interrupted by user'
        )
        return 1

    except Exception as e:
        print(f"\n[!] Fatal error: {e}")
        import traceback
        traceback.print_exc()
        timer.cancel()
        overall_elapsed = time.time() - overall_start_time
        send_tv_crawl_report('Amazon',
            stage_results=stage_results,
            failed_stages=['Fatal error'],
            overall_elapsed=overall_elapsed,
            stage_order=STAGE_ORDER,
            error_message=str(e)
        )
        return 1


if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)
