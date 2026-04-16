"""
Walmart TV Crawler - Integrated Sequential Execution
Executes main1 -> main2 -> bsr -> dt1 in sequence

If at least one of (main1, main2, bsr) succeeds, dt1 will run.
If any step fails, creates a failure log at C:\\samsung_dx_retail_com\\failed_wmart\\
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


def print_separator():
    """Print separator line"""
    print("=" * 80)


def print_stage_header(stage_name, stage_num, total_stages):
    """Print stage header"""
    print_separator()
    print(f"[STAGE {stage_num}/{total_stages}] {stage_name}")
    print_separator()


def read_stage_result(script_name):
    """
    Read stage result JSON file

    Returns:
        dict: {"collected_count": N, "target_count": M (dt1 only)} or None
    """
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
                if f.endswith('.json') and 'wmart_tv' in f:
                    os.remove(os.path.join(RESULT_DIR, f))
        else:
            os.makedirs(RESULT_DIR, exist_ok=True)
    except Exception as e:
        print(f"[WARNING] Failed to clean stage results: {e}")


def run_crawler(script_name, stage_name, extra_args=None):
    """
    Run a crawler script and return structured result

    Args:
        extra_args: 추가 커맨드라인 인자 리스트 (선택)

    Returns:
        dict: {"success": bool, "elapsed": float, "timeout": bool,
               "collected_count": int or None, "target_count": int or None}
    """
    cmd = [sys.executable, '-u', script_name] + (extra_args or [])
    start_time = time.time()
    print(f"\n[INFO] Starting {stage_name}...")
    print(f"[INFO] Command: {' '.join(cmd)}")
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
            cmd,
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

        # returncode 비정상이라도 수집 결과가 있으면 성공으로 재판정
        if not result["success"] and result["collected_count"] and result["collected_count"] > 0:
            print(f"[INFO] {stage_name}: returncode 비정상이지만 {result['collected_count']}개 수집됨 → 성공 처리")
            result["success"] = True

    return result


def create_failure_log(failed_stages):
    """Create failure log file"""
    try:
        log_dir = r"C:\samsung_dx_retail_com\failed_wmart"
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
                send_tv_crawl_report('Walmart',
                    stage_results=stage_results,
                    failed_stages=failed_stages,
                    overall_elapsed=time.time() - overall_start_time,
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
        print("Walmart TV Crawler - Integrated Sequential Execution")
        print(f"Batch ID: {batch_id}")
        print(f"Session ID: {session_start_time}")
        print(f"Start time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print_separator()

        # 이전 결과 파일 정리
        clean_stage_results()

        # 6시간 타이머 시작
        timer.start()

        MAIN_TOTAL_LIMIT = 300  # main1+main2 합산 수집 제한

        # Stage definitions
        stages = [
            ("wmart_tv_main1.py", "wmart_tv_main1"),
            ("wmart_tv_main2.py", "wmart_tv_main2"),
            ("wmart_tv_bsr.py", "wmart_tv_bsr"),
            ("wmart_tv_dt1.py", "wmart_tv_dt1")
        ]

        # Execute main1
        print_stage_header("wmart_tv_main1", 1, 4)
        result = run_crawler("wmart_tv_main1.py", "wmart_tv_main1")
        stage_results["wmart_tv_main1"] = result
        if not result["success"]:
            failed_stages.append("wmart_tv_main1")

        main1_collected = result.get("collected_count") or 0
        remaining = max(0, MAIN_TOTAL_LIMIT - main1_collected)
        print(f"\n[INFO] main1 수집: {main1_collected}개, main2 수집 제한: {remaining}개")

        print(f"\n[INFO] Waiting 5 seconds for driver cleanup...")
        time.sleep(5)

        # Execute main2 (남은 개수만큼만 수집)
        print_stage_header("wmart_tv_main2", 2, 4)
        result = run_crawler("wmart_tv_main2.py", "wmart_tv_main2",
                            extra_args=['--max-skus', str(remaining)])
        stage_results["wmart_tv_main2"] = result
        if not result["success"]:
            failed_stages.append("wmart_tv_main2")

        print(f"\n[INFO] Waiting 5 seconds for driver cleanup...")
        time.sleep(5)

        # Execute bsr
        print_stage_header("wmart_tv_bsr", 3, 4)
        result = run_crawler("wmart_tv_bsr.py", "wmart_tv_bsr")
        stage_results["wmart_tv_bsr"] = result
        if not result["success"]:
            failed_stages.append("wmart_tv_bsr")

        # main1+main2 합산 체크
        main2_collected = (stage_results.get("wmart_tv_main2", {}).get("collected_count") or 0)
        bsr_collected = (stage_results.get("wmart_tv_bsr", {}).get("collected_count") or 0)

        main_total = main1_collected + main2_collected
        print(f"\n[INFO] main1+main2 합산: {main_total} url, bsr: {bsr_collected} url")

        # Check if at least one of main1/main2/bsr succeeded
        main_stages_success = any([
            stage_results["wmart_tv_main1"]["success"],
            stage_results["wmart_tv_main2"]["success"],
            stage_results["wmart_tv_bsr"]["success"]
        ])

        # Execute dt1 only if at least one main stage succeeded
        if main_stages_success:
            print(f"\n[INFO] At least one main stage succeeded. Proceeding to detail crawler...")
            time.sleep(5)

            print_stage_header(stages[3][1], 4, 4)
            result = run_crawler(stages[3][0], stages[3][1])
            stage_results["wmart_tv_dt1"] = result

            # dt1 failed 판정: collected < target 또는 프로세스 실패
            dt1_target = result.get("target_count") or 0
            dt1_collected = result.get("collected_count") or 0
            if not result["success"] or dt1_collected < dt1_target:
                if "wmart_tv_dt1" not in failed_stages:
                    failed_stages.append("wmart_tv_dt1")
        else:
            print(f"\n[WARNING] All main stages (main1, main2, bsr) failed. Skipping detail crawler.")
            stage_results["wmart_tv_dt1"] = {
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
        print(f"{'Stage':<25} {'Status':<10} {'Collected':<20} {'Elapsed':<15}")
        print("-" * 80)

        for script, name in stages:
            sr = stage_results.get(name, {})
            success = sr.get("success")
            if success is True:
                status_str = "성공"
            elif success is False:
                status_str = "실패"
            else:
                status_str = "미실행"

            collected = sr.get("collected_count")
            target = sr.get("target_count")
            if collected is not None and target is not None:
                collected_str = f"{collected} / {target} sku"
            elif collected is not None:
                collected_str = f"{collected} url"
            else:
                collected_str = "-"

            elapsed = sr.get("elapsed", 0)
            if sr.get("timeout"):
                elapsed_str = "타임아웃"
            else:
                elapsed_str = f"{elapsed:.1f}s"

            print(f"{name:<25} {status_str:<10} {collected_str:<20} {elapsed_str:<15}")

        print("-" * 80)
        print(f"Total elapsed time: {overall_elapsed:.1f} seconds ({overall_elapsed/60:.1f} minutes)")
        print(f"End time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

        if failed_stages:
            print(f"\n[WARNING] {len(failed_stages)} stage(s) failed: {', '.join(failed_stages)}")
        else:
            print(f"\n[OK] All executed stages completed successfully!")

        print_separator()

        # 최종 리포트 발송
        send_tv_crawl_report('Walmart',
            stage_results=stage_results,
            failed_stages=failed_stages,
            overall_elapsed=overall_elapsed,
            is_interim=False
        )

        return 0 if not failed_stages else 1

    except KeyboardInterrupt:
        print("\n[!] Interrupted by user")
        timer.cancel()
        overall_elapsed = time.time() - overall_start_time
        send_tv_crawl_report('Walmart',
            stage_results=stage_results,
            failed_stages=['Interrupted by user'],
            overall_elapsed=overall_elapsed,
            is_interim=False,
            error_message='Crawler interrupted by user'
        )
        return 1

    except Exception as e:
        print(f"\n[!] Fatal error: {e}")
        import traceback
        traceback.print_exc()
        timer.cancel()
        overall_elapsed = time.time() - overall_start_time
        send_tv_crawl_report('Walmart',
            stage_results=stage_results,
            failed_stages=['Fatal error'],
            overall_elapsed=overall_elapsed,
            is_interim=False,
            error_message=str(e)
        )
        return 1


if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)
