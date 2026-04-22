"""
Amazon TV Crawler - Integrated Sequential Execution
Executes main1 -> bsr1 -> dt1 in sequence

If at least one of (main1, bsr1) succeeds, dt1 will run.
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
import psycopg2
from alert_monitor import send_tv_crawl_report
from amazon_config_loader import get_amazon_config
from config import DB_CONFIG

# Load config from DB
_config = get_amazon_config()
SCRIPT_TIMEOUT = _config.get_int('timing', 'script_timeout', 'amazon_tv_crawl', 39600)

RESULT_DIR = r"C:\samsung_dx_retail_com\stage_results"
INTERIM_REPORT_SECONDS = 21600  # 6 hours

STAGE_ORDER = ['amazon_tv_main1', 'amazon_tv_bsr1', 'amazon_tv_dt1']


def check_bsr_rank_continuity():
    """최근 amazon_tv_bsr 배치의 rank 연속성 확인 (read-only).

    returns (is_ok, count, max_rank, missing_ranks)
      - is_ok=True : ranks 1..max_rank 연속 (gap 없음) -> Amazon 노출 한도 자체가 그만큼
      - is_ok=False: gap 존재 또는 배치 없음 -> 실제 유실
    """
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cur = conn.cursor()
        cur.execute("""
            SELECT batch_id FROM public.amazon_tv_bsr
            WHERE batch_id IS NOT NULL
            ORDER BY batch_id DESC LIMIT 1
        """)
        row = cur.fetchone()
        if not row:
            cur.close(); conn.close()
            return (False, 0, 0, [])
        latest = row[0]
        cur.execute("""
            SELECT bsr_rank FROM public.amazon_tv_bsr
            WHERE batch_id=%s AND bsr_rank IS NOT NULL
            ORDER BY bsr_rank
        """, (latest,))
        ranks = [r[0] for r in cur.fetchall()]
        cur.close(); conn.close()
        if not ranks:
            return (False, 0, 0, [])
        count = len(ranks)
        max_r = max(ranks)
        missing = [i for i in range(1, max_r + 1) if i not in ranks]
        return (len(missing) == 0, count, max_r, missing)
    except Exception as e:
        print(f"[WARNING] BSR rank continuity check failed: {e}")
        return (False, 0, 0, [])


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
            timeout=SCRIPT_TIMEOUT
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
        print(f"\n[ERROR] {stage_name} timed out after {SCRIPT_TIMEOUT/3600:.0f} hours")
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
        print("Amazon TV Crawler - Integrated Sequential Execution")
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
            ("amazon_tv_dt1.py", "amazon_tv_dt1")
        ]

        MAIN1_MIN = 250  # main1 최소 수집 기준
        BSR_MIN = 100    # bsr 최소 수집 기준

        # Execute main1
        print_stage_header("amazon_tv_main1", 1, 3)
        result = run_crawler("amazon_tv_main1.py", "amazon_tv_main1")
        stage_results["amazon_tv_main1"] = result
        main1_collected = result.get("collected_count") or 0

        # main1 성공 판정: 250개 이상 수집
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

        # bsr 성공 판정: rank 연속성(gap 없음) 기준
        # - bsr_collected > 0 (이번 세션 실제 수집) AND DB ranks 연속 -> 성공
        # - gap 존재 또는 미수집 -> 실패 (실제 유실)
        is_ok, bsr_count_db, max_rank, missing = check_bsr_rank_continuity()
        if bsr_collected > 0 and is_ok:
            result["success"] = True
            print(f"\n[INFO] bsr1 성공: 세션 수집 {bsr_collected}개, DB {bsr_count_db}개 ranks 1..{max_rank} 연속 (gap 없음)")
        else:
            result["success"] = False
            failed_stages.append("amazon_tv_bsr1")
            if bsr_collected == 0:
                print(f"\n[INFO] bsr1 실패: 이번 세션 수집 0건 (stage_result 기준)")
            elif missing:
                print(f"\n[INFO] bsr1 실패: {bsr_count_db}개 수집, GAP 존재 - missing ranks: {missing}")
            else:
                print(f"\n[INFO] bsr1 실패: batch 없음 또는 DB 조회 오류")

        # dt1 실행: main/bsr 중 하나라도 수집했으면 실행
        if main1_collected == 0 and bsr_collected == 0:
            print(f"\n[WARNING] main1, bsr1 모두 0건 수집. dt1 실행 건너뜀.")
            stage_results["amazon_tv_dt1"] = {
                "success": None, "elapsed": 0, "timeout": False,
                "collected_count": None, "target_count": None
            }
        else:
            print(f"\n[INFO] Proceeding to detail crawler...")
            time.sleep(5)

            print_stage_header(stages[2][1], 3, 3)
            result = run_crawler(stages[2][0], stages[2][1])
            stage_results["amazon_tv_dt1"] = result

            # dt1 failed 판정: 200개 이하면 실패
            dt1_collected = result.get("collected_count") or 0
            if dt1_collected <= 200:
                if "amazon_tv_dt1" not in failed_stages:
                    failed_stages.append("amazon_tv_dt1")

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
