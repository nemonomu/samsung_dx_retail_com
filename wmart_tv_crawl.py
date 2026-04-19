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
import psycopg2
from alert_monitor import send_tv_crawl_report
from config import DB_CONFIG


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


def get_missing_bsr_ranks(session_start_str):
    """현재 세션 tv_retail_com 에서 누락된 bsr_rank (1~100) 반환
    Args:
        session_start_str: 세션 시작 시각 (crawl_datetime varchar 비교용)
    Returns:
        list[int]: 누락 bsr_rank 리스트 (빈 리스트면 완비). DB 에러 시 None.
    """
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cur = conn.cursor()
        cur.execute("""
            SELECT DISTINCT bsr_rank
            FROM tv_retail_com
            WHERE account_name = 'Walmart'
              AND crawl_datetime >= %s
              AND bsr_rank IS NOT NULL
        """, (session_start_str,))
        found = set(r[0] for r in cur.fetchall())
        cur.close()
        conn.close()
        return sorted(set(range(1, 101)) - found)
    except Exception as e:
        print(f"[ERROR] BSR completeness 쿼리 실패: {e}")
        return None


def get_main_dedup_count():
    """main1 + main2 최신 batch의 중복 제거 URL 개수 조회"""
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cur = conn.cursor()

        cur.execute("SELECT batch_id FROM wmart_tv_main_1 WHERE batch_id IS NOT NULL ORDER BY batch_id DESC LIMIT 1")
        m1 = cur.fetchone()
        cur.execute("SELECT batch_id FROM wmart_tv_main_2 WHERE batch_id IS NOT NULL ORDER BY batch_id DESC LIMIT 1")
        m2 = cur.fetchone()

        parts = []
        params = []
        if m1:
            parts.append("SELECT Product_url FROM wmart_tv_main_1 WHERE batch_id = %s AND Product_url IS NOT NULL AND Product_url != ''")
            params.append(m1[0])
        if m2:
            parts.append("SELECT Product_url FROM wmart_tv_main_2 WHERE batch_id = %s AND Product_url IS NOT NULL AND Product_url != ''")
            params.append(m2[0])

        if not parts:
            cur.close()
            conn.close()
            return 0

        query = f"SELECT COUNT(*) FROM ({' UNION '.join(parts)}) t"
        cur.execute(query, params)
        count = cur.fetchone()[0]

        cur.close()
        conn.close()
        return count
    except Exception as e:
        print(f"[ERROR] main dedup count query failed: {e}")
        return -1


def main():
    """Main execution function"""
    stage_results = {}
    failed_stages = []
    main_dedup = None  # main1+main2 중복제거 URL 수 (main2 완료 후 세팅)
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
                    is_interim=True,
                    main_dedup=main_dedup
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

        MAIN_MIN = 300   # main1+main2 합산 최소 수집 기준
        BSR_MIN = 100    # bsr 최소 수집 기준

        # Execute main1
        print_stage_header("wmart_tv_main1", 1, 4)
        result = run_crawler("wmart_tv_main1.py", "wmart_tv_main1")
        stage_results["wmart_tv_main1"] = result
        main1_collected = result.get("collected_count") or 0
        remaining = max(0, MAIN_TOTAL_LIMIT - main1_collected + 30)  # +30: main1/main2 URL 중복 여유분
        print(f"\n[INFO] main1 수집: {main1_collected}개, main2 수집 제한: {remaining}개 (중복 여유분 +30 포함)")

        print(f"\n[INFO] Waiting 5 seconds for driver cleanup...")
        time.sleep(5)

        # Execute main2 (남은 개수만큼만 수집)
        print_stage_header("wmart_tv_main2", 2, 4)
        result = run_crawler("wmart_tv_main2.py", "wmart_tv_main2",
                            extra_args=['--max-skus', str(remaining)])
        stage_results["wmart_tv_main2"] = result
        main2_collected = result.get("collected_count") or 0

        print(f"\n[INFO] Waiting 5 seconds for driver cleanup...")
        time.sleep(5)

        # Execute bsr
        print_stage_header("wmart_tv_bsr", 3, 4)
        result = run_crawler("wmart_tv_bsr.py", "wmart_tv_bsr")
        stage_results["wmart_tv_bsr"] = result
        bsr_collected = result.get("collected_count") or 0

        # main1+main2 합산 + 중복제거 성공 판정 (둘 다 >= MAIN_MIN)
        main_total = main1_collected + main2_collected
        main_dedup = get_main_dedup_count()

        if main_total >= MAIN_MIN and main_dedup >= MAIN_MIN:
            stage_results["wmart_tv_main1"]["success"] = True
            stage_results["wmart_tv_main2"]["success"] = True
        else:
            stage_results["wmart_tv_main1"]["success"] = False
            stage_results["wmart_tv_main2"]["success"] = False
            if "wmart_tv_main1" not in failed_stages:
                failed_stages.append("wmart_tv_main1")
        print(f"\n[INFO] main1+main2 합산: {main_total} url (기준: {MAIN_MIN}개)")
        print(f"[INFO] main1+main2 중복제거: {main_dedup} url (기준: {MAIN_MIN}개)")

        # bsr 성공 판정
        if bsr_collected >= BSR_MIN:
            stage_results["wmart_tv_bsr"]["success"] = True
        else:
            stage_results["wmart_tv_bsr"]["success"] = False
            if "wmart_tv_bsr" not in failed_stages:
                failed_stages.append("wmart_tv_bsr")
        print(f"[INFO] bsr 수집: {bsr_collected}개 (기준: {BSR_MIN}개)")

        # dt1 실행: main/bsr 중 하나라도 수집했으면 실행
        total_collected = main_total + bsr_collected
        if total_collected == 0:
            print(f"\n[WARNING] main1, main2, bsr 모두 0건 수집. dt1 실행 건너뜀.")
            stage_results["wmart_tv_dt1"] = {
                "success": None, "elapsed": 0, "timeout": False,
                "collected_count": None, "target_count": None
            }
        else:
            print(f"\n[INFO] Proceeding to detail crawler...")
            time.sleep(5)

            print_stage_header(stages[3][1], 4, 4)
            result = run_crawler(stages[3][0], stages[3][1])
            stage_results["wmart_tv_dt1"] = result

            # dt1 failed 판정: 프로세스 실행 실패만 (수집 건수 기준은 main/bsr에서만)
            if not result["success"]:
                if "wmart_tv_dt1" not in failed_stages:
                    failed_stages.append("wmart_tv_dt1")

            # BSR 1~100 완비 체크 + dt1 재실행 (최대 2회)
            # dt1 프로세스는 성공했지만 일부 URL 이 실패해 BSR rank 누락된 경우 자동 복구
            orchestrator_start_str = datetime.fromtimestamp(overall_start_time).strftime('%Y-%m-%d %H:%M:%S')
            MAX_BSR_RETRIES = 2
            for retry_attempt in range(1, MAX_BSR_RETRIES + 1):
                missing = get_missing_bsr_ranks(orchestrator_start_str)
                if missing is None:
                    print(f"\n[WARNING] BSR 완비 체크 실패 (DB 에러), 재시도 건너뜀")
                    break
                if not missing:
                    print(f"\n[OK] BSR 1~100 완비 ✓")
                    break
                print(f"\n{'='*80}")
                print(f"[BSR RETRY {retry_attempt}/{MAX_BSR_RETRIES}] BSR 누락 {len(missing)}개: {missing}")
                print(f"[BSR RETRY] dt1 재실행 (already_processed 필터로 누락 URL 만 재처리)")
                print(f"{'='*80}")
                time.sleep(5)
                retry_result = run_crawler(stages[3][0], stages[3][1])
                print(f"[BSR RETRY {retry_attempt}] 결과: success={retry_result['success']}, collected={retry_result.get('collected_count')}")

            # 최종 BSR 완비 체크 → 누락 남으면 failed_stages 추가
            final_missing = get_missing_bsr_ranks(orchestrator_start_str)
            if final_missing:
                print(f"\n[CRITICAL] BSR {len(final_missing)}개 최종 누락: {final_missing}")
                if "wmart_tv_dt1" not in failed_stages:
                    failed_stages.append("wmart_tv_dt1")

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
            is_interim=False,
            main_dedup=main_dedup
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
            error_message='Crawler interrupted by user',
            main_dedup=main_dedup
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
            error_message=str(e),
            main_dedup=main_dedup
        )
        return 1


if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)
