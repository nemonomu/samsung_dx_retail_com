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
from datetime import datetime
from alert_monitor import send_crawl_alert


def print_separator():
    """Print separator line"""
    print("=" * 80)


def print_stage_header(stage_name, stage_num, total_stages):
    """Print stage header"""
    print_separator()
    print(f"[STAGE {stage_num}/{total_stages}] {stage_name}")
    print_separator()


def run_crawler(script_name, stage_name):
    """
    Run a crawler script and return success status

    Args:
        script_name: Python script filename (e.g., 'amazon_tv_main1.py')
        stage_name: Display name for the stage

    Returns:
        bool: True if successful, False if failed
    """
    start_time = time.time()
    print(f"\n[INFO] Starting {stage_name}...")
    print(f"[INFO] Command: python {script_name}")
    print(f"[INFO] Start time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")

    try:
        # Run the script with real-time output (no buffering)
        result = subprocess.run(
            [sys.executable, '-u', script_name],  # -u: unbuffered output
            # Remove capture_output to enable real-time streaming
            stdout=None,  # Inherit parent's stdout
            stderr=None,  # Inherit parent's stderr
            text=True,
            encoding='utf-8',
            errors='replace',
            timeout=21600  # 6 hours timeout
        )

        elapsed_time = time.time() - start_time

        if result.returncode == 0:
            print(f"\n[OK] {stage_name} completed successfully")
            print(f"[INFO] Elapsed time: {elapsed_time:.1f} seconds")
            return True
        else:
            print(f"\n[ERROR] {stage_name} failed with return code {result.returncode}")
            print(f"[INFO] Elapsed time: {elapsed_time:.1f} seconds")
            return False

    except subprocess.TimeoutExpired:
        elapsed_time = time.time() - start_time
        print(f"\n[ERROR] {stage_name} timed out after 6 hours")
        print(f"[INFO] Elapsed time: {elapsed_time:.1f} seconds")
        return False
    except Exception as e:
        elapsed_time = time.time() - start_time
        print(f"\n[ERROR] {stage_name} failed with exception: {e}")
        print(f"[INFO] Elapsed time: {elapsed_time:.1f} seconds")
        return False


def create_failure_log(failed_stages):
    """
    Create failure log file in C:\\samsung_dx_retail_com\\failed_amazon\\

    Args:
        failed_stages: List of failed script names
    """
    try:
        # Create directory if not exists
        log_dir = r"C:\samsung_dx_retail_com\failed_amazon"
        os.makedirs(log_dir, exist_ok=True)

        # Create filename with current time (YYYYMMDDHHmm)
        timestamp = datetime.now().strftime("%Y%m%d%H%M")
        log_file = os.path.join(log_dir, f"{timestamp}.txt")

        # Write failed stages
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
    # Initialize variables for exception handling
    results = {}
    failed_stages = []
    overall_start_time = time.time()

    try:
        # Generate batch_id and session_start_time for consistency
        batch_id = datetime.now().strftime('%Y%m%d_%H%M%S')
        session_start_time = datetime.now().strftime('%Y%m%d%H%M')

        print_separator()
        print("Amazon TV Crawler - Integrated Sequential Execution")
        print(f"Batch ID: {batch_id}")
        print(f"Session ID: {session_start_time}")
        print(f"Start time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print_separator()

        # Stage definitions: (script_name, display_name)
        stages = [
            ("amazon_tv_main1.py", "Main Crawler (No Price Collection)"),
            ("amazon_tv_bsr1.py", "BSR Crawler (No Price Collection)"),
            ("amazon_tv_dt2.py", "Detail Crawler with Account Switching (unsandev0002 -> unsandev0003)")
        ]

        # Execute main1, bsr1
        for i, (script, name) in enumerate(stages[:2], 1):
            print_stage_header(name, i, 3)
            success = run_crawler(script, name)
            results[script] = success

            if not success:
                failed_stages.append(script)

            # Wait 5 seconds between stages for driver cleanup
            if i < 2:
                print(f"\n[INFO] Waiting 5 seconds for driver cleanup...")
                time.sleep(5)

        # Check if at least one of main1/bsr1 succeeded
        main_stages_success = any([results["amazon_tv_main1.py"],
                                    results["amazon_tv_bsr1.py"]])

        # Execute dt1 only if at least one main stage succeeded
        if main_stages_success:
            print(f"\n[INFO] At least one main stage succeeded. Proceeding to detail crawler...")
            time.sleep(5)

            print_stage_header(stages[2][1], 3, 3)
            success = run_crawler(stages[2][0], stages[2][1])
            results[stages[2][0]] = success

            if not success:
                failed_stages.append(stages[2][0])
        else:
            print(f"\n[WARNING] All main stages (main1, bsr1) failed. Skipping detail crawler.")
            results["amazon_tv_dt2.py"] = None  # Not executed

        # Create failure log if any stage failed
        if failed_stages:
            create_failure_log(failed_stages)

        # Print final summary
        overall_elapsed = time.time() - overall_start_time
        print_separator()
        print("EXECUTION SUMMARY")
        print_separator()
        print(f"{'Stage':<50} {'Status':<15}")
        print("-" * 80)

        for script, name in stages:
            status = results.get(script)
            if status is True:
                status_str = "SUCCESS"
            elif status is False:
                status_str = "FAILED"
            else:
                status_str = "SKIPPED"

            print(f"{name:<50} {status_str:<15}")

        print("-" * 80)
        print(f"Total elapsed time: {overall_elapsed:.1f} seconds ({overall_elapsed/60:.1f} minutes)")
        print(f"End time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

        if failed_stages:
            print(f"\n[WARNING] {len(failed_stages)} stage(s) failed")
            print(f"Failed stages: {', '.join(failed_stages)}")
        else:
            print(f"\n[OK] All executed stages completed successfully!")

        print_separator()

        # Send email notification
        send_crawl_alert(
            retailer='Amazon',
            results=results,
            failed_stages=failed_stages,
            elapsed_time=overall_elapsed
        )

        # Return exit code
        return 0 if not failed_stages else 1

    except KeyboardInterrupt:
        print("\n[!] Interrupted by user")
        overall_elapsed = time.time() - overall_start_time
        send_crawl_alert(
            retailer='Amazon',
            results=results,
            failed_stages=['Interrupted by user'],
            elapsed_time=overall_elapsed,
            error_message='Crawler interrupted by user'
        )
        return 1

    except Exception as e:
        print(f"\n[!] Fatal error: {e}")
        import traceback
        traceback.print_exc()
        overall_elapsed = time.time() - overall_start_time
        send_crawl_alert(
            retailer='Amazon',
            results=results,
            failed_stages=['Fatal error'],
            elapsed_time=overall_elapsed,
            error_message=str(e)
        )
        return 1


if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)
