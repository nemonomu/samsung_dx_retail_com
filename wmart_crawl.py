"""
Walmart TV Crawler - Integrated Sequential Execution
Executes main1 -> main2 -> bsr -> dt1 in sequence

If at least one of (main1, main2, bsr) succeeds, dt1 will run.
If any step fails, creates a failure log at C:\samsung_dx_retail_com\failed_wmart\
"""

import subprocess
import sys
import time
import os
from datetime import datetime


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
        script_name: Python script filename (e.g., 'wmart_tv_main1.py')
        stage_name: Display name for the stage

    Returns:
        bool: True if successful, False if failed
    """
    start_time = time.time()
    print(f"\n[INFO] Starting {stage_name}...")
    print(f"[INFO] Command: python {script_name}")
    print(f"[INFO] Start time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")

    try:
        # Run the script and capture output
        result = subprocess.run(
            [sys.executable, script_name],
            capture_output=True,
            text=True,
            encoding='utf-8',
            errors='replace'
        )

        # Print the output in real-time style
        if result.stdout:
            print(result.stdout)
        if result.stderr:
            print(result.stderr, file=sys.stderr)

        elapsed_time = time.time() - start_time

        if result.returncode == 0:
            print(f"\n[OK] {stage_name} completed successfully")
            print(f"[INFO] Elapsed time: {elapsed_time:.1f} seconds")
            return True
        else:
            print(f"\n[ERROR] {stage_name} failed with return code {result.returncode}")
            print(f"[INFO] Elapsed time: {elapsed_time:.1f} seconds")
            return False

    except Exception as e:
        elapsed_time = time.time() - start_time
        print(f"\n[ERROR] {stage_name} failed with exception: {e}")
        print(f"[INFO] Elapsed time: {elapsed_time:.1f} seconds")
        return False


def create_failure_log(failed_stages):
    """
    Create failure log file in C:\samsung_dx_retail_com\failed_wmart\

    Args:
        failed_stages: List of failed script names
    """
    try:
        # Create directory if not exists
        log_dir = r"C:\samsung_dx_retail_com\failed_wmart"
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
    print_separator()
    print("Walmart TV Crawler - Integrated Sequential Execution")
    print(f"Start time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print_separator()

    overall_start_time = time.time()

    # Stage definitions: (script_name, display_name)
    stages = [
        ("wmart_tv_main1.py", "Main Crawler Part 1 (Pages 1-5)"),
        ("wmart_tv_main2.py", "Main Crawler Part 2 (Pages 6-10)"),
        ("wmart_tv_bsr.py", "BSR Crawler"),
        ("wmart_tv_dt1.py", "Detail Crawler")
    ]

    results = {}
    failed_stages = []

    # Execute main1, main2, bsr
    for i, (script, name) in enumerate(stages[:3], 1):
        print_stage_header(name, i, 4)
        success = run_crawler(script, name)
        results[script] = success

        if not success:
            failed_stages.append(script)

        # Wait 5 seconds between stages for driver cleanup
        if i < 3:
            print(f"\n[INFO] Waiting 5 seconds for driver cleanup...")
            time.sleep(5)

    # Check if at least one of main1/main2/bsr succeeded
    main_stages_success = any([results["wmart_tv_main1.py"],
                                results["wmart_tv_main2.py"],
                                results["wmart_tv_bsr.py"]])

    # Execute dt1 only if at least one main stage succeeded
    if main_stages_success:
        print(f"\n[INFO] At least one main stage succeeded. Proceeding to detail crawler...")
        time.sleep(5)

        print_stage_header(stages[3][1], 4, 4)
        success = run_crawler(stages[3][0], stages[3][1])
        results[stages[3][0]] = success

        if not success:
            failed_stages.append(stages[3][0])
    else:
        print(f"\n[WARNING] All main stages (main1, main2, bsr) failed. Skipping detail crawler.")
        results["wmart_tv_dt1.py"] = None  # Not executed

    # Create failure log if any stage failed
    if failed_stages:
        create_failure_log(failed_stages)

    # Print final summary
    overall_elapsed = time.time() - overall_start_time
    print_separator()
    print("EXECUTION SUMMARY")
    print_separator()
    print(f"{'Stage':<40} {'Status':<15}")
    print("-" * 80)

    for script, name in stages:
        status = results.get(script)
        if status is True:
            status_str = "SUCCESS"
        elif status is False:
            status_str = "FAILED"
        else:
            status_str = "SKIPPED"

        print(f"{name:<40} {status_str:<15}")

    print("-" * 80)
    print(f"Total elapsed time: {overall_elapsed:.1f} seconds ({overall_elapsed/60:.1f} minutes)")
    print(f"End time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    if failed_stages:
        print(f"\n[WARNING] {len(failed_stages)} stage(s) failed")
        print(f"Failed stages: {', '.join(failed_stages)}")
    else:
        print(f"\n[OK] All executed stages completed successfully!")

    print_separator()

    # Return exit code
    return 0 if not failed_stages else 1


if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)
