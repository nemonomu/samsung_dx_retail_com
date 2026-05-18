from __future__ import annotations

import argparse
import sys
import time
import traceback
from datetime import datetime

import pytz

from unsan_test_runtime import install_test_runtime


TABLE_MAP = {
    "test_tv_retail_com": "tv_retail_com_bby_v2_test",
    "tv_retail_com": "tv_retail_com_bby_v2_test",
    "bby_tv_product_list": "bby_tv_product_list_v2_test",
}

STAGES = ("main", "bsr", "pmt", "trend", "detail")


def should_run(stage: str, resume_from: str | None) -> bool:
    if not resume_from:
        return True
    return STAGES.index(stage) >= STAGES.index(resume_from)


def main(argv=None) -> int:
    install_test_runtime(TABLE_MAP)

    from common.base_crawler import BaseCrawler
    from tv_bestbuy_old.bby_tv_main import BestBuyTVMainCrawler
    from tv_bestbuy_old.bby_tv_bsr import BestBuyTVBSRCrawler
    from tv_bestbuy_old.bby_tv_pmt import BestBuyTVPromotionCrawler
    from tv_bestbuy_old.bby_tv_trend import BestBuyTVTrendCrawler
    from tv_bestbuy_old.bby_tv_dt import BestBuyTVDetailCrawler

    parser = argparse.ArgumentParser(description="Run Unsan BestBuy TV crawler into v2 test tables.")
    parser.add_argument("--resume-from", choices=STAGES)
    parser.add_argument("--batch-id")
    parser.add_argument("--time_offset", type=int, default=0)
    args = parser.parse_args(argv)

    if args.resume_from and not args.batch_id:
        print("[ERROR] --batch-id is required when using --resume-from")
        return 1

    base = BaseCrawler()
    batch_id = args.batch_id or base.generate_batch_id("Bestbuy", time_offset_hours=args.time_offset)
    start_kst = datetime.now(pytz.timezone("Asia/Seoul"))
    print("=" * 60)
    print("BestBuy TV Unsan Crawler -> v2 test tables")
    print("=" * 60)
    print(f"batch_id: {batch_id}")
    print(f"start_kst: {start_kst:%Y-%m-%d %H:%M:%S}")

    stage_defs = (
        ("main", lambda: BestBuyTVMainCrawler(test_mode=False, batch_id=batch_id, time_offset_hours=args.time_offset).run()),
        ("bsr", lambda: BestBuyTVBSRCrawler(test_mode=False, batch_id=batch_id, time_offset_hours=args.time_offset).run()),
        ("pmt", lambda: BestBuyTVPromotionCrawler(test_mode=False, batch_id=batch_id, time_offset_hours=args.time_offset).run()),
        ("trend", lambda: BestBuyTVTrendCrawler(test_mode=False, batch_id=batch_id, time_offset_hours=args.time_offset).run()),
        ("detail", lambda: BestBuyTVDetailCrawler(batch_id=batch_id, test_mode=False, time_offset_hours=args.time_offset).run()),
    )

    results = {}
    for index, (stage, runner) in enumerate(stage_defs, 1):
        if not should_run(stage, args.resume_from):
            results[stage] = "skipped"
            continue
        print(f"\n[STEP {index}/5] {stage}")
        started = time.time()
        try:
            results[stage] = bool(runner())
        except Exception as exc:
            print(f"[ERROR] {stage}: {exc}")
            traceback.print_exc()
            results[stage] = False
        print(f"[STEP {index}/5] {stage} result={results[stage]} elapsed={time.time() - started:.1f}s")

    print("\n[DONE] BestBuy TV Unsan test run")
    print(f"batch_id: {batch_id}")
    for stage, result in results.items():
        print(f"  {stage}: {result}")
    return 0 if any(result is True for result in results.values()) else 1


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
