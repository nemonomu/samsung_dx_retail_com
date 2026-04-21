"""
wmart_tv_recover_main.py 가 auto-find 로 잡은 직전(최신) batch 의 시각으로
복구 레코드를 저장한 결과, 원래 복구 대상 세션(main1_batch_id)의 시간 범위를
벗어난 문제를 바로잡는 one-off 스크립트.

Walmart_tv_detail_crawled / tv_retail_com 의 해당 URL 레코드 중 target_datetime
기준 ±window-hours 윈도우를 벗어난 것만 target_datetime 으로 UPDATE 한다.
tv_item_mst 는 crawl_datetime 컬럼이 없으므로 건너뜀.

사용:
    python wmart_tv_fix_recover_datetime.py <main1_batch_id> <main2_batch_id>
예:
    python wmart_tv_fix_recover_datetime.py 20260420_130014 20260420_130655

이후:
    python wmart_tv_recover_main.py 20260420_130014 20260420_130655
"""
import argparse
import sys
from datetime import datetime, timedelta

import psycopg2
import pytz

from config import DB_CONFIG


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('main1_batch_id')
    parser.add_argument('main2_batch_id')
    parser.add_argument(
        '--window-hours', type=float, default=8.0,
        help='target_datetime 기준 ±N시간을 "올바른 세션" 윈도우로 간주. 밖은 잘못 저장된 복구분으로 판단. (default: 8)'
    )
    args = parser.parse_args()

    kst = pytz.timezone('Asia/Seoul').localize(
        datetime.strptime(args.main1_batch_id, '%Y%m%d_%H%M%S')
    )
    target_datetime = kst.astimezone().replace(tzinfo=None)
    window = timedelta(hours=args.window_hours)
    window_start = target_datetime - window
    window_end = target_datetime + window

    print(f"[INFO] main1_batch_id       = {args.main1_batch_id} (KST)")
    print(f"[INFO] target_datetime      = {target_datetime} (local)")
    print(f"[INFO] in-session window    = {window_start} ~ {window_end}")

    conn = psycopg2.connect(**DB_CONFIG)
    conn.autocommit = False
    cur = conn.cursor()

    cur.execute("""
        SELECT DISTINCT Product_url FROM (
            SELECT Product_url FROM wmart_tv_main_1
             WHERE batch_id = %s AND Product_url IS NOT NULL AND Product_url != ''
            UNION
            SELECT Product_url FROM wmart_tv_main_2
             WHERE batch_id = %s AND Product_url IS NOT NULL AND Product_url != ''
        ) s
    """, (args.main1_batch_id, args.main2_batch_id))
    source_urls = [row[0] for row in cur.fetchall()]
    print(f"[INFO] Source URLs (main1+main2 unique): {len(source_urls)}")
    if not source_urls:
        print("[ERROR] No source URLs found for given batch_ids")
        conn.close()
        sys.exit(1)

    cur.execute("""
        SELECT product_url, crawl_datetime, main_rank, bsr_rank
        FROM Walmart_tv_detail_crawled
        WHERE product_url = ANY(%s)
          AND (crawl_datetime < %s OR crawl_datetime > %s)
        ORDER BY crawl_datetime
    """, (source_urls, window_start, window_end))
    misplaced = cur.fetchall()

    print(f"\n[DIAGNOSE] Walmart_tv_detail_crawled records outside window: {len(misplaced)}")
    for url, dt, mr, br in misplaced:
        print(f"     {dt} | main_rank={mr} bsr_rank={br} | {url[:80]}...")

    if not misplaced:
        print("[OK] No misplaced records. Nothing to fix.")
        conn.close()
        return

    resp = input(f"\nUpdate these {len(misplaced)} record(s) in BOTH tables to crawl_datetime={target_datetime}? [y/N] ")
    if resp.strip().lower() != 'y':
        print("[ABORT] No changes made")
        conn.close()
        return

    cur.execute("""
        UPDATE Walmart_tv_detail_crawled
           SET crawl_datetime = %s
         WHERE product_url = ANY(%s)
           AND (crawl_datetime < %s OR crawl_datetime > %s)
    """, (target_datetime, source_urls, window_start, window_end))
    detail_updated = cur.rowcount
    print(f"[OK] Walmart_tv_detail_crawled updated: {detail_updated} row(s)")

    cur.execute("""
        UPDATE tv_retail_com
           SET crawl_datetime = %s
         WHERE product_url = ANY(%s)
           AND (crawl_datetime < %s OR crawl_datetime > %s)
    """, (target_datetime, source_urls, window_start, window_end))
    retail_updated = cur.rowcount
    print(f"[OK] tv_retail_com updated:              {retail_updated} row(s)")

    conn.commit()
    cur.close()
    conn.close()

    print(f"\n[DONE] Next step:")
    print(f"    python wmart_tv_recover_main.py {args.main1_batch_id} {args.main2_batch_id}")


if __name__ == '__main__':
    main()
