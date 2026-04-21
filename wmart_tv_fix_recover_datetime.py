"""
wmart_tv_recover_main.py 가 auto-find 로 잡은 직전(최신) batch 의 시각으로
복구 레코드를 저장한 결과, 원래 복구 대상 세션(main1_batch_id) 의 시간 범위를
벗어난 문제를 바로잡고 + tv_retail_com 누락을 진단하는 스크립트.

이 프로젝트의 crawl_datetime 컬럼은 varchar 이므로 (git c39fe07 참조)
비교/바인딩을 문자열로 수행한다. 포맷은 'YYYY-MM-DD HH:MM:SS' 로 저장되어
lexicographic 순서 == 시간 순서.

동작:
1. 진단 (SELECT only)
   - source = wmart_tv_main_1/2 (지정 batch_id) unique URL
   - Walmart_tv_detail_crawled 에 있지만 crawl_datetime 이 ±window-hours 밖인 레코드
   - tv_retail_com 에 있지만 crawl_datetime 이 ±window-hours 밖인 레코드
   - tv_retail_com 에 완전히 없는 URL (INSERT 필요 대상, 별도 스크립트로 처리 예정)
2. 사용자 확인 (y/N) 후 UPDATE 수행 (window 밖 → target_datetime)
3. tv_retail_com 누락 URL 은 INSERT 대상 후보로 리포트만 (이 스크립트에서는 생성하지 않음)

사용:
    python wmart_tv_fix_recover_datetime.py <main1_batch_id> <main2_batch_id>
예:
    python wmart_tv_fix_recover_datetime.py 20260420_130014 20260420_130655
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
        help='target_datetime 기준 ±N시간을 "올바른 세션" 윈도우로 간주. (default: 8)'
    )
    args = parser.parse_args()

    kst = pytz.timezone('Asia/Seoul').localize(
        datetime.strptime(args.main1_batch_id, '%Y%m%d_%H%M%S')
    )
    target_datetime = kst.astimezone().replace(tzinfo=None)
    window = timedelta(hours=args.window_hours)
    window_start = target_datetime - window
    window_end = target_datetime + window

    fmt = '%Y-%m-%d %H:%M:%S'
    target_str = target_datetime.strftime(fmt)
    window_start_str = window_start.strftime(fmt)
    window_end_str = window_end.strftime(fmt)

    print(f"[INFO] main1_batch_id       = {args.main1_batch_id} (KST)")
    print(f"[INFO] target_datetime      = {target_str} (local)")
    print(f"[INFO] in-session window    = {window_start_str} ~ {window_end_str}")
    print(f"[INFO] (crawl_datetime is varchar — compared as string)")

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
    print(f"\n[INFO] Source URLs (main1+main2 unique): {len(source_urls)}")
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
    """, (source_urls, window_start_str, window_end_str))
    detail_misplaced = cur.fetchall()
    print(f"\n[DIAGNOSE 1] Walmart_tv_detail_crawled records OUTSIDE window: {len(detail_misplaced)}")
    for url, dt, mr, br in detail_misplaced:
        print(f"     {dt} | main_rank={mr} bsr_rank={br} | {url[:80]}")

    cur.execute("""
        SELECT product_url, crawl_datetime, main_rank, bsr_rank
          FROM tv_retail_com
         WHERE product_url = ANY(%s)
           AND (crawl_datetime < %s OR crawl_datetime > %s)
         ORDER BY crawl_datetime
    """, (source_urls, window_start_str, window_end_str))
    retail_misplaced = cur.fetchall()
    print(f"\n[DIAGNOSE 2] tv_retail_com records OUTSIDE window: {len(retail_misplaced)}")
    for url, dt, mr, br in retail_misplaced:
        print(f"     {dt} | main_rank={mr} bsr_rank={br} | {url[:80]}")

    cur.execute("""
        SELECT DISTINCT product_url FROM tv_retail_com
         WHERE product_url = ANY(%s)
    """, (source_urls,))
    retail_present = {row[0] for row in cur.fetchall()}
    retail_absent = [u for u in source_urls if u not in retail_present]
    print(f"\n[DIAGNOSE 3] URLs in source but MISSING from tv_retail_com entirely: {len(retail_absent)}")
    for u in retail_absent:
        print(f"     MISSING | {u[:100]}")

    print(f"\n[SUMMARY]")
    print(f"   source URLs                             : {len(source_urls)}")
    print(f"   Walmart_tv_detail_crawled out-of-window : {len(detail_misplaced)}  (→ candidates for UPDATE)")
    print(f"   tv_retail_com out-of-window             : {len(retail_misplaced)}  (→ candidates for UPDATE)")
    print(f"   tv_retail_com missing entirely          : {len(retail_absent)}  (→ will need INSERT; run separate script)")

    if not detail_misplaced and not retail_misplaced:
        print("\n[OK] Nothing to UPDATE.")
        if retail_absent:
            print(f"[NOTE] {len(retail_absent)} URL(s) still missing from tv_retail_com — need INSERT tool.")
        conn.close()
        return

    resp = input(
        f"\nUpdate {len(detail_misplaced)} detail + {len(retail_misplaced)} retail row(s) "
        f"to crawl_datetime={target_str}? [y/N] "
    )
    if resp.strip().lower() != 'y':
        print("[ABORT] No changes made")
        conn.close()
        return

    cur.execute("""
        UPDATE Walmart_tv_detail_crawled
           SET crawl_datetime = %s
         WHERE product_url = ANY(%s)
           AND (crawl_datetime < %s OR crawl_datetime > %s)
    """, (target_str, source_urls, window_start_str, window_end_str))
    detail_updated = cur.rowcount
    print(f"[OK] Walmart_tv_detail_crawled updated: {detail_updated} row(s)")

    cur.execute("""
        UPDATE tv_retail_com
           SET crawl_datetime = %s
         WHERE product_url = ANY(%s)
           AND (crawl_datetime < %s OR crawl_datetime > %s)
    """, (target_str, source_urls, window_start_str, window_end_str))
    retail_updated = cur.rowcount
    print(f"[OK] tv_retail_com updated:              {retail_updated} row(s)")

    conn.commit()
    cur.close()
    conn.close()

    print(f"\n[DONE]")
    if retail_absent:
        print(f"[NEXT] {len(retail_absent)} URL(s) still need INSERT into tv_retail_com.")
        print(f"       Separate tool will copy from Walmart_tv_detail_crawled after your confirmation.")


if __name__ == '__main__':
    main()
