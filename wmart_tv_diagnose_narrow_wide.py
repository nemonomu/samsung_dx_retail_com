"""
narrow (사용자 관찰 범위) vs wide (target ±N시간) 비교 진단.

목적:
  사용자 초기 관찰 "tv_retail_com 에 293개 저장" 은 narrow 범위 기준.
  내 스크립트의 ±8h wide 범위 기준 으로는 306개.
  차이 (wide 안 but narrow 밖) 에 있는 row 들이 이전 recover 등이
  "잘못된 시각" 에 저장한 것일 가능성 — 해당 row 들의 crawl_datetime 패턴 확인.

SELECT 만 수행 (DB 쓰기 없음).

사용:
    python wmart_tv_diagnose_narrow_wide.py <main1_batch_id> <main2_batch_id>
예:
    python wmart_tv_diagnose_narrow_wide.py 20260420_130014 20260420_130655
"""
import argparse
from collections import Counter
from datetime import datetime, timedelta

import psycopg2
import pytz

from config import DB_CONFIG


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('main1_batch_id')
    parser.add_argument('main2_batch_id')
    parser.add_argument('--narrow-start', default='2026-04-20 00:00:14',
                        help='narrow window start (default: user-observed)')
    parser.add_argument('--narrow-end', default='2026-04-20 06:01:10',
                        help='narrow window end (default: user-observed)')
    parser.add_argument('--window-hours', type=float, default=8.0,
                        help='wide window = target ±N hours (default: 8)')
    args = parser.parse_args()

    kst = pytz.timezone('Asia/Seoul').localize(
        datetime.strptime(args.main1_batch_id, '%Y%m%d_%H%M%S')
    )
    target_dt = kst.astimezone().replace(tzinfo=None)
    window = timedelta(hours=args.window_hours)
    fmt = '%Y-%m-%d %H:%M:%S'
    wide_start = (target_dt - window).strftime(fmt)
    wide_end = (target_dt + window).strftime(fmt)

    print(f"[INFO] narrow window : [{args.narrow_start} ~ {args.narrow_end}]")
    print(f"[INFO] wide window   : [{wide_start} ~ {wide_end}]")
    print(f"[INFO] target local  : {target_dt.strftime(fmt)}")

    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()

    try:
        cur.execute("""
            SELECT DISTINCT Product_url FROM (
                SELECT Product_url FROM wmart_tv_main_1
                 WHERE batch_id = %s AND Product_url IS NOT NULL AND Product_url != ''
                UNION
                SELECT Product_url FROM wmart_tv_main_2
                 WHERE batch_id = %s AND Product_url IS NOT NULL AND Product_url != ''
            ) s
        """, (args.main1_batch_id, args.main2_batch_id))
        source_urls = [r[0] for r in cur.fetchall()]
        print(f"\n[INFO] source URLs (main1+main2 unique): {len(source_urls)}")

        cur.execute("""
            SELECT COUNT(DISTINCT product_url) FROM tv_retail_com
             WHERE product_url = ANY(%s)
               AND crawl_datetime >= %s AND crawl_datetime <= %s
               AND main_rank IS NOT NULL
        """, (source_urls, args.narrow_start, args.narrow_end))
        narrow_count = cur.fetchone()[0]

        cur.execute("""
            SELECT COUNT(DISTINCT product_url) FROM tv_retail_com
             WHERE product_url = ANY(%s)
               AND crawl_datetime >= %s AND crawl_datetime <= %s
               AND main_rank IS NOT NULL
        """, (source_urls, wide_start, wide_end))
        wide_count = cur.fetchone()[0]

        print(f"\n[COUNTS] tv_retail_com URLs with main_rank populated:")
        print(f"   narrow window : {narrow_count}")
        print(f"   wide window   : {wide_count}")
        print(f"   diff          : {wide_count - narrow_count} (wide-only)")

        cur.execute("""
            SELECT product_url, crawl_datetime, main_rank, bsr_rank
              FROM tv_retail_com
             WHERE product_url = ANY(%s)
               AND crawl_datetime >= %s AND crawl_datetime <= %s
               AND (crawl_datetime < %s OR crawl_datetime > %s)
               AND main_rank IS NOT NULL
             ORDER BY crawl_datetime
        """, (source_urls, wide_start, wide_end, args.narrow_start, args.narrow_end))
        misplaced = cur.fetchall()

        print(f"\n[MISPLACED in tv_retail_com] wide 안 but narrow 밖, main_rank populated ({len(misplaced)}):")
        for url, dt, mr, br in misplaced:
            br_str = str(br) if br is not None else ''
            print(f"     {dt} | main_rank={mr:<4} bsr_rank={br_str:<4} | {url[:70]}")

        if misplaced:
            hours = Counter(row[1][:13] for row in misplaced)
            print(f"\n[MISPLACED hourly distribution]")
            for hour, cnt in sorted(hours.items()):
                print(f"     {hour}:xx  → {cnt} row(s)")

        cur.execute("""
            SELECT product_url, crawl_datetime, main_rank, bsr_rank
              FROM Walmart_tv_detail_crawled
             WHERE product_url = ANY(%s)
               AND crawl_datetime >= %s AND crawl_datetime <= %s
               AND (crawl_datetime < %s OR crawl_datetime > %s)
               AND main_rank IS NOT NULL
             ORDER BY crawl_datetime
        """, (source_urls, wide_start, wide_end, args.narrow_start, args.narrow_end))
        detail_misplaced = cur.fetchall()

        print(f"\n[MISPLACED in Walmart_tv_detail_crawled] wide 안 but narrow 밖 ({len(detail_misplaced)}):")
        for url, dt, mr, br in detail_misplaced[:50]:
            br_str = str(br) if br is not None else ''
            print(f"     {dt} | main_rank={mr:<4} bsr_rank={br_str:<4} | {url[:70]}")
        if len(detail_misplaced) > 50:
            print(f"     ... ({len(detail_misplaced) - 50} more)")

    finally:
        cur.close()
        conn.close()


if __name__ == '__main__':
    main()
