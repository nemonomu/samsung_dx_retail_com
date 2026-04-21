"""
tv_retail_com 에서 main_rank 가 NULL 로 저장된 row 를 Walmart_tv_detail_crawled
기준으로 복구하는 스크립트.

문제 상황:
  source URL (wmart_tv_main_1/2) 는 tv_retail_com window 에 row 로 저장됐지만
  main_rank 컬럼이 NULL 인 경우가 있음. 사용자가 main_rank 기준 카운트 할 때
  누락 으로 보여 target (예: 300) 에 못 미침.

동작:
1. source = wmart_tv_main_1/2 (지정 batch_id) unique URL
2. tv_retail_com window 안에 해당 URL row 중 main_rank IS NULL 인 URL 식별
3. Walmart_tv_detail_crawled window 안 (URL 당 최신) 에서 main_rank 값 조회
4. 매핑 표시 [product_url → main_rank] + [y/N] 확인
5. UPDATE tv_retail_com SET main_rank = ... WHERE product_url=... AND window AND main_rank IS NULL
   (main_rank 컬럼만 수정. 다른 컬럼 건드리지 않음. IS NULL 조건 으로 기존 값 보호)

crawl_datetime 은 varchar → 문자열 비교.

사용:
    python wmart_tv_fix_main_rank.py <main1_batch_id> <main2_batch_id>
예:
    python wmart_tv_fix_main_rank.py 20260420_130014 20260420_130655
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
        help='target_datetime 기준 ±N시간을 세션 윈도우로 간주. (default: 8)'
    )
    args = parser.parse_args()

    kst = pytz.timezone('Asia/Seoul').localize(
        datetime.strptime(args.main1_batch_id, '%Y%m%d_%H%M%S')
    )
    target_dt = kst.astimezone().replace(tzinfo=None)
    window = timedelta(hours=args.window_hours)
    fmt = '%Y-%m-%d %H:%M:%S'
    target_str = target_dt.strftime(fmt)
    win_start_str = (target_dt - window).strftime(fmt)
    win_end_str = (target_dt + window).strftime(fmt)

    print(f"[INFO] main1_batch_id  = {args.main1_batch_id} (KST)")
    print(f"[INFO] target_datetime = {target_str} (local)")
    print(f"[INFO] session window  = [{win_start_str} ~ {win_end_str}]")
    print(f"[INFO] (crawl_datetime is varchar — string compare)")

    conn = psycopg2.connect(**DB_CONFIG)
    conn.autocommit = False
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
        if not source_urls:
            print("[ERROR] No source URLs found")
            return

        cur.execute("""
            SELECT DISTINCT product_url
              FROM tv_retail_com
             WHERE product_url = ANY(%s)
               AND crawl_datetime >= %s AND crawl_datetime <= %s
               AND main_rank IS NOT NULL
        """, (source_urls, win_start_str, win_end_str))
        with_rank = {r[0] for r in cur.fetchall()}
        null_rank_urls = [u for u in source_urls if u not in with_rank]

        print(f"[INFO] tv_retail_com with main_rank     : {len(with_rank)}")
        print(f"[INFO] tv_retail_com main_rank=NULL     : {len(null_rank_urls)}")

        if not null_rank_urls:
            print("\n[OK] No NULL main_rank rows. Nothing to do.")
            return

        cur.execute("""
            SELECT DISTINCT ON (product_url) product_url, main_rank
              FROM Walmart_tv_detail_crawled
             WHERE product_url = ANY(%s)
               AND crawl_datetime >= %s AND crawl_datetime <= %s
               AND main_rank IS NOT NULL
             ORDER BY product_url, crawl_datetime DESC
        """, (null_rank_urls, win_start_str, win_end_str))
        rank_map = {row[0]: row[1] for row in cur.fetchall()}

        resolvable = [u for u in null_rank_urls if u in rank_map]
        unresolvable = [u for u in null_rank_urls if u not in rank_map]

        print(f"\n[SUMMARY]")
        print(f"   NULL main_rank URLs total            : {len(null_rank_urls)}")
        print(f"   → resolvable (detail has main_rank)  : {len(resolvable)}")
        print(f"   → unresolvable (no detail main_rank) : {len(unresolvable)}")

        if unresolvable:
            print(f"\n[UNRESOLVABLE] (Walmart_tv_detail_crawled window 안 main_rank 도 NULL):")
            for u in unresolvable:
                print(f"     {u[:100]}")

        if not resolvable:
            print("\n[OK] Nothing to UPDATE.")
            return

        print(f"\n[UPDATE PLAN] (sorted by main_rank)")
        plan = sorted([(rank_map[u], u) for u in resolvable])
        for mr, u in plan:
            print(f"     main_rank={mr:<4} | {u[:80]}")

        resp = input(
            f"\nUPDATE tv_retail_com main_rank for {len(resolvable)} row(s) "
            f"(window-scoped, main_rank IS NULL only)? [y/N] "
        )
        if resp.strip().lower() != 'y':
            print("[ABORT] No changes made")
            return

        updated_total = 0
        for mr, u in plan:
            cur.execute("""
                UPDATE tv_retail_com
                   SET main_rank = %s
                 WHERE product_url = %s
                   AND crawl_datetime >= %s AND crawl_datetime <= %s
                   AND main_rank IS NULL
            """, (mr, u, win_start_str, win_end_str))
            if cur.rowcount == 0:
                print(f"[WARN] main_rank={mr} {u[:60]}... → matched 0 rows (skip)")
            else:
                updated_total += cur.rowcount

        conn.commit()
        print(f"\n[OK] tv_retail_com main_rank updated: {updated_total} row(s)")

        if unresolvable:
            print(f"\n[NEXT] {len(unresolvable)} URL(s) still NULL — detail 도 NULL 이므로 수동 조사 필요.")

    except Exception as e:
        conn.rollback()
        print(f"\n[ERROR] {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
    finally:
        cur.close()
        conn.close()


if __name__ == '__main__':
    main()
