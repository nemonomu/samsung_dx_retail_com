"""
tv_retail_com 에서 narrow session window 기준 main_rank count 를 300 에 맞추기
위해, 이미 저장된 bsr_rank 있는 main_rank=NULL row 에 임의 main_rank (gap 값)
를 부여하는 1회성 스크립트.

사용자 명시:
  "bsr에도 있는 url이라면 임의로라도 main_rank를 채워야해"
  "이미 총 322개가 있잖아" — 재크롤 불필요, 기존 row UPDATE 로 해결.

동작:
1. tv_retail_com narrow window 내 main_rank 사용 중인 값 조회 → gap 계산
2. bsr_rank IS NOT NULL AND main_rank IS NULL 인 row 를 crawl_datetime 순 --count 개 선택
3. gap 값 작은 순 --count 개를 각 row 에 1:1 매핑
4. [UPDATE PLAN] 표시 후 [y/N] 확인
5. ctid 기반 UPDATE (main_rank 컬럼만, main_rank IS NULL 이중 보호)

사용:
    python wmart_tv_fill_main_rank_arbitrary.py
    python wmart_tv_fill_main_rank_arbitrary.py --count 7
"""
import argparse
import sys

import psycopg2

from config import DB_CONFIG


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--narrow-start', default='2026-04-20 00:00:14',
                        help='narrow window start (default: user-observed)')
    parser.add_argument('--narrow-end', default='2026-04-20 06:01:10',
                        help='narrow window end (default: user-observed)')
    parser.add_argument('--count', type=int, default=7,
                        help='부여할 row 개수 (default 7)')
    parser.add_argument('--max-rank', type=int, default=307,
                        help='main_rank 범위 상한, gap 탐색용 (default 307)')
    parser.add_argument('--account', default='Walmart',
                        help='account_name 필터 — tv_retail_com 이 통합 테이블이므로 필수 (default: Walmart)')
    args = parser.parse_args()

    print(f"[INFO] narrow window : [{args.narrow_start} ~ {args.narrow_end}]")
    print(f"[INFO] account_name  : {args.account}")
    print(f"[INFO] count to fill : {args.count}")
    print(f"[INFO] rank range    : 1 ~ {args.max_rank}")

    conn = psycopg2.connect(**DB_CONFIG)
    conn.autocommit = False
    cur = conn.cursor()

    try:
        cur.execute("""
            SELECT DISTINCT main_rank FROM tv_retail_com
             WHERE crawl_datetime >= %s AND crawl_datetime <= %s
               AND main_rank IS NOT NULL
               AND main_rank BETWEEN 1 AND %s
               AND account_name = %s
        """, (args.narrow_start, args.narrow_end, args.max_rank, args.account))
        used_ranks = {row[0] for row in cur.fetchall()}
        gap_ranks = sorted(r for r in range(1, args.max_rank + 1) if r not in used_ranks)

        print(f"\n[INFO] main_rank used in narrow window : {len(used_ranks)}")
        print(f"[INFO] main_rank gap count              : {len(gap_ranks)}")
        print(f"[INFO] gap values                       : {gap_ranks}")

        if len(gap_ranks) < args.count:
            print(f"[ERROR] gap ({len(gap_ranks)}) < requested count ({args.count}) — abort")
            return
        chosen_ranks = gap_ranks[:args.count]
        print(f"[INFO] chosen ranks to assign           : {chosen_ranks}")

        cur.execute("""
            SELECT ctid, product_url, crawl_datetime, bsr_rank
              FROM tv_retail_com
             WHERE crawl_datetime >= %s AND crawl_datetime <= %s
               AND main_rank IS NULL
               AND bsr_rank IS NOT NULL
               AND account_name = %s
             ORDER BY crawl_datetime, bsr_rank
             LIMIT %s
        """, (args.narrow_start, args.narrow_end, args.account, args.count))
        target_rows = cur.fetchall()
        print(f"\n[INFO] target rows (main_rank=NULL AND bsr_rank NOT NULL, narrow): {len(target_rows)}")

        if len(target_rows) < args.count:
            print(f"[ERROR] target rows ({len(target_rows)}) < requested count ({args.count}) — abort")
            return

        print(f"\n[UPDATE PLAN]")
        plan = list(zip(target_rows, chosen_ranks))
        for (ctid, url, dt, br), mr in plan:
            print(f"   SET main_rank={mr:<4} | bsr_rank={br:<4} | {dt} | {url[:60]}")

        resp = input(
            f"\nUPDATE {args.count} row(s) with arbitrary main_rank "
            f"(narrow window, main_rank IS NULL only)? [y/N] "
        )
        if resp.strip().lower() != 'y':
            print("[ABORT] No changes made")
            return

        updated = 0
        for (ctid, url, dt, br), mr in plan:
            cur.execute("""
                UPDATE tv_retail_com
                   SET main_rank = %s
                 WHERE ctid = %s
                   AND main_rank IS NULL
            """, (mr, ctid))
            if cur.rowcount == 0:
                print(f"[WARN] main_rank={mr} ctid={ctid} → matched 0 rows (skip)")
            else:
                updated += cur.rowcount

        conn.commit()
        print(f"\n[OK] tv_retail_com updated: {updated} row(s)")
        print(f"[INFO] new narrow main_rank count (expected): {len(used_ranks) + updated}")

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
