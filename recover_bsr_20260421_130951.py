"""
Recovery script: batch 20260421_130951의 누락 BSR rank 5건을 복구

대상:
  rank 10 -> item B0DFNTKTYN  (sspa case, row exists -> UPDATE)
  rank 27 -> item B0DVX8WJ7S  (sspa case, row exists -> UPDATE)
  rank 53 -> item B0GM3KH77D  (sspa case, row exists -> UPDATE)
  rank 69 -> item B08SVZ775L  (variant->parent, row exists -> UPDATE)
  rank 28 -> item B0CJDSNN4T  (no row in batch -> INSERT minimal BSR-only row)

사용법:
  python recover_bsr_20260421_130951.py            # dry-run (기본)
  python recover_bsr_20260421_130951.py --commit   # 실제 커밋
"""
import sys
import os
import psycopg2

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from config import DB_CONFIG

TARGET_BATCH = '20260421_130951'
BSR_BATCH = '20260421_130730'
CALENDAR_WEEK = 'w17'
INSERT_CRAWL_DATETIME = '2026-04-21 01:00:00'

# (bsr_rank, detail_crawled.id, tv_retail_com.id, item) - 사전 실측 확인됨
UPDATE_TARGETS = [
    (10, 90226, 259938, 'B0DFNTKTYN'),
    (27, 90328, 260057, 'B0DVX8WJ7S'),
    (53, 90318, 260047, 'B0GM3KH77D'),
    (69, 90287, 260007, 'B08SVZ775L'),
]

INSERT_TARGETS = [
    (28, 'B0CJDSNN4T'),
]


def main(dry_run=True):
    conn = psycopg2.connect(**DB_CONFIG)
    conn.autocommit = False
    cur = conn.cursor()

    mode = 'DRY-RUN' if dry_run else 'COMMIT'
    print(f"=== BSR Recovery for batch {TARGET_BATCH} [{mode}] ===\n")

    # 0) Pre-check: 현재 bsr_rank 저장 수
    cur.execute("""
        SELECT COUNT(DISTINCT bsr_rank) FROM public.amazon_tv_detail_crawled
        WHERE batch_id=%s AND bsr_rank IS NOT NULL
    """, (TARGET_BATCH,))
    pre_count = cur.fetchone()[0]
    print(f"[PRE] distinct bsr_rank in batch: {pre_count}")

    # 1) Phase 1: UPDATE 4건 (detail_crawled + tv_retail_com)
    print(f"\n--- Phase 1: UPDATE {len(UPDATE_TARGETS)} rows ---")
    for bsr_rank, dc_id, trc_id, item in UPDATE_TARGETS:
        # detail_crawled 현재 상태 재확인
        cur.execute("""
            SELECT batch_id, item, main_rank, bsr_rank
            FROM public.amazon_tv_detail_crawled WHERE id=%s
        """, (dc_id,))
        r = cur.fetchone()
        if not r:
            print(f"  [SKIP] rank={bsr_rank} dc_id={dc_id} not found")
            continue
        if r[0] != TARGET_BATCH or r[1] != item:
            print(f"  [SKIP] rank={bsr_rank} dc_id={dc_id} identity mismatch: {r}")
            continue
        if r[3] is not None:
            print(f"  [SKIP] rank={bsr_rank} item={item} bsr_rank already {r[3]}")
            continue

        cur.execute("""
            UPDATE public.amazon_tv_detail_crawled SET bsr_rank=%s WHERE id=%s
        """, (bsr_rank, dc_id))
        d_rows = cur.rowcount

        # tv_retail_com도 같은 규칙으로 UPDATE
        cur.execute("""
            SELECT id, item, bsr_rank FROM public.tv_retail_com WHERE id=%s
        """, (trc_id,))
        tr = cur.fetchone()
        t_rows = 0
        if tr and tr[1] == item and tr[2] is None:
            cur.execute("""
                UPDATE public.tv_retail_com SET bsr_rank=%s WHERE id=%s
            """, (bsr_rank, trc_id))
            t_rows = cur.rowcount
        else:
            print(f"    [WARN] tv_retail_com id={trc_id} skip: {tr}")

        print(f"  [OK] rank={bsr_rank} item={item}: detail={d_rows}, retail={t_rows}")

    # 2) Phase 2: INSERT 1건 (rank 28)
    print(f"\n--- Phase 2: INSERT {len(INSERT_TARGETS)} rows ---")
    for bsr_rank, asin in INSERT_TARGETS:
        # 이미 존재하면 skip
        cur.execute("""
            SELECT COUNT(*) FROM public.amazon_tv_detail_crawled
            WHERE batch_id=%s AND item=%s
        """, (TARGET_BATCH, asin))
        if cur.fetchone()[0] > 0:
            print(f"  [SKIP] rank={bsr_rank} item={asin} already exists in target batch")
            continue

        # 원본 URL 가져오기
        cur.execute("""
            SELECT product_url FROM public.amazon_tv_bsr
            WHERE batch_id=%s AND bsr_rank=%s
        """, (BSR_BATCH, bsr_rank))
        r = cur.fetchone()
        if not r:
            print(f"  [SKIP] rank={bsr_rank} not found in amazon_tv_bsr batch {BSR_BATCH}")
            continue
        url = r[0]

        # detail_crawled INSERT
        cur.execute("""
            INSERT INTO public.amazon_tv_detail_crawled
            (account_name, batch_id, page_type, product_url, item,
             bsr_rank, calendar_week, crawl_datetime)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            RETURNING id
        """, ('Amazon', TARGET_BATCH, 'bsr', url, asin,
              bsr_rank, CALENDAR_WEEK, INSERT_CRAWL_DATETIME))
        new_dc_id = cur.fetchone()[0]

        # tv_retail_com INSERT
        cur.execute("""
            INSERT INTO public.tv_retail_com
            (account_name, page_type, product_url, item, bsr_rank,
             calendar_week, crawl_datetime, batch_id)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            RETURNING id
        """, ('Amazon', 'bsr', url, asin, bsr_rank,
              CALENDAR_WEEK, INSERT_CRAWL_DATETIME, TARGET_BATCH))
        new_trc_id = cur.fetchone()[0]

        print(f"  [OK] rank={bsr_rank} item={asin}: detail_id={new_dc_id}, retail_id={new_trc_id}")

    # 3) Post-check
    cur.execute("""
        SELECT COUNT(DISTINCT bsr_rank) FROM public.amazon_tv_detail_crawled
        WHERE batch_id=%s AND bsr_rank IS NOT NULL
    """, (TARGET_BATCH,))
    post_count = cur.fetchone()[0]
    print(f"\n[POST] distinct bsr_rank in batch: {pre_count} -> {post_count} (delta=+{post_count-pre_count})")

    cur.execute("""
        SELECT generate_series AS missing FROM generate_series(1,99)
        WHERE generate_series NOT IN (
          SELECT bsr_rank FROM public.amazon_tv_detail_crawled
          WHERE batch_id=%s AND bsr_rank IS NOT NULL
        )
    """, (TARGET_BATCH,))
    still_missing = [r[0] for r in cur.fetchall()]
    print(f"[POST] still missing ranks (1-99): {still_missing}")

    # 4) Commit or rollback
    if dry_run:
        conn.rollback()
        print(f"\n[DRY-RUN] rolled back. Use --commit to persist.")
    else:
        conn.commit()
        print(f"\n[COMMIT] persisted.")

    cur.close()
    conn.close()


if __name__ == '__main__':
    is_commit = '--commit' in sys.argv
    main(dry_run=not is_commit)
