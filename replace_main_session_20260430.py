"""
replace_main_session_20260430.py

batch_id=20260430_131024 의 main 세션을 main_crawled 20260430_162428 기준으로 완전 대체.

옵션 H' (efficient replace) — 162428 의 200 ASIN vs 131024 의 매트릭스:
  A: 117 — 162428 ASIN 이 131024 main 에 이미 있음 → UPDATE main_rank
  B:   7 — 162428 ASIN 이 131024 bsr 에만 있음 → UPDATE main_rank (page_type='bsr' 유지)
  C:  76 — 162428 ASIN 이 131024 어디에도 없음 → detail crawl + INSERT
  D:  82 — 131024 main 에 있지만 162428 에 없는 obsolete ASIN → DELETE

batch_id 는 기존 그대로 (20260430_131024) 유지.

동작:
  1) 백업: tv_retail_com_backup_20260430_131024 (IF NOT EXISTS, batch_id=131024 행만)
  2) ASIN 매트릭스 분류 (A/B/C/D)
  3) D DELETE 먼저 (잠재적 unique 충돌 방지)
  4) A, B UPDATE (main_rank 만)
  5) C 처리: AmazonDetailCrawler.scrape_detail_page 재사용 (crawler.batch_id 강제 override)
  6) 최종 검증 — batch 131024 의 page_type 별 row/unique/rank 분포 출력

실행:
  python -u replace_main_session_20260430.py
"""

import sys
import time
import random
import psycopg2

from config import DB_CONFIG
from amazon_tv_dt1 import AmazonDetailCrawler


TARGET_BATCH_ID = '20260430_131024'
NEW_MAIN_BATCH_ID = '20260430_162428'
BACKUP_TABLE = 'tv_retail_com_backup_20260430_131024'


def ensure_backup(conn):
    """131024 batch 행만 별도 테이블로 백업 (idempotent — 이미 있으면 통과)."""
    cur = conn.cursor()
    print(f"[BACKUP] ensuring '{BACKUP_TABLE}' (IF NOT EXISTS, batch_id={TARGET_BATCH_ID} only) ...")
    cur.execute(f"""
        CREATE TABLE IF NOT EXISTS "{BACKUP_TABLE}" AS
        SELECT * FROM tv_retail_com WHERE batch_id = '{TARGET_BATCH_ID}'
    """)
    cur.execute(f'SELECT COUNT(*) FROM "{BACKUP_TABLE}"')
    cnt = cur.fetchone()[0]
    print(f"[BACKUP] '{BACKUP_TABLE}' OK ({cnt} rows)")
    cur.close()


def load_classification(conn):
    """162428 의 ASIN 을 131024 와 매트릭스 분류해서 (case_a, case_b, case_c, case_d) 반환."""
    cur = conn.cursor()

    # A: 162428 ASIN 이 131024 main 에 이미 있음
    cur.execute("""
        SELECT m.main_rank,
               substring(m.product_url FROM '/dp/([A-Z0-9]{10})') AS asin
          FROM amazon_tv_main_crawled m
         WHERE m.batch_id = %s
           AND m.product_url IS NOT NULL
           AND substring(m.product_url FROM '/dp/([A-Z0-9]{10})') IN (
                 SELECT item FROM tv_retail_com
                  WHERE batch_id = %s AND page_type = 'main' AND account_name = 'Amazon'
                    AND item IS NOT NULL
              )
         ORDER BY m.main_rank
    """, (NEW_MAIN_BATCH_ID, TARGET_BATCH_ID))
    case_a = cur.fetchall()  # [(rank, asin), ...]

    # B: 162428 ASIN 이 131024 bsr 에만 있음 (main 엔 없음)
    cur.execute("""
        SELECT m.main_rank,
               substring(m.product_url FROM '/dp/([A-Z0-9]{10})') AS asin
          FROM amazon_tv_main_crawled m
         WHERE m.batch_id = %s
           AND m.product_url IS NOT NULL
           AND substring(m.product_url FROM '/dp/([A-Z0-9]{10})') IN (
                 SELECT item FROM tv_retail_com
                  WHERE batch_id = %s AND page_type = 'bsr' AND account_name = 'Amazon'
                    AND item IS NOT NULL
              )
           AND substring(m.product_url FROM '/dp/([A-Z0-9]{10})') NOT IN (
                 SELECT item FROM tv_retail_com
                  WHERE batch_id = %s AND page_type = 'main' AND account_name = 'Amazon'
                    AND item IS NOT NULL
              )
         ORDER BY m.main_rank
    """, (NEW_MAIN_BATCH_ID, TARGET_BATCH_ID, TARGET_BATCH_ID))
    case_b = cur.fetchall()

    # C: 162428 ASIN 이 131024 어디에도 없음 → detail crawl 필요
    cur.execute("""
        SELECT m.main_rank,
               m.product_url,
               substring(m.product_url FROM '/dp/([A-Z0-9]{10})') AS asin
          FROM amazon_tv_main_crawled m
         WHERE m.batch_id = %s
           AND m.product_url IS NOT NULL
           AND substring(m.product_url FROM '/dp/([A-Z0-9]{10})') NOT IN (
                 SELECT item FROM tv_retail_com
                  WHERE batch_id = %s AND account_name = 'Amazon'
                    AND item IS NOT NULL
              )
         ORDER BY m.main_rank
    """, (NEW_MAIN_BATCH_ID, TARGET_BATCH_ID))
    case_c = cur.fetchall()  # [(rank, url, asin), ...]

    # D: 131024 main 에는 있으나 162428 에 없는 obsolete ASIN
    cur.execute("""
        SELECT t.id, t.item, t.main_rank, t.retailer_sku_name
          FROM tv_retail_com t
         WHERE t.batch_id = %s AND t.page_type = 'main' AND t.account_name = 'Amazon'
           AND t.item NOT IN (
                 SELECT substring(product_url FROM '/dp/([A-Z0-9]{10})')
                   FROM amazon_tv_main_crawled
                  WHERE batch_id = %s AND product_url IS NOT NULL
              )
         ORDER BY t.main_rank
    """, (TARGET_BATCH_ID, NEW_MAIN_BATCH_ID))
    case_d = cur.fetchall()

    cur.close()
    return case_a, case_b, case_c, case_d


def apply_case_d_delete(conn, case_d):
    """D: 162428 에서 빠진 main 행 DELETE."""
    if not case_d:
        print("[D] nothing to delete")
        return 0
    cur = conn.cursor()
    ids = [r[0] for r in case_d]
    cur.execute("DELETE FROM tv_retail_com WHERE id = ANY(%s)", (ids,))
    n = cur.rowcount
    cur.close()
    print(f"[D] DELETED {n} obsolete main rows")
    return n


def apply_case_a_update(conn, case_a):
    """A: 131024 main 의 main_rank 를 162428 값으로 UPDATE."""
    if not case_a:
        print("[A] nothing to update")
        return 0
    cur = conn.cursor()
    n = 0
    for new_rank, asin in case_a:
        cur.execute("""
            UPDATE tv_retail_com
               SET main_rank = %s
             WHERE batch_id = %s AND page_type = 'main' AND account_name = 'Amazon'
               AND item = %s
        """, (new_rank, TARGET_BATCH_ID, asin))
        n += cur.rowcount
    cur.close()
    print(f"[A] UPDATED {n} main rows with new main_rank")
    return n


def apply_case_b_update(conn, case_b):
    """B: 131024 bsr-only 의 main_rank 만 채움 (page_type='bsr' 유지)."""
    if not case_b:
        print("[B] nothing to update")
        return 0
    cur = conn.cursor()
    n = 0
    for new_rank, asin in case_b:
        cur.execute("""
            UPDATE tv_retail_com
               SET main_rank = %s
             WHERE batch_id = %s AND page_type = 'bsr' AND account_name = 'Amazon'
               AND item = %s
        """, (new_rank, TARGET_BATCH_ID, asin))
        n += cur.rowcount
    cur.close()
    print(f"[B] UPDATED {n} bsr rows with main_rank (page_type='bsr' 유지)")
    return n


def process_case_c(crawler, case_c):
    """C: 76 ASIN detail crawl + INSERT (crawler.batch_id 를 TARGET_BATCH_ID 로 강제 override)."""
    if not case_c:
        print("[C] nothing to crawl")
        return 0, 0

    # crawler.batch_id 가 INSERT 시 사용됨 (amazon_tv_detail_crawled, tv_retail_com 양쪽 모두)
    crawler.batch_id = TARGET_BATCH_ID
    print(f"[C] crawler.batch_id 를 '{TARGET_BATCH_ID}' 로 override")

    success = 0
    fail = 0

    for idx, (main_rank, product_url, asin) in enumerate(case_c, start=1):
        print(f"\n{'-'*80}")
        print(f"[C {idx}/{len(case_c)}] main_rank={main_rank} asin={asin}")
        print(f"  URL: {product_url[:80]}...")

        url_data = {
            'asin': asin,
            'page_type': 'main',
            'url': product_url,
            'main_rank': main_rank,
            'bsr_rank': None,
            'number_of_units_purchased_past_month': None,
        }

        try:
            ok = crawler.scrape_detail_page(url_data)
            if ok:
                success += 1
                print(f"  [C-OK] inserted with batch_id={TARGET_BATCH_ID}")
            else:
                fail += 1
                print(f"  [C-FAIL] scrape_detail_page returned False")
        except Exception as e:
            fail += 1
            print(f"  [C-ERROR] {type(e).__name__}: {e}")

        # 페이지 간 백오프
        time.sleep(random.uniform(2, 4))

    print(f"\n[C] done: success={success} fail={fail}")
    return success, fail


def report_final(conn):
    cur = conn.cursor()
    cur.execute("""
        SELECT page_type,
               COUNT(*) AS rows,
               COUNT(DISTINCT item) AS unique_asin,
               COUNT(*) FILTER (WHERE main_rank IS NOT NULL) AS has_main_rank,
               COUNT(*) FILTER (WHERE bsr_rank IS NOT NULL) AS has_bsr_rank,
               MIN(main_rank), MAX(main_rank)
          FROM tv_retail_com
         WHERE batch_id = %s AND account_name = 'Amazon'
         GROUP BY page_type
         ORDER BY page_type
    """, (TARGET_BATCH_ID,))
    print(f"\n=== FINAL state of batch {TARGET_BATCH_ID} ===")
    print('page_type | rows | unique_asin | has_main_rank | has_bsr_rank | min_mr | max_mr')
    for r in cur.fetchall():
        print(r)

    # main_rank 충돌 검증 (같은 batch_id+page_type 내에서 main_rank 중복 있는지)
    cur.execute("""
        SELECT page_type, main_rank, COUNT(*)
          FROM tv_retail_com
         WHERE batch_id = %s AND account_name = 'Amazon' AND main_rank IS NOT NULL
         GROUP BY page_type, main_rank
        HAVING COUNT(*) > 1
         ORDER BY page_type, main_rank
    """, (TARGET_BATCH_ID,))
    dups = cur.fetchall()
    if dups:
        print(f"\n[WARN] main_rank 중복 발견 ({len(dups)} 건):")
        for r in dups[:10]:
            print(f"  {r}")
    else:
        print("\n[OK] main_rank 중복 없음")

    cur.close()


def main():
    print("=" * 80)
    print(f"Amazon TV main session 완전 대체")
    print(f"  target batch_id : {TARGET_BATCH_ID}")
    print(f"  source main batch: {NEW_MAIN_BATCH_ID}")
    print("=" * 80)

    # 1) Backup
    backup_conn = psycopg2.connect(**DB_CONFIG)
    backup_conn.autocommit = True
    ensure_backup(backup_conn)
    backup_conn.close()

    # 2) Classify
    work_conn = psycopg2.connect(**DB_CONFIG)
    work_conn.autocommit = True
    case_a, case_b, case_c, case_d = load_classification(work_conn)

    print(f"\n[CLASSIFY]")
    print(f"  A (UPDATE 131024 main main_rank)         : {len(case_a)}")
    print(f"  B (UPDATE 131024 bsr main_rank, page='bsr'): {len(case_b)}")
    print(f"  C (INSERT 신규 detail crawl 필요)         : {len(case_c)}")
    print(f"  D (DELETE 162428 에서 빠진 obsolete main): {len(case_d)}")
    print(f"  검증: A+B+C = {len(case_a)+len(case_b)+len(case_c)} (162428 ASIN 200 이어야 함)")
    print(f"        A+D = {len(case_a)+len(case_d)} (131024 main 199 이어야 함)")

    # 3) D first — DELETE obsolete (potential unique-key conflict 회피)
    apply_case_d_delete(work_conn, case_d)

    # 4) A, B updates
    apply_case_a_update(work_conn, case_a)
    apply_case_b_update(work_conn, case_b)

    # 5) C: detail crawl + INSERT
    if case_c:
        crawler = AmazonDetailCrawler()
        print(f"\n[C-SETUP] connect_db ...")
        if not crawler.connect_db():
            print("[ERROR] connect_db failed — abort case C (A/B/D 결과는 유지됨)")
            work_conn.close()
            return
        print(f"[C-SETUP] load_xpaths ...")
        if not crawler.load_xpaths():
            print("[ERROR] load_xpaths failed — abort case C")
            work_conn.close()
            return
        print(f"[C-SETUP] setup_driver ...")
        crawler.setup_driver()  # 내부에서 load_cookies 자동 호출
        print(f"[C-SETUP] driver ready")

        process_case_c(crawler, case_c)

        try:
            if crawler.page:
                crawler.page.quit()
        except Exception:
            pass
        try:
            if crawler.db_conn:
                crawler.db_conn.close()
        except Exception:
            pass
    else:
        print("\n[C] skip — no missing ASINs")

    # 6) Final verification
    report_final(work_conn)
    work_conn.close()

    print("\n[DONE]")


if __name__ == '__main__':
    main()
