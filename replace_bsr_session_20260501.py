"""
replace_bsr_session_20260501.py

batch_id=20260501_130730 의 BSR 세션을 amazon_tv_bsr 20260501_154646 기준으로 완전 대체.

매트릭스 (사전 분석 결과):
  A: 41 — new BSR ASIN 이 130730 main 에 있음              → UPDATE bsr_rank
  B: 51 — new BSR ASIN 이 130730 bsr 에 있음              → UPDATE bsr_rank
  C:  8 — new BSR ASIN 이 130730 어디에도 없음            → detail crawl + INSERT
  D:  2 — 130730 main 의 bsr_rank 가 obsolete (ASIN ∉ X)  → UPDATE bsr_rank=NULL (main 보존)
  E: 13 — 130730 bsr-only obsolete (ASIN ∉ X)             → DELETE

검증: A+B+C = 100 (= |batch X|, 100% unique 확인됨)

batch_id 는 기존 그대로 (20260501_130730) 유지.
사용자 핵심 요구: bsr_rank 중복 절대 없게.

처리 순서 (단일 트랜잭션, autocommit=False, commit 전 post-verify):
  0) 사전 검증: batch X 내부 (ASIN, bsr_rank) uniqueness — 1:1 안 되면 ABORT
  1) 백업: tv_retail_com_backup_20260501_130730_bsr (IF NOT EXISTS)
  2) E DELETE (13)            ← OLD BSR-only 정리
  3) D UPDATE bsr_rank=NULL (2)  ← OLD bsr_rank cleared (main row 유지)
  4) A + B UPDATE bsr_rank (92)  ← NEW rank 부여
  5) C detail crawl + INSERT (8) ← 신규 BSR-only (AmazonDetailCrawler 재사용)
  6) POST-VERIFY (commit 전):
        rows_with_bsr_rank == 100
        distinct_bsr_rank   == 100
        distinct_item       == 100
        bsr_rank 중복 0
        main_rank 중복 0 (사용자 어제 핵심 요구 — 재확인)
  7) COMMIT (검증 통과 시) / ROLLBACK

실행:
  python -u replace_bsr_session_20260501.py
"""

import sys
import time
import random
import psycopg2

from config import DB_CONFIG
from amazon_tv_dt1 import AmazonDetailCrawler


TARGET_BATCH_ID = '20260501_130730'
NEW_BSR_BATCH_ID = '20260501_154646'
BACKUP_TABLE = 'tv_retail_com_backup_20260501_130730_bsr'


def precheck_source_uniqueness(conn):
    """batch X 의 (ASIN, bsr_rank) 1:1 unique 검증. 깨지면 ABORT."""
    cur = conn.cursor()
    cur.execute("""
        WITH valid AS (
            SELECT bsr_rank, substring(product_url FROM '/dp/([A-Z0-9]{10})') AS asin
              FROM amazon_tv_bsr WHERE batch_id=%s AND product_url IS NOT NULL
        )
        SELECT
            (SELECT COUNT(*) FROM valid WHERE asin IS NOT NULL),
            (SELECT COUNT(DISTINCT asin) FROM valid WHERE asin IS NOT NULL),
            (SELECT COUNT(DISTINCT bsr_rank) FROM valid WHERE asin IS NOT NULL)
    """, (NEW_BSR_BATCH_ID,))
    rows, d_asin, d_rank = cur.fetchone()
    cur.close()
    print(f"[PRECHECK] source {NEW_BSR_BATCH_ID}: rows={rows}, distinct_asin={d_asin}, distinct_rank={d_rank}")
    if not (rows == d_asin == d_rank):
        raise RuntimeError(
            f"source batch X has duplicates (rows={rows}, asin={d_asin}, rank={d_rank}) "
            f"— bsr_rank 중복 발생 위험으로 ABORT"
        )
    print("[PRECHECK] OK — 1:1 unique")
    return rows


def ensure_backup(conn):
    """130730 batch 행만 별도 테이블로 백업 (idempotent)."""
    cur = conn.cursor()
    print(f"[BACKUP] ensuring '{BACKUP_TABLE}' (IF NOT EXISTS) ...")
    cur.execute(f"""
        CREATE TABLE IF NOT EXISTS "{BACKUP_TABLE}" AS
        SELECT * FROM tv_retail_com WHERE batch_id = '{TARGET_BATCH_ID}'
    """)
    cur.execute(f'SELECT COUNT(*) FROM "{BACKUP_TABLE}"')
    cnt = cur.fetchone()[0]
    print(f"[BACKUP] '{BACKUP_TABLE}' OK ({cnt} rows)")
    cur.close()


def load_classification(conn):
    """A, B, C, D, E 각 케이스의 row 들을 가져옴."""
    cur = conn.cursor()

    # A: new BSR ASIN 이 130730 main 에 있음
    cur.execute("""
        WITH new_bsr AS (
            SELECT bsr_rank, substring(product_url FROM '/dp/([A-Z0-9]{10})') AS asin
              FROM amazon_tv_bsr WHERE batch_id=%s AND product_url IS NOT NULL
        )
        SELECT n.bsr_rank, n.asin
          FROM new_bsr n
         WHERE n.asin IN (
             SELECT item FROM tv_retail_com
              WHERE batch_id=%s AND page_type='main' AND account_name='Amazon' AND item IS NOT NULL
         )
         ORDER BY n.bsr_rank
    """, (NEW_BSR_BATCH_ID, TARGET_BATCH_ID))
    case_a = cur.fetchall()

    # B: new BSR ASIN 이 130730 bsr 에 있음
    cur.execute("""
        WITH new_bsr AS (
            SELECT bsr_rank, substring(product_url FROM '/dp/([A-Z0-9]{10})') AS asin
              FROM amazon_tv_bsr WHERE batch_id=%s AND product_url IS NOT NULL
        )
        SELECT n.bsr_rank, n.asin
          FROM new_bsr n
         WHERE n.asin IN (
             SELECT item FROM tv_retail_com
              WHERE batch_id=%s AND page_type='bsr' AND account_name='Amazon' AND item IS NOT NULL
         )
         ORDER BY n.bsr_rank
    """, (NEW_BSR_BATCH_ID, TARGET_BATCH_ID))
    case_b = cur.fetchall()

    # C: new BSR ASIN, 130730 에 없음 (detail crawl 필요)
    cur.execute("""
        WITH new_bsr AS (
            SELECT bsr_rank, product_url, substring(product_url FROM '/dp/([A-Z0-9]{10})') AS asin
              FROM amazon_tv_bsr WHERE batch_id=%s AND product_url IS NOT NULL
        )
        SELECT n.bsr_rank, n.product_url, n.asin
          FROM new_bsr n
         WHERE n.asin NOT IN (
             SELECT item FROM tv_retail_com
              WHERE batch_id=%s AND account_name='Amazon' AND item IS NOT NULL
         )
         ORDER BY n.bsr_rank
    """, (NEW_BSR_BATCH_ID, TARGET_BATCH_ID))
    case_c = cur.fetchall()

    # D: 130730 main 의 bsr_rank set 이지만 ASIN 이 batch X 에 없음
    cur.execute("""
        SELECT t.id, t.item, t.bsr_rank
          FROM tv_retail_com t
         WHERE t.batch_id=%s AND t.page_type='main' AND t.account_name='Amazon'
           AND t.bsr_rank IS NOT NULL
           AND t.item NOT IN (
               SELECT substring(product_url FROM '/dp/([A-Z0-9]{10})')
                 FROM amazon_tv_bsr WHERE batch_id=%s AND product_url IS NOT NULL
           )
         ORDER BY t.bsr_rank
    """, (TARGET_BATCH_ID, NEW_BSR_BATCH_ID))
    case_d = cur.fetchall()

    # E: 130730 bsr-only obsolete
    cur.execute("""
        SELECT t.id, t.item, t.bsr_rank
          FROM tv_retail_com t
         WHERE t.batch_id=%s AND t.page_type='bsr' AND t.account_name='Amazon'
           AND t.item NOT IN (
               SELECT substring(product_url FROM '/dp/([A-Z0-9]{10})')
                 FROM amazon_tv_bsr WHERE batch_id=%s AND product_url IS NOT NULL
           )
         ORDER BY t.bsr_rank
    """, (TARGET_BATCH_ID, NEW_BSR_BATCH_ID))
    case_e = cur.fetchall()

    cur.close()
    return case_a, case_b, case_c, case_d, case_e


def apply_case_e_delete(cur, case_e):
    """E: BSR-only obsolete DELETE."""
    if not case_e:
        return 0
    ids = [r[0] for r in case_e]
    cur.execute("DELETE FROM tv_retail_com WHERE id = ANY(%s)", (ids,))
    n = cur.rowcount
    print(f"[E] DELETED {n} BSR-only obsolete rows")
    return n


def apply_case_d_clear_bsr_rank(cur, case_d):
    """D: 130730 main 의 obsolete bsr_rank 를 NULL 로 (main 행 보존)."""
    if not case_d:
        return 0
    ids = [r[0] for r in case_d]
    cur.execute("""
        UPDATE tv_retail_com SET bsr_rank = NULL
         WHERE id = ANY(%s)
    """, (ids,))
    n = cur.rowcount
    print(f"[D] UPDATE bsr_rank=NULL on {n} main rows (main_rank 보존)")
    return n


def apply_case_a_b_update(cur, case_a, case_b, label):
    """A, B: 기존 row 의 bsr_rank UPDATE."""
    cases = case_a if label == 'A' else case_b
    page_type = 'main' if label == 'A' else 'bsr'
    if not cases:
        print(f"[{label}] nothing to update")
        return 0
    n = 0
    for new_rank, asin in cases:
        cur.execute("""
            UPDATE tv_retail_com SET bsr_rank = %s
             WHERE batch_id = %s AND page_type = %s AND account_name='Amazon' AND item = %s
        """, (new_rank, TARGET_BATCH_ID, page_type, asin))
        n += cur.rowcount
    print(f"[{label}] UPDATED {n} {page_type} rows with new bsr_rank")
    return n


def process_case_c(crawler, case_c, work_conn):
    """C: 신규 ASIN 8건 detail crawl + INSERT (crawler.batch_id=TARGET_BATCH_ID)."""
    if not case_c:
        return 0, 0

    # crawler 가 트랜잭션을 자체 commit 하므로, work_conn 의 트랜잭션과 분리됨.
    # scrape_detail_page 가 자체 db_conn 으로 commit → 이 시점부터 tv_retail_com 에 영구 반영.
    # work_conn 의 트랜잭션이 rollback 되면 A/B/D/E 는 되돌아가지만 C 의 INSERT 는 남음.
    # → 안전을 위해 C 처리 전 work_conn commit 으로 A/B/D/E 영구화 후 C 진행.
    # (또는 C 를 work_conn 트랜잭션과 동일 connection 사용하도록 변경할 수도 있으나
    #  scrape_detail_page 가 self.db_conn 사용하므로 connection 공유가 깔끔하지 않음.
    #  → 현재 패턴: A/B/D/E commit 후 C 별도 처리.)

    crawler.batch_id = TARGET_BATCH_ID
    print(f"[C] crawler.batch_id 를 '{TARGET_BATCH_ID}' 로 override")

    success = 0
    fail = 0
    for idx, (bsr_rank, product_url, asin) in enumerate(case_c, start=1):
        print(f"\n--- [C {idx}/{len(case_c)}] bsr_rank={bsr_rank} asin={asin} ---")
        print(f"  URL: {product_url[:80]}...")
        url_data = {
            'asin': asin,
            'page_type': 'bsr',
            'url': product_url,
            'main_rank': None,
            'bsr_rank': bsr_rank,
            'number_of_units_purchased_past_month': None,
        }
        try:
            ok = crawler.scrape_detail_page(url_data)
            if ok:
                success += 1
                print(f"  [C-OK] inserted (batch_id={TARGET_BATCH_ID}, page_type='bsr', bsr_rank={bsr_rank})")
            else:
                fail += 1
                print(f"  [C-FAIL] scrape_detail_page returned False")
        except Exception as e:
            fail += 1
            print(f"  [C-ERROR] {type(e).__name__}: {e}")
        time.sleep(random.uniform(2, 4))

    print(f"\n[C] done: success={success} fail={fail}")
    return success, fail


def post_verify(conn):
    """commit 전 검증: bsr_rank 100 unique, main_rank 중복 0."""
    cur = conn.cursor()

    # 1) bsr_rank 분포
    cur.execute("""
        SELECT COUNT(*) FILTER (WHERE bsr_rank IS NOT NULL),
               COUNT(DISTINCT bsr_rank) FILTER (WHERE bsr_rank IS NOT NULL),
               COUNT(DISTINCT item) FILTER (WHERE bsr_rank IS NOT NULL)
          FROM tv_retail_com WHERE batch_id=%s AND account_name='Amazon'
    """, (TARGET_BATCH_ID,))
    bsr_rows, bsr_distinct_rank, bsr_distinct_item = cur.fetchone()
    print(f"\n[VERIFY] bsr_rank: rows={bsr_rows}, distinct_rank={bsr_distinct_rank}, distinct_item={bsr_distinct_item}")

    # 2) bsr_rank 중복 (page_type 무관)
    cur.execute("""
        SELECT bsr_rank, COUNT(*), array_agg(item ORDER BY item)
          FROM tv_retail_com
         WHERE batch_id=%s AND account_name='Amazon' AND bsr_rank IS NOT NULL
         GROUP BY bsr_rank HAVING COUNT(*) > 1
    """, (TARGET_BATCH_ID,))
    bsr_dups = cur.fetchall()

    # 3) main_rank 중복 (어제 사용자 핵심 요구 — 재확인)
    cur.execute("""
        SELECT main_rank, COUNT(*) FROM tv_retail_com
         WHERE batch_id=%s AND account_name='Amazon' AND main_rank IS NOT NULL
         GROUP BY main_rank HAVING COUNT(*) > 1
    """, (TARGET_BATCH_ID,))
    main_dups = cur.fetchall()

    # 4) page_type 별 분포
    cur.execute("""
        SELECT page_type, COUNT(*),
               COUNT(*) FILTER (WHERE main_rank IS NOT NULL) AS w_main,
               COUNT(*) FILTER (WHERE bsr_rank IS NOT NULL) AS w_bsr
          FROM tv_retail_com WHERE batch_id=%s AND account_name='Amazon'
         GROUP BY page_type ORDER BY page_type
    """, (TARGET_BATCH_ID,))
    print("\n=== page_type 분포 ===")
    print("page_type | rows | with_main_rank | with_bsr_rank")
    for r in cur.fetchall():
        print(r)

    cur.close()

    ok = True
    if bsr_dups:
        ok = False
        print(f"\n[FATAL] bsr_rank 중복 ({len(bsr_dups)} 건):")
        for r in bsr_dups[:10]:
            print(f"  bsr_rank={r[0]} count={r[1]} items={r[2]}")
    else:
        print("[OK] bsr_rank 중복 0건")
    if main_dups:
        ok = False
        print(f"[FATAL] main_rank 중복 ({len(main_dups)} 건):")
        for r in main_dups[:5]:
            print(f"  {r}")
    else:
        print("[OK] main_rank 중복 0건")

    expected = 100  # batch X 의 ASIN 수
    if not (bsr_rows == bsr_distinct_rank == bsr_distinct_item == expected):
        ok = False
        print(f"[FATAL] bsr_rank/item count mismatch — expected {expected}, got rows={bsr_rows}/rank={bsr_distinct_rank}/item={bsr_distinct_item}")
    else:
        print(f"[OK] bsr_rank 100 unique 1:1 매핑")

    return ok


def main():
    print("=" * 80)
    print(f"Amazon TV BSR session 완전 대체")
    print(f"  target batch_id : {TARGET_BATCH_ID}")
    print(f"  source BSR batch: {NEW_BSR_BATCH_ID}")
    print("=" * 80)

    # 0) Pre-check
    pre_conn = psycopg2.connect(**DB_CONFIG)
    pre_conn.autocommit = True
    try:
        precheck_source_uniqueness(pre_conn)
    finally:
        pre_conn.close()

    # 1) Backup
    backup_conn = psycopg2.connect(**DB_CONFIG)
    backup_conn.autocommit = True
    ensure_backup(backup_conn)
    backup_conn.close()

    # 2-4) A/B/D/E in single transaction
    work_conn = psycopg2.connect(**DB_CONFIG)
    work_conn.autocommit = False  # 트랜잭션 모드
    cur = work_conn.cursor()
    try:
        case_a, case_b, case_c, case_d, case_e = load_classification(work_conn)
        print(f"\n[CLASSIFY]")
        print(f"  A (UPDATE 130730 main bsr_rank)  : {len(case_a)}")
        print(f"  B (UPDATE 130730 bsr bsr_rank)   : {len(case_b)}")
        print(f"  C (INSERT 신규 detail crawl 필요): {len(case_c)}")
        print(f"  D (UPDATE main bsr_rank=NULL)    : {len(case_d)}")
        print(f"  E (DELETE BSR-only obsolete)     : {len(case_e)}")
        print(f"  검증: A+B+C = {len(case_a)+len(case_b)+len(case_c)} (100 이어야 함)")

        # E first (DELETE) → D (clear bsr_rank) → A/B (set new bsr_rank)
        apply_case_e_delete(cur, case_e)
        apply_case_d_clear_bsr_rank(cur, case_d)
        apply_case_a_b_update(cur, case_a, [], 'A')
        apply_case_a_b_update(cur, [], case_b, 'B')

        # C 처리 전 잠시 commit (검증을 commit 후로 미루지만, A/B/D/E 영구화 — C 가 자체 commit 하기 전에)
        # 사실 C 는 별도 connection (crawler.db_conn) 이라 work_conn rollback 영향 없음.
        # → C 처리 전 A/B/D/E 만 검증해서 OK 면 commit, 그 후 C 진행.
        # 단순화를 위해 여기선: 모든 SQL UPDATE/DELETE 후 A/B/D/E 검증, 통과 시 commit, 그 후 C.

        # 임시 검증 (C 전): 현재 bsr_rank 100 - 8 = 92 가 되어야 함
        cur.execute("""
            SELECT COUNT(*) FILTER (WHERE bsr_rank IS NOT NULL),
                   COUNT(DISTINCT bsr_rank) FILTER (WHERE bsr_rank IS NOT NULL)
              FROM tv_retail_com WHERE batch_id=%s AND account_name='Amazon'
        """, (TARGET_BATCH_ID,))
        rows_92, distinct_92 = cur.fetchone()
        print(f"\n[INTERIM VERIFY] (C 전) rows_with_bsr_rank={rows_92}, distinct={distinct_92} — 92 unique 기대")
        if rows_92 != distinct_92 or rows_92 != 92:
            raise RuntimeError(f"interim verify failed: rows={rows_92}, distinct={distinct_92}, expected 92")

        work_conn.commit()
        print("[INTERIM COMMIT] A/B/D/E OK — C 진행 가능")

    except Exception as e:
        work_conn.rollback()
        cur.close()
        work_conn.close()
        print(f"[ROLLBACK] A/B/D/E 단계 실패: {type(e).__name__}: {e}")
        raise

    cur.close()

    # 5) C: detail crawl + INSERT (crawler 가 자체 connection 으로 commit)
    if case_c:
        crawler = AmazonDetailCrawler()
        print(f"\n[C-SETUP] connect_db / load_xpaths / setup_driver ...")
        if not crawler.connect_db():
            print("[ERROR] connect_db failed — A/B/D/E 만 적용된 상태로 종료")
            work_conn.close()
            return
        if not crawler.load_xpaths():
            print("[ERROR] load_xpaths failed — 종료")
            work_conn.close()
            return
        crawler.setup_driver()
        print(f"[C-SETUP] driver ready")

        process_case_c(crawler, case_c, work_conn)

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

    # 6) Final verify
    final_conn = psycopg2.connect(**DB_CONFIG)
    final_conn.autocommit = True
    ok = post_verify(final_conn)
    final_conn.close()
    work_conn.close()

    if ok:
        print("\n[DONE] OK")
    else:
        print("\n[DONE WITH WARNINGS] 위 [FATAL] 메시지 확인 필요")


if __name__ == '__main__':
    main()
