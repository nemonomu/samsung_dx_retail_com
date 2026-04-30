"""
Amazon TV — star_rating / count_of_star_ratings 복구 스크립트
(count_of_reviews 는 본 스크립트에서 절대 건드리지 않음 — 지시사항)

대상:
  - tv_retail_com에서 account_name='Amazon', page_type='main',
    batch_id IN ('20260430_010909', '20260429_152852'),
    star_rating IS NULL AND count_of_star_ratings IS NULL 인 행 (총 88개 예상).

동작:
  1) tv_retail_com_backup_20260430 백업 테이블 생성 (이미 있으면 skip).
  2) 88개 NULL 행 로드 (id, item, product_url).
  3) AmazonDetailCrawler 인스턴스로 driver/cookies/xpaths 셋업.
  4) 각 URL: page.get → verify_page_loaded → _lazy_load_reviews →
     extract_star_rating / extract_count_of_star_ratings.
  5) 결과를 UPDATE만 수행 (INSERT 없음 — 중복 방지).
     star_rating, count_of_star_ratings 2개 컬럼만 갱신.
     star_rating == "No customer reviews"이면 cosr=0 으로만 보정 (cor 은 그대로 NULL).
  6) 1회만 시도 — 그래도 NULL이면 그대로 둠.
  7) 종료 시 batch_id별 잔여 NULL 카운트 출력.

실행:
  python recover_star_rating_20260430.py
"""

import sys
import time
import random
import psycopg2
from datetime import datetime
from lxml import html

from config import DB_CONFIG
from amazon_tv_dt1 import AmazonDetailCrawler


TARGET_BATCHES = ('20260430_010909', '20260429_152852')
BACKUP_TABLE = 'tv_retail_com_backup_20260430'
SOURCE_TABLE = 'tv_retail_com'


def ensure_backup(conn):
    """백업 테이블이 없을 때만 생성 (CREATE TABLE IF NOT EXISTS — 원자적, 충돌 없음)."""
    cur = conn.cursor()
    print(f"[BACKUP] ensuring '{BACKUP_TABLE}' (IF NOT EXISTS) ...")
    cur.execute(f'CREATE TABLE IF NOT EXISTS "{BACKUP_TABLE}" AS SELECT * FROM "{SOURCE_TABLE}"')
    cur.execute(f'SELECT COUNT(*) FROM "{BACKUP_TABLE}"')
    cnt = cur.fetchone()[0]
    print(f"[BACKUP] '{BACKUP_TABLE}' OK ({cnt} rows)")
    cur.close()


def load_null_rows(conn):
    """복구 대상 88개 (id, item, product_url) 로드."""
    cur = conn.cursor()
    cur.execute("""
        SELECT id, item, product_url, batch_id
          FROM tv_retail_com
         WHERE account_name='Amazon'
           AND page_type='main'
           AND batch_id IN %s
           AND star_rating IS NULL
           AND count_of_star_ratings IS NULL
         ORDER BY batch_id DESC, id
    """, (TARGET_BATCHES,))
    rows = cur.fetchall()
    cur.close()
    return rows


def update_row(conn, row_id, star_rating, count_of_star_ratings):
    """star_rating, count_of_star_ratings 2개 컬럼만 UPDATE (count_of_reviews는 건드리지 않음)."""
    cur = conn.cursor()
    cur.execute("""
        UPDATE tv_retail_com
           SET star_rating = %s,
               count_of_star_ratings = %s
         WHERE id = %s
    """, (star_rating, count_of_star_ratings, row_id))
    cur.close()


def report_residual_nulls(conn):
    cur = conn.cursor()
    cur.execute("""
        SELECT batch_id,
               COUNT(*) FILTER (WHERE star_rating IS NULL) as sr_null,
               COUNT(*) FILTER (WHERE count_of_star_ratings IS NULL) as cosr_null,
               COUNT(*) FILTER (WHERE star_rating IS NULL AND count_of_star_ratings IS NULL) as both_null
          FROM tv_retail_com
         WHERE account_name='Amazon' AND page_type='main' AND batch_id IN %s
         GROUP BY batch_id
         ORDER BY batch_id DESC
    """, (TARGET_BATCHES,))
    print("\n=== Residual NULL counts after recovery ===")
    print("batch_id | sr_null | cosr_null | both_null")
    for r in cur.fetchall():
        print(r)
    cur.close()


def main():
    print("=" * 80)
    print("Amazon TV — star_rating recovery script (batches: %s)" % (TARGET_BATCHES,))
    print("=" * 80)

    # Step 0: backup connection (autocommit on so DDL is committed)
    backup_conn = psycopg2.connect(**DB_CONFIG)
    backup_conn.autocommit = True
    ensure_backup(backup_conn)
    backup_conn.close()

    # Step 1: load NULL rows
    work_conn = psycopg2.connect(**DB_CONFIG)
    work_conn.autocommit = True
    null_rows = load_null_rows(work_conn)
    print(f"\n[INFO] {len(null_rows)} NULL rows to recover")
    if not null_rows:
        print("[INFO] Nothing to do — exiting.")
        work_conn.close()
        return

    # Step 2: setup crawler (db / xpaths / driver / cookies)
    crawler = AmazonDetailCrawler()
    print("\n[STEP] connect_db ...")
    if not crawler.connect_db():
        print("[ERROR] connect_db failed — abort.")
        work_conn.close()
        return

    print("[STEP] load_xpaths ...")
    if not crawler.load_xpaths():
        print("[ERROR] load_xpaths failed — abort.")
        work_conn.close()
        return

    print("[STEP] setup_driver ...")
    crawler.setup_driver()  # 내부에서 load_cookies 자동 호출 (amazon_tv_dt1.py:420)
    print("[OK] driver ready")

    # Step 3: per-row recovery
    success = 0
    no_review = 0
    still_null = 0
    failures = []

    for idx, (row_id, item, product_url, batch_id) in enumerate(null_rows, start=1):
        print(f"\n{'-'*80}")
        print(f"[{idx}/{len(null_rows)}] id={row_id} item={item} batch={batch_id}")
        print(f"  URL: {product_url[:90]}...")

        sr = None
        cosr = None

        try:
            crawler.page.get(product_url)
            time.sleep(random.uniform(3, 5))

            page_loaded = crawler.verify_page_loaded(wait_for_price=False)
            if not page_loaded:
                print("  [RETRY] page verification failed — re-accessing URL")
                time.sleep(3)
                crawler.page.get(product_url)
                time.sleep(random.uniform(3, 5))
                page_loaded = crawler.verify_page_loaded(wait_for_price=False)

            if not page_loaded:
                print("  [WARN] page still not verified — extracting from current DOM anyway")

            crawler._lazy_load_reviews()
            tree = html.fromstring(crawler.page.html)

            sr = crawler.extract_star_rating(tree)
            cosr = crawler.extract_count_of_star_ratings(tree)

            # amazon_tv_dt1.py:2085 의 "No customer reviews" 케이스: cosr=0 으로 보정
            # (count_of_reviews 는 사용자 지시에 따라 본 스크립트에서는 절대 건드리지 않음)
            if sr == "No customer reviews":
                cosr = 0
                no_review += 1

            # FALLBACK: 사용자 제공 xpath 단일 위치에서만 "No customer reviews" 검출
            # 위치: //*[@id="cr-top-reviews"]/div[2]/div/div/div (Amazon 신규 레이아웃)
            if sr is None and cosr is None:
                no_rev_text = crawler.extract_text_safe(
                    tree, '//*[@id="cr-top-reviews"]/div[2]/div/div/div'
                )
                if no_rev_text and 'No customer reviews' in no_rev_text:
                    sr = "No customer reviews"
                    cosr = 0
                    no_review += 1
                    print("  [FALLBACK] cr-top-reviews/div[2]/div/div/div = 'No customer reviews' → 보정")

            print(f"  [EXTRACT] star_rating={sr!r} cosr={cosr!r}")

            if sr is None and cosr is None:
                still_null += 1
                print("  [RESULT] still NULL — leaving row as-is per spec")
            else:
                update_row(work_conn, row_id, sr, cosr)
                success += 1
                print(f"  [RESULT] UPDATED id={row_id}")

        except Exception as e:
            print(f"  [ERROR] {type(e).__name__}: {e}")
            failures.append((row_id, product_url, str(e)))

        # 페이지 간 랜덤 딜레이
        delay = random.uniform(2, 4)
        time.sleep(delay)

    # Step 4: summary + residual NULL report
    print("\n" + "=" * 80)
    print("RECOVERY SUMMARY")
    print("=" * 80)
    print(f"  total target rows : {len(null_rows)}")
    print(f"  updated           : {success}")
    print(f"    of which 'No customer reviews' : {no_review}")
    print(f"  still NULL (skip) : {still_null}")
    print(f"  exceptions        : {len(failures)}")
    if failures:
        print("\n  failures:")
        for row_id, url, err in failures:
            print(f"    id={row_id} | {url[:80]} | {err}")

    report_residual_nulls(work_conn)

    # Step 5: cleanup
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
    work_conn.close()
    print("\n[DONE]")


if __name__ == '__main__':
    main()
