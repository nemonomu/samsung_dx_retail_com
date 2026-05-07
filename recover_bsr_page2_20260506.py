"""
recover_bsr_page2_20260506.py — 5/6 batch BSR PAGE 2 누락 50개 복구 일회성 스크립트

5/6 batch:
  - main: 20260506_130014
  - bsr:  20260506_130620 (PAGE 1 50개만 수집됨 — PAGE 2 BSR containers not found)
  - dt1:  20260506_130824

처리 흐름:
  1. 백업 테이블 3개 (IF NOT EXISTS):
       - amazon_tv_bsr_bak_20260506_pg2
       - amazon_tv_detail_crawled_bak_20260506_pg2
       - tv_retail_com_bak_20260506_pg2
  2. PAGE 2 URL 조회 + AmazonBSRCrawler.scrape_page() → amazon_tv_bsr 에 50개 INSERT
     (bsr.batch_id 를 기존 20260506_130620 으로 강제 set)
  3. PAGE 2 의 50개 ASIN 분류:
       - 기존 dt1 batch_id 의 amazon_tv_detail_crawled 에 동일 ASIN 존재 → 중복
       - 그 외 → 신규
  4. 중복 ASIN: detail_crawled + tv_retail_com 의 bsr_rank UPDATE (COALESCE 보존)
  5. 신규 ASIN: AmazonDetailCrawler.scrape_detail_page() → 정상 INSERT
     (dt1.batch_id 를 기존 20260506_130824 로 강제 set)

실패 정책 (사용자 결정):
  - PAGE 2 BSR scrape_page 실패 → 전체 abort
  - 신규 ASIN detail crawl 중 실패 → 전체 abort
  (백업은 그대로 유지)

Usage (RDP 에서 직접 실행):
    cd C:\\Users\\gomguard\\Documents\\퀵오일\\삼성전자\\samsung_dx_retail_com\\samsung_dx_retail_com
    git pull
    python -u recover_bsr_page2_20260506.py
"""

import sys

from amazon_tv_bsr1 import AmazonBSRCrawler
from amazon_tv_dt1 import AmazonDetailCrawler


# === CONFIG ===
BSR_BATCH_ID = "20260506_130620"
DT1_BATCH_ID = "20260506_130824"
PAGE_NUMBER = 2
BACKUP_SUFFIX = "bak_20260506_pg2"


def create_backups(db_conn):
    """3개 테이블 백업 (IF NOT EXISTS 로 멱등성 보장)"""
    cur = db_conn.cursor()
    backup_specs = [
        ("amazon_tv_bsr",
         f"amazon_tv_bsr_{BACKUP_SUFFIX}",
         "batch_id = %s",
         (BSR_BATCH_ID,)),
        ("amazon_tv_detail_crawled",
         f"amazon_tv_detail_crawled_{BACKUP_SUFFIX}",
         "batch_id = %s",
         (DT1_BATCH_ID,)),
        ("tv_retail_com",
         f"tv_retail_com_{BACKUP_SUFFIX}",
         "batch_id = %s AND account_name = 'Amazon'",
         (DT1_BATCH_ID,)),
    ]
    for src, dst, where, params in backup_specs:
        cur.execute(f"""
            CREATE TABLE IF NOT EXISTS {dst} AS
            SELECT * FROM {src} WHERE {where}
        """, params)
        cur.execute(f"SELECT COUNT(*) FROM {dst}")
        cnt = cur.fetchone()[0]
        print(f"[OK] Backup ready: {dst} ({cnt} rows)")
    db_conn.commit()
    cur.close()


def get_page_url(db_conn, page_number):
    cur = db_conn.cursor()
    cur.execute("""
        SELECT url FROM amazon_tv_bsr_page_url
         WHERE is_active = TRUE AND page_number = %s
    """, (page_number,))
    row = cur.fetchone()
    cur.close()
    if not row:
        raise RuntimeError(f"PAGE {page_number} URL 못 찾음 (amazon_tv_bsr_page_url)")
    return row[0]


def main():
    print("=" * 80)
    print(f"5/6 BSR PAGE {PAGE_NUMBER} 복구 — bsr_batch={BSR_BATCH_ID}, dt1_batch={DT1_BATCH_ID}")
    print("=" * 80)

    bsr = AmazonBSRCrawler()
    bsr.connect_db()
    if not bsr.db_conn:
        print("[FATAL] BSR DB 연결 실패")
        return 1

    # === STEP 1: 백업 ===
    print("\n[STEP 1/4] 백업 테이블 생성")
    create_backups(bsr.db_conn)

    # === STEP 2: PAGE 2 BSR 수집 ===
    print(f"\n[STEP 2/4] PAGE {PAGE_NUMBER} BSR 수집 (amazon_tv_bsr INSERT, batch_id={BSR_BATCH_ID})")
    page_url = get_page_url(bsr.db_conn, PAGE_NUMBER)
    print(f"  URL: {page_url[:100]}...")

    bsr.batch_id = BSR_BATCH_ID  # 기존 batch 에 추가 INSERT
    if not bsr.load_xpaths():
        print("[FATAL] BSR xpath load 실패")
        bsr.db_conn.close()
        return 1
    bsr.load_excluded_items()
    bsr.setup_driver()

    page_success = bsr.scrape_page(page_url, PAGE_NUMBER)
    try:
        bsr.driver.quit()
    except Exception:
        pass

    if not page_success:
        print("[FATAL] PAGE 2 BSR 수집 실패. 작업 중단. (백업 그대로 유지)")
        bsr.db_conn.close()
        return 1
    print(f"[OK] PAGE 2 amazon_tv_bsr INSERT 완료 (수집 {bsr.total_collected} 건)")

    # === STEP 3: PAGE 2 ASIN 분류 ===
    print("\n[STEP 3/4] PAGE 2 ASIN 분류 (중복 vs 신규)")
    cur = bsr.db_conn.cursor()
    cur.execute("""
        SELECT product_url, bsr_rank
          FROM amazon_tv_bsr
         WHERE batch_id = %s AND bsr_rank > 50
         ORDER BY bsr_rank
    """, (BSR_BATCH_ID,))
    page2_rows = cur.fetchall()
    print(f"  amazon_tv_bsr 의 PAGE 2 row (rank > 50): {len(page2_rows)}")

    cur.execute("""
        SELECT DISTINCT item FROM amazon_tv_detail_crawled
         WHERE batch_id = %s AND item IS NOT NULL
    """, (DT1_BATCH_ID,))
    existing_asins = {r[0] for r in cur.fetchall()}
    print(f"  dt1 batch 의 기존 ASIN: {len(existing_asins)}")
    cur.close()

    duplicates = []  # [(asin, bsr_rank, url)]
    new_items = []   # [(asin, bsr_rank, url)]
    for url, rank in page2_rows:
        asin = bsr.extract_asin(url)
        if not asin:
            print(f"  [SKIP] ASIN 추출 실패: {url[:80]}")
            continue
        if asin in existing_asins:
            duplicates.append((asin, rank, url))
        else:
            new_items.append((asin, rank, url))
    print(f"  중복 (UPDATE): {len(duplicates)}")
    print(f"  신규 (detail crawl + INSERT): {len(new_items)}")

    # === 중복 ASIN: bsr_rank UPDATE (COALESCE 보존) ===
    if duplicates:
        cur = bsr.db_conn.cursor()
        for asin, rank, _u in duplicates:
            cur.execute("""
                UPDATE amazon_tv_detail_crawled
                   SET bsr_rank = COALESCE(bsr_rank, %s)
                 WHERE item = %s AND batch_id = %s
            """, (rank, asin, DT1_BATCH_ID))
            cur.execute("""
                UPDATE tv_retail_com
                   SET bsr_rank = COALESCE(bsr_rank, %s)
                 WHERE item = %s AND batch_id = %s AND account_name = 'Amazon'
            """, (rank, asin, DT1_BATCH_ID))
        bsr.db_conn.commit()
        cur.close()
        print(f"[OK] 중복 ASIN bsr_rank UPDATE 완료: {len(duplicates)}")

    # === STEP 4: 신규 ASIN detail crawl ===
    print(f"\n[STEP 4/4] 신규 ASIN detail crawl ({len(new_items)} 건)")
    if new_items:
        dt1 = AmazonDetailCrawler()
        if not dt1.connect_db():
            print("[FATAL] dt1 DB 연결 실패")
            bsr.db_conn.close()
            return 1
        if not dt1.load_xpaths():
            print("[FATAL] dt1 xpath load 실패")
            dt1.db_conn.close()
            bsr.db_conn.close()
            return 1
        dt1.batch_id = DT1_BATCH_ID  # 기존 batch 에 INSERT
        dt1.setup_driver()

        for idx, (asin, rank, url) in enumerate(new_items, 1):
            print(f"\n[NEW {idx}/{len(new_items)}] ASIN={asin}, bsr_rank={rank}")
            url_data = {
                'asin': asin,
                'page_type': 'bsr',
                'url': url,
                'main_rank': None,
                'bsr_rank': rank,
                'number_of_units_purchased_past_month': None,
            }
            try:
                ok = dt1.scrape_detail_page(url_data)
                if not ok:
                    print(f"[FATAL] ASIN {asin} detail crawl 실패. 작업 중단.")
                    try:
                        dt1.page.quit()
                    except Exception:
                        pass
                    dt1.db_conn.close()
                    bsr.db_conn.close()
                    return 1
            except Exception as e:
                print(f"[FATAL] ASIN {asin} 처리 중 예외: {e}. 작업 중단.")
                try:
                    dt1.page.quit()
                except Exception:
                    pass
                dt1.db_conn.close()
                bsr.db_conn.close()
                return 1

        try:
            dt1.page.quit()
        except Exception:
            pass
        dt1.db_conn.close()

    bsr.db_conn.close()

    print("\n" + "=" * 80)
    print(f"[DONE] 5/6 PAGE {PAGE_NUMBER} 복구 완료")
    print(f"  - 중복 UPDATE: {len(duplicates)}")
    print(f"  - 신규 INSERT: {len(new_items)}")
    print(f"  - 백업 테이블: amazon_tv_bsr_{BACKUP_SUFFIX}, "
          f"amazon_tv_detail_crawled_{BACKUP_SUFFIX}, "
          f"tv_retail_com_{BACKUP_SUFFIX}")
    print("=" * 80)
    return 0


if __name__ == "__main__":
    sys.exit(main())
