"""
recover_main_session_20260508.py — 5/8 main1 누락 복구 + main_rank 정합성

5/8 batch 정보:
  - main1 (20260508_130015): Chrome launch 실패 → 0 row INSERT
  - bsr1  (20260508_130046): 정상 — 98 row
  - dt1   (20260508_130319): 정상 진행, 단 12시간 이전 main batch (20260508_010012) URL 사용
                              → detail_crawled / tv_retail_com 의 main_rank 가 stale (12시간 mismatch)

처리:
  1. 백업 (detail_crawled / tv_retail_com — dt1 batch 한정)
  2. main1 재실행 (env AMAZON_TV_MAIN1_BATCH_ID=20260508_130015 으로 강제)
     → amazon_tv_main_crawled 에 5/8 정확한 batch INSERT
  3. 새 main batch ASIN/main_rank 매핑
  4. dt1 batch row 분류 + 처리:
     A. 양쪽 (new main ∩ dt1 main 처리됨): main_rank UPDATE (새 batch 값으로)
     B. dt1 only (사라진 ASIN — 이전 batch 에는 있고 새 batch 에는 없음):
        - bsr batch (20260508_130046) 에 있음 → main_rank=NULL UPDATE (bsr_rank 유지)
        - 없음 → row DELETE (detail_crawled + tv_retail_com)
     C. new main only (새로 등장 — dt1 처리 안 됨): scrape_detail_page 호출, dt1 batch_id 강제 set

실패 정책: main1 재실행 실패 또는 신규 ASIN crawl 실패 시 abort (백업 유지).

Usage (RDP 에서 직접):
    cd C:\\Users\\gomguard\\Documents\\퀵오일\\삼성전자\\samsung_dx_retail_com\\samsung_dx_retail_com
    git pull
    python -u recover_main_session_20260508.py
"""

import os
import re
import sys

import psycopg2

from config import DB_CONFIG


# === CONFIG ===
NEW_MAIN_BATCH = "20260508_130015"
DT1_BATCH = "20260508_130319"
BSR_BATCH = "20260508_130046"
BACKUP_SUFFIX = "bak_20260508_main_recovery"

# main1 재실행 위해 env 변수 set (AmazonTVCrawler.run() 안 line 704 가 env 읽음)
# 단 main1 import 가 env 읽기 전이어야 함 — env set 후 import
os.environ['AMAZON_TV_MAIN1_BATCH_ID'] = NEW_MAIN_BATCH

from amazon_tv_main1 import AmazonTVCrawler
from amazon_tv_dt1 import AmazonDetailCrawler


def extract_asin(url):
    if not url:
        return None
    m = re.search(r'/dp/([A-Z0-9]{10})', url)
    return m.group(1) if m else None


def create_backups():
    db_conn = psycopg2.connect(**DB_CONFIG)
    db_conn.autocommit = True
    cur = db_conn.cursor()
    backups = [
        ("amazon_tv_detail_crawled",
         f"amazon_tv_detail_crawled_{BACKUP_SUFFIX}",
         "batch_id = %s", (DT1_BATCH,)),
        ("tv_retail_com",
         f"tv_retail_com_{BACKUP_SUFFIX}",
         "batch_id = %s AND account_name = 'Amazon'", (DT1_BATCH,)),
    ]
    for src, dst, where, params in backups:
        cur.execute(f"CREATE TABLE IF NOT EXISTS {dst} AS SELECT * FROM {src} WHERE {where}", params)
        cur.execute(f"SELECT COUNT(*) FROM {dst}")
        cnt = cur.fetchone()[0]
        print(f"[OK] Backup: {dst} ({cnt} rows)")
    cur.close()
    db_conn.close()


def main():
    print("=" * 80)
    print("5/8 main1 누락 복구 + main_rank 정합성")
    print(f"  새 main batch:  {NEW_MAIN_BATCH}")
    print(f"  dt1 batch:      {DT1_BATCH}")
    print(f"  bsr batch:      {BSR_BATCH}")
    print("=" * 80)

    # === STEP 1: 백업 ===
    print("\n[STEP 1/5] 백업 테이블 생성")
    create_backups()

    # === STEP 2: main1 재실행 ===
    print(f"\n[STEP 2/5] main1 재실행 (env AMAZON_TV_MAIN1_BATCH_ID={NEW_MAIN_BATCH})")
    main_crawler = AmazonTVCrawler()
    main_crawler.run()  # run() 안에서 env 읽어 batch_id set + INSERT + driver.quit()

    # main1 INSERT row 수 검증
    db_conn = psycopg2.connect(**DB_CONFIG)
    db_conn.autocommit = False
    cur = db_conn.cursor()
    cur.execute("""
        SELECT COUNT(*) FROM amazon_tv_main_crawled WHERE batch_id = %s
    """, (NEW_MAIN_BATCH,))
    new_main_count = cur.fetchone()[0]
    print(f"\n[CHECK] amazon_tv_main_crawled batch={NEW_MAIN_BATCH} row: {new_main_count}")
    if new_main_count == 0:
        print("[FATAL] main1 재실행 후에도 0 row. 작업 중단.")
        cur.close()
        db_conn.close()
        return 1

    # === STEP 3: ASIN 매핑 + dt1 batch 분류 ===
    print("\n[STEP 3/5] ASIN 매핑 + 분류")
    cur.execute("""
        SELECT product_url, main_rank
          FROM amazon_tv_main_crawled
         WHERE batch_id = %s AND product_url IS NOT NULL
    """, (NEW_MAIN_BATCH,))
    new_main_map = {}  # asin -> (main_rank, url)
    for url, rank in cur.fetchall():
        asin = extract_asin(url)
        if asin:
            new_main_map[asin] = (rank, url)
    print(f"  새 main ASIN: {len(new_main_map)}")

    cur.execute("""
        SELECT DISTINCT item FROM amazon_tv_detail_crawled
         WHERE batch_id = %s AND account_name = 'Amazon'
           AND main_rank IS NOT NULL AND item IS NOT NULL
    """, (DT1_BATCH,))
    old_dt1_main_asins = {r[0] for r in cur.fetchall()}
    print(f"  dt1 batch main URL ASIN: {len(old_dt1_main_asins)}")

    cur.execute("""
        SELECT DISTINCT item FROM amazon_tv_bsr
         WHERE batch_id = %s AND item IS NOT NULL
    """, (BSR_BATCH,))
    bsr_asins = {r[0] for r in cur.fetchall()}
    print(f"  bsr batch ASIN: {len(bsr_asins)}")

    intersect = set(new_main_map.keys()) & old_dt1_main_asins
    only_old = old_dt1_main_asins - set(new_main_map.keys())
    only_new = set(new_main_map.keys()) - old_dt1_main_asins
    bsr_keep = only_old & bsr_asins
    delete_asins = only_old - bsr_asins

    print(f"\n  분류 결과:")
    print(f"    A. 양쪽 (main_rank UPDATE):       {len(intersect)}")
    print(f"    B. 사라짐 + BSR 있음 (NULL UPDATE): {len(bsr_keep)}")
    print(f"    B. 사라짐 + BSR 없음 (DELETE):     {len(delete_asins)}")
    print(f"    C. 새로 등장 (dt1 추가 수집):      {len(only_new)}")

    # === STEP 4: A + B UPDATE/DELETE ===
    print("\n[STEP 4/5] UPDATE + DELETE (단일 트랜잭션)")
    try:
        # A. main_rank UPDATE
        for asin in intersect:
            rank, _ = new_main_map[asin]
            cur.execute("""
                UPDATE amazon_tv_detail_crawled SET main_rank = %s
                 WHERE item = %s AND batch_id = %s
            """, (rank, asin, DT1_BATCH))
            cur.execute("""
                UPDATE tv_retail_com SET main_rank = %s
                 WHERE item = %s AND batch_id = %s AND account_name = 'Amazon'
            """, (rank, asin, DT1_BATCH))

        # B-1. main_rank=NULL UPDATE (BSR keep)
        for asin in bsr_keep:
            cur.execute("""
                UPDATE amazon_tv_detail_crawled SET main_rank = NULL
                 WHERE item = %s AND batch_id = %s
            """, (asin, DT1_BATCH))
            cur.execute("""
                UPDATE tv_retail_com SET main_rank = NULL
                 WHERE item = %s AND batch_id = %s AND account_name = 'Amazon'
            """, (asin, DT1_BATCH))

        # B-2. DELETE
        for asin in delete_asins:
            cur.execute("""
                DELETE FROM amazon_tv_detail_crawled
                 WHERE item = %s AND batch_id = %s
            """, (asin, DT1_BATCH))
            cur.execute("""
                DELETE FROM tv_retail_com
                 WHERE item = %s AND batch_id = %s AND account_name = 'Amazon'
            """, (asin, DT1_BATCH))

        db_conn.commit()
        print(f"  [OK] UPDATE/DELETE 트랜잭션 commit")
    except Exception as e:
        db_conn.rollback()
        print(f"[FATAL] UPDATE/DELETE 실패 → 롤백: {e}")
        cur.close()
        db_conn.close()
        return 1
    cur.close()
    db_conn.close()

    # === STEP 5: C 신규 ASIN dt1 처리 ===
    if only_new:
        print(f"\n[STEP 5/5] 신규 ASIN dt1 처리: {len(only_new)} 건")
        dt1 = AmazonDetailCrawler()
        if not dt1.connect_db():
            print("[FATAL] dt1 DB 연결 실패")
            return 1
        if not dt1.load_xpaths():
            print("[FATAL] dt1 xpath load 실패")
            dt1.db_conn.close()
            return 1
        dt1.batch_id = DT1_BATCH  # 기존 dt1 batch 에 INSERT
        dt1.setup_driver()

        for idx, asin in enumerate(sorted(only_new), 1):
            rank, url = new_main_map[asin]
            print(f"\n[NEW {idx}/{len(only_new)}] ASIN={asin}, main_rank={rank}")
            url_data = {
                'asin': asin,
                'page_type': 'main',
                'url': url,
                'main_rank': rank,
                'bsr_rank': None,
                'number_of_units_purchased_past_month': None,
            }
            try:
                ok = dt1.scrape_detail_page(url_data)
                if not ok:
                    print(f"[FATAL] ASIN {asin} crawl 실패. abort.")
                    try:
                        dt1.page.quit()
                    except Exception:
                        pass
                    dt1.db_conn.close()
                    return 1
            except Exception as e:
                print(f"[FATAL] ASIN {asin} 예외: {e}")
                try:
                    dt1.page.quit()
                except Exception:
                    pass
                dt1.db_conn.close()
                return 1

        try:
            dt1.page.quit()
        except Exception:
            pass
        dt1.db_conn.close()
    else:
        print("\n[STEP 5/5] 신규 ASIN 없음 — skip")

    print("\n" + "=" * 80)
    print("[DONE] 5/8 main1 복구 완료")
    print(f"  - main_rank UPDATE:      {len(intersect)}")
    print(f"  - main_rank NULL UPDATE: {len(bsr_keep)}")
    print(f"  - DELETE:                {len(delete_asins)}")
    print(f"  - 신규 INSERT:           {len(only_new)}")
    print(f"  - 백업: amazon_tv_detail_crawled_{BACKUP_SUFFIX}, tv_retail_com_{BACKUP_SUFFIX}")
    print("=" * 80)
    return 0


if __name__ == "__main__":
    sys.exit(main())
