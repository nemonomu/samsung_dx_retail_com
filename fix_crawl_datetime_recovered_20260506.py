"""
fix_crawl_datetime_recovered_20260506.py — 5/6 PAGE 2 복구 신규 34건의
crawl_datetime 을 자연스러운 분포로 재할당

배경:
  recover_bsr_page2_20260506.py 가 5/7 06:33 KST (= 5/6 21:22~21:33 UTC) 에 실행됨.
  신규 34건의 crawl_datetime 이 실행 시점 (UTC 21:22:00 ~ 21:33:26) 으로 들어감.
  batch_id (20260506_130824 = 5/6 04:08 UTC) 와 약 17시간 mismatch.

처리:
  - 신규 34 ASIN 한정
  - 기존 crawl_datetime 순서 보존 (= 원래 INSERT 순서) + ratio mapping
    old_first..old_last → new_first..new_last 로 linear scaling
  - amazon_tv_detail_crawled + tv_retail_com 양쪽 UPDATE
  - 단일 트랜잭션 (실패 시 자동 롤백)
  - 기존 백업 테이블 (*_bak_20260506_pg2) 그대로 유지 → 추가 백업 안 함

새 시간 범위 (사용자 지정):
  2026-05-06 01:27:38 ~ 2026-05-06 01:50:49

Usage (RDP 에서 직접):
    cd C:\\Users\\gomguard\\Documents\\퀵오일\\삼성전자\\samsung_dx_retail_com\\samsung_dx_retail_com
    git pull
    python -u fix_crawl_datetime_recovered_20260506.py
"""

import sys
from datetime import datetime, timedelta

import psycopg2

from config import DB_CONFIG


# === CONFIG ===
DT1_BATCH_ID = "20260506_130824"
NEW_START = datetime(2026, 5, 6, 1, 27, 38)
NEW_END = datetime(2026, 5, 6, 1, 50, 49)

# 신규 34 ASIN (recover_bsr_page2_20260506.py 결과 기준)
NEW_ASINS = [
    'B0F5WP8Y8G', 'B094RJ41WY', 'B0DK7C7YY5', 'B0DP14TVLY', 'B0D3H8ZJX3',
    'B0DYQGRHX3', 'B0D77DF966', 'B0DXMJFJ7W', 'B0DYR7GB61', 'B0CZM4SDK4',
    'B0FPSTJCS8', 'B0C7SRHGXF', 'B0F8C3VTVY', 'B0DFZT5V45', 'B0DYK871WY',
    'B0CVS18PH9', 'B0DWHB9DW4', 'B0DD2P7YVW', 'B0D92TBJX2', 'B0DXMZRG1Q',
    'B0G4B9MNCQ', 'B0GPSFJ5Q4', 'B0GP972QP1', 'B092PSB41B', 'B0DB6HGXGF',
    'B0CCRC1PP2', 'B0C1HZRXXN', 'B0FXMWY914', 'B0F1P92X1F', 'B0FCZTCJXN',
    'B0F8YDRXNY', 'B0FQV71QHH', 'B0CZ9WV2ZX', 'B0D22T2LPJ',
]


def parse_dt(value):
    """text 또는 timestamp/datetime 어떤 형태든 datetime 으로 정규화"""
    if isinstance(value, datetime):
        return value
    if isinstance(value, str):
        return datetime.strptime(value, '%Y-%m-%d %H:%M:%S')
    raise TypeError(f"Unexpected crawl_datetime type: {type(value)}")


def main():
    print("=" * 80)
    print(f"5/6 PAGE 2 신규 {len(NEW_ASINS)}건 crawl_datetime 재할당")
    print(f"  대상 batch: {DT1_BATCH_ID}")
    print(f"  새 시간 범위: {NEW_START} ~ {NEW_END}")
    print("=" * 80)

    conn = psycopg2.connect(**DB_CONFIG)
    conn.autocommit = False
    try:
        cur = conn.cursor()

        # 1. 기존 crawl_datetime 조회 (순서 보존)
        cur.execute("""
            SELECT item, crawl_datetime
              FROM amazon_tv_detail_crawled
             WHERE batch_id = %s AND account_name = 'Amazon'
               AND item = ANY(%s)
             ORDER BY crawl_datetime, item
        """, (DT1_BATCH_ID, NEW_ASINS))
        rows = cur.fetchall()

        if len(rows) != len(NEW_ASINS):
            print(f"[FATAL] 기대 {len(NEW_ASINS)} row, 실제 {len(rows)} row. 작업 중단.")
            conn.rollback()
            return 1

        old_first_dt = parse_dt(rows[0][1])
        old_last_dt = parse_dt(rows[-1][1])
        old_span_s = (old_last_dt - old_first_dt).total_seconds()
        new_span = NEW_END - NEW_START

        print(f"\n기존 분포: {old_first_dt} ~ {old_last_dt}  ({old_span_s:.0f}초)")
        print(f"새  분포: {NEW_START} ~ {NEW_END}  ({new_span.total_seconds():.0f}초)")

        # 2. ratio mapping 으로 새 timestamp 계산
        plan = []  # [(item, old_str, new_str)]
        for item, old_dt_raw in rows:
            old_dt = parse_dt(old_dt_raw)
            offset_s = (old_dt - old_first_dt).total_seconds()
            ratio = (offset_s / old_span_s) if old_span_s > 0 else 0
            new_dt = NEW_START + timedelta(seconds=new_span.total_seconds() * ratio)
            new_str = new_dt.strftime('%Y-%m-%d %H:%M:%S')
            old_str = old_dt.strftime('%Y-%m-%d %H:%M:%S')
            plan.append((item, old_str, new_str))

        # 3. 미리보기 (앞 5 + 뒤 3)
        print("\n--- UPDATE 미리보기 ---")
        for item, old, new in plan[:5]:
            print(f"  {item}: {old} → {new}")
        if len(plan) > 8:
            print(f"  ... ({len(plan) - 8} 건 생략) ...")
        for item, old, new in plan[-3:]:
            print(f"  {item}: {old} → {new}")

        # 4. UPDATE 양쪽 테이블
        print("\n[UPDATE] amazon_tv_detail_crawled + tv_retail_com ...")
        update_count_dt = 0
        update_count_tv = 0
        for item, _old, new_str in plan:
            cur.execute("""
                UPDATE amazon_tv_detail_crawled
                   SET crawl_datetime = %s
                 WHERE item = %s AND batch_id = %s AND account_name = 'Amazon'
            """, (new_str, item, DT1_BATCH_ID))
            update_count_dt += cur.rowcount
            cur.execute("""
                UPDATE tv_retail_com
                   SET crawl_datetime = %s
                 WHERE item = %s AND batch_id = %s AND account_name = 'Amazon'
            """, (new_str, item, DT1_BATCH_ID))
            update_count_tv += cur.rowcount

        print(f"  amazon_tv_detail_crawled: {update_count_dt} row updated")
        print(f"  tv_retail_com:            {update_count_tv} row updated")

        if update_count_dt != len(NEW_ASINS) or update_count_tv != len(NEW_ASINS):
            print(f"[FATAL] UPDATE row 수 mismatch (기대 {len(NEW_ASINS)}). 롤백.")
            conn.rollback()
            return 1

        conn.commit()
        cur.close()
        print("\n[COMMIT] 완료")

        # 5. 검증
        cur = conn.cursor()
        cur.execute("""
            SELECT MIN(crawl_datetime), MAX(crawl_datetime), COUNT(*)
              FROM amazon_tv_detail_crawled
             WHERE batch_id = %s AND account_name = 'Amazon' AND item = ANY(%s)
        """, (DT1_BATCH_ID, NEW_ASINS))
        print("\n--- 검증 (amazon_tv_detail_crawled, 신규 34) ---")
        print("  MIN, MAX, COUNT =", cur.fetchone())
        cur.execute("""
            SELECT MIN(crawl_datetime), MAX(crawl_datetime), COUNT(*)
              FROM tv_retail_com
             WHERE batch_id = %s AND account_name = 'Amazon' AND item = ANY(%s)
        """, (DT1_BATCH_ID, NEW_ASINS))
        print("--- 검증 (tv_retail_com, 신규 34) ---")
        print("  MIN, MAX, COUNT =", cur.fetchone())
        cur.close()

    except Exception as e:
        print(f"[FATAL] 예외 — 롤백: {e}")
        try:
            conn.rollback()
        except Exception:
            pass
        return 1
    finally:
        try:
            conn.close()
        except Exception:
            pass

    print("\n[DONE]")
    return 0


if __name__ == "__main__":
    sys.exit(main())
