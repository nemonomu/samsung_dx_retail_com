"""
recover_lost_bsr_20260430.py

replace_main_session_20260430.py 의 case D DELETE 가 main+bsr 둘 다 가진 행도 무차별
DELETE 하여 BSR 데이터 11건이 함께 사라졌음. 백업에서 11건 복구.

복구 정책:
  - 백업에서 11개 row 를 그대로 가져와 tv_retail_com 에 INSERT
  - 단, 다음 두 컬럼만 override:
      main_rank  → NULL  (162428 에 없으므로 main 데이터 무효)
      page_type  → 'bsr' (이제 BSR 에서만 발견되는 행)
  - bsr_rank, detail 데이터(가격/이름/리뷰 등) 모두 백업 값 그대로 보존

대상 식별:
  tv_retail_com_backup_20260430_131024 에 있고 bsr_rank IS NOT NULL 이며,
  현재 tv_retail_com 에 동일 id 가 없는 (= 삭제된) 행.

검증:
  복구 후 batch 131024 의 bsr_rank 보유 행 수 = 99 (BSR 소스 130754 와 일치)

실행:
  python -u recover_lost_bsr_20260430.py
"""

import psycopg2
from config import DB_CONFIG


TARGET_BATCH_ID = '20260430_131024'
BACKUP_TABLE = 'tv_retail_com_backup_20260430_131024'


def get_columns(cur):
    """tv_retail_com 의 컬럼 목록을 ordinal_position 순서로 반환."""
    cur.execute("""
        SELECT column_name
          FROM information_schema.columns
         WHERE table_name = 'tv_retail_com'
         ORDER BY ordinal_position
    """)
    return [r[0] for r in cur.fetchall()]


def find_lost_ids(cur):
    """백업에 있고 bsr_rank 있으며 현재 tv_retail_com 에 없는 row 의 id."""
    cur.execute(f"""
        SELECT b.id, b.bsr_rank, b.item, b.retailer_sku_name
          FROM "{BACKUP_TABLE}" b
         WHERE b.bsr_rank IS NOT NULL
           AND NOT EXISTS (SELECT 1 FROM tv_retail_com t WHERE t.id = b.id)
         ORDER BY b.bsr_rank
    """)
    rows = cur.fetchall()
    return rows


def restore(cur, lost_ids):
    """백업의 row 를 tv_retail_com 에 INSERT (main_rank=NULL, page_type='bsr' override)."""
    cols = get_columns(cur)

    # SELECT 절 빌드: main_rank/page_type 만 override
    select_parts = []
    for c in cols:
        if c == 'main_rank':
            select_parts.append('NULL AS main_rank')
        elif c == 'page_type':
            select_parts.append("'bsr' AS page_type")
        else:
            select_parts.append(f'"{c}"')

    insert_cols = ', '.join(f'"{c}"' for c in cols)
    select_clause = ', '.join(select_parts)

    sql = f"""
        INSERT INTO tv_retail_com ({insert_cols})
        SELECT {select_clause}
          FROM "{BACKUP_TABLE}"
         WHERE id = ANY(%s)
    """
    cur.execute(sql, (lost_ids,))
    return cur.rowcount


def verify(cur):
    """복구 후 BSR 데이터 정합성 확인."""
    cur.execute("""
        SELECT COUNT(*) FILTER (WHERE bsr_rank IS NOT NULL)               AS bsr_rows,
               COUNT(DISTINCT bsr_rank) FILTER (WHERE bsr_rank IS NOT NULL) AS distinct_bsr_rank,
               MIN(bsr_rank), MAX(bsr_rank)
          FROM tv_retail_com
         WHERE batch_id = %s AND account_name = 'Amazon'
    """, (TARGET_BATCH_ID,))
    r = cur.fetchone()
    print(f"[VERIFY] bsr_rows={r[0]}, distinct_bsr_rank={r[1]}, min={r[2]}, max={r[3]}")

    # gap 검사
    cur.execute("""
        SELECT bsr_rank FROM tv_retail_com
         WHERE batch_id = %s AND account_name = 'Amazon' AND bsr_rank IS NOT NULL
         ORDER BY bsr_rank
    """, (TARGET_BATCH_ID,))
    ranks = [row[0] for row in cur.fetchall()]
    gaps = [r for r in range(1, max(ranks) + 1) if r not in set(ranks)]
    print(f"[VERIFY] bsr_rank 1~{max(ranks)} gaps = {len(gaps)} → {gaps}")

    # main_rank 중복 재검증 (사용자 핵심 요구)
    cur.execute("""
        SELECT main_rank, COUNT(*)
          FROM tv_retail_com
         WHERE batch_id = %s AND account_name = 'Amazon' AND main_rank IS NOT NULL
         GROUP BY main_rank HAVING COUNT(*) > 1
    """, (TARGET_BATCH_ID,))
    dups = cur.fetchall()
    if dups:
        print(f"[FATAL] main_rank 중복 발생: {dups}")
    else:
        print("[OK] main_rank 중복 0건")

    # page_type 별 row 분포
    cur.execute("""
        SELECT page_type, COUNT(*),
               COUNT(*) FILTER (WHERE main_rank IS NOT NULL) AS w_main,
               COUNT(*) FILTER (WHERE bsr_rank  IS NOT NULL) AS w_bsr
          FROM tv_retail_com
         WHERE batch_id = %s AND account_name = 'Amazon'
         GROUP BY page_type ORDER BY page_type
    """, (TARGET_BATCH_ID,))
    print("\n=== final batch state ===")
    print("page_type | rows | with_main_rank | with_bsr_rank")
    for r in cur.fetchall():
        print(r)


def main():
    print("=" * 80)
    print(f"BSR 데이터 11건 복구 (batch_id={TARGET_BATCH_ID})")
    print(f"  source backup: {BACKUP_TABLE}")
    print("=" * 80)

    conn = psycopg2.connect(**DB_CONFIG)
    conn.autocommit = False
    cur = conn.cursor()

    try:
        # 1) 식별
        lost = find_lost_ids(cur)
        print(f"\n[IDENTIFY] 복구 대상 (DELETED but had bsr_rank): {len(lost)} 건")
        for r in lost:
            print(f"  id={r[0]} bsr_rank={r[1]} item={r[2]} | {r[3][:60] if r[3] else 'NULL'}...")
        if not lost:
            print("[INFO] 복구할 row 없음 — 종료")
            conn.commit()
            return

        # 2) 복구 INSERT
        ids = [r[0] for r in lost]
        n = restore(cur, ids)
        print(f"\n[RESTORE] INSERT 된 row 수: {n}")

        # 3) 사후 검증 (commit 전에)
        verify(cur)

        # 4) commit
        conn.commit()
        print("\n[COMMIT] OK")

    except Exception as e:
        conn.rollback()
        print(f"\n[ROLLBACK] {type(e).__name__}: {e}")
        raise
    finally:
        cur.close()
        conn.close()

    print("\n[DONE]")


if __name__ == '__main__':
    main()
