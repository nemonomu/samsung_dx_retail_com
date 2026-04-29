"""
Amazon TV review recovery for batch_id=20260429_010932 (260429)

대상:
  - amazon_tv_detail_crawled.batch_id = '20260429_010932'
  - count_of_star_ratings IS NOT NULL AND NOT IN ('0','0.0')
  - detailed_review_content IS NULL OR summarized_review_content IS NULL

각 행마다:
  1. product_url 익명 접속 (no cookies, no login)
  2. _lazy_load_reviews (scroll.to_bottom + cardCount 8s 안정화)
  3. extract_summarized_review / extract_detailed_reviews (DB priority XPath, lxml tree)
  4. UPDATE amazon_tv_detail_crawled, tv_retail_com (NULL이었던 컬럼만 채움)

amazon_tv_dt1.py post-260429 신규 추출 로직과 동일 코드 인라인 사본.
재실행 가능 (NULL인 행만 다시 SELECT). --limit N 으로 소수 테스트 가능.
"""

import re
import sys
import time
import random
import traceback

import psycopg2
from lxml import html
from DrissionPage import ChromiumPage, ChromiumOptions

from config import DB_CONFIG


BATCH_ID = '20260429_010932'


def parse_args():
    limit = None
    for arg in sys.argv[1:]:
        if arg.startswith('--limit='):
            limit = int(arg.split('=', 1)[1])
    return limit


def load_xpaths(conn):
    cur = conn.cursor()
    cur.execute("""
        SELECT config_key, config_value, priority
        FROM amazon_tv_config
        WHERE category='xpath' AND is_active=TRUE
        ORDER BY config_key, priority
    """)
    rows = cur.fetchall()
    cur.close()

    groups = {}
    for ck, cv, p in rows:
        m = re.match(r'^(.+?)_(\d+)$', ck)
        prefix = m.group(1) if m else ck
        groups.setdefault(prefix, []).append((p, cv))

    return {k: [v for _, v in sorted(es, key=lambda x: x[0])] for k, es in groups.items()}


def fetch_targets(conn, limit=None):
    cur = conn.cursor()
    sql = """
        SELECT batch_id, item, product_url,
               detailed_review_content,
               summarized_review_content
        FROM amazon_tv_detail_crawled
        WHERE batch_id = %s
          AND count_of_star_ratings IS NOT NULL
          AND count_of_star_ratings::text NOT IN ('0','0.0')
          AND (detailed_review_content IS NULL OR detailed_review_content=''
               OR summarized_review_content IS NULL OR summarized_review_content='')
        ORDER BY item
    """
    params = [BATCH_ID]
    if limit is not None:
        sql += " LIMIT %s"
        params.append(limit)
    cur.execute(sql, params)
    rows = cur.fetchall()
    cur.close()
    return rows


def lazy_load_reviews(page):
    try:
        page.scroll.to_bottom()
        prev_cards = -1
        stable_count = 0
        for _ in range(8):
            time.sleep(1)
            cards = page.run_js(
                'return document.querySelectorAll(\'[id^="customer_review-"], [id^="customer_review_foreign-"]\').length'
            ) or 0
            if cards == prev_cards and cards > 0:
                stable_count += 1
                if stable_count >= 2:
                    break
            else:
                stable_count = 0
                prev_cards = cards
        print(f"  [DEBUG] cards={prev_cards if prev_cards >= 0 else 0}")
    except Exception as e:
        print(f"  [WARN] lazy-load: {e}")


def extract_summarized_review(tree, xpaths):
    try:
        paths = xpaths.get('summarized_review') or []
        for xpath in paths:
            for el in tree.xpath(xpath):
                text = el.text_content() if hasattr(el, 'text_content') else str(el)
                text = ' '.join(text.split())
                if text:
                    return text
        return None
    except Exception:
        return None


def extract_detailed_reviews(tree, xpaths):
    try:
        body_xpaths = xpaths.get('review_body_detail') or []
        if not body_xpaths:
            return None

        review_texts = []
        for xpath in body_xpaths:
            elements = tree.xpath(xpath)
            if not elements:
                continue
            for el in elements:
                text = el.text_content() if hasattr(el, 'text_content') else str(el)
                text = ' '.join(text.split())
                text = re.sub(r'\s*Read more\s*$', '', text, flags=re.IGNORECASE)
                if text and len(text) > 5:
                    review_texts.append(text)
            if review_texts:
                break

        if not review_texts:
            return None

        seen = set()
        deduped = []
        for t in review_texts:
            if t not in seen:
                seen.add(t)
                deduped.append(t)

        formatted = [f"review{i} - {r}" for i, r in enumerate(deduped, 1)]
        return ' ||| '.join(formatted)

    except Exception:
        return None


def update_both_tables(conn, batch_id, item, drc_new, drc_was_null, src_new, src_was_null):
    """NULL이었던 컬럼만 새 값으로 UPDATE. 새 값이 None이면 그 컬럼은 건너뜀.

    두 테이블을 한 트랜잭션으로 묶어 부분 실패 방지.
    """
    sets = []
    params = []
    if drc_was_null and drc_new:
        sets.append('detailed_review_content = %s')
        params.append(drc_new)
    if src_was_null and src_new:
        sets.append('summarized_review_content = %s')
        params.append(src_new)

    if not sets:
        return None

    where_params = list(params) + [batch_id, item]
    set_clause = ', '.join(sets)

    prev_autocommit = conn.autocommit
    conn.autocommit = False
    cur = conn.cursor()
    try:
        cur.execute(
            f"UPDATE amazon_tv_detail_crawled SET {set_clause} WHERE batch_id=%s AND item=%s",
            where_params,
        )
        n_amz = cur.rowcount
        cur.execute(
            f"UPDATE tv_retail_com SET {set_clause} WHERE batch_id=%s AND item=%s",
            where_params,
        )
        n_trc = cur.rowcount
        conn.commit()
        return (n_amz, n_trc)
    except Exception:
        conn.rollback()
        raise
    finally:
        cur.close()
        conn.autocommit = prev_autocommit


def main():
    limit = parse_args()
    conn = psycopg2.connect(**DB_CONFIG)
    conn.autocommit = True
    print(f"[INFO] DB connected")

    xpaths = load_xpaths(conn)
    print(f"[INFO] Loaded {sum(len(v) for v in xpaths.values())} XPaths in {len(xpaths)} groups")

    targets = fetch_targets(conn, limit=limit)
    print(f"[INFO] {len(targets)} target rows (batch={BATCH_ID}, limit={limit})")
    if not targets:
        print("[OK] Nothing to update")
        conn.close()
        return

    co = ChromiumOptions()
    co.set_argument('--lang=en-US')
    page = ChromiumPage(co)
    page.set.window.max()
    print('[OK] Browser ready (anonymous)')

    stats = {'updated': 0, 'no_extract': 0, 'error': 0, 'amz_rows': 0, 'trc_rows': 0}

    try:
        for idx, (batch_id, item, url, cur_drc, cur_src) in enumerate(targets, 1):
            drc_was_null = cur_drc is None or cur_drc == ''
            src_was_null = cur_src is None or cur_src == ''
            print()
            print('=' * 80)
            print(f'[{idx}/{len(targets)}] item={item} drc_null={drc_was_null} src_null={src_was_null}')

            try:
                page.get(url)
                time.sleep(random.uniform(3, 5))
                lazy_load_reviews(page)
                tree = html.fromstring(page.html)

                drc = extract_detailed_reviews(tree, xpaths) if drc_was_null else None
                src = extract_summarized_review(tree, xpaths) if src_was_null else None

                drc_label = (f"OK ({len(drc.split(' ||| '))})" if drc else 'NULL') if drc_was_null else '-'
                src_label = ('OK' if src else 'NULL') if src_was_null else '-'
                print(f'  extract: drc={drc_label} src={src_label}')

                res = update_both_tables(conn, batch_id, item, drc, drc_was_null, src, src_was_null)
                if res is None:
                    stats['no_extract'] += 1
                    print('  [SKIP] no new values')
                else:
                    n_amz, n_trc = res
                    stats['updated'] += 1
                    stats['amz_rows'] += n_amz
                    stats['trc_rows'] += n_trc
                    print(f'  [UPDATED] amz={n_amz} trc={n_trc}')
            except Exception as e:
                stats['error'] += 1
                print(f'  [ERROR] {e}')
                traceback.print_exc()

            time.sleep(random.uniform(1, 2))

    finally:
        try:
            page.quit()
        except Exception:
            pass
        conn.close()

    print()
    print('=' * 80)
    print('SUMMARY')
    print(f"  updated rows (any column changed): {stats['updated']}")
    print(f"  skipped (no extracted value):      {stats['no_extract']}")
    print(f"  errors:                            {stats['error']}")
    print(f"  amazon_tv_detail_crawled UPDATEs:  {stats['amz_rows']}")
    print(f"  tv_retail_com UPDATEs:             {stats['trc_rows']}")


if __name__ == '__main__':
    main()
