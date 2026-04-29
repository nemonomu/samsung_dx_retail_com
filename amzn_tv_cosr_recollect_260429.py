"""
amzn_tv_cosr_recollect_260429.py
batch_id='20260429_010932' 안에서 count_of_star_ratings='5' 인 행만 재수집해
amazon_tv_detail_crawled, tv_retail_com 양쪽을 UPDATE.

- 백업 테이블:
    amazon_tv_detail_crawled_bak_20260429_010932
    tv_retail_com_bak_20260429_010932  (account_name='Amazon')
- DB 활성 xpath만 사용 (하드코딩 fallback 없음, dt1/dt2와 동일 정규식)
- 추출 실패 시 UPDATE 스킵하고 [WARN] 로그
- WHERE 조건에 batch_id + count_of_star_ratings='5' 두 가지 모두 포함 (오작동 방지)
"""

import os
import sys
import time
import pickle
import re
import psycopg2
from lxml import html
from DrissionPage import ChromiumPage, ChromiumOptions

if sys.stdout.encoding != 'utf-8':
    sys.stdout.reconfigure(encoding='utf-8', errors='replace')

from config import DB_CONFIG, AMAZON_ACCOUNTS
from amazon_config_loader import get_amazon_config

_config = get_amazon_config()
ACCOUNT_NAME = _config.get_account('primary', 'amazon_tv_dt1') or 'ltyinvestmentl'
COOKIE_FILE = AMAZON_ACCOUNTS[ACCOUNT_NAME]['cookie_file']

TARGET_BATCH = '20260429_010932'
TARGET_VALUE = '5'
PAGE_SLEEP = 3


def load_cosr_xpaths(cur):
    cur.execute("""
        SELECT config_value
        FROM amazon_tv_config
        WHERE category = 'xpath'
          AND config_key LIKE 'count_of_star_ratings%'
          AND is_active = TRUE
        ORDER BY priority
    """)
    return [r[0] for r in cur.fetchall()]


def load_target_urls(cur):
    cur.execute("""
        SELECT DISTINCT product_url
        FROM amazon_tv_detail_crawled
        WHERE batch_id = %s
          AND count_of_star_ratings = %s
          AND product_url IS NOT NULL
        ORDER BY product_url
    """, (TARGET_BATCH, TARGET_VALUE))
    return [r[0] for r in cur.fetchall()]


def extract_text_safe(tree, xpath):
    if not xpath:
        return None
    try:
        elements = tree.xpath(xpath)
        if elements:
            if isinstance(elements[0], str):
                return elements[0].strip()
            return elements[0].text_content().strip()
        return None
    except Exception:
        return None


def extract_count_of_star_ratings(tree, xpaths):
    for xpath in xpaths:
        total_text = extract_text_safe(tree, xpath)
        if not total_text:
            continue
        m = re.search(r'([\d,]+)\s*global ratings?', total_text)
        if m:
            return int(m.group(1).replace(',', '')), xpath, total_text
        m = re.search(r'([\d,]+)\s*ratings?', total_text)
        if m:
            return int(m.group(1).replace(',', '')), xpath, total_text
        m = re.search(r'([\d,]+)', total_text)
        if m:
            return int(m.group(1).replace(',', '')), xpath, total_text
    return None, None, None


def setup_browser():
    co = ChromiumOptions()
    co.set_argument('--lang=en-US')
    page = ChromiumPage(co)
    page.set.window.max()
    return page


def load_cookies(page):
    if not os.path.exists(COOKIE_FILE):
        print(f"[WARN] cookie file not found: {COOKIE_FILE} (continuing)")
        return False
    homepage = _config.get_url('homepage') or 'https://www.amazon.com'
    page.get(homepage)
    time.sleep(2)
    page.set.cookies.clear()
    with open(COOKIE_FILE, 'rb') as f:
        cookies = pickle.load(f)
    loaded = 0
    for c in cookies:
        try:
            page.set.cookies(c)
            loaded += 1
        except Exception:
            pass
    print(f"[INFO] cookies loaded: {loaded}/{len(cookies)}")
    page.refresh()
    time.sleep(2)
    return True


def bypass_continue_shopping(page):
    try:
        if 'Continue shopping' in page.html:
            btn = page.ele('@type=submit', timeout=2)
            if btn:
                btn.click()
                time.sleep(2)
                return True
    except Exception:
        pass
    return False


def update_row(cur, url, new_value):
    """두 테이블 모두 UPDATE. WHERE에 batch_id + 기존값='5' 함께 포함."""
    cur.execute("""
        UPDATE amazon_tv_detail_crawled
           SET count_of_star_ratings = %s
         WHERE batch_id = %s
           AND product_url = %s
           AND count_of_star_ratings = %s
    """, (str(new_value), TARGET_BATCH, url, TARGET_VALUE))
    detail_n = cur.rowcount

    cur.execute("""
        UPDATE tv_retail_com
           SET count_of_star_ratings = %s
         WHERE batch_id = %s
           AND product_url = %s
           AND account_name = 'Amazon'
           AND count_of_star_ratings = %s
    """, (str(new_value), TARGET_BATCH, url, TARGET_VALUE))
    retail_n = cur.rowcount
    return detail_n, retail_n


def main():
    conn = psycopg2.connect(**DB_CONFIG)
    conn.autocommit = False
    cur = conn.cursor()

    xpaths = load_cosr_xpaths(cur)
    print(f"[INFO] DB xpaths: {len(xpaths)}")
    for i, xp in enumerate(xpaths, 1):
        print(f"  {i}. {xp}")

    urls = load_target_urls(cur)
    print(f"[INFO] target URLs (batch={TARGET_BATCH}, cosr='5'): {len(urls)}")
    if not urls:
        print("[INFO] nothing to do.")
        cur.close(); conn.close()
        return

    page = setup_browser()
    try:
        load_cookies(page)

        ok, fail, skipped = 0, 0, 0
        for i, url in enumerate(urls, 1):
            print(f"\n=== [{i}/{len(urls)}] {url[:120]}")
            try:
                page.get(url)
                time.sleep(PAGE_SLEEP)
                if bypass_continue_shopping(page):
                    print("  [INFO] bypassed Continue shopping")
                tree = html.fromstring(page.html)
                cosr, used_xp, raw = extract_count_of_star_ratings(tree, xpaths)
                if cosr is None:
                    print("  [WARN] extraction failed -> SKIP UPDATE")
                    fail += 1
                    continue
                if cosr == 5:
                    raw_prev = (raw[:120] + '...') if raw and len(raw) > 120 else raw
                    print(f"  [SKIP] re-extracted value is still 5 -> SKIP UPDATE  (xp={used_xp}, raw={raw_prev!r})")
                    skipped += 1
                    continue
                d, r = update_row(cur, url, cosr)
                conn.commit()
                print(f"  [OK] cosr={cosr}  (matched xp={used_xp})  detail_updated={d}  retail_updated={r}")
                ok += 1
            except Exception as e:
                conn.rollback()
                print(f"  [ERROR] {type(e).__name__}: {e}")
                fail += 1

        print(f"\n=== summary: ok={ok}, fail={fail}, still_5={skipped}, total={len(urls)} ===")
    finally:
        try:
            page.quit()
        except Exception:
            pass
        cur.close()
        conn.close()


if __name__ == '__main__':
    main()
