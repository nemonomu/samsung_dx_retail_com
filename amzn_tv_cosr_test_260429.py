"""
amzn_tv_cosr_test_260429.py
count_of_star_ratings 추출만 검증하는 테스트 스크립트.
- DB(amazon_tv_config, is_active=TRUE) 의 xpath만 사용 (하드코딩 fallback 없음)
- 정규식 처리는 dt1/dt2 의 extract_count_of_star_ratings 와 동일
- DB 저장 없음. 콘솔 로그만.
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

URLS = [
    "https://www.amazon.com/VIZIO-Class-31-50-Diag-Full-Array/dp/B00SMBF3NS/",
    "https://www.amazon.com/LG-Electronics-43UF6430-43-Inch-Ultra/dp/B010Q8ETR0/",
    "https://www.amazon.com/Vizio-1080p-Full-Array-LED-Smart/dp/B01CNPMVU6/",
]


def load_cosr_xpaths():
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    cur.execute("""
        SELECT config_value, priority
        FROM amazon_tv_config
        WHERE category = 'xpath'
          AND config_key LIKE 'count_of_star_ratings%'
          AND is_active = TRUE
        ORDER BY priority
    """)
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return [r[0] for r in rows]


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
    """dt1/dt2 와 동일 정규식 순서. (값, 매치된 xpath, raw text) 반환."""
    for xpath in xpaths:
        total_text = extract_text_safe(tree, xpath)
        if not total_text:
            continue
        m = re.search(r'([\d,]+)\s*global ratings?', total_text)
        if m:
            return int(m.group(1).replace(',', '')), xpath, total_text, 'global ratings'
        m = re.search(r'([\d,]+)\s*ratings?', total_text)
        if m:
            return int(m.group(1).replace(',', '')), xpath, total_text, 'ratings'
        m = re.search(r'([\d,]+)', total_text)
        if m:
            return int(m.group(1).replace(',', '')), xpath, total_text, 'numeric only'
    return None, None, None, None


def setup_browser():
    co = ChromiumOptions()
    co.set_argument('--lang=en-US')
    page = ChromiumPage(co)
    page.set.window.max()
    return page


def load_cookies(page):
    if not os.path.exists(COOKIE_FILE):
        print(f"[WARNING] cookie file not found: {COOKIE_FILE} (skip)")
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
    """봇 차단 'Continue shopping' 페이지 발견 시 버튼 클릭."""
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


def main():
    xpaths = load_cosr_xpaths()
    print(f"[INFO] DB xpaths (active, count_of_star_ratings): {len(xpaths)}")
    for i, xp in enumerate(xpaths, 1):
        print(f"  {i}. {xp}")
    if not xpaths:
        print("[ERROR] no active xpaths in DB. abort.")
        sys.exit(1)

    page = setup_browser()
    try:
        load_cookies(page)
        for i, url in enumerate(URLS, 1):
            print(f"\n=== [{i}/{len(URLS)}] ===")
            print(f"URL: {url}")
            page.get(url)
            time.sleep(3)
            if bypass_continue_shopping(page):
                print("[INFO] bypassed 'Continue shopping' bot block")
            tree = html.fromstring(page.html)
            cosr, used_xp, raw, pattern = extract_count_of_star_ratings(tree, xpaths)
            print(f"  count_of_star_ratings = {cosr}")
            if used_xp:
                raw_preview = (raw[:200] + '...') if raw and len(raw) > 200 else raw
                print(f"    matched xpath : {used_xp}")
                print(f"    regex pattern : {pattern}")
                print(f"    raw text      : {raw_preview!r}")
            else:
                print("    [WARN] no xpath matched")
    finally:
        try:
            page.quit()
        except Exception:
            pass


if __name__ == '__main__':
    main()
