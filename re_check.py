"""
recovery 전 진단: config DB xpath + 실제 페이지 HTML 비교
1건만 빠르게 확인
"""
import re
import sys
import psycopg2
from DrissionPage import ChromiumPage, ChromiumOptions
from lxml import html, etree
from config import DB_CONFIG
from bby_config_loader import get_config

# 인자로 URL, 없으면 DB에서 첫 번째 NULL 레코드
TEST_URL = sys.argv[1] if len(sys.argv) > 1 else None


def main():
    config = get_config()
    file_name = 'bby_tv_dt1'

    # URL 결정
    url = TEST_URL
    if not url:
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()
        cursor.execute("""
            SELECT product_url FROM bby_tv_crawl
            WHERE account_name = 'Bestbuy'
              AND crawl_datetime >= '2026-02-27 00:00:00'
              AND original_sku_price IS NULL AND savings IS NULL
              AND final_sku_price IS NOT NULL
              AND final_sku_price NOT IN ('no longer available', 'See price in cart')
              AND final_sku_price LIKE '%%$%%'
            ORDER BY
              CASE WHEN final_sku_price ~ '\\$[0-9,]+' THEN
                CAST(REPLACE(REPLACE(final_sku_price, '$', ''), ',', '') AS NUMERIC)
              ELSE 0 END DESC
            LIMIT 1
        """)
        row = cursor.fetchone()
        cursor.close()
        conn.close()
        if row:
            url = row[0]
            print(f"[DB] Most expensive NULL product: {url}")
        else:
            print("[ERROR] No NULL records found")
            return

    # 1. config DB 내용 확인
    print(f"\n{'='*80}")
    print("[1] Config DB xpath entries")
    print(f"{'='*80}")
    keys = ['price_block_container', 'savings_inner', 'original_price_inner', 'final_price_inner']
    for key in keys:
        xpaths = config.get_xpath_list(key, file_name)
        print(f"\n  {key}:")
        if xpaths:
            for i, x in enumerate(xpaths):
                print(f"    [{i+1}] {x}")
        else:
            print(f"    (None - using defaults)")

    # 2. 브라우저로 페이지 로드
    print(f"\n{'='*80}")
    print(f"[2] Loading page: {url}")
    print(f"{'='*80}")
    co = ChromiumOptions()
    co.no_imgs(True)
    page = ChromiumPage(co)

    try:
        page.get(url)
        h1 = page.ele('xpath://h1[contains(@class, "h4") or contains(@class, "heading")]', timeout=20)
        if h1:
            print(f"  [OK] Title: {h1.text[:80]}")
        else:
            print("  [ERROR] h1 not found")
            return

        # 가격 컨테이너 대기
        try:
            page.ele(
                'xpath://div[@data-testid="price-block-customer-price"] | //div[@data-testid="price-restricted-price-tap-for-price"]',
                timeout=10
            )
        except:
            pass

        page_source = page.html
        tree = html.fromstring(page_source)

        # 3. price-block 컨테이너 찾기
        print(f"\n{'='*80}")
        print("[3] price-block container search")
        print(f"{'='*80}")
        container_xpaths = config.get_xpath_list('price_block_container', file_name) or [
            '//div[@data-testid="price-block"]',
            '//div[contains(@class, "order-2")]'
        ]
        price_container = None
        for i, xpath in enumerate(container_xpaths):
            found = tree.xpath(xpath)
            print(f"  xpath[{i+1}] {xpath} -> {len(found)} matches")
            if found and price_container is None:
                price_container = found[0]

        if price_container is None:
            print("  [FAIL] Container not found!")
            return

        # 4. 컨테이너 내부 HTML 덤프
        print(f"\n{'='*80}")
        print("[4] Container inner HTML (2000 chars)")
        print(f"{'='*80}")
        inner = etree.tostring(price_container, encoding='unicode', method='html')
        print(inner[:2000])

        # 5. 컨테이너 내 data-testid 전수 조사
        print(f"\n{'='*80}")
        print("[5] All data-testid inside container")
        print(f"{'='*80}")
        for el in price_container.xpath('.//*[@data-testid]'):
            t = el.text_content().strip()[:60]
            print(f"  <{el.tag} data-testid=\"{el.get('data-testid')}\"> text=\"{t}\"")

        # 6. 컨테이너 내 data-lu-target 전수 조사
        print(f"\n{'='*80}")
        print("[6] All data-lu-target inside container")
        print(f"{'='*80}")
        for el in price_container.xpath('.//*[@data-lu-target]'):
            t = el.text_content().strip()[:60]
            print(f"  <{el.tag} data-lu-target=\"{el.get('data-lu-target')}\"> text=\"{t}\"")

        # 7. savings xpath 각각 테스트
        print(f"\n{'='*80}")
        print("[7] Savings extraction test")
        print(f"{'='*80}")
        savings_xpaths = config.get_xpath_list('savings_inner', file_name) or [
            './/span[@data-testid="price-block-total-savings-text"]',
            './/div[@data-testid="price-block-total-savings"]//span',
            './/span[contains(@style, "color: rgb(232, 30, 37)") and contains(., "Save")]'
        ]
        for i, xpath in enumerate(savings_xpaths):
            found = price_container.xpath(xpath)
            if found:
                text = found[0].text_content().strip()
                match = re.search(r'\$[\d,]+(?:\.\d{2})?', text)
                print(f"  xpath[{i+1}] {xpath}")
                print(f"    -> FOUND: \"{text}\" -> regex: {match.group() if match else 'NO MATCH'}")
            else:
                print(f"  xpath[{i+1}] {xpath} -> NOT FOUND")

        # 8. original_sku_price xpath 각각 테스트
        print(f"\n{'='*80}")
        print("[8] Original price extraction test")
        print(f"{'='*80}")
        orig_xpaths = config.get_xpath_list('original_price_inner', file_name) or [
            './/span[@data-lu-target="comp_value"]',
            './/span[@data-testid="price-block-regular-price-message-text"]//span[@data-lu-target="comp_value"]',
            './/span[contains(@style, "color: rgb(108, 111, 117)") and contains(., "$")]'
        ]
        for i, xpath in enumerate(orig_xpaths):
            found = price_container.xpath(xpath)
            if found:
                text = found[0].text_content().strip()
                print(f"  xpath[{i+1}] {xpath}")
                print(f"    -> FOUND: \"{text}\"")
            else:
                print(f"  xpath[{i+1}] {xpath} -> NOT FOUND")

        # 9. final_sku_price xpath 테스트
        print(f"\n{'='*80}")
        print("[9] Final price extraction test")
        print(f"{'='*80}")
        price_xpaths = config.get_xpath_list('final_price_inner', file_name) or [
            './/div[@data-testid="price-block-customer-price"]//span',
            './/div[@data-lu-target="customer_price"]//span',
        ]
        for i, xpath in enumerate(price_xpaths):
            found = price_container.xpath(xpath)
            if found:
                text = found[0].text_content().strip()
                print(f"  xpath[{i+1}] {xpath}")
                print(f"    -> FOUND: \"{text}\"")
            else:
                print(f"  xpath[{i+1}] {xpath} -> NOT FOUND")

        # 10. 전체 페이지에서 savings 관련 검색 (컨테이너 밖에 있을 수도)
        print(f"\n{'='*80}")
        print("[10] Full page search for savings elements")
        print(f"{'='*80}")
        for xpath in ['//span[@data-testid="price-block-total-savings-text"]',
                       '//div[@data-testid="price-block-total-savings"]',
                       '//*[contains(text(), "Save $")]',
                       '//*[@data-lu-target="comp_value"]',
                       '//*[contains(text(), "Was ")]']:
            found = tree.xpath(xpath)
            if found:
                for el in found[:3]:
                    t = el.text_content().strip()[:80]
                    print(f"  {xpath} -> FOUND: \"{t}\"")
            else:
                print(f"  {xpath} -> NOT FOUND")

    finally:
        page.quit()
        print("\n[INFO] Browser closed")


if __name__ == '__main__':
    main()
