"""
Walmart 할인 상품 DOM 구조 덤프 (진단용)
- 할인 있는 단일 URL에서 savings/original_price/discount_type 영역의 HTML을 덤프
- DB 저장 없음, 로그만 출력
"""
import time
import random
from DrissionPage import ChromiumPage, ChromiumOptions
from lxml import html, etree


TEST_URL = "https://www.walmart.com/ip/VIZIO-32-Class-Full-HD-1080p-LED-Smart-TV-New-VFD32M-0807/5337847390"

# 현재 제안 xpath
CURRENT_XPATHS = {
    'discount_type':  '//*[@id="maincontent"]/main/section/div[2]/div[2]/div/div[3]/div/div[1]/div/div[2]/div/div/div[2]/span[1]',
    'savings':        '//*[@id="maincontent"]/main/section/div[2]/div[2]/div/div[3]/div/div[1]/div/div[2]/div/div/div[2]/span[2]',
    'offer':          '//*[@id="maincontent"]/main/section/div[2]/div[2]/div/div[3]/div/div[1]/div/div[2]/div/div/section[1]/div/div',
    'original_price': '//span[@data-seo-id="strike-through-price"]',
}

# 덤프 대상 영역 (상위 컨테이너 여러 수준)
DUMP_AREAS = {
    'price_block_container (div[2])':
        '//*[@id="maincontent"]/main/section/div[2]/div[2]/div/div[3]/div/div[1]/div/div[2]/div/div',
    'savings_parent (div[2]/div[2])':
        '//*[@id="maincontent"]/main/section/div[2]/div[2]/div/div[3]/div/div[1]/div/div[2]/div/div/div[2]',
    'original_price_parent (div[1])':
        '//*[@id="maincontent"]/main/section/div[2]/div[2]/div/div[3]/div/div[1]/div/div[2]/div/div/div[1]',
}

# 추가 후보 xpath (속성/클래스 기반)
CANDIDATE_XPATHS = {
    'strike-through-price (attr)': '//span[@data-seo-id="strike-through-price"]',
    'dollar-saved (attr)':         '//span[@data-seo-id="dollar-saved"]',
    'price-was (class contains)':  '//span[contains(@class, "strike")]',
    'green-text (color)':          '//span[contains(@style, "267A03")]',
    'savings-container':           '//*[contains(text(), "You save") or contains(text(), "Savings")]',
    'rollback-badge':               '//*[contains(text(), "Rollback") or contains(text(), "Reduced price") or contains(text(), "Flash Deal")]',
}


def main():
    print("=" * 80)
    print("  Walmart DOM 덤프 (할인 상품 진단)")
    print("=" * 80)
    print(f"URL: {TEST_URL}\n")

    co = ChromiumOptions()
    co.no_imgs(True)
    page = ChromiumPage(co)

    print("[INFO] 홈페이지 웜업...")
    page.get("https://www.walmart.com")
    time.sleep(random.uniform(5, 8))

    print("[INFO] 타겟 URL 접속...")
    page.get(TEST_URL)
    try:
        page.ele('xpath://h1', timeout=15)
    except Exception:
        print("  [WARNING] h1 timeout")
    time.sleep(random.uniform(3, 5))

    tree = html.fromstring(page.html)

    # 1) 현재 제안 xpath가 잡는 값
    print("\n" + "=" * 80)
    print("  [1] 현재 제안 xpath 결과")
    print("=" * 80)
    for key, xp in CURRENT_XPATHS.items():
        elems = tree.xpath(xp)
        if elems:
            el = elems[0]
            if isinstance(el, str):
                print(f"  {key:20s} | matched=1 | text={el!r}")
            else:
                txt = el.text_content().strip()
                print(f"  {key:20s} | matched={len(elems)} | text={txt!r}")
                inner = etree.tostring(el, encoding='unicode', method='html')
                print(f"  {'':20s}   inner: {inner[:300]}")
        else:
            print(f"  {key:20s} | matched=0 | (no element)")

    # 2) 컨테이너 영역 HTML 덤프
    print("\n" + "=" * 80)
    print("  [2] 컨테이너 HTML 덤프")
    print("=" * 80)
    for label, xp in DUMP_AREAS.items():
        elems = tree.xpath(xp)
        print(f"\n  --- {label} (matched: {len(elems)}) ---")
        if elems:
            inner = etree.tostring(elems[0], encoding='unicode', method='html')
            print(inner[:2500])

    # 3) 후보 xpath 매칭 확인
    print("\n" + "=" * 80)
    print("  [3] 후보 xpath 매칭 확인")
    print("=" * 80)
    for label, xp in CANDIDATE_XPATHS.items():
        elems = tree.xpath(xp)
        print(f"\n  {label} ({xp})")
        print(f"    matched: {len(elems)}")
        for i, el in enumerate(elems[:3]):
            if isinstance(el, str):
                print(f"    [{i}] text={el!r}")
            else:
                print(f"    [{i}] text={el.text_content().strip()!r}")
                inner = etree.tostring(el, encoding='unicode', method='html')
                print(f"        inner: {inner[:200]}")

    try:
        page.quit()
    except Exception:
        pass


if __name__ == "__main__":
    main()
