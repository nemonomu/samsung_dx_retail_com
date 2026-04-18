"""
Walmart 신규 xpath 테스트 스크립트
- discount_type, savings, original_sku_price, offer 4개 필드만 추출
- DB 저장 없음, 로그만 출력
- wmart_tv_dt1.py의 추출 로직 그대로 복사
"""
import re
import time
import random
from DrissionPage import ChromiumPage, ChromiumOptions
from lxml import html


# ====================================================================
# 신규 xpath (테스트용 하드코딩)
# ====================================================================
XPATHS = {
    'discount_type':  '//*[@id="maincontent"]/main/section/div[2]/div[2]/div/div[3]/div/div[1]/div/div[2]/div/div/div[2]/span[1]',
    'savings':        '//*[@id="maincontent"]/main/section/div[2]/div[2]/div/div[3]/div/div[1]/div/div[2]/div/div/div[1]/section/div/div[2]/span[2]',
    'original_price': '//*[@id="maincontent"]/main/section/div[2]/div[2]/div/div[3]/div/div[1]/div/div[2]/div/div/div[1]/section/div/div[1]/span[2]',
    'offer':          '//*[@id="maincontent"]/main/section/div[2]/div[2]/div/div[3]/div/div[1]/div/div[2]/div/div/section[1]/div/div',
}


TEST_URLS = [
    "https://www.walmart.com/ip/VIZIO-32-Class-Full-HD-1080p-LED-Smart-TV-New-VFD32M-0807/5337847390",
    "https://www.walmart.com/ip/TCL-32Q2K-32-inch-Class-Q2K-Series-1080P-FHD-QLED-Smart-TV/17115511113",
    "https://www.walmart.com/ip/Samsung-UN40F6000FF-40-Smart-LCD-TV-HDTV-un40f6000ffxza/16231771458",
    "https://www.walmart.com/ip/Westinghouse-WD24HX1201-HD-LED-TV-2022/368877246",
    "https://www.walmart.com/ip/Hiro-Roku-TV-40-1080p-Full-HD-Smart-Roku-TV-with-Dolby-Audio-for-Streaming-H40C3C4/18096403374",
    "https://www.walmart.com/ip/Roku-Select-Series-32-FHD-TV/18051805520",
    "https://www.walmart.com/ip/Roku-Select-Series-40-FHD-TV/18094060067",
    "https://www.walmart.com/ip/Samsung-65-TV/18211860310",
    "https://www.walmart.com/ip/Philips-50-Class-4K-UHD-2160p-Google-Gaming-TV/17216450897",
    "https://www.walmart.com/ip/Philips-55-Class-7875-Series-4K-UHD-Ambilight-Roku-Smart-TV-55PUL7875-F7/19318668887",
    "https://www.walmart.com/ip/Westinghouse-HX-Series-24-23-6-Viewable-60Hz-LED-TV-2022/821539650",
    "https://www.walmart.com/ip/Westinghouse-WD24HX5201-24-720p-HD-LED-TV-with-Built-in-DVD-Player/125039315",
]


# ====================================================================
# 추출 함수 (wmart_tv_dt1.py와 동일 로직)
# ====================================================================
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


def extract_original_price(tree, savings):
    """savings가 있을 때만 추출 (wmart_tv_dt1.py:649-683 동일)"""
    if not savings:
        return None
    xpath = XPATHS.get('original_price')
    if not xpath:
        return None
    text = extract_text_safe(tree, xpath)
    if text:
        m = re.search(r'\$[\d,]+\.?\d*', text)
        if m:
            return m.group(0)
    return None


def extract_offer(tree):
    """'free offer' 포함 시 앞 숫자 추출 (wmart_tv_dt1.py:1078-1100 동일)"""
    xpath = XPATHS.get('offer')
    text = extract_text_safe(tree, xpath)
    if text and 'free offer' in text.lower():
        m = re.search(r'^(\d+)', text.strip())
        if m:
            return m.group(1)
    return None


# ====================================================================
# 메인
# ====================================================================
def main():
    print("=" * 80)
    print("  Walmart 신규 xpath 테스트 (discount_type / savings / original_sku_price / offer)")
    print("=" * 80)

    co = ChromiumOptions()
    co.no_imgs(True)
    page = ChromiumPage(co)
    print("[OK] Browser ready")

    # 세션 웜업
    print("[INFO] 홈페이지 접속 (세션 웜업)...")
    page.get("https://www.walmart.com")
    time.sleep(random.uniform(5, 8))

    results = []
    for idx, url in enumerate(TEST_URLS, 1):
        print(f"\n[{idx}/{len(TEST_URLS)}] {url}")
        try:
            page.get(url)

            # product_name 로딩 대기 (h1 기준)
            try:
                page.ele('xpath://h1', timeout=10)
            except Exception:
                print("  [WARNING] h1 대기 timeout, 계속 진행")

            time.sleep(random.uniform(2, 3))

            tree = html.fromstring(page.html)

            discount_type = extract_text_safe(tree, XPATHS['discount_type'])
            savings = extract_text_safe(tree, XPATHS['savings'])
            original_sku_price = extract_original_price(tree, savings)
            offer = extract_offer(tree)

            print(f"  discount_type      : {discount_type!r}")
            print(f"  savings            : {savings!r}")
            print(f"  original_sku_price : {original_sku_price!r}")
            print(f"  offer              : {offer!r}")

            results.append({
                'url': url,
                'discount_type': discount_type,
                'savings': savings,
                'original_sku_price': original_sku_price,
                'offer': offer,
            })

        except Exception as e:
            print(f"  [ERROR] {e}")
            results.append({
                'url': url,
                'discount_type': 'ERROR',
                'savings': 'ERROR',
                'original_sku_price': 'ERROR',
                'offer': 'ERROR',
            })

        time.sleep(random.uniform(3, 5))

    # 요약
    print("\n" + "=" * 80)
    print("  요약")
    print("=" * 80)
    all_null_count = 0
    for r in results:
        nulls = sum(1 for k in ('discount_type', 'savings', 'original_sku_price', 'offer') if r[k] is None)
        if nulls == 4:
            all_null_count += 1
        print(f"  [{nulls}/4 NULL] {r['url'].split('/')[-1]}")
    print(f"\n  전체 {len(results)} URL 중 4개 모두 NULL: {all_null_count}건")

    try:
        page.quit()
    except Exception:
        pass


if __name__ == "__main__":
    main()
