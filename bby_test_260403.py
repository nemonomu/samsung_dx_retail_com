"""
BestBuy top_mentions XPath 추출 테스트
- 상세페이지 → 리뷰페이지 순서로 top_mentions 추출 시도
- DB 저장 없이 로그 출력만
"""

import time
import random
import re
from lxml import html
from DrissionPage import ChromiumPage


TEST_URLS = [
    'https://www.bestbuy.com/product/hisense-85-class-qd7-series-miniled-qled-4k-uhd-hdr-smart-fire-tv-2025/J3Z9Z42S9C/sku/6621485',
    'https://www.bestbuy.com/product/pioneer-40-class-led-full-hd-1080p-smart-roku-tv/J2FPJK9PZF',
    'https://www.bestbuy.com/product/roku-55-class-4k-hdr-led-smart-rokutv-2025/J3PFCJQRH3',
    'https://www.bestbuy.com/product/toshiba-65-class-c350-series-led-4k-uhd-smart-fire-tv/J3Z9Z42L2Y',
    'https://www.bestbuy.com/product/tcl-40-class-s3-s-class-1080p-fhd-led-smart-tv-with-fire-tv-2023/J36QYTT9FY',
    'https://www.bestbuy.com/product/tcl-65-class-q7-series-qled-4k-uhd-hdr-smart-tv-with-google-tv-2023/J36QYTTXQV',
    'https://www.bestbuy.com/product/tcl-40-class-s3-s-class-led-full-hd-smart-tv-with-google-tv-2023/J36QYTT2V4',
    'https://www.bestbuy.com/product/tcl-32-class-s3-s-class-led-full-hd-smart-tv-with-google-tv-2023/J36QYTT2VR',
    'https://www.bestbuy.com/product/tcl-85-class-qm8-q-class-mini-led-qled-4k-hdr-smart-tv-with-google-tv-2023/J36QYTTXTW',
    'https://www.bestbuy.com/product/tcl-65-class-s4-s-class-4k-uhd-hdr-led-smart-tv-with-google-tv-2023/J36QYTTXGY',
]


def clean_mentions(raw_list):
    """텍스트에서 (숫자) 제거, 공백 정리"""
    cleaned = []
    for m in raw_list:
        m = re.sub(r'\s*\([\d,]+\)\s*', '', m).replace('\xa0', ' ').strip()
        if m:
            cleaned.append(m)
    return cleaned


def extract_detail_type1(tree):
    """상세페이지 구조1 (table): pros/cons가 콤마 구분 텍스트"""
    # pros
    pros_elements = tree.xpath("//table//tr/td/span[contains(@class, 'text-3') and contains(@class, 'flex-wrap')]")
    if not pros_elements:
        return None

    mentions = []
    for elem in pros_elements:
        text = elem.text_content().strip()
        if text:
            items = [item.strip() for item in text.split(',') if item.strip()]
            mentions.extend(items)

    return ', '.join(mentions) if mentions else None


def extract_detail_type2(tree):
    """상세페이지 구조2 (ul/li): SVG Advantage/Disadvantage Icon으로 구분"""
    # Pros: Advantage Icon 포함 div의 텍스트
    pros_elements = tree.xpath(
        "//ul//li//div[.//svg[@aria-label='Advantage Icon']]"
    )
    # Cons: Disadvantage Icon 포함 div의 텍스트
    cons_elements = tree.xpath(
        "//ul//li//div[.//svg[@aria-label='Disadvantage Icon']]"
    )

    if not pros_elements and not cons_elements:
        return None

    mentions = []
    for elem in pros_elements:
        text = elem.text_content().strip()
        if text:
            mentions.append(text)
    for elem in cons_elements:
        text = elem.text_content().strip()
        if text:
            mentions.append(text)

    return ', '.join(clean_mentions(mentions)) if mentions else None


def extract_reviewpage_type1(tree):
    """리뷰페이지 구조1 (기존): pros-container/cons-container + data-feature-name"""
    mentions = tree.xpath(
        "//div[contains(@class, 'pros-container') or contains(@class, 'cons-container')]"
        "//button[@data-feature-name]/@data-feature-name"
    )
    return ', '.join(clean_mentions(mentions)) if mentions else None


def extract_reviewpage_type2(tree):
    """리뷰페이지 구조2 (새): bg-success(pros)/bg-attention(cons) Chip 버튼"""
    # Pros: bg-success 버튼
    pros_elements = tree.xpath(
        "//button[contains(@class, 'bg-success')]//span[contains(@class, 'text-style-body')]"
    )
    # Cons: bg-attention 버튼
    cons_elements = tree.xpath(
        "//button[contains(@class, 'bg-attention')]//span[contains(@class, 'text-style-body')]"
    )

    if not pros_elements and not cons_elements:
        return None

    mentions = []
    for elem in pros_elements:
        text = elem.text_content().strip()
        if text:
            mentions.append(text)
    for elem in cons_elements:
        text = elem.text_content().strip()
        if text:
            mentions.append(text)

    return ', '.join(clean_mentions(mentions)) if mentions else None


def scroll_down(page):
    """5초간 천천히 스크롤 (리뷰 섹션 로딩 대기)"""
    for pct in [0.2, 0.4, 0.6, 0.8, 1.0]:
        page.run_js(f"window.scrollTo(0, document.body.scrollHeight * {pct})")
        time.sleep(1)


def find_review_button(page):
    """리뷰 더보기 버튼 클릭 (See All Reviews)"""
    result = page.run_js('''
        var btns = document.querySelectorAll('button, a');
        for (var i = 0; i < btns.length; i++) {
            var text = btns[i].textContent.trim().toLowerCase();
            if (text.includes('see all') && text.includes('review')) {
                btns[i].scrollIntoView({behavior: "smooth", block: "center"});
                btns[i].click();
                return 'clicked: ' + btns[i].textContent.trim().substring(0, 60);
            }
        }
        return 'not found';
    ''')
    return result


def main():
    page = ChromiumPage()
    print(f"{'='*80}")
    print(f"BestBuy top_mentions 추출 테스트 ({len(TEST_URLS)}개 URL)")
    print(f"추출 순서: 상세페이지(구조1→구조2) → 리뷰페이지(구조1→구조2)")
    print(f"{'='*80}\n")

    success_count = 0
    fail_count = 0

    for i, url in enumerate(TEST_URLS, 1):
        print(f"\n[{i}/{len(TEST_URLS)}] {url}")
        print(f"{'-'*70}")

        try:
            # 1) 상세페이지 접속
            page.get(url)
            time.sleep(random.uniform(3, 5))

            # 설문조사 팝업 닫기
            try:
                survey_btn = page.ele('#survey_invite_no', timeout=2)
                if survey_btn:
                    survey_btn.click()
                    time.sleep(1)
            except Exception:
                pass

            # 2) 스크롤 (리뷰 섹션 로딩 대기, 약 5초)
            scroll_down(page)

            page_html = page.html
            tree = html.fromstring(page_html)

            # 3) 상세페이지에서 추출 시도
            top_mentions = None
            source = None

            # 상세페이지 구조1 (table)
            result = extract_detail_type1(tree)
            if result:
                top_mentions = result
                source = '상세페이지-구조1(table)'

            # 상세페이지 구조2 (ul/li + Advantage/Disadvantage Icon)
            if not top_mentions:
                result = extract_detail_type2(tree)
                if result:
                    top_mentions = result
                    source = '상세페이지-구조2(icon)'

            # 4) 상세페이지에서 못 찾으면 → 리뷰페이지로 이동
            if not top_mentions:
                print(f"  상세페이지: 추출 실패 → 리뷰페이지 시도")

                see_all_result = find_review_button(page)
                print(f"  See All Reviews: {see_all_result}")

                if see_all_result != 'not found':
                    time.sleep(5)
                    print(f"  리뷰페이지 URL: {page.url[:100]}")

                    page_html = page.html
                    tree = html.fromstring(page_html)

                    # 리뷰페이지 구조1 (기존: pros-container/cons-container)
                    result = extract_reviewpage_type1(tree)
                    if result:
                        top_mentions = result
                        source = '리뷰페이지-구조1(data-feature-name)'

                    # 리뷰페이지 구조2 (새: bg-success/bg-attention)
                    if not top_mentions:
                        result = extract_reviewpage_type2(tree)
                        if result:
                            top_mentions = result
                            source = '리뷰페이지-구조2(bg-success/attention)'
                else:
                    print(f"  리뷰 버튼 못 찾음")

            # 5) 결과 출력
            if top_mentions:
                print(f"  [OK] top_mentions: {top_mentions}")
                print(f"       추출 소스: {source}")
                success_count += 1
            else:
                print(f"  [FAIL] top_mentions 추출 실패 (모든 구조에서 매칭 없음)")
                fail_count += 1

        except Exception as e:
            print(f"  [ERROR] {e}")
            fail_count += 1

        time.sleep(random.uniform(2, 4))

    print(f"\n{'='*80}")
    print(f"결과: 성공 {success_count}/{len(TEST_URLS)}, 실패 {fail_count}/{len(TEST_URLS)}")
    print(f"{'='*80}")

    page.quit()
    input("Press Enter to exit...")


if __name__ == '__main__':
    main()
