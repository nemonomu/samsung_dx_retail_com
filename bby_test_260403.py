"""
BestBuy top_mentions XPath 추출 테스트
- 리뷰페이지 접속 → reviewpage_top_mentions XPath로 추출
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

# 새로 추가할 XPath (data-feature-name 속성에서 직접 추출)
XPATH_REVIEWPAGE_TOP_MENTIONS = "//div[contains(@class, 'pros-container') or contains(@class, 'cons-container')]//button[@data-feature-name]/@data-feature-name"


def find_review_button(page):
    """리뷰 더보기 버튼 클릭 (See All Reviews)"""
    for scroll_pct in [0.5, 0.7, 0.8, 0.9, 1.0]:
        page.run_js(f"window.scrollTo(0, document.body.scrollHeight * {scroll_pct})")
        time.sleep(1)
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
        if result != 'not found':
            return result
    return 'not found'


def main():
    page = ChromiumPage()
    print(f"{'='*80}")
    print(f"BestBuy top_mentions XPath 추출 테스트 ({len(TEST_URLS)}개 URL)")
    print(f"XPath: {XPATH_REVIEWPAGE_TOP_MENTIONS}")
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

            # 2) See All Reviews 클릭 → 리뷰페이지 이동
            see_all_result = find_review_button(page)
            print(f"  See All Reviews: {see_all_result}")

            if see_all_result == 'not found':
                print(f"  [FAIL] 리뷰 버튼 못 찾음")
                fail_count += 1
                continue

            time.sleep(5)
            print(f"  리뷰페이지 URL: {page.url[:100]}")

            # 3) XPath로 top_mentions 추출
            page_html = page.html
            tree = html.fromstring(page_html)

            mentions = tree.xpath(XPATH_REVIEWPAGE_TOP_MENTIONS)

            if mentions:
                # 괄호+숫자 제거 (혹시 텍스트에 포함된 경우 대비)
                cleaned = [re.sub(r'\s*\(\d+\)\s*', '', m).strip() for m in mentions if m.strip()]
                top_mentions = ', '.join(cleaned)
                print(f"  [OK] top_mentions: {top_mentions}")
                success_count += 1
            else:
                print(f"  [FAIL] top_mentions 추출 실패 (XPath 매칭 없음)")
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
