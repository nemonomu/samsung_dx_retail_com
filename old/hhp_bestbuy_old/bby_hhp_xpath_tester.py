"""
BestBuy XPath Tester (DrissionPage 버전)

================================================================================
주요 기능
================================================================================
1. URL 입력 후 페이지 로드
2. 상세(Detail) / 리스트(List) 페이지 모드 선택
3. 상세 모드: 미리 정의된 XPath 목록 전체 자동 테스트
4. 리스트 모드: Base container로 아이템 추출 후 각 필드 테스트

================================================================================
사용법
================================================================================
python bestbuy/bby_xpath_tester.py

================================================================================
"""

import sys
import os
import time
import random
import traceback
from lxml import html
from DrissionPage import ChromiumPage

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
from common.setup import setup_environment
setup_environment(__file__)


# ================================================================================
# XPath 목록 정의 - 상세 페이지용
# ================================================================================
DETAIL_XPATH_LIST = {
    'top_mentions': "//h3[contains(text(), 'Top Mentions')]/following-sibling::ul//li/a[.//svg[@aria-label='Advantage Icon' or @aria-label='Disadvantage Icon']]",
    'top_mentions_fallback': "//div[@data-component-name='ReviewsProsConsSection']//ul/li",
}


# ================================================================================
# XPath 목록 정의 - 리스트 페이지용
# ================================================================================
# Base container XPath (각 상품 아이템)
LIST_BASE_CONTAINER = '//li[contains(@class, "product-list-item") or contains(@class, "slContainer")]'

# 리스트 아이템 내 필드 XPath (상대 경로 - .// 로 시작)
# Note: savings_amount는 추출 후 "Save " 제거 필요 (XPath로는 불가)
LIST_FIELD_XPATHS = {
    'pick_up_availability': './/div[@class="fulfillment"]//span[contains(text(), "Pickup") or contains(text(), "Pick up")]',
    'sku_status': './/div[@class="sponsored"]',
}


class BestBuyXPathTester:
    """BestBuy XPath 테스터 (DrissionPage 버전)"""

    def __init__(self):
        self.page = None

    def setup_driver(self):
        """DrissionPage 브라우저 설정"""
        print("[INFO] DrissionPage 브라우저 설정 중...")
        try:
            self.page = ChromiumPage()
            print("[INFO] DrissionPage 브라우저 설정 완료")
        except Exception as e:
            print(f"[ERROR] DrissionPage 설정 실패: {e}")
            raise

    def scroll_to_bottom(self):
        """스크롤: 200~350px씩 점진적 스크롤 → 페이지네이션 보이면 종료"""
        try:
            current_position = 0

            for _ in range(50):
                is_pagination_visible = self.page.run_js("""
                    var elem = document.querySelector("div.pagination-container");
                    if (!elem) return false;
                    var rect = elem.getBoundingClientRect();
                    return (rect.top >= 0 && rect.top <= window.innerHeight);
                """)

                if is_pagination_visible:
                    break

                scroll_step = random.randint(200, 350)
                current_position += scroll_step
                self.page.run_js(f"window.scrollTo(0, {current_position});")
                time.sleep(random.uniform(0.5, 0.7))

                total_height = self.page.run_js("return document.body.scrollHeight")
                if current_position >= total_height:
                    break

            time.sleep(random.uniform(0, 4))

        except Exception as e:
            print(f"[ERROR] Scroll failed: {e}")

    def handle_block(self):
        """차단/CAPTCHA 체크"""
        page_html_lower = self.page.html.lower()
        block_phrases = [
            'access denied',
            'please verify you are a human',
            'unusual activity',
        ]
        if any(phrase in page_html_lower for phrase in block_phrases):
            print("[WARNING] 차단/CAPTCHA 감지 - 수동 해결 후 엔터를 누르세요...")
            input()
            time.sleep(2)
            return True
        return False

    def extract_value(self, element):
        """요소에서 값 추출"""
        if hasattr(element, 'text_content'):
            return element.text_content().strip()
        else:
            return str(element).strip()

    def extract_all_values(self, elements):
        """모든 요소의 값을 추출하여 리스트로 반환"""
        values = []
        for elem in elements:
            value = self.extract_value(elem)
            if value:
                values.append(value)
        return values

    def test_detail_page(self, tree):
        """상세페이지 XPath 테스트"""
        print("\n" + "=" * 70)
        print("[상세페이지 XPath 테스트 결과]")
        print("=" * 70)

        if not DETAIL_XPATH_LIST:
            print("[INFO] DETAIL_XPATH_LIST가 비어있습니다. XPath를 추가하세요.")
            return

        for field_name, xpath in DETAIL_XPATH_LIST.items():
            print(f"\n[{field_name}]")
            print(f"  XPath: {xpath}")
            try:
                results = tree.xpath(xpath)
                if results:
                    print(f"  매칭 개수: {len(results)}개")
                    values = self.extract_all_values(results)
                    for i, value in enumerate(values, 1):
                        # 긴 값은 줄바꿈
                        if len(value) > 100:
                            print(f"  [{i}]: {value[:100]}...")
                            print(f"       (전체 길이: {len(value)})")
                        else:
                            print(f"  [{i}]: {value}")
                else:
                    print(f"  결과: (추출 실패 - 매칭 없음)")
            except Exception as e:
                print(f"  결과: (에러) {e}")

    def test_list_page(self, tree):
        """리스트페이지 XPath 테스트"""
        print("\n" + "=" * 70)
        print("[리스트페이지 XPath 테스트 결과]")
        print("=" * 70)

        # 1. Base container 테스트
        print(f"\n--- Base Container 테스트 ---")
        print(f"  XPath: {LIST_BASE_CONTAINER}")

        base_containers = tree.xpath(LIST_BASE_CONTAINER)
        print(f"  매칭 개수: {len(base_containers)}개")

        if not base_containers:
            print("  [ERROR] base_container를 찾을 수 없습니다.")
            return

        # 3. 첫 3개 아이템의 필드 테스트
        print(f"\n--- 아이템별 필드 테스트 (최대 3개) ---")

        for idx, item in enumerate(base_containers[:3], 1):
            print(f"\n{'='*50}")
            print(f"[아이템 {idx}]")
            print(f"{'='*50}")

            for field_name, xpath in LIST_FIELD_XPATHS.items():
                print(f"\n  [{field_name}]")
                print(f"    XPath: {xpath}")
                try:
                    results = item.xpath(xpath)
                    if results:
                        print(f"    매칭 개수: {len(results)}개")
                        values = self.extract_all_values(results)
                        for i, value in enumerate(values, 1):
                            if len(value) > 80:
                                print(f"    [{i}]: {value[:80]}...")
                            else:
                                print(f"    [{i}]: {value}")
                    else:
                        print(f"    결과: (추출 실패)")
                except Exception as e:
                    print(f"    결과: (에러) {e}")

    def test_url(self, url, mode, do_scroll=True):
        """URL 테스트"""
        try:
            print(f"\n[INFO] 페이지 로딩 중: {url[:80]}...")
            self.page.get(url)

            if mode == 'detail':
                # 상세페이지: 10초 대기
                time.sleep(10)
                if do_scroll:
                    self.scroll_to_bottom()
            else:
                # 리스트페이지: 스크롤 + 대기
                time.sleep(random.uniform(8, 12))
                if do_scroll:
                    self.scroll_to_bottom()
                    time.sleep(random.uniform(28, 32))

            # 차단 체크
            self.handle_block()

            # HTML 파싱
            page_html = self.page.html
            tree = html.fromstring(page_html)

            # 모드별 테스트
            if mode == 'detail':
                self.test_detail_page(tree)
            else:
                self.test_list_page(tree)

        except Exception as e:
            print(f"[ERROR] 테스트 실패: {e}")
            traceback.print_exc()

    def run(self):
        """실행"""
        try:
            print("\n" + "=" * 70)
            print("BestBuy XPath Tester (DrissionPage)")
            print("=" * 70)
            print("\n[XPath 목록]")
            print(f"  - DETAIL_XPATH_LIST: {len(DETAIL_XPATH_LIST)}개")
            print(f"  - LIST_FIELD_XPATHS: {len(LIST_FIELD_XPATHS)}개")

            # 드라이버 설정
            self.setup_driver()

            while True:
                print("\n" + "-" * 70)
                url = input("URL 입력 (종료: q): ").strip()

                if url.lower() == 'q':
                    print("[INFO] 종료합니다.")
                    break

                if not url:
                    print("[WARNING] URL을 입력하세요.")
                    continue

                if not url.startswith('http'):
                    url = 'https://' + url

                print("\n페이지 모드 선택:")
                print("  1. 상세페이지 (Detail) - DETAIL_XPATH_LIST 테스트")
                print("  2. 리스트페이지 (List) - LIST_FIELD_XPATHS 테스트")
                mode_choice = input("선택 (1/2): ").strip()

                if mode_choice == '1':
                    mode = 'detail'
                elif mode_choice == '2':
                    mode = 'list'
                else:
                    print("[WARNING] 잘못된 선택입니다.")
                    continue

                # 스크롤 여부 선택
                scroll_choice = input("하단 스크롤 로딩? (y/n) [기본: y]: ").strip().lower()
                do_scroll = scroll_choice != 'n'

                # URL 테스트
                self.test_url(url, mode, do_scroll)

            return True

        except Exception as e:
            print(f"[ERROR] {e}")
            traceback.print_exc()
            return False

        finally:
            if self.page:
                self.page.quit()


if __name__ == "__main__":
    tester = BestBuyXPathTester()
    tester.run()
