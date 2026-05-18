"""
BestBuy TV XPath Tester (DrissionPage 버전)

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
python tv/bestbuy/bby_tv_xpath_tester.py

================================================================================
TV Main 필요 XPath 필드 (8개)
================================================================================
- base_container      : 각 상품 아이템 컨테이너 (절대경로)
- retailer_sku_name   : 상품명 (상대경로)
- product_url         : 상품 URL (상대경로)
- offer               : 할인/프로모션 (상대경로)
- pick_up_availability: 매장 픽업 가능 여부 (상대경로)
- shipping_availability: 배송 가능 여부 (상대경로)
- delivery_availability: 배달 가능 여부 (상대경로)
- sku_status          : Sponsored 여부 (상대경로)
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
    'reviews_button': "//button[contains(text(), 'See All Customer Reviews')]",
    'reviews_button_fallback1': '//button[contains(., "See All Customer Reviews")]',
    'reviews_button_fallback2': '//a[contains(., "See All Customer Reviews")]',
    'reviews_button_fallback3': '//button[contains(@class, "Op9coqeII1kYHR9Q")]',
    'reviews_button_fallback4': '//a[contains(text(), "reviews")]',
}


# ================================================================================
# XPath 목록 정의 - 리스트 페이지용 (TV Main/BSR)
# ================================================================================
# Base container XPath (각 상품 아이템)
# Sponsored: <li class="slContainer">, 일반: <li class="product-list-item">
LIST_BASE_CONTAINER = '//li[contains(@class, "product-list-item") or contains(@class, "slContainer")]'

# 리스트 아이템 내 필드 XPath (상대 경로 - .// 로 시작)
LIST_FIELD_XPATHS = {
    'retailer_sku_name': './/h3[contains(@class, "product-title")]',
    'product_url': './/a[@class="product-list-item-link"]/@href',
    'offer': './/div[@data-testid="plus-x-offers"]//span[contains(text(), "offer")]',
    'pick_up_availability': './/div[@class="fulfillment"]//span[contains(text(), "Pickup") or contains(text(), "Pick up")]',
    'shipping_availability': './/div[@class="fulfillment"]//span[contains(text(), "Shipping") or contains(text(), "Get it")]',
    'delivery_availability': './/div[@class="fulfillment"]//span[contains(text(), "Delivery") or contains(text(), "delivery")]',
    'sku_status': './/div[@class="sponsored"]',
}


# ================================================================================
# XPath 목록 정의 - 트렌드 페이지용 (TV Trend)
# ================================================================================
# Base container XPath (각 상품 아이템 - 캐러셀 아이템)
TREND_BASE_CONTAINER = '//span[contains(text(), "Trending Deals")]/ancestor::div[contains(@class, "section-body")]//li[@data-carousel-index]'

# 트렌드 아이템 내 필드 XPath (상대 경로 - .// 로 시작)
TREND_FIELD_XPATHS = {
    'retailer_sku_name': './/span[contains(@class, "line-clamp-2")]',
    'product_url': '(.//a[contains(@href, "/product/")]/@href)[1]',
}


# ================================================================================
# XPath 목록 정의 - 프로모션 페이지용 (TV Promotion)
# ================================================================================
# 섹션 컨테이너 (promotion_type별로 캐러셀이 묶여있는 단위)
PROMOTION_SECTION_CONTAINER = '//div[@data-testid="section"][.//div[@data-testid="hero-experience-deals-carousel-test-id"]]'

# 섹션 내 promotion_type 추출 XPath (상대 경로)
PROMOTION_TYPE_XPATHS = {
    'promotion_type_h2': './/h2[contains(@class, "headline80")]',
    'promotion_type_h3': './/span[contains(@class, "hero-fluid-headline")]',
    'promotion_type_p':  './/p[contains(@class, "heading-4")]',
    'promotion_type_sub': './/span[contains(@class, "hero-fluid-subhead-2")]',
}

# 섹션 내 캐러셀 아이템 (상대 경로)
PROMOTION_BASE_CONTAINER = './/ul[contains(@class, "c-carousel-list")]/li[contains(@class, "c-carousel-item")]'

# 캐러셀 아이템 내 필드 XPath (상대 경로 - .// 로 시작)
PROMOTION_FIELD_XPATHS = {
    'retailer_sku_name': './/div[@data-testid="product-card-title"]/span',
    'product_url': '(.//a[contains(@href, "/product/")]/@href)[1]',
    'offer': './/div[contains(text(), "offers for you")]',
}


class BestBuyTVXPathTester:
    """BestBuy TV XPath 테스터 (DrissionPage 버전)"""

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

    def click_specs_button(self):
        """스펙 버튼 클릭 → 모달 오픈 (screen_size / electricity / model_year 추출용)"""
        specs_button_xpath = DETAIL_XPATH_LIST.get('specs_button', '')
        if not specs_button_xpath:
            print("[INFO] specs_button XPath 미설정 - 스킵")
            return False
        try:
            # 1차: DOM에서 먼저 찾기
            specs_button = self.page.ele(f'xpath:{specs_button_xpath}', timeout=2)
            if not specs_button:
                # 2차: 스크롤하며 찾기
                for scroll_count in range(10):
                    self.page.run_js(f"window.scrollTo({{top: {500 + scroll_count * 300}, behavior: 'smooth'}});")
                    time.sleep(0.4)
                    specs_button = self.page.ele(f'xpath:{specs_button_xpath}', timeout=1)
                    if specs_button:
                        break
            if specs_button:
                self.page.run_js("arguments[0].scrollIntoView({behavior: 'smooth', block: 'center'})", specs_button)
                time.sleep(0.5)
                specs_button.click()
                time.sleep(1)
                print("[INFO] 스펙 버튼 클릭 완료")
                return True
            else:
                print("[WARNING] 스펙 버튼 못 찾음")
                return False
        except Exception as e:
            print(f"[ERROR] 스펙 버튼 클릭 실패: {e}")
            return False

    def scroll_to_review_section(self):
        """리뷰 섹션으로 스크롤 → 6단계 필드 테스트용"""
        review_section_xpath = DETAIL_XPATH_LIST.get('review_section', '')
        if not review_section_xpath:
            print("[INFO] review_section XPath 미설정 - 스킵")
            return False
        try:
            # 1차: DOM에서 먼저 찾기
            review_section = self.page.ele(f'xpath:{review_section_xpath}', timeout=2)
            if not review_section:
                # 2차: 스크롤하며 찾기
                for scroll_count in range(10):
                    self.page.run_js(f"window.scrollTo({{top: {500 + scroll_count * 300}, behavior: 'smooth'}});")
                    time.sleep(0.4)
                    review_section = self.page.ele(f'xpath:{review_section_xpath}', timeout=1)
                    if review_section:
                        break
            if review_section:
                self.page.run_js("arguments[0].scrollIntoView({behavior: 'smooth', block: 'center'})", review_section)
                time.sleep(3)
                print("[INFO] 리뷰 섹션 스크롤 완료 (3초 대기)")
                return True
            else:
                print("[WARNING] 리뷰 섹션 못 찾음")
                return False
        except Exception as e:
            print(f"[ERROR] 리뷰 섹션 스크롤 실패: {e}")
            return False

    def click_reviews_button(self):
        """리뷰 더보기 버튼 클릭 → 리뷰 상세 페이지 이동 (7단계 필드 테스트용)"""
        reviews_button_xpath = DETAIL_XPATH_LIST.get('reviews_button', '')
        if not reviews_button_xpath:
            print("[INFO] reviews_button XPath 미설정 - 스킵")
            return False
        # reviews_button + fallback XPaths
        fallback_str = DETAIL_XPATH_LIST.get('reviews_button_fallback', '')
        fallback_xpaths = [x.strip() for x in fallback_str.split('|||') if x.strip()] if fallback_str else []
        all_xpaths = [reviews_button_xpath] + fallback_xpaths
        try:
            # 1차: DOM에서 먼저 찾기
            for xpath in all_xpaths:
                review_button = self.page.ele(f'xpath:{xpath}', timeout=2)
                if review_button:
                    self.page.run_js("arguments[0].scrollIntoView({behavior: 'smooth', block: 'center'})", review_button)
                    time.sleep(0.5)
                    review_button.click()
                    time.sleep(2)
                    print(f"[INFO] 리뷰 버튼 클릭 완료 (xpath: {xpath[:60]}...)")
                    return True
            # 2차: 스크롤하며 찾기
            for scroll_count in range(10):
                self.page.run_js(f"window.scrollTo({{top: {500 + scroll_count * 300}, behavior: 'smooth'}});")
                time.sleep(0.4)
                for xpath in all_xpaths:
                    review_button = self.page.ele(f'xpath:{xpath}', timeout=1)
                    if review_button:
                        self.page.run_js("arguments[0].scrollIntoView({behavior: 'smooth', block: 'center'})", review_button)
                        time.sleep(0.5)
                        review_button.click()
                        time.sleep(2)
                        print(f"[INFO] 리뷰 버튼 클릭 완료 (스크롤 후, xpath: {xpath[:60]}...)")
                        return True
            print("[WARNING] 리뷰 버튼 못 찾음")
            return False
        except Exception as e:
            print(f"[ERROR] 리뷰 버튼 클릭 실패: {e}")
            return False

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
            if not xpath:
                print(f"  XPath: (미설정)")
                continue
            print(f"  XPath: {xpath}")
            try:
                results = tree.xpath(xpath)
                if results:
                    print(f"  매칭 개수: {len(results)}개")
                    values = self.extract_all_values(results)
                    for i, value in enumerate(values, 1):
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

        # 2. 첫 3개 아이템의 필드 테스트
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

    def test_trend_page(self, tree):
        """트렌드페이지 XPath 테스트"""
        print("\n" + "=" * 70)
        print("[트렌드페이지 XPath 테스트 결과]")
        print("=" * 70)

        # 1. Base container 테스트
        print(f"\n--- Base Container 테스트 ---")
        print(f"  XPath: {TREND_BASE_CONTAINER}")

        base_containers = tree.xpath(TREND_BASE_CONTAINER)
        print(f"  매칭 개수: {len(base_containers)}개")

        if not base_containers:
            print("  [ERROR] base_container를 찾을 수 없습니다.")
            return

        # 2. 첫 3개 아이템의 필드 테스트
        print(f"\n--- 아이템별 필드 테스트 (최대 3개) ---")

        for idx, item in enumerate(base_containers[:3], 1):
            print(f"\n{'='*50}")
            print(f"[아이템 {idx}]")
            print(f"{'='*50}")

            for field_name, xpath in TREND_FIELD_XPATHS.items():
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

    def test_promotion_page(self, tree):
        """프로모션페이지 XPath 테스트 (섹션 기반)"""
        print("\n" + "=" * 70)
        print("[프로모션페이지 XPath 테스트 결과]")
        print("=" * 70)

        # 1. 섹션 컨테이너 탐색
        print(f"\n--- 섹션 컨테이너 테스트 ---")
        print(f"  XPath: {PROMOTION_SECTION_CONTAINER}")
        sections = tree.xpath(PROMOTION_SECTION_CONTAINER)
        print(f"  매칭 개수: {len(sections)}개")

        if not sections:
            print("  [ERROR] 섹션을 찾을 수 없습니다.")
            return

        # 2. 섹션별 순회
        for sec_idx, section in enumerate(sections, 1):
            print(f"\n{'='*70}")
            print(f"[섹션 {sec_idx}]")
            print(f"{'='*70}")

            # promotion_type 추출 (XPath별로 각각 출력)
            print(f"\n  --- promotion_type 추출 ---")
            for field_name, xpath in PROMOTION_TYPE_XPATHS.items():
                print(f"    [{field_name}] XPath: {xpath}")
                try:
                    results = section.xpath(xpath)
                    values = self.extract_all_values(results) if results else []
                    if values:
                        print(f"    결과: {' '.join(values)[:100]}")
                    else:
                        print(f"    결과: (결과없음)")
                except Exception as e:
                    print(f"    결과: (에러) {e}")

            # 캐러셀 아이템 탐색
            print(f"\n  --- 캐러셀 아이템 ---")
            print(f"  XPath: {PROMOTION_BASE_CONTAINER}")
            items = section.xpath(PROMOTION_BASE_CONTAINER)
            print(f"  매칭 개수: {len(items)}개")

            if not items:
                print("  [WARNING] 캐러셀 아이템 없음 - 스킵")
                continue

            # 첫 2개 아이템 필드 테스트
            for idx, item in enumerate(items[:2], 1):
                print(f"\n  [아이템 {idx}]")
                for field_name, xpath in PROMOTION_FIELD_XPATHS.items():
                    print(f"    [{field_name}] XPath: {xpath}")
                    try:
                        results = item.xpath(xpath)
                        values = self.extract_all_values(results) if results else []
                        if values:
                            print(f"    매칭 개수: {len(results)}개")
                            for i, v in enumerate(values, 1):
                                print(f"    [{i}]: {v[:80]}")
                        else:
                            print(f"    결과: (결과없음)")
                    except Exception as e:
                        print(f"    결과: (에러) {e}")

    def test_url(self, url, mode, do_scroll=True):
        """URL 테스트"""
        try:
            print(f"\n[INFO] 페이지 로딩 중: {url[:80]}...")
            self.page.get(url)

            if mode == 'detail':
                time.sleep(10)
                if do_scroll:
                    self.scroll_to_bottom()
                self.click_specs_button()  # 스펙 모달 테스트 시 주석 해제, 불필요 시 주석 처리
                # self.scroll_to_review_section()  # 리뷰 섹션 스크롤 테스트 시 주석 해제
                #self.click_reviews_button()  # 리뷰 상세 페이지 이동 테스트 시 주석 해제
            else:
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
            elif mode == 'trend':
                self.test_trend_page(tree)
            elif mode == 'promotion':
                self.test_promotion_page(tree)
            else:
                self.test_list_page(tree)

        except Exception as e:
            print(f"[ERROR] 테스트 실패: {e}")
            traceback.print_exc()

    def run(self):
        """실행"""
        try:
            print("\n" + "=" * 70)
            print("BestBuy TV XPath Tester (DrissionPage)")
            print("=" * 70)
            print("\n[XPath 목록]")
            print(f"  - DETAIL_XPATH_LIST: {len(DETAIL_XPATH_LIST)}개")
            print(f"  - LIST_FIELD_XPATHS: {len(LIST_FIELD_XPATHS)}개")
            print(f"  - TREND_FIELD_XPATHS: {len(TREND_FIELD_XPATHS)}개")
            print(f"  - PROMOTION_FIELD_XPATHS: {len(PROMOTION_FIELD_XPATHS)}개")

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
                print("  3. 트렌드페이지 (Trend) - TREND_FIELD_XPATHS 테스트")
                print("  4. 프로모션페이지 (Promotion) - PROMOTION_FIELD_XPATHS 테스트")
                mode_choice = input("선택 (1/2/3/4): ").strip()

                if mode_choice == '1':
                    mode = 'detail'
                elif mode_choice == '2':
                    mode = 'list'
                elif mode_choice == '3':
                    mode = 'trend'
                elif mode_choice == '4':
                    mode = 'promotion'
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
    tester = BestBuyTVXPathTester()
    tester.run()
