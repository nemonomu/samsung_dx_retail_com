"""
BestBuy Detail 페이지 크롤러

================================================================================
실행 모드
================================================================================
- 개별 실행: batch_id=None (하드코딩된 batch_id 사용)
- 통합 크롤러: batch_id를 파라미터로 전달

================================================================================
주요 기능
================================================================================
- product_list 테이블에서 해당 batch_id의 제품 URL 조회
- 각 제품 상세 페이지에서 리뷰, 별점, 스펙 등 추출
- Main/BSR/Trend에서 수집한 모든 제품 처리

================================================================================
저장 테이블
================================================================================
- hhp_retail_com (상세 정보 + 리뷰)
"""

import sys
import os
import time
import random
import traceback
import re
from datetime import datetime, timedelta
from lxml import html
from DrissionPage import ChromiumPage

# 공통 환경 설정 (작업 디렉토리, 한글 출력, 경로 설정)
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
from common.setup import setup_environment
setup_environment(__file__)

from common.base_crawler import BaseCrawler
from common.data_extractor import extract_numeric_value


class BestBuyDetailCrawler(BaseCrawler):
    """
    BestBuy Detail 페이지 크롤러
    """

    def __init__(self, batch_id=None, test_mode=False, time_offset_hours=0):
        """초기화. batch_id: 통합 크롤러에서 전달, test_mode: 테스트 모드 여부"""
        super().__init__()
        self.time_offset_hours = time_offset_hours
        self.account_name = 'Bestbuy'
        self.page_type = 'detail'
        self.batch_id = batch_id
        self.test_mode = test_mode
        # batch_id 없으면 개별 실행
        self.standalone = batch_id is None

        # DrissionPage 드라이버 (Selenium driver 대신 사용)
        self.page = None

    def setup_drission_driver(self):
        """DrissionPage 브라우저 설정 (봇 감지 우회 강화)"""
        try:
            self.page = ChromiumPage()
            print("[SUCCESS] DrissionPage setup complete")
        except Exception as e:
            print(f"[ERROR] DrissionPage setup failed: {e}")
            traceback.print_exc()
            raise

    def close_survey_popup(self):
        """설문조사 팝업 감지 및 'No, Thanks' 버튼 클릭 (DrissionPage용)"""
        try:
            survey_no_button = self.page.ele('#survey_invite_no', timeout=2)
            if survey_no_button:
                survey_no_button.click()
                print("[INFO] Survey popup closed")
                time.sleep(1)
        except Exception:
            pass  # 팝업 없으면 무시

    def extract_rating(self, text):
        """별점 텍스트에서 숫자 추출 (소수점 포함, 쉼표 제외)"""
        return extract_numeric_value(text, include_comma=False, include_decimal=True)

    def extract_review_count(self, text):
        """리뷰 개수 텍스트에서 숫자 추출 (쉼표 포함, 소수점 제외)"""
        return extract_numeric_value(text, include_comma=True, include_decimal=False)

    def get_hhp_specs_from_mst(self, item):
        """마스터 테이블에서 HHP 스펙 조회"""
        if not item:
            return None, None, None

        try:
            cursor = self.db_conn.cursor()
            cursor.execute("""
                SELECT hhp_carrier, hhp_color, hhp_storage FROM hhp_item_mst
                WHERE item = %s AND account_name = %s AND is_product = TRUE
            """, (item, self.account_name))
            row = cursor.fetchone()
            cursor.close()

            if row:
                return row[0], row[1], row[2]
            return None, None, None
        except Exception:
            return None, None, None

    def initialize(self):
        """초기화: batch_id 설정 → DB 연결 → XPath 로드 → DrissionPage 설정 → 로그 정리"""
        # batch_id 없으면 기본값 사용
        if not self.batch_id:
            self.batch_id = 't_b_20260113_001830'

        if not self.connect_db():
            return False
        if not self.load_xpaths(self.account_name, self.page_type, 'SEA', 'HHP'):
            return False

        # DrissionPage 설정 (Selenium 대신)
        try:
            self.setup_drission_driver()
        except Exception as e:
            print(f"[ERROR] Initialize failed: DrissionPage setup failed - {e}")
            traceback.print_exc()
            return False

        self.cleanup_old_logs()

        return True

    def load_product_list(self):
        """product_list 조회: batch_id 기준으로 제품 URL 및 기본 정보 조회"""
        try:
            cursor = self.db_conn.cursor()

            query = """
                SELECT
                    page_type, retailer_sku_name, final_sku_price, savings,
                    comparable_pricing as original_sku_price, offer,
                    pick_up_availability, fastest_delivery, delivery_availability,
                    sku_status, promotion_type, main_rank, bsr_rank, trend_rank,
                    product_url, calendar_week
                FROM bby_hhp_product_list
                WHERE account_name = %s AND batch_id = %s AND product_url IS NOT NULL
                ORDER BY id
            """

            cursor.execute(query, (self.account_name, self.batch_id))
            rows = cursor.fetchall()
            cursor.close()

            product_list = []
            for row in rows:
                product = {
                    'account_name': self.account_name,
                    'page_type': row[0],
                    'retailer_sku_name': row[1],
                    'final_sku_price': row[2],
                    'savings': row[3],
                    'original_sku_price': row[4],
                    'offer': row[5],
                    'pick_up_availability': row[6],
                    'fastest_delivery': row[7],
                    'delivery_availability': row[8],
                    'sku_status': row[9],
                    'promotion_type': row[10],
                    'main_rank': row[11],
                    'bsr_rank': row[12],
                    'trend_rank': row[13],
                    'product_url': row[14],
                    'calendar_week': row[15]
                }
                product_list.append(product)

            print(f"[INFO] Loaded {len(product_list)} products")
            return product_list

        except Exception as e:
            print(f"[ERROR] Failed to load product list: {e}")
            return []

    def extract_item_from_url(self, product_url):
        """URL에서 item (SKU ID) 추출"""
        if not product_url:
            return None

        try:
            cleaned_url = re.sub(r'/sku/\d+(/openbox\?.*)?$', '', product_url)
            cleaned_url = cleaned_url.split('?')[0]
            parts = cleaned_url.split('/')
            if not parts:
                return None
            item = parts[-1]
            # .p 확장자 제거 (예: 6507560.p → 6507560)
            if item.endswith('.p'):
                item = item[:-2]
            return item if item else None
        except Exception:
            return None

    def crawl_detail(self, product):
        """상세 페이지 크롤링: 페이지 로드 → 스크롤 전 추출 → 스크롤(최대 3번 재시도) 후 스펙 추출 → 유사제품 추출 → 리뷰 추출 → product_list + detail 데이터 결합 (DrissionPage 사용)"""
        try:
            product_url = product.get('product_url')
            if not product_url:
                return product

            # Referer 설정 (리스트 페이지에서 클릭한 것처럼 위장)
            self.page.set.headers({'Referer': 'https://www.bestbuy.com/site/searchpage.jsp?st=cellphone'})

            # 현재 URL 저장 (로드 전)
            previous_url = self.page.url if self.page else None

            self.page.get(product_url)
            time.sleep(random.uniform(3, 5))

            # 로드 확인: 현재 URL이 이전과 동일하면 실패로 간주
            current_url = self.page.url
            if previous_url and current_url == previous_url:
                print(f"[WARNING] 페이지 로드 실패 감지 (URL 변경 없음)")
                raise Exception("Page load failed - URL unchanged")

            # 설문조사 팝업 닫기
            self.close_survey_popup()

            page_html = self.page.html

            # 에러 페이지 감지
            error_keywords = ["this site can't be reached", "err_http2_protocol_error"]
            if any(kw in page_html.lower() for kw in error_keywords):
                print(f"[WARNING] 에러 페이지 감지")
                raise Exception("Page load failed - error page detected")

            tree = html.fromstring(page_html)

            # 원본 URL에서 item 추출 (페이지 로드 실패해도 정확한 item 유지)
            item = self.extract_item_from_url(product_url)

            # ========== 1단계: 상단 정보 추출 (최대 3회 재시도) ==========
            top_star_rating = None
            top_count_of_reviews = None
            trade_in = None

            MAX_RETRY_TOP = 3
            for attempt in range(1, MAX_RETRY_TOP + 1):
                page_html = self.page.html
                tree = html.fromstring(page_html)

                if top_star_rating is None:
                    top_star_rating = self.safe_extract(tree, 'top_star_rating')

                if top_count_of_reviews is None:
                    top_count_of_reviews = self.safe_extract(tree, 'top_count_of_reviews')

                if trade_in is None:
                    trade_in = self.safe_extract(tree, 'trade_in')
                    if trade_in:
                        print(f"├─ trade_in: {trade_in}")

                # 필수 필드 모두 추출 성공하면 종료 (Not yet reviewed인 경우도 성공으로 간주)
                if top_star_rating and top_count_of_reviews:
                    break
                if top_count_of_reviews and 'Not yet reviewed' in top_count_of_reviews:
                    break

                if attempt < MAX_RETRY_TOP:
                    time.sleep(1)
                else:
                    # 마지막 시도에서도 실패
                    missing = []
                    if not top_star_rating: missing.append('top_star_rating')
                    if not top_count_of_reviews: missing.append('top_count_of_reviews')
                    if not trade_in: missing.append('trade_in')
                    if missing:
                        print(f"├─ 상단 정보 일부 미추출: {', '.join(missing)}")

            # ========== 1-1단계: final_sku_price 없으면 상세페이지에서 추출 ==========
            # if not product.get('final_sku_price'):
            #     final_sku_price = self.safe_extract_join(tree, 'final_sku_price', separator='')
            #     if final_sku_price:
            #         product['final_sku_price'] = final_sku_price

            if not product.get('final_sku_price'):
                see_price_in_cart = self.safe_extract(tree, 'final_sku_price_see_price_in_cart')
                if see_price_in_cart:
                    product['final_sku_price'] = see_price_in_cart
                    print(f"├─ Price: {see_price_in_cart} (See Price in Cart)")
                else:
                    no_longer = self.safe_extract(tree, 'final_sku_price_no_longer_available')
                    if no_longer:
                        product['final_sku_price'] = 'no longer available'
                        print(f"├─ Price: no longer available")
                    else:
                        print(f"├─ Price: (추출 실패)")

            # ========== 1-2단계: original_sku_price 없으면 상세페이지에서 추출 ==========
            # if not product.get('original_sku_price'):
            #     original_sku_price = self.safe_extract(tree, 'original_sku_price')
            #     if original_sku_price:
            #         product['original_sku_price'] = original_sku_price

            # ========== 1-3단계: SKU(Model)/SKU Number(SKU, 리뷰 URL용) 추출 ==========
            # SKU(Model)
            sku_raw = self.safe_extract(tree, 'sku')
            sku = sku_raw.replace('Model:', '').strip() if sku_raw else None
            if sku:
                print(f"├─ Model: {sku}")
            else:
                print(f"├─ Model: (추출 실패)")

            # SKU Number(SKU, 리뷰 URL용)
            sku_number_raw = self.safe_extract(tree, 'sku_number')
            # "SKU: 6418031" 형태에서 숫자만 추출
            sku_number = ''.join(filter(str.isdigit, sku_number_raw)) if sku_number_raw else None
            if sku_number:
                print(f"├─ SKU Number: {sku_number}")
            else:
                print(f"├─ SKU Number: (추출 실패)")

            # ========== 1-4단계: trend인 경우 savings/pick_up/shipping availability 상세페이지에서 추출 ==========
            if product.get('page_type') == 'trend':
                if not product.get('savings'):
                    savings_raw = self.safe_extract(tree, 'savings')
                    if savings_raw:
                        product['savings'] = savings_raw.replace('Save ', '')

                if not product.get('pick_up_availability'):
                    pick_up_raw = self.safe_extract_join(tree, 'pick_up_availability', separator=' ')
                    if pick_up_raw:
                        product['pick_up_availability'] = pick_up_raw

                if not product.get('fastest_delivery'):
                    shipping_raw = self.safe_extract_join(tree, 'fastest_delivery', separator=' ')
                    if shipping_raw:
                        product['fastest_delivery'] = shipping_raw

            # ========== 2단계: HHP 스펙 추출 (마스터 테이블 우선, 없으면 추출 시도) ==========
            # 먼저 마스터 테이블에서 조회
            mst_carrier, mst_color, mst_storage = self.get_hhp_specs_from_mst(item)

            if mst_storage:
                # 마스터에 storage 있으면 마스터 값 사용 (추출 스킵)
                hhp_carrier = mst_carrier
                hhp_color = mst_color
                hhp_storage = mst_storage
                print(f"├─ HHP 스펙 (마스터): carrier={hhp_carrier}, color={hhp_color}, storage={hhp_storage}")
            else:
                # 마스터에 storage 없으면 추출 시도
                hhp_carrier = None
                hhp_storage = None
                hhp_color = None

                specs_button_xpath = self.xpaths.get('specs_button', {}).get('xpath')
                if specs_button_xpath:
                    specs_button_found = False

                    # 1차: DOM에서 먼저 찾기 (스크롤 없이)
                    try:
                        specs_button = self.page.ele(f'xpath:{specs_button_xpath}', timeout=2)
                        if specs_button:
                            self.page.run_js("arguments[0].scrollIntoView({behavior: 'smooth', block: 'center'})", specs_button)
                            time.sleep(0.5)
                            specs_button.click()
                            specs_button_found = True
                    except Exception:
                        pass

                    # 2차: DOM에서 못 찾으면 스크롤하며 찾기
                    if not specs_button_found:
                        max_scroll_attempts = 10
                        for scroll_count in range(max_scroll_attempts):
                            try:
                                scroll_distance = 500 + (scroll_count * 300)
                                self.page.run_js(f"window.scrollTo({{top: {scroll_distance}, behavior: 'smooth'}});")
                                time.sleep(0.4)

                                specs_button = self.page.ele(f'xpath:{specs_button_xpath}', timeout=1)
                                if specs_button:
                                    self.page.run_js("arguments[0].scrollIntoView({behavior: 'smooth', block: 'center'})", specs_button)
                                    time.sleep(0.5)
                                    specs_button.click()
                                    specs_button_found = True
                                    break
                            except Exception:
                                continue

                    if specs_button_found:
                        print(f"├─ specs button clicked (스펙 모달 열림)")
                        try:
                            # 모달 로딩 대기
                            time.sleep(1)

                            modal_html = self.page.html
                            modal_tree = html.fromstring(modal_html)

                            hhp_carrier = self.safe_extract(modal_tree, 'hhp_carrier')
                            hhp_storage = self.safe_extract(modal_tree, 'hhp_storage')
                            hhp_color = self.safe_extract(modal_tree, 'hhp_color')

                            # 스펙 모달창 닫기
                            try:
                                close_button_xpath = self.xpaths.get('close_button', {}).get('xpath')
                                if close_button_xpath:
                                    close_button = self.page.ele(f'xpath:{close_button_xpath}', timeout=2)
                                    if close_button:
                                        close_button.click()
                                        time.sleep(0.5)
                            except Exception:
                                try:
                                    self.page.actions.key_down('ESCAPE').key_up('ESCAPE')
                                    time.sleep(0.5)
                                except Exception:
                                    pass

                        except Exception:
                            pass

                # HHP 스펙 추출 결과 로그
                if hhp_carrier or hhp_color or hhp_storage:
                    print(f"├─ HHP 스펙 (추출): carrier={hhp_carrier}, color={hhp_color}, storage={hhp_storage}")

            # ========== 3단계: 유사 제품 추출 ==========
            similar_products_container_xpath = self.xpaths.get('similar_products_container', {}).get('xpath')
            retailer_sku_name_similar = None

            if similar_products_container_xpath:
                similar_products_found = False

                # 1차: DOM에서 먼저 찾기 (스크롤 없이)
                try:
                    similar_elements = self.page.eles(f'xpath:{similar_products_container_xpath}')
                    if similar_elements:
                        similar_products_found = True
                        self.page.run_js("arguments[0].scrollIntoView({behavior: 'smooth', block: 'center'})", similar_elements[0])
                        time.sleep(0.5)
                except Exception:
                    pass

                # 2차: DOM에서 못 찾으면 스크롤하며 찾기
                if not similar_products_found:
                    current_scroll = self.page.run_js("return window.pageYOffset;")
                    page_height = self.page.run_js("return document.body.scrollHeight")
                    max_scroll_attempts = 5

                    for _ in range(max_scroll_attempts):
                        if current_scroll >= page_height:
                            break
                        try:
                            similar_elements = self.page.eles(f'xpath:{similar_products_container_xpath}')
                            if similar_elements:
                                similar_products_found = True
                                self.page.run_js("arguments[0].scrollIntoView({behavior: 'smooth', block: 'center'})", similar_elements[0])
                                time.sleep(0.5)
                                break
                        except Exception:
                            pass

                        scroll_step = random.randint(280, 320)
                        current_scroll += scroll_step
                        self.page.run_js(f"window.scrollTo({{top: {current_scroll}, behavior: 'smooth'}});")
                        time.sleep(0.3)

                if similar_products_found:
                    time.sleep(0.3)
                    page_html = self.page.html
                    tree = html.fromstring(page_html)

                try:
                    product_cards = tree.xpath(similar_products_container_xpath)
                    if product_cards:
                        similar_product_names = []
                        name_xpath = self.xpaths.get('similar_product_name', {}).get('xpath')

                        for card in product_cards:
                            try:
                                if name_xpath:
                                    name_results = card.xpath(name_xpath)
                                    if name_results:
                                        similar_product_names.append(name_results[0])
                            except Exception:
                                continue

                        retailer_sku_name_similar = ' ||| '.join(similar_product_names) if similar_product_names else None
                except Exception:
                    retailer_sku_name_similar = None

            # ========== 4단계: 리뷰 섹션으로 스크롤 후 데이터 추출 ==========
            # 리뷰 없음 텍스트 패턴
            NO_REVIEWS_TEXTS = ['not yet reviewed']
            NO_REVIEWS_VALUE = "Not yet reviewed"

            star_rating = None
            count_of_reviews = None
            count_of_star_ratings = None
            recommendation_intent = None

            # 리뷰 없음 여부 먼저 판별
            # 1) "not yet reviewed" 포함
            # 2) Syndicated 리뷰 (예: "45 reviews from Skyworth USA") - BestBuy 자체 리뷰 아님
            is_no_reviews = False
            if top_count_of_reviews:
                if any(t in top_count_of_reviews.lower() for t in NO_REVIEWS_TEXTS):
                    is_no_reviews = True
                elif 'from' in top_count_of_reviews.lower():
                    is_no_reviews = True

            # 리뷰 섹션으로 스크롤 (리뷰가 있는 경우에만)
            review_section_found = False
            review_section_xpath = self.xpaths.get('review_section', {}).get('xpath')
            if not is_no_reviews and review_section_xpath:
                # 1차: DOM에서 먼저 찾기
                try:
                    review_section = self.page.ele(f'xpath:{review_section_xpath}', timeout=2)
                    if review_section:
                        self.page.run_js("arguments[0].scrollIntoView({behavior: 'smooth', block: 'center'})", review_section)
                        time.sleep(1)
                        review_section_found = True
                        print(f"├─ review section found (리뷰 섹션 도달)")
                except Exception:
                    pass

                # 2차: DOM에서 못 찾으면 스크롤하며 찾기 (유사제품 바로 아래이므로 작은 간격)
                if not review_section_found:
                    print(f"├─ review section not found (1차 시도 실패, 스크롤 시도)")
                    current_scroll = self.page.run_js("return window.pageYOffset;")
                    page_height = self.page.run_js("return document.body.scrollHeight")
                    max_scroll_attempts = 5

                    for attempt in range(max_scroll_attempts):
                        if current_scroll >= page_height:
                            break
                        try:
                            review_section = self.page.ele(f'xpath:{review_section_xpath}', timeout=0.5)
                            if review_section:
                                self.page.run_js("arguments[0].scrollIntoView({behavior: 'smooth', block: 'center'})", review_section)
                                time.sleep(1)
                                review_section_found = True
                                print(f"├─ review section found (리뷰 섹션 도달, 스크롤 {attempt+1}회)")
                                break
                        except Exception:
                            pass

                        scroll_step = random.randint(150, 200)
                        current_scroll += scroll_step
                        self.page.run_js(f"window.scrollTo({{top: {current_scroll}, behavior: 'smooth'}});")
                        time.sleep(0.15)

            if is_no_reviews:
                # 리뷰 없음 → 일괄 할당
                count_of_reviews = "0"
                star_rating = "Not yet reviewed"
                count_of_star_ratings = "0"
            else:
                # 리뷰 있음 → 추출 로직
                MAX_RETRY = 3
                for attempt in range(1, MAX_RETRY + 1):
                    page_html = self.page.html
                    tree = html.fromstring(page_html)

                    if count_of_reviews is None:
                        if top_count_of_reviews:
                            count_of_reviews = self.extract_review_count(top_count_of_reviews)
                        else:
                            count_of_reviews_raw = self.safe_extract(tree, 'count_of_reviews')
                            count_of_reviews = self.extract_review_count(count_of_reviews_raw)

                    if star_rating is None:
                        if top_star_rating:
                            star_rating = self.extract_rating(top_star_rating)
                        else:
                            star_rating_raw = self.safe_extract(tree, 'star_rating')
                            star_rating = self.extract_rating(star_rating_raw)

                    if count_of_star_ratings is None:
                        count_of_star_ratings = count_of_reviews
            
            # ========== 5단계: 리뷰 더보기 버튼 클릭 및 상세 리뷰 추출 ==========
            detailed_review_content = None
            reviews_button_xpath = self.xpaths.get('reviews_button', {}).get('xpath')
            detail_page_url = self.page.url

            # --- 미리 타겟 리뷰 URL 생성 ---
            target_review_url = None
            if sku_number:
                original_url = product.get('product_url', '')
                # 형식1: https://www.bestbuy.com/site/product-name/6557140.p
                if '/site/' in original_url and '/product/' not in original_url:
                    url_parts = original_url.split('/site/')
                    if len(url_parts) > 1:
                        product_path = url_parts[1].rsplit('/', 1)[0]
                        target_review_url = f"https://www.bestbuy.com/site/reviews/{product_path}/{sku_number}"
                # 형식2: https://www.bestbuy.com/product/product-name/CODE/sku/6578951
                elif '/product/' in original_url:
                    url_parts = original_url.split('/product/')
                    if len(url_parts) > 1:
                        product_part = url_parts[1].split('/sku/')[0]
                        product_part = product_part.rsplit('/', 1)[0]
                        target_review_url = f"https://www.bestbuy.com/site/reviews/{product_part}/{sku_number}"

            if is_no_reviews:
                print(f"├─ 리뷰 0건 - 상세 리뷰 추출 스킵")
            elif reviews_button_xpath:
                review_button_found = False

                # reviews_button + fallback XPaths
                fallback_str = self.xpaths.get('reviews_button_fallback', {}).get('xpath') or ''
                fallback_xpaths = [x.strip() for x in fallback_str.split('|||') if x.strip()]
                reviews_button_xpaths = [reviews_button_xpath] + fallback_xpaths

                # 1차: DOM에서 먼저 찾기 (4단계에서 이미 리뷰 섹션으로 스크롤됨)
                for xpath in reviews_button_xpaths:
                    try:
                        review_button = self.page.ele(f'xpath:{xpath}', timeout=2)
                        if review_button:
                            self.page.run_js("arguments[0].scrollIntoView({behavior: 'smooth', block: 'center'})", review_button)
                            time.sleep(0.5)
                            review_button.click()
                            review_button_found = True
                            print(f"├─ 리뷰 페이지 진입 성공 (1차 DOM 클릭)")
                            time.sleep(1.5)
                            break
                    except Exception:
                        continue

                # 2차: 리뷰 섹션 못 찾았으면 스크롤하며 찾기 (리뷰 섹션 찾았으면 스킵)
                if not review_button_found and not review_section_found:
                    current_position = self.page.run_js("return window.pageYOffset;")
                    scroll_height = self.page.run_js("return document.body.scrollHeight")
                    max_scroll_attempts = 5  # 15 → 5로 축소 (리뷰 URL 직접 접속이 더 빠름)

                    for _ in range(max_scroll_attempts):
                        if current_position >= scroll_height:
                            break

                        for xpath in reviews_button_xpaths:
                            try:
                                review_button = self.page.ele(f'xpath:{xpath}', timeout=0.5)  # 1초 → 0.5초
                                if review_button:
                                    self.page.run_js("arguments[0].scrollIntoView({behavior: 'smooth', block: 'center'})", review_button)
                                    time.sleep(0.5)
                                    review_button.click()
                                    review_button_found = True
                                    print(f"├─ 리뷰 페이지 진입 성공 (2차 스크롤 탐색 클릭)")
                                    time.sleep(1.5)
                                    break
                            except Exception:
                                continue

                        if review_button_found:
                            break

                        scroll_step = random.randint(400, 500)  # 더 큰 스크롤 간격
                        current_position += scroll_step
                        self.page.run_js(f"window.scrollTo({{top: {current_position}, behavior: 'smooth'}});")
                        time.sleep(0.2)  # 0.3 → 0.2초

                # 3차: 리뷰 버튼 못 찾으면 URL로 직접 접속 (1-3-1단계에서 추출한 sku_number 사용)
                if not review_button_found and target_review_url:
                    try:
                        self.page.get(target_review_url)
                        time.sleep(random.uniform(2, 3))
                        review_button_found = True  # URL 접속 성공으로 처리

                        # 설문조사 팝업 닫기
                        self.close_survey_popup()

                        print(f"├─ 리뷰 URL 직접 접속 성공 (3차): {target_review_url}")
                    except Exception as e:
                        print(f"[WARNING] 리뷰 URL 직접 접속 실패 (3차): {e}")
                        if "error page detected" in str(e).lower():
                            raise e
                elif not review_button_found and not target_review_url:
                    print(f"[WARNING] 리뷰 URL 생성 실패 - URL 형식 미지원")

                if not review_button_found:
                    print(f"├─ 리뷰 버튼 찾기(1, 2차) 및 URL 직접 접속(3차) 모두 실패하여 진입 불가 (sku_number={sku_number})")

                if review_button_found:
                    try:
                        detailed_review_xpaths = self.get_chain_xpaths('detailed_review_content')
                        if detailed_review_xpaths:
                            
                            # [단 1회 실행] 진입 실패(상세페이지 잔류) 초기 판정 및 강제 URL 다이렉트 이동
                            time.sleep(2)
                            current_review_url = self.page.url
                            
                            if target_review_url and current_review_url.split('?')[0] == detail_page_url.split('?')[0]:
                                print(f"├─ [WARNING] 변경없는 URL 감지 (상세페이지 잔류)! 리뷰 URL 강제 이동 시도...")
                                try:
                                    self.page.get(target_review_url)
                                    time.sleep(3)
                                    self.close_survey_popup()
                                    
                                    # [강제 이동 후 결과 확인] 이동 후에도 여전히 제자리라면 실패 처리
                                    if self.page.url.split('?')[0] == detail_page_url.split('?')[0]:
                                        print(f"├─ [WARNING] 강제 이동을 시도했으나 여전히 상세페이지에 잔류되어 있습니다.")
                                        raise Exception("Page load failed - stuck on detail page after forced redirect")
                                except Exception as e:
                                    print(f"├─ [WARNING] 강제 이동 불가(에러 발생): {e}")
                                    raise Exception(f"Page load failed - forced redirect aborted: {e}")
                                        
                            # [단 1회 실행] 공통 에러 페이지 감지
                            page_html = self.page.html
                            error_keywords_review = ["this site can't be reached", "err_http2_protocol_error", "we're sorry, something went wrong"]
                            if page_html and any(kw in page_html.lower() for kw in error_keywords_review):
                                print(f"├─ [WARNING] 접속된 리뷰 페이지가 에러 페이지입니다. (전체 에러로 전환)")
                                raise Exception("Page load failed - error page detected")
                                
                            # 순수 파싱 및 렌더링 지연 대비 대기 루프
                            max_review_retries = 3
                            for rev_retry in range(1, max_review_retries + 1):
                                if rev_retry > 1:
                                    print(f"├─ 상세 리뷰 파싱 0건: 렌더링/로딩 지연 의심 - 5초 대기 중... ({rev_retry}/{max_review_retries}회차)")
                                    time.sleep(5)
                                    page_html = self.page.html
                                tree = html.fromstring(page_html)

                                # chain: 여러 xpath 순서대로 시도
                                reviews_list = []
                                matched_xpath = None
                                for xpath_name, xpath_value in detailed_review_xpaths:
                                    results = tree.xpath(xpath_value)
                                    if results:
                                        reviews_list = results
                                        matched_xpath = xpath_name
                                        break
                                
                                if reviews_list:
                                    print(f"├─ 리뷰 xpath 매칭: {len(reviews_list)}건 (matched: {matched_xpath})")
                                    total_reviews_found = len(reviews_list)
                                    reviews_list = reviews_list[:20]
                                    formatted_reviews = []
                                    for idx, review in enumerate(reviews_list, 1):
                                        cleaned_review = review.replace('\r\n', ' ').replace('\n', ' ').replace('\r', ' ')
                                        cleaned_review = ' '.join(cleaned_review.split())
                                        formatted_reviews.append(f"review{idx} - {cleaned_review}")

                                    detailed_review_content = ' ||| '.join(formatted_reviews)
                                    print(f"├─ 상세 리뷰 추출: {len(formatted_reviews)}개 (전체 {total_reviews_found}개 중)")

                                    # recommendation_intent: 리뷰 페이지에서 chain 추출
                                    if recommendation_intent is None:
                                        recommendation_fallback_raw = self.safe_extract_chain(tree, 'reviewpage_recommendation_intent')
                                        if recommendation_fallback_raw:
                                            if '%' in recommendation_fallback_raw:
                                                recommendation_intent = f"{recommendation_fallback_raw} would recommend to a friend"
                                            else:
                                                recommendation_intent = f"{recommendation_fallback_raw}% would recommend to a friend"
                                            print(f"├─ recommendation_intent (리뷰페이지): {recommendation_intent}")
                                            
                                    break
                                else:
                                    if rev_retry == max_review_retries:
                                        print(f"[DEBUG] 상세 리뷰 요소를 찾을 수 없음 (총 {max_review_retries}회 시도 실패 / XPath 매칭 0건)")
                        else:
                            print(f"[DEBUG] DB 안에 'detailed_review_content' 본문용 XPath가 설정되어 있지 않음")
                    except Exception as e:
                        print(f"[DEBUG] 리뷰 본문 데이터를 추출하는 중 예외 에러 발생: {e}")
                        pass
            else:
                print(f"[DEBUG] DB에 'reviews_button' XPath가 등록되어 있지 않아 5단계 전체 (리뷰 버튼 클릭 및 상세 리뷰 추출)가 스킵됨")


            # 결합된 데이터
            combined_data = product.copy()
            combined_data.update({
                'item': item,
                'sku': sku,
                'count_of_reviews': count_of_reviews,
                'star_rating': star_rating,
                'count_of_star_ratings': count_of_star_ratings,
                'trade_in': trade_in,
                'recommendation_intent': recommendation_intent,
                'hhp_storage': hhp_storage if hhp_storage else None,
                'hhp_color': hhp_color if hhp_color else None,
                'hhp_carrier': hhp_carrier if hhp_carrier else None,
                'detailed_review_content': detailed_review_content,
                'retailer_sku_name_similar': retailer_sku_name_similar,
                'crawl_strdatetime': (datetime.now() + timedelta(hours=self.time_offset_hours)).strftime('%Y-%m-%d %H:%M:%S')
            })

            # 결과 요약 로그
            print(f"└─ 결과: ★{star_rating or '-'} | 리뷰 {count_of_reviews or '-'}개 | item={item}")

            return combined_data

        except Exception as e:
            print(f"[ERROR] Detail crawl failed: {e}")
            product['crawl_strdatetime'] = (datetime.now() + timedelta(hours=self.time_offset_hours)).strftime('%Y-%m-%d %H:%M:%S')
            return product

    def upsert_item_mst(self, product):
        """hhp_item_mst 테이블에 INSERT 또는 UPDATE
        - 조회 결과 없음 → INSERT (sku, hhp_carrier, hhp_color, hhp_storage)
        - 조회 결과 있음 → 기존 값이 NULL/빈값인 필드만 UPDATE
        """
        item = product.get('item')
        if not item:
            return

        try:
            cursor = self.db_conn.cursor()
            new_sku = product.get('sku') or ''
            product_url = product.get('product_url')
            new_carrier = product.get('hhp_carrier') or None
            new_color = product.get('hhp_color') or None
            new_storage = product.get('hhp_storage') or None

            # 기존 데이터 조회
            cursor.execute("""
                SELECT sku, hhp_carrier, hhp_color, hhp_storage FROM hhp_item_mst
                WHERE item = %s AND account_name = %s
            """, (item, self.account_name))

            row = cursor.fetchone()

            if row is None:
                # 조회 결과 없음 → INSERT
                cursor.execute("""
                    INSERT INTO hhp_item_mst (item, account_name, sku, product_url, hhp_carrier, hhp_color, hhp_storage)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                """, (item, self.account_name, new_sku, product_url, new_carrier, new_color, new_storage))
                self.db_conn.commit()
                insert_info = []
                if new_sku:
                    insert_info.append(f"sku={new_sku}")
                if new_carrier:
                    insert_info.append(f"carrier={new_carrier}")
                if new_color:
                    insert_info.append(f"color={new_color}")
                if new_storage:
                    insert_info.append(f"storage={new_storage}")
                print(f"  ├─ ITEM_MST: INSERT ({item}) - {', '.join(insert_info) if insert_info else '값 없음'}")
            else:
                # 기존 값이 없는 필드만 업데이트
                existing_sku, existing_carrier, existing_color, existing_storage = row
                updates = []
                params = []

                if not (existing_sku or '') and new_sku:
                    updates.append("sku = %s")
                    params.append(new_sku)
                if not (existing_carrier or '') and new_carrier:
                    updates.append("hhp_carrier = %s")
                    params.append(new_carrier)
                if not (existing_color or '') and new_color:
                    updates.append("hhp_color = %s")
                    params.append(new_color)
                if not (existing_storage or '') and new_storage:
                    updates.append("hhp_storage = %s")
                    params.append(new_storage)

                if updates:
                    # 업데이트할 필드와 값 저장 (로그용)
                    updated_info = []
                    if not (existing_sku or '') and new_sku:
                        updated_info.append(f"sku={new_sku}")
                    if not (existing_carrier or '') and new_carrier:
                        updated_info.append(f"carrier={new_carrier}")
                    if not (existing_color or '') and new_color:
                        updated_info.append(f"color={new_color}")
                    if not (existing_storage or '') and new_storage:
                        updated_info.append(f"storage={new_storage}")

                    updates.append("product_url = %s")
                    params.append(product_url)
                    updates.append("updated_at = %s")
                    params.append(datetime.now())
                    params.extend([item, self.account_name])

                    cursor.execute(f"""
                        UPDATE hhp_item_mst SET {', '.join(updates)}
                        WHERE item = %s AND account_name = %s
                    """, params)
                    self.db_conn.commit()
                    print(f"  ├─ ITEM_MST: UPDATE ({item}) - {', '.join(updated_info)}")
                else:
                    print(f"  ├─ ITEM_MST: SKIP ({item}) - 업데이트할 필드 없음")

            cursor.close()

        except Exception as e:
            print(f"[ERROR] upsert_item_mst failed: {item}: {e}")
            self.db_conn.rollback()

    def save_to_retail_com(self, product):
        """DB 저장: 1개씩 INSERT"""
        if not product:
            return False

        try:
            cursor = self.db_conn.cursor()

            # 테스트 모드면 test_hhp_retail_com, 통합 크롤러면 hhp_retail_com
            table_name = 'test_hhp_retail_com' if self.test_mode else 'hhp_retail_com'

            insert_query = f"""
                INSERT INTO {table_name} (
                    country, product, item, account_name, page_type,
                    count_of_reviews, retailer_sku_name, product_url,
                    star_rating, count_of_star_ratings, sku_popularity,
                    final_sku_price, original_sku_price, savings, discount_type,
                    offer,
                    pick_up_availability, fastest_delivery, delivery_availability,
                    inventory_status, sku_status,
                    trade_in, recommendation_intent,
                    hhp_storage, hhp_color, hhp_carrier,
                    detailed_review_content, summarized_review_content,
                    retailer_sku_name_similar,
                    main_rank, bsr_rank, trend_rank,
                    promotion_type,
                    calendar_week, crawl_strdatetime, batch_id
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s
                )
            """

            values = (
                'SEA', 'HHP', product.get('item'), self.account_name, product.get('page_type'),
                product.get('count_of_reviews'), product.get('retailer_sku_name'), product.get('product_url'),
                product.get('star_rating'), product.get('count_of_star_ratings'), product.get('sku_popularity'),
                product.get('final_sku_price'), product.get('original_sku_price'), product.get('savings'), product.get('discount_type'),
                product.get('offer'),
                product.get('pick_up_availability'), product.get('fastest_delivery'), product.get('delivery_availability'),
                product.get('inventory_status'), product.get('sku_status'),
                product.get('trade_in'), product.get('recommendation_intent'),
                product.get('hhp_storage'), product.get('hhp_color'), product.get('hhp_carrier'),
                product.get('detailed_review_content'), product.get('summarized_review_content'),
                product.get('retailer_sku_name_similar'),
                product.get('main_rank'), product.get('bsr_rank'), product.get('trend_rank'),
                product.get('promotion_type'),
                product.get('calendar_week'), product.get('crawl_strdatetime'), self.batch_id
            )

            cursor.execute(insert_query, values)
            self.db_conn.commit()
            cursor.close()
            return True

        except Exception as e:
            print(f"[ERROR] DB save failed: {product.get('item')}: {e}")
            traceback.print_exc()
            self.db_conn.rollback()
            return False

    def restart_browser(self):
        """브라우저 재시작 (메모리 정리)"""
        try:
            if self.page:
                self.page.quit()
                time.sleep(2)
            self.setup_drission_driver()
            print("[SUCCESS] Browser restarted")
            return True
        except Exception as e:
            print(f"[ERROR] Browser restart failed: {e}")
            return False

    def run(self):
        """실행: initialize() → load_product_list() → 제품별 crawl_detail() → save_to_retail_com() → 리소스 정리"""
        try:
            if not self.initialize():
                print("[ERROR] Initialization failed")
                return False

            product_list = self.load_product_list()
            if not product_list:
                print("[ERROR] No products found")
                return False

            total_saved = 0
            RESTART_INTERVAL = 20  # 20개마다 브라우저 재시작
            first_error_logged = False  # 첫 에러 페이지 시각 로그 여부

            crawl_start_time = datetime.now()
            print(f"[RATE-LIMIT] 수집 시작: {crawl_start_time.strftime('%H:%M:%S')}")

            for i, product in enumerate(product_list, 1):
                try:
                    # 20개마다 브라우저 재시작 (메모리 정리, 타임아웃 방지)
                    if i > 1 and (i - 1) % RESTART_INTERVAL == 0:
                        print(f"\n[INFO] 브라우저 재시작 ({i-1}개 처리 완료, 메모리 정리)")
                        if not self.restart_browser():
                            print("[WARNING] 브라우저 재시작 실패, 계속 진행")

                    sku_name = product.get('retailer_sku_name') or 'N/A'
                    print(f"\n{'='*70}")
                    print(f"[{i}/{len(product_list)}] {sku_name[:60]}")
                    print(f"{'='*70}")

                    combined_data = self.crawl_detail(product)
                    if combined_data:
                        self.upsert_item_mst(combined_data)
                        if self.save_to_retail_com(combined_data):
                            total_saved += 1

                    time.sleep(random.uniform(5, 8))

                except Exception as e:
                    error_msg = str(e).lower()
                    print(f"[ERROR] Product {i} failed: {e}")

                    if "error page detected" in error_msg and not first_error_logged:
                        elapsed = (datetime.now() - crawl_start_time).total_seconds()
                        print(f"[RATE-LIMIT] 첫 차단 발생: {datetime.now().strftime('%H:%M:%S')} (수집 시작 후 {int(elapsed)}초, {i-1}건 수집)")
                        first_error_logged = True

                    # 타임아웃 또는 페이지 로드 실패시 브라우저 재시작 후 재시도
                    if "timeout" in error_msg or "time out" in error_msg or "url unchanged" in error_msg:
                        print(f"[INFO] 브라우저 재시작 후 재시도")
                        if self.restart_browser():
                            try:
                                combined_data = self.crawl_detail(product)
                                if combined_data:
                                    self.upsert_item_mst(combined_data)
                                    if self.save_to_retail_com(combined_data):
                                        total_saved += 1
                                print(f"[SUCCESS] 재시도 성공: {sku_name[:30]}")
                            except Exception as retry_e:
                                print(f"[ERROR] 재시도 실패: {retry_e}")
                    continue

            table_name = 'test_hhp_retail_com' if self.test_mode else 'hhp_retail_com'
            print(f"[DONE] Processed: {len(product_list)}, Saved: {total_saved}, Table: {table_name}, batch_id: {self.batch_id}")
            return True

        except Exception as e:
            print(f"[ERROR] Crawler failed: {e}")
            traceback.print_exc()
            return False

        finally:
            if self.page:
                self.page.quit()
            if self.db_conn:
                self.db_conn.close()
            if self.standalone:
                input("Press Enter to exit...")


def main():
    """개별 실행 진입점 (테스트 모드, 기본 배치 ID 사용)"""
    import argparse

    parser = argparse.ArgumentParser(description='BestBuy HHP Detail Crawler')
    parser.add_argument('--time_offset', type=int, default=None, help='시간 오프셋 (기본값: 0)')
    args = parser.parse_args()

    time_offset = args.time_offset
    if time_offset is None:
        from common.setup import get_input_with_timeout
        try:
            val = get_input_with_timeout("시간 오프셋을 입력하세요 (엔터 시 기본값 0) [10초 대기]: ", default="0", timeout=10.0)
            time_offset = int(val)
        except Exception:
            time_offset = 0

    crawler = BestBuyDetailCrawler(batch_id=None, test_mode=True, time_offset_hours=time_offset)
    crawler.run()


if __name__ == '__main__':
    main()
