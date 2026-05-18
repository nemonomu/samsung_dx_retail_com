"""
BestBuy Detail Update 크롤러

================================================================================
실행 모드
================================================================================
- 개별 실행: python bby_hhp_dt_update.py --batch-id <batch_id> [--mode 1|2] [--start-id N]
- batch_id 필수 (인자 또는 stdin)
- mode: 1=item IS NULL (기본), 2=count_of_reviews IS NULL

================================================================================
주요 기능
================================================================================
- hhp_retail_com 테이블에서 조건에 맞는 레코드 조회 (디테일 수집 실패/미수집 건)
- 각 제품 상세 페이지에서 리뷰, 별점, 스펙 등 재추출
- 기존 레코드를 UPDATE (새로 추출한 필드만)

================================================================================
저장 테이블
================================================================================
- hhp_retail_com (UPDATE)
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


class BestBuyDetailUpdateCrawler(BaseCrawler):
    """
    BestBuy Detail Update 크롤러 (item IS NULL 재수집)
    """

    # 조회 조건 모드
    MODE_ITEM_NULL = '1'          # item IS NULL (페이지 에러 재수집)
    MODE_REVIEW_NULL = '2'        # count_of_reviews IS NULL (리뷰 미수집 재수집)
    MODE_BOTH = '3'               # item IS NULL OR count_of_reviews IS NULL (둘 다)

    def __init__(self, batch_id=None, start_id=None, mode=None, time_offset_hours=0):
        """초기화. batch_id: 필수, start_id: 특정 id 이후부터 조회, mode: 조회 조건"""
        super().__init__()
        self.time_offset_hours = time_offset_hours
        self.account_name = 'Bestbuy'
        self.page_type = 'detail'
        self.batch_id = batch_id
        self.start_id = start_id
        self.mode = mode or self.MODE_ITEM_NULL
        self.standalone = batch_id is None

        # DrissionPage 드라이버
        self.page = None

    def setup_drission_driver(self):
        """DrissionPage 브라우저 설정 (봇 감지 우회 강화)"""
        try:
            from DrissionPage import ChromiumOptions
            co = ChromiumOptions()
            if getattr(self, 'use_http1', False):
                co.set_argument('--disable-http2')
            self.page = ChromiumPage(co)
            print(f"[SUCCESS] DrissionPage setup complete{' (HTTP/1.1 강제)' if getattr(self, 'use_http1', False) else ''}")
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
        """초기화: DB 연결 → XPath 로드 → DrissionPage 설정 → 로그 정리"""
        if not self.batch_id:
            print("[ERROR] batch_id가 필요합니다.")
            return False

        if not self.connect_db():
            return False
        if not self.load_xpaths(self.account_name, self.page_type, 'SEA', 'HHP'):
            return False

        # DrissionPage 설정
        try:
            self.setup_drission_driver()
        except Exception as e:
            print(f"[ERROR] Initialize failed: DrissionPage setup failed - {e}")
            traceback.print_exc()
            return False

        self.cleanup_old_logs()

        return True

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

    def load_product_list(self):
        """hhp_retail_com 테이블에서 조건에 맞는 제품 조회 (UPDATE 대상)"""
        try:
            cursor = self.db_conn.cursor()

            # 모드별 조건
            if self.mode == self.MODE_BOTH:
                condition = "AND (item IS NULL OR count_of_reviews IS NULL)"
                mode_desc = "item IS NULL OR count_of_reviews IS NULL"
            elif self.mode == self.MODE_REVIEW_NULL:
                condition = "AND count_of_reviews IS NULL"
                mode_desc = "count_of_reviews IS NULL"
            else:
                condition = "AND item IS NULL"
                mode_desc = "item IS NULL"

            query = f"""
                SELECT
                    id,
                    page_type, retailer_sku_name, final_sku_price, savings,
                    original_sku_price, offer,
                    pick_up_availability, fastest_delivery, delivery_availability,
                    sku_status, promotion_type, main_rank, bsr_rank, trend_rank,
                    product_url, calendar_week
                FROM hhp_retail_com
                WHERE account_name = %s AND batch_id = %s AND product_url IS NOT NULL {condition}
            """
            params = [self.account_name, self.batch_id]

            if self.start_id:
                query += " AND id >= %s"
                params.append(self.start_id)

            query += " ORDER BY id"

            cursor.execute(query, params)
            rows = cursor.fetchall()
            cursor.close()

            product_list = []
            for row in rows:
                product = {
                    'id': row[0],  # UPDATE용 id
                    'account_name': self.account_name,
                    'page_type': row[1],
                    'retailer_sku_name': row[2],
                    'final_sku_price': row[3],
                    'savings': row[4],
                    'original_sku_price': row[5],
                    'offer': row[6],
                    'pick_up_availability': row[7],
                    'fastest_delivery': row[8],
                    'delivery_availability': row[9],
                    'sku_status': row[10],
                    'promotion_type': row[11],
                    'main_rank': row[12],
                    'bsr_rank': row[13],
                    'trend_rank': row[14],
                    'product_url': row[15],
                    'calendar_week': row[16]
                }
                product_list.append(product)

            print(f"[INFO] Loaded {len(product_list)} products ({mode_desc})")
            return product_list

        except Exception as e:
            print(f"[ERROR] Failed to load product list: {e}")
            traceback.print_exc()
            return []

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

            # 에러 페이지 / 빈 페이지 감지 (최대 3회 새로고침 재시도)
            error_keywords = ["this site can't be reached", "err_http2_protocol_error", "application error", "a client-side exception"]
            def _is_bad_page(h):
                if any(kw in h.lower() for kw in error_keywords):
                    return 'error'
                if len(h.strip()) < 500 or '<body></body>' in h.lower() or '<body> </body>' in h.lower():
                    return 'empty'
                return None

            MAX_REFRESH_RETRY = 3
            for refresh_attempt in range(1, MAX_REFRESH_RETRY + 1):
                bad_type = _is_bad_page(page_html)
                if bad_type:
                    print(f"[WARNING] {'에러' if bad_type == 'error' else '빈'} 페이지 감지 - 새로고침 재시도 ({refresh_attempt}/{MAX_REFRESH_RETRY})")
                    time.sleep(random.uniform(2, 3))
                    self.page.refresh()
                    time.sleep(random.uniform(3, 5))
                    page_html = self.page.html
                else:
                    break
            else:
                bad_type = _is_bad_page(page_html)
                if bad_type:
                    print(f"[WARNING] {'에러' if bad_type == 'error' else '빈'} 페이지 감지 - {MAX_REFRESH_RETRY}회 재시도 실패")
                    raise Exception("Page load failed - error page detected")

            tree = html.fromstring(page_html)

            # 원본 URL에서 item 추출 (페이지 로드 후 추출 - 에러 시 item NULL로 식별)
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

                # 2차: DOM에서 못 찾으면 스크롤하며 찾기
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

                    if recommendation_intent is None:
                        recommendation_intent_raw = self.safe_extract(tree, 'recommendation_intent')
                        if recommendation_intent_raw:
                            if '%' in recommendation_intent_raw:
                                recommendation_intent = f"{recommendation_intent_raw} would recommend to a friend"
                            else:
                                recommendation_intent = f"{recommendation_intent_raw}% would recommend to a friend"
                            print(f"├─ recommendation_intent (상세페이지): {recommendation_intent}")

                    # 필수 필드 모두 추출 성공하면 종료
                    if star_rating and count_of_reviews and count_of_star_ratings and recommendation_intent:
                        if attempt > 1:
                            print(f"├─ 리뷰 데이터 추출 완료 (재시도 {attempt}회)")
                        break

                    # 재시도 필요
                    if attempt < MAX_RETRY:
                        time.sleep(1)
                    else:
                        # 마지막 시도에서도 실패
                        missing = []
                        if not star_rating: missing.append('star_rating')
                        if not count_of_reviews: missing.append('count_of_reviews')
                        if not count_of_star_ratings: missing.append('count_of_star_ratings')
                        if not recommendation_intent: missing.append('recommendation_intent')
                        if missing:
                            print(f"├─ 리뷰 데이터 일부 미추출: {', '.join(missing)}")

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
                            error_keywords = ["this site can't be reached", "err_http2_protocol_error", "we're sorry, something went wrong"]
                            if any(kw in page_html.lower() for kw in error_keywords):
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
            if "error page detected" in str(e).lower():
                raise
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
            existing = cursor.fetchone()

            if not existing:
                # 신규 INSERT
                cursor.execute("""
                    INSERT INTO hhp_item_mst (item, account_name, sku, product_url, hhp_carrier, hhp_color, hhp_storage, created_at, updated_at)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, (item, self.account_name, new_sku, product_url, new_carrier, new_color, new_storage, datetime.now(), datetime.now()))
            else:
                # 기존 값이 NULL/빈값인 필드만 UPDATE
                updates = []
                params = []
                old_sku, old_carrier, old_color, old_storage = existing

                if (not old_sku or old_sku == '') and new_sku:
                    updates.append("sku = %s")
                    params.append(new_sku)
                if not old_carrier and new_carrier:
                    updates.append("hhp_carrier = %s")
                    params.append(new_carrier)
                if not old_color and new_color:
                    updates.append("hhp_color = %s")
                    params.append(new_color)
                if not old_storage and new_storage:
                    updates.append("hhp_storage = %s")
                    params.append(new_storage)

                # product_url은 항상 업데이트
                if product_url:
                    updates.append("product_url = %s")
                    params.append(product_url)

                if updates:
                    updates.append("updated_at = %s")
                    params.append(datetime.now())
                    params.append(item)
                    params.append(self.account_name)
                    cursor.execute(f"""
                        UPDATE hhp_item_mst SET {', '.join(updates)}
                        WHERE item = %s AND account_name = %s
                    """, params)

            self.db_conn.commit()
            cursor.close()

        except Exception as e:
            print(f"[ERROR] upsert_item_mst failed: {item}: {e}")
            traceback.print_exc()
            self.db_conn.rollback()

    def update_retail_com(self, product):
        """DB 저장: id 기준으로 UPDATE (새로 추출한 필드만)"""
        if not product:
            return False

        row_id = product.get('id')
        if not row_id:
            print(f"[ERROR] DB update failed: id가 없음")
            return False

        try:
            cursor = self.db_conn.cursor()

            updates = []
            params = []

            # item은 항상 업데이트 (이 크롤러의 핵심 목적)
            if product.get('item'):
                updates.append("item = %s")
                params.append(product.get('item'))

            # 크롤링으로 새로 추출한 필드들 (NOT NULL인 것만)
            update_fields = [
                ('count_of_reviews', 'count_of_reviews'),
                ('star_rating', 'star_rating'),
                ('count_of_star_ratings', 'count_of_star_ratings'),
                ('sku_popularity', 'sku_popularity'),
                ('trade_in', 'trade_in'),
                ('recommendation_intent', 'recommendation_intent'),
                ('hhp_carrier', 'hhp_carrier'),
                ('hhp_storage', 'hhp_storage'),
                ('hhp_color', 'hhp_color'),
                ('detailed_review_content', 'detailed_review_content'),
                ('summarized_review_content', 'summarized_review_content'),
                ('retailer_sku_name_similar', 'retailer_sku_name_similar'),
            ]

            for db_col, key in update_fields:
                value = product.get(key)
                if value is not None:
                    updates.append(f"{db_col} = %s")
                    params.append(value)

            # crawl_strdatetime은 항상 업데이트
            updates.append("crawl_strdatetime = %s")
            params.append(product.get('crawl_strdatetime'))

            if not updates:
                print(f"[WARNING] 업데이트할 컬럼 없음: id={row_id}")
                cursor.close()
                return False

            params.append(row_id)
            update_query = f"UPDATE hhp_retail_com SET {', '.join(updates)} WHERE id = %s"

            cursor.execute(update_query, params)
            self.db_conn.commit()
            cursor.close()
            return True

        except Exception as e:
            print(f"[ERROR] DB update failed: id={row_id}, {e}")
            traceback.print_exc()
            self.db_conn.rollback()
            return False

    def restart_browser(self):
        """브라우저 재시작 (메모리 정리)"""
        try:
            if self.page:
                self.page.quit()
        except Exception:
            pass

        try:
            time.sleep(2)
            self.setup_drission_driver()
            return True
        except Exception as e:
            print(f"[ERROR] Browser restart failed: {e}")
            return False

    def run(self):
        """실행: initialize() → load_product_list() → 제품별 crawl_detail() → update_retail_com() → 리소스 정리"""
        try:
            # 로그 파일 생성
            self.start_logging(self.batch_id or 'dt_update')

            if not self.initialize():
                print("[ERROR] Initialization failed")
                return False

            product_list = self.load_product_list()
            if not product_list:
                print("[INFO] No products to update")
                return True  # 업데이트 대상 없음 (에러 아님)

            total_updated = 0
            RESTART_INTERVAL = 20  # 20개마다 브라우저 재시작
            first_error_logged = False  # 첫 에러 페이지 시각 로그 여부

            crawl_start_time = datetime.now()
            print(f"[RATE-LIMIT] 수집 시작: {(crawl_start_time + timedelta(hours=self.time_offset_hours)).strftime('%H:%M:%S')}")

            for i, product in enumerate(product_list, 1):
                try:
                    # 20개마다 브라우저 재시작 (메모리 정리, 타임아웃 방지)
                    if i > 1 and (i - 1) % RESTART_INTERVAL == 0:
                        print(f"\n[INFO] 브라우저 재시작 ({i-1}개 처리 완료, 메모리 정리)")
                        if not self.restart_browser():
                            print("[WARNING] 브라우저 재시작 실패, 계속 진행")

                    sku_name = product.get('retailer_sku_name') or 'N/A'
                    row_id = product.get('id')
                    print(f"\n{'='*70}")
                    print(f"[{i}/{len(product_list)}] (id={row_id}) {sku_name}")
                    print(f"{'='*70}")

                    combined_data = self.crawl_detail(product)
                    if combined_data:
                        self.upsert_item_mst(combined_data)
                        if self.update_retail_com(combined_data):
                            total_updated += 1

                    time.sleep(random.uniform(5, 8))

                except Exception as e:
                    error_msg = str(e).lower()
                    print(f"[ERROR] Product {i} failed: {e}")

                    # 에러 페이지 감지 → 브라우저 종료 + 20분 대기 후 재시작
                    if "error page detected" in error_msg:
                        if not first_error_logged:
                            elapsed = (datetime.now() - crawl_start_time).total_seconds()
                            print(f"[RATE-LIMIT] 첫 차단 발생: {(datetime.now() + timedelta(hours=self.time_offset_hours)).strftime('%H:%M:%S')} (수집 시작 후 {int(elapsed)}초, {i-1}건 수집)")
                            first_error_logged = True
                        print(f"[INFO] 에러 페이지 감지 - 브라우저 종료 후 20분 대기")
                        if self.page:
                            try:
                                self.page.quit()
                                self.page = None
                            except Exception:
                                pass
                        wait_minutes = 20
                        for remaining in range(wait_minutes * 60, 0, -60):
                            print(f"[WAIT] {remaining // 60}분 남음...")
                            time.sleep(60)
                        print(f"[INFO] 대기 완료 - 브라우저 재시작")
                        self.setup_drission_driver()
                        try:
                            combined_data = self.crawl_detail(product)
                            if combined_data:
                                self.upsert_item_mst(combined_data)
                                if self.update_retail_com(combined_data):
                                    total_updated += 1
                            print(f"[SUCCESS] 재시도 성공: {sku_name[:30]}")
                        except Exception as retry_e:
                            print(f"[ERROR] 재시도 실패: {retry_e}")
                        continue

                    # 타임아웃 또는 페이지 로드 실패시 브라우저 종료 + 20분 대기 후 재시작
                    if "timeout" in error_msg or "time out" in error_msg or "url unchanged" in error_msg:
                        print(f"[INFO] 타임아웃 감지 - 브라우저 종료 후 20분 대기")
                        if self.page:
                            try:
                                self.page.quit()
                                self.page = None
                            except Exception:
                                pass
                        wait_minutes = 20
                        for remaining in range(wait_minutes * 60, 0, -60):
                            print(f"[WAIT] {remaining // 60}분 남음...")
                            time.sleep(60)
                        print(f"[INFO] 대기 완료 - 브라우저 재시작")
                        self.setup_drission_driver()
                        try:
                            combined_data = self.crawl_detail(product)
                            if combined_data:
                                self.upsert_item_mst(combined_data)
                                if self.update_retail_com(combined_data):
                                    total_updated += 1
                            print(f"[SUCCESS] 재시도 성공: {sku_name[:30]}")
                        except Exception as retry_e:
                            print(f"[ERROR] 재시도 실패: {retry_e}")
                    continue

            print(f"[DONE] Processed: {len(product_list)}, Updated: {total_updated}, batch_id: {self.batch_id}")
            return True

        except Exception as e:
            print(f"[ERROR] Crawler failed: {e}")
            traceback.print_exc()
            return False

        finally:
            self.stop_logging()
            if self.page:
                self.page.quit()
            if self.db_conn:
                self.db_conn.close()
            input("Press Enter to exit...")


TIME_OFFSET_HOURS = 0


def fetch_today_batch_ids():
    """오늘 날짜의 BestBuy batch_id 목록을 DB에서 조회"""
    import psycopg2
    from config import DB_CONFIG
    today_str = (datetime.now() + timedelta(hours=TIME_OFFSET_HOURS)).strftime('%Y%m%d')
    try:
        conn = psycopg2.connect(**DB_CONFIG, database='postgres')
        cursor = conn.cursor()
        cursor.execute(
            "SELECT DISTINCT batch_id FROM hhp_retail_com "
            "WHERE account_name = 'Bestbuy' AND batch_id LIKE %s "
            "ORDER BY batch_id DESC",
            (f'b_{today_str}%',)
        )
        batch_ids = [row[0] for row in cursor.fetchall()]
        cursor.close()
        conn.close()
        return batch_ids
    except Exception as e:
        print(f"[WARN] batch_id 조회 실패: {e}")
        return []


def main():
    """개별 실행 진입점"""
    import argparse
    parser = argparse.ArgumentParser(description='BestBuy HHP Detail Update Crawler')
    parser.add_argument('--batch-id', type=str, help='Batch ID to process')
    parser.add_argument('--start-id', type=int, help='Start from this id (WHERE id >= start_id)')
    parser.add_argument('--mode', type=str, choices=['1', '2', '3'], help='1: item IS NULL, 2: count_of_reviews IS NULL, 3: both')
    parser.add_argument('--time_offset', type=int, default=None, help='시간 오프셋 (기본값: 0)')
    args = parser.parse_args()

    global TIME_OFFSET_HOURS
    time_offset_arg = getattr(args, 'time_offset', None)
    if time_offset_arg is not None:
        TIME_OFFSET_HOURS = time_offset_arg
    else:
        print("\n시간 오프셋 (수집 시간에 더할 시간) 선택:")
        print("   4 : 기본값 (서버 시간 + 4시간)")
        print("   0 : 현재 서버 시간 그대로 사용")
        print("  -4 : 서버 시간 - 4시간")
        offset_input = input("오프셋 시간 입력 (기본: 4): ").strip()
        try:
            TIME_OFFSET_HOURS = int(offset_input) if offset_input else 4
        except ValueError:
            TIME_OFFSET_HOURS = 4

    batch_id = args.batch_id
    if not batch_id:
        today_batch_ids = fetch_today_batch_ids()
        if today_batch_ids:
            print(f"\n오늘({(datetime.now() + timedelta(hours=TIME_OFFSET_HOURS)).strftime('%Y-%m-%d')}) BestBuy batch_id 목록:")
            for i, bid in enumerate(today_batch_ids, 1):
                print(f"  {i}. {bid}")
            print(f"  0. 직접 입력")
            choice = input("\n번호 선택 ").strip()
            if choice.isdigit() and 1 <= int(choice) <= len(today_batch_ids):
                batch_id = today_batch_ids[int(choice) - 1]
            elif choice == '0' or not choice.isdigit():
                if choice == '0':
                    batch_id = input("batch_id 입력: ").strip()
                else:
                    batch_id = choice
        else:
            print(f"오늘({(datetime.now() + timedelta(hours=TIME_OFFSET_HOURS)).strftime('%Y-%m-%d')}) BestBuy batch_id가 없습니다.")
            batch_id = input("batch_id 직접 입력: ").strip()
        if not batch_id:
            print("[ERROR] batch_id가 필요합니다.")
            return

    start_id = args.start_id
    if not start_id:
        start_id_input = input("시작 id (엔터: 처음부터): ").strip()
        start_id = int(start_id_input) if start_id_input else None

    mode = args.mode
    if not mode:
        print("조회 모드 선택:")
        print("  1: item IS NULL (페이지 에러 재수집)")
        print("  2: count_of_reviews IS NULL (리뷰 미수집 재수집)")
        print("  3: 둘 다 (item IS NULL OR count_of_reviews IS NULL)")
        mode_input = input("모드 입력 (기본: 1): ").strip()
        mode = mode_input if mode_input in ('1', '2', '3') else '1'

    print("\nHTTP 프로토콜 선택:")
    print("  1. HTTP/1.1 강제")
    print("  2. HTTP/2 (기본)")
    http_input = input("선택 (기본: 2): ").strip()
    use_http1 = http_input == '1'

    crawler = BestBuyDetailUpdateCrawler(batch_id=batch_id, start_id=start_id, mode=mode, time_offset_hours=TIME_OFFSET_HOURS)
    crawler.use_http1 = use_http1
    crawler.run()


if __name__ == '__main__':
    main()
