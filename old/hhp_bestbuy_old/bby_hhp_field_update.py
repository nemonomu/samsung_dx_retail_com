"""
BestBuy HHP 필드별 재수집 도구

================================================================================
용도
================================================================================
- 특정 batch_id의 데이터 중 선택한 필드를 재수집하여 UPDATE
- dt_update와 동일한 추출 로직 사용, 선택한 필드만 UPDATE
- crawl_strdatetime(수집시간)은 업데이트하지 않음

================================================================================
사용법
================================================================================
1. 스크립트 실행
2. 모드 선택 (테스트/운영)
3. 필드 선택 (복수 선택 가능, 쉼표 구분)
4. batch_id 선택
5. 범위 선택 (null/all)

================================================================================
지원 필드
================================================================================
- detailed_review_content: 리뷰 본문
- top_mentions: 탑 멘션
- recommendation_intent: 추천 의향
- count_of_reviews: 리뷰 수
- star_rating: 별점
- retailer_sku_name: 제품명
- trade_in: 트레이드인
- hhp_carrier: 통신사
- hhp_storage: 저장용량
- hhp_color: 색상
"""

import sys
import os
import time
import random
import re
import traceback
from datetime import datetime, timedelta
from lxml import html

# 공통 환경 설정
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
from common.setup import setup_environment
setup_environment(__file__)

from DrissionPage import ChromiumPage
from common.base_crawler import BaseCrawler
from common.data_extractor import extract_numeric_value


SUPPORTED_FIELDS = [
    'detailed_review_content',
    'top_mentions',
    'recommendation_intent',
    'count_of_reviews',
    'star_rating',
    'trade_in',
    'retailer_sku_name',
    'retailer_sku_name_similar',
]


class FieldUpdateCrawler(BaseCrawler):
    """
    필드별 재수집 크롤러 — dt_update 추출 로직 사용, 선택 필드만 UPDATE
    """

    def __init__(self, selected_fields, batch_id, test_mode=False, null_only=True, time_offset_hours=0):
        """초기화"""
        super().__init__()
        self.account_name = 'Bestbuy'
        self.page_type = 'detail'
        self.selected_fields = selected_fields
        self.batch_id = batch_id
        self.test_mode = test_mode
        self.null_only = null_only
        self.time_offset_hours = time_offset_hours
        self.table_name = 'test_hhp_retail_com' if test_mode else 'hhp_retail_com'
        self.page = None

    def setup_browser(self):
        """DrissionPage 브라우저 설정"""
        try:
            self.page = ChromiumPage()
            print("[SUCCESS] DrissionPage setup complete")
            return True
        except Exception as e:
            print(f"[ERROR] DrissionPage setup failed: {e}")
            traceback.print_exc()
            return False

    def close_survey_popup(self):
        """설문조사 팝업 감지 및 닫기"""
        try:
            survey_no_button = self.page.ele('#survey_invite_no', timeout=2)
            if survey_no_button:
                survey_no_button.click()
                print("[INFO] Survey popup closed")
                time.sleep(1)
        except Exception:
            pass

    def initialize_session(self):
        """세션 초기화: BestBuy 메인 페이지 방문"""
        try:
            print("[INFO] 세션 초기화 중...")
            self.page.get('https://www.bestbuy.com')
            time.sleep(random.uniform(3, 5))
            self.close_survey_popup()
            print("[INFO] 세션 초기화 완료")
            return True
        except Exception as e:
            print(f"[ERROR] 세션 초기화 실패: {e}")
            return False

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
            if item.endswith('.p'):
                item = item[:-2]
            return item if item else None
        except Exception:
            return None

    def extract_rating(self, text):
        return extract_numeric_value(text, include_comma=False, include_decimal=True)

    def extract_review_count(self, text):
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
            return (row[0], row[1], row[2]) if row else (None, None, None)
        except Exception:
            return None, None, None

    def load_target_items(self):
        """선택한 필드가 NULL인 제품 조회"""
        try:
            cursor = self.db_conn.cursor()

            if self.null_only:
                null_conditions = []
                for field in self.selected_fields:
                    null_conditions.append(f"({field} IS NULL OR {field} = '')")
                where_null = ' OR '.join(null_conditions)

                query = f"""
                    SELECT id, item, product_url, retailer_sku_name
                    FROM {self.table_name}
                    WHERE batch_id = %s AND account_name = %s AND product_url IS NOT NULL
                    AND ({where_null})
                """

                if 'detailed_review_content' in self.selected_fields:
                    query += " AND (count_of_reviews IS NULL OR count_of_reviews != '0')"
                
                if 'recommendation_intent' in self.selected_fields:
                    query += " AND (star_rating IS NOT NULL AND star_rating != 'Not yet reviewed')"

                scope_text = "NULL만"
            else:
                query = f"""
                    SELECT id, item, product_url, retailer_sku_name
                    FROM {self.table_name}
                    WHERE batch_id = %s AND account_name = %s AND product_url IS NOT NULL
                """
                scope_text = "전체"

            query += " ORDER BY id"

            cursor.execute(query, (self.batch_id, self.account_name))
            rows = cursor.fetchall()
            cursor.close()

            items = [{'id': r[0], 'item': r[1], 'product_url': r[2], 'retailer_sku_name': r[3]} for r in rows]
            print(f"[INFO] 재수집 대상: {len(items)}건 (범위: {scope_text})")
            return items

        except Exception as e:
            print(f"[ERROR] 대상 조회 실패: {e}")
            traceback.print_exc()
            return []

    def crawl_detail(self, product):
        """상세 페이지 크롤링 — dt_update와 동일한 추출 로직"""
        try:
            product_url = product.get('product_url')
            if not product_url:
                return None

            self.page.set.headers({'Referer': 'https://www.bestbuy.com/site/searchpage.jsp?st=cellphone'})

            previous_url = self.page.url if self.page else None
            self.page.get(product_url)
            time.sleep(random.uniform(3, 5))

            current_url = self.page.url
            if previous_url and current_url == previous_url:
                raise Exception("Page load failed - URL unchanged")

            self.close_survey_popup()

            page_html = self.page.html

            # 에러 페이지 감지
            error_keywords = ["this site can't be reached", "err_http2_protocol_error"]
            if any(kw in page_html.lower() for kw in error_keywords):
                raise Exception("Page load failed - error page detected")

            tree = html.fromstring(page_html)
            item = self.extract_item_from_url(product_url)

            result = {'id': product.get('id'), 'item': item}

            # 리뷰 관련 필드가 선택되었는지 판별
            REVIEW_SECTION_FIELDS = {'top_mentions', 'recommendation_intent', 'count_of_reviews', 'star_rating'}
            REVIEW_PAGE_FIELDS = {'detailed_review_content', 'top_mentions', 'recommendation_intent'}
            need_review_page = bool(REVIEW_PAGE_FIELDS & set(self.selected_fields))
            need_review_section = bool(REVIEW_SECTION_FIELDS & set(self.selected_fields)) or need_review_page

            # ========== 1단계: 상단 정보 추출 ==========
            top_star_rating = None
            top_count_of_reviews = None
            trade_in = None

            for attempt in range(1, 4):
                page_html = self.page.html
                tree = html.fromstring(page_html)

                if top_star_rating is None:
                    top_star_rating = self.safe_extract(tree, 'top_star_rating')
                if top_count_of_reviews is None:
                    top_count_of_reviews = self.safe_extract(tree, 'top_count_of_reviews')
                if 'trade_in' in self.selected_fields and trade_in is None:
                    trade_in = self.safe_extract(tree, 'trade_in')
                    if trade_in:
                        print(f"├─ trade_in: {trade_in}")

                if top_star_rating and top_count_of_reviews:
                    break
                if top_count_of_reviews and 'Not yet reviewed' in top_count_of_reviews:
                    break
                if attempt < 3:
                    time.sleep(1)

            result['trade_in'] = trade_in

            # ========== 1-1단계: SKU Number 추출 (리뷰 페이지 접속용) ==========
            sku_number = None
            if need_review_page:
                sku_number_raw = self.safe_extract(tree, 'sku_number')
                sku_number = ''.join(filter(str.isdigit, sku_number_raw)) if sku_number_raw else None

            # retailer_sku_name
            if 'retailer_sku_name' in self.selected_fields:
                result['retailer_sku_name'] = self.safe_extract(tree, 'retailer_sku_name') or product.get('retailer_sku_name')

            # ========== 유사 제품 추출 ==========
            if 'retailer_sku_name_similar' in self.selected_fields:
                similar_products_container_xpath = self.xpaths.get('similar_products_container', {}).get('xpath')
                retailer_sku_name_similar = None

                if similar_products_container_xpath:
                    similar_products_found = False

                    # 1차: DOM에서 먼저 찾기
                    try:
                        similar_elements = self.page.eles(f'xpath:{similar_products_container_xpath}')
                        if similar_elements:
                            similar_products_found = True
                            self.page.run_js("arguments[0].scrollIntoView({behavior: 'smooth', block: 'center'})", similar_elements[0])
                            time.sleep(0.5)
                    except Exception:
                        pass

                    # 2차: 스크롤하며 찾기
                    if not similar_products_found:
                        current_scroll = self.page.run_js("return window.pageYOffset;")
                        page_height = self.page.run_js("return document.body.scrollHeight")
                        for _ in range(5):
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
                            if retailer_sku_name_similar:
                                print(f"├─ 유사제품: {len(similar_product_names)}개")
                    except Exception:
                        retailer_sku_name_similar = None

                result['retailer_sku_name_similar'] = retailer_sku_name_similar

            # 리뷰 관련 필드가 선택 안 됐으면 여기서 리턴
            if not need_review_section and not need_review_page:
                print(f"└─ 상단 정보만 추출 완료")
                return result

            # ========== 2단계: 리뷰 섹션 ==========
            NO_REVIEWS_TEXTS = ['not yet reviewed']
            star_rating = None
            count_of_reviews = None
            count_of_star_ratings = None
            top_mentions = None
            recommendation_intent = None

            is_no_reviews = False
            if top_count_of_reviews:
                if any(t in top_count_of_reviews.lower() for t in NO_REVIEWS_TEXTS):
                    is_no_reviews = True
                elif 'from' in top_count_of_reviews.lower():
                    is_no_reviews = True

            # 리뷰 섹션 스크롤
            review_section_found = False
            review_section_xpath = self.xpaths.get('review_section', {}).get('xpath')
            if not is_no_reviews and review_section_xpath and need_review_section:
                try:
                    review_section = self.page.ele(f'xpath:{review_section_xpath}', timeout=2)
                    if review_section:
                        self.page.run_js("arguments[0].scrollIntoView({behavior: 'smooth', block: 'center'})", review_section)
                        time.sleep(1)
                        review_section_found = True
                        print(f"├─ review section found")
                except Exception:
                    pass

                if not review_section_found:
                    current_scroll = self.page.run_js("return window.pageYOffset;")
                    page_height = self.page.run_js("return document.body.scrollHeight")
                    for attempt in range(5):
                        if current_scroll >= page_height:
                            break
                        try:
                            review_section = self.page.ele(f'xpath:{review_section_xpath}', timeout=0.5)
                            if review_section:
                                self.page.run_js("arguments[0].scrollIntoView({behavior: 'smooth', block: 'center'})", review_section)
                                time.sleep(1)
                                review_section_found = True
                                break
                        except Exception:
                            pass
                        scroll_step = random.randint(150, 200)
                        current_scroll += scroll_step
                        self.page.run_js(f"window.scrollTo({{top: {current_scroll}, behavior: 'smooth'}});")
                        time.sleep(0.15)

            if is_no_reviews:
                count_of_reviews = "0"
                star_rating = "Not yet reviewed"
                count_of_star_ratings = "0"
            elif need_review_section:
                for attempt in range(1, 4):
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

                    if 'top_mentions' in self.selected_fields and top_mentions is None:
                        top_mentions_raw = self.safe_extract_chain_join(tree, 'top_mentions', separator=', ')
                        if top_mentions_raw:
                            top_mentions = ', '.join([re.sub(r'\s*\(\d+\)\s*', '', m).strip() for m in top_mentions_raw.split(', ')])
                            print(f"├─ top_mentions (상세페이지): {top_mentions}")

                    if 'recommendation_intent' in self.selected_fields and recommendation_intent is None:
                        recommendation_intent_raw = self.safe_extract_chain(tree, 'recommendation_intent')
                        if recommendation_intent_raw:
                            if '%' in recommendation_intent_raw:
                                recommendation_intent = f"{recommendation_intent_raw} would recommend to a friend"
                            else:
                                recommendation_intent = f"{recommendation_intent_raw}% would recommend to a friend"
                            print(f"├─ recommendation_intent (상세페이지): {recommendation_intent}")

                    if star_rating and count_of_reviews:
                        break
                    if attempt < 3:
                        time.sleep(1)

            result['count_of_reviews'] = count_of_reviews
            result['star_rating'] = star_rating
            result['count_of_star_ratings'] = count_of_star_ratings
            result['top_mentions'] = top_mentions
            result['recommendation_intent'] = recommendation_intent

            # ========== 3단계: 리뷰 상세 + fallback 추출 ==========
            if need_review_page:
                still_need = False
                if 'detailed_review_content' in self.selected_fields:
                    still_need = True
                elif 'top_mentions' in self.selected_fields and top_mentions is None:
                    still_need = True
                elif 'recommendation_intent' in self.selected_fields and recommendation_intent is None:
                    still_need = True
                need_review_page = still_need

            if not need_review_page:
                print(f"└─ 리뷰 페이지 접속 불필요 (필요 데이터 모두 추출됨) — 스킵")
                return result

            detailed_review_content = None

            if is_no_reviews:
                print(f"├─ 리뷰 0건 - 상세 리뷰 추출 스킵")
            else:
                reviews_button_xpath = self.xpaths.get('reviews_button', {}).get('xpath')
                review_button_found = False

                if reviews_button_xpath:
                    fallback_str = self.xpaths.get('reviews_button_fallback', {}).get('xpath') or ''
                    fallback_xpaths = [x.strip() for x in fallback_str.split('|||') if x.strip()]
                    reviews_button_xpaths = [reviews_button_xpath] + fallback_xpaths

                    # 1차: DOM에서 찾기
                    for xpath in reviews_button_xpaths:
                        try:
                            review_button = self.page.ele(f'xpath:{xpath}', timeout=2)
                            if review_button:
                                self.page.run_js("arguments[0].scrollIntoView({behavior: 'smooth', block: 'center'})", review_button)
                                time.sleep(0.5)
                                review_button.click()
                                review_button_found = True
                                time.sleep(1.5)
                                break
                        except Exception:
                            continue

                    # 2차: 스크롤하며 찾기
                    if not review_button_found and not review_section_found:
                        current_position = self.page.run_js("return window.pageYOffset;")
                        scroll_height = self.page.run_js("return document.body.scrollHeight")
                        for _ in range(5):
                            if current_position >= scroll_height:
                                break
                            for xpath in reviews_button_xpaths:
                                try:
                                    review_button = self.page.ele(f'xpath:{xpath}', timeout=0.5)
                                    if review_button:
                                        self.page.run_js("arguments[0].scrollIntoView({behavior: 'smooth', block: 'center'})", review_button)
                                        time.sleep(0.5)
                                        review_button.click()
                                        review_button_found = True
                                        time.sleep(1.5)
                                        break
                                except Exception:
                                    continue
                            if review_button_found:
                                break
                            scroll_step = random.randint(400, 500)
                            current_position += scroll_step
                            self.page.run_js(f"window.scrollTo({{top: {current_position}, behavior: 'smooth'}});")
                            time.sleep(0.2)

                    # 3차: URL로 직접 접속
                    if not review_button_found and sku_number:
                        try:
                            review_url = None
                            if '/site/' in product_url and '/product/' not in product_url:
                                url_parts = product_url.split('/site/')
                                if len(url_parts) > 1:
                                    product_path = url_parts[1].rsplit('/', 1)[0]
                                    review_url = f"https://www.bestbuy.com/site/reviews/{product_path}/{sku_number}"
                            elif '/product/' in product_url:
                                url_parts = product_url.split('/product/')
                                if len(url_parts) > 1:
                                    product_part = url_parts[1].split('/sku/')[0]
                                    product_part = product_part.rsplit('/', 1)[0]
                                    review_url = f"https://www.bestbuy.com/site/reviews/{product_part}/{sku_number}"

                            if review_url:
                                self.page.get(review_url)
                                time.sleep(random.uniform(2, 3))

                                # 리뷰 페이지 에러 감지
                                review_html = self.page.html
                                if any(kw in review_html.lower() for kw in error_keywords):
                                    print(f"[WARNING] 리뷰 페이지 에러 감지")
                                    raise Exception("Page load failed - error page detected")

                                review_button_found = True
                                self.close_survey_popup()
                                print(f"├─ 리뷰 URL 접속: {review_url}")
                        except Exception as e:
                            print(f"[WARNING] 리뷰 URL 접속 실패: {e}")
                            if "error page detected" in str(e).lower():
                                raise

                if review_button_found:
                    try:
                        time.sleep(2)
                        page_html = self.page.html
                        tree = html.fromstring(page_html)

                        detailed_review_xpaths = self.get_chain_xpaths('detailed_review_content')
                        if 'detailed_review_content' in self.selected_fields and detailed_review_xpaths:
                            # chain: 여러 xpath 순서대로 시도
                            reviews_list = []
                            matched_xpath = None
                            for xpath_name, xpath_value in detailed_review_xpaths:
                                results = tree.xpath(xpath_value)
                                if results:
                                    reviews_list = results
                                    matched_xpath = xpath_name
                                    break
                            print(f"├─ 리뷰 xpath 매칭: {len(reviews_list)}건 (matched: {matched_xpath})")
                            if reviews_list:
                                total_reviews_found = len(reviews_list)
                                reviews_list = reviews_list[:20]
                                formatted_reviews = []
                                for idx, review in enumerate(reviews_list, 1):
                                    cleaned_review = review.replace('\r\n', ' ').replace('\n', ' ').replace('\r', ' ')
                                    cleaned_review = ' '.join(cleaned_review.split())
                                    formatted_reviews.append(f"review{idx} - {cleaned_review}")
                                detailed_review_content = ' ||| '.join(formatted_reviews)
                                print(f"├─ 상세 리뷰: {len(formatted_reviews)}개 (전체 {total_reviews_found}개 중)")

                        # top_mentions fallback (리뷰페이지)
                        if 'top_mentions' in self.selected_fields and top_mentions is None:
                            for tm_attempt in range(1, 4):
                                top_mentions_fallback_raw = self.safe_extract_chain_join(tree, 'reviewpage_top_mentions', separator=', ')
                                if top_mentions_fallback_raw:
                                    top_mentions = ', '.join([re.sub(r'\s*\(\d+\)\s*', '', m).strip() for m in top_mentions_fallback_raw.split(', ')])
                                    print(f"├─ top_mentions (리뷰페이지): {top_mentions}")
                                    break
                                if tm_attempt < 3:
                                    time.sleep(2)
                                    page_html = self.page.html
                                    tree = html.fromstring(page_html)

                        # recommendation_intent fallback (리뷰페이지)
                        if 'recommendation_intent' in self.selected_fields and recommendation_intent is None:
                            recommendation_fallback_raw = self.safe_extract_chain(tree, 'reviewpage_recommendation_intent')
                            if recommendation_fallback_raw:
                                if '%' in recommendation_fallback_raw:
                                    recommendation_intent = f"{recommendation_fallback_raw} would recommend to a friend"
                                else:
                                    recommendation_intent = f"{recommendation_fallback_raw}% would recommend to a friend"
                                print(f"├─ recommendation_intent (리뷰페이지): {recommendation_intent}")
                    except Exception:
                        pass

            result['detailed_review_content'] = detailed_review_content
            result['top_mentions'] = top_mentions
            result['recommendation_intent'] = recommendation_intent

            print(f"└─ 결과: ★{star_rating or '-'} | 리뷰 {count_of_reviews or '-'}개 | item={item}")

            return result

        except Exception as e:
            print(f"[ERROR] Detail crawl failed: {e}")
            if "error page detected" in str(e).lower():
                raise
            return None

    def update_selected_fields(self, data):
        """선택한 필드만 UPDATE (crawl_strdatetime 업데이트 안 함)"""
        if not data:
            return False

        row_id = data.get('id')
        if not row_id:
            return False

        try:
            cursor = self.db_conn.cursor()
            updates = []
            params = []

            update_fields = list(self.selected_fields)
            if 'count_of_reviews' in self.selected_fields and 'count_of_star_ratings' not in update_fields:
                update_fields.append('count_of_star_ratings')

            for field in update_fields:
                value = data.get(field)
                if value is not None:
                    updates.append(f"{field} = CASE WHEN ({field} IS NULL OR {field} = '') THEN %s ELSE {field} END")
                    params.append(value)

            if not updates:
                print(f"[WARNING] 업데이트할 값 없음: id={row_id}")
                cursor.close()
                return False

            params.append(row_id)
            query = f"UPDATE {self.table_name} SET {', '.join(updates)} WHERE id = %s"
            cursor.execute(query, params)
            self.db_conn.commit()
            cursor.close()
            return True

        except Exception as e:
            print(f"[ERROR] UPDATE 실패 (id={row_id}): {e}")
            self.db_conn.rollback()
            return False

    def run(self):
        """실행"""
        try:
            if not self.connect_db():
                return False
            if not self.load_xpaths(self.account_name, self.page_type, 'SEA', 'HHP'):
                return False
            if not self.setup_browser():
                return False
            if not self.initialize_session():
                return False

            self.cleanup_old_logs()

            items = self.load_target_items()
            if not items:
                print("[INFO] 재수집 대상이 없습니다.")
                return True

            print(f"\n{'='*60}")
            print(f" 필드 재수집 시작")
            print(f" - Fields: {', '.join(self.selected_fields)}")
            print(f" - Batch ID: {self.batch_id}")
            print(f" - Table: {self.table_name}")
            print(f" - Target: {len(items)}건")
            print(f"{'='*60}\n")

            success_count = 0
            fail_count = 0

            for i, item in enumerate(items, 1):
                try:
                    sku_name = item.get('retailer_sku_name') or 'N/A'
                    print(f"\n[{i}/{len(items)}] (id={item['id']}) {sku_name[:60]}")

                    data = self.crawl_detail(item)
                    if data and self.update_selected_fields(data):
                        print(f"[OK] UPDATE 성공 (id={item['id']})")
                        success_count += 1
                    else:
                        fail_count += 1

                    time.sleep(random.uniform(3, 5))

                except Exception as e:
                    error_msg = str(e).lower()
                    print(f"[ERROR] Product {i} failed: {e}")

                    if "timeout" in error_msg or "time out" in error_msg or "url unchanged" in error_msg:
                        print(f"[INFO] 브라우저 재시작 후 재시도")
                        self.restart_browser()
                        try:
                            data = self.crawl_detail(item)
                            if data and self.update_selected_fields(data):
                                success_count += 1
                                print(f"[SUCCESS] 재시도 성공")
                            else:
                                fail_count += 1
                        except Exception:
                            fail_count += 1
                    else:
                        fail_count += 1
                    continue

            print(f"\n{'='*60}")
            print(f" 재수집 완료")
            print(f" - 성공: {success_count}건")
            print(f" - 실패: {fail_count}건")
            print(f"{'='*60}\n")

            return True

        except Exception as e:
            print(f"[ERROR] 실행 실패: {e}")
            traceback.print_exc()
            return False

        finally:
            if self.page:
                try:
                    self.page.quit()
                except Exception:
                    pass
            if self.db_conn:
                try:
                    self.db_conn.close()
                except Exception:
                    pass


def main():
    """메인 함수"""
    import argparse
    parser = argparse.ArgumentParser(description='BestBuy HHP Field Update Crawler')
    args = parser.parse_args()

    print("\n" + "="*60)
    print(" BestBuy HHP 필드별 재수집 도구")
    print("="*60)

    # 모드 선택
    print("\n[모드 선택]")
    print("  1. 테스트 (test_hhp_retail_com)")
    print("  2. 운영 (hhp_retail_com)")
    mode_input = input("선택 (1/2) [기본: 1]: ").strip()
    if mode_input not in ['1', '2', '']:
        print("[ERROR] 잘못된 선택입니다.")
        return
    test_mode = mode_input != '2'
    table_name = 'test_hhp_retail_com' if test_mode else 'hhp_retail_com'

    # 필드 선택 (복수 가능)
    print(f"\n[필드 선택] (복수: 쉼표 구분, 예: 1,2)")
    for i, field in enumerate(SUPPORTED_FIELDS, 1):
        print(f"  {i}. {field}")
    field_input = input("선택: ").strip()

    try:
        selected_indices = [int(x.strip()) - 1 for x in field_input.split(',')]
        selected_fields = []
        for idx in selected_indices:
            if 0 <= idx < len(SUPPORTED_FIELDS):
                selected_fields.append(SUPPORTED_FIELDS[idx])
            else:
                print(f"[ERROR] 잘못된 번호: {idx + 1}")
                return
    except ValueError:
        print("[ERROR] 숫자를 입력해주세요.")
        return

    if not selected_fields:
        print("[ERROR] 필드를 선택해주세요.")
        return

    # batch_id 선택
    try:
        import psycopg2
        from config import DB_CONFIG
        conn = psycopg2.connect(**DB_CONFIG, database='postgres')
        cursor = conn.cursor()
        cursor.execute(f"""
            SELECT batch_id FROM (
                SELECT batch_id, MAX(crawl_strdatetime) as recent_time
                FROM {table_name}
                WHERE account_name = 'Bestbuy'
                GROUP BY batch_id
            ) AS recent_batches
            ORDER BY recent_time DESC
            LIMIT 10
        """)
        batch_ids = [row[0] for row in cursor.fetchall()]
        cursor.close()
        conn.close()
    except Exception as e:
        print(f"[DB ERROR] batch_id 로드 실패: {e}")
        batch_ids = []

    print(f"\n[batch_id 선택]")
    print(f"  0. 직접 입력")
    for i, bid in enumerate(batch_ids, 1):
        print(f"  {i}. {bid}")

    batch_input = input("선택: ").strip()
    try:
        batch_idx = int(batch_input)
        if batch_idx == 0:
            batch_id = input("batch_id 입력: ").strip()
        elif 1 <= batch_idx <= len(batch_ids):
            batch_id = batch_ids[batch_idx - 1]
        else:
            print("[ERROR] 잘못된 번호입니다.")
            return
    except ValueError:
        batch_id = batch_input

    if not batch_id:
        print("[ERROR] batch_id를 입력해주세요.")
        return

    # 범위 선택
    print("\n[범위 선택]")
    print("  1. NULL만 재수집")
    print("  2. 전체 재수집")
    scope_input = input("선택 (1/2) [기본: 1]: ").strip()
    null_only = scope_input != '2'

    # 확인
    print(f"\n[설정 확인]")
    print(f"  - 테이블: {table_name}")
    print(f"  - 필드: {', '.join(selected_fields)}")
    print(f"  - batch_id: {batch_id}")
    print(f"  - 범위: {'NULL만' if null_only else '전체'}")

    confirm = input("\n진행하시겠습니까? (y/n): ").strip().lower()
    if confirm != 'y':
        print("취소되었습니다.")
        return

    crawler = FieldUpdateCrawler(
        selected_fields=selected_fields,
        batch_id=batch_id,
        test_mode=test_mode,
        null_only=null_only
    )
    crawler.run()

    input("\nPress Enter to exit...")


if __name__ == '__main__':
    main()
