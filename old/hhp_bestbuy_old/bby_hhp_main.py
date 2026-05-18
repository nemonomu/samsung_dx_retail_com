"""
BestBuy Main 페이지 크롤러

================================================================================
실행 모드
================================================================================
- 개별 실행: test_mode=True (기본값)
- 통합 크롤러: test_mode 및 batch_id를 파라미터로 전달

================================================================================
주요 기능
================================================================================
- Main 페이지에서 제품 리스트 수집
- main_rank는 페이지 관계없이 1부터 순차 증가
- 테스트 모드: test_count 설정값만큼 수집
- 운영 모드: max_products 설정값만큼 수집

================================================================================
저장 테이블
================================================================================
- bby_hhp_product_list (제품 목록)
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


class BestBuyMainCrawler(BaseCrawler):
    """
    BestBuy Main 페이지 크롤러
    """

    def extract_item_from_url(self, product_url):
        """URL에서 item (SKU ID) 추출 - dt.py와 동일한 로직"""
        if not product_url:
            return None

        try:
            # /sku/숫자, /openbox, ?쿼리 제거
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

    def is_product_excluded(self, item):
        """item_mst에서 is_product=FALSE인지 확인"""
        if not item:
            return False

        try:
            cursor = self.db_conn.cursor()
            cursor.execute("""
                SELECT is_product FROM hhp_item_mst
                WHERE item = %s AND account_name = %s
            """, (item, self.account_name))
            row = cursor.fetchone()
            cursor.close()

            # 조회 결과 없으면 제외 안함 (신규 item)
            if row is None:
                return False
            # is_product가 False이면 제외
            return row[0] is False
        except Exception:
            return False

    def __init__(self, test_mode=True, batch_id=None, time_offset_hours=0):
        """초기화. test_mode: 테스트(True)/운영 모드(False), batch_id: 통합 크롤러에서 전달"""
        super().__init__()
        self.test_mode = test_mode
        self.time_offset_hours = time_offset_hours
        self.account_name = 'Bestbuy'
        self.page_type = 'main'
        self.batch_id = batch_id
        self.calendar_week = None
        self.url_template = None

        self.test_count = 1  # 테스트 모드
        self.max_products = 300  # 운영 모드
        self.max_pages = 20  # 최대 페이지 수
        self.current_rank = 0
        self.saved_urls = set()  # 중복 URL 추적용
        self.excluded_keywords = [
            'Screen Magnifier', 'mount', 'holder', 'cable', 'adapter', 'stand', 'wallet'
        ]  # 제외할 키워드 리스트 (retailer_sku_name에 포함 시 수집 제외)

        # DrissionPage 드라이버 (Selenium driver 대신 사용)
        self.page = None

        # 통계 변수
        self.stats = {
            'collected': 0,         # 수집 진행한 갯수
            'duplicates': 0,        # 중복 URL 제거 갯수
            'keyword_filtered': 0,  # 키워드 필터링 갯수
            'openbox_filtered': 0,  # Open Box 제외 갯수
            'non_product': 0,       # is_product=FALSE 제외 갯수
            'inserted': 0,          # INSERT 갯수
            'skipped_by_target': 0  # target 도달 후 미검사 갯수
        }

    def setup_drission_driver(self):
        """DrissionPage 브라우저 설정 (봇 감지 우회 강화)"""
        try:
            self.page = ChromiumPage()
            print("[SUCCESS] DrissionPage setup complete")
        except Exception as e:
            print(f"[ERROR] DrissionPage setup failed: {e}")
            traceback.print_exc()
            raise

    def initialize(self):
        """초기화: DB 연결 → XPath 로드 → URL 템플릿 로드 → DrissionPage 설정 → batch_id 생성 → 1개월 전 로그 정리"""
        # 1. DB 연결
        if not self.connect_db():
            print("[ERROR] Initialize failed: DB connection failed")
            return False

        # 2. XPath 로드
        if not self.load_xpaths(self.account_name, self.page_type, 'SEA', 'HHP'):
            print(f"[ERROR] Initialize failed: XPath load failed (account={self.account_name}, page_type={self.page_type})")
            return False

        # 3. URL 템플릿 로드
        self.url_template = self.load_page_urls(self.account_name, self.page_type, 'SEA', 'HHP')
        if not self.url_template:
            print(f"[ERROR] Initialize failed: URL template load failed (account={self.account_name}, page_type={self.page_type})")
            return False

        # 4. DrissionPage 설정 (Selenium 대신)
        try:
            self.setup_drission_driver()
        except Exception as e:
            print(f"[ERROR] Initialize failed: DrissionPage setup failed - {e}")
            traceback.print_exc()
            return False

        # 5. batch_id 생성 (개별 실행 시 test_mode=True)
        if not self.batch_id:
            self.batch_id = self.generate_batch_id(self.account_name, test_mode=True, time_offset_hours=self.time_offset_hours)

        # 6. calendar_week 생성 및 로그 정리
        self.calendar_week = self.generate_calendar_week(time_offset_hours=self.time_offset_hours)
        self.cleanup_old_logs()

        print(f"[INFO] Initialize completed: batch_id={self.batch_id}, calendar_week={self.calendar_week}")
        return True

    def scroll_to_bottom(self):
        """스크롤: 300px씩 점진적 스크롤 → 페이지네이션 보이면 종료 (DrissionPage용)"""
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

                scroll_step = random.randint(280, 320)
                current_position += scroll_step
                self.page.run_js(f"window.scrollTo(0, {current_position});")
                time.sleep(random.uniform(0.8, 1.2))

                total_height = self.page.run_js("return document.body.scrollHeight")
                if current_position >= total_height:
                    break

            time.sleep(random.uniform(0, 4))

        except Exception as e:
            print(f"[ERROR] Scroll failed: {e}")
            traceback.print_exc()

    def crawl_page(self, page_number):
        """페이지 크롤링: 페이지 로드 → 제품 파싱 → URL 누락 시 1스텝 스크롤 로딩 → 반복 (스마트 스크롤)"""
        try:
            url = self.url_template.replace('{page}', str(page_number))
            base_container_xpath = self.xpaths.get('base_container', {}).get('xpath')
            if not base_container_xpath:
                print("[ERROR] base_container XPath not found")
                return []

            self.page.get(url)
            time.sleep(random.uniform(3, 5))

            # 1. 0개인 경우 로드 실패 예외처리 (최대 3회 새로고침)
            for refresh_attempt in range(1, 4):
                page_html = self.page.html
                tree = html.fromstring(page_html)
                if len(tree.xpath(base_container_xpath)) == 0:
                    print(f"[WARNING] Page {page_number}: 0 products found, refresh attempt {refresh_attempt}/3")
                    if refresh_attempt < 3:
                        self.page.refresh()
                        time.sleep(random.uniform(5, 8))
                    continue
                break

            # 리프레쉬 3회 후에도 0개이면 빈 리스트 반환
            if len(tree.xpath(base_container_xpath)) == 0:
                print(f"[ERROR] Page {page_number}: No products found after 3 refresh attempts")
                return []

            current_position = 0
            products = []
            max_scroll_attempts = 30
            bottom_wait_count = 0

            # 2. 파싱 및 스크롤 루프 (url 못찾은거 있으면 스크롤 1회 > 약 5초 대기 > 재파싱 반복)
            for scroll_attempt in range(1, max_scroll_attempts + 1):
                page_html = self.page.html
                tree = html.fromstring(page_html)
                base_containers = tree.xpath(base_container_xpath)

                products.clear()
                for idx, item in enumerate(base_containers, 1):
                    try:
                        product_url_raw = self.safe_extract(item, 'product_url')
                        if not product_url_raw or product_url_raw == '#':
                            product_url = None
                        elif product_url_raw.startswith('/'):
                            product_url = f"https://www.bestbuy.com{product_url_raw}"
                        else:
                            product_url = product_url_raw

                        savings_raw = self.safe_extract(item, 'savings')
                        savings = savings_raw.replace('Save ', '') if savings_raw else None

                        offer_raw = self.safe_extract(item, 'offer')
                        offer = None
                        if offer_raw:
                            match = re.search(r'\d+', offer_raw)
                            offer = match.group() if match else offer_raw

                        products.append({
                            'account_name': self.account_name,
                            'page_type': self.page_type,
                            'retailer_sku_name': self.safe_extract(item, 'retailer_sku_name'),
                            'final_sku_price': self.safe_extract_join(item, 'final_sku_price', separator=''),
                            'savings': savings,
                            'comparable_pricing': self.safe_extract(item, 'comparable_pricing'),
                            'offer': offer,
                            'pick_up_availability': self.safe_extract(item, 'pick_up_availability'),
                            'fastest_delivery': self.safe_extract(item, 'fastest_delivery'),
                            'delivery_availability': self.safe_extract(item, 'delivery_availability'),
                            'sku_status': self.safe_extract(item, 'sku_status'),
                            'promotion_type': self.safe_extract(item, 'promotion_type'),
                            'main_rank': 0,
                            'page_number': page_number,
                            'product_url': product_url,
                            'calendar_week': self.calendar_week,
                            'crawl_strdatetime': (datetime.now() + timedelta(hours=self.time_offset_hours)).strftime('%Y-%m-%d %H:%M:%S'),
                            'batch_id': self.batch_id
                        })
                    except Exception as e:
                        continue

                null_url_count = sum(1 for p in products if not p.get('product_url'))
                total_found = len(products)

                # 조건 1: 모두 찾았으면 바텀까지 가볍게 스크롤 후 반복문 종료
                if total_found > 0 and null_url_count == 0:
                    print(f"[INFO] Page {page_number}: All {total_found} URLs loaded successfully! Quick scrolling to bottom...")
                    for _ in range(20):
                        is_bottom = self.page.run_js("""
                            var elem = document.querySelector("div.pagination-container");
                            if (!elem) return false;
                            var rect = elem.getBoundingClientRect();
                            return (rect.top >= 0 && rect.top <= window.innerHeight);
                        """)

                        total_height_check = self.page.run_js("return document.body.scrollHeight")
                        if is_bottom or current_position >= total_height_check:
                            break

                        scroll_step = random.randint(500, 800)
                        current_position += scroll_step
                        self.page.run_js(f"window.scrollTo(0, {current_position});")
                        time.sleep(random.uniform(1.0, 2.0))

                    break

                print(f"[INFO] Page {page_number}: Parsed {total_found} products, {null_url_count} URLs missing. Scrolling... ({scroll_attempt}/{max_scroll_attempts})")

                # 하단 도달 체크
                is_pagination_visible = self.page.run_js("""
                    var elem = document.querySelector("div.pagination-container");
                    if (!elem) return false;
                    var rect = elem.getBoundingClientRect();
                    return (rect.top >= 0 && rect.top <= window.innerHeight);
                """)

                total_height = self.page.run_js("return document.body.scrollHeight")

                if is_pagination_visible or current_position >= total_height:
                    bottom_wait_count += 1
                    if bottom_wait_count >= 3:
                        print(f"[INFO] Page {page_number}: Reached bottom {bottom_wait_count} times. Giving up scroll.")
                        break
                    self.page.run_js("window.scrollTo(0, 0);")
                    current_position = 0
                    time.sleep(random.uniform(2, 3))

                # 스크롤 1회 내리고 대기
                scroll_step = random.randint(400, 600)
                current_position += scroll_step
                self.page.run_js(f"window.scrollTo(0, {current_position});")
                time.sleep(random.uniform(4, 6))

            print(f"[INFO] Page {page_number}: {len(products)} products")
            return products

        except Exception as e:
            print(f"[ERROR] Page {page_number} failed: {e}")
            traceback.print_exc()
            return []

    def save_products(self, products):
        """DB 저장: BATCH_SIZE 배치 → RETRY_SIZE 배치 → 1개씩 (3-tier retry)"""
        if not products:
            return 0

        # 수집 갯수 통계
        self.stats['collected'] += len(products)

        # 키워드 필터링, 중복 제거, is_product 체크 및 rank 할당
        unique_products = []
        for idx, product in enumerate(products):

            # 제외 키워드 필터링 (먼저 수행)
            retailer_sku_name = product.get('retailer_sku_name') or ''
            sku_name_lower = retailer_sku_name.lower()
            matched_keyword = None
            if self.excluded_keywords:
                for keyword in self.excluded_keywords:
                    pattern = rf"(?:^|[^a-z0-9]){re.escape(keyword.lower())}(?:[^a-z0-9]|$)"
                    if re.search(pattern, sku_name_lower):
                        matched_keyword = keyword
                        break
                        
            if matched_keyword:
                print(f"[SKIP] 제외 키워드 '{matched_keyword}' 포함: {retailer_sku_name[:40]}...")
                self.stats['keyword_filtered'] += 1
                continue

            # URL에서 item 추출 (dt.py와 동일한 로직)
            product_url = product.get('product_url')
            item = self.extract_item_from_url(product_url)

            # openbox URL 제외
            if product_url and 'openbox' in product_url.lower():
                print(f"[SKIP] Open Box 상품 제외: {product_url}")
                self.stats['openbox_filtered'] += 1
                continue

            # is_product=FALSE 체크 (비제품 제외)
            if self.is_product_excluded(item):
                print(f"[SKIP] 비제품(is_product=FALSE): {retailer_sku_name[:40] if retailer_sku_name else 'N/A'}...")
                self.stats['non_product'] += 1
                continue

            # 중복 item 필터링
            if item and item in self.saved_urls:
                print(f"[SKIP] 중복 item: {retailer_sku_name[:40] if retailer_sku_name else 'N/A'}...")
                self.stats['duplicates'] += 1
                continue

            if item:
                self.saved_urls.add(item)

            # rank 할당 (중복 제거된 제품에만 순차적으로)
            self.current_rank += 1
            target = self.test_count if self.test_mode else self.max_products
            if self.current_rank > target:
                self.stats['skipped_by_target'] += len(products) - idx
                break
            product['main_rank'] = self.current_rank
            unique_products.append(product)

        if not unique_products:
            return 0

        try:
            cursor = self.db_conn.cursor()
            insert_query = """
                INSERT INTO bby_hhp_product_list (
                    account_name, page_type, retailer_sku_name,
                    final_sku_price, savings, comparable_pricing,
                    offer, pick_up_availability, fastest_delivery, delivery_availability,
                    sku_status, promotion_type, main_rank, main_page_number, product_url,
                    calendar_week, crawl_strdatetime, batch_id
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                )
            """

            BATCH_SIZE = 20
            RETRY_SIZE = 5
            total_saved = 0

            def product_to_tuple(product):
                return (
                    product['account_name'],
                    product['page_type'],
                    product['retailer_sku_name'],
                    product['final_sku_price'],
                    product['savings'],
                    product['comparable_pricing'],
                    product['offer'],
                    product['pick_up_availability'],
                    product['fastest_delivery'],
                    product['delivery_availability'],
                    product['sku_status'],
                    product['promotion_type'],
                    product['main_rank'],
                    product['page_number'],
                    product['product_url'],
                    product['calendar_week'],
                    product['crawl_strdatetime'],
                    product['batch_id']
                )

            def save_batch(batch_products):
                values_list = [product_to_tuple(p) for p in batch_products]
                cursor.executemany(insert_query, values_list)
                self.db_conn.commit()
                return len(batch_products)

            for batch_start in range(0, len(unique_products), BATCH_SIZE):
                batch_end = min(batch_start + BATCH_SIZE, len(unique_products))
                batch_products = unique_products[batch_start:batch_end]

                try:
                    total_saved += save_batch(batch_products)

                except Exception:
                    self.db_conn.rollback()

                    for sub_start in range(0, len(batch_products), RETRY_SIZE):
                        sub_end = min(sub_start + RETRY_SIZE, len(batch_products))
                        sub_batch = batch_products[sub_start:sub_end]

                        try:
                            total_saved += save_batch(sub_batch)

                        except Exception:
                            self.db_conn.rollback()

                            for single_product in sub_batch:
                                try:
                                    cursor.execute(insert_query, product_to_tuple(single_product))
                                    self.db_conn.commit()
                                    total_saved += 1
                                except Exception as single_error:
                                    print(f"[ERROR] DB save failed: {(single_product.get('retailer_sku_name') or 'N/A')[:30]}: {single_error}")
                                    query = cursor.mogrify(insert_query, product_to_tuple(single_product))
                                    print(f"[DEBUG] Query:\n{query.decode('utf-8')}")
                                    traceback.print_exc()
                                    self.db_conn.rollback()

            cursor.close()
            self.stats['inserted'] += total_saved
            return total_saved

        except Exception as e:
            print(f"[ERROR] Failed to save products: {e}")
            traceback.print_exc()
            return 0

    def run(self):
        """실행: initialize() → 페이지별 crawl_page() → save_products() → 리소스 정리"""
        try:
            if not self.initialize():
                print("[ERROR] Initialization failed")
                return False

            total_products = 0
            target_products = self.test_count if self.test_mode else self.max_products
            self.current_rank = 0
            page_num = 1

            while total_products < target_products and page_num <= self.max_pages:
                products = self.crawl_page(page_num)

                if not products:
                    if page_num > 1:
                        break
                    print(f"[ERROR] No products found at page {page_num}")
                else:
                    saved_count = self.save_products(products)
                    total_products += saved_count

                    if total_products >= target_products:
                        break

                time.sleep(random.uniform(5, 8))
                page_num += 1

            if page_num > self.max_pages:
                print(f"[INFO] Max pages ({self.max_pages}) reached")

            print(f"[DONE] Page: {page_num}, Saved: {total_products}, batch_id: {self.batch_id}")
            return True

        except Exception as e:
            print(f"[ERROR] Crawler failed: {e}")
            traceback.print_exc()
            return False

        finally:
            # 통계 출력
            print(f"\n{'='*50}")
            print(f"[통계] 수집: {self.stats['collected']}, 중복제거: {self.stats['duplicates']}, 키워드필터: {self.stats['keyword_filtered']}, OpenBox: {self.stats['openbox_filtered']}, 비제품: {self.stats['non_product']}, INSERT: {self.stats['inserted']}")
            if self.stats.get('skipped_by_target', 0) > 0:
                print(f"  미검사: {self.stats['skipped_by_target']} (target 도달 후 나머지)")
            print(f"{'='*50}")

            if self.page:
                self.page.quit()
            if self.db_conn:
                self.db_conn.close()


def main():
    """개별 실행 진입점 (테스트 모드)"""
    import argparse

    parser = argparse.ArgumentParser(description='BestBuy HHP Main Crawler')
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

    crawler = BestBuyMainCrawler(test_mode=True, time_offset_hours=time_offset)
    crawler.run()
    input("\n[완료] 엔터를 누르면 종료됩니다...")


if __name__ == '__main__':
    main()