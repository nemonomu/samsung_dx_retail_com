"""
BestBuy TV BSR 페이지 크롤러

================================================================================
실행 모드
================================================================================
- 개별 실행: test_mode=True (기본값)
- 통합 크롤러: test_mode 및 batch_id를 파라미터로 전달

================================================================================
주요 기능
================================================================================
- BSR 페이지에서 제품 리스트 수집 (bsr_rank 포함)
- 테스트 모드: test_count 설정값만큼 수집
- 운영 모드: max_products 설정값만큼 수집

================================================================================
저장 테이블
================================================================================
- bby_tv_product_list (제품 목록)
"""

import sys
import os
import time
import traceback
import random
import re
from datetime import datetime, timedelta
from lxml import html
from DrissionPage import ChromiumOptions, ChromiumPage

# 공통 환경 설정 (작업 디렉토리, 한글 출력, 경로 설정)
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
from common.setup import setup_environment
setup_environment(__file__)

from common.base_crawler import BaseCrawler



class BestBuyTVBSRCrawler(BaseCrawler):
    """
    BestBuy TV BSR 페이지 크롤러
    """

    def extract_item_from_url(self, product_url):
        """URL에서 item (SKU ID) 추출 - dt.py와 동일한 로직"""
        if not product_url:
            return None
        try:
            # /sku/숫자 또는 /openbox 제거
            cleaned_url = re.sub(r'/sku/\d+(/openbox\?.*)?', '', product_url)
            # 쿼리스트링 제거
            cleaned_url = cleaned_url.split('?')[0]
            # 마지막 path segment 추출
            parts = cleaned_url.split('/')
            if not parts:
                return None
            item = parts[-1]
            # .p 확장자 제거
            if item.endswith('.p'):
                item = item[:-2]
            return item if item else None
        except Exception:
            return None

    def is_product_excluded(self, item):
        """tv_item_mst에서 is_product=FALSE인지 확인"""
        if not item:
            return False
        try:
            cursor = self.db_conn.cursor()
            cursor.execute("""
                SELECT is_product FROM tv_item_mst
                WHERE item = %s AND account_name = %s
            """, (item, self.account_name))
            row = cursor.fetchone()
            cursor.close()
            if row is None:
                return False
            return row[0] is False
        except Exception:
            return False

    def __init__(self, test_mode=True, batch_id=None, time_offset_hours=0):
        """초기화. test_mode: 테스트(True)/운영 모드(False), batch_id: 통합 크롤러에서 전달"""
        super().__init__()
        self.test_mode = test_mode
        self.account_name = 'Bestbuy'
        self.page_type = 'bsr'
        self.batch_id = batch_id
        self.time_offset_hours = time_offset_hours
        self.calendar_week = None
        self.url_template = None
        self.current_rank = 0
        self.db_url_map = {}       # {정규화URL: 원본URL} - Main에서 저장된 URL
        self.crawled_urls = set()  # BSR에서 수집한 정규화 URL (페이지 간 중복 방지)

        self.test_count = 1  # 테스트 모드
        self.max_products = 100  # 운영 모드
        self.max_pages = 20  # 최대 페이지 수

        # 통계 변수
        self.stats = {
            'collected': 0,         # 수집 진행한 갯수
            'duplicates': 0,        # 중복 URL 제거 갯수
            'openbox_filtered': 0,  # Open Box 제외 갯수
            'non_product': 0,       # is_product=FALSE 제외 갯수
            'updated': 0,           # UPDATE 갯수
            'inserted': 0,          # INSERT 갯수
            'skipped_by_target': 0  # target 도달 후 미검사 갯수
        }

        # DrissionPage 드라이버 (Selenium driver 대신 사용)
        self.page = None

    def setup_drission_driver(self):
        """DrissionPage 브라우저 설정 (봇 감지 우회 강화)"""
        try:
            opts = ChromiumOptions()
            opts.set_argument("--disable-blink-features=AutomationControlled")
            opts.set_argument("--disable-features=IsolateOrigins,site-per-process")
            opts.set_argument("--no-first-run")
            opts.set_argument("--no-default-browser-check")
            opts.set_argument("--disable-dev-shm-usage")
            opts.set_argument("--lang=en-US,en;q=0.9")
            opts.set_argument("--window-size=1366,768")
            opts.set_argument("--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36")
            self.page = ChromiumPage(opts)
            self.page.set.headers({
                "Accept-Language": "en-US,en;q=0.9",
                "Upgrade-Insecure-Requests": "1",
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
            })
            print("[SUCCESS] DrissionPage setup complete")
        except Exception as e:
            print(f"[ERROR] DrissionPage setup failed: {e}")
            traceback.print_exc()
            raise

    def initialize(self):
        """초기화: DB 연결 → XPath 로드 → URL 템플릿 로드 → DrissionPage 설정 → batch_id 생성 → 로그 정리"""
        if not self.connect_db():
            return False

        # XPath 로드 (DB 조회)
        if not self.load_xpaths(self.account_name, self.page_type, 'SEA', 'TV'):
            return False

        # URL 템플릿 로드 (DB 조회)
        self.url_template = self.load_page_urls(self.account_name, self.page_type, 'SEA', 'TV')

        if not self.url_template:
            return False

        # DrissionPage 설정 (Selenium 대신)
        try:
            self.setup_drission_driver()
        except Exception as e:
            print(f"[ERROR] Initialize failed: DrissionPage setup failed - {e}")
            traceback.print_exc()
            return False

        # batch_id 생성 (개별 실행 시 test_mode=True)
        if not self.batch_id:
            self.batch_id = self.generate_batch_id(self.account_name, test_mode=True, time_offset_hours=self.time_offset_hours)

        self.calendar_week = self.generate_calendar_week(time_offset_hours=self.time_offset_hours)
        self.cleanup_old_logs()

        # DB에서 기존 URL 캐시 로드 (Main에서 저장된 URL → 정규화 매핑)
        self.db_url_map = self.build_db_url_cache()

        return True

    def build_db_url_cache(self):
        """DB에서 현재 batch_id의 URL을 조회하여 {item: 원본URL} dict로 반환"""
        try:
            cursor = self.db_conn.cursor()
            query = """
                SELECT product_url FROM bby_tv_product_list
                WHERE account_name = %s AND batch_id = %s
            """
            cursor.execute(query, (self.account_name, self.batch_id))
            rows = cursor.fetchall()
            cursor.close()

            db_url_map = {}
            for (db_url,) in rows:
                if db_url:
                    item = self.extract_item_from_url(db_url)
                    if item and item not in db_url_map:
                        db_url_map[item] = db_url

            print(f"[INFO] DB URL cache loaded: {len(db_url_map)} items")
            return db_url_map

        except Exception as e:
            print(f"[WARNING] build_db_url_cache failed: {e}")
            return {}

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

                # 스크롤을 한 줄(제품 블록) 정도 내리고 충분히 대기하여 로딩 유도
                scroll_step = random.randint(400, 600)
                current_position += scroll_step
                self.page.run_js(f"window.scrollTo(0, {current_position});")
                time.sleep(random.uniform(1.5, 2.5))

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

                        offer_raw = self.safe_extract(item, 'offer')
                        offer = None
                        if offer_raw:
                            match = re.search(r'\d+', offer_raw)
                            offer = match.group() if match else offer_raw

                        delivery_raw = self.safe_extract(item, 'delivery_availability')
                        delivery = delivery_raw if delivery_raw and 'delivery' in delivery_raw.lower() else None

                        sku_status_raw = self.safe_extract(item, 'sku_status')
                        sku_status = 'Sponsored' if sku_status_raw and 'sponsored' in sku_status_raw.lower() else None

                        products.append({
                            'account_name': self.account_name,
                            'page_type': self.page_type,
                            'retailer_sku_name': self.safe_extract(item, 'retailer_sku_name'),
                            'offer': offer,
                            'pick_up_availability': self.safe_extract(item, 'pick_up_availability'),
                            'fastest_delivery': self.safe_extract(item, 'fastest_delivery'),
                            'delivery_availability': delivery,
                            'sku_status': sku_status,
                            'bsr_rank': 0,
                            'page_number': page_number,
                            'product_url': product_url,
                            'calendar_week': self.calendar_week,
                            'crawl_datetime': (datetime.now() + timedelta(hours=self.time_offset_hours)).strftime('%Y-%m-%d %H:%M:%S'),
                            'batch_id': self.batch_id
                        })
                    except Exception as e:
                        continue

                null_url_count = sum(1 for p in products if not p.get('product_url'))
                total_found = len(products)

                # 조건 1: 모두 찾았으면 바텀까지 가볍게 스크롤 후 반복문 종료 (자연스러운 봇 동작)
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
                            
                        # 남은 공간 빠르게 1~2초 간격으로 스크롤
                        scroll_step = random.randint(500, 800)
                        current_position += scroll_step
                        self.page.run_js(f"window.scrollTo(0, {current_position});")
                        time.sleep(random.uniform(1.0, 2.0))
                        
                    break

                print(f"[INFO] Page {page_number}: Parsed {total_found} products, {null_url_count} URLs missing. Scrolling... ({scroll_attempt}/{max_scroll_attempts})")

                # 하단 도달(페이지네이션 보임) 체크
                is_pagination_visible = self.page.run_js("""
                    var elem = document.querySelector("div.pagination-container");
                    if (!elem) return false;
                    var rect = elem.getBoundingClientRect();
                    return (rect.top >= 0 && rect.top <= window.innerHeight);
                """)
                
                total_height = self.page.run_js("return document.body.scrollHeight")
                
                if is_pagination_visible or current_position >= total_height:
                    # 바텀에 도달했는데 null_url이 있다면 맨 위로 올라가서 다시 스크롤을 내리며 훑어보기
                    bottom_wait_count += 1
                    if bottom_wait_count >= 3:
                        print(f"[INFO] Page {page_number}: Reached bottom {bottom_wait_count} times. Giving up scroll.")
                        break
                        
                    print(f"[WARNING] Page {page_number}: Reached bottom but {null_url_count} URLs missing. Scrolling back to TOP... ({bottom_wait_count}/3)")
                    current_position = 0
                    self.page.run_js("window.scrollTo(0, 0);")
                    time.sleep(random.uniform(3, 5))
                    continue

                # 스크롤 1회 내리고 5초 대기 (봇 탐지를 피하기 위해 4~6초 랜덤)
                scroll_step = random.randint(400, 600)
                current_position += scroll_step
                self.page.run_js(f"window.scrollTo(0, {current_position});")
                time.sleep(random.uniform(4, 6))

            print(f"[INFO] Page {page_number}: Final parsed products: {len(products)}")
            return products

        except Exception as e:
            print(f"[ERROR] Page {page_number} failed: {e}")
            traceback.print_exc()
            return []

    def save_products(self, products):
        """DB 저장: bsr_rank 할당 → UPDATE 즉시 실행 / INSERT 배치 처리"""
        if not products:
            return {'insert': 0, 'update': 0}

        # 수집 갯수 통계
        self.stats['collected'] += len(products)

        try:
            cursor = self.db_conn.cursor()
            insert_count = 0
            update_count = 0
            products_to_insert = []

            update_query = """
                UPDATE bby_tv_product_list
                SET bsr_rank = %s, bsr_page_number = %s
                WHERE account_name = %s AND batch_id = %s AND product_url = %s
            """

            for idx, product in enumerate(products):
                retailer_sku_name = product.get('retailer_sku_name') or ''

                # URL에서 item 추출 (dt.py와 동일한 로직)
                product_url = product.get('product_url')
                item = self.extract_item_from_url(product_url)

                # openbox URL 제외
                if product_url and 'openbox' in product_url.lower():
                    print(f"[SKIP] Open Box 상품 제외: {product_url}")
                    self.stats['openbox_filtered'] += 1
                    continue

                # 1. is_product=FALSE 체크 (비제품 제외)
                if self.is_product_excluded(item):
                    print(f"[SKIP] 비제품(is_product=FALSE): {retailer_sku_name[:40] if retailer_sku_name else 'N/A'}...")
                    self.stats['non_product'] += 1
                    continue

                # 2. 페이지 간 중복 체크 (이미 수집한 item → 스킵)
                if item in self.crawled_urls:
                    self.stats['duplicates'] += 1
                    continue
                self.crawled_urls.add(item)

                # bsr_rank 할당
                self.current_rank += 1
                target = self.test_count if self.test_mode else self.max_products
                if self.current_rank > target:
                    self.stats['skipped_by_target'] += len(products) - idx
                    break
                product['bsr_rank'] = self.current_rank

                # 3. DB 캐시에서 기존 item 체크 → UPDATE / INSERT 분류
                matched_url = self.db_url_map.get(item)
                if matched_url:
                    try:
                        cursor.execute(update_query, (
                            product['bsr_rank'],
                            product['page_number'],
                            self.account_name,
                            product['batch_id'],
                            matched_url  # DB에 저장된 원본 URL 사용
                        ))
                        self.db_conn.commit()
                        update_count += 1
                    except Exception as e:
                        print(f"[ERROR] UPDATE failed: {product_url[:50] if product_url else 'N/A'}: {e}")
                        self.db_conn.rollback()
                else:
                    products_to_insert.append(product)

            if not products_to_insert and update_count == 0:
                print("[INFO] No products to save")
                cursor.close()
                return {'insert': 0, 'update': 0}

            # INSERT 처리 (3-tier retry: BATCH_SIZE → RETRY_SIZE → 1개씩)
            if products_to_insert:
                insert_query = """
                    INSERT INTO bby_tv_product_list (
                        account_name, page_type, retailer_sku_name,
                        offer, pick_up_availability, fastest_delivery, delivery_availability,
                        sku_status, bsr_rank, bsr_page_number, product_url,
                        calendar_week, crawl_datetime, batch_id
                    ) VALUES (
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                    )
                """

                BATCH_SIZE = 20
                RETRY_SIZE = 5

                def product_to_tuple(product):
                    return (
                        product['account_name'],
                        product['page_type'],
                        product['retailer_sku_name'],
                        product['offer'],
                        product['pick_up_availability'],
                        product['fastest_delivery'],
                        product['delivery_availability'],
                        product['sku_status'],
                        product['bsr_rank'],
                        product['page_number'],
                        product['product_url'],
                        product['calendar_week'],
                        product['crawl_datetime'],
                        product['batch_id']
                    )

                def save_batch(batch_products):
                    values_list = [product_to_tuple(p) for p in batch_products]
                    cursor.executemany(insert_query, values_list)
                    self.db_conn.commit()
                    return len(batch_products)

                for batch_start in range(0, len(products_to_insert), BATCH_SIZE):
                    batch_end = min(batch_start + BATCH_SIZE, len(products_to_insert))
                    batch_products = products_to_insert[batch_start:batch_end]

                    try:
                        insert_count += save_batch(batch_products)

                    except Exception:
                        self.db_conn.rollback()

                        for sub_start in range(0, len(batch_products), RETRY_SIZE):
                            sub_end = min(sub_start + RETRY_SIZE, len(batch_products))
                            sub_batch = batch_products[sub_start:sub_end]

                            try:
                                insert_count += save_batch(sub_batch)

                            except Exception:
                                self.db_conn.rollback()

                                for single_product in sub_batch:
                                    try:
                                        cursor.execute(insert_query, product_to_tuple(single_product))
                                        self.db_conn.commit()
                                        insert_count += 1
                                    except Exception as single_error:
                                        print(f"[ERROR] DB save failed: {(single_product.get('retailer_sku_name') or 'N/A')[:30]}: {single_error}")
                                        query = cursor.mogrify(insert_query, product_to_tuple(single_product))
                                        print(f"[DEBUG] Query:\n{query.decode('utf-8')}")
                                        traceback.print_exc()
                                        self.db_conn.rollback()

            cursor.close()
            self.stats['updated'] += update_count
            self.stats['inserted'] += insert_count
            return {'insert': insert_count, 'update': update_count}

        except Exception as e:
            print(f"[ERROR] Failed to save products: {e}")
            traceback.print_exc()
            return {'insert': 0, 'update': 0}

    def run(self):
        """실행: initialize() → 페이지별 crawl_page() → save_products() → 리소스 정리"""
        try:
            if not self.initialize():
                print("[ERROR] Initialization failed")
                return False

            total_insert = 0
            total_update = 0
            target_products = self.test_count if self.test_mode else self.max_products
            self.current_rank = 0
            page_num = 1

            total_saved = 0  # 실제 저장된 개수 (필터링 후)
            while total_saved < target_products and page_num <= self.max_pages:
                products = self.crawl_page(page_num)

                if not products:
                    if page_num > 1:
                        break
                    print(f"[ERROR] No products found at page {page_num}")
                else:
                    # product_url NULL 3개 이상이면 에러 페이지로 판단
                    null_url_count = sum(1 for p in products if not p.get('product_url'))
                    if null_url_count >= 3:
                        print(f"[WARNING] Page {page_num}: product_url NULL {null_url_count}/{len(products)} — 에러 페이지로 판단")
                        print(f"[INFO] 브라우저 종료 후 20분 대기")
                        if self.page:
                            try:
                                self.page.quit()
                                self.page = None
                            except Exception:
                                pass
                        for remaining in range(20, 0, -1):
                            print(f"[WAIT] {remaining}분 남음...")
                            time.sleep(60)
                        print(f"[INFO] 대기 완료 — 브라우저 재시작 후 Page {page_num} 재시도")
                        self.setup_drission_driver()
                        continue  # 같은 page_num으로 재시도

                    result = self.save_products(products)
                    total_insert += result['insert']
                    total_update += result['update']
                    total_saved = self.current_rank  # bsr_rank 기준 (필터링 후 실제 저장된 개수)

                    if total_saved >= target_products:
                        break

                time.sleep(random.uniform(5, 8))
                page_num += 1

            if page_num > self.max_pages:
                print(f"[INFO] Max pages ({self.max_pages}) reached, total saved: {total_saved}")

            print(f"[DONE] Page: {page_num}, Update: {total_update}, Insert: {total_insert}, batch_id: {self.batch_id}")
            return True

        except Exception as e:
            print(f"[ERROR] Crawler failed: {e}")
            traceback.print_exc()
            return False

        finally:
            # 통계 출력
            print(f"\n{'='*50}")
            print(f"[통계] 수집: {self.stats['collected']}, 중복제거: {self.stats['duplicates']}, OpenBox: {self.stats['openbox_filtered']}, 비제품: {self.stats['non_product']}, UPDATE: {self.stats['updated']}, INSERT: {self.stats['inserted']}")
            if self.stats.get('skipped_by_target', 0) > 0:
                print(f"  미검사: {self.stats['skipped_by_target']} (target 도달 후 나머지)")
            print(f"{'='*50}")

            if self.page:
                self.page.quit()
            if self.db_conn:
                self.db_conn.close()


import argparse

def main():
    """개별 실행 진입점 (테스트 모드)"""
    parser = argparse.ArgumentParser(description='BestBuy TV BSR Crawler')
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

    crawler = BestBuyTVBSRCrawler(test_mode=True, time_offset_hours=time_offset)
    crawler.run()
    input("\n[완료] 엔터를 누르면 종료됩니다...")


if __name__ == '__main__':
    main()
