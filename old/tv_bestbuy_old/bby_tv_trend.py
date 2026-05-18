"""
BestBuy TV Trend 페이지 크롤러

================================================================================
실행 모드
================================================================================
- 개별 실행: test_mode=True (기본값)
- 통합 크롤러: test_mode 및 batch_id를 파라미터로 전달

================================================================================
주요 기능
================================================================================
- Trend 페이지에서 제품 리스트 수집 (trend_rank 포함)
- 테스트 모드: test_count 설정값만큼 수집
- 운영 모드: 단일 페이지 전체 크롤링

================================================================================
저장 테이블
================================================================================
- bby_tv_product_list (제품 목록)
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



class BestBuyTVTrendCrawler(BaseCrawler):
    """
    BestBuy TV Trend 페이지 크롤러
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
        self.page_type = 'trend'
        self.batch_id = batch_id
        self.time_offset_hours = time_offset_hours
        self.calendar_week = None
        self.url_template = None
        self.current_rank = 0

        self.test_count = 1  # 테스트 모드

        self.db_url_map = {}       # {정규화URL: 원본URL} - Main에서 저장된 URL
        self.crawled_urls = set()  # Trend에서 수집한 정규화 URL (중복 방지)

        # 통계 변수
        self.stats = {
            'collected': 0,         # 수집 진행한 갯수
            'duplicates': 0,        # 중복 URL 제거 갯수
            'openbox_filtered': 0,  # Open Box 제외 갯수
            'non_product': 0,       # is_product=FALSE 제외 갯수
            'updated': 0,           # UPDATE 갯수
            'inserted': 0           # INSERT 갯수
        }

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

    def crawl_page(self):
        """페이지 크롤링: 페이지 로드 → HTML 파싱(최대 3회) → 제품 데이터 추출 (DrissionPage 사용)"""
        try:
            url = self.url_template

            base_container_xpath = self.xpaths.get('base_container', {}).get('xpath')
            if not base_container_xpath:
                print("[ERROR] base_container XPath not found")
                return []

            self.page.get(url)
            time.sleep(random.uniform(25, 35))

            base_containers = []
            expected_products = 10

            for attempt in range(1, 4):
                page_html = self.page.html
                tree = html.fromstring(page_html)
                base_containers = tree.xpath(base_container_xpath)

                print(f"[INFO] Attempt {attempt}: Found {len(base_containers)} items")
                if len(base_containers) >= expected_products:
                    break

                if attempt < 3:
                    time.sleep(random.uniform(8, 12))

            target_products = self.test_count if self.test_mode else len(base_containers)
            containers_to_process = base_containers[:target_products]

            products = []
            for idx, item in enumerate(containers_to_process, 1):
                try:
                    retailer_sku_name = self.safe_extract(item, 'retailer_sku_name') or ''

                    self.current_rank += 1

                    product_url_raw = self.safe_extract(item, 'product_url')
                    product_url = f"https://www.bestbuy.com{product_url_raw}" if product_url_raw and product_url_raw.startswith('/') else product_url_raw

                    product_data = {
                        'account_name': self.account_name,
                        'page_type': self.page_type,
                        'retailer_sku_name': retailer_sku_name,
                        'trend_rank': self.current_rank,
                        'product_url': product_url,
                        'calendar_week': self.calendar_week,
                        'crawl_datetime': (datetime.now() + timedelta(hours=self.time_offset_hours)).strftime('%Y-%m-%d %H:%M:%S'),
                        'batch_id': self.batch_id
                    }

                    products.append(product_data)

                except Exception as e:
                    print(f"[ERROR] Product {idx} extract failed: {e}")
                    traceback.print_exc()
                    continue

            print(f"[INFO] Trend page: {len(products)} products")
            return products

        except Exception as e:
            print(f"[ERROR] Trend page failed: {e}")
            traceback.print_exc()
            return []

    def save_products(self, products):
        """DB 저장: URL 정규화 → DB 캐시 확인 → UPDATE(기존) / INSERT(신규) → 3-tier retry"""
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
                SET trend_rank = %s
                WHERE account_name = %s AND batch_id = %s AND product_url = %s
            """

            for product in products:
                # URL에서 item 추출 (dt.py와 동일한 로직)
                product_url = product.get('product_url')
                item = self.extract_item_from_url(product_url)
                retailer_sku_name = product.get('retailer_sku_name') or ''

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

                # 2. 페이지 내 중복 체크 (이미 수집한 item → 스킵)
                if item in self.crawled_urls:
                    self.stats['duplicates'] += 1
                    continue
                self.crawled_urls.add(item)

                # 3. DB 캐시에서 기존 item 체크 → UPDATE / INSERT 분류
                matched_url = self.db_url_map.get(item)
                if matched_url:
                    try:
                        cursor.execute(update_query, (
                            product['trend_rank'],
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

            # INSERT 처리 (3-tier retry: BATCH_SIZE → RETRY_SIZE → 1개씩)
            if products_to_insert:
                insert_query = """
                    INSERT INTO bby_tv_product_list (
                        account_name, page_type, retailer_sku_name,
                        trend_rank, product_url,
                        calendar_week, crawl_datetime, batch_id
                    ) VALUES (
                        %s, %s, %s, %s, %s, %s, %s, %s
                    )
                """

                BATCH_SIZE = 20
                RETRY_SIZE = 5

                def product_to_tuple(product):
                    return (
                        product['account_name'],
                        product['page_type'],
                        product['retailer_sku_name'],
                        product['trend_rank'],
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

            # 통계 업데이트
            self.stats['updated'] += update_count
            self.stats['inserted'] += insert_count

            return {'insert': insert_count, 'update': update_count}

        except Exception as e:
            print(f"[ERROR] Failed to save products: {e}")
            traceback.print_exc()
            return {'insert': 0, 'update': 0}

    def run(self):
        """실행: initialize() → crawl_page() → save_products() → 리소스 정리"""
        try:
            if not self.initialize():
                print("[ERROR] Initialization failed")
                return False

            self.current_rank = 0
            products = self.crawl_page()

            if not products:
                print("[ERROR] No products found")
                return False

            result = self.save_products(products)

            print(f"[DONE] Update: {result['update']}, Insert: {result['insert']}, batch_id: {self.batch_id}")
            return True

        except Exception as e:
            print(f"[ERROR] Crawler failed: {e}")
            traceback.print_exc()
            return False

        finally:
            # 통계 출력
            print(f"\n{'='*60}")
            print(f"[STATS] Collected: {self.stats['collected']}, "
                  f"Duplicates: {self.stats['duplicates']}, "
                  f"OpenBox: {self.stats['openbox_filtered']}, "
                  f"NonProduct: {self.stats['non_product']}, "
                  f"Updated: {self.stats['updated']}, "
                  f"Inserted: {self.stats['inserted']}")
            print(f"{'='*60}\n")

            if self.page:
                self.page.quit()
            if self.db_conn:
                self.db_conn.close()


import argparse

def main():
    """개별 실행 진입점 (테스트 모드)"""
    parser = argparse.ArgumentParser(description='BestBuy TV Trend Crawler')
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

    crawler = BestBuyTVTrendCrawler(test_mode=True, time_offset_hours=time_offset)
    crawler.run()
    input("\n[완료] 엔터를 누르면 종료됩니다...")


if __name__ == '__main__':
    main()
