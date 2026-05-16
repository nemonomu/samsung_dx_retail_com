"""
BestBuy TV Promotion 페이지 크롤러

================================================================================
실행 모드
================================================================================
- 개별 실행: test_mode=True (기본값)
- 통합 크롤러: test_mode 및 batch_id를 파라미터로 전달

================================================================================
주요 기능
================================================================================
- Promotion 페이지에서 제품 리스트 수집 (promotion_position, promotion_type 포함)
- promotion_type: 페이지 상단 배너 문구 (h2 + p 결합)
- 테스트 모드: test_count 설정값만큼 수집
- 운영 모드: 단일 페이지 전체 크롤링 (최대 6개)

================================================================================
저장 테이블
================================================================================
- bby_tv_product_list (제품 목록)
  - 기존 제품(main/bsr): promotion_position, promotion_type UPDATE
  - 신규 제품: INSERT
"""

import sys
import os
import time
import random
import traceback
import re
from datetime import datetime, timedelta
from lxml import html
from DrissionPage import ChromiumOptions, ChromiumPage

# 공통 환경 설정 (작업 디렉토리, 한글 출력, 경로 설정)
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
from common.setup import setup_environment
setup_environment(__file__)

from common.base_crawler import BaseCrawler



class BestBuyTVPromotionCrawler(BaseCrawler):
    """
    BestBuy TV Promotion 페이지 크롤러
    """

    def __init__(self, test_mode=True, batch_id=None, time_offset_hours=0):
        """초기화. test_mode: 테스트(True)/운영 모드(False), batch_id: 통합 크롤러에서 전달"""
        super().__init__()
        self.test_mode = test_mode
        self.account_name = 'Bestbuy'
        self.page_type = 'promotion'
        self.batch_id = batch_id
        self.time_offset_hours = time_offset_hours
        self.calendar_week = None
        self.url_template = None
        self.current_rank = 0

        self.test_count = 1  # 테스트 모드

        self.db_url_map = {}       # {item: 원본URL} - Main에서 저장된 URL

        # 통계 변수 (promotion_type별)
        self.stats_by_type = {}  # {promotion_type: {'collected': 0, 'updated': 0, ...}}
        self.stats = {
            'openbox_filtered': 0,
            'non_product': 0,
        }

        # DrissionPage 드라이버
        self.page = None

    def extract_item_from_url(self, product_url):
        """URL에서 item (SKU ID) 추출 - dt.py와 동일한 로직"""
        if not product_url:
            return None
        try:
            cleaned_url = re.sub(r'/sku/\d+(/openbox\?.*)?', '', product_url)
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
        if not self.load_xpaths(self.account_name, self.page_type, 'SEA', 'TV'):
            return False
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

        # DB에서 기존 URL 캐시 로드 (Main에서 저장된 URL → item 매핑)
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

    def extract_promotion_type(self, section):
        """섹션 요소에서 promotion_type 추출 (4개 XPath 시도 후 결합)"""
        type_fields = ['promotion_type_h2', 'promotion_type_h3', 'promotion_type_p', 'promotion_type_sub']
        parts = []
        for field in type_fields:
            xpath = self.xpaths.get(field, {}).get('xpath')
            if not xpath:
                continue
            try:
                elems = section.xpath(xpath)
                if elems:
                    text = ' '.join(elems[0].text_content().split()).strip()
                    if text:
                        parts.append(text)
            except Exception:
                continue
        promotion_type = ' '.join(parts).strip() or None
        return promotion_type

    def crawl_page(self):
        """페이지 크롤링: 페이지 로드 → HTML 파싱(최대 3회) → 섹션별 제품 데이터 추출"""
        try:
            url = self.url_template

            section_container_xpath = self.xpaths.get('section_container', {}).get('xpath')
            base_container_xpath = self.xpaths.get('base_container', {}).get('xpath')
            if not section_container_xpath or not base_container_xpath:
                print("[ERROR] section_container 또는 base_container XPath not found")
                return []

            self.page.get(url)
            time.sleep(random.uniform(25, 35))

            sections = []
            for attempt in range(1, 4):
                page_html = self.page.html
                tree = html.fromstring(page_html)
                sections = tree.xpath(section_container_xpath)

                print(f"[INFO] Attempt {attempt}: Found {len(sections)} sections")
                if sections:
                    break

                if attempt < 3:
                    time.sleep(random.uniform(8, 12))

            if not sections:
                print("[ERROR] No sections found")
                return []

            sections_to_process = sections[:self.test_count] if self.test_mode else sections

            products = []
            for sec_idx, section in enumerate(sections_to_process, 1):
                promotion_type = self.extract_promotion_type(section)
                print(f"[INFO] 섹션 {sec_idx} promotion_type: {promotion_type}")

                items = section.xpath(base_container_xpath)
                print(f"[INFO] 섹션 {sec_idx}: {len(items)}개 아이템")

                for pos, item in enumerate(items, 1):
                    try:
                        retailer_sku_name = self.safe_extract(item, 'retailer_sku_name') or ''
                        product_url_raw = self.safe_extract(item, 'product_url')
                        product_url = f"https://www.bestbuy.com{product_url_raw}" if product_url_raw and product_url_raw.startswith('/') else product_url_raw

                        # offer 추출 (+N offers for you)
                        offer_raw = self.safe_extract(item, 'offer')
                        offer = None
                        if offer_raw:
                            match = re.search(r'\d+', offer_raw)
                            offer = match.group() if match else offer_raw

                        product_data = {
                            'account_name': self.account_name,
                            'page_type': self.page_type,
                            'retailer_sku_name': retailer_sku_name,
                            'promotion_position': pos,
                            'promotion_type': promotion_type,
                            'offer': offer,
                            'product_url': product_url,
                            'calendar_week': self.calendar_week,
                            'crawl_datetime': (datetime.now() + timedelta(hours=self.time_offset_hours)).strftime('%Y-%m-%d %H:%M:%S'),
                            'batch_id': self.batch_id
                        }
                        products.append(product_data)

                    except Exception as e:
                        print(f"[ERROR] 섹션 {sec_idx} 아이템 {pos} 추출 실패: {e}")
                        continue

            print(f"[INFO] 총 수집: {len(products)}개 ({len(sections_to_process)}개 섹션)")
            return products

        except Exception as e:
            print(f"[ERROR] Promotion page failed: {e}")
            traceback.print_exc()
            return []

    def save_products(self, products):
        """DB 저장: URL 정규화 → DB 캐시 확인 → UPDATE(기존) / INSERT(신규) → 3-tier retry"""
        if not products:
            return {'insert': 0, 'update': 0}

        try:
            cursor = self.db_conn.cursor()
            insert_count = 0
            update_count = 0
            products_to_insert = []

            update_query = """
                UPDATE bby_tv_product_list
                SET promotion_position = %s, promotion_type = %s, offer = %s
                WHERE account_name = %s AND batch_id = %s AND product_url = %s
            """

            def get_type_stats(ptype):
                """promotion_type별 통계 dict 반환 (없으면 생성)"""
                if ptype not in self.stats_by_type:
                    self.stats_by_type[ptype] = {'collected': 0, 'updated': 0, 'inserted': 0, 'skipped': 0}
                return self.stats_by_type[ptype]

            for product in products:
                # URL에서 item 추출 (dt.py와 동일한 로직)
                product_url = product.get('product_url')
                item = self.extract_item_from_url(product_url)
                retailer_sku_name = product.get('retailer_sku_name') or ''
                ptype = product.get('promotion_type') or 'Unknown'
                type_stats = get_type_stats(ptype)
                type_stats['collected'] += 1

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

                # 2. DB 캐시에서 기존 item 체크 → UPDATE / INSERT 분류
                matched_url = self.db_url_map.get(item)
                if matched_url:
                    try:
                        # 기존에 promotion_position이 이미 있으면 스킵 (첫 번째 값 유지)
                        cursor.execute("""
                            SELECT promotion_position, promotion_type FROM bby_tv_product_list
                            WHERE account_name = %s AND batch_id = %s AND product_url = %s
                        """, (self.account_name, product['batch_id'], matched_url))
                        existing = cursor.fetchone()
                        if existing and existing[0] is not None:
                            print(f"[SKIP] 이미 프로모션 등록됨 - item: {item}, "
                                  f"기존 position: {existing[0]}, 새 position: {product['promotion_position']}, "
                                  f"기존 type: {existing[1]}, 새 type: {product['promotion_type']}")
                            type_stats['skipped'] += 1
                            continue

                        cursor.execute(update_query, (
                            product['promotion_position'],
                            product['promotion_type'],
                            product['offer'],
                            self.account_name,
                            product['batch_id'],
                            matched_url
                        ))
                        self.db_conn.commit()
                        update_count += 1
                        type_stats['updated'] += 1
                    except Exception as e:
                        print(f"[ERROR] UPDATE failed: {product_url[:50] if product_url else 'N/A'}: {e}")
                        self.db_conn.rollback()
                else:
                    products_to_insert.append(product)

            # INSERT 처리 (단건)
            if products_to_insert:
                insert_query = """
                    INSERT INTO bby_tv_product_list (
                        account_name, page_type, retailer_sku_name,
                        promotion_position, promotion_type, offer,
                        product_url, calendar_week, crawl_datetime, batch_id
                    ) VALUES (
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                    )
                """

                for product in products_to_insert:
                    ptype = product.get('promotion_type') or 'Unknown'
                    type_stats = get_type_stats(ptype)
                    try:
                        cursor.execute(insert_query, (
                            product['account_name'],
                            product['page_type'],
                            product['retailer_sku_name'],
                            product['promotion_position'],
                            product['promotion_type'],
                            product['offer'],
                            product['product_url'],
                            product['calendar_week'],
                            product['crawl_datetime'],
                            product['batch_id']
                        ))
                        self.db_conn.commit()
                        insert_count += 1
                        type_stats['inserted'] += 1
                    except Exception as e:
                        print(f"[ERROR] INSERT failed: {(product.get('retailer_sku_name') or 'N/A')[:30]}: {e}")
                        self.db_conn.rollback()

            cursor.close()

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
            # 통계 출력 (promotion_type별)
            print(f"\n{'='*60}")
            for ptype, ts in self.stats_by_type.items():
                print(f"[STATS] Type: {ptype}")
                print(f"        Collected: {ts['collected']}, Updated: {ts['updated']}, "
                      f"Inserted: {ts['inserted']}, Skipped: {ts['skipped']}")
            print(f"[STATS] OpenBox: {self.stats['openbox_filtered']}, "
                  f"NonProduct: {self.stats['non_product']}")
            print(f"{'='*60}\n")

            if self.page:
                self.page.quit()
            if self.db_conn:
                self.db_conn.close()


import argparse

def main():
    """개별 실행 진입점 (테스트 모드)"""
    parser = argparse.ArgumentParser(description='BestBuy TV Promotion Crawler')
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

    crawler = BestBuyTVPromotionCrawler(test_mode=True, time_offset_hours=time_offset)
    crawler.run()
    input("\n[완료] 엔터를 누르면 종료됩니다...")


if __name__ == '__main__':
    main()
