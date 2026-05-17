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
import csv
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit
from datetime import datetime, timedelta
from lxml import html
from DrissionPage import ChromiumPage, ChromiumOptions

# 공통 환경 설정 (작업 디렉토리, 한글 출력, 경로 설정)
RUNNING_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'running')
if RUNNING_DIR not in sys.path:
    sys.path.insert(0, RUNNING_DIR)
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
from common.setup import setup_environment
setup_environment(__file__)

from common.base_crawler import BaseCrawler
from config import DB_CONFIG
from db_readonly import connect_readonly
from listing_sku import extract_numeric_sku, extract_sponsored_status
from listing_graphql import (
    ListingGraphQLSkuCollector,
    direct_listing_products,
    extract_listing_products_from_html,
    listing_graphql_only_enabled,
    save_listing_operation_from_html,
)
from data_paths import graphql_registry_dir, listing_parsed_dir
from step01_main_listing import BestBuyTVMainCrawler as GraphQLListingCrawlerBase



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
        self.csv_output_dir = str(listing_parsed_dir("bsr"))
        self.graphql_output_dir = str(graphql_registry_dir())
        self.csv_output_path = os.path.join(self.csv_output_dir, 'bby_tv_bsr1_vpn_test.csv')

        if os.path.exists(self.csv_output_path):
            os.remove(self.csv_output_path)

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

    def connect_db(self):
        """V2 listing crawler는 DB에서 설정만 읽고 절대 저장하지 않는다."""
        try:
            self.db_conn = connect_readonly({**DB_CONFIG, 'database': 'postgres'})
            print("[SUCCESS] Read-only database connected")
            return True
        except Exception as e:
            print(f"[ERROR] Database connection failed: {e}")
            traceback.print_exc()
            return False

    def setup_drission_driver(self):
        """DrissionPage 브라우저 설정 (봇 감지 우회 강화)"""
        try:
            co = ChromiumOptions()
            co.auto_port()
            co.no_imgs(True)
            self.page = ChromiumPage(co)
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

    def get_page_html_safely(self, page_number, context, max_attempts=3):
        """Read page HTML with recovery for transient DrissionPage CDP stalls."""
        last_error = None
        for attempt in range(1, max_attempts + 1):
            try:
                return self.page.html
            except Exception as exc:
                last_error = exc
                print(f"[WARNING] Page {page_number}: html read failed during {context} ({attempt}/{max_attempts}): {exc}")
                try:
                    js_html = self.page.run_js("return document.documentElement.outerHTML;")
                    if js_html:
                        print(f"[INFO] Page {page_number}: recovered HTML via JS outerHTML")
                        return js_html
                except Exception as js_exc:
                    print(f"[WARNING] Page {page_number}: JS outerHTML fallback failed: {js_exc}")
                if attempt < max_attempts:
                    try:
                        self.page.refresh()
                    except Exception as refresh_exc:
                        print(f"[WARNING] Page {page_number}: refresh after HTML timeout failed: {refresh_exc}")
                    time.sleep(random.uniform(5, 8))
        raise last_error

    def run_js_safely(self, script, default=None, context="run_js", timeout=8):
        """Run JS without letting optional scroll checks fail the parsed page."""
        try:
            return self.page.run_js(script, timeout=timeout)
        except Exception as exc:
            print(f"[WARNING] JS failed during {context}: {exc}")
            return default

    def ensure_24_results_url(self, url):
        """Force Best Buy listing pages to request at least 24 products per page."""
        if not url:
            return url
        try:
            parts = urlsplit(url)
            query = dict(parse_qsl(parts.query, keep_blank_values=True))
            query["nrp"] = "24"
            return urlunsplit((parts.scheme, parts.netloc, parts.path, urlencode(query), parts.fragment))
        except Exception:
            separator = "&" if "?" in url else "?"
            return f"{url}{separator}nrp=24"

    def crawl_page(self, page_number):
        """페이지 크롤링: 페이지 로드 → 제품 파싱 → URL 누락 시 1스텝 스크롤 로딩 → 반복 (스마트 스크롤)"""
        products = []
        sku_collector = ListingGraphQLSkuCollector(self.page, output_dir=self.graphql_output_dir)
        try:
            url = self.ensure_24_results_url(self.url_template.replace('{page}', str(page_number)))
            expected_page_products = int(os.environ.get("BBY_LISTING_EXPECTED_PAGE_PRODUCTS", "24"))
            base_container_xpath = self.xpaths.get('base_container', {}).get('xpath')
            if not base_container_xpath:
                print("[ERROR] base_container XPath not found")
                return []

            listing_defaults = {
                'account_name': self.account_name,
                'bsr_rank': 0,
                'calendar_week': self.calendar_week,
                'crawl_datetime': (datetime.now() + timedelta(hours=self.time_offset_hours)).strftime('%Y-%m-%d %H:%M:%S'),
                'batch_id': self.batch_id,
            }
            try:
                direct_products = direct_listing_products(
                    self.graphql_output_dir,
                    self.page_type,
                    page_number,
                    defaults=listing_defaults,
                    page_size=expected_page_products,
                )
                if direct_products:
                    print(f"[INFO] Page {page_number}: Direct GraphQL listing rows collected: {len(direct_products)}")
                    if len(direct_products) >= expected_page_products:
                        return direct_products
                    print(f"[WARNING] Page {page_number}: Direct GraphQL rows {len(direct_products)}/{expected_page_products}; falling back to browser discovery")
                    if os.environ.get("BBY_LISTING_BROWSER_FALLBACK", "1").strip().lower() in {"0", "false", "no"}:
                        raise RuntimeError(f"Direct GraphQL rows below minimum {len(direct_products)}/{expected_page_products}")
                else:
                    print(f"[INFO] Page {page_number}: No reusable direct listing GraphQL operation captured; starting browser discovery")
            except Exception as exc:
                print(f"[WARNING] Page {page_number}: Direct GraphQL listing failed: {exc}")
                if os.environ.get("BBY_LISTING_BROWSER_FALLBACK", "1").strip().lower() in {"0", "false", "no"}:
                    raise

            sku_collector.start()
            self.page.get(url)
            time.sleep(random.uniform(3, 5))
            sku_collector.drain(4)

            initial_html = self.get_page_html_safely(page_number, "initial HTML/API payload parse")
            try:
                cookies = {}
                for cookie in self.page.cookies():
                    if cookie.get("name"):
                        cookies[cookie.get("name")] = cookie.get("value")
                path = save_listing_operation_from_html(self.graphql_output_dir, initial_html, cookies=cookies)
                if path:
                    print(f"[INFO] Saved Apollo listing GraphQL operation from HTML -> {path}")
            except Exception as exc:
                print(f"[WARNING] Page {page_number}: Apollo listing operation capture failed: {exc}")
            try:
                initial_tree = html.fromstring(initial_html)
                initial_card_count = len(initial_tree.xpath(base_container_xpath))
            except Exception:
                initial_tree = None
                initial_card_count = 0

            api_products = sku_collector.listing_products(
                self.page_type,
                page_number=page_number,
                defaults=listing_defaults,
            )
            if api_products:
                print(f"[INFO] Page {page_number}: GraphQL listing rows collected: {len(api_products)}")
                if len(api_products) >= expected_page_products:
                    return api_products
                print(f"[WARNING] Page {page_number}: GraphQL rows {len(api_products)}/{expected_page_products}; waiting for more GraphQL/API rows")
                for attempt in range(1, 7):
                    self.run_js_safely(f"window.scrollTo(0, {attempt * 900});", context=f"page {page_number} graphql scroll {attempt}")
                    time.sleep(random.uniform(1.5, 2.5))
                    sku_collector.drain(2)
                    api_products = sku_collector.listing_products(
                        self.page_type,
                        page_number=page_number,
                        defaults=listing_defaults,
                    )
                    print(f"[INFO] Page {page_number}: GraphQL rows after wait {attempt}: {len(api_products)}/{expected_page_products}")
                    if len(api_products) >= expected_page_products:
                        return api_products

            if listing_graphql_only_enabled():
                if api_products:
                    raise RuntimeError(f"GraphQL-only listing rows below minimum {len(api_products)}/{expected_page_products}")
                raise RuntimeError("GraphQL-only listing did not produce rows; DOM/HTML fallback disabled")

            payload_products = extract_listing_products_from_html(initial_html, self.page_type, page_number=page_number)
            if payload_products:
                for product in payload_products:
                    for key, value in listing_defaults.items():
                        product.setdefault(key, value)
                print(f"[INFO] Page {page_number}: HTML/API listing rows collected: {len(payload_products)}")
                if len(payload_products) >= expected_page_products:
                    return payload_products
                print(f"[WARNING] Page {page_number}: HTML/API rows {len(payload_products)}/{expected_page_products}; continuing GraphQL/API wait before DOM fallback")

            # 1. 0개인 경우 로드 실패 예외처리 (최대 3회 새로고침)
            for refresh_attempt in range(1, 4):
                if refresh_attempt == 1 and initial_tree is not None:
                    tree = initial_tree
                else:
                    page_html = self.get_page_html_safely(page_number, f"initial parse {refresh_attempt}")
                    tree = html.fromstring(page_html)
                if len(tree.xpath(base_container_xpath)) == 0:
                    print(f"[WARNING] Page {page_number}: 0 products found, refresh attempt {refresh_attempt}/3")
                    if refresh_attempt < 3:
                        self.page.refresh()
                        time.sleep(random.uniform(5, 8))
                        sku_collector.drain(3)
                    continue
                break

            # 리프레쉬 3회 후에도 0개이면 빈 리스트 반환
            if len(tree.xpath(base_container_xpath)) == 0:
                print(f"[ERROR] Page {page_number}: No products found after 3 refresh attempts")
                return []

            current_position = 0
            max_scroll_attempts = 30
            bottom_wait_count = 0

            # 2. 파싱 및 스크롤 루프 (url 못찾은거 있으면 스크롤 1회 > 약 5초 대기 > 재파싱 반복)
            for scroll_attempt in range(1, max_scroll_attempts + 1):
                page_html = self.get_page_html_safely(page_number, f"scroll parse {scroll_attempt}")
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

                        numeric_sku = extract_numeric_sku(item, product_url)
                        offer_raw = self.safe_extract(item, 'offer')
                        offer = None
                        if offer_raw:
                            match = re.search(r'\d+', offer_raw)
                            offer = match.group() if match else offer_raw

                        delivery_raw = self.safe_extract(item, 'delivery_availability')
                        delivery = delivery_raw if delivery_raw and 'delivery' in delivery_raw.lower() else None

                        sku_status_raw = self.safe_extract(item, 'sku_status')
                        sku_status = extract_sponsored_status(item, sku_status_raw)

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
                            'numeric_sku': numeric_sku,
                            'calendar_week': self.calendar_week,
                            'crawl_datetime': (datetime.now() + timedelta(hours=self.time_offset_hours)).strftime('%Y-%m-%d %H:%M:%S'),
                            'batch_id': self.batch_id
                        })
                    except Exception as e:
                        continue

                null_url_count = sum(1 for p in products if not p.get('product_url'))
                total_found = len(products)

                if total_found > 0 and null_url_count > 0 and len(sku_collector.products) >= total_found:
                    sku_collector.apply_by_order(products)
                    null_url_count = sum(1 for p in products if not p.get('product_url'))
                    if null_url_count == 0:
                        print(f"[INFO] Page {page_number}: Completed listing rows from GraphQL order map: {total_found}")
                        break

                # 조건 1: 목표 수량까지 모두 찾았으면 바텀까지 가볍게 스크롤 후 반복문 종료
                if total_found >= expected_page_products and null_url_count == 0:
                    print(f"[INFO] Page {page_number}: All {total_found}/{expected_page_products} URLs loaded successfully! Quick scrolling to bottom...")
                    for _ in range(20):
                        is_bottom = self.run_js_safely("""
                            var elem = document.querySelector("div.pagination-container");
                            if (!elem) return false;
                            var rect = elem.getBoundingClientRect();
                            return (rect.top >= 0 && rect.top <= window.innerHeight);
                        """, default=False, context=f"page {page_number} quick bottom check")

                        total_height_check = self.run_js_safely(
                            "return document.body.scrollHeight",
                            default=current_position,
                            context=f"page {page_number} quick height check",
                        )
                        if is_bottom or current_position >= total_height_check:
                            break

                        # 남은 공간 빠르게 1~2초 간격으로 스크롤
                        scroll_step = random.randint(500, 800)
                        current_position += scroll_step
                        self.run_js_safely(
                            f"window.scrollTo(0, {current_position});",
                            context=f"page {page_number} quick scroll",
                        )
                        time.sleep(random.uniform(1.0, 2.0))
                        sku_collector.drain(1.5)

                    break

                if total_found > 0 and null_url_count == 0:
                    print(f"[INFO] Page {page_number}: Parsed {total_found}/{expected_page_products} complete URLs; continuing scroll for remaining products")

                print(f"[INFO] Page {page_number}: Parsed {total_found} products, {null_url_count} URLs missing. Scrolling... ({scroll_attempt}/{max_scroll_attempts})")

                # 하단 도달(페이지네이션 보임) 체크
                is_pagination_visible = self.run_js_safely("""
                    var elem = document.querySelector("div.pagination-container");
                    if (!elem) return false;
                    var rect = elem.getBoundingClientRect();
                    return (rect.top >= 0 && rect.top <= window.innerHeight);
                """, default=True, context=f"page {page_number} pagination check")

                total_height = self.run_js_safely(
                    "return document.body.scrollHeight",
                    default=current_position,
                    context=f"page {page_number} height check",
                )

                if is_pagination_visible or current_position >= total_height:
                    # 바텀에 도달했는데 null_url이 있다면 맨 위로 올라가서 다시 스크롤을 내리며 훑어보기
                    bottom_wait_count += 1
                    if bottom_wait_count >= 3:
                        print(f"[INFO] Page {page_number}: Reached bottom {bottom_wait_count} times. Giving up scroll.")
                        break

                    print(f"[WARNING] Page {page_number}: Reached bottom but {null_url_count} URLs missing. Scrolling back to TOP... ({bottom_wait_count}/3)")
                    current_position = 0
                    self.run_js_safely("window.scrollTo(0, 0);", context=f"page {page_number} top scroll")
                    time.sleep(random.uniform(3, 5))
                    sku_collector.drain(2)
                    continue

                # 스크롤 1회 내리고 5초 대기 (봇 탐지를 피하기 위해 4~6초 랜덤)
                scroll_step = random.randint(400, 600)
                current_position += scroll_step
                self.run_js_safely(f"window.scrollTo(0, {current_position});", context=f"page {page_number} scroll")
                time.sleep(random.uniform(4, 6))
                sku_collector.drain(2)

            sku_collector.apply(products)
            print(f"[INFO] Page {page_number}: Final parsed products: {len(products)}")
            if len(products) < expected_page_products:
                raise RuntimeError(
                    f"Page {page_number}: listing products below minimum "
                    f"{len(products)}/{expected_page_products}; refusing partial page save"
                )
            return products

        except Exception as e:
            print(f"[ERROR] Page {page_number} failed: {e}")
            strict_direct = os.environ.get("BBY_LISTING_BROWSER_FALLBACK", "1").strip().lower() in {"0", "false", "no"}
            if not (strict_direct and "listing GraphQL" in str(e)):
                traceback.print_exc()
            if products and len(products) >= int(os.environ.get("BBY_LISTING_EXPECTED_PAGE_PRODUCTS", "24")):
                sku_collector.apply(products)
                print(f"[WARNING] Page {page_number}: returning {len(products)} products parsed before failure")
                return products
            return []
        finally:
            sku_collector.stop()

    def save_products(self, products):
        """V2 저장: DB UPDATE/INSERT 없이 dt1 입력용 CSV에만 저장한다."""
        if not products:
            return {'insert': 0, 'update': 0}

        # 수집 갯수 통계
        self.stats['collected'] += len(products)

        try:
            csv_products = []
            update_count = 0
            page_valid_items = 0

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

                if item:
                    page_valid_items += 1

                # 2. 페이지 간 중복 체크 (이미 수집한 item → 스킵)
                if item in self.crawled_urls:
                    print(f"[SKIP] 중복 item={item}: {retailer_sku_name[:40] if retailer_sku_name else 'N/A'}... url={product_url}")
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
                csv_products.append(product)

            if not csv_products:
                print("[INFO] No products to save")
                return {'insert': 0, 'update': 0}

            expected_page_products = int(os.environ.get("BBY_LISTING_EXPECTED_PAGE_PRODUCTS", "24"))
            if not self.test_mode and page_valid_items < expected_page_products:
                print(
                    f"[ERROR] Page products below minimum "
                    f"{page_valid_items}/{expected_page_products}; refusing partial page save"
                )
                return {'insert': 0, 'update': 0}

            fieldnames = [
                'account_name', 'batch_id', 'page_type', 'bsr_rank', 'retailer_sku_name',
                'offer', 'pick_up_availability', 'shipping_availability',
                'delivery_availability', 'sku_status', 'product_url', 'numeric_sku',
                'crawl_datetime', 'calendar_week', 'page_number', 'fastest_delivery'
            ]
            file_exists = os.path.exists(self.csv_output_path)
            with open(self.csv_output_path, 'a', newline='', encoding='utf-8-sig') as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                if not file_exists or os.path.getsize(self.csv_output_path) == 0:
                    writer.writeheader()
                for product in csv_products:
                    row = {field: product.get(field) for field in fieldnames}
                    row['shipping_availability'] = product.get('fastest_delivery')
                    writer.writerow(row)

            insert_count = len(csv_products)
            self.stats['inserted'] += insert_count
            print(f"[CSV] Saved {insert_count} rows to {self.csv_output_path}")
            return {'insert': insert_count, 'update': update_count}

        except Exception as e:
            print(f"[ERROR] Failed to save products to CSV: {e}")
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


class BestBuyTVBSRCrawler(GraphQLListingCrawlerBase):
    """BSR listing crawler using the same direct GraphQL replay path as main."""

    def __init__(self, test_mode=True, batch_id=None, time_offset_hours=0):
        BaseCrawler.__init__(self)
        self.test_mode = test_mode
        self.account_name = 'Bestbuy'
        self.page_type = 'bsr'
        self.batch_id = batch_id
        self.time_offset_hours = time_offset_hours
        self.calendar_week = None
        self.url_template = None

        self.test_count = 1
        self.max_products = 100
        self.max_pages = 20
        self.current_rank = 0
        self.saved_urls = set()
        self.csv_output_dir = str(listing_parsed_dir("bsr"))
        self.graphql_output_dir = str(graphql_registry_dir())
        self.csv_output_path = os.path.join(self.csv_output_dir, 'bby_tv_bsr1_vpn_test.csv')
        self.raw_target_output_path = os.path.join(self.csv_output_dir, 'bby_tv_bsr_raw_target_list.csv')
        self.main_data_dir = os.path.dirname(self.csv_output_dir)
        self.graphql_raw_dir = os.path.join(self.main_data_dir, "raw_graphql")
        self.benchmarks_dir = os.path.join(self.main_data_dir, "benchmarks")
        self.page_benchmarks_path = os.path.join(self.benchmarks_dir, "page_benchmarks.csv")
        self.page_summary_path = os.path.join(self.csv_output_dir, "bsr_page_summary.json")
        self.raw_search_summary_path = os.path.join(self.main_data_dir, "raw_search_summary.json")
        self.manifest_path = os.path.join(self.main_data_dir, "manifest.json")
        self.graphql_operation = None
        self.graphql_endpoint = None
        self.zenrows_client = None
        self.page_benchmarks = []
        self.raw_search = []
        self.run_started_at = None
        self.run_start_perf = None

        if os.path.exists(self.csv_output_path):
            os.remove(self.csv_output_path)
        if os.path.exists(self.raw_target_output_path):
            os.remove(self.raw_target_output_path)

        self.stats = {
            'collected': 0,
            'duplicates': 0,
            'openbox_filtered': 0,
            'non_product': 0,
            'inserted': 0,
            'skipped_by_target': 0,
        }

    def prepare_graphql_payload(self, page_number, referer_url):
        payload = super().prepare_graphql_payload(page_number, referer_url)
        variables = payload.setdefault("variables", {})
        variables.setdefault("sort", {})
        variables["sort"]["sort"] = os.getenv("BESTBUY_BSR_SORT", "Best-Selling")
        return payload

    def parse_graphql_product(self, product, page_number, extra=None):
        row = super().parse_graphql_product(product, page_number, extra)
        if row:
            row.pop("main_rank", None)
            row["bsr_rank"] = 0
        return row

    def save_products(self, products):
        """V2 저장: DB INSERT 없이 dt 입력용 BSR CSV에만 저장한다."""
        if not products:
            return 0

        self.stats['collected'] += len(products)
        unique_products = []
        for idx, product in enumerate(products):
            retailer_sku_name = product.get('retailer_sku_name') or ''
            product_url = product.get('product_url')
            item = self.extract_item_from_url(product_url)

            if product_url and 'openbox' in product_url.lower():
                print(f"[SKIP] Open Box 상품 제외: {product_url}")
                self.stats['openbox_filtered'] += 1
                continue

            if self.is_product_excluded(item):
                print(f"[SKIP] 비제품(is_product=FALSE): {retailer_sku_name[:40] if retailer_sku_name else 'N/A'}...")
                self.stats['non_product'] += 1
                continue

            if item and item in self.saved_urls:
                print(f"[SKIP] 중복 item={item}: {retailer_sku_name[:40] if retailer_sku_name else 'N/A'}... url={product_url}")
                self.stats['duplicates'] += 1
                continue

            if item:
                self.saved_urls.add(item)

            self.current_rank += 1
            target = self.test_count if self.test_mode else self.max_products
            if self.current_rank > target:
                self.stats['skipped_by_target'] += len(products) - idx
                break
            product['bsr_rank'] = self.current_rank
            unique_products.append(product)

        if not unique_products:
            return 0

        fieldnames = [
            'account_name', 'batch_id', 'page_type', 'bsr_rank', 'retailer_sku_name',
            'offer', 'pick_up_availability', 'shipping_availability',
            'delivery_availability', 'sku_status', 'product_url', 'numeric_sku',
            'sku_id', 'bsin', 'source_product_url',
            'final_sku_price', 'original_sku_price', 'savings',
            'crawl_datetime', 'calendar_week', 'page_number', 'fastest_delivery'
        ]
        try:
            file_exists = os.path.exists(self.csv_output_path)
            with open(self.csv_output_path, 'a', newline='', encoding='utf-8-sig') as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                if not file_exists or os.path.getsize(self.csv_output_path) == 0:
                    writer.writeheader()
                for product in unique_products:
                    row = {field: product.get(field) for field in fieldnames}
                    row['shipping_availability'] = product.get('fastest_delivery')
                    writer.writerow(row)

            saved_count = len(unique_products)
            self.stats['inserted'] += saved_count
            print(f"[CSV] Saved {saved_count} rows to {self.csv_output_path}")
            return saved_count
        except Exception as e:
            print(f"[ERROR] Failed to save products to CSV: {e}")
            traceback.print_exc()
            return 0

    def write_benchmark_outputs(self, total_products):
        super().write_benchmark_outputs(total_products)


import argparse

def main():
    """개별 실행 진입점 (테스트 모드)"""
    parser = argparse.ArgumentParser(description='BestBuy TV BSR Crawler')
    parser.add_argument('--time_offset', type=int, default=0, help='시간 오프셋 (기본값: 0)')
    args = parser.parse_args()

    test_mode = os.environ.get('BBY_TEST_MODE', '0') == '1'
    crawler = BestBuyTVBSRCrawler(test_mode=test_mode, time_offset_hours=args.time_offset)
    crawler.run()

if __name__ == '__main__':
    main()


