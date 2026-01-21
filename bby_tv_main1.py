"""
Best Buy TV Main Page Crawler (Modified v1)

수집 내용:
- retailer_sku_name (제품명)
- product_url (상품 URL)
- offer (오퍼 개수)
- pick_up_availability (픽업 가능 여부)
- shipping_availability (배송 가능 여부)
- delivery_availability (배달 가능 여부)
- sku_status (Sponsored 여부)
- main_rank (순위)

저장 테이블: bestbuy_tv_main_crawl (page_type='main')

v1 수정사항:
1. offer는 숫자만 저장 (예: "+2 offers for you" -> "2")
2. SKU Status는 sponsored만 수집 (Regular는 공란)
3. item -> retailer_sku_name으로 변경
4. price, savings, original_sku_price, star_rating 수집 제거
   - 해당 필드들은 bby_tv_dt1.py에서 상세 페이지 크롤링 시 수집
"""
import time
import random
import re
import os
import psycopg2
from datetime import datetime
import pytz
from DrissionPage import ChromiumPage, ChromiumOptions
from lxml import html
from data_validator import DataValidator

# Import database configuration
from config import DB_CONFIG
from bby_config_loader import get_config
from bby_utils import load_excluded_items, is_excluded_url

class BestBuyTVCrawler:
    def __init__(self):
        self.page = None
        self.db_conn = None
        self.total_collected = 0
        self.error_messages = []

        # Config loader 초기화
        self.config = get_config()
        self.file_name = 'bby_tv_main1'

        # Check for TEST_MODE
        if os.environ.get('TEST_MODE') == '1':
            self.max_products = int(os.environ.get('TEST_MAX_PRODUCTS', '3'))
        else:
            self.max_products = self.config.get_int('constant', 'max_products_main', self.file_name, 300)

        # Data validator 초기화
        session_start_time = os.environ.get('SESSION_START_TIME', datetime.now().strftime('%Y%m%d%H%M'))
        self.validator = DataValidator(session_start_time)
        self.korea_tz = pytz.timezone('Asia/Seoul')
        self.batch_id = datetime.now(self.korea_tz).strftime('%Y%m%d_%H%M%S')

        # Load excluded items (is_product=false)
        self.excluded_items = load_excluded_items()

    def connect_db(self):
        """Connect to PostgreSQL database"""
        try:
            self.db_conn = psycopg2.connect(**DB_CONFIG)
            print("[OK] Database connected")
            return True
        except Exception as e:
            print(f"[ERROR] Database connection failed: {e}")
            return False

    def setup_browser(self):
        """Setup DrissionPage ChromiumPage - 최소 설정"""
        try:
            print("[INFO] Setting up DrissionPage browser...")

            # 가장 기본적인 설정만 사용
            self.page = ChromiumPage()

            print("[OK] DrissionPage browser setup complete")
        except Exception as e:
            print(f"[ERROR] Browser setup failed: {e}")
            import traceback
            traceback.print_exc()
            raise

    def close_browser(self):
        """Close the current browser"""
        if self.page:
            try:
                self.page.quit()
            except:
                pass
            self.page = None

    def load_page_urls(self):
        """Load page URLs from database"""
        try:
            cursor = self.db_conn.cursor()
            table_name = self.config.get_table('main_page_url')
            cursor.execute(f"""
                SELECT page_number, url
                FROM {table_name}
                WHERE is_active = TRUE
                ORDER BY page_number
            """)

            urls = cursor.fetchall()
            cursor.close()
            print(f"[OK] Loaded {len(urls)} page URLs")
            return urls

        except Exception as e:
            print(f"[ERROR] Failed to load page URLs: {e}")
            return []

    def extract_text_safe(self, element, xpath):
        """Safely extract text from element using xpath"""
        try:
            result = element.xpath(xpath)
            if result:
                if isinstance(result[0], str):
                    return result[0].strip()
                else:
                    return result[0].text_content().strip()
            return None
        except Exception as e:
            return None

    def scrape_page(self, url, page_number):
        """Scrape a single Best Buy page"""
        try:
            print(f"\n[PAGE {page_number}] Accessing: {url[:80]}...")
            self.page.get(url)

            print("[INFO] Waiting for page to load...")
            wait_range = self.config.get_timing_range('page_load_wait', self.file_name)
            time.sleep(random.uniform(wait_range[0], wait_range[1]) if wait_range else random.uniform(5, 8))

            # Wait for product list to load
            try:
                css_product_list = self.config.get('css', 'product_list_item', self.file_name, '.product-list-item')
                element_timeout = self.config.get_float('timing', 'element_timeout', self.file_name, 20)
                self.page.wait.ele_displayed(css_product_list, timeout=element_timeout)
                print("[OK] Product list loaded")
            except Exception as e:
                print(f"[WARNING] Product list not found: {e}")

            # Slow scroll to trigger lazy loading - scroll by small increments
            print("[INFO] Performing slow scroll to trigger lazy loading...")

            scroll_step = self.config.get_int('constant', 'scroll_step_px', self.file_name, 300)
            current_position = 0

            scroll_wait = self.config.get_float('timing', 'scroll_wait', self.file_name, 1.0)
            page_end_wait = self.config.get_float('timing', 'page_end_check_wait', self.file_name, 2)
            scroll_up_wait = self.config.get_float('timing', 'scroll_up_wait', self.file_name, 0.3)
            top_scroll_wait = self.config.get_float('timing', 'top_scroll_wait', self.file_name, 2)

            while True:
                current_position += scroll_step
                self.page.run_js(f"window.scrollTo(0, {current_position})")
                time.sleep(scroll_wait)

                # 현재 높이 확인
                new_height = self.page.run_js("return document.body.scrollHeight")

                # 페이지 끝에 도달했는지 확인
                if current_position >= new_height:
                    # 끝에 도달, 추가 대기 후 확인
                    time.sleep(page_end_wait)
                    final_height = self.page.run_js("return document.body.scrollHeight")
                    if final_height == new_height:
                        print(f"[DEBUG] Reached bottom at {current_position}px")
                        break

                # 10번 스크롤마다 로그
                if (current_position // scroll_step) % 10 == 0:
                    print(f"[DEBUG] Scrolled to {current_position}px, page height: {new_height}px")

            # 다시 천천히 위로 스크롤 (이미지 로드 확인)
            print("[INFO] Scrolling back up slowly...")
            while current_position > 0:
                current_position -= scroll_step * 2  # 올라갈 땐 좀 더 빠르게
                if current_position < 0:
                    current_position = 0
                self.page.run_js(f"window.scrollTo(0, {current_position})")
                time.sleep(scroll_up_wait)

            # 맨 위로
            self.page.scroll.to_top()
            time.sleep(top_scroll_wait)

            # 제품 링크 개수 확인 - 부족하면 추가 대기
            css_product_link = 'css:' + self.config.get('css', 'product_link', self.file_name, '.product-list-item-link')
            min_product_count = self.config.get_int('constant', 'min_product_count', self.file_name, 20)
            product_load_retry = self.config.get_retry('product_load', self.file_name, 3)
            extra_scroll_wait = self.config.get_float('timing', 'extra_scroll_wait', self.file_name, 3)

            product_links = self.page.eles(css_product_link)
            if len(product_links) < min_product_count:
                print(f"[WARNING] Only {len(product_links)} products found, waiting more...")
                for _ in range(product_load_retry):
                    self.page.scroll.to_bottom()
                    time.sleep(extra_scroll_wait)
                    self.page.scroll.to_top()
                    time.sleep(top_scroll_wait)
                    product_links = self.page.eles(css_product_link)
                    print(f"[DEBUG] Products after extra scroll: {len(product_links)}")
                    if len(product_links) >= min_product_count:
                        break

            # 이미지 로드 상태 확인
            print("[INFO] Checking image load status...")
            css_product_image = self.config.get('css', 'product_image', self.file_name, 'img.product-image')
            loaded_images = self.page.run_js(f"""
                const imgs = document.querySelectorAll('{css_product_image}');
                let loaded = 0;
                imgs.forEach(img => {{
                    if (img.complete && img.naturalHeight > 0 && !img.src.includes('coming-soon')) {{
                        loaded++;
                    }}
                }});
                return loaded + '/' + imgs.length;
            """)
            print(f"[DEBUG] Images loaded: {loaded_images}")

            # Get page source and parse with lxml
            page_source = self.page.html
            tree = html.fromstring(page_source)

            # Find all product containers
            xpath_container = self.config.get('xpath', 'product_container', self.file_name, '//li[contains(@class, "product-list-item") and contains(@class, "grid-view")]')
            xpath_product_link = self.config.get('xpath', 'product_link', self.file_name, './/a[@class="product-list-item-link"]')
            all_containers = tree.xpath(xpath_container)

            # Filter containers that have product links (more reliable)
            containers = [c for c in all_containers if c.xpath(xpath_product_link)]
            print(f"[INFO] Found {len(all_containers)} total containers, {len(containers)} with product links")

            collected_count = 0

            # Save HTML for debugging (all pages for troubleshooting)
            with open(f'bestbuy_page_{page_number}_debug.html', 'w', encoding='utf-8') as f:
                f.write(page_source)
            print(f"[DEBUG] Saved page source to bestbuy_page_{page_number}_debug.html")

            # XPath/정규식 설정 로드
            xpath_product_title_list = self.config.get_xpath_list('product_title', self.file_name) or ['.//h2[contains(@class, "product-title")]']
            xpath_product_url = self.config.get('xpath', 'product_url', self.file_name, './/a[@class="product-list-item-link"]/@href')
            xpath_offer = self.config.get('xpath', 'offer', self.file_name, './/div[@data-testid="plus-x-offers"]//span[@class="font-sans text-default text-style-body-md-400"]')
            xpath_pickup = self.config.get('xpath', 'pickup_availability', self.file_name, './/div[@class="fulfillment"]//p[contains(., "Pick up")]')
            xpath_shipping = self.config.get('xpath', 'shipping_availability', self.file_name, './/div[@class="fulfillment"]//p[contains(., "Get it") or contains(., "FREE")]')
            xpath_delivery = self.config.get('xpath', 'delivery_availability', self.file_name, './/div[@class="fulfillment"]//p[contains(., "Delivery")]')
            xpath_sponsored = self.config.get('xpath', 'sponsored', self.file_name, './/div[@class="sponsored"]')
            offer_regex = self.config.get_constant('offer_regex', None, r'(\d+)')
            base_url = self.config.get_url('base_url') or 'https://www.bestbuy.com'

            for idx, container in enumerate(containers, 1):
                # max_products 도달하면 수집 중단
                if self.total_collected >= self.max_products:
                    print(f"[INFO] Reached maximum {self.max_products} products. Stopping collection.")
                    break

                try:
                    # Extract product name (Retailer_SKU_Name)
                    product_name = None
                    for xpath in xpath_product_title_list:
                        try:
                            product_name_elem = container.xpath(xpath)
                            if product_name_elem:
                                if isinstance(product_name_elem[0], str):
                                    product_name = product_name_elem[0].strip()
                                else:
                                    product_name = product_name_elem[0].text_content().strip()
                                if product_name:
                                    break
                        except:
                            continue

                    if not product_name:
                        # Save container HTML for debugging (all failed items)
                        container_html = html.tostring(container, encoding='unicode', pretty_print=True)
                        with open(f'bestbuy_page{page_number}_container_{idx}_debug.html', 'w', encoding='utf-8') as f:
                            f.write(container_html)
                        print(f"  [SKIP {idx}] No product name found (debug file saved)")
                        continue

                    # Extract product URL
                    product_url_elem = container.xpath(xpath_product_url)
                    if product_url_elem:
                        product_url = product_url_elem[0]
                        # 상대 경로인 경우에만 도메인 추가
                        if product_url.startswith('/'):
                            product_url = f"{base_url}{product_url}"
                    else:
                        product_url = None

                    # Skip Open Box products
                    if product_url and 'openbox' in product_url.lower():
                        print(f"  [SKIP {idx}] Open Box product - excluded")
                        continue

                    # Skip is_product=false items
                    if product_url and is_excluded_url(product_url, self.excluded_items):
                        print(f"  [SKIP {idx}] is_product=false - excluded")
                        continue

                    # Extract Offer (+ X offers) - 숫자만 저장
                    offer_elem = container.xpath(xpath_offer)
                    offer = None
                    if offer_elem:
                        offer_text = offer_elem[0].text_content().strip()
                        match = re.search(offer_regex, offer_text)
                        if match:
                            offer = match.group(1)

                    # Extract Pick-Up Availability
                    pickup_elem = container.xpath(xpath_pickup)
                    pickup = pickup_elem[0].text_content().strip() if pickup_elem else None

                    # Extract Shipping Availability
                    shipping_elem = container.xpath(xpath_shipping)
                    shipping = shipping_elem[0].text_content().strip() if shipping_elem else None

                    # Extract Delivery Availability (Delivery only, ignore Installation)
                    delivery_elem = container.xpath(xpath_delivery)
                    if delivery_elem:
                        delivery_text = delivery_elem[0].text_content().strip()
                        if "Delivery" in delivery_text:
                            delivery = delivery_text
                        else:
                            delivery = None
                    else:
                        delivery = None

                    # Extract SKU_Status (Sponsored만 수집, Regular는 공란)
                    status_elem = container.xpath(xpath_sponsored)
                    sku_status = "Sponsored" if status_elem else None

                    # Validate data quality
                    self.validator.validate_item(product_name, product_url, 'bby_tv_main1')

                    # Save to database
                    # 저장 성공 시에만 total_collected 증가
                    page_type = self.config.get_constant('page_type_main', self.file_name, 'main')
                    if self.save_to_db(
                        page_type=page_type,
                        main_rank=self.total_collected + 1,  # 다음 rank 값 전달
                        retailer_sku_name=product_name,
                        offer=offer,
                        pickup=pickup,
                        shipping=shipping,
                        delivery=delivery,
                        sku_status=sku_status,
                        product_url=product_url
                    ):
                        self.total_collected += 1  # 저장 성공 시에만 증가
                        collected_count += 1
                        print(f"  [{idx}/{len(containers)}] {product_name[:60]}...")

                except Exception as e:
                    print(f"  [ERROR {idx}] Failed to extract data: {e}")
                    continue

            print(f"[PAGE {page_number}] Collected {collected_count} products (Total: {self.total_collected})")

            # max_products에 도달했으면 더 이상 수집하지 않음
            if self.total_collected >= self.max_products:
                print(f"[INFO] Maximum {self.max_products} products reached. Stopping page collection.")
                return False

            return True

        except Exception as e:
            print(f"[ERROR] Failed to scrape page {page_number}: {e}")
            import traceback
            traceback.print_exc()
            return False

    def save_to_db(self, page_type, main_rank, retailer_sku_name, offer, pickup, shipping, delivery,
                   sku_status, product_url):
        """Save product data to database (without price/savings/star_rating)"""
        try:
            cursor = self.db_conn.cursor()
            table_name = self.config.get_table('main_data')
            account_name = self.config.get_constant('account_name', None, 'Bestbuy')

            # Check for duplicate product_url in the same batch
            cursor.execute(f"""
                SELECT COUNT(*) FROM {table_name}
                WHERE batch_id = %s AND product_url = %s
            """, (self.batch_id, product_url))

            count = cursor.fetchone()[0]

            if count > 0:
                cursor.close()
                print(f"  [SKIP] Duplicate URL already saved in this batch")
                return False

            # Calculate calendar week
            calendar_week = f"w{datetime.now().isocalendar().week}"

            # Calculate crawl_datetime (format: YYYY-MM-DD HH:MM:SS)
            now = datetime.now()
            crawl_datetime = now.strftime('%Y-%m-%d %H:%M:%S')

            cursor.execute(f"""
                INSERT INTO {table_name}
                (account_name, batch_id, page_type, main_rank, retailer_sku_name,
                 Offer, Pick_Up_Availability, Shipping_Availability, Delivery_Availability,
                 SKU_Status, Product_url, crawl_datetime, calendar_week)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (account_name, self.batch_id, page_type, main_rank, retailer_sku_name,
                  offer, pickup, shipping, delivery, sku_status, product_url, crawl_datetime, calendar_week))

            self.db_conn.commit()
            cursor.close()

            return True

        except Exception as e:
            print(f"[ERROR] Failed to save to DB: {e}")
            self.error_messages.append(f"DB save error: {e}")
            return False

    def run(self):
        """Main execution"""
        try:
            print("="*80)
            print(f"Best Buy TV Main Page Crawler (Modified) (Batch ID: {self.batch_id})")
            print("="*80)

            # Connect to database
            if not self.connect_db():
                return

            # Load page URLs
            page_urls = self.load_page_urls()
            if not page_urls:
                print("[ERROR] No page URLs found")
                return

            # Setup browser once (프로필 충돌 방지)
            print("[INFO] Setting up browser...")
            self.setup_browser()

            # Visit homepage first for stealth
            print("[INFO] Visiting homepage first...")
            homepage_url = self.config.get_url('homepage') or 'https://www.bestbuy.com'
            homepage_wait = self.config.get_timing_range('homepage_wait', self.file_name)
            self.page.get(homepage_url)
            time.sleep(random.uniform(homepage_wait[0], homepage_wait[1]) if homepage_wait else random.uniform(3, 5))

            # Scrape each page with same browser session
            between_pages_wait = self.config.get_timing_range('between_pages_wait', self.file_name)
            for page_number, url in page_urls:
                try:
                    # Scrape the target page
                    if not self.scrape_page(url, page_number):
                        # scrape_page returns False if max_products reached or error occurred
                        if self.total_collected >= self.max_products:
                            print(f"[INFO] Stopping page collection - reached maximum {self.max_products} products")
                            break
                        else:
                            print(f"[WARNING] Failed to scrape page {page_number}, continuing...")

                    # Random delay between pages
                    time.sleep(random.uniform(between_pages_wait[0], between_pages_wait[1]) if between_pages_wait else random.uniform(3, 5))

                except Exception as e:
                    print(f"[ERROR] Failed to process page {page_number}: {e}")
                    continue

            print("\n" + "="*80)
            print(f"Best Buy Crawling completed! Total collected: {self.total_collected} products")
            print("="*80)

            if self.error_messages:
                print("\nErrors encountered:")
                for error in self.error_messages:
                    print(f"  - {error}")

            # 데이터 검증 요약 출력
            summary = self.validator.get_summary()
            if summary['total'] > 0:
                print("\n" + "="*80)
                print("DATA VALIDATION SUMMARY")
                print("="*80)
                print(f"Total Issues Detected: {summary['total']}")
                for issue_type, count in sorted(summary['by_type'].items()):
                    print(f"  {issue_type}: {count}")
                print(f"\nLog file: C:\\samsung_dx_retail_com\\problems\\{self.validator.session_start_time}.txt")
                print("="*80)
                self.validator.write_summary()
            else:
                print("\n[OK] No data quality issues detected")

        except Exception as e:
            print(f"[ERROR] Crawler failed: {e}")
            import traceback
            traceback.print_exc()

        finally:
            self.close_browser()
            if self.db_conn:
                self.db_conn.close()


if __name__ == "__main__":
    try:
        crawler = BestBuyTVCrawler()
        crawler.run()
    except Exception as e:
        print(f"\n[FATAL ERROR] {e}")
        import traceback
        traceback.print_exc()

    print("\n[INFO] Crawler terminated. Exiting...")
