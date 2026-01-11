"""
Best Buy Best-Selling TV Crawler (Modified v1)

Best Selling TV 페이지 크롤러
URL: https://www.bestbuy.com/site/searchpage.jsp?id=pcat17071&sp=Best-Selling&st=tv

수집 내용:
- retailer_sku_name (제품명)
- product_url (상품 URL)
- offer (오퍼 개수)
- pick_up_availability (픽업 가능 여부)
- shipping_availability (배송 가능 여부)
- delivery_availability (배달 가능 여부)
- sku_status (Sponsored 여부)
- bsr_rank (순위)

저장 테이블: bby_tv_bsr_crawl (page_type='bsr')

v1 수정사항:
- price, savings, original_sku_price, star_rating 수집 제거
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

class BestBuyBSRCrawler:
    def __init__(self):
        self.page = None
        self.db_conn = None
        self.total_collected = 0
        self.error_messages = []

        # Check for TEST_MODE
        if os.environ.get('TEST_MODE') == '1':
            self.max_products = int(os.environ.get('TEST_MAX_PRODUCTS', '3'))
        else:
            self.max_products = 100

        # Data validator 초기화
        session_start_time = os.environ.get('SESSION_START_TIME', datetime.now().strftime('%Y%m%d%H%M'))
        self.validator = DataValidator(session_start_time)
        self.korea_tz = pytz.timezone('Asia/Seoul')
        self.batch_id = datetime.now(self.korea_tz).strftime('%Y%m%d_%H%M%S')

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
        """Setup DrissionPage ChromiumPage"""
        try:
            print("[INFO] Setting up DrissionPage browser...")

            co = ChromiumOptions()
            co.set_argument('--disable-dev-shm-usage')
            co.set_argument('--no-sandbox')
            co.set_argument('--window-size=1920,1080')
            co.set_argument('--lang=en-US,en')
            co.set_argument('--disable-blink-features=AutomationControlled')
            co.set_timeouts(page_load=60)

            self.page = ChromiumPage(co)

            print("[OK] DrissionPage browser setup complete")
        except Exception as e:
            print(f"[ERROR] Browser setup failed: {e}")
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
            cursor.execute("""
                SELECT page_number, url
                FROM bby_tv_bsr_page_url
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
            time.sleep(random.uniform(5, 8))

            # Wait for product list to load
            try:
                self.page.wait.ele_displayed('.product-list-item', timeout=20)
                print("[OK] Product list loaded")
            except Exception as e:
                print(f"[WARNING] Product list not found: {e}")

            # Aggressive scroll to trigger lazy loading of all products
            print("[INFO] Performing aggressive scroll to load all products...")

            # First pass - scroll down to bottom multiple times
            for scroll_round in range(3):
                print(f"[DEBUG] Scroll round {scroll_round + 1}/3")
                scroll_height = self.page.run_js("return document.body.scrollHeight")
                screen_height = self.page.run_js("return window.innerHeight")

                current_position = 0
                while current_position < scroll_height:
                    current_position += screen_height
                    self.page.run_js(f"window.scrollTo(0, {current_position})")
                    time.sleep(2)

                    # Check if new content loaded
                    new_scroll_height = self.page.run_js("return document.body.scrollHeight")
                    if new_scroll_height > scroll_height:
                        scroll_height = new_scroll_height
                        print(f"[DEBUG] Page height increased to {scroll_height}")

                # Scroll to absolute bottom
                self.page.scroll.to_bottom()
                time.sleep(3)
                print(f"[DEBUG] Completed scroll round {scroll_round + 1}, final height: {scroll_height}")

            # Scroll back to top slowly
            print("[INFO] Scrolling back to top...")
            self.page.scroll.to_top()
            time.sleep(2)

            # Wait until enough products are loaded (target: 20+ product links)
            print("[INFO] Waiting for products to fully render...")
            target_count = 20
            max_wait = 30  # 최대 30초 대기
            wait_interval = 2
            elapsed = 0

            while elapsed < max_wait:
                product_links = self.page.eles('css:a.product-list-item-link')
                product_count = len(product_links)
                print(f"[DEBUG] Product links loaded: {product_count}")

                if product_count >= target_count:
                    print(f"[OK] {product_count} products loaded, proceeding...")
                    break

                time.sleep(wait_interval)
                elapsed += wait_interval

                # 추가 스크롤 시도 (lazy loading 트리거)
                if elapsed % 10 == 0:
                    self.page.scroll.to_bottom()
                    time.sleep(1)
                    self.page.scroll.to_top()

            if elapsed >= max_wait:
                print(f"[WARNING] Timeout reached, proceeding with {product_count} products")

            # Get page source and parse with lxml
            page_source = self.page.html
            tree = html.fromstring(page_source)

            # Find all product containers
            # Base container: li with class "product-list-item grid-view"
            # Filter to only include containers with actual product links
            all_containers = tree.xpath('//li[contains(@class, "product-list-item") and contains(@class, "grid-view")]')

            # Filter containers that have product links (more reliable)
            containers = [c for c in all_containers if c.xpath('.//a[@class="product-list-item-link"]')]
            print(f"[INFO] Found {len(all_containers)} total containers, {len(containers)} with product links")

            collected_count = 0

            # Save HTML for debugging (all pages for troubleshooting)
            with open(f'bestbuy_bsr_page_{page_number}_debug.html', 'w', encoding='utf-8') as f:
                f.write(page_source)
            print(f"[DEBUG] Saved page source to bestbuy_bsr_page_{page_number}_debug.html")

            for idx, container in enumerate(containers, 1):
                # max_products 도달하면 수집 중단
                if self.total_collected >= self.max_products:
                    print(f"[INFO] Reached maximum {self.max_products} products. Stopping collection.")
                    break

                try:
                    # Extract product name (Retailer_SKU_Name)
                    # Try multiple possible XPaths (ordered by reliability)
                    product_name = None
                    product_name_xpaths = [
                        './/h2[contains(@class, "product-title")]',
                        './/a[@class="product-list-item-link"]//h2',
                        './/div[@class="sku-block-content-title"]//h2',
                        './/h2[@class="sku-title"]',
                        './/div[contains(@class, "sku-title")]//h2',
                        './/a[contains(@class, "product-title")]//h2',
                        './/div[contains(@class, "information")]//h2',
                        './/h2',  # Last resort: any h2 in container
                        './/a[@class="product-list-item-link"]/@title',  # Link title attribute
                        './/a[@class="product-list-item-link"]/@aria-label',  # Accessibility label
                    ]

                    for xpath in product_name_xpaths:
                        try:
                            product_name_elem = container.xpath(xpath)
                            if product_name_elem:
                                if isinstance(product_name_elem[0], str):
                                    product_name = product_name_elem[0].strip()
                                else:
                                    product_name = product_name_elem[0].text_content().strip()
                                if product_name:  # Found non-empty name
                                    break
                        except:
                            continue

                    if not product_name:
                        # Save container HTML for debugging (all failed items)
                        container_html = html.tostring(container, encoding='unicode', pretty_print=True)
                        with open(f'bestbuy_bsr_page{page_number}_container_{idx}_debug.html', 'w', encoding='utf-8') as f:
                            f.write(container_html)
                        print(f"  [SKIP {idx}] No product name found (debug file saved)")
                        continue

                    # Extract product URL
                    product_url_elem = container.xpath('.//a[@class="product-list-item-link"]/@href')
                    if product_url_elem:
                        product_url = product_url_elem[0]
                        # 상대 경로인 경우에만 도메인 추가
                        if product_url.startswith('/'):
                            product_url = f"https://www.bestbuy.com{product_url}"
                    else:
                        product_url = None

                    # Extract Offer (+ X offers) - 숫자만 저장
                    offer_elem = container.xpath('.//div[@data-testid="plus-x-offers"]//span[@class="font-sans text-default text-style-body-md-400"]')
                    offer = None
                    if offer_elem:
                        offer_text = offer_elem[0].text_content().strip()
                        # 숫자만 추출 (예: "+2 offers for you" -> "2")
                        match = re.search(r'(\d+)', offer_text)
                        if match:
                            offer = match.group(1)

                    # Extract Pick-Up Availability
                    pickup_elem = container.xpath('.//div[@class="fulfillment"]//p[contains(., "Pick up")]')
                    pickup = pickup_elem[0].text_content().strip() if pickup_elem else None

                    # Extract Shipping Availability
                    shipping_elem = container.xpath('.//div[@class="fulfillment"]//p[contains(., "Get it") or contains(., "FREE")]')
                    shipping = shipping_elem[0].text_content().strip() if shipping_elem else None

                    # Extract Delivery Availability (Delivery only, ignore Installation)
                    delivery_elem = container.xpath('.//div[@class="fulfillment"]//p[contains(., "Delivery")]')
                    if delivery_elem:
                        # Only take "Delivery" text, not "Installation"
                        delivery_text = delivery_elem[0].text_content().strip()
                        # Filter out if it's only about Installation
                        if "Delivery" in delivery_text:
                            delivery = delivery_text
                        else:
                            delivery = None
                    else:
                        delivery = None

                    # Extract SKU_Status (Sponsored만 수집, Regular는 공란)
                    status_elem = container.xpath('.//div[@class="sponsored"]')
                    sku_status = "Sponsored" if status_elem else None

                    # Validate data quality
                    self.validator.validate_item(product_name, product_url, 'bby_tv_bsr1')

                    # Save to database
                    # 저장 성공 시에만 total_collected 증가
                    if self.save_to_db(
                        page_type='bsr',
                        bsr_rank=self.total_collected + 1,  # 다음 rank 값 전달
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

    def save_to_db(self, page_type, bsr_rank, retailer_sku_name, offer, pickup, shipping, delivery,
                   sku_status, product_url):
        """Save product data to database (without price/savings/star_rating)"""
        try:
            cursor = self.db_conn.cursor()

            # Check for duplicate product_url in the same batch
            cursor.execute("""
                SELECT COUNT(*) FROM bby_tv_bsr1
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

            cursor.execute("""
                INSERT INTO bby_tv_bsr1
                (account_name, batch_id, page_type, bsr_rank, retailer_sku_name,
                 Offer, Pick_Up_Availability, Shipping_Availability, Delivery_Availability,
                 SKU_Status, Product_url, crawl_datetime, calendar_week)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, ('Bestbuy', self.batch_id, page_type, bsr_rank, retailer_sku_name,
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
            print(f"Best Buy Best-Selling TV Crawler (Batch ID: {self.batch_id})")
            print("="*80)

            # Connect to database
            if not self.connect_db():
                return

            # Load page URLs
            page_urls = self.load_page_urls()
            if not page_urls:
                print("[ERROR] No page URLs found")
                return

            # Scrape each page with fresh browser (page-by-page restart for bot detection bypass)
            for page_number, url in page_urls:
                try:
                    # Setup fresh browser for each page
                    print(f"\n[INFO] Setting up fresh browser for page {page_number}...")
                    self.setup_browser()

                    # Visit homepage first for stealth
                    print("[INFO] Visiting homepage first...")
                    self.page.get("https://www.bestbuy.com")
                    time.sleep(random.uniform(3, 5))

                    # Scrape the target page
                    if not self.scrape_page(url, page_number):
                        # scrape_page returns False if max_products reached or error occurred
                        if self.total_collected >= self.max_products:
                            print(f"[INFO] Stopping page collection - reached maximum {self.max_products} products")
                            self.close_browser()
                            break
                        else:
                            print(f"[WARNING] Failed to scrape page {page_number}, continuing...")

                    # Close browser after each page
                    self.close_browser()

                    # Random delay between pages
                    time.sleep(random.uniform(5, 8))

                except Exception as e:
                    print(f"[ERROR] Failed to process page {page_number}: {e}")
                    self.close_browser()
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
        crawler = BestBuyBSRCrawler()
        crawler.run()
    except Exception as e:
        print(f"\n[FATAL ERROR] {e}")
        import traceback
        traceback.print_exc()

    print("\n[INFO] Crawler terminated. Exiting...")
