import time
import random
import psycopg2
from datetime import datetime
import pytz
from DrissionPage import ChromiumPage
from lxml import html
import re
from urllib.parse import urlparse, parse_qs, unquote

# Import database configuration
from config import DB_CONFIG
from wmart_config_loader import get_wmart_config

class WalmartTVBSRCrawler:
    def __init__(self):
        self.page = None
        self.db_conn = None
        self.xpaths = {}
        self.total_collected = 0
        self.excluded_urls = set()  # URLs to exclude (is_product=false)
        # Load config from DB
        self.config = get_wmart_config()
        self.max_skus = self.config.get_constant_int('max_skus', 'wmart_tv_bsr', default=100)
        self.sequential_id = 1  # ID counter for 1-100
        # Generate batch_id using Korea timezone
        korea_tz = pytz.timezone('Asia/Seoul')
        self.batch_id = datetime.now(korea_tz).strftime('%Y%m%d_%H%M%S')

    def connect_db(self):
        """Connect to PostgreSQL database"""
        try:
            self.db_conn = psycopg2.connect(**DB_CONFIG)
            print("[OK] Database connected")
            return True
        except Exception as e:
            print(f"[ERROR] Database connection failed: {e}")
            return False

    def load_excluded_urls(self):
        """Load URLs to exclude (is_product=false from tv_item_mst)"""
        try:
            cursor = self.db_conn.cursor()
            cursor.execute("""
                SELECT product_url FROM tv_item_mst
                WHERE is_product = FALSE AND product_url IS NOT NULL
            """)

            for row in cursor.fetchall():
                if row[0]:
                    # Normalize: remove trailing slash for consistent comparison
                    self.excluded_urls.add(row[0].rstrip('/'))

            cursor.close()
            print(f"[OK] Loaded {len(self.excluded_urls)} excluded URLs (is_product=false)")
            return True
        except Exception as e:
            print(f"[WARNING] Failed to load excluded URLs: {e}")
            return True  # Continue anyway

    def load_xpaths(self):
        """Load XPath selectors from database"""
        try:
            cursor = self.db_conn.cursor()
            cursor.execute("""
                SELECT data_field, xpath, css_selector
                FROM xpath_selectors
                WHERE mall_name = 'Walmart' AND page_type = 'main' AND is_active = TRUE
            """)

            for row in cursor.fetchall():
                self.xpaths[row[0]] = {
                    'xpath': row[1],
                    'css': row[2]
                }

            cursor.close()
            print(f"[OK] Loaded {len(self.xpaths)} XPath selectors")
            return True

        except Exception as e:
            print(f"[ERROR] Failed to load XPaths: {e}")
            return False

    def load_page_urls(self):
        """Load page URLs from database"""
        try:
            cursor = self.db_conn.cursor()
            cursor.execute("""
                SELECT page_number, url
                FROM wmart_tv_bsr_page_url
                WHERE page_type = 'bsr' AND is_active = TRUE
                ORDER BY page_number
            """)

            urls = cursor.fetchall()
            cursor.close()
            print(f"[OK] Loaded {len(urls)} page URLs")
            return urls

        except Exception as e:
            print(f"[ERROR] Failed to load page URLs: {e}")
            return []

    def setup_driver(self):
        """Setup DrissionPage browser"""
        try:
            self.page = ChromiumPage()
            print("[OK] Browser setup complete (DrissionPage)")
            return True
        except Exception as e:
            print(f"[ERROR] Browser setup failed: {e}")
            import traceback
            traceback.print_exc()
            return False

    def check_robot_page(self, page_source):
        """Check if page is showing 'Robot or human?' challenge"""
        robot_texts = self.config.get_robot_check_texts()
        for text in robot_texts:
            if text in page_source:
                return True
        return False

    def handle_captcha(self):
        """Handle 'PRESS & HOLD' CAPTCHA if present"""
        try:
            print("[INFO] Checking for CAPTCHA...")
            page_content = self.page.html.lower()
            captcha_keywords = self.config.get_captcha_keywords()
            if any(keyword in page_content for keyword in captcha_keywords):
                print("[WARNING] CAPTCHA keywords found in page")
                captcha_wait = self.config.get_float('timing', 'captcha_wait', default=60)
                print(f"[INFO] CAPTCHA detection - waiting {int(captcha_wait)} seconds for manual intervention...")
                time.sleep(captcha_wait)
                print("[INFO] Continuing after wait...")
                return True
            else:
                print("[INFO] No CAPTCHA detected")
                return True
        except Exception as e:
            print(f"[WARNING] CAPTCHA check failed: {e}")
            return True

    def initialize_session(self):
        """Initialize session with natural browsing pattern to avoid bot detection"""
        try:
            print("[INFO] Initializing session - navigating to Walmart homepage...")

            # Get timing and scroll config values
            homepage_wait = self.config.get_timing_range('homepage_wait') or (8, 12)
            browse_wait = self.config.get_timing_range('browse_wait') or (10, 15)
            scroll_wait_range = self.config.get_timing_range('scroll_wait') or (1, 2)
            robot_recovery_wait = self.config.get_float('timing', 'robot_recovery_wait', default=30)
            random_scroll_range = self.config.get_scroll_range('random_amount') or (150, 300)
            recovery_scroll_range = self.config.get_scroll_range('recovery_amount') or (200, 500)
            homepage_url = self.config.get_url('homepage') or "https://www.walmart.com"

            self.page.get(homepage_url)
            time.sleep(random.uniform(*homepage_wait))

            # Check for robot detection
            if self.check_robot_page(self.page.html):
                print("[WARNING] Robot detection on homepage. Handling CAPTCHA...")
                self.handle_captcha()
                captcha_after_wait = self.config.get_timing_range('captcha_after_wait') or (3, 5)
                time.sleep(random.uniform(*captcha_after_wait))

                if self.check_robot_page(self.page.html):
                    print("[WARNING] Still showing robot detection, trying recovery...")
                    # Slow scroll down
                    scroll_between_wait = self.config.get_timing_range('scroll_between_wait') or (1.5, 2.5)
                    for i in range(5):
                        scroll_amount = random.randint(*random_scroll_range)
                        self.page.run_js(f"window.scrollBy(0, {scroll_amount})")
                        time.sleep(random.uniform(*scroll_between_wait))
                    self.page.run_js("window.scrollBy(0, -200)")
                    time.sleep(random.uniform(*scroll_wait_range))

                print(f"[INFO] Waiting {int(robot_recovery_wait)} seconds...")
                time.sleep(robot_recovery_wait)

                print("[INFO] Reloading page...")
                self.page.refresh()
                time.sleep(random.uniform(*browse_wait))

                if self.check_robot_page(self.page.html):
                    print("[ERROR] Still getting robot detection after recovery")
                    print("[INFO] Attempting to continue anyway...")

            # Simple homepage exploration
            print("[INFO] Exploring homepage...")
            time.sleep(random.uniform(*scroll_wait_range) * 2)

            # Random scrolling
            for _ in range(random.randint(2, 4)):
                scroll_amount = random.randint(*recovery_scroll_range)
                self.page.run_js(f"window.scrollBy(0, {scroll_amount})")
                time.sleep(random.uniform(*scroll_wait_range))

            # Scroll back to top
            self.page.run_js("window.scrollTo(0, 0)")
            time.sleep(random.uniform(*scroll_wait_range) + 1)

            print("[OK] Session initialized")
            return True

        except Exception as e:
            print(f"[ERROR] Failed to initialize session: {e}")
            import traceback
            traceback.print_exc()
            return False

    def add_random_mouse_movements(self):
        """Add random mouse movements to appear more human (DrissionPage handles this internally)"""
        pass  # DrissionPage has built-in bot detection bypass

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
        """Scrape a single page"""
        try:
            print(f"\n[PAGE {page_number}] Accessing: {url[:80]}...")

            # Get timing ranges from config
            browse_wait = self.config.get_timing_range('browse_wait') or (10, 15)
            scroll_wait_range = self.config.get_timing_range('scroll_wait') or (1, 2)
            page_load_wait = self.config.get_timing_range('page_load_wait', 'wmart_tv_bsr') or (8, 12)
            direct_url_wait = self.config.get_timing_range('direct_url_wait') or (12, 18)
            scroll_wait = self.config.get_float('timing', 'scroll_wait', default=2)
            scroll_max_rounds = self.config.get_int('scroll', 'max_rounds', default=2)
            browse_tvs_url = self.config.get_url('browse_tvs') or "https://www.walmart.com/browse/electronics/tvs/3944_1060825"

            # For page 1, navigate naturally through browse page
            if page_number == 1:
                print("[INFO] Navigating to Walmart browse page first...")
                try:
                    # Try browse electronics category first
                    self.page.get(browse_tvs_url)
                    time.sleep(random.uniform(*browse_wait))

                    print("[OK] Browse page loaded successfully")
                    # Add human-like behavior
                    self.add_random_mouse_movements()
                    time.sleep(random.uniform(*scroll_wait_range) * 2)

                    # Scroll a bit
                    for _ in range(2):
                        self.page.run_js("window.scrollBy(0, 400);")
                        time.sleep(random.uniform(*scroll_wait_range))

                    # Now access the best seller URL directly
                    print("[INFO] Now navigating to best seller page...")
                    self.page.get(url)
                    time.sleep(random.uniform(*page_load_wait))
                except Exception as e:
                    print(f"[WARNING] Browse navigation failed: {e}, using direct URL...")
                    self.page.get(url)
                    time.sleep(random.uniform(*direct_url_wait))
            else:
                # For other pages, direct access
                self.page.get(url)
                time.sleep(random.uniform(*page_load_wait))

            # Scroll to load all products
            print("[INFO] Scrolling to load products...")
            for _ in range(scroll_max_rounds):
                self.page.run_js("window.scrollTo(0, document.body.scrollHeight);")
                time.sleep(scroll_wait)
            self.page.run_js("window.scrollTo(0, 0);")
            time.sleep(scroll_wait)

            # Get page source and parse
            page_source = self.page.html
            tree = html.fromstring(page_source)

            # Find all product containers
            base_xpath = self.xpaths['base_container']['xpath']
            products = tree.xpath(base_xpath)
            print(f"[INFO] Found {len(products)} product containers")

            # Process each product
            collected_count = 0
            for idx, product in enumerate(products, 1):
                if self.total_collected >= self.max_skus:
                    print(f"[INFO] Reached maximum SKU limit ({self.max_skus})")
                    return False

                # Extract product name
                product_name = self.extract_text_safe(product, self.xpaths['product_name']['xpath'])
                if not product_name:
                    continue

                # Extract product URL
                product_url_raw = self.extract_text_safe(product, self.xpaths['product_url']['xpath'])
                product_url = self.normalize_product_url(product_url_raw) if product_url_raw else None

                # Skip if URL is in excluded list (is_product=false)
                if product_url and product_url.rstrip('/') in self.excluded_urls:
                    print(f"  [{idx}/{len(products)}] SKIP: is_product=false - {product_name[:40]}...")
                    continue

                # Extract availability fields
                pickup = self.extract_text_safe(product, self.xpaths['pickup_availability']['xpath'])
                shipping = self.extract_text_safe(product, self.xpaths['shipping_availability']['xpath'])
                delivery = self.extract_text_safe(product, self.xpaths['delivery_availability']['xpath'])

                # Extract SKU status
                rollback = self.extract_text_safe(product, self.xpaths['sku_status_rollback']['xpath'])
                sponsored = self.extract_text_safe(product, self.xpaths['sku_status_sponsored']['xpath'])
                sku_status = "Rollback" if rollback else ("Sponsored" if sponsored else None)

                # Extract membership discount
                membership_discount_elem = self.extract_text_safe(product, self.xpaths['membership_discount']['xpath'])
                membership_discount = "Walmart Plus" if membership_discount_elem else None

                # Extract availability (numbers only: "only 1 left" -> "1")
                available_quantity_raw = self.extract_text_safe(product, self.xpaths['available_quantity']['xpath'])
                available_quantity = self.extract_number_only(available_quantity_raw) if available_quantity_raw else None
                inventory_status = self.extract_text_safe(product, self.xpaths['inventory_status']['xpath'])

                data = {
                    'page_type': 'bsr',
                    'Retailer_SKU_Name': product_name,
                    'Pick_Up_Availability': pickup,
                    'Shipping_Availability': shipping,
                    'Delivery_Availability': delivery,
                    'SKU_Status': sku_status,
                    'Retailer_Membership_Discounts': membership_discount,
                    'Available_Quantity_for_Purchase': available_quantity,
                    'Inventory_Status': inventory_status,
                    'Product_url': product_url
                }

                # Save to database
                if self.save_to_db(data):
                    collected_count += 1
                    self.total_collected += 1
                    print(f"  [{idx}/{len(products)}] Collected: {product_name[:50]}...")

            print(f"[PAGE {page_number}] Collected {collected_count} products (Total: {self.total_collected}/{self.max_skus})")
            return True

        except Exception as e:
            print(f"[ERROR] Failed to scrape page {page_number}: {e}")
            import traceback
            traceback.print_exc()
            return True

    def normalize_product_url(self, raw_url):
        """Normalize product URL to clean format"""
        if not raw_url:
            return None

        if '/sp/track?' in raw_url:
            try:
                parsed = urlparse(raw_url)
                query_params = parse_qs(parsed.query)
                if 'rd' in query_params:
                    redirect_url = query_params['rd'][0]
                    decoded_url = unquote(redirect_url)
                    if '/ip/' in decoded_url:
                        ip_path = decoded_url.split('/ip/')[1]
                        clean_path = '/ip/' + ip_path.split('?')[0]
                        return f"https://www.walmart.com{clean_path}"
            except:
                pass

        if raw_url.startswith('/ip/'):
            clean_path = raw_url.split('?')[0]
            return f"https://www.walmart.com{clean_path}"

        if raw_url.startswith('http'):
            if '/ip/' in raw_url:
                return raw_url.split('?')[0]
            return raw_url

        return raw_url

    def clean_price_text(self, price_text):
        """Extract clean price from complex price HTML text"""
        if not price_text:
            return None

        price_text = ' '.join(price_text.split())
        match = re.search(r'\$\s*(\d[\d,]*)\s*(\d{2})', price_text)
        if match:
            dollars = match.group(1).replace(',', '')
            cents = match.group(2)
            return f"${dollars}.{cents}"

        return price_text

    def extract_number_only(self, text):
        """Extract only numbers from text (for offer and available_quantity)
        Examples: '4 free offers from Apple' -> '4', 'only 1 left' -> '1'
        """
        if not text:
            return None

        # Search for first number in the text
        match = re.search(r'(\d+)', text)
        if match:
            return match.group(1)

        return None

    def save_to_db(self, data):
        """Save collected data with collection order (1-100)"""
        try:
            cursor = self.db_conn.cursor()

            # Check for duplicate product_url in the same batch
            product_url = data.get('Product_url')
            if product_url:
                cursor.execute("""
                    SELECT COUNT(*) FROM wmart_tv_bsr_crawl
                    WHERE batch_id = %s AND Product_url = %s
                """, (self.batch_id, product_url))

                count = cursor.fetchone()[0]

                if count > 0:
                    cursor.close()
                    print(f"  [SKIP] Duplicate URL already saved in this batch")
                    return False

            bsr_rank = self.sequential_id

            # Calculate calendar week
            calendar_week = f"w{datetime.now().isocalendar().week}"

            # Calculate crawl_strdatetime (format: 202511051100000000)
            now = datetime.now()
            crawl_strdatetime = now.strftime('%Y%m%d%H%M%S') + '0000'

            account_name = self.config.get_constant('account_name', default='Walmart')
            cursor.execute("""
                INSERT INTO wmart_tv_bsr_crawl
                (account_name, bsr_rank, page_type, Retailer_SKU_Name,
                 Pick_Up_Availability, Shipping_Availability, Delivery_Availability,
                 SKU_Status, Retailer_Membership_Discounts, Available_Quantity_for_Purchase,
                 Inventory_Status, Product_url, batch_id, calendar_week, crawl_strdatetime)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                RETURNING id
            """, (
                account_name,
                bsr_rank,
                data['page_type'],
                data['Retailer_SKU_Name'],
                data['Pick_Up_Availability'],
                data['Shipping_Availability'],
                data['Delivery_Availability'],
                data['SKU_Status'],
                data['Retailer_Membership_Discounts'],
                data['Available_Quantity_for_Purchase'],
                data['Inventory_Status'],
                data['Product_url'],
                self.batch_id,
                calendar_week,
                crawl_strdatetime
            ))

            result = cursor.fetchone()
            if result:
                self.sequential_id += 1

            self.db_conn.commit()
            cursor.close()
            return result is not None

        except Exception as e:
            print(f"[ERROR] Failed to save to DB: {e}")
            return False

    def run(self):
        """Main execution"""
        try:
            print("="*80)
            print("Walmart TV BSR Crawler")
            print(f"Batch ID: {self.batch_id}")
            print("="*80)

            if not self.connect_db():
                return

            if not self.load_xpaths():
                return

            # Load excluded URLs (is_product=false)
            self.load_excluded_urls()

            page_urls = self.load_page_urls()
            if not page_urls:
                print("[ERROR] No page URLs found")
                return

            self.setup_driver()

            # Initialize session (visit Walmart homepage first to avoid bot detection)
            if not self.initialize_session():
                print("[WARNING] Session initialization had issues, continuing anyway...")

            # Get timing config
            between_pages = self.config.get_timing_range('between_pages', 'wmart_tv_bsr') or (5, 8)

            # Scrape each page
            for page_number, url in page_urls:
                if self.total_collected >= self.max_skus:
                    break

                if not self.scrape_page(url, page_number):
                    break

                time.sleep(random.uniform(*between_pages))

            print("\n" + "="*80)
            print(f"Crawling completed! Total collected: {self.total_collected} SKUs")
            print("="*80)

        except Exception as e:
            print(f"[ERROR] Crawler failed: {e}")
            import traceback
            traceback.print_exc()

        finally:
            # 결과 JSON 저장
            try:
                import json, os
                result_dir = r"C:\samsung_dx_retail_com\stage_results"
                os.makedirs(result_dir, exist_ok=True)
                with open(os.path.join(result_dir, "wmart_tv_bsr.json"), "w") as f:
                    json.dump({"collected_count": self.total_collected}, f)
            except Exception as e:
                print(f"[WARNING] Failed to write result JSON: {e}")

            if self.page:
                try:
                    self.page.quit()
                except Exception as e:
                    print(f"[WARNING] Error closing driver: {e}")
                    try:
                        # Force kill chrome processes
                        import os as _os
                        _os.system("taskkill /F /IM chrome.exe /T 2>nul")
                        _os.system("taskkill /F /IM chromedriver.exe /T 2>nul")
                    except:
                        pass
            if self.db_conn:
                try:
                    self.db_conn.close()
                except:
                    pass


if __name__ == "__main__":
    try:
        crawler = WalmartTVBSRCrawler()
        crawler.run()
    except Exception as e:
        print(f"\n[FATAL ERROR] {e}")
        import traceback
        traceback.print_exc()

    print("\n[INFO] BSR Crawler completed.")
