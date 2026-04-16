import time
import random
import psycopg2
from datetime import datetime
import pytz
from DrissionPage import ChromiumPage
from lxml import html
import re
import os
import sys
import json
from urllib.parse import urlparse, parse_qs, unquote

# Import database configuration
from config import DB_CONFIG
from wmart_config_loader import get_wmart_config


class Tee:
    """stdout을 콘솔과 파일 둘 다에 출력"""
    def __init__(self, filename):
        self.terminal = sys.stdout
        self.log = open(filename, 'w', encoding='utf-8')

    def write(self, message):
        self.terminal.write(message)
        self.log.write(message)
        self.log.flush()

    def flush(self):
        self.terminal.flush()
        self.log.flush()

    def close(self):
        self.log.close()

class WalmartTVCrawler:
    def __init__(self):
        self.page = None
        self.db_conn = None
        self.xpaths = {}
        self.total_collected = 0
        self.excluded_urls = set()  # URLs to exclude (is_product=false)
        # Load config from DB
        self.config = get_wmart_config()
        self.max_skus = self.config.get_constant_int('max_skus', 'wmart_tv_main2', default=9999)
        self.sequential_id = 1  # ID counter
        # Generate batch_id using Korea timezone
        korea_tz = pytz.timezone('Asia/Seoul')
        self.batch_id = datetime.now(korea_tz).strftime('%Y%m%d_%H%M%S')

    def connect_db(self):
        """Connect to PostgreSQL database"""
        try:
            self.db_conn = psycopg2.connect(**DB_CONFIG)
            print("[OK] Database connected")

            # Get last main_rank from wmart_tv_main_1 and set sequential_id
            cursor = self.db_conn.cursor()
            cursor.execute("""
                SELECT MAX(main_rank)
                FROM wmart_tv_main_1
            """)
            result = cursor.fetchone()
            last_main_rank = result[0] if result and result[0] is not None else 0
            self.sequential_id = last_main_rank + 1
            cursor.close()

            print(f"[OK] Starting main_rank from: {self.sequential_id}")

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
                SELECT data_field, xpath, css_selector, fallback_xpath
                FROM xpath_selectors
                WHERE mall_name = 'Walmart' AND page_type = 'main' AND is_active = TRUE
            """)

            for row in cursor.fetchall():
                self.xpaths[row[0]] = {
                    'xpath': row[1],
                    'css': row[2],
                    'fallback_xpath': row[3]
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
                FROM wmart_tv_main_page_url
                WHERE page_type = 'main' AND is_active = TRUE
                ORDER BY page_number
            """)

            urls = cursor.fetchall()
            cursor.close()
            print(f"[OK] Loaded {len(urls)} page URLs")
            return urls

        except Exception as e:
            print(f"[ERROR] Failed to load page URLs: {e}")
            return []

    def setup_browser(self):
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

    def add_random_mouse_movements(self):
        """Placeholder for mouse movements (simplified for Selenium)"""
        # Mouse movements are not critical for scraping, so we skip this in Selenium
        pass

    def extract_text_safe(self, element, xpath, fallback_xpath=None):
        """Safely extract text from element using xpath, with optional fallback"""
        try:
            result = element.xpath(xpath)
            if result:
                if isinstance(result[0], str):
                    return result[0].strip()
                else:
                    return result[0].text_content().strip()
            # Try fallback xpath if primary failed
            if fallback_xpath:
                result = element.xpath(fallback_xpath)
                if result:
                    if isinstance(result[0], str):
                        return result[0].strip()
                    else:
                        return result[0].text_content().strip()
            return None
        except Exception as e:
            return None

    def check_robot_page(self, page_source):
        """Check if page is showing 'Robot or human?' challenge"""
        robot_texts = self.config.get_robot_check_texts()
        for text in robot_texts:
            if text in page_source:
                return True
        return False

    def handle_captcha(self):
        """Handle 'PRESS & HOLD' CAPTCHA if present (simplified for Selenium)"""
        try:
            print("[INFO] Checking for CAPTCHA...")
            page_content = self.page.html.lower()
            captcha_keywords = self.config.get_captcha_keywords()
            if any(keyword in page_content for keyword in captcha_keywords):
                print("[WARNING] CAPTCHA keywords found in page")
                captcha_wait = self.config.get_float('timing', 'captcha_wait', default=60)
                print(f"[INFO] CAPTCHA detection - waiting {int(captcha_wait)} seconds for manual intervention...")
                try:
                    self.page.get_screenshot(path=f"captcha_screen_{int(time.time())}.png")
                    print("[INFO] Screenshot saved for debugging")
                except:
                    pass
                time.sleep(captcha_wait)
                return True
            else:
                print("[INFO] No CAPTCHA detected")
                return True
        except Exception as e:
            print(f"[WARNING] CAPTCHA check failed: {e}")
            return True

    def scrape_page(self, url, page_number, retry_count=0, is_first_page=False):
        """Scrape a single page"""
        max_retries = self.config.get_retry('max_retries', 'wmart_tv_main2', default=2)

        try:
            print(f"\n[PAGE {page_number}] Accessing: {url[:80]}...")

            # Get timing ranges from config
            browse_wait = self.config.get_timing_range('browse_wait') or (10, 15)
            scroll_wait_range = self.config.get_timing_range('scroll_wait') or (1, 2)
            page_load_wait = self.config.get_timing_range('page_load_wait', 'wmart_tv_main2') or (8, 12)
            direct_url_wait = self.config.get_timing_range('direct_url_wait') or (12, 18)
            product_load_wait = self.config.get_timing_range('product_load_wait') or (5, 8)
            captcha_wait = self.config.get_float('timing', 'captcha_wait', default=60)
            robot_base_wait = self.config.get_float('timing', 'robot_retry_base_wait', default=30)
            robot_increment = self.config.get_float('timing', 'robot_retry_increment', default=15)
            scroll_wait = self.config.get_float('timing', 'scroll_wait', default=3)
            scroll_max_rounds = self.config.get_int('scroll', 'max_rounds', default=2)
            browse_tvs_url = self.config.get_url('browse_tvs') or "https://www.walmart.com/browse/electronics/tvs/3944_1060825"

            # For first page in this session, navigate naturally through browse page
            if is_first_page and retry_count == 0:
                print("[INFO] Navigating to Walmart browse page first...")
                try:
                    # Try browse electronics category first
                    self.page.get(browse_tvs_url)
                    time.sleep(random.uniform(*browse_wait))

                    # Check for robot detection and handle CAPTCHA if needed
                    if self.check_robot_page(self.page.html):
                        print("[WARNING] Robot detected on browse page, handling CAPTCHA...")
                        self.handle_captcha()
                        time.sleep(random.uniform(*scroll_wait_range) * 2)

                    # If no robot detection (or after handling CAPTCHA)
                    if not self.check_robot_page(self.page.html):
                        print("[OK] Browse page loaded successfully")
                        # Add human-like behavior
                        self.add_random_mouse_movements()
                        time.sleep(random.uniform(*scroll_wait_range) * 2)

                        # Scroll a bit
                        for _ in range(2):
                            self.page.run_js("window.scrollBy(0, 400)")
                            time.sleep(random.uniform(*scroll_wait_range))

                        # Now access the search URL directly
                        print("[INFO] Now navigating to search page...")
                        self.page.get(url)
                        time.sleep(random.uniform(*page_load_wait))
                    else:
                        print("[WARNING] Robot still detected after CAPTCHA, using direct URL...")
                        self.page.get(url)
                        time.sleep(random.uniform(*direct_url_wait))
                except Exception as e:
                    print(f"[WARNING] Browse navigation failed: {e}, using direct URL...")
                    self.page.get(url)
                    time.sleep(random.uniform(*direct_url_wait))
            else:
                self.page.get(url)
                time.sleep(random.uniform(*direct_url_wait))

            # Check for robot detection and handle CAPTCHA
            page_source = None
            try:
                page_source = self.page.html
            except Exception as e:
                if "navigating" in str(e).lower():
                    print(f"[WARNING] Page still navigating (likely bot detection)")
                    print(f"[INFO] Please solve CAPTCHA manually if needed...")
                    print(f"[INFO] Waiting {int(captcha_wait)} seconds for manual intervention...")
                    time.sleep(captcha_wait)

                    # Try to get content again
                    try:
                        page_source = self.page.html
                        print("[OK] Page content retrieved after waiting")
                    except Exception as e2:
                        print(f"[ERROR] Still cannot get page content: {e2}")
                        # Will retry this page
                        raise
                else:
                    raise
            if self.check_robot_page(page_source):
                print(f"[WARNING] Robot detection page detected.")

                # Try to handle CAPTCHA first
                if self.handle_captcha():
                    print("[OK] CAPTCHA handled, checking page again...")
                    captcha_after_wait = self.config.get_timing_range('captcha_after_wait') or (3, 5)
                    time.sleep(random.uniform(*captcha_after_wait))
                    page_source = self.page.html

                    # Check if robot detection is gone
                    if not self.check_robot_page(page_source):
                        print("[OK] Robot detection bypassed after CAPTCHA")
                        # Continue with scraping (fall through)
                    else:
                        print("[WARNING] Robot detection still present after CAPTCHA")

                # If still robot detected, retry
                if self.check_robot_page(self.page.html):
                    if retry_count < max_retries:
                        print(f"[WARNING] Retrying... {retry_count + 1}/{max_retries}")
                        wait_time = robot_base_wait + retry_count * robot_increment
                        print(f"[INFO] Waiting {int(wait_time)} seconds before retry...")
                        time.sleep(wait_time)

                        print("[INFO] Refreshing page...")
                        self.page.refresh()
                        time.sleep(random.uniform(*browse_wait))

                        return self.scrape_page(url, page_number, retry_count + 1, is_first_page=False)
                    else:
                        print(f"[ERROR] Failed to bypass robot detection after {max_retries} retries")
                        print("[INFO] Saving page source for debugging...")
                        with open(f'walmart_robot_page_{page_number}.html', 'w', encoding='utf-8') as f:
                            f.write(page_source)
                        return False

            # Wait for page to load
            print("[INFO] Waiting for products to load...")
            time.sleep(random.uniform(*product_load_wait))

            # Scroll to load all products
            print("[INFO] Scrolling to load all products...")
            last_height = self.page.run_js("return document.body.scrollHeight")

            for scroll_round in range(scroll_max_rounds):
                self.page.run_js("window.scrollTo(0, document.body.scrollHeight)")
                time.sleep(scroll_wait)

                new_height = self.page.run_js("return document.body.scrollHeight")
                if new_height == last_height:
                    break
                last_height = new_height

            # Scroll back to top
            self.page.run_js("window.scrollTo(0, 0)")
            time.sleep(2)

            # Get page source and parse with lxml
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

                # Extract product name (required field)
                product_name = self.extract_text_safe(product, self.xpaths['product_name']['xpath'])

                if not product_name:
                    print(f"  [{idx}/{len(products)}] SKIP: No product name found")
                    continue

                # Extract product URL and normalize it
                product_url_raw = self.extract_text_safe(product, self.xpaths['product_url']['xpath'])
                product_url = self.normalize_product_url(product_url_raw) if product_url_raw else None

                # Skip if URL is in excluded list (is_product=false)
                if product_url and product_url.rstrip('/') in self.excluded_urls:
                    print(f"  [{idx}/{len(products)}] SKIP: is_product=false - {product_name[:40]}...")
                    continue

                # Final_SKU_Price and Original_SKU_Price will be collected by wmart_tv_dt1.py
                # Not collecting prices in main crawler
                final_price = None
                original_price = None

                # Offer is collected by dt1 (detail page), not main page
                offer = None

                # Extract Pick-Up_Availability
                pickup_raw = self.extract_text_safe(product, self.xpaths['pickup_availability']['xpath'])
                pickup = pickup_raw if pickup_raw else None

                # Extract Shipping_Availability
                shipping_raw = self.extract_text_safe(product, self.xpaths['shipping_availability']['xpath'])
                shipping = shipping_raw if shipping_raw else None

                # Extract Delivery_Availability
                delivery_raw = self.extract_text_safe(product, self.xpaths['delivery_availability']['xpath'])
                delivery = delivery_raw if delivery_raw else None

                # Extract SKU_Status (check both Rollback and Sponsored)
                rollback = self.extract_text_safe(product, self.xpaths['sku_status_rollback']['xpath'])
                sponsored = self.extract_text_safe(product, self.xpaths['sku_status_sponsored']['xpath'])

                sku_status = None
                if rollback:
                    sku_status = "Rollback"
                elif sponsored:
                    sku_status = "Sponsored"

                # Extract Retailer_Membership_Discounts
                membership_discount_elem = self.extract_text_safe(product, self.xpaths['membership_discount']['xpath'])
                membership_discount = "Walmart Plus" if membership_discount_elem else None

                # Extract Available_Quantity_for_Purchase (numbers only: "only 1 left" -> "1")
                available_quantity_raw = self.extract_text_safe(product, self.xpaths['available_quantity']['xpath'], self.xpaths['available_quantity'].get('fallback_xpath'))
                available_quantity = self.extract_number_only(available_quantity_raw) if available_quantity_raw else None

                # Extract Inventory_Status
                inventory_status = self.extract_text_safe(product, self.xpaths['inventory_status']['xpath'], self.xpaths['inventory_status'].get('fallback_xpath'))

                data = {
                    'page_type': 'main',
                    'Retailer_SKU_Name': product_name,
                    'Final_SKU_Price': final_price,
                    'Original_SKU_Price': original_price,
                    'Offer': offer,
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
                    print(f"  [{idx}/{len(products)}] Collected: {data['Retailer_SKU_Name'][:50]}...")

            print(f"[PAGE {page_number}] Collected {collected_count} products (Total: {self.total_collected}/{self.max_skus})")
            return True

        except Exception as e:
            print(f"[ERROR] Failed to scrape page {page_number}: {e}")
            import traceback
            traceback.print_exc()
            return True  # Continue to next page

    def normalize_product_url(self, raw_url):
        """Normalize product URL to clean format"""
        if not raw_url:
            return None

        # Type 1: Tracking URL (/sp/track?...rd=encoded_url)
        if '/sp/track?' in raw_url:
            try:
                parsed = urlparse(raw_url)
                query_params = parse_qs(parsed.query)

                # Extract 'rd' parameter (redirect URL)
                if 'rd' in query_params:
                    redirect_url = query_params['rd'][0]
                    # Decode URL-encoded string
                    decoded_url = unquote(redirect_url)

                    # Extract clean /ip/... path from decoded URL
                    if '/ip/' in decoded_url:
                        ip_path = decoded_url.split('/ip/')[1]
                        # Remove extra parameters after product ID
                        clean_path = '/ip/' + ip_path.split('?')[0]
                        return f"https://www.walmart.com{clean_path}"
            except Exception as e:
                pass  # Fall through to Type 2 handling

        # Type 2: Relative path (/ip/...)
        if raw_url.startswith('/ip/'):
            # Remove query parameters after product ID
            clean_path = raw_url.split('?')[0]
            return f"https://www.walmart.com{clean_path}"

        # Type 3: Already full URL
        if raw_url.startswith('http'):
            # Clean up query parameters if needed
            if '/ip/' in raw_url:
                base_url = raw_url.split('?')[0]
                return base_url
            return raw_url

        return raw_url

    def clean_price_text(self, price_text):
        """Extract clean price from complex price HTML text"""
        if not price_text:
            return None

        # Remove extra whitespace and newlines
        price_text = ' '.join(price_text.split())

        # Try to extract price pattern like "$1,797 99" or "$238 00"
        # Look for dollar sign followed by numbers
        match = re.search(r'\$\s*(\d[\d,]*)\s*(\d{2})', price_text)
        if match:
            dollars = match.group(1).replace(',', '')
            cents = match.group(2)
            return f"${dollars}.{cents}"

        # Fallback: just return cleaned text
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
        """Save collected data with collection order (1-300)"""
        try:
            cursor = self.db_conn.cursor()

            # Check for duplicate product_url in the same batch
            product_url = data.get('Product_url')
            if product_url:
                cursor.execute("""
                    SELECT COUNT(*) FROM wmart_tv_main_2
                    WHERE batch_id = %s AND Product_url = %s
                """, (self.batch_id, product_url))

                count = cursor.fetchone()[0]

                if count > 0:
                    cursor.close()
                    print(f"  [SKIP] Duplicate URL already saved in this batch")
                    return False

            # Use sequential_id (1-300) for collection order (main_rank)
            main_rank = self.sequential_id

            # Calculate calendar week
            calendar_week = f"w{datetime.now().isocalendar().week}"

            # Calculate crawl_strdatetime (format: 202511051100000000)
            now = datetime.now()
            crawl_strdatetime = now.strftime('%Y%m%d%H%M%S') + '0000'

            account_name = self.config.get_constant('account_name', default='Walmart')
            cursor.execute("""
                INSERT INTO wmart_tv_main_2
                (account_name, main_rank, page_type, Retailer_SKU_Name, Final_SKU_Price, Original_SKU_Price,
                 Offer, Pick_Up_Availability, Shipping_Availability, Delivery_Availability,
                 SKU_Status, Retailer_Membership_Discounts, Available_Quantity_for_Purchase,
                 Inventory_Status, Product_url, batch_id, calendar_week, crawl_strdatetime)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                RETURNING id
            """, (
                account_name,
                main_rank,
                data['page_type'],
                data['Retailer_SKU_Name'],
                data['Final_SKU_Price'],
                data['Original_SKU_Price'],
                data['Offer'],
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
                crawl_strdatetime  # New field
            ))

            result = cursor.fetchone()

            if result:
                # Increment sequential ID for next product
                self.sequential_id += 1

            self.db_conn.commit()
            cursor.close()

            return result is not None

        except Exception as e:
            print(f"[ERROR] Failed to save to DB: {e}")
            return False

    def initialize_session(self):
        """Initialize session with natural browsing pattern (simplified for Selenium)"""
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

            # Check for robot detection and handle CAPTCHA
            if self.check_robot_page(self.page.html):
                print("[WARNING] Robot detection on homepage. Handling CAPTCHA...")
                self.handle_captcha()
                captcha_after_wait = self.config.get_timing_range('captcha_after_wait') or (3, 5)
                time.sleep(random.uniform(*captcha_after_wait))

                # If still showing robot detection, try recovery behavior
                if self.check_robot_page(self.page.html):
                    print("[WARNING] Still showing robot detection, trying recovery...")

                    # Slow scroll down
                    print("[INFO] Scrolling slowly...")
                    scroll_between_wait = self.config.get_timing_range('scroll_between_wait') or (1.5, 2.5)
                    for i in range(5):
                        scroll_amount = random.randint(*random_scroll_range)
                        self.page.run_js(f"window.scrollBy(0, {scroll_amount})")
                        time.sleep(random.uniform(*scroll_between_wait))

                    # Scroll back up a bit
                    self.page.run_js("window.scrollBy(0, -200)")
                    time.sleep(random.uniform(*scroll_wait_range))

                # Wait longer
                print(f"[INFO] Waiting {int(robot_recovery_wait)} seconds...")
                time.sleep(robot_recovery_wait)

                # Try reload
                print("[INFO] Reloading page...")
                self.page.refresh()
                time.sleep(random.uniform(*browse_wait))

                # Check again
                if self.check_robot_page(self.page.html):
                    print("[ERROR] Still getting robot detection after recovery")
                    print("[INFO] Attempting to continue anyway...")

            # Simple homepage exploration
            print("[INFO] Exploring homepage...")
            for _ in range(random.randint(2, 4)):
                scroll_amount = random.randint(*recovery_scroll_range)
                self.page.run_js(f"window.scrollBy(0, {scroll_amount})")
                time.sleep(random.uniform(*scroll_wait_range))

            self.page.run_js("window.scrollTo(0, 0)")
            time.sleep(random.uniform(*scroll_wait_range) + 1)

            print("[OK] Session initialized")
            return True

        except Exception as e:
            print(f"[ERROR] Failed to initialize session: {e}")
            import traceback
            traceback.print_exc()
            return False

    def run(self):
        """Main execution"""
        try:
            # Get page range from config
            page_start, page_end = self.config.get_page_range('wmart_tv_main2')

            print("="*80)
            print(f"Walmart TV Crawler [Part 2: Pages {page_start}-{page_end}] - Starting (undetected-chromedriver)")
            print(f"Batch ID: {self.batch_id}")
            print("="*80)

            # Connect to database
            if not self.connect_db():
                return

            # Load XPaths and URLs
            if not self.load_xpaths():
                return

            # Load excluded URLs (is_product=false)
            self.load_excluded_urls()

            page_urls = self.load_page_urls()
            if not page_urls:
                print("[ERROR] No page URLs found")
                return

            # Only process pages based on config (page_start to page_end)
            page_urls = page_urls[page_start-1:page_end]
            print(f"[INFO] Processing pages {page_start}-{page_end} only ({len(page_urls)} pages)")

            # Setup browser
            if not self.setup_browser():
                return

            # Get timing and retry config values
            max_page_retries = self.config.get_retry('max_page_retries', 'wmart_tv_main2', default=2)
            retry_wait = self.config.get_timing_range('retry_wait') or (10, 15)
            between_pages = self.config.get_timing_range('between_pages', 'wmart_tv_main2') or (8, 12)

            # Scrape each page with retry logic
            is_first_page = True  # Flag for browse page warmup on first page
            for page_number, url in page_urls:
                if self.total_collected >= self.max_skus:
                    break

                page_success = False

                for retry_attempt in range(max_page_retries + 1):
                    try:
                        if retry_attempt > 0:
                            print(f"\n[RETRY] Attempting page {page_number} again (attempt {retry_attempt + 1}/{max_page_retries + 1})")
                            time.sleep(random.uniform(*retry_wait))

                        # Only do warmup on first attempt of first page
                        do_warmup = is_first_page and retry_attempt == 0
                        if self.scrape_page(url, page_number, retry_count=0, is_first_page=do_warmup):
                            page_success = True
                            is_first_page = False  # Only first page gets warmup
                            break
                        else:
                            # scrape_page returned False (robot detection failed)
                            if retry_attempt < max_page_retries:
                                print(f"[WARNING] Page {page_number} failed, will retry...")
                            else:
                                print(f"[ERROR] Page {page_number} failed after {max_page_retries + 1} attempts, skipping...")

                    except Exception as e:
                        print(f"[ERROR] Exception on page {page_number}: {e}")
                        if retry_attempt < max_page_retries:
                            print(f"[INFO] Will retry page {page_number}...")
                        else:
                            print(f"[ERROR] Page {page_number} failed after {max_page_retries + 1} attempts, skipping...")
                            import traceback
                            traceback.print_exc()

                # Continue to next page even if this one failed
                if not page_success:
                    print(f"[INFO] Continuing to next page...")

                # Random delay between pages
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
                import json
                result_dir = r"C:\samsung_dx_retail_com\stage_results"
                os.makedirs(result_dir, exist_ok=True)
                with open(os.path.join(result_dir, "wmart_tv_main2.json"), "w") as f:
                    json.dump({"collected_count": self.total_collected}, f)
            except Exception as e:
                print(f"[WARNING] Failed to write result JSON: {e}")

            if self.page:
                try:
                    self.page.quit()
                except:
                    pass
            if self.db_conn:
                try:
                    self.db_conn.close()
                except:
                    pass


if __name__ == "__main__":
    # 로그 파일 설정 (실행 시작 일시)
    os.makedirs("C:\\samsung_dx_retail_com\\log", exist_ok=True)
    log_filename = "C:\\samsung_dx_retail_com\\log\\" + datetime.now().strftime("%Y%m%d_%H%M%S") + "_main2.txt"
    tee = Tee(log_filename)
    sys.stdout = tee

    try:
        crawler = WalmartTVCrawler()

        # --max-skus 인자 처리 (오케스트레이터에서 main1+main2 합산 300 제한용)
        import argparse
        parser = argparse.ArgumentParser()
        parser.add_argument('--max-skus', type=int, default=None)
        args, _ = parser.parse_known_args()
        if args.max_skus is not None:
            crawler.max_skus = args.max_skus
            print(f"[INFO] max_skus overridden to {args.max_skus} (from orchestrator)")

        crawler.run()
    except Exception as e:
        print(f"\n[FATAL ERROR] {e}")
        import traceback
        traceback.print_exc()

    print("\n[INFO] Crawler completed. Window will close automatically...")
    print(f"[INFO] Log saved to: {log_filename}")

    # 로그 파일 닫기
    sys.stdout = tee.terminal
    tee.close()
