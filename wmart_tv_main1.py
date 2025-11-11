import time
import random
import psycopg2
from datetime import datetime
import undetected_chromedriver as uc
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from lxml import html
import re
import os
import json
from urllib.parse import urlparse, parse_qs, unquote

# Import database configuration
from config import DB_CONFIG

class WalmartTVCrawler:
    def __init__(self):
        self.driver = None
        self.wait = None
        self.db_conn = None
        self.xpaths = {}
        self.total_collected = 0
        self.max_skus = 300
        self.sequential_id = 1  # ID counter for 1-300
        self.batch_id = int(time.time())  # Batch ID for this session

    def connect_db(self):
        """Connect to PostgreSQL database"""
        try:
            self.db_conn = psycopg2.connect(**DB_CONFIG)
            print("[OK] Database connected")
            return True
        except Exception as e:
            print(f"[ERROR] Database connection failed: {e}")
            return False

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
        """Setup undetected-chromedriver browser"""
        try:
            options = uc.ChromeOptions()
            options.add_argument('--disable-blink-features=AutomationControlled')
            options.add_argument('--disable-dev-shm-usage')
            options.add_argument('--no-sandbox')
            options.add_argument('--disable-setuid-sandbox')
            options.add_argument('--start-maximized')
            options.add_argument('--disable-infobars')
            options.add_argument('--window-size=1920,1080')

            self.driver = uc.Chrome(options=options, use_subprocess=True)
            self.wait = WebDriverWait(self.driver, 20)
            print("[OK] Browser setup complete (undetected-chromedriver)")
            return True
        except Exception as e:
            print(f"[ERROR] Browser setup failed: {e}")
            return False

    def extract_text_safe(self, element, xpath):
        """Safely extract text from element using xpath"""
        try:
            result = element.xpath(xpath)
            if result:
                # Handle attribute extraction (e.g., @href)
                if isinstance(result[0], str):
                    return result[0].strip()
                # Handle element extraction
                else:
                    return result[0].text_content().strip()
            return None
        except Exception as e:
            return None

    def check_robot_page(self, page_source):
        """Check if page is showing 'Robot or human?' challenge"""
        if "Robot or human?" in page_source or "Enter the characters you see below" in page_source:
            return True
        return False

    def handle_captcha(self):
        """Handle 'PRESS & HOLD' CAPTCHA if present"""
        try:
            print("[INFO] Checking for CAPTCHA...")

            # Multiple selectors to find CAPTCHA button
            captcha_selectors = [
                'text=PRESS & HOLD',
                'text="PRESS & HOLD"',
                'text=/PRESS.*HOLD/i',
                'button:has-text("PRESS")',
                'button:has-text("HOLD")',
                '[class*="captcha"]',
                '[id*="captcha"]',
                '[class*="PressHold"]',
                '[class*="press-hold"]'
            ]

            button = None
            found_selector = None

            # Try each selector
            for selector in captcha_selectors:
                try:
                    temp_button = self.page.locator(selector).first
                    if temp_button.is_visible(timeout=2000):
                        button = temp_button
                        found_selector = selector
                        print(f"[OK] CAPTCHA detected with selector: {selector}")
                        break
                except:
                    continue

            # If no button found with locators, check page content
            if not button:
                page_content = self.driver.page_source.lower()
                if any(keyword in page_content for keyword in ['press & hold', 'press and hold', 'captcha', 'human verification']):
                    print("[WARNING] CAPTCHA keywords found in page but button not located")
                    print("[INFO] Page may require manual intervention")
                    # Save screenshot for debugging
                    try:
                        self.page.screenshot(path=f"captcha_screen_{int(time.time())}.png")
                        print("[INFO] Screenshot saved for debugging")
                    except:
                        pass

                    # Pause for manual intervention
                    print("[INFO] Please solve CAPTCHA manually if present...")
                    print("[INFO] Waiting 30 seconds...")
                    time.sleep(30)
                    return True
                else:
                    print("[INFO] No CAPTCHA detected")
                    return True

            # Try to solve CAPTCHA automatically
            print("[OK] Attempting to solve CAPTCHA automatically...")

            # Get button position
            box = button.bounding_box()
            if box:
                # Move mouse to button center
                center_x = box['x'] + box['width'] / 2
                center_y = box['y'] + box['height'] / 2

                self.page.mouse.move(center_x, center_y)
                time.sleep(random.uniform(0.3, 0.6))

                # Press and hold with progressive retry strategy
                hold_times = [
                    random.uniform(7, 9),    # 1st attempt: 7-9 seconds
                    10,                      # 2nd attempt: 10 seconds
                    20                       # 3rd attempt: 20 seconds
                ]

                for attempt, hold_time in enumerate(hold_times, 1):
                    # Move to button center (refresh position each time)
                    box = button.bounding_box()
                    if box:
                        center_x = box['x'] + box['width'] / 2
                        center_y = box['y'] + box['height'] / 2
                        self.page.mouse.move(center_x, center_y)
                        time.sleep(random.uniform(0.3, 0.6))

                    # Press and hold
                    self.page.mouse.down()
                    print(f"[INFO] Holding button (attempt {attempt}/3)...")
                    print(f"[INFO] Holding for {hold_time:.1f} seconds...")
                    time.sleep(hold_time)
                    self.page.mouse.up()

                    print(f"[OK] CAPTCHA button released (attempt {attempt}/3)")
                    time.sleep(random.uniform(3, 5))  # Wait for verification

                    # Check if CAPTCHA was solved
                    try:
                        if not button.is_visible(timeout=3000):
                            print(f"[OK] CAPTCHA solved successfully on attempt {attempt}")
                            return True
                        else:
                            if attempt < 3:
                                print(f"[WARNING] CAPTCHA still visible, trying again with longer hold...")
                            else:
                                print(f"[WARNING] CAPTCHA still visible after {attempt} attempts")
                    except:
                        # If button is gone (exception), consider it solved
                        print(f"[OK] CAPTCHA appears to be solved on attempt {attempt}")
                        return True

                # All 3 attempts failed, wait for manual intervention
                print("[INFO] Automatic solving failed after 3 attempts, please solve manually...")
                print("[INFO] Waiting 60 seconds for manual intervention...")
                time.sleep(60)

                # Check again after manual intervention time
                try:
                    if not button.is_visible(timeout=2000):
                        print("[OK] CAPTCHA solved (likely manually)")
                        return True
                    else:
                        print("[WARNING] CAPTCHA still present after manual wait")
                        return False
                except:
                    print("[OK] CAPTCHA appears to be solved")
                    return True
            else:
                print("[WARNING] Could not get button position")
                return False

        except Exception as e:
            print(f"[WARNING] CAPTCHA check failed: {e}")
            return True  # Continue anyway

    def scrape_page(self, url, page_number, retry_count=0):
        """Scrape a single page"""
        max_retries = 2

        try:
            print(f"\n[PAGE {page_number}] Accessing: {url[:80]}...")

            # For page 1, navigate naturally through browse page
            if page_number == 1 and retry_count == 0:
                print("[INFO] Navigating to Walmart browse page first...")
                try:
                    # Try browse electronics category first
                    self.driver.get("https://www.walmart.com/browse/electronics/tvs/3944_1060825", wait_until="domcontentloaded", timeout=90000)
                    time.sleep(random.uniform(10, 15))

                    # Check for robot detection and handle CAPTCHA if needed
                    if self.check_robot_page(self.driver.page_source):
                        print("[WARNING] Robot detected on browse page, handling CAPTCHA...")
                        self.handle_captcha()
                        time.sleep(random.uniform(2, 4))

                    # If no robot detection (or after handling CAPTCHA)
                    if not self.check_robot_page(self.driver.page_source):
                        print("[OK] Browse page loaded successfully")
                        # Add human-like behavior
                        self.add_random_mouse_movements()
                        time.sleep(random.uniform(2, 4))

                        # Scroll a bit
                        for _ in range(2):
                            self.page.evaluate("window.scrollBy(0, 400)")
                            time.sleep(random.uniform(1, 2))

                        # Now access the search URL directly
                        print("[INFO] Now navigating to search page...")
                        self.driver.get(url, wait_until="domcontentloaded", timeout=90000)
                        time.sleep(random.uniform(8, 12))
                    else:
                        print("[WARNING] Robot still detected after CAPTCHA, using direct URL...")
                        self.driver.get(url, wait_until="domcontentloaded", timeout=90000)
                        time.sleep(random.uniform(12, 18))
                except Exception as e:
                    print(f"[WARNING] Browse navigation failed: {e}, using direct URL...")
                    self.driver.get(url, wait_until="domcontentloaded", timeout=90000)
                    time.sleep(random.uniform(12, 18))
            else:
                self.driver.get(url, wait_until="domcontentloaded", timeout=90000)
                time.sleep(random.uniform(12, 18))

            # Check for robot detection and handle CAPTCHA
            page_source = None
            try:
                page_source = self.driver.page_source
            except Exception as e:
                if "navigating" in str(e).lower():
                    print(f"[WARNING] Page still navigating (likely bot detection)")
                    print(f"[INFO] Please solve CAPTCHA manually if needed...")
                    print(f"[INFO] Waiting 60 seconds for manual intervention...")
                    time.sleep(60)

                    # Try to get content again
                    try:
                        page_source = self.driver.page_source
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
                    time.sleep(random.uniform(3, 5))
                    page_source = self.driver.page_source

                    # Check if robot detection is gone
                    if not self.check_robot_page(page_source):
                        print("[OK] Robot detection bypassed after CAPTCHA")
                        # Continue with scraping (fall through)
                    else:
                        print("[WARNING] Robot detection still present after CAPTCHA")

                # If still robot detected, retry
                if self.check_robot_page(self.driver.page_source):
                    if retry_count < max_retries:
                        print(f"[WARNING] Retrying... {retry_count + 1}/{max_retries}")
                        wait_time = 30 + retry_count * 15
                        print(f"[INFO] Waiting {wait_time} seconds before retry...")
                        time.sleep(wait_time)

                        print("[INFO] Refreshing page...")
                        self.page.reload(wait_until="domcontentloaded", timeout=90000)
                        time.sleep(random.uniform(10, 15))

                        return self.scrape_page(url, page_number, retry_count + 1)
                    else:
                        print(f"[ERROR] Failed to bypass robot detection after {max_retries} retries")
                        print("[INFO] Saving page source for debugging...")
                        with open(f'walmart_robot_page_{page_number}.html', 'w', encoding='utf-8') as f:
                            f.write(page_source)
                        return False

            # Wait for page to load
            print("[INFO] Waiting for products to load...")
            time.sleep(random.uniform(5, 8))

            # Scroll to load all products
            print("[INFO] Scrolling to load all products...")
            last_height = self.page.evaluate("document.body.scrollHeight")

            for scroll_round in range(2):
                self.page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                time.sleep(3)

                new_height = self.page.evaluate("document.body.scrollHeight")
                if new_height == last_height:
                    break
                last_height = new_height

            # Scroll back to top
            self.page.evaluate("window.scrollTo(0, 0)")
            time.sleep(2)

            # Get page source and parse with lxml
            page_source = self.driver.page_source
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

                # Final_SKU_Price and Original_SKU_Price will be collected by wmart_tv_dt1.py
                # Not collecting prices in main crawler
                final_price = None
                original_price = None

                # Extract Offer (numbers only: "4 free offers from Apple" -> "4")
                offer_raw = self.extract_text_safe(product, self.xpaths['offer']['xpath'])
                offer = self.extract_number_only(offer_raw) if offer_raw else None

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
                available_quantity_raw = self.extract_text_safe(product, self.xpaths['available_quantity']['xpath'])
                available_quantity = self.extract_number_only(available_quantity_raw) if available_quantity_raw else None

                # Extract Inventory_Status
                inventory_status = self.extract_text_safe(product, self.xpaths['inventory_status']['xpath'])

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
                    SELECT COUNT(*) FROM wmart_tv_main_1
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

            cursor.execute("""
                INSERT INTO wmart_tv_main_1
                (account_name, main_rank, page_type, Retailer_SKU_Name, Final_SKU_Price, Original_SKU_Price,
                 Offer, Pick_Up_Availability, Shipping_Availability, Delivery_Availability,
                 SKU_Status, Retailer_Membership_Discounts, Available_Quantity_for_Purchase,
                 Inventory_Status, Product_url, batch_id, calendar_week, crawl_strdatetime)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                RETURNING id
            """, (
                'Walmart',  # account_name
                main_rank,  # Changed from "order" to main_rank
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
        """Initialize session with natural browsing pattern"""
        try:
            print("[INFO] Initializing session - navigating to Walmart homepage...")

            # Navigate directly to Walmart homepage
            self.page.goto("https://www.walmart.com", wait_until="domcontentloaded")
            time.sleep(random.uniform(8, 12))

            # Add mouse movements
            self.add_random_mouse_movements()
            time.sleep(random.uniform(1, 2))

            # Check for robot detection and handle CAPTCHA
            if self.check_robot_page(self.driver.page_source):
                print("[WARNING] Robot detection on homepage. Handling CAPTCHA...")

                # Try CAPTCHA first
                self.handle_captcha()
                time.sleep(random.uniform(3, 5))

                # If still showing robot detection, try recovery behavior
                if self.check_robot_page(self.driver.page_source):
                    print("[WARNING] Still showing robot detection, trying recovery...")

                    # More natural recovery behavior
                    print("[INFO] Simulating human confusion - random mouse movements...")
                    for _ in range(5):
                        self.add_random_mouse_movements()
                        time.sleep(random.uniform(0.8, 1.5))

                    # Slow scroll down
                    print("[INFO] Scrolling slowly...")
                    for i in range(5):
                        scroll_amount = random.randint(150, 300)
                        self.page.evaluate(f"window.scrollBy(0, {scroll_amount})")
                        time.sleep(random.uniform(1.5, 2.5))

                    # Scroll back up a bit
                    self.page.evaluate("window.scrollBy(0, -200)")
                    time.sleep(random.uniform(1, 2))

                # Wait longer
                print("[INFO] Waiting 30 seconds...")
                time.sleep(30)

                # Try reload
                print("[INFO] Reloading page...")
                self.page.reload(wait_until="domcontentloaded")
                time.sleep(random.uniform(10, 15))

                # Check again
                if self.check_robot_page(self.driver.page_source):
                    print("[ERROR] Still getting robot detection after recovery")
                    print("[INFO] Attempting to continue anyway...")
                    # Don't return False, try to continue

            # More human-like exploration
            print("[INFO] Exploring homepage...")
            self.add_random_mouse_movements()
            time.sleep(random.uniform(2, 4))

            # Random scrolling
            for _ in range(random.randint(2, 4)):
                scroll_amount = random.randint(200, 500)
                self.page.evaluate(f"window.scrollBy(0, {scroll_amount})")
                time.sleep(random.uniform(1, 2))
                self.add_random_mouse_movements()

            # Scroll back to top to see search box
            print("[INFO] Scrolling back to top...")
            self.page.evaluate("window.scrollTo(0, 0)")
            time.sleep(random.uniform(2, 3))

            # Try to search for TV from homepage
            print("[INFO] Searching for 'TV' from homepage...")
            try:
                # Try multiple selectors for Walmart search box
                search_box = None
                selectors = [
                    "input[type='search']",
                    "input[aria-label*='Search']",
                    "input[placeholder*='Search']",
                    "input[name='q']"
                ]

                for selector in selectors:
                    try:
                        print(f"[DEBUG] Trying search box selector: {selector}")
                        search_box = self.page.wait_for_selector(selector, timeout=5000)
                        if search_box:
                            print(f"[OK] Found search box with: {selector}")
                            break
                    except:
                        continue

                if search_box:
                    # Add random mouse movements before clicking
                    print("[INFO] Moving mouse naturally...")
                    self.page.mouse.move(random.randint(100, 300), random.randint(100, 300))
                    time.sleep(random.uniform(0.5, 1))
                    self.page.mouse.move(random.randint(400, 600), random.randint(200, 400))
                    time.sleep(random.uniform(0.3, 0.7))

                    # Click on search box
                    print("[INFO] Clicking search box...")
                    search_box.click()
                    time.sleep(random.uniform(2, 3))  # Increased wait time

                    # Type "TV" character by character with longer delay
                    print("[INFO] Typing 'TV' in search box...")
                    for char in "TV":
                        search_box.type(char, delay=random.uniform(400, 800))  # Increased typing delay

                    # Wait for search suggestions to appear
                    time.sleep(random.uniform(3, 5))  # Increased wait time

                    # Try to click search button instead of pressing Enter (more human-like)
                    print("[INFO] Looking for search button...")
                    try:
                        search_button = self.page.query_selector("button[aria-label='Search']")
                        if not search_button:
                            search_button = self.page.query_selector("button[data-automation-id='searchButton']")

                        if search_button:
                            print("[INFO] Clicking search button...")
                            search_button.click()
                        else:
                            print("[INFO] Search button not found, pressing Enter...")
                            search_box.press("Enter")
                    except:
                        print("[INFO] Pressing Enter to search...")
                        search_box.press("Enter")

                    # Wait longer for page load
                    print("[INFO] Waiting for search results...")
                    self.page.wait_for_load_state("domcontentloaded")
                    time.sleep(random.uniform(8, 12))  # Increased wait time

                    # Add natural scrolling behavior after search
                    print("[INFO] Scrolling naturally...")
                    for _ in range(2):
                        self.page.mouse.wheel(0, random.randint(200, 400))
                        time.sleep(random.uniform(1, 2))

                    print("[OK] Successfully searched for TV")
                else:
                    print("[WARNING] Could not find search box, will try alternative method later")

            except Exception as e:
                print(f"[WARNING] Failed to search from homepage: {e}")
                print("[INFO] Will try alternative navigation method later")

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
            print("="*80)
            print("Walmart TV Crawler [Part 1: Pages 1-5] - Starting (Playwright Mode)")
            print(f"Batch ID: {self.batch_id}")
            print("="*80)

            # Connect to database
            if not self.connect_db():
                return

            # Load XPaths and URLs
            if not self.load_xpaths():
                return

            page_urls = self.load_page_urls()
            if not page_urls:
                print("[ERROR] No page URLs found")
                return

            # Only process pages 1-5
            page_urls = page_urls[:5]
            print(f"[INFO] Processing pages 1-5 only ({len(page_urls)} pages)")

            # Setup Playwright
            if not self.setup_browser():
                return

            # Scrape each page with retry logic
            for page_number, url in page_urls:
                if self.total_collected >= self.max_skus:
                    break

                # Retry failed pages up to 2 times
                max_page_retries = 2
                page_success = False

                for retry_attempt in range(max_page_retries + 1):
                    try:
                        if retry_attempt > 0:
                            print(f"\n[RETRY] Attempting page {page_number} again (attempt {retry_attempt + 1}/{max_page_retries + 1})")
                            time.sleep(random.uniform(10, 15))

                        if self.scrape_page(url, page_number):
                            page_success = True
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
                time.sleep(random.uniform(8, 12))

            print("\n" + "="*80)
            print(f"Crawling completed! Total collected: {self.total_collected} SKUs")
            print("="*80)

        except Exception as e:
            print(f"[ERROR] Crawler failed: {e}")
            import traceback
            traceback.print_exc()

        finally:
            if self.driver:
                try:
                    self.driver.quit()
                except:
                    pass
            if self.db_conn:
                try:
                    self.db_conn.close()
                except:
                    pass


if __name__ == "__main__":
    try:
        crawler = WalmartTVCrawler()
        crawler.run()
    except Exception as e:
        print(f"\n[FATAL ERROR] {e}")
        import traceback
        traceback.print_exc()

    print("\n[INFO] Crawler completed. Window will close automatically...")
