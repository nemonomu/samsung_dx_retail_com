import time
import random
import psycopg2
import undetected_chromedriver as uc
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from lxml import html
import re
from urllib.parse import urlparse, parse_qs, unquote

# Import database configuration
from config import DB_CONFIG

class WalmartTVBSRCrawler:
    def __init__(self):
        self.drivers = []  # Store all driver instances for manual verification
        self.db_conn = None
        self.xpaths = {}
        self.total_collected = 0
        self.max_skus = 100  # BSR 1-100
        self.sequential_id = 1  # ID counter for 1-100

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
                WHERE mall_name = 'Walmart' AND page_type = 'bsr' AND is_active = TRUE
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

    def create_new_driver(self):
        """Create a new Chrome WebDriver instance for each page"""
        options = uc.ChromeOptions()

        # Basic options
        options.add_argument('--disable-dev-shm-usage')
        options.add_argument('--no-sandbox')
        options.add_argument('--disable-gpu')
        options.add_argument('--window-size=1920,1080')
        options.add_argument('--lang=en-US,en;q=0.9')

        # Preferences
        prefs = {
            "profile.default_content_setting_values.notifications": 2,
            "credentials_enable_service": False,
            "profile.password_manager_enabled": False,
        }
        options.add_experimental_option("prefs", prefs)

        # Use undetected_chromedriver (auto-detect Chrome version)
        driver = uc.Chrome(options=options)
        driver.set_page_load_timeout(60)

        print("[OK] New Chrome instance created")
        return driver

    def add_random_mouse_movements(self, driver):
        """Add random mouse movements to appear more human"""
        try:
            from selenium.webdriver.common.action_chains import ActionChains
            actions = ActionChains(driver)

            # Random small movements
            for _ in range(random.randint(2, 4)):
                x_offset = random.randint(-100, 100)
                y_offset = random.randint(-100, 100)
                actions.move_by_offset(x_offset, y_offset)
                actions.pause(random.uniform(0.1, 0.3))

            actions.perform()
        except Exception as e:
            pass  # Silent fail if mouse movement doesn't work

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

    def scrape_page(self, driver, wait, url, page_number, retry_count=0):
        """Scrape a single page"""
        max_retries = 2

        try:
            print(f"\n[PAGE {page_number}] Accessing: {url[:80]}...")

            # Directly access the best_seller URL
            driver.get(url)
            time.sleep(random.uniform(12, 18))

            # Check for robot detection
            page_source = driver.page_source
            if self.check_robot_page(page_source):
                if retry_count < max_retries:
                    print(f"[WARNING] Robot detection page detected. Retry {retry_count + 1}/{max_retries}...")
                    wait_time = 30 + retry_count * 15
                    print(f"[INFO] Waiting {wait_time} seconds before retry...")
                    time.sleep(wait_time)

                    print("[INFO] Refreshing page...")
                    driver.refresh()
                    time.sleep(random.uniform(10, 15))

                    return self.scrape_page(driver, wait, url, page_number, retry_count + 1)
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
            last_height = driver.execute_script("return document.body.scrollHeight")

            for scroll_round in range(2):
                driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                time.sleep(3)

                new_height = driver.execute_script("return document.body.scrollHeight")
                if new_height == last_height:
                    break
                last_height = new_height

            # Scroll back to top
            driver.execute_script("window.scrollTo(0, 0);")
            time.sleep(2)

            # Get page source and parse with lxml
            page_source = driver.page_source
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

                # Extract Final_SKU_Price
                final_price_raw = self.extract_text_safe(product, self.xpaths['final_price']['xpath'])
                final_price = self.clean_price_text(final_price_raw) if final_price_raw else None

                # Extract Original_SKU_Price
                original_price_raw = self.extract_text_safe(product, self.xpaths['original_price']['xpath'])
                original_price = original_price_raw if original_price_raw else None

                # Extract Offer
                offer = self.extract_text_safe(product, self.xpaths['offer']['xpath'])

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

                # Extract Available_Quantity_for_Purchase
                available_quantity = self.extract_text_safe(product, self.xpaths['available_quantity']['xpath'])

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
                    'Rank': None,  # To be added later
                    'Product_url': product_url
                }

                # Save to database
                if self.save_to_db(data):
                    collected_count += 1
                    self.total_collected += 1
                    print(f"  [{idx}/{len(products)}] Collected: {data['Retailer_SKU_Name'][:50]}... | Price: {final_price or 'N/A'}")

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

    def save_to_db(self, data):
        """Save collected data with collection order (1-300)"""
        try:
            cursor = self.db_conn.cursor()

            # Use sequential_id (1-300) for collection order
            collection_order = self.sequential_id

            cursor.execute("""
                INSERT INTO wmart_tv_bsr_crawl
                ("order", page_type, Retailer_SKU_Name, Final_SKU_Price, Original_SKU_Price,
                 Offer, Pick_Up_Availability, Shipping_Availability, Delivery_Availability,
                 SKU_Status, Retailer_Membership_Discounts, Available_Quantity_for_Purchase,
                 Inventory_Status, Rank, Product_url)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                RETURNING id
            """, (
                collection_order,
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
                data['Rank'],
                data['Product_url']
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

    def initialize_session(self, driver):
        """Initialize session by visiting Walmart homepage first"""
        try:
            print("[INFO] Initializing session - visiting Walmart homepage...")
            driver.get("https://www.walmart.com")
            time.sleep(random.uniform(8, 12))

            # Add random mouse movements
            self.add_random_mouse_movements(driver)
            time.sleep(random.uniform(1, 3))

            # Check if we got the robot page on homepage
            if self.check_robot_page(driver.page_source):
                print("[WARNING] Robot detection on homepage. Trying recovery...")

                # Try scrolling
                for _ in range(3):
                    driver.execute_script("window.scrollBy(0, 300);")
                    time.sleep(random.uniform(0.5, 1))

                time.sleep(20)

                # Add more mouse movements
                self.add_random_mouse_movements(driver)

                driver.refresh()
                time.sleep(random.uniform(10, 15))

                if self.check_robot_page(driver.page_source):
                    print("[ERROR] Cannot bypass robot detection on homepage")
                    return False

            print("[OK] Session initialized successfully")
            return True

        except Exception as e:
            print(f"[ERROR] Failed to initialize session: {e}")
            return False

    def run(self):
        """Main execution"""
        try:
            print("="*80)
            print("Walmart TV BSR Crawler - Pages will remain open for verification")
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

            print(f"\n[INFO] Will crawl {len(page_urls)} pages")
            print(f"[INFO] Each page will open in a new Chrome window")
            print(f"[INFO] All windows will remain open for verification")

            input("\nPress ENTER to start crawling...")

            # Process each page in a new browser window
            for page_number, url in page_urls:
                if self.total_collected >= self.max_skus:
                    print(f"[INFO] Reached maximum SKU limit ({self.max_skus}), stopping...")
                    break

                print(f"\n{'#'*80}")
                print(f"Opening new Chrome window for PAGE {page_number}")
                print(f"{'#'*80}")

                # Create new driver for this page
                driver = self.create_new_driver()
                wait = WebDriverWait(driver, 20)
                self.drivers.append(driver)

                # Initialize session (only prompt for first page)
                if page_number == 1:
                    print("\n[ACTION REQUIRED] Solve CAPTCHA on first window if needed")
                    if not self.initialize_session(driver):
                        print("[WARNING] Session initialization failed, proceeding anyway...")
                        print("[INFO] Will attempt direct access to search pages...")
                        time.sleep(random.uniform(5, 10))
                    input(f"\nPress ENTER when ready to crawl page {page_number}...")
                else:
                    self.initialize_session(driver)

                # Scrape this page
                if not self.scrape_page(driver, wait, url, page_number):
                    print(f"[WARNING] Failed to scrape page {page_number}")

                # Small delay between opening windows
                time.sleep(2)

            # All pages done
            print("\n" + "="*80)
            print(f"[INFO] Crawling complete!")
            print(f"[INFO] Total collected: {self.total_collected} SKUs")
            print(f"[INFO] {len(self.drivers)} Chrome windows are open")
            print(f"[INFO] Review each window to verify the collected products")
            print(f"[INFO] Press ENTER to close all browsers and exit...")
            print("="*80 + "\n")
            input()

        except Exception as e:
            print(f"[ERROR] Crawler failed: {e}")
            import traceback
            traceback.print_exc()

        finally:
            # Close all drivers
            for driver in self.drivers:
                try:
                    driver.quit()
                except:
                    pass

            if self.db_conn:
                self.db_conn.close()


if __name__ == "__main__":
    try:
        crawler = WalmartTVBSRCrawler()
        crawler.run()
    except Exception as e:
        print(f"\n[FATAL ERROR] {e}")
        import traceback
        traceback.print_exc()

    print("\n[INFO] BSR Crawler completed. Window will close automatically...")
