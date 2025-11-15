import time
import random
import re
import sys
import psycopg2
from datetime import datetime
import pytz
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager
from lxml import html

# Configure stdout encoding for Windows console
if sys.stdout.encoding != 'utf-8':
    sys.stdout.reconfigure(encoding='utf-8', errors='replace')

# Import database configuration
from config import DB_CONFIG

class AmazonBSRCrawler:
    def __init__(self):
        self.driver = None
        self.wait = None
        self.db_conn = None
        self.xpaths = {}
        self.total_collected = 0
        self.error_messages = []
        self.batch_id = None  # Batch ID for this crawling session

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
        """Load XPath selectors for BSR page from database"""
        try:
            cursor = self.db_conn.cursor()
            cursor.execute("""
                SELECT data_field, xpath, css_selector
                FROM xpath_selectors
                WHERE mall_name = 'Amazon' AND page_type = 'bsr_page' AND is_active = TRUE
            """)

            for row in cursor.fetchall():
                self.xpaths[row[0]] = {
                    'xpath': row[1],
                    'css': row[2]
                }

            cursor.close()
            print(f"[OK] Loaded {len(self.xpaths)} XPath selectors for BSR page")

            if len(self.xpaths) == 0:
                print("[WARNING] No XPath selectors found for BSR page!")
                print("Please add XPath selectors to xpath_selectors table with page_type='bsr_page'")
                return False

            return True

        except Exception as e:
            print(f"[ERROR] Failed to load XPaths: {e}")
            return False

    def load_page_urls(self):
        """Load BSR page URLs from database"""
        try:
            cursor = self.db_conn.cursor()
            cursor.execute("""
                SELECT page_number, url
                FROM amazon_tv_bsr_page_url
                WHERE is_active = TRUE
                ORDER BY page_number
            """)

            urls = cursor.fetchall()
            cursor.close()
            print(f"[OK] Loaded {len(urls)} BSR page URLs")
            return urls

        except Exception as e:
            print(f"[ERROR] Failed to load BSR page URLs: {e}")
            return []

    def setup_driver(self):
        """Setup Chrome WebDriver"""
        chrome_options = Options()
        chrome_options.add_argument('--disable-blink-features=AutomationControlled')
        chrome_options.add_argument('--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36')
        chrome_options.add_argument('--disable-dev-shm-usage')
        chrome_options.add_argument('--no-sandbox')
        chrome_options.add_argument('--window-size=1920,1080')
        chrome_options.add_argument('--start-maximized')
        chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
        chrome_options.add_experimental_option('useAutomationExtension', False)

        # Add more realistic browser settings
        chrome_options.add_argument('--disable-gpu')
        chrome_options.add_argument('--lang=en-US,en;q=0.9')
        chrome_options.add_argument('--disable-blink-features=AutomationControlled')

        # Add preferences to appear more like a real browser
        prefs = {
            "profile.default_content_setting_values.notifications": 2,
            "credentials_enable_service": False,
            "profile.password_manager_enabled": False
        }
        chrome_options.add_experimental_option("prefs", prefs)

        service = Service(ChromeDriverManager().install())
        self.driver = webdriver.Chrome(service=service, options=chrome_options)
        self.wait = WebDriverWait(self.driver, 20)

        # More comprehensive webdriver property masking
        self.driver.execute_cdp_cmd('Page.addScriptToEvaluateOnNewDocument', {
            'source': '''
                Object.defineProperty(navigator, 'webdriver', {
                    get: () => undefined
                });
                Object.defineProperty(navigator, 'plugins', {
                    get: () => [1, 2, 3, 4, 5]
                });
                Object.defineProperty(navigator, 'languages', {
                    get: () => ['en-US', 'en']
                });
                window.chrome = {
                    runtime: {}
                };
            '''
        })

        print("[OK] WebDriver setup complete")

    def scroll_to_load_all(self):
        """Scroll down to load all 50 items on BSR page"""
        try:
            print("[INFO] Scrolling to load all items...")

            # Scroll down multiple times to ensure all 50 items are loaded
            scroll_pause_time = 2

            # Get initial height
            last_height = self.driver.execute_script("return document.body.scrollHeight")

            # Scroll down progressively in multiple steps
            for i in range(5):  # Increase scroll attempts
                # Scroll down by percentage
                scroll_position = (i + 1) * 20  # 20%, 40%, 60%, 80%, 100%
                self.driver.execute_script(f"window.scrollTo(0, document.body.scrollHeight * {scroll_position / 100});")
                print(f"[DEBUG] Scrolled to {scroll_position}%")
                time.sleep(scroll_pause_time)

            # Scroll to absolute bottom
            for i in range(3):
                self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                time.sleep(scroll_pause_time)

                # Calculate new height
                new_height = self.driver.execute_script("return document.body.scrollHeight")
                print(f"[DEBUG] Page height: {new_height}")

                if new_height == last_height:
                    print(f"[DEBUG] No more content to load (attempt {i+1}/3)")
                    break

                last_height = new_height

            # Scroll back to top to ensure all elements are in DOM
            self.driver.execute_script("window.scrollTo(0, 0);")
            time.sleep(1)

            # Wait for any lazy-loaded content
            time.sleep(2)

            print("[OK] Scrolling completed")
            return True

        except Exception as e:
            print(f"[ERROR] Failed to scroll: {e}")
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

    def extract_star_rating(self, text):
        """Extract numeric rating from text like '4.4 out of 5 stars' -> '4.4'"""
        if not text:
            return None
        try:
            # Extract first number (rating)
            match = re.search(r'([\d.]+)', text)
            if match:
                return match.group(1)
            return None
        except Exception as e:
            return None

    def check_and_handle_throttling(self, page_number, url, max_retries=2):
        """Check for throttling message and refresh if needed"""
        for retry in range(max_retries):
            page_source = self.driver.page_source.lower()

            # Check for throttling message
            if "request was throttled" in page_source or "please wait a moment and refresh" in page_source:
                print(f"[WARNING] Throttling detected on page {page_number} (attempt {retry + 1}/{max_retries})")
                print("[INFO] Waiting before refresh...")
                time.sleep(random.uniform(15, 20))

                print("[INFO] Refreshing page...")
                self.driver.refresh()
                time.sleep(random.uniform(8, 12))
            else:
                print("[OK] No throttling detected")
                return True

        # After max_retries refreshes, if still throttled, try accessing URL directly
        page_source = self.driver.page_source.lower()
        if "request was throttled" in page_source or "please wait a moment and refresh" in page_source:
            print(f"[WARNING] Still throttled after {max_retries} refreshes. Trying direct URL access...")
            time.sleep(random.uniform(20, 25))

            print(f"[INFO] Accessing URL directly: {url[:80]}...")
            self.driver.get(url)
            time.sleep(random.uniform(10, 15))

            # Check one more time
            page_source = self.driver.page_source.lower()
            if "request was throttled" in page_source or "please wait a moment and refresh" in page_source:
                print(f"[ERROR] Still throttled after direct URL access")
                return False
            else:
                print("[OK] Direct URL access successful")
                return True

        return True

    def check_and_handle_sorry_page(self, max_retries=3):
        """Check for sorry/robot check page and refresh if needed

        Returns:
            bool: True if page is OK, False if still sorry page after retries
        """
        for attempt in range(max_retries):
            page_source = self.driver.page_source.lower()
            title = self.driver.title.lower()

            # Check for sorry/robot check page (check first 2000 chars for performance)
            is_sorry_page = (
                'sorry' in title or
                'robot check' in title or
                'sorry' in page_source[:2000] or
                'robot check' in page_source[:2000]
            )

            if is_sorry_page:
                print(f"  [WARNING] Sorry/Robot check page detected (attempt {attempt + 1}/{max_retries})")
                if attempt < max_retries - 1:
                    print(f"  [INFO] Refreshing page in 3-5 seconds...")
                    time.sleep(random.uniform(3, 5))  # Wait before refresh
                    self.driver.refresh()
                    print(f"  [INFO] Page refreshed, waiting for load...")
                    time.sleep(random.uniform(4, 6))  # Wait after refresh
                    continue
                else:
                    print(f"  [ERROR] Still sorry page after {max_retries} retries, skipping this page...")
                    return False
            else:
                if attempt > 0:
                    print(f"  [OK] Page loaded successfully after {attempt} refresh(es)")
                return True  # Page is OK

        return False

    def scrape_page(self, url, page_number):
        """Scrape a single BSR page"""
        try:
            print(f"\n[PAGE {page_number}] Accessing: {url[:80]}...")
            self.driver.get(url)

            # Wait longer for initial page load
            print("[INFO] Waiting for page to load...")
            time.sleep(random.uniform(8, 12))

            # Check and handle sorry page with refresh retries
            if not self.check_and_handle_sorry_page(max_retries=3):
                print(f"[SKIP] Skipping page {page_number} due to persistent sorry/robot check page")
                return False

            # Check for throttling and handle it
            if not self.check_and_handle_throttling(page_number, url):
                screenshot_path = f"bsr_page_{page_number}_throttled.png"
                self.driver.save_screenshot(screenshot_path)
                print(f"[ERROR] Page still throttled. Screenshot saved to {screenshot_path}")
                return False

            # Check if page loaded properly
            page_height = self.driver.execute_script("return document.body.scrollHeight")
            print(f"[DEBUG] Initial page height: {page_height}")

            if page_height < 1000:
                print("[WARNING] Page may not have loaded properly, waiting longer...")
                time.sleep(15)
                page_height = self.driver.execute_script("return document.body.scrollHeight")
                print(f"[DEBUG] Page height after wait: {page_height}")

                # If still failed, save screenshot for debugging
                if page_height < 1000:
                    screenshot_path = f"bsr_page_{page_number}_error.png"
                    self.driver.save_screenshot(screenshot_path)
                    print(f"[ERROR] Page failed to load. Screenshot saved to {screenshot_path}")
                    print(f"[DEBUG] Current URL: {self.driver.current_url}")
                    print("[DEBUG] Checking page title...")
                    print(f"[DEBUG] Page title: {self.driver.title}")
                    return False

            # Wait for BSR containers to be present using explicit wait
            print("[INFO] Waiting for BSR containers to load...")
            try:
                self.wait.until(EC.presence_of_element_located((By.CLASS_NAME, "zg-no-numbers")))
                print("[OK] BSR containers detected")
            except Exception as e:
                print(f"[ERROR] BSR containers not found: {e}")
                screenshot_path = f"bsr_page_{page_number}_no_containers.png"
                self.driver.save_screenshot(screenshot_path)
                print(f"[ERROR] Screenshot saved to {screenshot_path}")
                return False

            # Scroll to load all items (up to 50)
            self.scroll_to_load_all()

            # Get page source and parse with lxml
            page_source = self.driver.page_source
            tree = html.fromstring(page_source)

            # Get XPaths from database
            base_container_xpath = self.xpaths.get('base_container', {}).get('xpath', '')
            rank_xpath = self.xpaths.get('rank', {}).get('xpath', '')
            product_name_xpath = self.xpaths.get('product_name', {}).get('xpath', '')
            product_url_xpath = self.xpaths.get('product_url', {}).get('xpath', '')
            final_sku_price_xpath = self.xpaths.get('final_sku_price', {}).get('xpath', '')
            count_of_reviews_xpath = self.xpaths.get('count_of_reviews', {}).get('xpath', '')
            star_rating_xpath = self.xpaths.get('star_rating', {}).get('xpath', '')

            if not all([base_container_xpath, rank_xpath, product_name_xpath, product_url_xpath]):
                print("[ERROR] Required XPaths not found")
                return False

            print(f"[DEBUG] Base Container XPath: {base_container_xpath}")
            print(f"[DEBUG] Rank XPath: {rank_xpath}")
            print(f"[DEBUG] Product Name XPath: {product_name_xpath}")
            print(f"[DEBUG] Product URL XPath: {product_url_xpath}")

            # Find all BSR product containers
            containers = tree.xpath(base_container_xpath)
            print(f"[INFO] Found {len(containers)} BSR product containers")

            collected_count = 0

            # Extract data from each container
            for idx, container in enumerate(containers, 1):
                try:
                    # Extract rank (e.g., "#1", "#2", "#3")
                    rank_text = self.extract_text_safe(container, rank_xpath)
                    if rank_text:
                        # Remove "#" and convert to integer
                        bsr_rank = int(rank_text.replace('#', '').strip())
                    else:
                        print(f"  [SKIP {idx}] No rank found")
                        continue

                    # Extract product name
                    product_name = self.extract_text_safe(container, product_name_xpath)
                    if not product_name:
                        print(f"  [SKIP {idx}] Rank #{bsr_rank}: No product name found")
                        continue

                    # Extract product URL
                    product_url_path = self.extract_text_safe(container, product_url_xpath)
                    if product_url_path:
                        # Build complete URL
                        if product_url_path.startswith('http'):
                            product_url = product_url_path
                        else:
                            product_url = f"https://www.amazon.com{product_url_path}"
                    else:
                        product_url = None
                        print(f"  [WARNING] Rank #{bsr_rank}: No URL found")

                    # Extract final_sku_price (disabled - will be collected in detail crawler)
                    final_sku_price = None

                    # Extract count_of_reviews
                    count_of_reviews = self.extract_text_safe(container, count_of_reviews_xpath)

                    # Extract star_rating (extract number only)
                    star_rating_raw = self.extract_text_safe(container, star_rating_xpath)
                    star_rating = self.extract_star_rating(star_rating_raw)

                    # Save to database
                    if self.save_to_db(bsr_rank, product_name, product_url, final_sku_price,
                                       count_of_reviews, star_rating):
                        collected_count += 1
                        self.total_collected += 1
                        print(f"  [{idx}/{len(containers)}] Rank #{bsr_rank}: {product_name[:60]}...")
                        print(f"      Price: {final_sku_price or 'N/A'} | Reviews: {count_of_reviews or 'N/A'} | Rating: {star_rating or 'N/A'}")
                    else:
                        print(f"  [FAILED {idx}] Rank #{bsr_rank}: Database save failed")

                except Exception as e:
                    print(f"  [ERROR {idx}] Failed to extract data: {e}")
                    continue

            print(f"[PAGE {page_number}] Collected {collected_count} products (Total: {self.total_collected})")
            return True

        except Exception as e:
            print(f"[ERROR] Failed to scrape page {page_number}: {e}")
            import traceback
            traceback.print_exc()
            return False

    def save_to_db(self, bsr_rank, product_name, product_url=None, final_sku_price=None,
                   count_of_reviews=None, star_rating=None):
        """Save BSR data to database"""
        try:
            cursor = self.db_conn.cursor()

            # Calculate calendar week
            calendar_week = f"w{datetime.now().isocalendar().week}"

            # Calculate crawl_strdatetime (format: 202511040300559260)
            korea_tz = pytz.timezone('Asia/Seoul')
            now = datetime.now(korea_tz)
            crawl_strdatetime = now.strftime('%Y%m%d%H%M%S') + now.strftime('%f')[:4]

            cursor.execute("""
                INSERT INTO amazon_tv_bsr
                (account_name, bsr_rank, page_type, Retailer_SKU_Name, product_url, final_sku_price,
                 original_sku_price, count_of_reviews, star_rating, batch_id, calendar_week, crawl_strdatetime)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, ('Amazon', bsr_rank, 'bsr', product_name, product_url, final_sku_price,
                  None, count_of_reviews, star_rating, self.batch_id, calendar_week, crawl_strdatetime))

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
            print("Amazon TV BSR1 Crawler (No Price Collection)")
            print("="*80)

            # Connect to database
            if not self.connect_db():
                return

            # Generate batch_id for this session (Korea timezone)
            korea_tz = pytz.timezone('Asia/Seoul')
            self.batch_id = datetime.now(korea_tz).strftime('%Y%m%d_%H%M%S')
            print(f"[OK] Batch ID: {self.batch_id}")

            # Load XPaths
            if not self.load_xpaths():
                print("[ERROR] Please add XPath selectors first!")
                return

            # Load page URLs
            page_urls = self.load_page_urls()
            if not page_urls:
                print("[ERROR] No BSR page URLs found")
                return

            # Setup WebDriver
            self.setup_driver()

            # Scrape each page
            for page_number, url in page_urls:
                if not self.scrape_page(url, page_number):
                    print(f"[WARNING] Failed to scrape page {page_number}, continuing...")

                # Random delay between pages (longer to avoid throttling)
                if page_number < len(page_urls):
                    delay = random.uniform(10, 15)
                    print(f"[INFO] Waiting {delay:.1f} seconds before next page...")
                    time.sleep(delay)

            print("\n" + "="*80)
            print(f"BSR Crawling completed! Total collected: {self.total_collected} items")
            print("="*80)

            if self.error_messages:
                print("\nErrors encountered:")
                for error in self.error_messages:
                    print(f"  - {error}")

        except Exception as e:
            print(f"[ERROR] Crawler failed: {e}")
            import traceback
            traceback.print_exc()

        finally:
            if self.driver:
                self.driver.quit()
            if self.db_conn:
                self.db_conn.close()


if __name__ == "__main__":
    try:
        crawler = AmazonBSRCrawler()
        crawler.run()
    except Exception as e:
        print(f"\n[FATAL ERROR] {e}")
        import traceback
        traceback.print_exc()

    print("\n[INFO] Crawler terminated. Exiting...")
