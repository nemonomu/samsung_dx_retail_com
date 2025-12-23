"""
Amazon TV Test Script - Extract specific fields only
Fields: retailer_sku_name, final_sku_price, count_of_star_ratings, star_rating, count_of_reviews
Based on amazon_tv_dt2.py - No DB save, just extraction and logging with timing

DEBUG MODE: Includes timing measurements to diagnose page load issues
"""

import time
import random
import sys
import psycopg2
import pickle
import os
from datetime import datetime
import pytz
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from selenium.common.exceptions import TimeoutException
from webdriver_manager.chrome import ChromeDriverManager
from lxml import html
import re

# Configure stdout encoding for Windows console
if sys.stdout.encoding != 'utf-8':
    sys.stdout.reconfigure(encoding='utf-8', errors='replace')

# Import database and account configuration
from config import DB_CONFIG, AMAZON_ACCOUNTS

# Cookie file path (uses unsandev0004 for amazon_tv_crawl.py)
COOKIE_FILE = AMAZON_ACCOUNTS['unsandev0004']['cookie_file']


class AmazonTVTestCrawler:
    def __init__(self):
        self.driver = None
        self.db_conn = None
        self.xpaths = {}
        # Generate batch_id using Korea timezone
        korea_tz = pytz.timezone('Asia/Seoul')
        self.batch_id = datetime.now(korea_tz).strftime('%Y%m%d_%H%M%S')

    def connect_db(self):
        """Connect to PostgreSQL database"""
        try:
            self.db_conn = psycopg2.connect(**DB_CONFIG)
            self.db_conn.autocommit = True
            print("[OK] Database connected (autocommit enabled)")
            return True
        except Exception as e:
            print(f"[ERROR] Database connection failed: {e}")
            return False

    def load_xpaths(self):
        """Load XPath selectors from database"""
        try:
            print("[INFO] Loading XPath selectors from database...")
            cursor = self.db_conn.cursor()

            # Check if table exists
            cursor.execute("""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables
                    WHERE table_name = 'xpath_selectors'
                )
            """)
            table_exists = cursor.fetchone()[0]

            if not table_exists:
                print("[ERROR] Table 'xpath_selectors' does not exist")
                cursor.close()
                return False

            cursor.execute("""
                SELECT data_field, xpath
                FROM xpath_selectors
                WHERE mall_name = 'Amazon' AND page_type = 'detail_page' AND is_active = TRUE
            """)

            rows = cursor.fetchall()
            for row in rows:
                self.xpaths[row[0]] = row[1]

            cursor.close()

            if len(self.xpaths) == 0:
                print("[WARNING] No XPath selectors found for Amazon detail_page")
                print("[INFO] You may need to populate xpath_selectors table first")
            else:
                print(f"[OK] Loaded {len(self.xpaths)} XPath selectors")

            return True

        except Exception as e:
            print(f"[ERROR] Failed to load XPaths: {e}")
            import traceback
            traceback.print_exc()
            return False

    def setup_driver(self):
        """Setup Chrome WebDriver"""
        try:
            print("[INFO] Setting up Chrome WebDriver...")
            chrome_options = Options()
            chrome_options.add_argument('--disable-blink-features=AutomationControlled')
            chrome_options.add_argument('--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36')
            chrome_options.add_argument('--disable-dev-shm-usage')
            chrome_options.add_argument('--no-sandbox')
            chrome_options.add_argument('--window-size=1920,1080')
            chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
            chrome_options.add_experimental_option('useAutomationExtension', False)

            print("[INFO] Installing ChromeDriver...")
            service = Service(ChromeDriverManager().install())
            self.driver = webdriver.Chrome(service=service, options=chrome_options)

            # Anti-detection scripts
            print("[INFO] Applying anti-detection scripts...")
            self.driver.execute_cdp_cmd('Page.addScriptToEvaluateOnNewDocument', {
                'source': '''
                    Object.defineProperty(navigator, 'webdriver', {
                        get: () => undefined
                    });
                '''
            })

            print("[OK] WebDriver setup complete")

            # Load cookies for login
            self.load_cookies()

        except Exception as e:
            print(f"[ERROR] Failed to setup WebDriver: {e}")
            import traceback
            traceback.print_exc()
            raise

    def load_cookies(self):
        """Load cookies from file for authenticated access"""
        print(f"[INFO] Loading cookies from {COOKIE_FILE}...")

        if not os.path.exists(COOKIE_FILE):
            print(f"[WARNING] Cookie file not found: {COOKIE_FILE}")
            print("[WARNING] Review collection may fail without login.")
            print("[INFO] To create cookie file, run amazon_login.py first")
            return False

        try:
            print("[INFO] Accessing Amazon.com to set cookies...")
            self.driver.get("https://www.amazon.com")
            time.sleep(2)

            with open(COOKIE_FILE, 'rb') as f:
                cookies = pickle.load(f)
                print(f"[DEBUG] Found {len(cookies)} cookies in file")
                for cookie in cookies:
                    try:
                        self.driver.add_cookie(cookie)
                    except Exception as e:
                        print(f"[DEBUG] Failed to add cookie: {e}")

            print("[INFO] Refreshing page with cookies...")
            self.driver.refresh()
            time.sleep(2)
            print(f"[OK] Cookies loaded successfully")
            return True

        except Exception as e:
            print(f"[WARNING] Failed to load cookies: {e}")
            import traceback
            traceback.print_exc()
            return False

    def verify_page_loaded(self, wait_for_price=True):
        """
        Verify page is properly loaded before extraction (from dt1.py).
        - Check for bot detection (captcha/robot page)
        - Wait for key elements to load

        Returns:
            dict: timing information for each element wait
        """
        timing = {}

        # Wait for product title element (required)
        start = time.time()
        try:
            WebDriverWait(self.driver, 10).until(
                EC.presence_of_element_located((By.ID, 'productTitle'))
            )
            timing['productTitle'] = {'status': 'LOADED', 'time': time.time() - start}
            print(f"  [TIMING] productTitle loaded in {timing['productTitle']['time']:.2f}s")
        except:
            timing['productTitle'] = {'status': 'TIMEOUT', 'time': time.time() - start}
            print(f"  [TIMING] productTitle TIMEOUT after {timing['productTitle']['time']:.2f}s")
            return timing  # Page not loaded properly

        # Wait for price element (optional but important)
        if wait_for_price:
            start = time.time()
            try:
                WebDriverWait(self.driver, 5).until(
                    EC.presence_of_element_located((By.XPATH, '//*[@id="corePriceDisplay_desktop_feature_div"] | //*[@id="corePrice_feature_div"] | //*[@id="outOfStock"]'))
                )
                timing['priceElement'] = {'status': 'LOADED', 'time': time.time() - start}
                print(f"  [TIMING] priceElement loaded in {timing['priceElement']['time']:.2f}s")
            except:
                timing['priceElement'] = {'status': 'TIMEOUT', 'time': time.time() - start}
                print(f"  [TIMING] priceElement TIMEOUT after {timing['priceElement']['time']:.2f}s - may be unavailable product")

        # Wait for star rating element (optional)
        start = time.time()
        try:
            WebDriverWait(self.driver, 3).until(
                EC.presence_of_element_located((By.XPATH, '//*[@id="acrPopover"] | //*[@id="averageCustomerReviews"] | //span[contains(text(), "No customer reviews")]'))
            )
            timing['starRating'] = {'status': 'LOADED', 'time': time.time() - start}
            print(f"  [TIMING] starRating loaded in {timing['starRating']['time']:.2f}s")
        except:
            timing['starRating'] = {'status': 'TIMEOUT', 'time': time.time() - start}
            print(f"  [TIMING] starRating TIMEOUT after {timing['starRating']['time']:.2f}s")

        return timing

    def extract_text_safe(self, tree, xpath):
        """Safely extract text from XPath"""
        if not xpath:
            return None
        try:
            elements = tree.xpath(xpath)
            if elements:
                if isinstance(elements[0], str):
                    return elements[0].strip()
                else:
                    return elements[0].text_content().strip()
            return None
        except Exception:
            return None

    def extract_star_rating(self, tree):
        """Extract star rating (format: '4.5' or 'No customer reviews')"""
        try:
            # Try to get star rating from database XPath
            star_rating_text = self.extract_text_safe(tree, self.xpaths.get('star_rating'))

            if star_rating_text:
                # Check for "No customer reviews" first
                if "No customer reviews" in star_rating_text:
                    return "No customer reviews"

                # Return the star rating as-is if it contains a number
                if re.search(r'\d', star_rating_text):
                    return star_rating_text

            # Fallback: Check for "No customer reviews" at specific location
            no_reviews_xpaths = [
                '//*[@id="cm-cr-dp-review-header"]/h3/span',
                '//span[@data-hook="top-customer-reviews-title"]',
                '//div[@id="cm-cr-dp-review-header"]//span[contains(text(), "No customer reviews")]',
                '//span[contains(text(), "No customer reviews")]'
            ]

            for xpath in no_reviews_xpaths:
                text = self.extract_text_safe(tree, xpath)
                if text and "No customer reviews" in text:
                    return "No customer reviews"

            return None

        except Exception as e:
            print(f"  [WARNING] Failed to extract star rating: {e}")
            return None

    def extract_count_of_reviews(self, tree):
        """Extract count of reviews from detail page (fallback)
        Format: '1,484' or '0' for no reviews
        """
        try:
            xpaths = [
                '//*[@id="acrCustomerReviewText"]',
                '//span[@id="acrCustomerReviewText"]',
                '//a[@id="acrCustomerReviewLink"]//span'
            ]

            for xpath in xpaths:
                reviews_text = self.extract_text_safe(tree, xpath)
                if reviews_text:
                    # Check for "No customer reviews" first -> return 0
                    if "No customer reviews" in reviews_text:
                        return "0"

                    # "1,484 ratings" -> "1,484"
                    match = re.search(r'([\d,]+)', reviews_text)
                    if match:
                        return match.group(1)

            # Fallback: Check for "No customer reviews" at specific location -> return 0
            no_reviews_xpaths = [
                '//*[@id="cm-cr-dp-review-header"]/h3/span',
                '//span[@data-hook="top-customer-reviews-title"]',
                '//div[@id="cm-cr-dp-review-header"]//span[contains(text(), "No customer reviews")]',
                '//span[contains(text(), "No customer reviews")]'
            ]

            for xpath in no_reviews_xpaths:
                text = self.extract_text_safe(tree, xpath)
                if text and "No customer reviews" in text:
                    return "0"

            return None

        except Exception as e:
            print(f"  [WARNING] Failed to extract count of reviews: {e}")
            return None

    def extract_count_of_reviews_from_review_page(self, product_url):
        """Extract count_of_reviews from review page (same logic as dt1.py)

        Returns:
            str: count_of_reviews (e.g., '385') or None
        """
        try:
            # Get current page HTML
            tree = html.fromstring(self.driver.page_source)

            # Extract "See more reviews" link
            review_link_xpaths = [
                '//a[@data-hook="see-all-reviews-link-foot"]/@href',
                '//*[@id="reviews-medley-footer"]//a[contains(@href, "product-reviews")]/@href',
                '//*[@id="reviews-medley-footer"]/div[2]/a/@href',
                '//a[contains(text(), "See more reviews")]/@href',
                '//a[contains(text(), "See all reviews")]/@href',
                '//a[contains(@href, "product-reviews")]/@href'
            ]

            review_link = None
            for idx, xpath in enumerate(review_link_xpaths, 1):
                result = tree.xpath(xpath)
                if result:
                    review_link = result[0]
                    print(f"  [DEBUG] Found review link with XPath #{idx}: {review_link[:80]}...")
                    break

            if not review_link:
                print("  [WARNING] Could not find review page link, falling back to detail page")
                return None

            # Use original review link
            if review_link.startswith('http'):
                review_url = review_link
            else:
                review_url = "https://www.amazon.com" + review_link

            print(f"  [INFO] Navigating to review page: {review_url[:80]}...")
            self.driver.get(review_url)
            time.sleep(random.uniform(3, 4))

            # Wait for count_of_reviews element to load (from dt1.py)
            count_of_reviews = None
            try:
                WebDriverWait(self.driver, 5).until(
                    EC.presence_of_element_located((By.XPATH, '//*[@id="filter-info-section"] | //div[@data-hook="cr-filter-info-review-rating-count"]'))
                )
                # Element loaded - extract count_of_reviews
                tree = html.fromstring(self.driver.page_source)
                count_xpaths = [
                    '//*[@id="filter-info-section"]/div',
                    '//div[@data-hook="cr-filter-info-review-rating-count"]',
                    '//div[contains(@data-hook, "review-rating-count")]'
                ]

                for xpath in count_xpaths:
                    count_elements = tree.xpath(xpath)
                    if count_elements:
                        count_text = count_elements[0].text_content().strip() if hasattr(count_elements[0], 'text_content') else str(count_elements[0]).strip()
                        if count_text:
                            print(f"  [DEBUG] count_text found: {count_text[:100]}...")
                            # Pattern 1: "385 customer reviews"
                            match = re.search(r'([\d,]+)\s*customer\s*reviews?', count_text, re.IGNORECASE)
                            if not match:
                                # Pattern 2: "385 with reviews"
                                match = re.search(r'([\d,]+)\s*with\s*reviews?', count_text, re.IGNORECASE)
                            if not match:
                                # Pattern 3: Just "385 reviews"
                                match = re.search(r'([\d,]+)\s*reviews?', count_text, re.IGNORECASE)
                            if match:
                                count_of_reviews = match.group(1)
                                print(f"  [OK] Extracted count_of_reviews from review page: {count_of_reviews}")
                                break
            except:
                print("  [WARNING] count_of_reviews element not loaded - skipping extraction")

            # Navigate back to product page
            print(f"  [INFO] Navigating back to product page...")
            self.driver.get(product_url)
            time.sleep(random.uniform(2, 3))

            return count_of_reviews

        except Exception as e:
            print(f"  [WARNING] Failed to extract count_of_reviews from review page: {e}")
            try:
                self.driver.get(product_url)
                time.sleep(2)
            except:
                pass
            return None

    def extract_count_of_star_ratings(self, tree):
        """Extract total star rating count (sum of all star ratings)
        Returns: integer (e.g., 2449) or None
        """
        try:
            # Get total count from "2,449 global ratings"
            total_text = self.extract_text_safe(tree, '//*[@id="cm_cr_dp_d_rating_histogram"]/div[3]')
            if not total_text:
                return None

            # Extract number from "2,449 global ratings"
            total_match = re.search(r'([\d,]+)\s*global ratings?', total_text)
            if not total_match:
                return None

            total_count = int(total_match.group(1).replace(',', ''))
            return total_count

        except Exception as e:
            print(f"  [WARNING] Failed to extract star ratings count: {e}")
            return None

    def extract_final_sku_price(self, tree):
        """Extract final SKU price from detail page
        Priority order: Special cases first, then normal price extraction
        """
        try:
            # PRIORITY 1: Check for "Currently unavailable."
            currently_unavailable_xpaths = [
                '//*[@id="outOfStock"]/div/div[1]/span[1]',
                '//*[@id="availability"]/span[2]/span',
                '//span[@class="a-color-price a-text-bold"]',
                '//span[@class="a-size-medium a-color-success"]'
            ]

            for xpath in currently_unavailable_xpaths:
                text = self.extract_text_safe(tree, xpath)
                if text and 'currently unavailable' in text.lower():
                    return "Currently unavailable."

            # PRIORITY 2: Check for "Price higher than typical"
            price_higher_xpaths = [
                '//*[@id="fod-cx-message-with-learn-more"]/span[1]',
                '//span[@id="fod-cx-message-with-learn-more"]/span[1]',
                '//span[contains(text(), "Price higher than typical")]'
            ]

            for xpath in price_higher_xpaths:
                text = self.extract_text_safe(tree, xpath)
                if text and 'price higher than typical' in text.lower():
                    return "Price higher than typical"

            # PRIORITY 3: Check for "No featured offers available"
            no_offers_xpaths = [
                '//*[@id="fod-cx-message-with-learn-more"]/span[1]',
                '//span[@id="fod-cx-message-with-learn-more"]/span[1]',
                '//span[contains(text(), "No featured offers available")]'
            ]

            for xpath in no_offers_xpaths:
                text = self.extract_text_safe(tree, xpath)
                if text and 'no featured offers available' in text.lower():
                    return "No featured offers available"

            # PRIORITY 4: Check for "See price in cart"
            see_price_xpaths = [
                '//*[@id="corePriceDisplay_desktop_feature_div"]/table/tbody/tr/td[2]/span/a',
                '//a[contains(text(), "See price in cart")]',
                '//span[@class="a-declarative"]//a[contains(text(), "See price in cart")]'
            ]

            for xpath in see_price_xpaths:
                text = self.extract_text_safe(tree, xpath)
                if text and 'see price in cart' in text.lower():
                    return "See price in cart"

            # PRIORITY 5: Check for "To see our price, add this item to your cart."
            add_to_cart_xpaths = [
                '//*[@id="corePriceDisplay_desktop_feature_div"]/table/tbody/tr/td[2]',
                '//table[@class="a-lineitem"]//td[contains(text(), "To see our price")]',
                '//td[contains(text(), "To see our price, add this item to your cart")]'
            ]

            for xpath in add_to_cart_xpaths:
                text = self.extract_text_safe(tree, xpath)
                if text and 'to see our price, add this item to your cart' in text.lower():
                    return "To see our price, add this item to your cart."

            # NORMAL EXTRACTION: Try to extract regular price
            xpaths = [
                '//*[@id="corePriceDisplay_desktop_feature_div"]/div[1]/span[1]',
                '//*[@id="corePriceDisplay_desktop_feature_div"]/div[1]/span[3]/span[2]',
                '//*[@id="corePriceDisplay_desktop_feature_div"]/div[1]/span[3]/span[2]/span[1]',
                '//span[@class="a-price aok-align-center reinventPricePriceToPayMargin priceToPay"]//span[@class="a-offscreen"]',
                '//*[@id="corePrice_feature_div"]/div/div/span[1]/span[1]',
                '//*[@id="corePrice_feature_div"]//span[@class="a-offscreen"]'
            ]

            for xpath in xpaths:
                price_text = self.extract_text_safe(tree, xpath)
                if price_text:
                    # Extract only "$XXX.XX" format
                    match = re.search(r'\$[\d,]+\.?\d*', price_text)
                    if match:
                        return match.group()
                    return price_text.strip()

            return None

        except Exception as e:
            print(f"  [WARNING] Failed to extract final SKU price: {e}")
            return None

    def scrape_url(self, url):
        """Scrape a single URL and extract fields with timing analysis"""
        try:
            print(f"\n{'='*80}")
            print(f"[INFO] Accessing: {url[:80]}...")

            # Measure page load time
            page_start_time = time.time()
            self.driver.get(url)
            time.sleep(random.uniform(3, 5))

            page_load_time = time.time() - page_start_time
            print(f"  [TIMING] Initial page load time: {page_load_time:.2f}s")

            # Verify page is properly loaded (from dt1.py)
            print(f"\n  [INFO] Verifying page load status...")
            timing = self.verify_page_loaded(wait_for_price=True)

            # Check if page loaded properly
            if timing.get('productTitle', {}).get('status') == 'TIMEOUT':
                print(f"  [ERROR] Page verification failed - productTitle not loaded")
                return None

            print(f"  [INFO] Page loaded and verified, extracting data...")

            page_source = self.driver.page_source
            tree = html.fromstring(page_source)

            # Extract count_of_star_ratings first (from detail page)
            count_of_star_ratings = self.extract_count_of_star_ratings(tree)

            # Extract count_of_reviews from review page (same as dt1.py)
            count_of_reviews = self.extract_count_of_reviews_from_review_page(url)

            # Verify page is properly loaded after returning from review page (from dt1.py)
            print(f"\n  [INFO] Verifying page load after returning from review page...")
            timing_after_review = self.verify_page_loaded(wait_for_price=True)
            if timing_after_review.get('productTitle', {}).get('status') == 'TIMEOUT':
                print(f"  [WARNING] Page verification failed after review page return - continuing with available data")

            # Re-parse page source after returning from review page
            tree = html.fromstring(self.driver.page_source)

            # Fallback: If count_of_reviews not found from review page, try detail page
            if not count_of_reviews:
                count_of_reviews = self.extract_count_of_reviews(tree)

            # Extract other fields from detail page
            retailer_sku_name = self.extract_text_safe(tree, self.xpaths.get('product_name'))
            final_sku_price = self.extract_final_sku_price(tree)
            star_rating = self.extract_star_rating(tree)

            # Log results
            print(f"\n  {'='*60}")
            print(f"  [RESULT] Extraction Results:")
            print(f"  {'='*60}")
            print(f"  retailer_sku_name     : {retailer_sku_name[:60] + '...' if retailer_sku_name and len(retailer_sku_name) > 60 else retailer_sku_name}")
            print(f"  final_sku_price       : {final_sku_price}")
            print(f"  count_of_star_ratings : {count_of_star_ratings}")
            print(f"  star_rating           : {star_rating}")
            print(f"  count_of_reviews      : {count_of_reviews}")
            print(f"  {'='*60}")

            # Check for empty values and warn
            empty_fields = []
            if not retailer_sku_name:
                empty_fields.append("retailer_sku_name")
            if not final_sku_price:
                empty_fields.append("final_sku_price")
            if not count_of_star_ratings:
                empty_fields.append("count_of_star_ratings")
            if not star_rating:
                empty_fields.append("star_rating")
            if not count_of_reviews:
                empty_fields.append("count_of_reviews")

            if empty_fields:
                print(f"\n  [WARNING] EMPTY FIELDS DETECTED: {', '.join(empty_fields)}")
                print(f"  [DEBUG] Timing info: {timing}")

            total_time = time.time() - page_start_time
            print(f"\n  [TIMING] Total URL processing time: {total_time:.2f}s")

            return {
                'url': url,
                'retailer_sku_name': retailer_sku_name,
                'final_sku_price': final_sku_price,
                'count_of_star_ratings': count_of_star_ratings,
                'star_rating': star_rating,
                'count_of_reviews': count_of_reviews,
                'timing': timing,
                'total_time': total_time
            }

        except Exception as e:
            print(f"  [ERROR] Failed to scrape: {e}")
            import traceback
            traceback.print_exc()
            return None

    def run(self, test_urls=None):
        """Main execution"""
        try:
            print("="*80)
            print(f"Amazon TV Test Crawler - Starting (Batch ID: {self.batch_id})")
            print("Fields: retailer_sku_name, final_sku_price, count_of_star_ratings, star_rating, count_of_reviews")
            print("="*80)

            # Default test URLs if none provided
            if not test_urls:
                test_urls = [
                    'https://www.amazon.com/dp/B0D1XL87F4',
                ]

            # Step 1: Connect to database
            print("\n[STEP 1/3] Connecting to database...")
            if not self.connect_db():
                print("[ERROR] Failed to connect to database. Stopping.")
                return

            # Step 2: Load XPaths
            print("\n[STEP 2/3] Loading XPath selectors...")
            if not self.load_xpaths():
                print("[ERROR] Failed to load XPath selectors. Stopping.")
                return

            # Step 3: Setup WebDriver
            print("\n[STEP 3/3] Setting up WebDriver...")
            self.setup_driver()
            print("[OK] WebDriver ready")

            # Scrape each URL
            print(f"\n[INFO] Scraping {len(test_urls)} URLs...")

            results = []
            for idx, url in enumerate(test_urls, 1):
                print(f"\n[{idx}/{len(test_urls)}] Processing...")
                result = self.scrape_url(url)
                if result:
                    results.append(result)

                # Random delay between requests
                if idx < len(test_urls):
                    delay = random.uniform(2, 4)
                    print(f"[INFO] Waiting {delay:.1f} seconds...")
                    time.sleep(delay)

            # Final summary
            print("\n" + "="*80)
            print("FINAL SUMMARY")
            print("="*80)
            for idx, r in enumerate(results, 1):
                print(f"\n[{idx}] {r['url'][:60]}...")
                sku_name = r['retailer_sku_name']
                print(f"    retailer_sku_name     : {sku_name[:50] + '...' if sku_name and len(sku_name) > 50 else sku_name}")
                print(f"    final_sku_price       : {r['final_sku_price']}")
                print(f"    count_of_star_ratings : {r['count_of_star_ratings']}")
                print(f"    star_rating           : {r['star_rating']}")
                print(f"    count_of_reviews      : {r['count_of_reviews']}")
                print(f"    total_time            : {r['total_time']:.2f}s")

            # Summary of empty fields
            print("\n" + "="*80)
            print("EMPTY FIELDS ANALYSIS")
            print("="*80)
            for idx, r in enumerate(results, 1):
                empty = []
                if not r['retailer_sku_name']:
                    empty.append("retailer_sku_name")
                if not r['final_sku_price']:
                    empty.append("final_sku_price")
                if not r['count_of_star_ratings']:
                    empty.append("count_of_star_ratings")
                if not r['star_rating']:
                    empty.append("star_rating")
                if not r['count_of_reviews']:
                    empty.append("count_of_reviews")
                if empty:
                    print(f"[{idx}] EMPTY: {', '.join(empty)}")
                    print(f"    URL: {r['url'][:70]}...")
            print("="*80)

        except Exception as e:
            print(f"\n[ERROR] Crawler failed: {e}")
            import traceback
            traceback.print_exc()

        finally:
            print("\n[INFO] Cleaning up...")
            if self.driver:
                try:
                    self.driver.quit()
                    print("[OK] WebDriver closed")
                except:
                    pass
            if self.db_conn:
                try:
                    self.db_conn.close()
                    print("[OK] Database connection closed")
                except:
                    pass


if __name__ == "__main__":
    # Test URLs provided by user (2023-12-23)
    custom_urls = [
        'https://www.amazon.com/Dungeon-Core-Complete-Boxed-Set-ebook/dp/B0DNVK9RW3/ref=sr_1_250?crid=RY3XOG3VC795&dib=eyJ2IjoiMSJ9.FV0dvVruYiD6AYvkj99jki5wGe1tLX640Q1zwSHDOme420SPZfTfceq-TgzxoagfdyDXEAt-zNadL1qIi5kQrlZp71PnpneKR0KCxfGnJ57jTf7B4qTTiuV6NB-LGIqEJK4inTjcm12hySuIXIDTFzzIqvZqR72kCRDXR---QzJ78dQbWsxjDbk5k2q2oim_DA9GjVvOLHxgvIupyD8dE7pdVZBF4HJb1UDBpCftg1Y.7WHBi5oHvXpNHJRvlO-7xkOWneJKGNLSd6uIg4M4CdU&dib_tag=se&keywords=TV&qid=1766361924&sprefix=tv%2Caps%2C287&sr=8-250&xpid=Cg0_BApgal-UM',
        'https://www.amazon.com/INSIGNIA-75-inch-Class-Remote-NS75-UQFL26/dp/B0DWBMW3LD/ref=zg_bs_g_172659_d_sccl_6/139-9346033-8880350?th=1',
        'https://www.amazon.com/TCL-65QM8K-120HZ-144HZ-Reflective-Television/dp/B0F53CZ4WT/ref=zg_bs_g_172659_d_sccl_8/139-9346033-8880350?psc=1',
        'https://www.amazon.com/Westinghouse-Roku-Connectivity-Compatible-Assistant/dp/B0BZT9Y3L9/ref=zg_bs_g_172659_d_sccl_12/139-9346033-8880350?psc=1',
        'https://www.amazon.com/INSIGNIA-fire-tv-65-inch-class-f50-series-4k-smart-tv/dp/B0DM2LQV3F/ref=zg_bs_g_172659_d_sccl_16/139-9346033-8880350?psc=1',
        'https://www.amazon.com/SAMSUNG-65-Inch-Processor-Xcelerator-Samsung/dp/B0DXMJFJ7W/ref=zg_bs_g_172659_d_sccl_22/139-9346033-8880350?psc=1',
        'https://www.amazon.com/FPD-43-inch-Chromecast-Palette-CG43-P3/dp/B0CRRRMK5W/ref=zg_bs_g_172659_d_sccl_24/139-9346033-8880350?psc=1',
        'https://www.amazon.com/KTC-Portable-Standbyme-EDLA-Certified-Octa-core/dp/B0DMV9QJWR/ref=zg_bs_g_172659_d_sccl_26/139-9346033-8880350?psc=1',
        'https://www.amazon.com/amazon-fire-tv-65-inch-omni-series-4k-smart-tv/dp/B08T6J1HG8/ref=zg_bs_g_172659_d_sccl_27/139-9346033-8880350?psc=1',
        'https://www.amazon.com/Roku-Brilliant-Automatic-Brightness-Streaming/dp/B0CLFD3NF5/ref=zg_bs_g_172659_d_sccl_28/139-9346033-8880350?psc=1',
        'https://www.amazon.com/Westinghouse-Roku-Connectivity-Compatible-Assistant/dp/B0CCRC1PP2/ref=zg_bs_g_172659_d_sccl_30/139-9346033-8880350?psc=1',
        'https://www.amazon.com/TinyTV-Portable-Television-Working-minature/dp/B0CMSGKMBC/ref=zg_bs_g_172659_d_sccl_82/139-9346033-8880350?psc=1',
        'https://www.amazon.com/Sony-65-Inch-Backlight-Features-K-65XR70/dp/B0CVQ4FQJ9/ref=zg_bs_g_172659_d_sccl_84/139-9346033-8880350?psc=1',
        'https://www.amazon.com/FPD-Google-Built-Television-CG50-C3/dp/B0DJQJHK7Y/ref=zg_bs_g_172659_d_sccl_87/139-9346033-8880350?psc=1',
        'https://www.amazon.com/othoig-Compact-Speakers-Digital-Kitchen/dp/B0FP2N67YP/ref=zg_bs_g_172659_d_sccl_88/139-9346033-8880350?psc=1',
        'https://www.amazon.com/TCL-32S350G-Assistant-Compatible-Television/dp/B0C1HZ9HCM/ref=zg_bs_g_172659_d_sccl_89/139-9346033-8880350?psc=1',
        'https://www.amazon.com/Samsung-Processor-Upscaling-Validated-Xcelerator/dp/B0FJ2KBSSV/ref=zg_bs_g_172659_d_sccl_92/139-9346033-8880350?psc=1',
        'https://www.amazon.com/Sony-Processor-Technology-Television-K-65XR80M2/dp/B0DYK7Y2YB/ref=zg_bs_g_172659_d_sccl_93/139-9346033-8880350?psc=1',
        'https://www.amazon.com/Sony-BRAVIA-Inch-Mini-Array/dp/B0F84Y22C9/ref=sr_1_106?crid=RY3XOG3VC795&dib=eyJ2IjoiMSJ9.c9xSDNwaJLqROuLKjWWvg1jAH06Yfp7kdDPfhqm7P5Lf5WbSGLinVwbjpYMu09gxIHETQVNJs2FLNPwbDpec5y2wqEYvG-zpdO_I0g7BjYajSLTQiKtWT4R0om0DsCnYVy13EBhQdt1Zath3LIdlABMXYcH6O4s6aN_mKMZ7AQQsmlaydR-JoM_yQNogr--0gHfPwRdWg56EnG0nTT3Ih98xzchFmFHtR-M1JwvV53E.Yaw0zvg5TaVcPJAC2xzA55NuB_up2B_MaAOLCgkxcX8&dib_tag=se&keywords=TV&qid=1766384725&sprefix=tv%2Caps%2C287&sr=8-106&xpid=Cg0_BApgal-UM',
        'https://www.amazon.com/TCL-65-Inch-QD-Mini-Google-Channel/dp/B0DWMJQ7SK/ref=sr_1_111?crid=RY3XOG3VC795&dib=eyJ2IjoiMSJ9.c9xSDNwaJLqROuLKjWWvg1jAH06Yfp7kdDPfhqm7P5Lf5WbSGLinVwbjpYMu09gxIHETQVNJs2FLNPwbDpec5y2wqEYvG-zpdO_I0g7BjYajSLTQiKtWT4R0om0DsCnYVy13EBhQdt1Zath3LIdlABMXYcH6O4s6aN_mKMZ7AQQsmlaydR-JoM_yQNogr--0gHfPwRdWg56EnG0nTT3Ih98xzchFmFHtR-M1JwvV53E.Yaw0zvg5TaVcPJAC2xzA55NuB_up2B_MaAOLCgkxcX8&dib_tag=se&keywords=TV&qid=1766384725&sprefix=tv%2Caps%2C287&sr=8-111&xpid=Cg0_BApgal-UM',
        'https://www.amazon.com/SYLVOX-Waterproof-Weatherproof-Television-Assistant/dp/B0D22T2LQW/ref=sr_1_125?crid=RY3XOG3VC795&dib=eyJ2IjoiMSJ9.tVOdpjd5SPcWdXNYKs88s-Uy2qopizLXbEaM7JLpbHX7cqAvM1iUSt3irYRuIwIaTW8qoH72I3klT8NjfECelRXy2RU_wZ__IESha-Cs-5t8LQZydjNTC9rMAcGNjij61CiFHbmOvKO06NOBEYnmmfocKLWa12i5pwgqYbGes5LBi1wg7UOFWL6pK3SQ3B-CGcojwpaBXZbsh0M9BswjOC9AkZNKlXTH7gqKFdy6KQ0.C-mVy516GW8kleSFx7DEArf8QO_hqbnnlnIzRhOZpL8&dib_tag=se&keywords=TV&qid=1766384743&sprefix=tv%2Caps%2C287&sr=8-125&xpid=Cg0_BApgal-UM',
        'https://www.amazon.com/Sony-Inch-Ultra-X90L-Playstation%C2%AE/dp/B0C3FTV6DW/ref=sr_1_128?crid=RY3XOG3VC795&dib=eyJ2IjoiMSJ9.tVOdpjd5SPcWdXNYKs88s-Uy2qopizLXbEaM7JLpbHX7cqAvM1iUSt3irYRuIwIaTW8qoH72I3klT8NjfECelRXy2RU_wZ__IESha-Cs-5t8LQZydjNTC9rMAcGNjij61CiFHbmOvKO06NOBEYnmmfocKLWa12i5pwgqYbGes5LBi1wg7UOFWL6pK3SQ3B-CGcojwpaBXZbsh0M9BswjOC9AkZNKlXTH7gqKFdy6KQ0.C-mVy516GW8kleSFx7DEArf8QO_hqbnnlnIzRhOZpL8&dib_tag=se&keywords=TV&qid=1766384743&sprefix=tv%2Caps%2C287&sr=8-128&xpid=Cg0_BApgal-UM',
        'https://www.amazon.com/Dungeon-Core-Complete-Boxed-Set-ebook/dp/B0DNVK9RW3/ref=sr_1_249?crid=RY3XOG3VC795&dib=eyJ2IjoiMSJ9.EriR50lo_MVc4ihAC_XwHTM4O598krEhc0wm78IuMIBJwMKfTYIOfSKdqTivhtywNMFXDzTJGhxNvPwwbIM_Vlly-9nFf1-gQy5qb9e64mx2rzSpR1stzi1af4Zmg3OMfj5NSyzmWMfzH2_ceXHEgqUBcwBRJ6RwClqqZeOaCP4X8Jv1fwgfrjilYxoz5spuIHRD98KKbgSMh-CLT3Oyq_uwww1clDbNpSehzMUmbJM.9_7lDnIv9vPKwG2XLn2b03TEudbV38AxRDB97v638EA&dib_tag=se&keywords=TV&qid=1766384896&sprefix=tv%2Caps%2C287&sr=8-249&xpid=Cg0_BApgal-UM',
        'https://www.amazon.com/SuperSonic-1080p-Widescreen-Input-39-Inch/dp/B00SYG21O0/ref=sr_1_111?crid=RY3XOG3VC795&dib=eyJ2IjoiMSJ9.f2n4ci8FQ8FRuDVKVCRt8sMFHp2-20dJY7_0YnLttIDO1r2QxzWxYmMYHrBsOsZkZn_f8rb353BWOngF-Zs6TT7iiTWGmRgBADJMUvjOzSeOJIiyjlA-3EDwtJdegvL4qExgNt4FWRhBgb4mtfXqEU6H49U01zQpgCryLV6bLkM7SiUaDp3XEhTmeQOtan_eqEXccIW6q2Slhu4UGxkmTJAmgrv__v_KfEUbNlvUGrI.yXWK02fE_yTDO27ptx7L7z5NzwTto3gi3TDAnNVrJRk&dib_tag=se&keywords=TV&qid=1766404929&sprefix=tv%2Caps%2C287&sr=8-111&xpid=Cg0_BApgal-UM',
        'https://www.amazon.com/Dungeon-Core-Complete-Boxed-Set-ebook/dp/B0DNVK9RW3/ref=sr_1_249?crid=RY3XOG3VC795&dib=eyJ2IjoiMSJ9.2S5S_8Bu3xTr6x-Pn4_qwPEDX_VpaernTxl_K9Q6SL5fycvGrDNcYkapn8tRGrGtqMIZBr6IeLuahThQPRg9yjNUceHm4B7jJc2NECCLasOrL6IHNA0jz7AXyE71VCZ1fj5NSyzmWMfzH2_ceXHEgvxH57TN9oQEcywxWBES26cGhqblabtKhErQzm59f8LI6Mox792-E4U7Y6haobrP5R27yXDj902U5vFZWNLlo4o.t1AITOFAqSsX0u3BB6BBWz9HyhRkRSI-s_OWgLpvIY0&dib_tag=se&keywords=TV&qid=1766405105&sprefix=tv%2Caps%2C287&sr=8-249&xpid=Cg0_BApgal-UM',
        'https://www.amazon.com/VIZIO-D24fM-K01-TBD/dp/B0B286BGSL/ref=zg_bs_g_172659_d_sccl_42/141-2222062-3749145?th=1',
        'https://www.amazon.com/LG-Upscaling-Filmmaker-Orchestra-OLED65B5PUA-AUSZ/dp/B0FFWW5BZZ/ref=zg_bs_g_172659_d_sccl_44/141-2222062-3749145?psc=1',
        'https://www.amazon.com/Roku-Smart-2025-Television-Streaming/dp/B0DWHVZHBY/ref=zg_bs_g_172659_d_sccl_46/141-2222062-3749145?psc=1',
        'https://www.amazon.com/Fire-TV-Omni-QLED-Series-65-inch/dp/B0DD2P7YVW/ref=zg_bs_g_172659_d_sccl_9/141-2222062-3749145?psc=1',
        'https://www.amazon.com/LG-Upscaling-Filmmaker-Orchestra-OLED55G5WUA/dp/B0DYQGRHX3/ref=zg_bs_g_172659_d_sccl_11/141-2222062-3749145?th=1',
        'https://www.amazon.com/Feihe-Television-Kitchen-Bedroom-Entertainment/dp/B0F8C3VTVY/ref=zg_bs_g_172659_d_sccl_13/141-2222062-3749145?psc=1',
        'https://www.amazon.com/INSIGNIA-fire-tv-65-inch-class-f50-series-4k-smart-tv/dp/B0DM2LQV3F/ref=zg_bs_g_172659_d_sccl_14/141-2222062-3749145?psc=1',
        'https://www.amazon.com/Hisense-40-inch-QLED-FHD-Fire-TV/dp/B0FBYQHQH1/ref=zg_bs_g_172659_d_sccl_16/141-2222062-3749145?psc=1',
        'https://www.amazon.com/Roku-Brilliant-Automatic-Brightness-Streaming/dp/B0CLFD3NF5/ref=zg_bs_g_172659_d_sccl_26/141-2222062-3749145?psc=1',
        'https://www.amazon.com/othoig-Compact-Speakers-Digital-Kitchen/dp/B0FP2N67YP/ref=zg_bs_g_172659_d_sccl_81/141-2222062-3749145?psc=1',
        'https://www.amazon.com/TinyTV-Portable-Television-Working-minature/dp/B0CMSGKMBC/ref=zg_bs_g_172659_d_sccl_82/141-2222062-3749145?psc=1',
        'https://www.amazon.com/TCL-43S450R-Assistant-Compatibility-Television/dp/B0C1J14MZT/ref=zg_bs_g_172659_d_sccl_83/141-2222062-3749145?psc=1',
        'https://www.amazon.com/Feihe-Screen-Kitchen-Camper-Bedroom/dp/B0FM8FM7MG/ref=zg_bs_g_172659_d_sccl_95/141-2222062-3749145?psc=1',
        'https://www.amazon.com/ONN-32-inch-Connectivity-100012589-Renewed/dp/B0CBD61VK6/ref=zg_bs_g_172659_d_sccl_96/141-2222062-3749145?th=1',
        'https://www.amazon.com/othoig-12-5inch-Portable-Essential-Lightweight/dp/B0DJ7JJBC8/ref=zg_bs_g_172659_d_sccl_97/141-2222062-3749145?psc=1',
        'https://www.amazon.com/Sony-Exclusive-Features-PlayStation-K-65XR80/dp/B0CVQ6YLH7/ref=zg_bs_g_172659_d_sccl_98/141-2222062-3749145?psc=1',
    ]

    try:
        crawler = AmazonTVTestCrawler()
        crawler.run(test_urls=custom_urls)
    except Exception as e:
        print(f"\n[FATAL ERROR] {e}")
        import traceback
        traceback.print_exc()

    print("\n[INFO] Test completed.")
