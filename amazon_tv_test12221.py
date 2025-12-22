"""
Amazon TV Test Script with Account Switching
- Extract: final_sku_price, retailer_sku_name, count_of_reviews, star_rating, count_of_star_ratings
- Switch account after 3 products
- Based on amazon_tv_dt2.py - No DB save, just extraction and logging
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
from webdriver_manager.chrome import ChromeDriverManager
from lxml import html
import re

# Configure stdout encoding for Windows console
if sys.stdout.encoding != 'utf-8':
    sys.stdout.reconfigure(encoding='utf-8', errors='replace')

# Import database and account configuration
from config import DB_CONFIG, AMAZON_ACCOUNTS

# Cookie files for account switching (same as dt2.py)
COOKIE_FILE_1 = AMAZON_ACCOUNTS['unsandev0002']['cookie_file']  # First 3 products
COOKIE_FILE_2 = AMAZON_ACCOUNTS['unsandev0003']['cookie_file']  # Remaining products
ACCOUNT_SWITCH_AT = 3  # Switch account after this many products


class AmazonTVTestCrawler:
    def __init__(self):
        self.driver = None
        self.db_conn = None
        self.xpaths = {}
        self.total_collected = 0
        self.current_cookie_file = COOKIE_FILE_1  # Start with first account
        self.account_switched = False  # Track if account has been switched
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

            cursor.execute("""
                SELECT data_field, xpath
                FROM xpath_selectors
                WHERE mall_name = 'Amazon' AND page_type = 'detail_page' AND is_active = TRUE
            """)

            rows = cursor.fetchall()
            for row in rows:
                self.xpaths[row[0]] = row[1]

            cursor.close()
            print(f"[OK] Loaded {len(self.xpaths)} XPath selectors")
            return True

        except Exception as e:
            print(f"[ERROR] Failed to load XPaths: {e}")
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

            service = Service(ChromeDriverManager().install())
            self.driver = webdriver.Chrome(service=service, options=chrome_options)

            # Anti-detection scripts
            self.driver.execute_cdp_cmd('Page.addScriptToEvaluateOnNewDocument', {
                'source': '''
                    Object.defineProperty(navigator, 'webdriver', {
                        get: () => undefined
                    });
                '''
            })

            print("[OK] WebDriver setup complete")
            return True

        except Exception as e:
            print(f"[ERROR] Failed to setup WebDriver: {e}")
            return False

    def load_cookies(self, cookie_file=None):
        """Load cookies from file for authenticated access"""
        if cookie_file is None:
            cookie_file = self.current_cookie_file

        print(f"[INFO] Loading cookies from {cookie_file}...")

        if not os.path.exists(cookie_file):
            print(f"[WARNING] Cookie file not found: {cookie_file}")
            return False

        try:
            self.driver.get("https://www.amazon.com")
            time.sleep(2)

            with open(cookie_file, 'rb') as f:
                cookies = pickle.load(f)
                print(f"[DEBUG] Found {len(cookies)} cookies in file")
                for cookie in cookies:
                    try:
                        self.driver.add_cookie(cookie)
                    except Exception as e:
                        pass

            self.driver.refresh()
            time.sleep(2)
            print(f"[OK] Cookies loaded successfully")
            return True

        except Exception as e:
            print(f"[WARNING] Failed to load cookies: {e}")
            return False

    def switch_account(self):
        """Switch to second account by loading new cookies"""
        print("\n" + "=" * 80)
        print(f"[INFO] Switching account after {self.total_collected} products...")
        print(f"[INFO] Current cookie: {self.current_cookie_file}")
        print(f"[INFO] Switching to: {COOKIE_FILE_2}")
        print("=" * 80)

        try:
            # Clear existing cookies
            self.driver.delete_all_cookies()
            print("[INFO] Cleared existing cookies")

            # Update current cookie file
            self.current_cookie_file = COOKIE_FILE_2

            # Load new cookies
            if self.load_cookies(COOKIE_FILE_2):
                self.account_switched = True
                print("[OK] Account switched successfully!")
                return True
            else:
                print("[ERROR] Failed to load new account cookies")
                return False

        except Exception as e:
            print(f"[ERROR] Failed to switch account: {e}")
            return False

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
        except Exception as e:
            return None

    def extract_retailer_sku_name(self, tree):
        """Extract retailer SKU name (product title)"""
        try:
            retailer_sku_name = self.extract_text_safe(tree, self.xpaths.get('product_name'))
            if not retailer_sku_name:
                retailer_sku_name = self.extract_text_safe(tree, '//*[@id="productTitle"]')
            return retailer_sku_name
        except Exception as e:
            print(f"  [WARNING] Failed to extract retailer_sku_name: {e}")
            return None

    def extract_star_rating(self, tree):
        """Extract star rating (format: '4.5 out of 5 stars' or 'No customer reviews')"""
        try:
            star_rating_text = self.extract_text_safe(tree, self.xpaths.get('star_rating'))

            if star_rating_text:
                if "No customer reviews" in star_rating_text:
                    return "No customer reviews"
                if re.search(r'\d', star_rating_text):
                    return star_rating_text

            # Fallback
            no_reviews_xpaths = [
                '//*[@id="cm-cr-dp-review-header"]/h3/span',
                '//span[@data-hook="top-customer-reviews-title"]',
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
        """Extract count of reviews from detail page (fallback)"""
        try:
            xpaths = [
                '//*[@id="acrCustomerReviewText"]',
                '//span[@id="acrCustomerReviewText"]',
                '//a[@id="acrCustomerReviewLink"]//span'
            ]

            for xpath in xpaths:
                reviews_text = self.extract_text_safe(tree, xpath)
                if reviews_text:
                    if "No customer reviews" in reviews_text:
                        return "0"
                    match = re.search(r'([\d,]+)', reviews_text)
                    if match:
                        return match.group(1)

            return None

        except Exception as e:
            print(f"  [WARNING] Failed to extract count of reviews: {e}")
            return None

    def extract_count_of_reviews_from_review_page(self, product_url):
        """Extract count_of_reviews from review page (same logic as dt2.py)"""
        try:
            tree = html.fromstring(self.driver.page_source)

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
                    print(f"  [DEBUG] Found review link with XPath #{idx}")
                    break

            if not review_link:
                print("  [WARNING] Could not find review page link, falling back to detail page")
                return None

            if review_link.startswith('http'):
                review_url = review_link
            else:
                review_url = "https://www.amazon.com" + review_link

            print(f"  [INFO] Navigating to review page...")
            self.driver.get(review_url)
            time.sleep(random.uniform(3, 4))

            count_of_reviews = None
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
                        match = re.search(r'([\d,]+)\s*customer\s*reviews?', count_text, re.IGNORECASE)
                        if not match:
                            match = re.search(r'([\d,]+)\s*with\s*reviews?', count_text, re.IGNORECASE)
                        if not match:
                            match = re.search(r'([\d,]+)\s*reviews?', count_text, re.IGNORECASE)
                        if match:
                            count_of_reviews = match.group(1)
                            print(f"  [OK] Extracted count_of_reviews from review page: {count_of_reviews}")
                            break

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
        """Extract total star rating count"""
        try:
            total_text = self.extract_text_safe(tree, '//*[@id="cm_cr_dp_d_rating_histogram"]/div[3]')
            if not total_text:
                return None

            total_match = re.search(r'([\d,]+)\s*global ratings?', total_text)
            if not total_match:
                return None

            total_count = int(total_match.group(1).replace(',', ''))
            return total_count

        except Exception as e:
            print(f"  [WARNING] Failed to extract star ratings count: {e}")
            return None

    def extract_final_sku_price(self, tree):
        """Extract final SKU price from detail page"""
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
                    match = re.search(r'\$[\d,]+\.?\d*', price_text)
                    if match:
                        return match.group()
                    return price_text.strip()

            return None

        except Exception as e:
            print(f"  [WARNING] Failed to extract final SKU price: {e}")
            return None

    def scrape_url(self, url):
        """Scrape a single URL and extract fields"""
        try:
            print(f"\n{'='*80}")
            print(f"[INFO] Accessing: {url[:80]}...")
            print(f"[INFO] Using account: {self.current_cookie_file}")

            self.driver.get(url)
            time.sleep(random.uniform(3, 5))

            print(f"  [INFO] Page loaded, extracting data...")

            page_source = self.driver.page_source
            tree = html.fromstring(page_source)

            # Extract retailer_sku_name first
            retailer_sku_name = self.extract_retailer_sku_name(tree)

            # Extract count_of_star_ratings (from detail page)
            count_of_star_ratings = self.extract_count_of_star_ratings(tree)

            # Extract count_of_reviews from review page (same as dt2.py)
            count_of_reviews = self.extract_count_of_reviews_from_review_page(url)

            # Re-parse page source after returning from review page
            tree = html.fromstring(self.driver.page_source)

            # Fallback: If count_of_reviews not found from review page, try detail page
            if not count_of_reviews:
                count_of_reviews = self.extract_count_of_reviews(tree)

            # Extract other fields from detail page
            final_sku_price = self.extract_final_sku_price(tree)
            star_rating = self.extract_star_rating(tree)

            # Log results
            print(f"\n  {'='*60}")
            print(f"  [RESULT] Extraction Results:")
            print(f"  {'='*60}")
            print(f"  retailer_sku_name     : {retailer_sku_name[:60] if retailer_sku_name else None}...")
            print(f"  final_sku_price       : {final_sku_price}")
            print(f"  count_of_reviews      : {count_of_reviews}")
            print(f"  star_rating           : {star_rating}")
            print(f"  count_of_star_ratings : {count_of_star_ratings}")
            print(f"  {'='*60}")

            return {
                'url': url,
                'retailer_sku_name': retailer_sku_name,
                'final_sku_price': final_sku_price,
                'count_of_reviews': count_of_reviews,
                'star_rating': star_rating,
                'count_of_star_ratings': count_of_star_ratings
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
            print(f"Amazon TV Test Crawler with Account Switching")
            print(f"Batch ID: {self.batch_id}")
            print(f"Switch account after: {ACCOUNT_SWITCH_AT} products")
            print("Fields: retailer_sku_name, final_sku_price, count_of_reviews, star_rating, count_of_star_ratings")
            print("="*80)

            if not test_urls:
                print("[ERROR] No test URLs provided")
                return

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
            if not self.setup_driver():
                print("[ERROR] Failed to setup WebDriver. Stopping.")
                return

            # Load initial cookies
            print("\n[INFO] Loading initial account cookies...")
            self.load_cookies(COOKIE_FILE_1)

            # Scrape each URL
            print(f"\n[INFO] Scraping {len(test_urls)} URLs...")
            print(f"[INFO] Account switch will occur after {ACCOUNT_SWITCH_AT} products")

            results = []
            for idx, url in enumerate(test_urls, 1):
                print(f"\n[{idx}/{len(test_urls)}] Processing...")

                # Check if we need to switch account
                if self.total_collected >= ACCOUNT_SWITCH_AT and not self.account_switched:
                    if not self.switch_account():
                        print("[WARNING] Account switch failed, continuing with current account...")

                result = self.scrape_url(url)
                if result:
                    results.append(result)
                    self.total_collected += 1

                # Random delay between requests
                if idx < len(test_urls):
                    delay = random.uniform(2, 4)
                    print(f"[INFO] Waiting {delay:.1f} seconds...")
                    time.sleep(delay)

            # Final summary
            print("\n" + "="*80)
            print("FINAL SUMMARY")
            print("="*80)
            print(f"Total products collected: {self.total_collected}")
            print(f"Account switched: {self.account_switched}")
            print("-"*80)

            for idx, r in enumerate(results, 1):
                account = "Account 1" if idx <= ACCOUNT_SWITCH_AT else "Account 2"
                print(f"\n[{idx}] ({account}) {r['url'][:50]}...")
                print(f"    retailer_sku_name     : {r['retailer_sku_name'][:50] if r['retailer_sku_name'] else None}...")
                print(f"    final_sku_price       : {r['final_sku_price']}")
                print(f"    count_of_reviews      : {r['count_of_reviews']}")
                print(f"    star_rating           : {r['star_rating']}")
                print(f"    count_of_star_ratings : {r['count_of_star_ratings']}")
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
    # Test URLs provided by user
    custom_urls = [
        'https://www.amazon.com/SuperSonic-1080p-Widescreen-Input-39-Inch/dp/B00SYG21O0/ref=sr_1_154',
        'https://www.amazon.com/Dungeon-Core-Complete-Boxed-Set-ebook/dp/B0DNVK9RW3/ref=sr_1_250',
        'https://www.amazon.com/Hisense-Class-Mini-LED-Google-65U8QG/dp/B0F1DV217B/ref=zg_bs_g_172659_d_sccl_40',
        # --- Account switch after 3 products ---
        'https://www.amazon.com/Sony-Exclusive-Features-PlayStation%C2%AE5-K-43S20M2/dp/B0DYKBNW89/ref=zg_bs_g_172659_d_sccl_42',
        'https://www.amazon.com/Monster-Portable-Entertainment-Playtime-Resistant/dp/B082349G1C/ref=zg_bs_g_172659_d_sccl_43',
        'https://www.amazon.com/TCL-55QM7K-120HZ-144HZ-Reflective-Television/dp/B0DVWXXRDL/ref=zg_bs_g_172659_d_sccl_44',
        'https://www.amazon.com/Westinghouse-24-inch-Television-Bluetooth-Connectivity/dp/B0FC34R88H/ref=zg_bs_g_172659_d_sccl_3',
        'https://www.amazon.com/tcl-fire-tv-75-inch-class-q65-qled-smart-tv/dp/B0D4PD6VDX/ref=zg_bs_g_172659_d_sccl_15',
        'https://www.amazon.com/amazon-fire-tv-65-inch-omni-series-4k-smart-tv/dp/B08T6J1HG8/ref=zg_bs_g_172659_d_sccl_16',
        'https://www.amazon.com/LG-Upscaling-Filmmaker-Orchestra-OLED65G5WUA/dp/B0DYQR8R98/ref=zg_bs_g_172659_d_sccl_19',
        'https://www.amazon.com/TinyTV-Portable-Television-Working-minature/dp/B0CMSGKMBC/ref=zg_bs_g_172659_d_sccl_20',
    ]

    try:
        crawler = AmazonTVTestCrawler()
        crawler.run(test_urls=custom_urls)
    except Exception as e:
        print(f"\n[FATAL ERROR] {e}")
        import traceback
        traceback.print_exc()

    print("\n[INFO] Test completed.")
