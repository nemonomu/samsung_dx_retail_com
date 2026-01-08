"""
Amazon TV Test Script for Debugging Empty Fields
- Test 5 specific URLs with empty field issues
- Show DB XPath values and extraction results
- Debug why fields are empty
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

COOKIE_FILE = AMAZON_ACCOUNTS['unsandev0002']['cookie_file']

# Test URLs - 4 with all fields empty, 1 with only star_rating
TEST_URLS = [
    # All fields empty
    "https://www.amazon.com/TCL-65QM8K-120HZ-144HZ-Reflective-Television/dp/B0F53CZ4WT/ref=zg_bs_g_172659_d_sccl_8/139-9346033-8880350?psc=1",
    "https://www.amazon.com/Westinghouse-Roku-Connectivity-Compatible-Assistant/dp/B0BZT9Y3L9/ref=zg_bs_g_172659_d_sccl_12/139-9346033-8880350?psc=1",
    "https://www.amazon.com/INSIGNIA-fire-tv-65-inch-class-f50-series-4k-smart-tv/dp/B0DM2LQV3F/ref=zg_bs_g_172659_d_sccl_16/139-9346033-8880350?psc=1",
    "https://www.amazon.com/SAMSUNG-65-Inch-Processor-Xcelerator-Samsung/dp/B0DXMJFJ7W/ref=zg_bs_g_172659_d_sccl_22/139-9346033-8880350?psc=1",
    # Only star_rating has value
    "https://www.amazon.com/FPD-43-inch-Chromecast-Palette-CG43-P3/dp/B0CRRRMK5W/ref=zg_bs_g_172659_d_sccl_24/139-9346033-8880350?psc=1"
]


class AmazonTVDebugCrawler:
    def __init__(self):
        self.driver = None
        self.db_conn = None
        self.xpaths = {}
        korea_tz = pytz.timezone('Asia/Seoul')
        self.batch_id = datetime.now(korea_tz).strftime('%Y%m%d_%H%M%S')

    def connect_db(self):
        """Connect to PostgreSQL database"""
        try:
            self.db_conn = psycopg2.connect(**DB_CONFIG)
            self.db_conn.autocommit = True
            print("[OK] Database connected")
            return True
        except Exception as e:
            print(f"[ERROR] Database connection failed: {e}")
            return False

    def load_xpaths(self):
        """Load XPath selectors from database and show them"""
        try:
            print("\n[INFO] Loading XPath selectors from database...")
            cursor = self.db_conn.cursor()

            cursor.execute("""
                SELECT data_field, xpath
                FROM xpath_selectors
                WHERE mall_name = 'Amazon' AND page_type = 'detail_page' AND is_active = TRUE
            """)

            rows = cursor.fetchall()
            print("\n" + "=" * 80)
            print("DB XPath Selectors:")
            print("=" * 80)
            for row in rows:
                self.xpaths[row[0]] = row[1]
                print(f"  {row[0]}: {row[1]}")
            print("=" * 80 + "\n")

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

    def load_cookies(self):
        """Load cookies from file"""
        print(f"[INFO] Loading cookies from {COOKIE_FILE}...")

        if not os.path.exists(COOKIE_FILE):
            print(f"[WARNING] Cookie file not found: {COOKIE_FILE}")
            return False

        try:
            self.driver.get("https://www.amazon.com")
            time.sleep(2)

            with open(COOKIE_FILE, 'rb') as f:
                cookies = pickle.load(f)
                for cookie in cookies:
                    try:
                        self.driver.add_cookie(cookie)
                    except:
                        pass

            self.driver.refresh()
            time.sleep(2)
            print(f"[OK] Cookies loaded successfully")
            return True

        except Exception as e:
            print(f"[WARNING] Failed to load cookies: {e}")
            return False

    def extract_text_safe(self, tree, xpath, label=""):
        """Safely extract text from XPath with debug info"""
        if not xpath:
            print(f"    [DEBUG] {label}: XPath is None")
            return None
        try:
            elements = tree.xpath(xpath)
            if elements:
                if isinstance(elements[0], str):
                    result = elements[0].strip()
                else:
                    result = elements[0].text_content().strip()
                print(f"    [DEBUG] {label}: Found - '{result[:80]}...'" if len(str(result)) > 80 else f"    [DEBUG] {label}: Found - '{result}'")
                return result
            else:
                print(f"    [DEBUG] {label}: No match for xpath: {xpath[:60]}...")
            return None
        except Exception as e:
            print(f"    [DEBUG] {label}: Error - {e}")
            return None

    def extract_star_rating(self, tree):
        """Extract star rating with debug"""
        print("\n  [EXTRACT] star_rating:")

        # Try DB XPath first
        db_xpath = self.xpaths.get('star_rating')
        print(f"    [DEBUG] DB xpath for star_rating: {db_xpath}")

        star_rating_text = self.extract_text_safe(tree, db_xpath, "DB XPath")

        if star_rating_text:
            if "No customer reviews" in star_rating_text:
                return "No customer reviews"
            if re.search(r'\d', star_rating_text):
                # Extract just the number
                match = re.search(r'(\d+\.?\d*)', star_rating_text)
                if match:
                    return match.group(1)
                return star_rating_text

        # Fallback XPaths
        fallback_xpaths = [
            '//*[@id="acrPopover"]/@title',
            '//span[@data-hook="rating-out-of-text"]',
            '//*[@id="averageCustomerReviews"]//span[@class="a-icon-alt"]'
        ]

        print("    [DEBUG] Trying fallback XPaths...")
        for xpath in fallback_xpaths:
            text = self.extract_text_safe(tree, xpath, f"Fallback {xpath[:40]}")
            if text:
                if re.search(r'\d', text):
                    match = re.search(r'(\d+\.?\d*)', text)
                    if match:
                        return match.group(1)
                    return text

        # Check for "No customer reviews"
        no_reviews_xpaths = [
            '//*[@id="cm-cr-dp-review-header"]/h3/span',
            '//span[@data-hook="top-customer-reviews-title"]',
            '//span[contains(text(), "No customer reviews")]'
        ]

        for xpath in no_reviews_xpaths:
            text = self.extract_text_safe(tree, xpath, "No reviews check")
            if text and "No customer reviews" in text:
                return "No customer reviews"

        return None

    def extract_count_of_star_ratings(self, tree):
        """Extract total star rating count with debug"""
        print("\n  [EXTRACT] count_of_star_ratings:")

        # Primary XPath
        primary_xpath = '//*[@id="cm_cr_dp_d_rating_histogram"]/div[3]'
        total_text = self.extract_text_safe(tree, primary_xpath, "Primary XPath")

        if total_text:
            total_match = re.search(r'([\d,]+)\s*global ratings?', total_text)
            if total_match:
                result = int(total_match.group(1).replace(',', ''))
                print(f"    [DEBUG] Extracted count: {result}")
                return result

        # Fallback XPaths
        fallback_xpaths = [
            '//*[@id="acrCustomerReviewText"]',
            '//span[@id="acrCustomerReviewText"]'
        ]

        print("    [DEBUG] Trying fallback XPaths...")
        for xpath in fallback_xpaths:
            text = self.extract_text_safe(tree, xpath, f"Fallback")
            if text:
                match = re.search(r'([\d,]+)', text)
                if match:
                    result = int(match.group(1).replace(',', ''))
                    print(f"    [DEBUG] Extracted count from fallback: {result}")
                    return result

        return None

    def extract_final_sku_price(self, tree):
        """Extract final SKU price with debug"""
        print("\n  [EXTRACT] final_sku_price:")

        # Check special cases first
        special_cases = [
            ('Currently unavailable', [
                '//*[@id="outOfStock"]/div/div[1]/span[1]',
                '//*[@id="availability"]/span[2]/span'
            ]),
            ('Price higher than typical', [
                '//*[@id="fod-cx-message-with-learn-more"]/span[1]'
            ]),
            ('No featured offers', [
                '//*[@id="fod-cx-message-with-learn-more"]/span[1]'
            ]),
            ('See price in cart', [
                '//*[@id="corePriceDisplay_desktop_feature_div"]/table/tbody/tr/td[2]/span/a'
            ])
        ]

        for case_name, xpaths in special_cases:
            for xpath in xpaths:
                text = self.extract_text_safe(tree, xpath, f"Special: {case_name}")
                if text and case_name.lower() in text.lower():
                    return case_name

        # Normal price extraction
        price_xpaths = [
            '//*[@id="corePriceDisplay_desktop_feature_div"]/div[1]/span[1]',
            '//*[@id="corePriceDisplay_desktop_feature_div"]/div[1]/span[3]/span[2]',
            '//*[@id="corePriceDisplay_desktop_feature_div"]/div[1]/span[3]/span[2]/span[1]',
            '//span[@class="a-price aok-align-center reinventPricePriceToPayMargin priceToPay"]//span[@class="a-offscreen"]',
            '//*[@id="corePrice_feature_div"]/div/div/span[1]/span[1]',
            '//*[@id="corePrice_feature_div"]//span[@class="a-offscreen"]',
            '//span[@class="a-offscreen"]'
        ]

        print("    [DEBUG] Trying price XPaths...")
        for xpath in price_xpaths:
            price_text = self.extract_text_safe(tree, xpath, "Price")
            if price_text:
                match = re.search(r'\$[\d,]+\.?\d*', price_text)
                if match:
                    return match.group()

        return None

    def extract_count_of_reviews_from_review_page(self, product_url):
        """Extract count_of_reviews from review page"""
        print("\n  [EXTRACT] count_of_reviews (from review page):")
        try:
            tree = html.fromstring(self.driver.page_source)

            review_link_xpaths = [
                '//a[@data-hook="see-all-reviews-link-foot"]/@href',
                '//*[@id="reviews-medley-footer"]//a[contains(@href, "product-reviews")]/@href',
                '//a[contains(@href, "product-reviews")]/@href'
            ]

            review_link = None
            for idx, xpath in enumerate(review_link_xpaths, 1):
                result = tree.xpath(xpath)
                if result:
                    review_link = result[0]
                    print(f"    [DEBUG] Found review link with XPath #{idx}")
                    break

            if not review_link:
                print("    [WARNING] Could not find review page link")
                return None

            if review_link.startswith('http'):
                review_url = review_link
            else:
                review_url = "https://www.amazon.com" + review_link

            print(f"    [INFO] Navigating to review page...")
            self.driver.get(review_url)
            time.sleep(random.uniform(3, 4))

            count_of_reviews = None
            tree = html.fromstring(self.driver.page_source)

            count_xpaths = [
                '//*[@id="filter-info-section"]/div',
                '//div[@data-hook="cr-filter-info-review-rating-count"]'
            ]

            for xpath in count_xpaths:
                count_elements = tree.xpath(xpath)
                if count_elements:
                    count_text = count_elements[0].text_content().strip()
                    if count_text:
                        print(f"    [DEBUG] Review page text: {count_text[:100]}...")
                        match = re.search(r'([\d,]+)\s*(?:customer\s*reviews?|with\s*reviews?|reviews?)', count_text, re.IGNORECASE)
                        if match:
                            count_of_reviews = match.group(1)
                            print(f"    [OK] Extracted: {count_of_reviews}")
                            break

            # Navigate back
            print(f"    [INFO] Navigating back to product page...")
            self.driver.get(product_url)
            time.sleep(random.uniform(2, 3))

            return count_of_reviews

        except Exception as e:
            print(f"    [ERROR] Failed: {e}")
            try:
                self.driver.get(product_url)
                time.sleep(2)
            except:
                pass
            return None

    def scrape_url(self, url, idx):
        """Scrape a single URL with detailed debugging"""
        try:
            print(f"\n{'='*80}")
            print(f"[{idx}/5] Processing URL:")
            print(f"  {url}")
            print("=" * 80)

            self.driver.get(url)
            time.sleep(random.uniform(4, 6))

            # Check current URL (in case of redirect)
            current_url = self.driver.current_url
            print(f"\n[INFO] Current URL: {current_url[:80]}...")

            # Check page title
            title = self.driver.title
            print(f"[INFO] Page title: {title[:60]}...")

            page_source = self.driver.page_source
            tree = html.fromstring(page_source)

            # Extract retailer_sku_name first (BEFORE review page)
            print("\n  [EXTRACT] retailer_sku_name:")
            db_xpath = self.xpaths.get('product_name')
            print(f"    [DEBUG] DB xpath for product_name: {db_xpath}")
            retailer_sku_name = self.extract_text_safe(tree, db_xpath, "DB XPath")
            if not retailer_sku_name:
                fallback = '//*[@id="productTitle"]'
                retailer_sku_name = self.extract_text_safe(tree, fallback, "Fallback")

            # Extract star_rating (BEFORE review page - same as dt1.py)
            star_rating = self.extract_star_rating(tree)

            # Extract count_of_star_ratings (BEFORE review page)
            count_of_star_ratings = self.extract_count_of_star_ratings(tree)

            # Now go to review page
            count_of_reviews = self.extract_count_of_reviews_from_review_page(url)

            # Re-parse page after returning from review page
            print("\n  [INFO] Re-parsing page after review page return...")
            tree_after = html.fromstring(self.driver.page_source)

            # Fallback for count_of_reviews
            if not count_of_reviews:
                print("  [INFO] Fallback: extracting count_of_reviews from detail page...")
                xpaths = ['//*[@id="acrCustomerReviewText"]']
                for xpath in xpaths:
                    text = self.extract_text_safe(tree_after, xpath, "Fallback")
                    if text:
                        match = re.search(r'([\d,]+)', text)
                        if match:
                            count_of_reviews = match.group(1)
                            break

            # Extract final_sku_price (AFTER review page - same as dt1.py)
            final_sku_price = self.extract_final_sku_price(tree_after)

            # Summary
            print(f"\n  {'='*60}")
            print(f"  [SUMMARY] Extraction Results:")
            print(f"  {'='*60}")
            print(f"  retailer_sku_name     : {retailer_sku_name[:60] if retailer_sku_name else 'None'}...")
            print(f"  star_rating           : {star_rating}")
            print(f"  count_of_star_ratings : {count_of_star_ratings}")
            print(f"  count_of_reviews      : {count_of_reviews}")
            print(f"  final_sku_price       : {final_sku_price}")
            print(f"  {'='*60}")

            return {
                'url': url,
                'retailer_sku_name': retailer_sku_name,
                'star_rating': star_rating,
                'count_of_star_ratings': count_of_star_ratings,
                'count_of_reviews': count_of_reviews,
                'final_sku_price': final_sku_price
            }

        except Exception as e:
            print(f"  [ERROR] Failed to scrape: {e}")
            import traceback
            traceback.print_exc()
            return None

    def run(self):
        """Run the debug test"""
        print("=" * 80)
        print("Amazon TV Debug Test")
        print(f"Testing {len(TEST_URLS)} URLs with empty field issues")
        print("=" * 80)

        # Step 1: Connect to database
        print("\n[STEP 1] Connecting to database...")
        if not self.connect_db():
            return

        # Step 2: Load XPaths
        print("\n[STEP 2] Loading XPath selectors...")
        if not self.load_xpaths():
            return

        # Step 3: Setup WebDriver
        print("\n[STEP 3] Setting up WebDriver...")
        if not self.setup_driver():
            return

        # Step 4: Load cookies
        print("\n[STEP 4] Loading cookies...")
        self.load_cookies()

        # Step 5: Process each URL
        print(f"\n[STEP 5] Processing {len(TEST_URLS)} URLs...")
        results = []
        for idx, url in enumerate(TEST_URLS, 1):
            result = self.scrape_url(url, idx)
            if result:
                results.append(result)
            time.sleep(random.uniform(2, 3))

        # Final summary
        print("\n" + "=" * 80)
        print("FINAL SUMMARY")
        print("=" * 80)
        for r in results:
            print(f"\nURL: {r['url'][:60]}...")
            print(f"  Name: {'✓' if r['retailer_sku_name'] else '✗'}")
            print(f"  Star Rating: {'✓ ' + str(r['star_rating']) if r['star_rating'] else '✗'}")
            print(f"  Star Count: {'✓ ' + str(r['count_of_star_ratings']) if r['count_of_star_ratings'] else '✗'}")
            print(f"  Review Count: {'✓ ' + str(r['count_of_reviews']) if r['count_of_reviews'] else '✗'}")
            print(f"  Price: {'✓ ' + str(r['final_sku_price']) if r['final_sku_price'] else '✗'}")

        # Cleanup
        if self.driver:
            self.driver.quit()
        if self.db_conn:
            self.db_conn.close()


if __name__ == "__main__":
    crawler = AmazonTVDebugCrawler()
    crawler.run()
