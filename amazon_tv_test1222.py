"""
Amazon TV Test Script - Extract specific fields only
Fields: final_sku_price, count_of_reviews, count_of_star_ratings, star_ratings
Based on amazon_tv_dt1.py - No DB save, just extraction and logging
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
                print(f"  [DEBUG] Loaded XPath: {row[0]} = {row[1][:50]}...")

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
            # XPath: //*[@id="acrCustomerReviewText"]
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
                    # Extract only numbers and comma
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
            # Priority: data-hook attribute is most reliable
            review_link_xpaths = [
                '//a[@data-hook="see-all-reviews-link-foot"]/@href',  # Most reliable - data-hook attribute
                '//*[@id="reviews-medley-footer"]//a[contains(@href, "product-reviews")]/@href',  # Footer container
                '//*[@id="reviews-medley-footer"]/div[2]/a/@href',  # Legacy structure
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
                else:
                    print(f"  [DEBUG] XPath #{idx} not found")

            if not review_link:
                print("  [WARNING] Could not find review page link, falling back to detail page")
                return None

            # Extract ASIN from review link
            asin_match = re.search(r'/product-reviews/([A-Z0-9]{10})', review_link)
            if asin_match:
                asin = asin_match.group(1)
                print(f"  [DEBUG] Extracted ASIN from review link: {asin}")
            else:
                print(f"  [WARNING] Could not extract ASIN from review link")

            # Use original review link (Amazon requires specific ref parameter for pagination)
            if review_link.startswith('http'):
                review_url = review_link
            else:
                review_url = "https://www.amazon.com" + review_link

            print(f"  [INFO] Navigating to review page: {review_url[:80]}...")
            self.driver.get(review_url)
            time.sleep(random.uniform(3, 4))
            print(f"  [DEBUG] Actual URL after navigation: {self.driver.current_url[:80]}...")

            # Extract count_of_reviews from review page
            # XPath: //*[@id="filter-info-section"]/div or div[@data-hook="cr-filter-info-review-rating-count"]
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
                        print(f"  [DEBUG] count_text found: {count_text[:100]}...")
                        # Try multiple patterns
                        # Pattern 1: "385 customer reviews" or "1,234 customer reviews"
                        match = re.search(r'([\d,]+)\s*customer\s*reviews?', count_text, re.IGNORECASE)
                        if not match:
                            # Pattern 2: "385 with reviews" (from "1,891 global ratings, 385 with reviews")
                            match = re.search(r'([\d,]+)\s*with\s*reviews?', count_text, re.IGNORECASE)
                        if not match:
                            # Pattern 3: Just "385 reviews"
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
            import traceback
            traceback.print_exc()
            # Try to navigate back to product page
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

            # Extract number from "2,449 global ratings" or "1 global rating"
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
            import re

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
            # NOTE: Checked before "No featured offers" because they use same XPath location
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
                '//*[@id="corePriceDisplay_desktop_feature_div"]/div[1]/span[1]',  # New primary xpath
                '//*[@id="corePriceDisplay_desktop_feature_div"]/div[1]/span[3]/span[2]',  # Main container
                '//*[@id="corePriceDisplay_desktop_feature_div"]/div[1]/span[3]/span[2]/span[1]',  # Main with span[1]
                '//span[@class="a-price aok-align-center reinventPricePriceToPayMargin priceToPay"]//span[@class="a-offscreen"]',  # Generic offscreen
                '//*[@id="corePrice_feature_div"]/div/div/span[1]/span[1]',  # Side container
                '//*[@id="corePrice_feature_div"]//span[@class="a-offscreen"]'  # Side generic
            ]

            for xpath in xpaths:
                price_text = self.extract_text_safe(tree, xpath)
                if price_text:
                    # Extract only "$XXX.XX" or "$X,XXX.XX" format
                    # Remove "with X percent savings" and other extra text
                    match = re.search(r'\$[\d,]+\.?\d*', price_text)
                    if match:
                        return match.group()
                    # Fallback: return original if no price pattern found
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

            self.driver.get(url)
            time.sleep(random.uniform(3, 5))

            print(f"  [INFO] Page loaded, extracting data...")

            page_source = self.driver.page_source
            tree = html.fromstring(page_source)

            # Extract count_of_star_ratings first (from detail page)
            count_of_star_ratings = self.extract_count_of_star_ratings(tree)

            # Extract count_of_reviews from review page (same as dt1.py)
            count_of_reviews = self.extract_count_of_reviews_from_review_page(url)

            # Re-parse page source after returning from review page
            tree = html.fromstring(self.driver.page_source)

            # Fallback: If count_of_reviews not found from review page, try detail page
            if not count_of_reviews:
                count_of_reviews = self.extract_count_of_reviews(tree)

            # Extract other fields from detail page
            final_sku_price = self.extract_final_sku_price(tree)
            star_ratings = self.extract_star_rating(tree)

            # Log results
            print(f"\n  {'='*60}")
            print(f"  [RESULT] Extraction Results:")
            print(f"  {'='*60}")
            print(f"  final_sku_price       : {final_sku_price}")
            print(f"  count_of_reviews      : {count_of_reviews}")
            print(f"  count_of_star_ratings : {count_of_star_ratings}")
            print(f"  star_ratings          : {star_ratings}")
            print(f"  {'='*60}")

            return {
                'url': url,
                'final_sku_price': final_sku_price,
                'count_of_reviews': count_of_reviews,
                'count_of_star_ratings': count_of_star_ratings,
                'star_ratings': star_ratings
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
            print("Fields: final_sku_price, count_of_reviews, count_of_star_ratings, star_ratings")
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
                print(f"    final_sku_price       : {r['final_sku_price']}")
                print(f"    count_of_reviews      : {r['count_of_reviews']}")
                print(f"    count_of_star_ratings : {r['count_of_star_ratings']}")
                print(f"    star_ratings          : {r['star_ratings']}")
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
        'https://www.amazon.com/Dungeon-Core-Complete-Boxed-Set-ebook/dp/B0DNVK9RW3/ref=sr_1_249',
        'https://www.amazon.com/SAMSUNG-85-Inch-Terrace-QN85LST9C-WMN5870TC/dp/B0CK49MM6X/ref=sr_1_98',
        'https://www.amazon.com/Sony-Exclusive-Features-Playstation-K-65XR90/dp/B0D53N7BSK/ref=sr_1_99',
        'https://www.amazon.com/LG-Processor-Wireless-Connectivity-OLED83M3PUA/dp/B0CF3N22TS/ref=sr_1_108',
        'https://www.amazon.com/VIZIO-V-HDR-Smart-V756x-J03/dp/B09N6HWCM6/ref=sr_1_109',
        'https://www.amazon.com/Fire-TV-Omni-QLED-Series-65-inch/dp/B0DD2P7YVW/ref=sr_1_116',
        'https://www.amazon.com/Panasonic-77-inch-Adaptive-Refresh-Hands-Free/dp/B0FTGM5V2R/ref=sr_1_134',
        'https://www.amazon.com/Monster-Portable-Entertainment-Playtime-Resistant/dp/B082349G1C/ref=sr_1_144',
        'https://www.amazon.com/Hisense-75-Inch-Mini-LED-Google-75U9N/dp/B0D2PPJLGC/ref=sr_1_169',
        'https://www.amazon.com/TCL-Class-UHD-Smart-Roku/dp/B0CDCP8G4S/ref=sr_1_222',
        'https://www.amazon.com/FPD-43-inch-Chromecast-Palette-CG43-P3/dp/B0CRRRMK5W/ref=zg_bs_g_172659_d_sccl_86',
        'https://www.amazon.com/ONN-32-inch-Connectivity-100012589-Renewed/dp/B0BXFSJJXN/ref=zg_bs_g_172659_d_sccl_90',
        'https://www.amazon.com/Sony-Processor-Technology-Television-K-65XR80M2/dp/B0DYK7Y2YB/ref=zg_bs_g_172659_d_sccl_91',
        'https://www.amazon.com/TCL-55-Inch-NXTVISION-Google-Canvas/dp/B0DB6HGXGF/ref=zg_bs_g_172659_d_sccl_93',
        'https://www.amazon.com/SuperSonic-1080p-Widescreen-Input-39-Inch/dp/B00SYG21O0/ref=sr_1_154',
        'https://www.amazon.com/Hisense-Class-Mini-LED-Google-65U8QG/dp/B0F1DV217B/ref=zg_bs_g_172659_d_sccl_40',
        'https://www.amazon.com/Sony-Exclusive-Features-PlayStation%C2%AE5-K-43S20M2/dp/B0DYKBNW89/ref=zg_bs_g_172659_d_sccl_42',
        'https://www.amazon.com/TCL-55QM7K-120HZ-144HZ-Reflective-Television/dp/B0DVWXXRDL/ref=zg_bs_g_172659_d_sccl_44',
        'https://www.amazon.com/Westinghouse-24-inch-Television-Bluetooth-Connectivity/dp/B0FC34R88H/ref=zg_bs_g_172659_d_sccl_3',
        'https://www.amazon.com/tcl-fire-tv-75-inch-class-q65-qled-smart-tv/dp/B0D4PD6VDX/ref=zg_bs_g_172659_d_sccl_15',
        'https://www.amazon.com/amazon-fire-tv-65-inch-omni-series-4k-smart-tv/dp/B08T6J1HG8/ref=zg_bs_g_172659_d_sccl_16',
        'https://www.amazon.com/LG-Upscaling-Filmmaker-Orchestra-OLED65G5WUA/dp/B0DYQR8R98/ref=zg_bs_g_172659_d_sccl_19',
        'https://www.amazon.com/TinyTV-Portable-Television-Working-minature/dp/B0CMSGKMBC/ref=zg_bs_g_172659_d_sccl_20',
        'https://www.amazon.com/KTC-Portable-Standbyme-EDLA-Certified-Octa-core/dp/B0DMV9QJWR/ref=zg_bs_g_172659_d_sccl_23',
        'https://www.amazon.com/Roku-Smart-2025-Television-Entertainment/dp/B0DWGKMNND/ref=zg_bs_g_172659_d_sccl_24',
        'https://www.amazon.com/amazon-fire-tv-65-inch-omni-mini-led-series-smart-tv/dp/B0C1TQYNWX/ref=zg_bs_g_172659_d_sccl_25',
        'https://www.amazon.com/SYLVOX-15-6-Under-Cabinet-Smart/dp/B0BK1L1Z9G/ref=zg_bs_g_172659_d_sccl_95',
        'https://www.amazon.com/othoig-12-5inch-Portable-Essential-Lightweight/dp/B0DJ7JJBC8/ref=zg_bs_g_172659_d_sccl_96',
        'https://www.amazon.com/Cozyla-Mate-Portable-Support-Rotation/dp/B0D5CBHKXG/ref=zg_bs_g_172659_d_sccl_97',
        'https://www.amazon.com/Fire-TV-4-Series-50-inch/dp/B0DFNFX2K7/ref=sr_1_88',
        'https://www.amazon.com/TCL-115QM891G-Accelerator-Streaming-Television/dp/B0CZMLJNCQ/ref=sr_1_89',
        'https://www.amazon.com/SAMSUNG-Anti-Reflection-Bluetooth-Connection-QN85LST7C/dp/B0CNTRYS7K/ref=sr_1_93',
        'https://www.amazon.com/Samsung-Exclusive-Protection-Streaming-Beginners/dp/B0F5YK7LSR/ref=sr_1_215',
        'https://www.amazon.com/TCL-55-Inch-55Q650F-Streaming-Television/dp/B0C1J581SJ/ref=sr_1_220',
        'https://www.amazon.com/Reception-Amplifier-Channels-Supports-1080p-16-4ft/dp/B0G7C41G4V/ref=zg_bs_g_172659_d_sccl_83',
        'https://www.amazon.com/Sony-65-Inch-Backlight-Features-K-65XR70/dp/B0CVQ4FQJ9/ref=zg_bs_g_172659_d_sccl_86',
        'https://www.amazon.com/othoig-Compact-Digital-Charger-Kitchen/dp/B0FQCBBW6C/ref=zg_bs_g_172659_d_sccl_89',
        'https://www.amazon.com/Samsung-65-Inch-65QN80F-Tracking-Processor/dp/B0DXMVX717/ref=zg_bs_g_172659_d_sccl_90',
        'https://www.amazon.com/TCL-32S350G-Assistant-Compatible-Television/dp/B0C1HZ9HCM/ref=zg_bs_g_172659_d_sccl_94',
        'https://www.amazon.com/VIZIO-50-inch-Premium-Compatibility-M50QXM-K01/dp/B09VCZCR1W/ref=zg_bs_g_172659_d_sccl_99',
        'https://www.amazon.com/Westinghouse-Parental-Controls-Non-Smart-Monitor/dp/B09QRM1LVN/ref=zg_bs_g_172659_d_sccl_47',
        'https://www.amazon.com/Roku-Smart-2025-Television-Streaming/dp/B0DWHVZHBY/ref=zg_bs_g_172659_d_sccl_2',
        'https://www.amazon.com/Westinghouse-Roku-Connectivity-Compatible-Assistant/dp/B0BZT9Y3L9/ref=zg_bs_g_172659_d_sccl_8',
        'https://www.amazon.com/Feihe-Television-Kitchen-Bedroom-Entertainment/dp/B0F8C3VTVY/ref=zg_bs_g_172659_d_sccl_9',
        'https://www.amazon.com/LG-65-Inch-Processor-AI-Powered-OLED65C4PUA/dp/B0CVS18PH9/ref=zg_bs_g_172659_d_sccl_11',
        'https://www.amazon.com/Roku-Smart-TV-55-Inch-Backlit/dp/B0CVP6WK62/ref=zg_bs_g_172659_d_sccl_24',
        'https://www.amazon.com/SAMSUNG-65-Inch-Processor-Xcelerator-Samsung/dp/B0DXMJFJ7W/ref=zg_bs_g_172659_d_sccl_26',
        'https://www.amazon.com/VIZIO-720P-Smart-Dual-Band-WiFi/dp/B0D81P3D79/ref=zg_bs_g_172659_d_sccl_30',
        'https://www.amazon.com/TCL-40S350R-Compatible-Compatibility-Television/dp/B0C1J1TWQM/ref=zg_bs_g_172659_d_sccl_35',
        'https://www.amazon.com/Sony-65-Inch-Exclusive-Features-PlayStation%C2%AE5/dp/B0CVPMF4HQ/ref=zg_bs_g_172659_d_sccl_37',
        'https://www.amazon.com/SAMSUNG-65-Inch-Processor-Upscaling-Xcelerator/dp/B0DXMJGQWC/ref=zg_bs_g_172659_d_sccl_39',
        'https://www.amazon.com/TCL-98-inch-98Q651G-Multi-Chanel-Accelerator/dp/B0CZM4SDK4/ref=zg_bs_g_172659_d_sccl_19',
        'https://www.amazon.com/LG-Upscaling-Filmmaker-Orchestra-55QNED82AUA/dp/B0F1PDTQY2/ref=zg_bs_g_172659_d_sccl_21',
        'https://www.amazon.com/Roku-Brilliant-Automatic-Brightness-Streaming/dp/B0CLFD3NF5/ref=zg_bs_g_172659_d_sccl_28',
        'https://www.amazon.com/insignia-fire-tv-50-inch-class-f30-series-4k-smart-tv/dp/B0BTTVRWPR/ref=zg_bs_g_172659_d_sccl_81',
        'https://www.amazon.com/Samsung-Processor-Upscaling-Validated-Xcelerator/dp/B0FJ2KBSSV/ref=zg_bs_g_172659_d_sccl_92',
        'https://www.amazon.com/Tyler-Portable-Widescreen-Detachable-Antennas/dp/B01NH5M1ER/ref=zg_bs_g_172659_d_sccl_98',
    ]

    try:
        crawler = AmazonTVTestCrawler()
        crawler.run(test_urls=custom_urls)
    except Exception as e:
        print(f"\n[FATAL ERROR] {e}")
        import traceback
        traceback.print_exc()

    print("\n[INFO] Test completed.")
