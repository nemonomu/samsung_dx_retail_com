"""
Test script to detect first duplicate product and show evidence
Does not save to DB - only logs duplicate information
"""
import time
import random
import psycopg2
import undetected_chromedriver as uc
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from lxml import html

# Database configuration
DB_CONFIG = {
    'host': 'samsung-dx-crawl.csnixzmkuppn.ap-northeast-2.rds.amazonaws.com',
    'port': 5432,
    'database': 'postgres',
    'user': 'postgres',
    'password': 'admin2025!'
}

class DuplicateDetector:
    def __init__(self):
        self.driver = None
        self.wait = None
        self.db_conn = None
        self.xpaths = {}
        self.seen_asins = {}  # ASIN -> (page_number, page_url, product_name)
        self.duplicates_found = []  # List of duplicate info
        self.max_duplicates = 10  # Stop after finding 10 duplicates

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
                WHERE mall_name = 'Amazon' AND page_type = 'main_page' AND is_active = TRUE
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
                FROM page_urls
                WHERE mall_name = 'Amazon' AND is_active = TRUE
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
        """Setup Chrome WebDriver with undetected_chromedriver"""
        print("[INFO] Setting up undetected Chrome driver...")

        options = uc.ChromeOptions()
        options.add_argument('--disable-blink-features=AutomationControlled')

        # Use undetected_chromedriver
        self.driver = uc.Chrome(options=options, version_main=None)
        self.driver.maximize_window()
        self.wait = WebDriverWait(self.driver, 10)

        print("[OK] Undetected WebDriver setup complete")

    def establish_session(self):
        """Establish Amazon session naturally"""
        print("\n" + "="*80)
        print("[INFO] Establishing Amazon session...")
        print("="*80)

        # Visit Amazon homepage first
        self.driver.get("https://www.amazon.com")
        time.sleep(5)

        print("\n[ACTION REQUIRED] Please check the browser window:")
        print("  1. If you see CAPTCHA, please solve it manually")
        print("  2. If you see 'Sorry! something went wrong!', refresh the page")
        print("  3. If page looks normal (Amazon homepage), you're good")
        print("  4. Press ENTER when ready to continue...")
        print("="*80 + "\n")

        input("Press ENTER to continue...")

        print("[OK] Session established, starting crawl...")

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

    def extract_product_name(self, element):
        """Extract product name with multiple fallback XPaths"""
        xpaths_to_try = [
            self.xpaths['product_name']['xpath'],
            './/h2/a/span',
            './/a[.//h2]//span',
            './/h2',
            './/span[@class="a-size-medium"]',
            './/span[@class="a-size-base-plus"]',
        ]

        for xpath in xpaths_to_try:
            result = self.extract_text_safe(element, xpath)
            if result and len(result.strip()) > 0:
                return result

        return None

    def check_page(self, url, page_number):
        """Check a single page for duplicate products"""
        try:
            print(f"\n{'='*80}")
            print(f"[PAGE {page_number}] Accessing: {url}")
            print(f"{'='*80}")

            self.driver.get(url)

            # Longer initial wait
            print(f"[INFO] Initial page load wait...")
            time.sleep(8)

            # Wait for search results to load
            print(f"[INFO] Waiting for search results to load...")
            try:
                WebDriverWait(self.driver, 20).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, "[data-component-type='s-search-result']"))
                )
                print(f"[OK] Search results detected")
                time.sleep(random.uniform(5, 7))
            except Exception as e:
                print(f"[WARNING] Timeout waiting for search results: {e}")
                time.sleep(3)

            # Get page source and parse
            page_source = self.driver.page_source
            tree = html.fromstring(page_source)

            # Find all product containers
            base_xpath = self.xpaths['base_container']['xpath']
            products = tree.xpath(base_xpath)

            print(f"[INFO] Found {len(products)} containers")

            # Filter out ads/widgets
            valid_products = []
            for product in products:
                cel_widget = product.get('cel_widget_id', '')
                component_type = product.get('data-component-type', '')
                data_component_id = product.get('data-component-id', '')

                is_excluded = False
                if 'loom-desktop' in cel_widget:
                    is_excluded = True
                elif 'sb-themed' in cel_widget:
                    is_excluded = True
                elif 'multi-brand' in cel_widget:
                    is_excluded = True
                elif component_type == 's-messaging-widget':
                    is_excluded = True
                elif 'VideoLandscapeCarouselWidget' in data_component_id:
                    is_excluded = True

                if not is_excluded:
                    data_index = product.get('data-index', '999')
                    try:
                        data_index = int(data_index)
                    except:
                        data_index = 999
                    valid_products.append((data_index, product))

            valid_products.sort(key=lambda x: x[0])
            valid_products = [product for _, product in valid_products]

            print(f"[INFO] Valid products: {len(valid_products)}")

            # VALIDATION: Check if we're actually on the correct page
            if len(valid_products) > 0:
                print(f"\n[VALIDATION] Checking if we're on the correct page...")
                expected_min_sr = (page_number - 1) * 16 + 1
                expected_max_sr = page_number * 16

                first_product = valid_products[0]
                first_url = self.extract_text_safe(first_product, self.xpaths['product_url']['xpath'])

                if first_url:
                    import re
                    sr_match = re.search(r'sr=8-(\d+)', first_url)
                    if sr_match:
                        actual_sr = int(sr_match.group(1))
                        print(f"[VALIDATION] Expected sr range: {expected_min_sr}-{expected_max_sr}")
                        print(f"[VALIDATION] Actual first product sr: {actual_sr}")

                        if actual_sr < expected_min_sr or actual_sr > expected_max_sr:
                            print(f"\n{'!'*80}")
                            print(f"🚨 PAGE MISMATCH DETECTED! 🚨")
                            print(f"{'!'*80}")
                            print(f"We requested page {page_number} but got products from a different page!")
                            print(f"Expected sr: {expected_min_sr}-{expected_max_sr}, Got: {actual_sr}")
                            print(f"This indicates the browser didn't navigate to the new page.")
                            print(f"{'!'*80}\n")

                            # Take screenshot for evidence
                            screenshot_path = f"page_mismatch_{page_number}_sr{actual_sr}.png"
                            self.driver.save_screenshot(screenshot_path)
                            print(f"[DEBUG] Screenshot saved: {screenshot_path}")

                            # Skip this page
                            print(f"[WARNING] Skipping page {page_number} due to mismatch.")
                            return True
                        else:
                            print(f"[VALIDATION] ✓ Page verification passed - we're on the correct page")
                    else:
                        print(f"[VALIDATION] Could not extract sr number from URL")
                else:
                    print(f"[VALIDATION] Could not extract URL for validation")

            # If no valid products found, try going to Amazon home first
            if len(valid_products) == 0:
                print(f"[WARNING] No valid products found. Trying workaround...")

                # Save screenshot for debugging
                screenshot_path = f"debug_page_{page_number}_before.png"
                self.driver.save_screenshot(screenshot_path)
                print(f"[DEBUG] Screenshot saved: {screenshot_path}")
                print(f"[DEBUG] Please share this screenshot to diagnose the issue")

                print(f"[INFO] Going to Amazon homepage first...")

                self.driver.get("https://www.amazon.com")
                time.sleep(random.uniform(2, 3))

                print(f"[INFO] Returning to search page...")
                self.driver.get(url)

                # Wait for search results again
                try:
                    WebDriverWait(self.driver, 15).until(
                        EC.presence_of_element_located((By.CSS_SELECTOR, "[data-component-type='s-search-result']"))
                    )
                    print(f"[OK] Search results detected after workaround")
                    time.sleep(random.uniform(4, 6))
                except Exception as e:
                    print(f"[ERROR] Still no results after workaround: {e}")
                    return True  # Continue to next page

                # Re-parse page
                page_source = self.driver.page_source
                tree = html.fromstring(page_source)
                products = tree.xpath(base_xpath)

                print(f"[INFO] Found {len(products)} containers after workaround")

                # Re-filter
                valid_products = []
                for product in products:
                    cel_widget = product.get('cel_widget_id', '')
                    component_type = product.get('data-component-type', '')
                    data_component_id = product.get('data-component-id', '')

                    is_excluded = False
                    if 'loom-desktop' in cel_widget:
                        is_excluded = True
                    elif 'sb-themed' in cel_widget:
                        is_excluded = True
                    elif 'multi-brand' in cel_widget:
                        is_excluded = True
                    elif component_type == 's-messaging-widget':
                        is_excluded = True
                    elif 'VideoLandscapeCarouselWidget' in data_component_id:
                        is_excluded = True

                    if not is_excluded:
                        data_index = product.get('data-index', '999')
                        try:
                            data_index = int(data_index)
                        except:
                            data_index = 999
                        valid_products.append((data_index, product))

                valid_products.sort(key=lambda x: x[0])
                valid_products = [product for _, product in valid_products]

                print(f"[INFO] Valid products after workaround: {len(valid_products)}")

                if len(valid_products) == 0:
                    print(f"[ERROR] Still no products after workaround. Skipping page.")

                    # Save screenshot after workaround failed
                    screenshot_path_after = f"debug_page_{page_number}_after.png"
                    self.driver.save_screenshot(screenshot_path_after)
                    print(f"[DEBUG] Screenshot after workaround saved: {screenshot_path_after}")

                    return True  # Continue to next page

            # Check each product for duplicates
            for idx, product in enumerate(valid_products, 1):
                asin = product.get('data-asin', None)
                if not asin or asin.strip() == '':
                    continue

                product_name = self.extract_product_name(product)
                if not product_name:
                    continue

                # Check if we've seen this ASIN before
                if asin in self.seen_asins:
                    prev_page, prev_url, prev_name = self.seen_asins[asin]

                    duplicate_info = {
                        'asin': asin,
                        'first_page': prev_page,
                        'first_url': prev_url,
                        'first_name': prev_name,
                        'duplicate_page': page_number,
                        'duplicate_url': url,
                        'duplicate_name': product_name
                    }
                    self.duplicates_found.append(duplicate_info)

                    print(f"\n{'#'*80}")
                    print(f"🚨 DUPLICATE #{len(self.duplicates_found)} DETECTED! 🚨")
                    print(f"{'#'*80}")
                    print(f"\n[FIRST OCCURRENCE]")
                    print(f"  Page Number: {prev_page}")
                    print(f"  Page URL: {prev_url}")
                    print(f"  Product Name: {prev_name}")
                    print(f"  ASIN: {asin}")

                    print(f"\n[DUPLICATE OCCURRENCE]")
                    print(f"  Page Number: {page_number}")
                    print(f"  Page URL: {url}")
                    print(f"  Product Name: {product_name}")
                    print(f"  ASIN: {asin}")

                    print(f"\n[ANALYSIS]")
                    print(f"  Same ASIN: {asin}")
                    print(f"  Same Product Name: {prev_name == product_name}")
                    print(f"  Pages Apart: {page_number - prev_page}")
                    print(f"{'#'*80}\n")

                    # Check if we've found enough duplicates
                    if len(self.duplicates_found) >= self.max_duplicates:
                        print(f"\n{'='*80}")
                        print(f"Found {self.max_duplicates} duplicates. Stopping crawler.")
                        print(f"{'='*80}\n")
                        return False  # Stop crawling

                    # Continue to find more duplicates
                    print(f"[INFO] Continuing to find more duplicates... ({len(self.duplicates_found)}/{self.max_duplicates})\n")
                    continue  # Skip storing, but continue crawling

                # Store this ASIN
                self.seen_asins[asin] = (page_number, url, product_name)
                print(f"  [{idx}] Stored: {product_name[:60]}... (ASIN: {asin})")

            return True  # Continue to next page

        except Exception as e:
            print(f"[ERROR] Failed to check page {page_number}: {e}")
            import traceback
            traceback.print_exc()
            return True

    def run(self):
        """Main execution"""
        try:
            print("="*80)
            print("Amazon Duplicate Detector - Test Mode")
            print("="*80)

            if not self.connect_db():
                return

            if not self.load_xpaths():
                return

            page_urls = self.load_page_urls()
            if not page_urls:
                print("[ERROR] No page URLs found")
                return

            self.setup_driver()

            # Establish session with manual verification
            self.establish_session()

            # Check each page until duplicate found
            for page_number, url in page_urls:
                if not self.check_page(url, page_number):
                    break  # Duplicate found, stop

                time.sleep(random.uniform(2, 4))

            print("\n" + "="*80)
            print("Test completed!")
            print(f"Total unique products seen: {len(self.seen_asins)}")
            print(f"Total duplicates found: {len(self.duplicates_found)}")
            print("="*80)

            if self.duplicates_found:
                print("\n" + "="*80)
                print("DUPLICATE SUMMARY")
                print("="*80)
                for i, dup in enumerate(self.duplicates_found, 1):
                    print(f"\n[DUPLICATE {i}]")
                    print(f"  ASIN: {dup['asin']}")
                    print(f"  Product: {dup['first_name'][:70]}...")
                    print(f"  First seen: Page {dup['first_page']}")
                    print(f"  Duplicated on: Page {dup['duplicate_page']}")
                    print(f"  Pages apart: {dup['duplicate_page'] - dup['first_page']}")
                print("="*80)

        except Exception as e:
            print(f"[ERROR] Test failed: {e}")
            import traceback
            traceback.print_exc()

        finally:
            if self.driver:
                self.driver.quit()
            if self.db_conn:
                self.db_conn.close()


if __name__ == "__main__":
    detector = DuplicateDetector()
    detector.run()
