"""
Verify TV Crawler - Check what products are collected from page 1
Only prints product names, doesn't save to DB
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

class TVCrawlerVerifier:
    def __init__(self):
        self.drivers = []  # Store all driver instances
        self.db_conn = None
        self.xpaths = {}

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
        """Load URLs for pages 11-20 from database"""
        try:
            cursor = self.db_conn.cursor()
            cursor.execute("""
                SELECT page_number, url
                FROM page_urls
                WHERE mall_name = 'Amazon' AND page_number >= 11 AND page_number <= 20 AND is_active = TRUE
                ORDER BY page_number
            """)

            results = cursor.fetchall()
            cursor.close()

            if results:
                print(f"[OK] Loaded {len(results)} page URLs")
                return results
            else:
                print("[ERROR] No URLs found")
                return []

        except Exception as e:
            print(f"[ERROR] Failed to load page URLs: {e}")
            return []

    def create_new_driver(self):
        """Create a new Chrome WebDriver instance"""
        options = uc.ChromeOptions()
        options.add_argument('--disable-blink-features=AutomationControlled')

        driver = uc.Chrome(options=options, version_main=None)
        driver.maximize_window()

        return driver

    def establish_session(self, driver, page_num):
        """Establish Amazon session for a specific driver"""
        print(f"\n[PAGE {page_num}] Establishing session...")

        driver.get("https://www.amazon.com")
        time.sleep(3)

        print(f"[PAGE {page_num}] Session established")

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

    def verify_page(self, driver, url, page_number):
        """Verify what products are collected from a page"""
        try:
            print(f"\n{'='*80}")
            print(f"[PAGE {page_number}] Accessing: {url}")
            print(f"{'='*80}")

            driver.get(url)

            # Wait for page load
            print(f"[INFO] Waiting for search results to load...")
            time.sleep(8)

            try:
                WebDriverWait(driver, 20).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, "[data-component-type='s-search-result']"))
                )
                print(f"[OK] Search results detected")
                time.sleep(random.uniform(5, 7))
            except Exception as e:
                print(f"[WARNING] Timeout waiting for search results: {e}")
                time.sleep(3)

            # Parse page
            page_source = driver.page_source
            tree = html.fromstring(page_source)

            # Find all product containers
            base_xpath = self.xpaths['base_container']['xpath']
            products = tree.xpath(base_xpath)

            print(f"\n[INFO] Found {len(products)} total containers")

            # Filter products
            valid_products = []
            excluded_products = []

            for product in products:
                cel_widget = product.get('cel_widget_id', '')
                component_type = product.get('data-component-type', '')
                data_component_id = product.get('data-component-id', '')

                is_excluded = False
                exclude_reason = ''

                if 'loom-desktop' in cel_widget:
                    is_excluded = True
                    exclude_reason = 'loom-desktop'
                elif 'sb-themed' in cel_widget:
                    is_excluded = True
                    exclude_reason = 'sb-themed'
                elif 'multi-brand' in cel_widget:
                    is_excluded = True
                    exclude_reason = 'multi-brand'
                elif 'FEATURED_ASINS_LIST' in cel_widget:
                    is_excluded = True
                    exclude_reason = 'FEATURED_ASINS_LIST'
                elif component_type == 's-messaging-widget':
                    is_excluded = True
                    exclude_reason = 's-messaging-widget'
                elif 'VideoLandscapeCarouselWidget' in data_component_id:
                    is_excluded = True
                    exclude_reason = 'VideoLandscapeCarouselWidget'

                if is_excluded:
                    product_name = self.extract_product_name(product)
                    excluded_products.append({
                        'name': product_name,
                        'reason': exclude_reason,
                        'cel_widget': cel_widget
                    })
                else:
                    data_index = product.get('data-index', '999')
                    try:
                        data_index = int(data_index)
                    except:
                        data_index = 999
                    valid_products.append((data_index, product))

            # Sort by data-index
            valid_products.sort(key=lambda x: x[0])
            valid_products = [product for _, product in valid_products]

            print(f"[INFO] Valid products: {len(valid_products)}")
            print(f"[INFO] Excluded products: {len(excluded_products)}")

            # Print excluded products
            if excluded_products:
                print(f"\n{'='*80}")
                print(f"EXCLUDED PRODUCTS ({len(excluded_products)})")
                print(f"{'='*80}")
                for i, excluded in enumerate(excluded_products, 1):
                    print(f"\n[EXCLUDED {i}]")
                    print(f"  Reason: {excluded['reason']}")
                    print(f"  cel_widget_id: {excluded['cel_widget']}")
                    print(f"  Name: {excluded['name'][:70] if excluded['name'] else 'N/A'}...")

            # Print valid products
            print(f"\n{'='*80}")
            print(f"COLLECTED PRODUCTS FROM PAGE {page_number} ({len(valid_products)})")
            print(f"{'='*80}")

            for idx, product in enumerate(valid_products, 1):
                asin = product.get('data-asin', 'N/A')
                product_name = self.extract_product_name(product)

                if product_name:
                    print(f"\n[{idx}] ASIN: {asin}")
                    print(f"    {product_name}")
                else:
                    print(f"\n[{idx}] ASIN: {asin} - [NO NAME EXTRACTED]")

            print(f"\n{'='*80}")
            print(f"Page {page_number} - Total products: {len(valid_products)}")
            print(f"{'='*80}\n")

        except Exception as e:
            print(f"[ERROR] Failed to verify page: {e}")
            import traceback
            traceback.print_exc()

    def run(self):
        """Main execution"""
        try:
            print("="*80)
            print("TV Crawler Verifier - Pages 11-20 Check")
            print("="*80)

            if not self.connect_db():
                return

            if not self.load_xpaths():
                return

            page_urls = self.load_page_urls()
            if not page_urls:
                return

            print(f"\n[INFO] Will verify {len(page_urls)} pages")
            print(f"[INFO] Each page will open in a new Chrome window")
            print(f"[INFO] Please solve CAPTCHA on first window if needed")

            input("\nPress ENTER to start verification...")

            # Process each page in a new browser window
            for page_number, url in page_urls:
                print(f"\n{'#'*80}")
                print(f"Opening new Chrome window for PAGE {page_number}")
                print(f"{'#'*80}")

                # Create new driver for this page
                driver = self.create_new_driver()
                self.drivers.append(driver)

                # Establish session (only prompt for CAPTCHA on first page)
                if page_number == 1:
                    print("\n[ACTION REQUIRED] Solve CAPTCHA on first window if needed")
                    self.establish_session(driver, page_number)
                    input(f"\nPress ENTER when ready to crawl page {page_number}...")
                else:
                    self.establish_session(driver, page_number)

                # Verify this page
                self.verify_page(driver, url, page_number)

                # Small delay between opening windows
                time.sleep(2)

            # All pages done
            print("\n" + "="*80)
            print(f"[INFO] Verification complete!")
            print(f"[INFO] {len(self.drivers)} Chrome windows are open")
            print(f"[INFO] Review each window to verify the collected products")
            print(f"[INFO] Press ENTER to close all browsers and exit...")
            print("="*80 + "\n")
            input()

        except Exception as e:
            print(f"[ERROR] Verification failed: {e}")
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
    verifier = TVCrawlerVerifier()
    verifier.run()
