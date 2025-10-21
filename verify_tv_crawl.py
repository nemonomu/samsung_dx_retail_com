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
        self.driver = None
        self.wait = None
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

    def load_page_url(self):
        """Load page 1 URL from database"""
        try:
            cursor = self.db_conn.cursor()
            cursor.execute("""
                SELECT url
                FROM page_urls
                WHERE mall_name = 'Amazon' AND page_number = 1 AND is_active = TRUE
            """)

            result = cursor.fetchone()
            cursor.close()

            if result:
                print(f"[OK] Loaded page 1 URL")
                return result[0]
            else:
                print("[ERROR] No URL found for page 1")
                return None

        except Exception as e:
            print(f"[ERROR] Failed to load page URL: {e}")
            return None

    def setup_driver(self):
        """Setup Chrome WebDriver with undetected_chromedriver"""
        print("[INFO] Setting up undetected Chrome driver...")

        options = uc.ChromeOptions()
        options.add_argument('--disable-blink-features=AutomationControlled')

        self.driver = uc.Chrome(options=options, version_main=None)
        self.driver.maximize_window()
        self.wait = WebDriverWait(self.driver, 10)

        print("[OK] Undetected WebDriver setup complete")

    def establish_session(self):
        """Establish Amazon session"""
        print("\n" + "="*80)
        print("[INFO] Establishing Amazon session...")
        print("="*80)

        self.driver.get("https://www.amazon.com")
        time.sleep(5)

        print("\n[ACTION REQUIRED] Please check the browser window:")
        print("  1. If you see CAPTCHA, please solve it manually")
        print("  2. If page looks normal (Amazon homepage), you're good")
        print("  3. Press ENTER when ready to continue...")
        print("="*80 + "\n")

        input("Press ENTER to continue...")

        print("[OK] Session established")

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

    def verify_page_1(self, url):
        """Verify what products are collected from page 1"""
        try:
            print(f"\n{'='*80}")
            print(f"[PAGE 1] Accessing: {url}")
            print(f"{'='*80}")

            self.driver.get(url)

            # Wait for page load
            print(f"[INFO] Waiting for search results to load...")
            time.sleep(8)

            try:
                WebDriverWait(self.driver, 20).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, "[data-component-type='s-search-result']"))
                )
                print(f"[OK] Search results detected")
                time.sleep(random.uniform(5, 7))
            except Exception as e:
                print(f"[WARNING] Timeout waiting for search results: {e}")
                time.sleep(3)

            # Parse page
            page_source = self.driver.page_source
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
            print(f"COLLECTED PRODUCTS FROM PAGE 1 ({len(valid_products)})")
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
            print(f"Total products that would be collected: {len(valid_products)}")
            print(f"{'='*80}\n")

            # Keep browser open for manual verification
            print("\n" + "="*80)
            print("[INFO] Verification complete!")
            print("[INFO] Browser will stay open for manual verification.")
            print("[INFO] Compare the products above with what you see in the browser.")
            print("[INFO] Press ENTER to close the browser and exit...")
            print("="*80 + "\n")
            input()

        except Exception as e:
            print(f"[ERROR] Failed to verify page: {e}")
            import traceback
            traceback.print_exc()

    def run(self):
        """Main execution"""
        try:
            print("="*80)
            print("TV Crawler Verifier - Page 1 Check")
            print("="*80)

            if not self.connect_db():
                return

            if not self.load_xpaths():
                return

            page_url = self.load_page_url()
            if not page_url:
                return

            self.setup_driver()
            self.establish_session()
            self.verify_page_1(page_url)

        except Exception as e:
            print(f"[ERROR] Verification failed: {e}")
            import traceback
            traceback.print_exc()

        finally:
            if self.driver:
                self.driver.quit()
            if self.db_conn:
                self.db_conn.close()


if __name__ == "__main__":
    verifier = TVCrawlerVerifier()
    verifier.run()
