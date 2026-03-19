"""
wmart_tv_test260319.py
- wmart_tv_main1.py 기반 테스트 스크립트
- available_quantity_for_purchase, inventory_status 두 컬럼만 로그 출력
- DB 저장 안 함
"""
import time
import random
import psycopg2
from DrissionPage import ChromiumPage
from lxml import html
import re

from config import DB_CONFIG
from wmart_config_loader import get_wmart_config


class WalmartTVTest:
    def __init__(self):
        self.page = None
        self.db_conn = None
        self.xpaths = {}
        self.config = get_wmart_config()

    def connect_db(self):
        try:
            self.db_conn = psycopg2.connect(**DB_CONFIG)
            print("[OK] Database connected")
            return True
        except Exception as e:
            print(f"[ERROR] Database connection failed: {e}")
            return False

    def load_xpaths(self):
        try:
            cursor = self.db_conn.cursor()
            cursor.execute("""
                SELECT data_field, xpath, css_selector, fallback_xpath
                FROM xpath_selectors
                WHERE mall_name = 'Walmart' AND page_type = 'main' AND is_active = TRUE
            """)

            for row in cursor.fetchall():
                self.xpaths[row[0]] = {
                    'xpath': row[1],
                    'css': row[2],
                    'fallback_xpath': row[3]
                }

            cursor.close()
            print(f"[OK] Loaded {len(self.xpaths)} XPath selectors")

            # 테스트 대상 xpath 출력
            for field in ['available_quantity', 'inventory_status']:
                if field in self.xpaths:
                    print(f"  {field}:")
                    print(f"    xpath:    {self.xpaths[field]['xpath']}")
                    print(f"    fallback: {self.xpaths[field].get('fallback_xpath')}")
            return True
        except Exception as e:
            print(f"[ERROR] Failed to load XPaths: {e}")
            return False

    def load_page_urls(self):
        try:
            cursor = self.db_conn.cursor()
            cursor.execute("""
                SELECT page_number, url
                FROM wmart_tv_main_page_url
                WHERE page_type = 'main' AND is_active = TRUE
                ORDER BY page_number
                LIMIT 1
            """)
            urls = cursor.fetchall()
            cursor.close()
            print(f"[OK] Loaded {len(urls)} page URL(s) for test")
            return urls
        except Exception as e:
            print(f"[ERROR] Failed to load page URLs: {e}")
            return []

    def setup_browser(self):
        try:
            self.page = ChromiumPage()
            print("[OK] Browser setup complete (DrissionPage)")
            return True
        except Exception as e:
            print(f"[ERROR] Browser setup failed: {e}")
            return False

    def extract_text_safe(self, element, xpath, fallback_xpath=None):
        try:
            result = element.xpath(xpath)
            if result:
                if isinstance(result[0], str):
                    return result[0].strip()
                else:
                    return result[0].text_content().strip()
            if fallback_xpath:
                result = element.xpath(fallback_xpath)
                if result:
                    if isinstance(result[0], str):
                        return result[0].strip()
                    else:
                        return result[0].text_content().strip()
            return None
        except Exception as e:
            return None

    def extract_number_only(self, text):
        if not text:
            return None
        match = re.search(r'(\d+)', text)
        if match:
            return match.group(1)
        return None

    def check_robot_page(self, page_source):
        robot_texts = self.config.get_robot_check_texts()
        for text in robot_texts:
            if text in page_source:
                return True
        return False

    def handle_captcha(self):
        try:
            print("[INFO] Checking for CAPTCHA...")
            page_content = self.page.html.lower()
            captcha_keywords = self.config.get_captcha_keywords()
            if any(keyword in page_content for keyword in captcha_keywords):
                print("[WARNING] CAPTCHA keywords found in page")
                captcha_wait = self.config.get_float('timing', 'captcha_wait', default=60)
                print(f"[INFO] CAPTCHA detection - waiting {int(captcha_wait)} seconds for manual intervention...")
                print("[INFO] Please solve CAPTCHA manually if present")
                time.sleep(captcha_wait)
                print("[INFO] Continuing after wait...")
                return True
            else:
                print("[INFO] No CAPTCHA detected")
                return True
        except Exception as e:
            print(f"[WARNING] CAPTCHA check failed: {e}")
            return True

    def scrape_page(self, url, page_number):
        try:
            print(f"\n[PAGE {page_number}] Accessing: {url[:80]}...")

            browse_wait = self.config.get_timing_range('browse_wait') or (10, 15)
            scroll_wait_range = self.config.get_timing_range('scroll_wait') or (1, 2)
            page_load_wait = self.config.get_timing_range('page_load_wait', 'wmart_tv_main1') or (8, 12)
            direct_url_wait = self.config.get_timing_range('direct_url_wait') or (12, 18)
            product_load_wait = self.config.get_timing_range('product_load_wait') or (5, 8)
            captcha_wait = self.config.get_float('timing', 'captcha_wait', default=60)
            scroll_wait = self.config.get_float('timing', 'scroll_wait', default=3)
            scroll_max_rounds = self.config.get_int('scroll', 'max_rounds', default=2)
            browse_tvs_url = self.config.get_url('browse_tvs') or "https://www.walmart.com/browse/electronics/tvs/3944_1060825"

            # For page 1, navigate naturally through browse page
            if page_number == 1:
                print("[INFO] Navigating to Walmart browse page first...")
                try:
                    self.page.get(browse_tvs_url)
                    time.sleep(random.uniform(*browse_wait))

                    if self.check_robot_page(self.page.html):
                        print("[WARNING] Robot detected on browse page, handling CAPTCHA...")
                        self.handle_captcha()
                        time.sleep(random.uniform(*scroll_wait_range) * 2)

                    if not self.check_robot_page(self.page.html):
                        print("[OK] Browse page loaded successfully")
                        time.sleep(random.uniform(*scroll_wait_range) * 2)

                        for _ in range(2):
                            self.page.run_js("window.scrollBy(0, 400)")
                            time.sleep(random.uniform(*scroll_wait_range))

                        print("[INFO] Now navigating to search page...")
                        self.page.get(url)
                        time.sleep(random.uniform(*page_load_wait))
                    else:
                        print("[WARNING] Robot still detected after CAPTCHA, using direct URL...")
                        self.page.get(url)
                        time.sleep(random.uniform(*direct_url_wait))
                except Exception as e:
                    print(f"[WARNING] Browse navigation failed: {e}, using direct URL...")
                    self.page.get(url)
                    time.sleep(random.uniform(*direct_url_wait))
            else:
                self.page.get(url)
                time.sleep(random.uniform(*direct_url_wait))

            # Check for robot detection
            page_source = None
            try:
                page_source = self.page.html
            except Exception as e:
                if "navigating" in str(e).lower():
                    print(f"[WARNING] Page still navigating, waiting {int(captcha_wait)}s...")
                    time.sleep(captcha_wait)
                    try:
                        page_source = self.page.html
                    except Exception as e2:
                        print(f"[ERROR] Still cannot get page content: {e2}")
                        return
                else:
                    raise

            if self.check_robot_page(page_source):
                print(f"[WARNING] Robot detection page detected.")
                self.handle_captcha()
                captcha_after_wait = self.config.get_timing_range('captcha_after_wait') or (3, 5)
                time.sleep(random.uniform(*captcha_after_wait))
                page_source = self.page.html

                if not self.check_robot_page(page_source):
                    print("[OK] Robot detection bypassed after CAPTCHA")
                else:
                    print("[ERROR] Robot detection still present. Aborting.")
                    return

            # Wait for page to load
            print("[INFO] Waiting for products to load...")
            time.sleep(random.uniform(*product_load_wait))

            # Scroll to load all products
            print("[INFO] Scrolling to load all products...")
            last_height = self.page.run_js("return document.body.scrollHeight")

            for scroll_round in range(scroll_max_rounds):
                self.page.run_js("window.scrollTo(0, document.body.scrollHeight)")
                time.sleep(scroll_wait)

                new_height = self.page.run_js("return document.body.scrollHeight")
                if new_height == last_height:
                    break
                last_height = new_height

            self.page.run_js("window.scrollTo(0, 0)")
            time.sleep(2)

            # Get page source and parse with lxml
            page_source = self.page.html
            tree = html.fromstring(page_source)

            # Find all product containers
            base_xpath = self.xpaths['base_container']['xpath']
            products = tree.xpath(base_xpath)

            print(f"[INFO] Found {len(products)} product containers")
            print(f"\n{'='*80}")
            print(f"{'No':>3} | {'Product Name':<50} | {'Avail Qty':>10} | {'Inventory Status'}")
            print(f"{'='*80}")

            for idx, product in enumerate(products, 1):
                product_name = self.extract_text_safe(product, self.xpaths['product_name']['xpath'])
                if not product_name:
                    continue

                # Extract Available_Quantity_for_Purchase
                available_quantity_raw = self.extract_text_safe(
                    product,
                    self.xpaths['available_quantity']['xpath'],
                    self.xpaths['available_quantity'].get('fallback_xpath')
                )
                available_quantity = self.extract_number_only(available_quantity_raw) if available_quantity_raw else None

                # Extract Inventory_Status
                inventory_status = self.extract_text_safe(
                    product,
                    self.xpaths['inventory_status']['xpath'],
                    self.xpaths['inventory_status'].get('fallback_xpath')
                )

                print(f"{idx:>3} | {product_name[:50]:<50} | {str(available_quantity or ''):>10} | {inventory_status or ''}")

            print(f"{'='*80}")
            print("[DONE] Test complete")

        except Exception as e:
            print(f"[ERROR] scrape_page failed: {e}")
            import traceback
            traceback.print_exc()

    def run(self):
        print("=" * 60)
        print("Walmart TV Test - available_quantity & inventory_status")
        print("=" * 60)

        if not self.connect_db():
            return
        if not self.load_xpaths():
            return

        urls = self.load_page_urls()
        if not urls:
            print("[ERROR] No URLs found")
            return

        if not self.setup_browser():
            return

        page_number, url = urls[0]
        self.scrape_page(url, page_number)

        if self.db_conn:
            self.db_conn.close()
        if self.page:
            self.page.quit()


if __name__ == '__main__':
    crawler = WalmartTVTest()
    crawler.run()
