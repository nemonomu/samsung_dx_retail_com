import time
import random
import re
import sys
import os
import pickle
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

# Import database / account configuration
from config import DB_CONFIG, AMAZON_ACCOUNTS
from amazon_config_loader import get_amazon_config

# Load config from DB
_config = get_amazon_config()

# Cookie file (DB primary account, fallback to dt1's account if absent)
_ACCOUNT_NAME = (_config.get_account('primary', 'amazon_tv_main1')
                 or _config.get_account('primary', 'amazon_tv_dt1')
                 or 'ltyinvestmentl')
_account_cfg = AMAZON_ACCOUNTS.get(_ACCOUNT_NAME, {})
COOKIE_FILE = _account_cfg.get('cookie_file') or f'amazon_cookies_{_ACCOUNT_NAME}.pkl'


# Tee class for logging to both console and file
class Tee:
    def __init__(self, log_file_path):
        self.terminal = sys.stdout
        self.log_file = open(log_file_path, 'a', encoding='utf-8')

    def write(self, message):
        self.terminal.write(message)
        if not self.log_file.closed:
            self.log_file.write(message)
            self.log_file.flush()

    def flush(self):
        self.terminal.flush()
        if not self.log_file.closed:
            self.log_file.flush()

    def close(self):
        if not self.log_file.closed:
            self.log_file.close()


class AmazonTVCrawler:
    def __init__(self):
        self.driver = None
        self.wait = None
        self.db_conn = None
        self.xpaths = {}
        self.total_collected = 0
        self.max_skus = _config.get_constant_int('max_skus', 'amazon_tv_main1', 400)
        self.sorry_page_max_retry = _config.get_retry('sorry_page_max', 'amazon_tv_main1', 3)
        self.sequential_id = 1  # ID counter for 1-max_skus
        self.batch_id = None  # Batch ID for this crawling session
        self.excluded_items = set()  # Items with is_product=false in tv_item_mst
        self._exit_code = 1  # Default to failure, set to 0 on success

    def connect_db(self):
        """Connect to PostgreSQL database"""
        try:
            self.db_conn = psycopg2.connect(**DB_CONFIG)
            self.db_conn.autocommit = True  # Enable autocommit mode
            print("[OK] Database connected (autocommit enabled)")
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
                FROM amazon_tv_main_page_url
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

    def load_excluded_items(self):
        """Load items with is_product=false from tv_item_mst"""
        try:
            cursor = self.db_conn.cursor()
            cursor.execute("""
                SELECT item FROM tv_item_mst
                WHERE is_product = FALSE AND item IS NOT NULL
            """)

            for row in cursor.fetchall():
                self.excluded_items.add(row[0])

            cursor.close()
            print(f"[OK] Loaded {len(self.excluded_items)} excluded items (is_product=false)")
            return True

        except Exception as e:
            print(f"[WARNING] Failed to load excluded items: {e}")
            return False

    def extract_asin(self, url):
        """Extract ASIN from Amazon URL.

        sspa/click 스폰서드 URL은 redirect_url 파라미터 안에 ASIN이 URL-encoded
        (`%2Fdp%2FXXXX`) 또는 이중 인코딩(`%252Fdp%252FXXXX`)으로 들어있어
        일반 /dp/ 매칭으로는 못 잡음. unquote를 두 번 적용하면 두 깊이 모두 평탄화.
        """
        if not url:
            return None
        try:
            from urllib.parse import unquote
            decoded = unquote(unquote(url))
            match = re.search(r'/dp/([A-Z0-9]{10})', decoded)
            if match:
                return match.group(1)
            return None
        except:
            return None

    def setup_driver(self):
        """Setup Chrome WebDriver"""
        user_agent = _config.get_browser('user_agent', 'amazon_tv_main1') or 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36'
        chrome_options = Options()
        chrome_options.add_argument('--disable-blink-features=AutomationControlled')
        chrome_options.add_argument(f'--user-agent={user_agent}')
        chrome_options.add_argument('--disable-dev-shm-usage')
        chrome_options.add_argument('--no-sandbox')
        chrome_options.add_argument('--window-size=1920,1080')
        chrome_options.add_argument('--start-maximized')
        chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
        chrome_options.add_experimental_option('useAutomationExtension', False)

        # Add more realistic browser settings
        chrome_options.add_argument('--disable-gpu')
        chrome_options.add_argument('--lang=en-US,en;q=0.9')

        # Add preferences to appear more like a real browser
        prefs = {
            "profile.default_content_setting_values.notifications": 2,
            "credentials_enable_service": False,
            "profile.password_manager_enabled": False
        }
        chrome_options.add_experimental_option("prefs", prefs)

        service = Service(ChromeDriverManager().install())

        # Browser launch with 3-attempt retry (dt1/bsr1 패턴 이식)
        # Why retry: 직전에 종료한 Chromium 의 자식 프로세스/디버그 포트 점유가 OS 에서 정리되기
        # 전 재호출되면 'session not created: Chrome instance exited' 로 실패. 짧은 대기 후
        # 재시도하면 대개 회복됨.
        max_attempts = 3
        last_error = None
        for attempt in range(1, max_attempts + 1):
            try:
                self.driver = webdriver.Chrome(service=service, options=chrome_options)
                last_error = None
                if attempt > 1:
                    print(f"[OK] Browser launch succeeded on attempt {attempt}/{max_attempts}")
                break
            except Exception as e:
                last_error = e
                err_msg = str(e) or e.__class__.__name__
                print(f"[ERROR] Browser launch failed (attempt {attempt}/{max_attempts}): {err_msg[:200]}")
                if attempt < max_attempts:
                    wait_s = 5 * attempt  # 5s, 10s
                    print(f"[INFO] Retrying in {wait_s}s — 잔존 chrome.exe / 디버그 포트 점유 가능")
                    time.sleep(wait_s)

        if last_error is not None:
            raise last_error

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

    def extract_available_quantity(self, text):
        """Extract only number from availability text: 'Only 1 left in stock - order soon' -> '1'"""
        if not text:
            return None

        try:
            # Extract first number from text
            match = re.search(r'(\d+)', text)
            if match:
                return match.group(1)
            return None

        except Exception as e:
            return None

    def extract_product_name(self, element):
        """Extract product name with multiple fallback XPaths"""
        # Try multiple XPath strategies in order of preference
        xpaths_to_try = [
            self.xpaths['product_name']['xpath'],  # Primary: .//h2//span
            './/h2/a/span',                         # Alternative 1: h2 > a > span
            './/a[.//h2]//span',                    # Alternative 2: span in a that has h2
            './/h2',                                # Alternative 3: h2 text content
            './/span[@class="a-size-medium"]',      # Alternative 4: by class
            './/span[@class="a-size-base-plus"]',   # Alternative 5: by class
        ]

        for idx, xpath in enumerate(xpaths_to_try):
            result = self.extract_text_safe(element, xpath)
            if result and len(result.strip()) > 0:
                # Debug: log which XPath worked for non-primary paths
                if idx > 0 and result:
                    pass  # Silently use fallback
                return result

        return None

    def _is_sorry_page(self):
        """Detect Amazon sorry/robot check page from current driver state."""
        try:
            page_source = self.driver.page_source.lower()
            title = self.driver.title.lower()
            return (
                'sorry' in title or
                'robot check' in title or
                'sorry' in page_source[:2000] or
                'robot check' in page_source[:2000]
            )
        except Exception:
            return False

    def load_cookies(self):
        """Load cookies from pickle file for authenticated Amazon session.
        Returns True on success, False otherwise.
        Selenium 호환: amazon.com 으로 먼저 이동 후 add_cookie 로 주입.
        """
        if not os.path.exists(COOKIE_FILE):
            print(f"  [WARNING] Cookie file not found: {COOKIE_FILE}")
            return False
        try:
            print(f"  [INFO] Loading cookies from {COOKIE_FILE}...")
            self.driver.get('https://www.amazon.com')
            time.sleep(2)
            try:
                self.driver.delete_all_cookies()
            except Exception:
                pass

            with open(COOKIE_FILE, 'rb') as f:
                cookies = pickle.load(f)

            loaded = 0
            for cookie in cookies:
                ck = dict(cookie) if isinstance(cookie, dict) else cookie
                # Selenium 은 sameSite 값이 Strict/Lax/None 만 허용 — 그 외엔 제거
                if isinstance(ck, dict) and 'sameSite' in ck and ck['sameSite'] not in ('Strict', 'Lax', 'None'):
                    ck.pop('sameSite', None)
                # expiry 가 float 면 int 변환
                if isinstance(ck, dict) and 'expiry' in ck and isinstance(ck['expiry'], float):
                    ck['expiry'] = int(ck['expiry'])
                try:
                    self.driver.add_cookie(ck)
                    loaded += 1
                except Exception:
                    pass

            print(f"  [DEBUG] Loaded {loaded}/{len(cookies)} cookies")
            self.driver.refresh()
            time.sleep(random.uniform(3, 5))
            print(f"  [OK] Cookies loaded")
            return True
        except Exception as e:
            print(f"  [WARNING] Failed to load cookies: {e}")
            return False

    def check_and_handle_sorry_page(self, url, max_retries=3):
        """Check for sorry/robot check page; recover via refresh, then via cookie reload.

        Layer 1: refresh up to max_retries times.
        Layer 2: 모든 refresh 실패 시 쿠키 재로드 + URL 재접속 1회.
        Returns:
            bool: True if page is OK, False if still sorry after all attempts.
        """
        # Layer 1: refresh 기반 retry
        for attempt in range(max_retries):
            if not self._is_sorry_page():
                if attempt > 0:
                    print(f"  [OK] Page loaded successfully after {attempt} refresh(es)")
                return True

            print(f"  [WARNING] Sorry/Robot check page detected (attempt {attempt + 1}/{max_retries})")
            if attempt < max_retries - 1:
                print(f"  [INFO] Refreshing page in 3-5 seconds...")
                time.sleep(random.uniform(3, 5))
                self.driver.refresh()
                print(f"  [INFO] Page refreshed, waiting for load...")
                time.sleep(random.uniform(4, 6))

        # Layer 2: 쿠키 재로드 retry
        print(f"  [RETRY] All {max_retries} refreshes failed - trying cookie reload + URL re-access...")
        if self.load_cookies():
            print(f"  [INFO] Re-accessing URL after cookie reload: {url[:80]}...")
            try:
                self.driver.get(url)
                time.sleep(random.uniform(4, 6))
                if not self._is_sorry_page():
                    print(f"  [OK] Page loaded after cookie reload retry")
                    return True
                print(f"  [ERROR] Still sorry page after cookie reload")
            except Exception as e:
                print(f"  [ERROR] URL re-access after cookie reload failed: {e}")
        else:
            print(f"  [ERROR] Cookie reload failed (no cookie file or load error)")

        print(f"  [ERROR] Sorry page persists after refresh + cookie reload, skipping this page...")
        return False

    def scrape_page(self, url, page_number):
        """Scrape a single page"""
        try:
            print(f"\n[PAGE {page_number}] Accessing: {url[:80]}...")
            self.driver.get(url)

            # Check and handle sorry page: refresh retries → cookie reload retry
            if not self.check_and_handle_sorry_page(url, max_retries=self.sorry_page_max_retry):
                print(f"[SKIP] Skipping page {page_number} due to persistent sorry/robot check page")
                return True  # Continue to next page (not break)

            # Wait for search results to actually load
            print(f"[INFO] Waiting for search results to load...")
            try:
                # Wait up to 15 seconds for search result containers to appear
                WebDriverWait(self.driver, 15).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, "[data-component-type='s-search-result']"))
                )
                print(f"[OK] Search results detected")

                # Additional wait for all elements to render
                time.sleep(random.uniform(4, 6))

            except Exception as e:
                print(f"[WARNING] Timeout waiting for search results: {e}")
                # Still try to parse, might be blocked or error page
                time.sleep(3)

            # DEBUG: Verify current URL after load
            current_url = self.driver.current_url
            print(f"[DEBUG] Current URL after load: {current_url[:100]}...")
            if current_url != url:
                print(f"[WARNING] URL changed! Expected: {url[:50]}, Got: {current_url[:50]}")

            # Get page source and parse with lxml
            page_source = self.driver.page_source
            tree = html.fromstring(page_source)

            # DEBUG: Check page source size
            print(f"[DEBUG] Page source size: {len(page_source)} bytes")

            # Find all product containers (excluding ads/widgets)
            base_xpath = self.xpaths['base_container']['xpath']
            products = tree.xpath(base_xpath)

            print(f"[INFO] Found {len(products)} total containers")

            # Filter out excluded containers and sort by page order
            valid_products = []
            excluded_count = 0
            for product in products:
                # Check if it's a valid product (not ad/widget)
                cel_widget = product.get('cel_widget_id', '')
                component_type = product.get('data-component-type', '')
                data_component_id = product.get('data-component-id', '')

                # More specific exclude conditions - only exclude exact matches
                is_excluded = False

                # Exclude sponsored/ad widgets
                if 'loom-desktop' in cel_widget:
                    is_excluded = True
                elif 'sb-themed' in cel_widget:
                    is_excluded = True
                elif 'multi-brand' in cel_widget:
                    is_excluded = True
                elif 'FEATURED_ASINS_LIST' in cel_widget:
                    is_excluded = True  # Exclude "4 stars and above" recommendation sections
                # Only exclude messaging/video widgets, not video products
                elif component_type == 's-messaging-widget':
                    is_excluded = True
                elif 'VideoLandscapeCarouselWidget' in data_component_id:
                    is_excluded = True

                if is_excluded:
                    excluded_count += 1
                    continue

                # Get data-index for sorting
                data_index = product.get('data-index', '999')
                try:
                    data_index = int(data_index)
                except:
                    data_index = 999

                valid_products.append((data_index, product))

            if excluded_count > 0:
                print(f"[INFO] Excluded {excluded_count} containers (ads/widgets)")

            # Sort by data-index (page order)
            valid_products.sort(key=lambda x: x[0])
            valid_products = [product for _, product in valid_products]

            print(f"[INFO] Valid products after filtering: {len(valid_products)}")

            # Debug: Show warning if less than 16 products on early pages
            if page_number <= 10 and len(valid_products) < 16:
                print(f"[WARNING] Only {len(valid_products)} valid products found on page {page_number}")
                print(f"[DEBUG] Total containers: {len(products)}, Excluded: {excluded_count}, Valid: {len(valid_products)}")

            # DEBUG: Show first 3 products on this page
            print(f"\n[DEBUG] First 3 products on page {page_number}:")
            for debug_idx, debug_product in enumerate(valid_products[:3], 1):
                debug_name = self.extract_product_name(debug_product)
                debug_url_path = self.extract_text_safe(debug_product, self.xpaths['product_url']['xpath'])
                print(f"  {debug_idx}. Name: {debug_name[:50] if debug_name else 'NULL'}... | URL: {debug_url_path[:50] if debug_url_path else 'NULL'}...")

            # Process all valid products per page
            collected_count = 0
            for idx, product in enumerate(valid_products, 1):
                if self.total_collected >= self.max_skus:
                    print(f"[INFO] Reached maximum SKU limit ({self.max_skus})")
                    return False

                # Extract data
                product_url_path = self.extract_text_safe(product, self.xpaths['product_url']['xpath'])

                # DEBUG: Print URL extraction result for first product
                if idx == 1:
                    print(f"\n[DEBUG] URL XPath: {self.xpaths['product_url']['xpath']}")
                    print(f"[DEBUG] Extracted path: {product_url_path}")

                # Build complete URL
                product_url = f"https://www.amazon.com{product_url_path}" if product_url_path else None

                if idx == 1:
                    print(f"[DEBUG] Final URL: {product_url}\n")

                # Extract discount type and validate
                discount_type_raw = self.extract_text_safe(product, self.xpaths['deal_badge']['xpath'])
                # Only keep "Limited time deal", set others to None
                discount_type = discount_type_raw if discount_type_raw == "Limited time deal" else None

                # Extract product name with fallback XPaths
                product_name = self.extract_product_name(product)

                # Skip if no product name (critical field)
                if not product_name:
                    print(f"  [{idx}] SKIP: No product name found (tried all XPath alternatives)")
                    continue

                # Skip Prime Video products
                if "Prime Video" in product_name or "prime video" in product_name.lower():
                    print(f"  [{idx}] SKIP: Prime Video product - {product_name[:60]}...")
                    continue

                # Skip book products (Paperback, Kindle, Audible, etc.)
                book_keywords = ["Paperback", "Kindle", "Audible", "Hardcover", "Audio CD", "audiobook"]
                if any(keyword.lower() in product_name.lower() for keyword in book_keywords):
                    print(f"  [{idx}] SKIP: Book product - {product_name[:60]}...")
                    continue

                # 비-상품 placement 필터: product_url 이 없으면 스킵.
                # 사례: Prime Video TV 프로그램/영화 타일 ('Fallout', 'Beast Games' 등) —
                #       검색결과에 노출되지만 구매 가능한 상품이 아니라 url 이 NULL.
                # NULL 이 들어가면 dedup_key=None 으로 dedup 이 우회되어 페이지마다 중복 누적되는 결함.
                if not product_url or not str(product_url).strip():
                    print(f"  [{idx}] SKIP: No product URL (likely Prime Video / non-product placement) - {product_name[:60] if product_name else '[NO NAME]'}")
                    continue

                # ASIN 단위 dedup (sspa/click 등 URL 변형으로 같은 SKU가 새지 않게).
                # ASIN 추출 불가한 URL은 URL 자체를 fallback 키로 사용.
                if not hasattr(self, '_seen_asins'):
                    self._seen_asins = {}

                asin = self.extract_asin(product_url)
                dedup_key = asin or product_url

                if dedup_key and dedup_key in self._seen_asins:
                    prev_page = self._seen_asins[dedup_key]
                    key_kind = 'ASIN' if asin else 'URL'
                    print(f"  [{idx}] SKIP: Duplicate {key_kind} (already collected from page {prev_page})")
                    print(f"         Product: {product_name[:60]}...")
                    continue

                # Check if this item is excluded (is_product=false in tv_item_mst)
                if asin and asin in self.excluded_items:
                    print(f"  [{idx}] SKIP: Excluded item (is_product=false) - ASIN: {asin}")
                    print(f"         Product: {product_name[:60]}...")
                    continue

                # Extract price (disabled - will be collected in detail crawler)
                final_price = None

                # Extract available quantity (only numbers)
                available_qty_raw = self.extract_text_safe(product, self.xpaths['stock_availability']['xpath'])
                available_qty = self.extract_available_quantity(available_qty_raw)

                data = {
                    'account_name': 'Amazon',
                    'page_type': 'main',
                    'page_number': page_number,
                    'Retailer_SKU_Name': product_name,
                    'Number_of_units_purchased_past_month': None,  # Will be collected in detail crawler
                    'Final_SKU_Price': None,  # Will be collected in detail crawler
                    'Original_SKU_Price': None,  # Will be collected in detail crawler
                    'Shipping_Info': self.extract_text_safe(product, self.xpaths['shipping_info']['xpath']),
                    'Available_Quantity_for_Purchase': available_qty,
                    'Discount_Type': discount_type,
                    'Product_URL': product_url
                }

                # Save to database
                if self.save_to_db(data):
                    collected_count += 1
                    self.total_collected += 1

                    # Track this dedup key (ASIN preferred, URL fallback)
                    if dedup_key:
                        self._seen_asins[dedup_key] = page_number

                    # DEBUG: Show detailed saved data
                    print(f"  [{idx}] ✓ SAVED (main_rank #{self.sequential_id - 1}):")
                    print(f"           Name: {data['Retailer_SKU_Name'][:60] if data['Retailer_SKU_Name'] else '[NO NAME]'}...")
                    print(f"           Price: {final_price or 'N/A'}")
                    print(f"           Qty: {available_qty or 'N/A'}")
                    print(f"           URL: {product_url[:60] if product_url else 'NULL'}...")
                else:
                    print(f"  [{idx}] ✗ FAILED: {data['Retailer_SKU_Name'][:40]}... - DB save error")

            print(f"\n[PAGE {page_number}] Summary:")
            print(f"  - Collected: {collected_count} products")
            print(f"  - Total progress: {self.total_collected}/{self.max_skus}")
            print(f"  - Next sequential ID: {self.sequential_id}")

            return True

        except Exception as e:
            print(f"[ERROR] Failed to scrape page {page_number}: {e}")
            import traceback
            print(traceback.format_exc())
            return True  # Continue to next page

    def save_to_db(self, data):
        """Save collected data with collection order (1-400)"""
        cursor = None
        try:
            # Temporarily disable autocommit for transaction
            self.db_conn.autocommit = False

            # Use sequential_id (1-300) for collection order (main_rank)
            main_rank = self.sequential_id

            # Calculate calendar week
            calendar_week = f"w{datetime.now().isocalendar().week}"

            # Calculate crawl_strdatetime (format: 202511040300559260)
            now = datetime.now()
            crawl_strdatetime = now.strftime('%Y%m%d%H%M%S') + now.strftime('%f')[:4]

            cursor = self.db_conn.cursor()

            # Insert to amazon_tv_main_crawled
            cursor.execute("""
                INSERT INTO amazon_tv_main_crawled
                (main_rank, account_name, page_type, Retailer_SKU_Name, Number_of_units_purchased_past_month,
                 Final_SKU_Price, Original_SKU_Price, Shipping_Info,
                 Available_Quantity_for_Purchase, Discount_Type, Product_URL, batch_id, calendar_week, crawl_strdatetime)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                main_rank,
                data['account_name'],
                data['page_type'],
                data['Retailer_SKU_Name'],
                data['Number_of_units_purchased_past_month'],
                data['Final_SKU_Price'],
                data['Original_SKU_Price'],
                data['Shipping_Info'],
                data['Available_Quantity_for_Purchase'],
                data['Discount_Type'],
                data['Product_URL'],
                self.batch_id,
                calendar_week,
                crawl_strdatetime
            ))

            # Commit transaction
            self.db_conn.commit()

            # Increment sequential ID for next product
            self.sequential_id += 1

            cursor.close()

            # Re-enable autocommit
            self.db_conn.autocommit = True

            return True

        except Exception as e:
            # Rollback on any error (including duplicate)
            try:
                self.db_conn.rollback()
            except:
                pass

            if cursor:
                try:
                    cursor.close()
                except:
                    pass

            # Re-enable autocommit
            self.db_conn.autocommit = True

            # Don't print error for duplicate keys (expected behavior)
            if 'duplicate key' not in str(e):
                print(f"[ERROR] Failed to save to DB: {e}")

            return False

    def run(self):
        """Main execution"""
        try:
            print("="*80)
            print("Amazon TV Main1 Crawler - Starting (No Price Collection)")
            print("="*80)

            # Connect to database
            if not self.connect_db():
                return

            # Close and reconnect to ensure clean connection state
            try:
                self.db_conn.close()
                print("[INFO] Closed existing connection")
            except:
                pass

            if not self.connect_db():
                return

            # Generate batch_id for this session (env override 가능 — catch-up 용)
            korea_tz = pytz.timezone('Asia/Seoul')
            _override_batch = os.environ.get('AMAZON_TV_MAIN1_BATCH_ID', '').strip()
            if _override_batch:
                self.batch_id = _override_batch
                print(f"[INFO] batch_id override from env: {self.batch_id}")
            else:
                self.batch_id = datetime.now(korea_tz).strftime('%Y%m%d_%H%M%S')
                print(f"[OK] Batch ID: {self.batch_id}")

            # Load XPaths and URLs
            if not self.load_xpaths():
                return

            page_urls = self.load_page_urls()
            if not page_urls:
                print("[ERROR] No page URLs found")
                return

            # Load excluded items (is_product=false)
            self.load_excluded_items()

            # Setup WebDriver
            self.setup_driver()

            # Scrape each page
            for page_number, url in page_urls:
                if self.total_collected >= self.max_skus:
                    break

                if not self.scrape_page(url, page_number):
                    break

                # Random delay between pages
                time.sleep(random.uniform(2, 4))

            print("\n" + "="*80)
            print(f"Crawling completed! Total collected: {self.total_collected} SKUs")

            # DEBUG: Show duplicate statistics (ASIN-keyed)
            if hasattr(self, '_seen_asins'):
                print(f"[DEBUG] Unique ASINs/URLs collected: {len(self._seen_asins)}")
                if len(self._seen_asins) != self.total_collected:
                    print(f"[WARNING] Mismatch! Total collected ({self.total_collected}) != Unique keys ({len(self._seen_asins)})")
                    print(f"[WARNING] This suggests duplicate products were collected!")

            print("="*80)

            # 0건 수집 시 실패 처리
            if self.total_collected == 0:
                print("[ERROR] 0 products collected - marking as FAILED")
                self._exit_code = 1
            else:
                self._exit_code = 0

        except Exception as e:
            print(f"[ERROR] Crawler failed: {e}")
            self._exit_code = 1

        finally:
            # 결과 JSON 저장
            try:
                import json
                result_dir = r"C:\samsung_dx_retail_com\stage_results"
                os.makedirs(result_dir, exist_ok=True)
                with open(os.path.join(result_dir, "amazon_tv_main1.json"), "w") as f:
                    json.dump({"collected_count": self.total_collected}, f)
            except Exception as e:
                print(f"[WARNING] Failed to write result JSON: {e}")

            if self.driver:
                self.driver.quit()
            if self.db_conn:
                self.db_conn.close()


if __name__ == "__main__":
    # Setup log file
    _log_dir = r'C:\samsung_dx_retail_com\log'
    os.makedirs(_log_dir, exist_ok=True)

    # Delete log files older than 30 days
    _cutoff_time = time.time() - (30 * 24 * 60 * 60)
    for _f in os.listdir(_log_dir):
        _fpath = os.path.join(_log_dir, _f)
        if os.path.isfile(_fpath) and _fpath.endswith('.txt'):
            if os.path.getmtime(_fpath) < _cutoff_time:
                try:
                    os.remove(_fpath)
                except:
                    pass

    _log_filename = datetime.now().strftime('%Y%m%d_%H%M%S') + '_main1.txt'
    _log_filepath = os.path.join(_log_dir, _log_filename)
    _tee = Tee(_log_filepath)
    sys.stdout = _tee
    print(f"[INFO] Log file: {_log_filepath}")

    try:
        crawler = AmazonTVCrawler()
        crawler.run()
    except Exception as e:
        print(f"\n[FATAL ERROR] {e}")
        import traceback
        traceback.print_exc()

    print("\n[INFO] Crawler terminated.")
    sys.stdout = sys.__stdout__   # interpreter shutdown 시 closed file flush 방지
    _tee.close()
    sys.exit(getattr(crawler, '_exit_code', 1))
