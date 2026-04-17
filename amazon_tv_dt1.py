import time
import random
import sys
import psycopg2
import pickle
import json
import os
from datetime import datetime, timedelta
import pytz
from DrissionPage import ChromiumPage, ChromiumOptions
from lxml import html
import re

# Configure stdout encoding for Windows console
if sys.stdout.encoding != 'utf-8':
    sys.stdout.reconfigure(encoding='utf-8', errors='replace')

# Tee class for logging to both console and file
class Tee:
    def __init__(self, log_file_path):
        self.terminal = sys.stdout
        self.log_file = open(log_file_path, 'a', encoding='utf-8')

    def write(self, message):
        self.terminal.write(message)
        self.log_file.write(message)
        self.log_file.flush()

    def flush(self):
        self.terminal.flush()
        self.log_file.flush()

    def close(self):
        self.log_file.close()


# Import database and account configuration
from config import DB_CONFIG, AMAZON_ACCOUNTS
from amazon_login import login_to_amazon_dp, save_cookies_dp
from amazon_config_loader import get_amazon_config

# Load config from DB
_config = get_amazon_config()

# Account configuration (uses unsandev0004 for amazon_tv_crawl.py)
ACCOUNT_NAME = _config.get_account('primary', 'amazon_tv_dt1') or 'ltyinvestmentl'
COOKIE_FILE = AMAZON_ACCOUNTS[ACCOUNT_NAME]['cookie_file']
AMAZON_EMAIL = AMAZON_ACCOUNTS[ACCOUNT_NAME]['email']
AMAZON_PASSWORD = AMAZON_ACCOUNTS[ACCOUNT_NAME]['password']
import pandas as pd
from alert_monitor import send_sku_renewed_alert

class AmazonDetailCrawler:
    def __init__(self):
        self.page = None
        self.db_conn = None
        self.xpaths = {}
        self.total_collected = 0
        self.max_skus = _config.get_constant_int('max_skus', 'amazon_tv_dt1', 300)
        self.target_reviews = _config.get_constant_int('target_reviews', 'amazon_tv_dt1', 20)
        self.max_review_pages = _config.get_constant_int('max_review_pages', 'amazon_tv_dt1', 3)
        self.sorry_page_max_retry = _config.get_retry('sorry_page_max', 'amazon_tv_dt1', 5)
        self.browser_restart_after_fail = _config.get_retry('browser_restart_after_fail', 'amazon_tv_dt1', 5)
        # Generate batch_id using Korea timezone
        korea_tz = pytz.timezone('Asia/Seoul')
        self.batch_id = datetime.now(korea_tz).strftime('%Y%m%d_%H%M%S')
        # Error tracking lists for alert email
        self.rv_detail_null_records = []  # count_of_reviews > 0 but detailed_review_content is null
        self.reviews_equals_ratings_records = []  # count_of_reviews == count_of_star_ratings
        self.fsp_null_records = []  # final_sku_price is null
        self.cosr_null_records = []  # count_of_star_ratings is null but star_rating exists
        self.screen_size_mismatch_records = []  # screen_size mismatch between extracted and tv_item_mst
        # self.sku_updated_records = []  # sku renewed in tv_item_mst (temporarily disabled)
        self.current_account = _config.get_account('primary', 'amazon_tv_dt1') or 'unsandev0004'
        self.processed_asins = set()  # Real-time duplicate check for ASINs (after redirect)

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

    def get_item_mst_data(self, item):
        """Get screen_size from tv_item_mst for given item"""
        try:
            if not self.db_conn or not item:
                return None
            cursor = self.db_conn.cursor()
            cursor.execute("""
                SELECT screen_size FROM tv_item_mst WHERE item = %s
            """, (item,))
            row = cursor.fetchone()
            cursor.close()
            if row:
                return {'screen_size': row[0]}
            return None
        except Exception as e:
            print(f"  [WARNING] Failed to get item_mst data: {e}")
            return None

    def load_xpaths(self):
        """Load XPath selectors from amazon_tv_config table"""
        try:
            print("[INFO] Loading XPath selectors from amazon_tv_config...")
            cursor = self.db_conn.cursor()

            # Load XPath entries from amazon_tv_config (category='xpath')
            # Group by prefix (e.g., star_rating_1, star_rating_2 -> star_rating)
            cursor.execute("""
                SELECT config_key, config_value, priority
                FROM amazon_tv_config
                WHERE category = 'xpath' AND is_active = TRUE
                ORDER BY config_key, priority
            """)

            rows = cursor.fetchall()
            xpath_groups = {}

            for row in rows:
                config_key, config_value, priority = row
                # Extract prefix from key (e.g., star_rating_1 -> star_rating)
                # Handle keys like 'screen_size_1', 'review_link_1', etc.
                import re
                match = re.match(r'^(.+?)_(\d+)$', config_key)
                if match:
                    prefix = match.group(1)
                else:
                    prefix = config_key  # Use as-is if no _N suffix

                if prefix not in xpath_groups:
                    xpath_groups[prefix] = []
                xpath_groups[prefix].append({'value': config_value, 'priority': priority})

            # Sort each group by priority and store as list
            for prefix, entries in xpath_groups.items():
                sorted_entries = sorted(entries, key=lambda x: x['priority'])
                self.xpaths[prefix] = [e['value'] for e in sorted_entries]

            cursor.close()

            if len(self.xpaths) == 0:
                print("[WARNING] No XPath selectors found in amazon_tv_config")
                print("[INFO] Run setup_amazon_tv_config.py to populate XPath entries")
            else:
                total_xpaths = sum(len(v) for v in self.xpaths.values())
                print(f"[OK] Loaded {total_xpaths} XPaths in {len(self.xpaths)} groups")
                for prefix in sorted(self.xpaths.keys())[:5]:
                    print(f"  [DEBUG] {prefix}: {len(self.xpaths[prefix])} XPaths")
                if len(self.xpaths) > 5:
                    print(f"  [DEBUG] ... and {len(self.xpaths) - 5} more groups")

            return True

        except Exception as e:
            print(f"[ERROR] Failed to load XPaths: {e}")
            import traceback
            traceback.print_exc()
            return False

    def extract_asin(self, url):
        """Extract ASIN from Amazon URL
        Example: https://www.amazon.com/.../dp/B0F19KLHG3/... -> B0F19KLHG3
        """
        try:
            import re
            match = re.search(r'/dp/([A-Z0-9]{10})', url)
            if match:
                return match.group(1)
            return url  # Fallback to full URL if ASIN not found
        except:
            return url

    def load_product_urls(self):
        """Load product URLs from amazon_tv_main_crawled and amazon_tv_bsr tables (latest batch only)
        Uses ASIN for duplicate detection but stores full URLs"""
        try:
            print("[INFO] Loading product URLs from database...")
            cursor = self.db_conn.cursor()

            # Check if amazon_tv_main_crawled table exists
            cursor.execute("""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables
                    WHERE table_name = 'amazon_tv_main_crawled'
                )
            """)
            main_crawled_exists = cursor.fetchone()[0]
            print(f"[DEBUG] Table 'amazon_tv_main_crawled' exists: {main_crawled_exists}")

            # Check if amazon_tv_bsr table exists
            cursor.execute("""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables
                    WHERE table_name = 'amazon_tv_bsr'
                )
            """)
            bsr_exists = cursor.fetchone()[0]
            print(f"[DEBUG] Table 'amazon_tv_bsr' exists: {bsr_exists}")

            # Get latest batch_id from amazon_tv_main_crawled
            main_batch_id = None
            if main_crawled_exists:
                cursor.execute("""
                    SELECT batch_id
                    FROM amazon_tv_main_crawled
                    WHERE batch_id IS NOT NULL
                    ORDER BY batch_id DESC
                    LIMIT 1
                """)
                main_batch_result = cursor.fetchone()
                main_batch_id = main_batch_result[0] if main_batch_result else None

            # Get latest batch_id from amazon_tv_bsr
            bsr_batch_id = None
            if bsr_exists:
                cursor.execute("""
                    SELECT batch_id
                    FROM amazon_tv_bsr
                    WHERE batch_id IS NOT NULL
                    ORDER BY batch_id DESC
                    LIMIT 1
                """)
                bsr_batch_result = cursor.fetchone()
                bsr_batch_id = bsr_batch_result[0] if bsr_batch_result else None

            print(f"[INFO] Latest batch_id - Main: {main_batch_id}, BSR: {bsr_batch_id}")

            # Dictionary to store merged URL data: {asin: {page_type, url, main_rank, bsr_rank}}
            # Use ASIN as key for duplicate detection, but store full URL
            url_data_map = {}

            # Load from amazon_tv_main_crawled (main) - latest batch only
            if main_batch_id:
                print(f"[INFO] Loading main URLs from batch {main_batch_id}...")
                cursor.execute("""
                    SELECT product_url, main_rank, number_of_units_purchased_past_month
                    FROM amazon_tv_main_crawled
                    WHERE batch_id = %s
                      AND product_url IS NOT NULL
                      AND product_url != ''
                    ORDER BY main_rank
                """, (main_batch_id,))
                main_rows = cursor.fetchall()
                for url, main_rank, number_of_units_purchased_past_month in main_rows:
                    asin = self.extract_asin(url)  # Extract ASIN for duplicate detection

                    # Clean number_of_units_purchased_past_month: remove commas and convert to int
                    clean_units = None
                    if number_of_units_purchased_past_month:
                        try:
                            clean_units = int(str(number_of_units_purchased_past_month).replace(',', '').strip())
                        except (ValueError, AttributeError):
                            clean_units = None

                    if asin not in url_data_map:
                        url_data_map[asin] = {
                            'asin': asin,  # Store ASIN for duplicate check
                            'page_type': 'main',
                            'url': url,  # Store full URL
                            'main_rank': main_rank,
                            'bsr_rank': None,
                            'number_of_units_purchased_past_month': clean_units
                        }
                print(f"[OK] Loaded {len(main_rows)} main URLs")
            else:
                print("[WARNING] No main batch_id found in amazon_tv_main_crawled")

            # Load from amazon_tv_bsr (bsr) - latest batch only
            if bsr_batch_id:
                print(f"[INFO] Loading BSR URLs from batch {bsr_batch_id}...")
                cursor.execute("""
                    SELECT product_url, bsr_rank
                    FROM amazon_tv_bsr
                    WHERE batch_id = %s
                      AND product_url IS NOT NULL
                      AND product_url != ''
                    ORDER BY bsr_rank
                """, (bsr_batch_id,))
                bsr_rows = cursor.fetchall()
                for url, bsr_rank in bsr_rows:
                    asin = self.extract_asin(url)  # Extract ASIN for duplicate detection
                    if asin in url_data_map:
                        # ASIN already exists in main - add bsr_rank only
                        url_data_map[asin]['bsr_rank'] = bsr_rank
                    else:
                        # New ASIN from bsr
                        url_data_map[asin] = {
                            'asin': asin,  # Store ASIN for duplicate check
                            'page_type': 'bsr',
                            'url': url,  # Store full URL
                            'main_rank': None,
                            'bsr_rank': bsr_rank,
                            'number_of_units_purchased_past_month': None  # BSR doesn't have this
                        }
                print(f"[OK] Loaded {len(bsr_rows)} BSR URLs")
            else:
                print("[WARNING] No BSR batch_id found in amazon_tv_bsr")

            cursor.close()

            # Convert dictionary to list (maintains insertion order: main first, then bsr)
            all_urls = list(url_data_map.values())

            # Count duplicates from source tables
            total_loaded = 0
            if main_batch_id:
                cursor = self.db_conn.cursor()
                cursor.execute("SELECT COUNT(*) FROM amazon_tv_main_crawled WHERE batch_id = %s", (main_batch_id,))
                total_loaded += cursor.fetchone()[0]
                cursor.close()
            if bsr_batch_id:
                cursor = self.db_conn.cursor()
                cursor.execute("SELECT COUNT(*) FROM amazon_tv_bsr WHERE batch_id = %s", (bsr_batch_id,))
                total_loaded += cursor.fetchone()[0]
                cursor.close()

            duplicates = total_loaded - len(all_urls)
            if duplicates > 0:
                print(f"[INFO] Found {duplicates} duplicate URLs - rank information merged")

            print(f"[OK] Total unique URLs from main/bsr: {len(all_urls)}")

            # Filter out already processed items from current session (based on ASIN/item)
            # Using ASIN instead of URL to handle redirects (original URL vs final URL mismatch)
            print("[INFO] Checking for already processed items (current session, ASIN-based)...")
            cursor = self.db_conn.cursor()

            # Use main batch_id as session start time
            if main_batch_id:
                session_start_time = datetime.strptime(main_batch_id, '%Y%m%d_%H%M%S')
                session_start_str = session_start_time.strftime('%Y-%m-%d %H:%M:%S')

                print(f"[INFO] Session start time (from main batch): {session_start_str}")

                # Get all distinct processed ASINs (item) from current session in amazon_tv_detail_crawled
                # item column stores ASIN extracted from final URL after redirect
                cursor.execute("""
                    SELECT DISTINCT item
                    FROM amazon_tv_detail_crawled
                    WHERE item IS NOT NULL
                      AND crawl_datetime >= %s
                """, (session_start_str,))

                already_processed_asins = {row[0] for row in cursor.fetchall()}
                print(f"[INFO] Found {len(already_processed_asins)} already processed ASINs in current session")
            else:
                already_processed_asins = set()
                print(f"[WARNING] No main batch_id found, skipping duplicate check")

            cursor.close()

            # Filter out already processed items (ASIN-based)
            new_urls = [url_data for url_data in all_urls
                        if url_data['asin'] not in already_processed_asins]

            # Summary
            already_processed_count = len(all_urls) - len(new_urls)
            print(f"[INFO] Already processed (skipped): {already_processed_count}")
            print(f"[OK] New URLs to process: {len(new_urls)}")

            if len(new_urls) == 0:
                if len(all_urls) > 0:
                    print("[WARNING] All URLs have been processed already in current session!")
                else:
                    print("[ERROR] No product URLs found! Please check:")
                    print("  1. amazon_tv_main_crawled table has data with valid batch_id")
                    print("  2. amazon_tv_bsr table has data with valid batch_id")
                    print("  3. Product_URL/product_url columns are not empty")

            return new_urls

        except Exception as e:
            print(f"[ERROR] Failed to load product URLs: {e}")
            import traceback
            traceback.print_exc()
            return []

    def setup_driver(self):
        """Setup DrissionPage ChromiumPage"""
        try:
            print("[INFO] Setting up DrissionPage browser...")
            co = ChromiumOptions()
            co.set_argument('--lang=en-US')

            self.page = ChromiumPage(co)
            self.page.set.window.max()

            print("[OK] DrissionPage browser setup complete")

            # Load cookies for login
            self.load_cookies()

        except Exception as e:
            print(f"[ERROR] Failed to setup browser: {e}")
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
            homepage_url = _config.get_url('homepage') or 'https://www.amazon.com'
            self.page.get(homepage_url)
            time.sleep(2)

            # 기존 브라우저 쿠키 초기화 (이전 세션 잔존 방지)
            self.page.set.cookies.clear()
            print("[INFO] Browser cookies cleared")

            with open(COOKIE_FILE, 'rb') as f:
                cookies = pickle.load(f)
                print(f"[DEBUG] Found {len(cookies)} cookies in file")
                loaded_count = 0
                for cookie in cookies:
                    try:
                        self.page.set.cookies(cookie)
                        loaded_count += 1
                    except Exception as e:
                        pass
                print(f"[DEBUG] Successfully loaded {loaded_count}/{len(cookies)} cookies")

            print("[INFO] Refreshing page with cookies...")
            self.page.refresh()
            time.sleep(3)
            print(f"[OK] Cookies loaded successfully")
            return True

        except Exception as e:
            print(f"[WARNING] Failed to load cookies: {e}")
            import traceback
            traceback.print_exc()
            return False

    def log_failed_url(self, url, reason="no_sku_name"):
        """Save failed URL to file for later investigation"""
        try:
            failed_dir = r"C:\samsung_dx_retail_com\failed_amazon"
            os.makedirs(failed_dir, exist_ok=True)

            filename = datetime.now().strftime('%Y%m%d') + f"_{reason}.txt"
            filepath = os.path.join(failed_dir, filename)

            with open(filepath, 'a', encoding='utf-8') as f:
                f.write(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} | {url}\n")

            print(f"  [LOG] Failed URL saved to {filepath}")
        except Exception as e:
            print(f"  [WARNING] Failed to log URL: {e}")

    def verify_page_loaded(self, wait_for_price=True):
        """
        Verify page is properly loaded before extraction.
        - Check for bot detection (captcha/robot page)
        - Wait for key elements to load

        Returns:
            bool: True if page is ready, False if bot detection or load failure
        """
        try:
            # Wait for product title element (required)
            elem = self.page.ele('#productTitle', timeout=10)
            if not elem:
                print("  [WARNING] Product title element not found within timeout")
                return False

            # Wait for price element (optional but important)
            if wait_for_price:
                price_elem = self.page.ele('xpath://*[@id="corePriceDisplay_desktop_feature_div"] | //*[@id="corePrice_feature_div"] | //*[@id="outOfStock"]', timeout=5)
                if not price_elem:
                    print("  [WARNING] Price element not found within timeout - may be unavailable product")

            # Wait for star rating element (optional)
            star_elem = self.page.ele('xpath://*[@id="acrPopover"] | //*[@id="averageCustomerReviews"] | //span[contains(text(), "No customer reviews")]', timeout=3)
            if not star_elem:
                print("  [WARNING] Star rating element not found within timeout")

            return True

        except Exception as e:
            print(f"  [ERROR] Page verification failed: {e}")
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

    def extract_with_xpaths(self, tree, xpaths_key, fallback_xpaths=None):
        """Extract text using multiple XPaths from DB, with fallback
        Args:
            tree: lxml tree object
            xpaths_key: key in self.xpaths (e.g., 'product_name')
            fallback_xpaths: list of fallback xpaths if DB returns None
        Returns:
            Extracted text or None
        """
        xpaths = self.xpaths.get(xpaths_key) or fallback_xpaths or []
        for xpath in xpaths:
            result = self.extract_text_safe(tree, xpath)
            if result:
                return result
        return None

    def clean_rank(self, rank_text):
        """Remove parentheses content from rank text"""
        if not rank_text:
            return None
        # Remove content in parentheses: "#8,565 in Electronics (See Top 100 in Electronics)" -> "#8,565 in Electronics"
        cleaned = re.sub(r'\s*\([^)]*\)', '', rank_text)
        return cleaned.strip()

    def clean_membership_discount(self, text):
        """Extract Prime membership discount text (from 'Prime members get FREE delivery' to before 'Join Prime')"""
        if not text:
            return None

        # Find "Prime members get FREE delivery" start
        if "Prime members get FREE delivery" in text:
            start_idx = text.find("Prime members get FREE delivery")
            text = text[start_idx:]

            # Remove "Join Prime" and everything after
            if "Join Prime" in text:
                text = text.split("Join Prime")[0].strip()

            return text.strip()

        return None

    def extract_screen_size(self, tree, retailer_sku_name=None):
        """Extract screen size (format: '32 inches')
        Ensures proper formatting with 'inches' suffix
        Falls back to extracting from retailer_sku_name if not found in tree
        """
        try:
            # Use DB XPaths if available, otherwise use hardcoded fallback
            xpaths = self.xpaths.get('screen_size') or [
                '//tr[contains(@class, "po-display.size")]//td[@class="a-span9"]//span[@class="a-size-base po-break-word"]',
                '//table[@class="a-normal a-spacing-small"]//tr[contains(@class, "po-display.size")]//td[@class="a-span9"]//span[@class="a-size-base po-break-word"]',
                '//*[@id="poExpander"]/div[1]/div/table/tbody/tr[2]/td[2]/span',  # tr[2] typically contains Screen Size
                '//tr[contains(@class, "po-display.size")]//span[@class="a-size-base po-break-word"]',
                # Lowest priority: inline-twister Size selector (format: "50-inch")
                '//*[@id="inline-twister-expanded-dimension-text-size_name"]'
            ]

            for xpath in xpaths:
                size_text = self.extract_text_safe(tree, xpath)
                if size_text:
                    import re

                    size_text = size_text.strip()
                    print(f"  [DEBUG] screen_size found: '{size_text}' (repr: {repr(size_text)})")

                    # Skip invalid values (like "tilt", "fix", etc.)
                    # Screen size must contain a number
                    if not re.search(r'\d', size_text):
                        print(f"  [DEBUG] Skipped - no digit found")
                        continue

                    # Handle "50-inch" format -> "50 inches"
                    if '-inch' in size_text.lower():
                        # Extract number from "50-inch" or "50-Inch"
                        match = re.search(r'(\d+\.?\d*)-inch', size_text.lower())
                        if match:
                            return f"{match.group(1)} inches"
                        continue

                    # Check if it's a number (with optional " symbol like "43", "15.6", or "50"") -> "X inches"
                    if re.match(r'^\d+\.?\d*"?$', size_text):
                        # Extract just the number, removing " if present
                        size_number = re.search(r'(\d+\.?\d*)', size_text).group(1)
                        return f"{size_number} inches"

                    # Handle "32 Inches" or "15.6 inches" format
                    # Extract number + "inch/inches" pattern
                    match = re.search(r'(\d+\.?\d*)\s*inch', size_text.lower())
                    if match:
                        return f"{match.group(1)} inches"

                    # If no valid screen size format found, continue to next XPath
                    continue

            # Fallback: Extract from retailer_sku_name if provided
            if retailer_sku_name:
                import re
                # Look for patterns: "25 inch", "55-Inch", "50"", etc.
                # Matches: number + (space/hyphen + inch/inches OR double quote)
                match = re.search(r'(\d+\.?\d*)(?:[\s-]*inch(?:es)?|"|\'\'?)', retailer_sku_name, re.IGNORECASE)
                if match:
                    size_number = match.group(1)
                    print(f"  [INFO] Extracted screen_size from retailer_sku_name: {size_number} inches")
                    return f"{size_number} inches"

            return None

        except Exception as e:
            print(f"  [WARNING] Failed to extract screen size: {e}")
            return None

    def extract_model_year(self, tree):
        """Extract model year from item details dialog or compare table (fallback)"""
        try:
            # Use DB XPaths only
            xpaths = self.xpaths.get('model_year') or []

            for xpath in xpaths:
                year_text = self.extract_text_safe(tree, xpath)
                if year_text:
                    year_text = year_text.strip()

                    # Handle year range format (e.g., "2022/2023" -> "2023")
                    if '/' in year_text:
                        try:
                            # Split and extract all year numbers
                            years = year_text.split('/')
                            year_numbers = [int(y.strip()) for y in years if y.strip().isdigit() and len(y.strip()) == 4]
                            if year_numbers:
                                # Return the higher (more recent) year
                                year_text = str(max(year_numbers))
                        except (ValueError, AttributeError):
                            # If parsing fails, continue to next xpath
                            continue

                    # Validate it's a 4-digit year
                    if re.match(r'^\d{4}$', year_text):
                        return year_text

            return None

        except Exception as e:
            print(f"  [WARNING] Failed to extract model year: {e}")
            return None

    def extract_sku(self, tree):
        """Extract SKU (Model Number) from Item details container
        Same location as Model Year, Rank_1, Rank_2

        Example HTML:
        <tr>
            <th class="a-color-secondary a-size-base prodDetSectionEntry">Model Number</th>
            <td class="a-size-base prodDetAttrValue">HG50AU800NF</td>
        </tr>

        Returns: SKU string or "no sku" if not found
        """
        try:
            # Use DB XPaths if available, otherwise use hardcoded fallback
            xpaths = self.xpaths.get('model_number') or [
                # Primary: Item details - Model Number
                '//tr[.//th[contains(text(), "Model Number")]]/td[@class="a-size-base prodDetAttrValue"]',
                '//table[@id="productDetails_techSpec_section_1"]//tr[.//th[contains(text(), "Model Number")]]/td',
                '//table//tr[.//th[contains(text(), "Model Number")]]/td',
                '//*[@id="productDetails_expanderTables_depthRightSections"]//tr[.//th[contains(text(), "Model Number")]]/td',
                # Alternative patterns
                '//th[contains(text(), "Model Number")]/following-sibling::td',
                '//th[contains(@class, "prodDetSectionEntry") and contains(text(), "Model Number")]/following-sibling::td'
            ]

            for xpath in xpaths:
                sku_text = self.extract_text_safe(tree, xpath)
                if sku_text:
                    sku_text = sku_text.strip()
                    if sku_text and len(sku_text) >= 3:
                        return sku_text

            return "no sku"

        except Exception as e:
            print(f"  [WARNING] Failed to extract SKU: {e}")
            return "no sku"

    def extract_star_rating(self, tree):
        """Extract star rating (format: '4.5' or 'No customer reviews')
        PRIORITY: Actual star rating first, then "No customer reviews" fallback
        """
        try:
            # Use DB XPaths if available, otherwise use hardcoded fallback
            star_rating_xpaths = self.xpaths.get('star_rating') or [
                '//*[@id="acrPopover"]/@title',
                '//*[@id="averageCustomerReviews"]//span[@class="a-icon-alt"]',
                '//span[@data-hook="rating-out-of-text"]',
                '//*[@id="acrPopover"]/span[1]/a/span'
            ]

            # PRIORITY 1: Try to extract actual star rating first
            for xpath in star_rating_xpaths:
                star_rating_text = self.extract_text_safe(tree, xpath)

                if star_rating_text:
                    # Extract "X.X out of 5" pattern
                    match = re.search(r'(\d+\.?\d*)\s*out of\s*5', star_rating_text)
                    if match:
                        return match.group(1)

                    # Return as-is if it contains a number (but not "No customer reviews" text)
                    if re.search(r'\d', star_rating_text) and "No customer reviews" not in star_rating_text:
                        return star_rating_text

            # PRIORITY 2: Only check for "No customer reviews" if no star rating found
            # Check from the same XPaths first
            for xpath in star_rating_xpaths:
                if not xpath:
                    continue
                star_rating_text = self.extract_text_safe(tree, xpath)
                if star_rating_text and "No customer reviews" in star_rating_text:
                    return "No customer reviews"

            # Fallback: Check for "No customer reviews" at specific locations (narrowed scope)
            no_reviews_xpaths = self.xpaths.get('no_reviews') or [
                '//*[@id="cm-cr-dp-review-header"]/h3/span',
                '//span[@data-hook="top-customer-reviews-title"]',
                '//div[@id="cm-cr-dp-review-header"]//span[contains(text(), "No customer reviews")]'
                # Removed broad '//span[contains(text(), "No customer reviews")]' - causes false positives
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
        """Extract count of reviews (format: '1,484' or '0' for no reviews)"""
        try:
            # First check for "0 customer reviews" pattern in reviewsMedley section
            # e.g., "There are 0 customer reviews and 2 customer ratings."
            zero_reviews_xpaths = self.xpaths.get('zero_reviews') or [
                '//*[@id="reviewsMedley"]//div[@class="a-box-inner"]',
                '//*[@id="reviewsMedley"]/div/div[2]/div/div[2]/div[3]/div[2]/div/div',
                '//div[contains(text(), "customer reviews and")]'
            ]
            for xpath in zero_reviews_xpaths:
                text = self.extract_text_safe(tree, xpath)
                if text:
                    # Match "0 customer reviews" or "There are 0 customer reviews"
                    match = re.search(r'(\d+)\s*customer\s*reviews?', text, re.IGNORECASE)
                    if match and match.group(1) == '0':
                        return "0"

            # XPath: //*[@id="acrCustomerReviewText"]
            xpaths = self.xpaths.get('review_count') or [
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

    def extract_count_of_star_ratings(self, tree):
        """Extract total star rating count (sum of all star ratings)
        Returns: integer (e.g., 2449) or None
        """
        try:
            # Use DB XPaths if available, otherwise use hardcoded fallback
            xpaths = self.xpaths.get('count_of_star_ratings') or [
                '//*[@id="cm_cr_dp_d_rating_histogram"]/div[3]',
                '//*[@id="acrCustomerReviewText"]',
                '//span[@id="acrCustomerReviewText"]',
                '//a[@id="acrCustomerReviewLink"]//span'
            ]

            for xpath in xpaths:
                total_text = self.extract_text_safe(tree, xpath)
                if total_text:
                    # Try "2,449 global ratings" pattern first
                    total_match = re.search(r'([\d,]+)\s*global ratings?', total_text)
                    if total_match:
                        return int(total_match.group(1).replace(',', ''))

                    # Fallback: "2,414 ratings" pattern
                    total_match = re.search(r'([\d,]+)\s*ratings?', total_text)
                    if total_match:
                        return int(total_match.group(1).replace(',', ''))

                    # Fallback: "(102)" or "102 Reviews" (new Amazon layout 2026-03)
                    total_match = re.search(r'([\d,]+)', total_text)
                    if total_match:
                        return int(total_match.group(1).replace(',', ''))

            return None

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
            currently_unavailable_xpaths = self.xpaths.get('unavailable') or [
                '//*[@id="outOfStock"]/div/div[1]/span[1]',
                '//*[@id="availability"]/span[2]/span',
                '//span[@class="a-color-price a-text-bold"]',
                '//span[@class="a-size-medium a-color-success"]'
            ]

            for xpath in currently_unavailable_xpaths:
                text = self.extract_text_safe(tree, xpath)
                if text and 'currently unavailable' in text.lower():
                    return "Currently unavailable."

            # PRIORITY 2: Check for "Price higher than typical" / "High price"
            # NOTE: Checked before "No featured offers" because they use same XPath location
            price_higher_xpaths = self.xpaths.get('price_higher') or [
                '//*[@id="fod-cx-message-with-learn-more"]/span[1]',
                '//span[@id="fod-cx-message-with-learn-more"]/span[1]',
                '//span[contains(text(), "Price higher than typical")]',
                '//span[contains(text(), "High price")]'
            ]

            for xpath in price_higher_xpaths:
                text = self.extract_text_safe(tree, xpath)
                if text:
                    text_lower = text.lower().strip()
                    if 'price higher than typical' in text_lower:
                        return "Price higher than typical"
                    if text_lower == 'high price':
                        return "High price"

            # PRIORITY 3: Check for "No featured offers available"
            no_offers_xpaths = self.xpaths.get('no_offers') or [
                '//*[@id="fod-cx-message-with-learn-more"]/span[1]',
                '//span[@id="fod-cx-message-with-learn-more"]/span[1]',
                '//span[contains(text(), "No featured offers available")]'
            ]

            for xpath in no_offers_xpaths:
                text = self.extract_text_safe(tree, xpath)
                if text and 'no featured offers available' in text.lower():
                    return "No featured offers available"

            # PRIORITY 4: Check for "See price in cart"
            see_price_xpaths = self.xpaths.get('see_price_cart') or [
                '//*[@id="corePriceDisplay_desktop_feature_div"]/table/tbody/tr/td[2]/span/a',
                '//a[contains(text(), "See price in cart")]',
                '//span[@class="a-declarative"]//a[contains(text(), "See price in cart")]'
            ]

            for xpath in see_price_xpaths:
                text = self.extract_text_safe(tree, xpath)
                if text and 'see price in cart' in text.lower():
                    return "See price in cart"

            # PRIORITY 5: Check for "To see our price, add this item to your cart."
            add_to_cart_xpaths = self.xpaths.get('add_cart_price') or [
                '//*[@id="corePriceDisplay_desktop_feature_div"]/table/tbody/tr/td[2]',
                '//table[@class="a-lineitem"]//td[contains(text(), "To see our price")]',
                '//td[contains(text(), "To see our price, add this item to your cart")]'
            ]

            for xpath in add_to_cart_xpaths:
                text = self.extract_text_safe(tree, xpath)
                if text and 'to see our price, add this item to your cart' in text.lower():
                    return "To see our price, add this item to your cart."

            # NORMAL EXTRACTION: Try to extract regular price
            xpaths = self.xpaths.get('price') or [
                '//*[@id="corePriceDisplay_desktop_feature_div"]/div[1]/span[1]',  # New primary xpath
                '//*[@id="corePriceDisplay_desktop_feature_div"]/div[1]/span[3]/span[2]',  # Main container
                '//*[@id="corePriceDisplay_desktop_feature_div"]/div[1]/span[3]/span[2]/span[1]',  # Main with span[1]
                '//span[@class="a-price aok-align-center reinventPricePriceToPayMargin priceToPay"]//span[@class="a-offscreen"]',  # Generic offscreen
                '//*[@id="corePrice_feature_div"]/div/div/span[1]/span[1]',  # Side container
                '//*[@id="corePrice_feature_div"]//span[@class="a-offscreen"]',  # Side generic
                '//*[@id="corePrice_desktop"]/div/table/tbody/tr/td[2]/span[1]/span[1]'  # Table-based price (fallback)
            ]

            for xpath in xpaths:
                price_text = self.extract_text_safe(tree, xpath)
                if price_text:
                    # Extract only "$XXX.XX" or "$X,XXX.XX" format
                    # Remove "with X percent savings" and other extra text
                    match = re.search(r'\$[\d,]+\.?\d*', price_text)
                    if match:
                        return match.group()
                    # No price pattern found, try next xpath
                    continue

            return None

        except Exception as e:
            print(f"  [WARNING] Failed to extract final SKU price: {e}")
            return None

    def extract_original_sku_price(self, tree):
        """Extract original SKU price from detail page (optional field)
        Limited to corePriceDisplay_desktop_feature_div container only to avoid picking prices from other sections
        """
        try:
            # Only search within corePriceDisplay_desktop_feature_div container
            xpaths = self.xpaths.get('original_price') or [
                '//*[@id="corePriceDisplay_desktop_feature_div"]/div[2]/span/span[1]/span[2]/span/span[1]',
                '//*[@id="corePriceDisplay_desktop_feature_div"]/div[2]//span[@class="a-offscreen"]'
            ]

            for xpath in xpaths:
                price_text = self.extract_text_safe(tree, xpath)
                if price_text:
                    # Extract "$149.99" format
                    return price_text.strip()

            return None

        except Exception as e:
            print(f"  [WARNING] Failed to extract original SKU price: {e}")
            return None

    def calculate_savings(self, final_price, original_price):
        """Calculate savings as difference between original and final price"""
        try:
            if not final_price or not original_price:
                return None

            # Extract numeric values from "$119.99" format
            import re
            final_match = re.search(r'[\d,]+\.?\d*', final_price.replace(',', ''))
            original_match = re.search(r'[\d,]+\.?\d*', original_price.replace(',', ''))

            if final_match and original_match:
                final_value = float(final_match.group())
                original_value = float(original_match.group())
                savings = original_value - final_value

                if savings > 0:
                    # Return in "$30.00" format
                    return f"${savings:.2f}"

            return None

        except Exception as e:
            print(f"  [WARNING] Failed to calculate savings: {e}")
            return None

    def extract_summarized_review(self, tree):
        """Extract AI-generated review summary (may not exist on all pages)"""
        try:
            # Use DB XPaths if available, otherwise use hardcoded fallback
            xpaths = self.xpaths.get('summarized_review') or [
                '//div[@data-testid="overall-summary"]//span[contains(@class, "__SAR2l0zNyyuZ")]',
                '//*[@id="product-summary"]/p[1]/span',
                '//div[@data-testid="overall-summary"]//span'
            ]

            for xpath in xpaths:
                summary = self.extract_text_safe(tree, xpath)
                if summary:
                    return summary
            return None
        except Exception as e:
            return None

    def extract_detailed_reviews(self, product_url):
        """Extract detailed reviews from product detail page (JavaScript 직접 추출)"""
        try:
            # 페이지 하단까지 점진적 스크롤 → lazy loading 트리거
            try:
                for _ in range(5):
                    self.page.run_js("window.scrollBy(0, window.innerHeight)")
                    time.sleep(0.5)
                time.sleep(2)
                print(f"  [DEBUG] scrolled to bottom for lazy loading")
            except Exception as e:
                print(f"  [DEBUG] scroll failed: {e}")

            # JavaScript로 리뷰 텍스트 직접 추출
            # 구조: [id^="customer_review-"] > ... > div[data-hook="review-collapsed"] > span
            js_code = """
            var reviews = [];
            var containers = document.querySelectorAll('[id^="customer_review-"], [id^="customer_review_foreign-"]');
            containers.forEach(function(container) {
                var collapsed = container.querySelector('[data-hook="review-collapsed"]');
                if (collapsed) {
                    var span = collapsed.querySelector('span');
                    if (span && span.innerText.trim().length > 5) {
                        reviews.push(span.innerText.trim());
                    }
                }
            });
            return reviews;
            """
            review_texts = self.page.run_js(js_code)
            print(f"  [DEBUG] JS extracted reviews: {len(review_texts) if review_texts else 0}")

            # JS 실패 시 fallback: DrissionPage XPath (DB 로드)
            if not review_texts:
                fallback_xpaths = self.xpaths.get('review_body_detail') or [
                    '//*[starts-with(@id, "customer_review-")]/div[4]/span/div/div[1]/span',
                    '//*[starts-with(@id, "customer_review-")]/div[4]/span/div/div[1]',
                    '//*[starts-with(@id, "customer_review_foreign-")]/div[4]/span/div/div[1]/span',
                    '//*[starts-with(@id, "customer_review_foreign-")]/div[4]/span/div/div[1]',
                ]
                for xpath in fallback_xpaths:
                    try:
                        first_elem = self.page.ele(f'xpath:{xpath}', timeout=10)
                        if first_elem:
                            elems = self.page.eles(f'xpath:{xpath}')
                            review_texts = []
                            for e in elems:
                                t = ' '.join((e.text or '').split())
                                if t and len(t) > 5:
                                    review_texts.append(t)
                            if review_texts:
                                print(f"  [DEBUG] Fallback XPath found: {len(review_texts)} reviews")
                                break
                    except Exception:
                        continue

            if not review_texts:
                print(f"  [DEBUG] No review elements found on detail page")
                return None

            all_reviews = []
            collected_reviews = set()
            for text in review_texts:
                review_text = ' '.join(text.split())
                review_text = re.sub(r'\s*Read more\s*$', '', review_text, flags=re.IGNORECASE)
                if review_text and len(review_text) > 5 and review_text not in collected_reviews:
                    all_reviews.append(review_text)
                    collected_reviews.add(review_text)

            print(f"  [DEBUG] extracted reviews: {len(all_reviews)}")
            if all_reviews:
                formatted_reviews = [f"review{idx} - {review}" for idx, review in enumerate(all_reviews, 1)]
                return ' ||| '.join(formatted_reviews)
            else:
                return None

        except Exception as e:
            print(f"  [WARNING] Failed to extract detailed reviews: {e}")
            return None

    def check_and_handle_sorry_page(self, max_retries=3):
        """Sorry/Robot check 페이지 감지 및 새로고침 재시도 (HHP 패턴)"""
        for attempt in range(max_retries):
            page_html_lower = self.page.html.lower()
            title_lower = (self.page.title or '').lower()

            is_sorry_page = (
                'sorry' in title_lower or
                'robot check' in title_lower or
                'sorry' in page_html_lower[:2000] or
                'robot check' in page_html_lower[:2000] or
                "couldn't find that page" in page_html_lower[:3000] or
                'dogs of amazon' in page_html_lower[:3000]
            )

            if is_sorry_page:
                print(f"  [WARNING] Sorry/Robot/Dogs page detected (attempt {attempt + 1}/{max_retries})")
                if attempt < max_retries - 1:
                    time.sleep(random.uniform(2, 3))
                    self.page.refresh()
                    time.sleep(random.uniform(5, 8))
                    continue
                else:
                    print(f"  [ERROR] Still sorry page after {max_retries} retries")
                    return False
            else:
                if attempt > 0:
                    print(f"  [OK] Page loaded after {attempt} refresh(es)")
                return True

        return False

    def extract_detailed_reviews_from_review_page(self, product_url):
        """Extract up to 20 detailed reviews and count_of_reviews from review pages
        Uses DrissionPage browser (HHP pattern) - clicks 'See more reviews' button

        Returns:
            tuple: (detailed_review_content, count_of_reviews)
        """
        # 재시도 포함 최대 2회 시도
        for attempt in range(2):
            try:
                if attempt > 0:
                    print(f"  [INFO] Retrying review extraction (attempt {attempt + 1}/2)...")
                    self.page.quit()
                    self.setup_driver()
                    self.load_cookies()
                    self.page.get(product_url)
                    time.sleep(random.uniform(5, 8))
                    tree = html.fromstring(self.page.html)
                    if not tree.xpath('//a[contains(@href, "product-reviews")]/@href'):
                        print(f"  [INFO] Page not loaded properly, refreshing...")
                        self.page.refresh()
                        time.sleep(random.uniform(5, 8))

                # Extract ASIN from product URL
                asin = self.extract_asin(product_url)

                # === DrissionPage 브라우저로 리뷰 페이지 접근 (HHP 패턴) ===
                # 1단계: 'See more reviews' 버튼 클릭 시도
                clicked = False
                try:
                    button = self.page.ele("xpath://a[@data-hook='see-all-reviews-link-foot']", timeout=5)
                    if button:
                        button.scroll.to_see()
                        time.sleep(1)
                        button.click()
                        clicked = True
                        print(f"  [OK] 'See more reviews' button clicked")
                except Exception:
                    pass

                # 2단계: 버튼 클릭 실패 시 URL로 직접 이동
                if not clicked:
                    review_url = f"https://www.amazon.com/product-reviews/{asin}/ref=cm_cr_dp_d_show_all_btm?ie=UTF8&reviewerType=all_reviews"
                    print(f"  [INFO] Button not found, navigating to: {review_url}")
                    self.page.get(review_url)

                time.sleep(random.uniform(8, 12))

                # 3단계: Sorry/Robot/Dogs 페이지 처리
                if not self.check_and_handle_sorry_page(max_retries=self.sorry_page_max_retry):
                    print(f"  [WARNING] Review page Sorry/Dogs - skipping")
                    self.page.get(product_url)
                    time.sleep(random.uniform(2, 3))
                    return None, None

                # 4단계: 로그인 페이지 리다이렉트 처리
                current_url = self.page.url
                if 'signin' in current_url or 'ap/signin' in current_url:
                    print(f"  [WARNING] Redirected to login page, attempting login...")
                    try:
                        login_to_amazon_dp(self.page, AMAZON_EMAIL, AMAZON_PASSWORD)
                        save_cookies_dp(self.page, COOKIE_FILE)
                        review_url = f"https://www.amazon.com/product-reviews/{asin}/ref=cm_cr_dp_d_show_all_btm?ie=UTF8&reviewerType=all_reviews"
                        self.page.get(review_url)
                        time.sleep(random.uniform(8, 12))
                        if 'signin' in self.page.url:
                            print(f"  [WARNING] Still on login page after login attempt")
                            self.page.get(product_url)
                            time.sleep(random.uniform(2, 3))
                            return None, None
                    except Exception as e:
                        print(f"  [WARNING] Login failed: {e}")
                        self.page.get(product_url)
                        time.sleep(random.uniform(2, 3))
                        return None, None

                # 5단계: 리뷰 페이지 HTML 파싱
                page_html = self.page.html
                tree = html.fromstring(page_html)

                page_html_lower = page_html.lower()
                if "couldn't find that page" in page_html_lower or 'dogs of amazon' in page_html_lower:
                    print(f"  [WARNING] Review page 404/Dogs detected in HTML")
                    self.page.get(product_url)
                    time.sleep(random.uniform(2, 3))
                    return None, None

                # 6단계: count_of_reviews 추출
                count_of_reviews = None
                count_xpaths = [
                    '//*[@id="filter-info-section"]/div',
                    '//div[@data-hook="cr-filter-info-review-rating-count"]',
                    '//div[contains(@data-hook, "review-rating-count")]',
                    '//*[@id="filter-info-section"]'
                ]

                for xpath in count_xpaths:
                    count_elements = tree.xpath(xpath)
                    if count_elements:
                        count_text = count_elements[0].text_content().strip() if hasattr(count_elements[0], 'text_content') else str(count_elements[0]).strip()
                        if count_text:
                            print(f"  [DEBUG] count_text found: {count_text[:100]}...")
                            match = re.search(r'([\d,]+)\s*customer\s*reviews?', count_text, re.IGNORECASE)
                            if not match:
                                match = re.search(r'([\d,]+)\s*with\s*reviews?', count_text, re.IGNORECASE)
                            if not match:
                                match = re.search(r'([\d,]+)\s*reviews?', count_text, re.IGNORECASE)
                            if not match:
                                match = re.search(r'([\d,]+)\s*total\s*ratings?,\s*([\d,]+)', count_text, re.IGNORECASE)
                                if match:
                                    count_of_reviews = match.group(2)
                                    print(f"  [OK] Extracted count_of_reviews (fallback): {count_of_reviews}")
                                    break
                            if match:
                                count_of_reviews = match.group(1)
                                print(f"  [OK] Extracted count_of_reviews: {count_of_reviews}")
                                break

                if not count_of_reviews:
                    print(f"  [WARNING] count_of_reviews not found")

                # 7단계: 리뷰 수집
                all_reviews = []
                collected_reviews = set()
                print(f"  [DEBUG] Parsing review page 1...")

                review_container_xpaths = self.xpaths.get('review_container') or [
                    '//div[starts-with(@id, "customer_review-") or starts-with(@id, "customer_review_foreign-")]',
                    '//div[@data-hook="review"]',
                ]
                review_containers = []
                review_container_xpath = None
                for xpath in review_container_xpaths:
                    review_containers = tree.xpath(xpath)
                    if review_containers:
                        review_container_xpath = xpath
                        break

                if review_containers:
                    for container in review_containers[:10]:
                        body_elem = container.xpath('.//span[@data-hook="review-body"]/span')
                        if body_elem:
                            review_text = body_elem[0].text_content().strip()
                            review_text = re.sub(r'\s*Read more\s*$', '', review_text, flags=re.IGNORECASE)
                            if review_text and review_text not in collected_reviews:
                                all_reviews.append(review_text)
                                collected_reviews.add(review_text)

                print(f"  [INFO] Review page 1: collected {len(all_reviews)} reviews")

                count_int = 0
                if count_of_reviews:
                    try:
                        count_int = int(str(count_of_reviews).replace(',', ''))
                    except:
                        count_int = 0

                # 8단계: 페이지네이션 (DrissionPage 브라우저로 Next 클릭)
                current_page = 1
                max_pages = self.max_review_pages

                while len(all_reviews) < self.target_reviews and current_page < max_pages:
                    try:
                        next_button = self.page.ele('xpath://li[@class="a-last"]/a', timeout=3)
                        if not next_button:
                            print(f"  [WARNING] Next page button not found")
                            break

                        next_button.scroll.to_see()
                        time.sleep(random.uniform(1, 2))
                        next_button.click()
                        current_page += 1
                        time.sleep(random.uniform(5, 10))

                        page_html = self.page.html
                        tree = html.fromstring(page_html)

                        review_containers = []
                        if review_container_xpath:
                            review_containers = tree.xpath(review_container_xpath)
                        if not review_containers:
                            for xpath in review_container_xpaths:
                                review_containers = tree.xpath(xpath)
                                if review_containers:
                                    review_container_xpath = xpath
                                    break

                        print(f"  [DEBUG] Review page {current_page}: found {len(review_containers)} review containers")

                        page_count = 0
                        duplicates = 0
                        if review_containers:
                            for container in review_containers[:10]:
                                if len(all_reviews) >= self.target_reviews:
                                    break
                                body_elem = container.xpath('.//span[@data-hook="review-body"]/span')
                                if body_elem:
                                    review_text = body_elem[0].text_content().strip()
                                    review_text = re.sub(r'\s*Read more\s*$', '', review_text, flags=re.IGNORECASE)
                                    if review_text:
                                        if review_text in collected_reviews:
                                            duplicates += 1
                                            continue
                                        all_reviews.append(review_text)
                                        collected_reviews.add(review_text)
                                        page_count += 1

                        if duplicates > 0:
                            print(f"  [WARNING] Found {duplicates} duplicate reviews on page {current_page}")
                        print(f"  [INFO] Review page {current_page}: added {page_count} reviews, total {len(all_reviews)} reviews")

                        if page_count == 0:
                            print(f"  [INFO] No new reviews on page {current_page}, stopping pagination")
                            break

                    except Exception as e:
                        print(f"  [WARNING] Could not fetch page {current_page + 1}: {e}")
                        break

                # 상세 페이지로 복귀
                self.page.get(product_url)
                time.sleep(random.uniform(2, 3))

                # max_pages까지 갔는데 target_reviews개 미만이면 재시도
                if count_int >= self.target_reviews and len(all_reviews) < self.target_reviews and attempt == 0:
                    print(f"  [INFO] Only collected {len(all_reviews)} reviews (expected {self.target_reviews}), will retry...")
                    continue

                # Limit to target_reviews and format as "1-review, 2-review, ..."
                reviews = all_reviews[:self.target_reviews]
                if reviews:
                    formatted_reviews = []
                    for idx, review in enumerate(reviews, 1):
                        formatted_reviews.append(f"{idx}-{review}")
                    print(f"  [OK] Collected {len(reviews)} detailed reviews from review page")
                    return ", ".join(formatted_reviews), count_of_reviews
                else:
                    print("  [WARNING] No reviews found on review page")
                    return None, count_of_reviews

            except Exception as e:
                print(f"  [WARNING] Failed to extract detailed reviews from review page: {e}")
                if attempt == 0:
                    continue
                return None, None

        print(f"  [WARNING] No reviews extracted after retries")
        return None, None

    def clean_shipping_text(self, text):
        """Clean shipping text by removing JavaScript and stopping at 'Join Prime'"""
        if not text:
            return None

        # Stop at "Join Prime" (and remove it)
        if "Join Prime" in text:
            text = text.split("Join Prime")[0].strip()

        # Remove JavaScript patterns (function(...) or any text containing curly braces with code)
        text = re.sub(r'\(function\(.*', '', text)
        text = re.sub(r'\{[^}]*\}', '', text)

        # Clean up extra whitespace
        text = ' '.join(text.split())

        return text.strip() if text.strip() else None

    def extract_shipping_info(self, tree):
        """Extract shipping info from up to 3 locations, concatenated with comma"""
        try:
            shipping_parts = []

            # Use DB XPaths if available, otherwise use hardcoded fallback
            # Location 1: Primary delivery message
            xpath1 = (self.xpaths.get('delivery_primary') or ['//*[@id="mir-layout-DELIVERY_BLOCK-slot-PRIMARY_DELIVERY_MESSAGE_LARGE"]/span'])[0]
            text1 = self.extract_text_safe(tree, xpath1)
            text1 = self.clean_shipping_text(text1)
            if text1:
                shipping_parts.append(text1)

            # Location 2: Secondary delivery message
            xpath2 = (self.xpaths.get('delivery_secondary') or ['//*[@id="mir-layout-DELIVERY_BLOCK-slot-SECONDARY_DELIVERY_MESSAGE_LARGE"]/span'])[0]
            text2 = self.extract_text_safe(tree, xpath2)
            text2 = self.clean_shipping_text(text2)
            if text2:
                shipping_parts.append(text2)

            # Location 3: Holiday delivery message
            xpath3 = (self.xpaths.get('delivery_holiday') or ['//*[@id="mir-layout-DELIVERY_BLOCK-slot-HOLIDAY_DELIVERY_MESSAGE"]/b/font'])[0]
            text3 = self.extract_text_safe(tree, xpath3)
            text3 = self.clean_shipping_text(text3)
            if text3:
                shipping_parts.append(text3)

            if shipping_parts:
                return ', '.join(shipping_parts)
            return None

        except Exception as e:
            print(f"  [WARNING] Failed to extract shipping info: {e}")
            return None

    def extract_available_quantity_for_purchase(self, tree):
        """Extract available quantity (e.g., '6' from 'Only 6 left in stock')
        - If starts with 'Only': extract the number
        - If 'In Stock': return 'In Stock'
        - Otherwise: return None
        """
        try:
            # Use DB XPath if available, otherwise use hardcoded fallback
            xpath = (self.xpaths.get('availability') or ['//*[@id="availability"]/span'])[0]
            text = self.extract_text_safe(tree, xpath)
            if text:
                text = text.strip()
                # Extract number if starts with "Only"
                if text.startswith("Only"):
                    match = re.search(r'Only\s+(\d+)', text)
                    if match:
                        return match.group(1)
                # Return "In Stock" if text is "In Stock"
                elif text.lower() == "in stock":
                    return "In Stock"
            return None

        except Exception as e:
            print(f"  [WARNING] Failed to extract available quantity: {e}")
            return None

    def extract_number_of_units_purchased_past_month(self, tree):
        """Extract number of units purchased past month from detail page
        - '300+ bought' -> 300
        - '10K+ bought' -> 10000
        - '1.5K+ bought' -> 1500
        Returns integer value for DB storage
        """
        try:
            # Use DB XPath if available, otherwise use hardcoded fallback
            xpath = (self.xpaths.get('number_of_units_purchased_past_month') or ['//*[@id="social-proofing-faceout-title-tk_bought"]/span[1]'])[0]
            text = self.extract_text_safe(tree, xpath)
            if text:
                text = text.strip()
                # Extract number part with optional K (e.g., "300" or "10K" or "1.5K")
                match = re.search(r'([\d.]+)(K?)\s*\+?\s*bought', text, re.IGNORECASE)
                if match:
                    num_str = match.group(1)
                    suffix = match.group(2).upper()
                    try:
                        num = float(num_str)
                        if suffix == 'K':
                            num = int(num * 1000)
                        else:
                            num = int(num)
                        return num
                    except ValueError:
                        return None
            return None

        except Exception as e:
            print(f"  [WARNING] Failed to extract number_of_units_purchased_past_month: {e}")
            return None

    def extract_discount_type(self, tree):
        """Extract discount type (e.g., 'Limited time deal', 'Ends in 13 hours 7 minutes')"""
        try:
            # Use DB XPath if available, otherwise use hardcoded fallback
            xpath = (self.xpaths.get('discount_type') or ['//*[@id="dealBadgeSupportingText"]'])[0]
            text = self.extract_text_safe(tree, xpath)
            if text:
                # 카운트다운 타이머인 경우
                if 'Ends in' in text:
                    # 1순위: 스크린 리더 라벨에서 깔끔한 텍스트 추출
                    sr_xpaths = [
                        '//*[@id="deals_countdown_timer_from_hours_screen_reader_label" and not(contains(@class, "aok-hidden"))]',
                        '//*[@id="deals_countdown_timer_from_minutes_without_seconds_screen_reader_label" and not(contains(@class, "aok-hidden"))]',
                        '//*[@id="deals_countdown_timer_from_seconds_screen_reader_label" and not(contains(@class, "aok-hidden"))]',
                    ]
                    for sr_xpath in sr_xpaths:
                        sr_text = self.extract_text_safe(tree, sr_xpath)
                        if sr_text and 'NO_OF' not in sr_text:
                            return re.sub(r'\s+', ' ', sr_text).strip()
                    # 2순위: 타이머 span에서 직접 추출 (e.g., "13:07:04")
                    timer_text = self.extract_text_safe(tree, '//*[@id="detailpage-dealBadge-countdown-timer"]')
                    if timer_text:
                        return f"Ends in {timer_text.strip()}"
                    return None
                # 일반 텍스트 (e.g., "Limited time deal")
                return re.sub(r'\s+', ' ', text).strip()
            return None

        except Exception as e:
            print(f"  [WARNING] Failed to extract discount type: {e}")
            return None

    def scrape_detail_page(self, url_data):
        """Scrape detail page and extract information"""
        try:
            page_type = url_data['page_type']
            url = url_data['url']

            print(f"\n{'='*80}")
            print(f"[{page_type.upper()}] Accessing: {url[:80]}...")
            print(f"[INFO] Page type: {page_type} | Main rank: {url_data.get('main_rank', 'N/A')} | BSR rank: {url_data.get('bsr_rank', 'N/A')}")

            self.page.get(url)
            time.sleep(random.uniform(3, 5))

            # Verify page is properly loaded before extraction (with retry logic)
            page_loaded = self.verify_page_loaded(wait_for_price=True)
            skip_sku_name = False  # Flag to skip retailer_sku_name extraction

            if not page_loaded:
                # 1차 재시도: URL 재접속
                print(f"  [RETRY 1/2] Page verification failed - retrying with URL re-access...")
                time.sleep(3)
                self.page.get(url)
                time.sleep(random.uniform(3, 5))
                page_loaded = self.verify_page_loaded(wait_for_price=True)

                if not page_loaded:
                    # 2차 재시도: 쿠키 재로드 후 URL 재접속
                    print(f"  [RETRY 2/2] Still failed - reloading cookies and retrying...")
                    self.load_cookies()
                    self.page.get(url)
                    time.sleep(random.uniform(3, 5))
                    page_loaded = self.verify_page_loaded(wait_for_price=True)

                    if not page_loaded:
                        # 3차 실패: 제품명 제외하고 나머지 수집 시도
                        print(f"  [WARNING] Page verification failed after retries - continuing without sku_name")
                        self.log_failed_url(url, "no_sku_name")
                        skip_sku_name = True

            print(f"  [INFO] Page loaded and verified, extracting data...")

            # Click "Item details" section to expand it (needed for item, rank_1, rank_2)
            try:
                # Find "Item details" button specifically (not "Display")
                item_details_button = self.page.ele('xpath://span[contains(text(), "Item details")]/ancestor::a[contains(@class, "a-expander-header")]', timeout=3)
                if item_details_button:
                    # Check if already expanded
                    aria_expanded = item_details_button.attr('aria-expanded')
                    if aria_expanded != "true":
                        item_details_button.click(by_js=True)
                        time.sleep(1)
                        print("  [INFO] Expanded 'Item details' section")
                    else:
                        print("  [INFO] 'Item details' already expanded")
            except Exception as e:
                print(f"  [WARNING] Could not find/click 'Item details': {e}")

            page_source = self.page.html
            tree = html.fromstring(page_source)

            # Extract data
            if skip_sku_name:
                retailer_sku_name = None  # Skip extraction due to page load failure
                print(f"  [INFO] Skipping retailer_sku_name extraction (page load failed)")
            else:
                retailer_sku_name = self.extract_with_xpaths(tree, 'product_name', ['//*[@id="productTitle"]'])

                # retailer_sku_name NULL 시 refresh 후 재추출
                if not retailer_sku_name:
                    print(f"  [WARNING] retailer_sku_name is NULL - refreshing page and retrying...")
                    self.page.refresh()
                    time.sleep(10)
                    page_source = self.page.html
                    tree = html.fromstring(page_source)
                    retailer_sku_name = self.extract_with_xpaths(tree, 'product_name', ['//*[@id="productTitle"]'])
                    print(f"  [INFO] Retry result - retailer_sku_name: {'OK' if retailer_sku_name else 'STILL NULL'}")

            star_rating = self.extract_star_rating(tree)

            # SKU_Popularity - only collect if "Amazon's Choice"
            sku_popularity_raw = self.extract_with_xpaths(tree, 'sku_popularity')
            sku_popularity = sku_popularity_raw if sku_popularity_raw and "Amazon's" in sku_popularity_raw and "Choice" in sku_popularity_raw else None

            # Retailer_Membership_Discounts - clean Prime text
            membership_discount_raw = self.extract_with_xpaths(tree, 'membership_discount')
            membership_discount = self.clean_membership_discount(membership_discount_raw)

            # Item - Extract ASIN from final URL (after redirect)
            # sspa/click URL redirects to actual product page with /dp/ASIN/
            final_url = self.page.url
            item = self.extract_asin(final_url)

            if item and len(item) == 10:
                print(f"  [OK] Extracted item from URL (ASIN): {item}")
                # Real-time duplicate check (for sspa/click redirects)
                if item in self.processed_asins:
                    print(f"  [SKIP] Already processed ASIN in this session: {item}")
                    return False
                self.processed_asins.add(item)
            else:
                print(f"  [WARNING] Could not extract valid ASIN from URL: {final_url}")
                item = None

            # Ranks - extract using DB xpaths
            rank_1_raw = self.extract_with_xpaths(tree, 'rank1')
            rank_1 = self.clean_rank(rank_1_raw)

            rank_2_raw = self.extract_with_xpaths(tree, 'rank2')
            rank_2 = self.clean_rank(rank_2_raw)

            # Extract screen_size with tv_item_mst fallback
            extracted_screen_size = self.extract_screen_size(tree, retailer_sku_name)
            item_mst_data = self.get_item_mst_data(item)
            mst_screen_size = item_mst_data.get('screen_size') if item_mst_data else None

            # Determine final screen_size with fallback and mismatch tracking
            if extracted_screen_size and mst_screen_size:
                if extracted_screen_size != mst_screen_size:
                    print(f"  [WARNING] screen_size mismatch: extracted='{extracted_screen_size}', tv_item_mst='{mst_screen_size}'")
                    self.screen_size_mismatch_records.append({
                        'item': item,
                        'url': url,
                        'extracted': extracted_screen_size,
                        'mst_value': mst_screen_size
                    })
                screen_size = extracted_screen_size  # Use extracted value
            elif extracted_screen_size:
                screen_size = extracted_screen_size  # No mst value, use extracted
            elif mst_screen_size:
                screen_size = mst_screen_size  # Fallback to mst value
                print(f"  [INFO] Using screen_size from tv_item_mst: {mst_screen_size}")
            else:
                screen_size = None

            # Extract model_year (NEW)
            model_year = self.extract_model_year(tree)

            # Extract SKU (Model Number) for tv_item_mst
            sku = self.extract_sku(tree)
            print(f"  [✓] SKU (Model Number): {sku}")

            # Extract count of star ratings
            count_of_star_ratings = self.extract_count_of_star_ratings(tree)

            # Extract summarized review content (dynamically loaded by JavaScript)
            summarized_review_content = None
            try:
                # Wait for the summarized review element to load (up to 10 seconds)
                summary_element = self.page.ele('xpath://div[@data-testid="overall-summary"]//span[contains(@class, "__SAR2l0zNyyuZ")]', timeout=10)
                if summary_element:
                    summarized_review_content = summary_element.text.strip() if summary_element.text else None
                if summarized_review_content:
                    print(f"  [INFO] Found summarized review: {summarized_review_content[:50]}...")
            except Exception as e:
                print(f"  [WARNING] Summarized review not found (may not exist for this product): {str(e)[:100]}")

            # Extract prices from detail page BEFORE navigating to review page
            # This ensures price extraction happens on the fully loaded detail page
            final_sku_price = self.extract_final_sku_price(tree)
            # If final_sku_price is a special text value, set original_sku_price to None
            special_price_texts = [
                "Currently unavailable.",
                "Price higher than typical",
                "No featured offers available",
                "See price in cart",
                "To see our price, add this item to your cart."
            ]
            if final_sku_price in special_price_texts:
                original_sku_price = None
            else:
                original_sku_price = self.extract_original_sku_price(tree)

            # final_sku_price가 original_sku_price보다 높으면 original_sku_price를 None으로 설정
            if final_sku_price and original_sku_price:
                try:
                    final_match = re.search(r'[\d,]+\.?\d*', final_sku_price.replace(',', ''))
                    original_match = re.search(r'[\d,]+\.?\d*', original_sku_price.replace(',', ''))
                    if final_match and original_match:
                        final_val = float(final_match.group().replace(',', ''))
                        original_val = float(original_match.group().replace(',', ''))
                        if final_val > original_val:
                            print(f"  [INFO] final_sku_price({final_sku_price}) > original_sku_price({original_sku_price}), setting original to None")
                            original_sku_price = None
                except Exception as e:
                    print(f"  [WARNING] Price comparison failed: {e}")

            savings = self.calculate_savings(final_sku_price, original_sku_price)

            # Extract shipping info, available quantity, and discount type
            shipping_info = self.extract_shipping_info(tree)
            available_quantity = self.extract_available_quantity_for_purchase(tree)
            discount_type = self.extract_discount_type(tree)
            number_of_units_purchased = self.extract_number_of_units_purchased_past_month(tree)

            # [주석처리] 리뷰페이지 접속 중단 - 상세페이지에서 리뷰 수집
            # # Extract detailed review content and count_of_reviews from review page (up to 20 reviews)
            # # Skip review page navigation if star_rating == "No customer reviews"
            # # This avoids collecting wrong reviews from bundle products (e.g., Asurion warranty reviews)
            #
            # # First check for "0 customer reviews" pattern on detail page
            # # e.g., "There are 0 customer reviews and 2 customer ratings."
            # zero_reviews_detected = False
            # zero_reviews_xpaths = [
            #     '//*[@id="reviewsMedley"]//div[@class="a-box-inner"]',
            #     '//*[@id="reviewsMedley"]/div/div[2]/div/div[2]/div[3]/div[2]/div/div',
            #     '//div[contains(text(), "customer reviews and")]'
            # ]
            # for xpath in zero_reviews_xpaths:
            #     zero_text = self.extract_text_safe(tree, xpath)
            #     if zero_text:
            #         match = re.search(r'(\d+)\s*customer\s*reviews?', zero_text, re.IGNORECASE)
            #         if match and match.group(1) == '0':
            #             print(f"  [INFO] Detected '0 customer reviews' on detail page: {zero_text[:80]}...")
            #             zero_reviews_detected = True
            #             break
            #
            # if star_rating == "No customer reviews" or zero_reviews_detected:
            #     print(f"  [INFO] Skipping review page - No customer reviews (count_of_reviews=0)")
            #     detailed_review_content = None
            #     count_of_reviews = "0"
            # else:
            #     detailed_review_content, count_of_reviews = self.extract_detailed_reviews_from_review_page(url)

            # count_of_reviews: None (DB에 NULL 저장)
            count_of_reviews = None
            # detailed_review_content: 상세페이지에서 수집 (기존 extract_detailed_reviews 함수 활용)
            detailed_review_content = self.extract_detailed_reviews(url)

            data = {
                'page_type': page_type,
                'product_url': final_url,
                'Retailer_SKU_Name': retailer_sku_name,
                'Star_Rating': star_rating,
                'SKU_Popularity': sku_popularity,
                'Retailer_Membership_Discounts': membership_discount,
                'item': item,
                'sku': sku,  # Model Number for tv_item_mst
                'Rank_1': rank_1,
                'Rank_2': rank_2,
                'screen_size': screen_size,
                'model_year': model_year,  # Extracted from item details dialog
                'count_of_reviews': count_of_reviews,
                'Count_of_Star_Ratings': count_of_star_ratings,
                'Summarized_Review_Content': summarized_review_content,
                'Detailed_Review_Content': detailed_review_content,
                'main_rank': url_data.get('main_rank'),
                'bsr_rank': url_data.get('bsr_rank'),
                'number_of_units_purchased_past_month': number_of_units_purchased,  # Extracted from detail page
                'final_sku_price': final_sku_price,  # Extracted from detail page
                'original_sku_price': original_sku_price,  # Extracted from detail page
                'savings': savings,  # Calculated from prices
                'shipping_info': shipping_info,
                'available_quantity_for_purchase': available_quantity,
                'discount_type': discount_type
            }

            # Check if all 5 key fields are null - retry with 3x wait time
            key_fields_null = (
                not retailer_sku_name and
                not star_rating and
                not count_of_star_ratings and
                not count_of_reviews and
                not final_sku_price
            )

            if key_fields_null:
                print(f"  [WARNING] All 5 key fields are null - retrying with extended wait for all elements...")

                # Re-access the URL
                self.page.get(url)

                # Wait for all key elements to load with extended timeout
                print(f"  [INFO] Waiting for product title element...")
                if self.page.ele('#productTitle', timeout=30):
                    print(f"  [OK] Product title element loaded")
                else:
                    print(f"  [WARNING] Product title element not loaded within 30s")

                print(f"  [INFO] Waiting for star rating element...")
                if self.page.ele('xpath://*[@id="acrPopover"] | //*[@id="averageCustomerReviews"]', timeout=30):
                    print(f"  [OK] Star rating element loaded")
                else:
                    print(f"  [WARNING] Star rating element not loaded within 30s")

                print(f"  [INFO] Waiting for count of star ratings element...")
                if self.page.ele('#acrCustomerReviewText', timeout=30):
                    print(f"  [OK] Count of star ratings element loaded")
                else:
                    print(f"  [WARNING] Count of star ratings element not loaded within 30s")

                print(f"  [INFO] Waiting for price element...")
                if self.page.ele('xpath://*[@id="corePriceDisplay_desktop_feature_div"] | //*[@id="corePrice_feature_div"]', timeout=30):
                    print(f"  [OK] Price element loaded")
                else:
                    print(f"  [WARNING] Price element not loaded within 30s")

                print(f"  [INFO] All element waits complete, re-extracting data...")

                # Re-parse page
                page_source = self.page.html
                tree = html.fromstring(page_source)

                # Re-extract key fields
                retailer_sku_name = self.extract_with_xpaths(tree, 'product_name', ['//*[@id="productTitle"]'])
                star_rating = self.extract_star_rating(tree)
                count_of_star_ratings = self.extract_count_of_star_ratings(tree)
                final_sku_price = self.extract_final_sku_price(tree)

                # [주석처리] 리뷰페이지 접속 중단 - 상세페이지에서 리뷰 수집
                count_of_reviews = None
                detailed_review_content = self.extract_detailed_reviews(url)

                # Update data dict
                data['Retailer_SKU_Name'] = retailer_sku_name
                data['Star_Rating'] = star_rating
                data['Count_of_Star_Ratings'] = count_of_star_ratings
                data['count_of_reviews'] = count_of_reviews
                data['final_sku_price'] = final_sku_price
                data['Detailed_Review_Content'] = detailed_review_content

                print(f"  [INFO] Retry complete - Name: {'OK' if retailer_sku_name else 'NULL'}, Star: {'OK' if star_rating else 'NULL'}, Price: {'OK' if final_sku_price else 'NULL'}")

            # Check for error conditions and retry up to 3 times
            try:
                def check_error_conditions(data):
                    """Check for null/error conditions in extracted data"""
                    cor = data.get('count_of_reviews')
                    cosr = data.get('Count_of_Star_Ratings')
                    drc = data.get('Detailed_Review_Content')
                    fsp = data.get('final_sku_price')
                    sr = data.get('Star_Rating')
                    src = data.get('Summarized_Review_Content')

                    cor_int = int(str(cor).replace(',', '')) if cor else 0
                    cosr_int = int(str(cosr).replace(',', '')) if cosr else 0
                    drc_is_null = drc is None or (isinstance(drc, str) and drc.strip() == '')
                    fsp_is_null = fsp is None or (isinstance(fsp, str) and fsp.strip() == '')
                    sr_is_null = sr is None or str(sr).strip() == ''
                    sr_is_no_reviews = sr is not None and str(sr).strip().lower() == 'no customer reviews'
                    cor_is_null = cor is None
                    cosr_is_null = cosr is None
                    src_is_null = src is None or (isinstance(src, str) and src.strip() == '')

                    # Error conditions
                    has_sr_null = sr_is_null  # star_rating is null
                    has_cor_null = cor_is_null  # count_of_reviews is null
                    has_cosr_null = cosr_is_null and not sr_is_no_reviews  # count_of_star_ratings is null (but ok if no reviews)
                    has_fsp_null = fsp_is_null  # final_sku_price is null
                    has_rv_detail_null = cor_int > 0 and drc_is_null  # reviews > 0 but content null
                    has_src_null = cor_int > 0 and src_is_null  # reviews > 0 but summarized review null

                    # 수집된 리뷰 개수 계산 (count가 수집된 개수보다 크면 재시도, 최대 20개까지)
                    collected_review_count = 0
                    if drc and isinstance(drc, str):
                        collected_review_count = len(drc.split(' ||| '))
                    has_rv_insufficient = cor_int > collected_review_count and collected_review_count < 20

                    return {
                        'has_sr_null': has_sr_null,
                        'has_cor_null': has_cor_null,
                        'has_cosr_null': has_cosr_null,
                        'has_fsp_null': has_fsp_null,
                        'has_rv_detail_null': has_rv_detail_null,
                        'has_rv_insufficient': has_rv_insufficient,
                        'has_src_null': has_src_null,
                        'cor_int': cor_int,
                        'cosr_int': cosr_int,
                        'collected_review_count': collected_review_count,
                        'sr': sr,
                        'sr_is_no_reviews': sr_is_no_reviews
                    }

                def has_any_error(errors):
                    """Check if any error condition exists (count_of_reviews 제외 - XPath 변경으로 수집 불가해도 진행)"""
                    return (errors['has_sr_null'] or
                            errors['has_cosr_null'] or errors['has_fsp_null'] or
                            errors['has_rv_detail_null'] or errors['has_rv_insufficient'] or
                            errors['has_src_null'])

                def re_extract_summarized_review():
                    """Re-extract summarized review content"""
                    try:
                        summary_element = self.page.ele('xpath://div[@data-testid="overall-summary"]//span[contains(@class, "__SAR2l0zNyyuZ")]', timeout=10)
                        if summary_element:
                            return summary_element.text.strip() if summary_element.text else None
                        return None
                    except:
                        return None

                def re_extract_fields(errors, url, data):
                    """Re-extract fields based on error type"""
                    tree = html.fromstring(self.page.html)

                    if errors['has_sr_null']:
                        sr = self.extract_star_rating(tree)
                        data['Star_Rating'] = sr
                        print(f"    - star_rating: {sr}")

                    if errors['has_cosr_null']:
                        cosr = self.extract_count_of_star_ratings(tree)
                        data['Count_of_Star_Ratings'] = cosr
                        print(f"    - count_of_star_ratings: {cosr}")

                    if errors['has_fsp_null']:
                        fsp = self.extract_final_sku_price(tree)
                        data['final_sku_price'] = fsp
                        print(f"    - final_sku_price: {fsp}")

                    if errors['has_src_null']:
                        src = re_extract_summarized_review()
                        data['Summarized_Review_Content'] = src
                        print(f"    - summarized_review: {'OK' if src else 'NULL'}")

                    if errors['has_rv_detail_null'] or errors['has_rv_insufficient']:
                        # [주석처리] 리뷰페이지 접속 중단 - 상세페이지에서 리뷰 수집
                        data['count_of_reviews'] = None
                        data['Detailed_Review_Content'] = self.extract_detailed_reviews(url)
                        new_collected = len((data['Detailed_Review_Content'] or '').split(' ||| ')) if data['Detailed_Review_Content'] else 0
                        print(f"    - count_of_reviews: None, detailed_review: {new_collected} collected (from detail page)")

                # Initial error check
                errors = check_error_conditions(data)

                # Retry once if any error detected (1회만 재시도)
                if has_any_error(errors):
                    print(f"  [RETRY 1/1] Errors: sr={errors['has_sr_null']}, cor={errors['has_cor_null']}, cosr={errors['has_cosr_null']}, fsp={errors['has_fsp_null']}, rv={errors['has_rv_detail_null']}, rv_insuf={errors['has_rv_insufficient']}, src={errors['has_src_null']}")
                    print(f"  [RETRY 1/1] Refreshing page...")
                    self.page.refresh()
                    time.sleep(3)

                    # Re-extract fields
                    print(f"  [RETRY 1/1] Re-extracting fields...")
                    re_extract_fields(errors, url, data)

                    # Check if errors are resolved
                    errors = check_error_conditions(data)
                    if not has_any_error(errors):
                        print(f"  [OK] All errors resolved after retry")
                    else:
                        print(f"  [WARNING] Some errors persist after retry")

                # Final error check for logging
                errors = check_error_conditions(data)
                cor_int = errors['cor_int']
                cosr_int = errors['cosr_int']
                sr = errors['sr']

                # Add to error records only if error persists after all retries
                if errors['has_rv_detail_null']:
                    print(f"  [WARNING] rv_detail_null persists: count_of_reviews={cor_int}, detailed_review_content=NULL")
                    print(f"            URL: {data.get('product_url', 'N/A')}")
                    self.rv_detail_null_records.append({
                        'url': data.get('product_url', 'N/A'),
                        'count_of_reviews': cor_int,
                        'count_of_star_ratings': cosr_int,
                        'account': self.current_account
                    })

                if errors['has_rv_insufficient']:
                    print(f"  [WARNING] rv_insufficient persists: count_of_reviews={cor_int}, collected={errors['collected_review_count']}")
                    print(f"            URL: {data.get('product_url', 'N/A')}")

                if errors['has_fsp_null']:
                    print(f"  [WARNING] fsp_null persists: final_sku_price=NULL")
                    print(f"            URL: {data.get('product_url', 'N/A')}")
                    self.fsp_null_records.append({
                        'url': data.get('product_url', 'N/A'),
                        'account': self.current_account
                    })

                if errors['has_cosr_null']:
                    print(f"  [WARNING] cosr_null persists: star_rating={sr}, count_of_star_ratings=NULL")
                    print(f"            URL: {data.get('product_url', 'N/A')}")
                    self.cosr_null_records.append({
                        'url': data.get('product_url', 'N/A'),
                        'star_rating': sr,
                        'account': self.current_account
                    })

                # reviews_equals_ratings: not retryable (data pattern issue, not fetch error)
                if cor_int > 0 and cosr_int > 0 and cor_int == cosr_int:
                    print(f"  [WARNING] reviews_equals_ratings detected: count_of_reviews={cor_int}, count_of_star_ratings={cosr_int}")
                    print(f"            URL: {data.get('product_url', 'N/A')}")
                    self.reviews_equals_ratings_records.append({
                        'url': data.get('product_url', 'N/A'),
                        'count_of_reviews': cor_int,
                        'count_of_star_ratings': cosr_int,
                        'account': self.current_account
                    })
            except Exception as e:
                print(f"  [WARNING] Error check failed: {str(e)[:100]}")

            # Save to database
            if self.save_to_db(data):
                self.total_collected += 1
                print(f"  [OK] Collected: {retailer_sku_name[:50] if retailer_sku_name else '[NO NAME]'}...")
                print(f"       Star: {star_rating or 'N/A'} | Popularity: {sku_popularity or 'N/A'}")
                print(f"       Price: {final_sku_price or 'N/A'} | Original: {original_sku_price or 'N/A'}")
                print(f"       Rank1: {rank_1 or 'N/A'} | Rank2: {rank_2 or 'N/A'}")
                print(f"       Main Rank: {data['main_rank'] or 'N/A'} | BSR Rank: {data['bsr_rank'] or 'N/A'}")
                print(f"       Screen Size: {screen_size or 'N/A'} | Reviews Count: {count_of_reviews or 'N/A'}")
                print(f"       Star Counts: {count_of_star_ratings or 'N/A'}")
                print(f"       Review Summary: {summarized_review_content[:80] + '...' if summarized_review_content and len(summarized_review_content) > 80 else summarized_review_content or 'N/A'}")

                # Show detailed review count (use data dict for retry-updated value)
                final_drc = data.get('Detailed_Review_Content')
                if final_drc:
                    try:
                        review_count = len(final_drc.split(' ||| '))
                        print(f"       Detailed Reviews: {review_count} collected")
                    except:
                        print(f"       Detailed Reviews: N/A")
                else:
                    print(f"       Detailed Reviews: N/A")

                return True
            else:
                print(f"  [FAILED] Could not save data")
                return False

        except Exception as e:
            print(f"  [ERROR] Failed to scrape detail page: {e}")
            return False

    def save_to_db(self, data):
        """Save collected data to database"""
        cursor = None
        try:
            print(f"  [DB] Saving to database...")
            print(f"       Product: {(data.get('Retailer_SKU_Name') or 'N/A')[:60]}...")
            print(f"       Item (SKU): {data.get('item', 'N/A')}")

            # Temporarily disable autocommit for transaction
            self.db_conn.autocommit = False

            cursor = self.db_conn.cursor()

            # Calculate calendar week
            calendar_week = f"w{datetime.now().isocalendar().week}"

            # Calculate crawl_datetime (format: 2025-11-04 03:00:55)
            now = datetime.now()
            crawl_datetime = now.strftime('%Y-%m-%d %H:%M:%S')

            # If "No customer reviews", set count_of_star_ratings and count_of_reviews to 0
            if data['Star_Rating'] == "No customer reviews":
                data['Count_of_Star_Ratings'] = 0
                data['count_of_reviews'] = "0"

            # Insert to amazon_tv_detail_crawled
            cursor.execute("""
                INSERT INTO amazon_tv_detail_crawled
                (account_name, batch_id, page_type, product_url, Retailer_SKU_Name, Star_Rating,
                 SKU_Popularity, Retailer_Membership_Discounts, item,
                 Rank_1, Rank_2, screen_size, count_of_reviews, Count_of_Star_Ratings,
                 Summarized_Review_Content, Detailed_Review_Content, calendar_week, crawl_datetime,
                 main_rank, bsr_rank, final_sku_price, original_sku_price)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                'Amazon',
                self.batch_id,
                data['page_type'],
                data['product_url'],
                data['Retailer_SKU_Name'],
                data['Star_Rating'],
                data['SKU_Popularity'],
                data['Retailer_Membership_Discounts'],
                data['item'],
                data['Rank_1'],
                data['Rank_2'],
                data['screen_size'],
                data['count_of_reviews'],
                data['Count_of_Star_Ratings'],
                data['Summarized_Review_Content'],
                data['Detailed_Review_Content'],
                calendar_week,
                crawl_datetime,
                data['main_rank'],
                data['bsr_rank'],
                data['final_sku_price'],
                data['original_sku_price']
            ))

            # Also insert into unified tv_retail_com table
            # Convert count_of_reviews to integer (remove commas if present)
            count_of_reviews_int = None
            if data['count_of_reviews']:
                try:
                    count_of_reviews_int = int(str(data['count_of_reviews']).replace(',', ''))
                except:
                    count_of_reviews_int = None

            cursor.execute("""
                INSERT INTO tv_retail_com
                (item, account_name, page_type, count_of_reviews, retailer_sku_name, product_url,
                 star_rating, count_of_star_ratings, screen_size, sku_popularity,
                 final_sku_price, original_sku_price, savings, discount_type, offer,
                 pick_up_availability, shipping_availability, delivery_availability, shipping_info,
                 available_quantity_for_purchase, inventory_status, sku_status, retailer_membership_discounts,
                 detailed_review_content, summarized_review_content, top_mentions, recommendation_intent,
                 main_rank, bsr_rank, rank_1, rank_2, promotion_position, trend_rank,
                 number_of_ppl_purchased_yesterday, number_of_ppl_added_to_carts, retailer_sku_name_similar,
                 estimated_annual_electricity_use, promotion_type, number_of_units_purchased_past_month, model_year,
                 calendar_week, crawl_datetime)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                data['item'],
                'Amazon',  # account_name
                data['page_type'],
                count_of_reviews_int,  # Converted to integer
                data['Retailer_SKU_Name'],
                data['product_url'],
                data['Star_Rating'],
                data['Count_of_Star_Ratings'],  # Now integer (total count)
                data['screen_size'],
                data['SKU_Popularity'],
                data['final_sku_price'],  # Extracted from detail page
                data['original_sku_price'],  # Extracted from detail page
                data['savings'],  # Calculated from prices
                data.get('discount_type'),  # discount_type
                None,  # offer (Amazon doesn't have this)
                None,  # pick_up_availability (Amazon doesn't have this)
                None,  # shipping_availability (Amazon doesn't have this)
                None,  # delivery_availability (Amazon doesn't have this)
                data.get('shipping_info'),  # shipping_info
                data.get('available_quantity_for_purchase'),  # available_quantity_for_purchase
                None,  # inventory_status (Amazon doesn't have this)
                None,  # sku_status (Amazon doesn't have this)
                data['Retailer_Membership_Discounts'],
                data['Detailed_Review_Content'],
                data['Summarized_Review_Content'],
                None,  # top_mentions (Amazon doesn't have this)
                None,  # recommendation_intent (Amazon doesn't have this)
                data['main_rank'],
                data['bsr_rank'],
                data['Rank_1'],
                data['Rank_2'],
                None,  # promotion_position (Amazon doesn't have this)
                None,  # trend_rank (Amazon doesn't have this)
                None,  # number_of_ppl_purchased_yesterday (Amazon doesn't have this)
                None,  # number_of_ppl_added_to_carts (Amazon doesn't have this)
                None,  # retailer_sku_name_similar (Amazon doesn't have this)
                None,  # estimated_annual_electricity_use (Amazon doesn't have this)
                None,  # promotion_type (Amazon doesn't have this)
                data.get('number_of_units_purchased_past_month'),  # From main_crawled
                data.get('model_year'),  # From item details dialog
                calendar_week,
                crawl_datetime
            ))

            # Insert into tv_item_mst (with duplicate check on item, update sku and screen_size on conflict)
            if data.get('item'):
                # # Check existing sku before upsert (temporarily disabled)
                # cursor.execute("""
                #     SELECT sku FROM tv_item_mst WHERE item = %s
                # """, (data['item'],))
                # existing_row = cursor.fetchone()
                # existing_sku = existing_row[0] if existing_row else None

                new_sku = data.get('sku', 'no sku')
                cursor.execute("""
                    INSERT INTO tv_item_mst (item, product_url, sku, account_name, screen_size)
                    VALUES (%s, %s, %s, %s, %s)
                    ON CONFLICT (item) DO UPDATE SET
                        screen_size = COALESCE(tv_item_mst.screen_size, EXCLUDED.screen_size)
                """, (
                    data['item'],
                    data['product_url'],
                    new_sku,
                    'Amazon',
                    data.get('screen_size')
                ))
                print(f"  [DB] ✓ tv_item_mst upsert (item: {data['item']}, sku: {new_sku}, screen_size: {data.get('screen_size')})")

                # # Track sku renewal (temporarily disabled)
                # if existing_sku and new_sku and existing_sku != new_sku:
                #     self.sku_updated_records.append({
                #         'account_name': 'Amazon',
                #         'item': data['item'],
                #         'product_url': data['product_url'],
                #         'old_sku': existing_sku,
                #         'new_sku': new_sku
                #     })
                #     print(f"  [INFO] SKU renewed: {existing_sku} -> {new_sku}")

            # Commit transaction
            self.db_conn.commit()

            cursor.close()

            # Re-enable autocommit
            self.db_conn.autocommit = True

            print(f"  [DB] ✓ Successfully saved to amazon_tv_detail_crawled + tv_retail_com + tv_item_mst")
            print(f"       SKU: {data.get('sku', 'no sku')}")

            return True

        except Exception as e:
            # Rollback on any error
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

            # 모든 에러를 명확하게 출력 (중복 키 포함)
            if 'duplicate key' in str(e):
                print(f"  [WARNING] Duplicate key - product already exists in DB")
                print(f"            URL: {data.get('product_url', 'N/A')[:80]}...")
            else:
                print(f"  [ERROR] Failed to save to DB: {e}")
                import traceback
                traceback.print_exc()

            return False

    def run(self, start_from=0):
        """Main execution. start_from: 0-based index to skip already-processed URLs"""
        try:
            print("="*80)
            print(f"Amazon TV Detail1 Crawler (Price Collection from Detail Pages) - Starting (Batch ID: {self.batch_id})")
            if start_from > 0:
                print(f"[INFO] Resuming from index {start_from + 1}")
            print("="*80)

            # Step 1: Connect to database
            print("\n[STEP 1/5] Connecting to database...")
            if not self.connect_db():
                print("[ERROR] Failed to connect to database. Stopping.")
                return

            # Step 2: Load XPaths
            print("\n[STEP 2/5] Loading XPath selectors...")
            if not self.load_xpaths():
                print("[ERROR] Failed to load XPath selectors. Stopping.")
                return

            # Step 3: Load product URLs
            print("\n[STEP 3/5] Loading product URLs...")
            product_urls = self.load_product_urls()
            if not product_urls:
                print("[ERROR] No product URLs found. Stopping.")
                return

            # Step 4: Setup Browser
            print("\n[STEP 4/5] Setting up Browser...")
            max_setup_retries = 3
            for setup_attempt in range(max_setup_retries):
                try:
                    self.setup_driver()
                    print("[OK] Browser ready")
                    break
                except Exception as e:
                    if setup_attempt < max_setup_retries - 1:
                        print(f"[WARNING] Browser setup failed (attempt {setup_attempt + 1}/{max_setup_retries}), retrying in 10 seconds...")
                        time.sleep(10)
                    else:
                        print(f"[ERROR] Browser setup failed after {max_setup_retries} attempts")
                        raise

            # Step 5: Scrape each detail page
            print("\n[STEP 5/5] Starting to scrape detail pages...")
            print(f"[INFO] Total pages to scrape: {len(product_urls)}")

            if start_from > 0:
                product_urls = product_urls[start_from:]
                print(f"[INFO] Skipped first {start_from} URLs, remaining: {len(product_urls)}")

            consecutive_failures = 0
            failed_urls = []  # 수집 실패 URL 목록

            # BSR 고유 URL 개수 미리 계산 (max_skus 제한에서 제외)
            bsr_only_count = sum(1 for u in product_urls if u.get('page_type') == 'bsr' and u.get('main_rank') is None)
            max_skus_logged = False

            for idx, url_data in enumerate(product_urls, start_from + 1):
                # Check if we've reached the maximum SKU limit (BSR 고유 URL은 제한 제외)
                is_bsr_only = url_data.get('page_type') == 'bsr' and url_data.get('main_rank') is None
                if self.total_collected >= self.max_skus and not is_bsr_only:
                    if not max_skus_logged:
                        print(f"[INFO] max_skus({self.max_skus}) 도달, main URL 스킵 → BSR 고유 URL {bsr_only_count}개 계속 수집")
                        max_skus_logged = True
                    if bsr_only_count > 0:
                        continue
                    print(f"\n{'='*80}")
                    print(f"[INFO] Reached maximum SKU limit ({self.max_skus})")
                    print(f"[INFO] Stopping collection. Total collected: {self.total_collected}")
                    break

                print(f"\n{'='*80}")
                print(f"Processing {idx}/{len(product_urls)}")

                success = self.scrape_detail_page(url_data)

                if success:
                    consecutive_failures = 0
                else:
                    consecutive_failures += 1
                    failed_urls.append(url_data)
                    if consecutive_failures >= self.browser_restart_after_fail:
                        print(f"\n[WARNING] {consecutive_failures} consecutive failures detected - restarting browser...")
                        try:
                            self.page.quit()
                        except Exception:
                            pass
                        time.sleep(5)
                        try:
                            self.setup_driver()
                            consecutive_failures = 0
                            print("[OK] Browser restarted successfully")
                        except Exception as e:
                            print(f"[ERROR] Browser restart failed: {e}")
                            print("[ERROR] Stopping crawler due to unrecoverable browser failure")
                            break

                # Random delay between requests
                delay = random.uniform(2, 4)
                print(f"[INFO] Waiting {delay:.1f} seconds before next request...")
                time.sleep(delay)

            # 실패 URL 재시도 (최대 2라운드)
            for retry_round in range(1, 3):
                if not failed_urls:
                    break
                print(f"\n{'='*80}")
                print(f"[RECOVERY] 실패 URL 재시도 라운드 {retry_round} - {len(failed_urls)}개")
                print(f"{'='*80}")

                print(f"[RECOVERY] 브라우저 재시작...")
                try:
                    self.page.quit()
                except:
                    pass
                time.sleep(5)
                self.setup_driver()

                still_failed = []
                for i, url_data in enumerate(failed_urls, 1):
                    print(f"\n[RECOVERY {retry_round}] {i}/{len(failed_urls)} - {url_data.get('url', '')[:60]}...")
                    success = self.scrape_detail_page(url_data)
                    if success:
                        print(f"  [RECOVERY OK] 재시도 성공")
                    else:
                        print(f"  [RECOVERY FAILED] 재시도 실패")
                        still_failed.append(url_data)
                    time.sleep(random.uniform(2, 4))

                print(f"[RECOVERY] 라운드 {retry_round} 완료: {len(failed_urls) - len(still_failed)}개 복구, {len(still_failed)}개 여전히 실패")
                failed_urls = still_failed

            print("\n" + "="*80)
            print(f"Detail Crawling completed!")
            print(f"Total collected: {self.total_collected} (max limit: {self.max_skus})")
            if failed_urls:
                print(f"[WARNING] 최종 실패 URL: {len(failed_urls)}개")
            print("="*80)

            # Send alert email
            try:
                cursor = self.db_conn.cursor()
                cursor.execute("""
                    SELECT retailer_sku_name, star_rating, count_of_star_ratings, count_of_reviews,
                           screen_size, sku_popularity, final_sku_price, original_sku_price,
                           savings, discount_type, offer, pick_up_availability,
                           shipping_availability, delivery_availability, shipping_info,
                           available_quantity_for_purchase, inventory_status, sku_status,
                           retailer_membership_discounts, detailed_review_content, summarized_review_content,
                           top_mentions, recommendation_intent, main_rank, bsr_rank, trend_rank,
                           rank_1, rank_2, promotion_position, number_of_ppl_purchased_yesterday,
                           number_of_ppl_added_to_carts, number_of_units_purchased_past_month,
                           retailer_sku_name_similar, estimated_annual_electricity_use, promotion_type, model_year
                    FROM tv_retail_com
                    WHERE account_name = 'Amazon'
                    AND crawl_datetime::timestamp >= NOW() - INTERVAL '4 hours'
                """)
                rows = cursor.fetchall()
                columns = [
                    'retailer_sku_name', 'star_rating', 'count_of_star_ratings', 'count_of_reviews',
                    'screen_size', 'sku_popularity', 'final_sku_price', 'original_sku_price',
                    'savings', 'discount_type', 'offer', 'pick_up_availability',
                    'shipping_availability', 'delivery_availability', 'shipping_info',
                    'available_quantity_for_purchase', 'inventory_status', 'sku_status',
                    'retailer_membership_discounts', 'detailed_review_content', 'summarized_review_content',
                    'top_mentions', 'recommendation_intent', 'main_rank', 'bsr_rank', 'trend_rank',
                    'rank_1', 'rank_2', 'promotion_position', 'number_of_ppl_purchased_yesterday',
                    'number_of_ppl_added_to_carts', 'number_of_units_purchased_past_month',
                    'retailer_sku_name_similar', 'estimated_annual_electricity_use', 'promotion_type', 'model_year'
                ]
                results_df = pd.DataFrame(rows, columns=columns)
                cursor.close()

                # # Send SKU renewed alert if any (temporarily disabled)
                # if self.sku_updated_records:
                #     send_sku_renewed_alert('Amazon TV', self.sku_updated_records)
            except Exception as e:
                print(f"[WARNING] Failed to send alert: {e}")

        except Exception as e:
            print(f"\n[ERROR] Crawler failed: {e}")
            import traceback
            traceback.print_exc()

        finally:
            # 결과 JSON 저장
            try:
                import json
                result_dir = r"C:\samsung_dx_retail_com\stage_results"
                os.makedirs(result_dir, exist_ok=True)
                target = len(product_urls) if 'product_urls' in locals() else 0
                with open(os.path.join(result_dir, "amazon_tv_dt1.json"), "w") as f:
                    json.dump({
                        "target_count": target,
                        "collected_count": self.total_collected
                    }, f)
            except Exception as e:
                print(f"[WARNING] Failed to write result JSON: {e}")

            print("\n[INFO] Cleaning up...")
            if self.page:
                try:
                    self.page.quit()
                    print("[OK] Browser closed")
                except:
                    pass
            if self.db_conn:
                try:
                    self.db_conn.close()
                    print("[OK] Database connection closed")
                except:
                    pass


if __name__ == "__main__":
    # Setup log file with execution start time
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

    _log_filename = datetime.now().strftime('%Y%m%d_%H%M%S') + '_dt1.txt'
    _log_filepath = os.path.join(_log_dir, _log_filename)
    _tee = Tee(_log_filepath)
    sys.stdout = _tee
    print(f"[INFO] Log file: {_log_filepath}")

    # Parse start_from argument: python amazon_tv_dt1.py 99
    _start_from = 0
    if len(sys.argv) > 1:
        try:
            _start_from = int(sys.argv[1]) - 1  # user provides 1-based, convert to 0-based
            if _start_from < 0:
                _start_from = 0
            print(f"[INFO] Start from: {_start_from + 1} (skipping first {_start_from})")
        except ValueError:
            print(f"[WARNING] Invalid start argument '{sys.argv[1]}', starting from 1")

    try:
        crawler = AmazonDetailCrawler()
        crawler.run(start_from=_start_from)
    except Exception as e:
        print(f"\n[FATAL ERROR] {e}")
        import traceback
        traceback.print_exc()

    print("\n[INFO] Crawler terminated. Exiting...")
    _tee.close()
