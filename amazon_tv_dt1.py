import time
import random
import sys
import psycopg2
import pickle
import json
import os
from datetime import datetime, timedelta
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
import pandas as pd
from alert_monitor import monitor_and_alert

class AmazonDetailCrawler:
    def __init__(self):
        self.driver = None
        self.db_conn = None
        self.xpaths = {}
        self.total_collected = 0
        self.max_skus = 300  # Maximum SKUs to collect (final limit)
        # Generate batch_id using Korea timezone
        korea_tz = pytz.timezone('Asia/Seoul')
        self.batch_id = datetime.now(korea_tz).strftime('%Y%m%d_%H%M%S')
        # Error tracking lists for alert email
        self.rv_detail_null_records = []  # count_of_reviews > 0 but detailed_review_content is null
        self.reviews_equals_ratings_records = []  # count_of_reviews == count_of_star_ratings
        self.fsp_null_records = []  # final_sku_price is null
        self.cosr_null_records = []  # count_of_star_ratings is null but star_rating exists
        self.screen_size_mismatch_records = []  # screen_size mismatch between extracted and tv_item_mst
        self.current_account = 'unsandev0004'  # Current Amazon account

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

            # Filter out already processed URLs from current session (based on main batch start time)
            print("[INFO] Checking for already processed URLs (current session)...")
            cursor = self.db_conn.cursor()

            # Use main batch_id as session start time
            if main_batch_id:
                session_start_time = datetime.strptime(main_batch_id, '%Y%m%d_%H%M%S')
                session_start_str = session_start_time.strftime('%Y-%m-%d %H:%M:%S')

                print(f"[INFO] Session start time (from main batch): {session_start_str}")

                # Get all distinct processed URLs from current session in amazon_tv_detail_crawled
                cursor.execute("""
                    SELECT DISTINCT product_url
                    FROM amazon_tv_detail_crawled
                    WHERE product_url IS NOT NULL
                      AND crawl_datetime >= %s
                """, (session_start_str,))

                already_processed_urls = {row[0] for row in cursor.fetchall()}
                print(f"[INFO] Found {len(already_processed_urls)} already processed URLs in current session")
            else:
                already_processed_urls = set()
                print(f"[WARNING] No main batch_id found, skipping duplicate check")

            cursor.close()

            # Filter out already processed URLs
            new_urls = [url_data for url_data in all_urls
                        if url_data['url'] not in already_processed_urls]

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
            try:
                WebDriverWait(self.driver, 10).until(
                    EC.presence_of_element_located((By.ID, 'productTitle'))
                )
            except:
                print("  [WARNING] Product title element not found within timeout")
                return False

            # Wait for price element (optional but important)
            if wait_for_price:
                try:
                    WebDriverWait(self.driver, 5).until(
                        EC.presence_of_element_located((By.XPATH, '//*[@id="corePriceDisplay_desktop_feature_div"] | //*[@id="corePrice_feature_div"] | //*[@id="outOfStock"]'))
                    )
                except:
                    print("  [WARNING] Price element not found within timeout - may be unavailable product")

            # Wait for star rating element (optional)
            try:
                WebDriverWait(self.driver, 3).until(
                    EC.presence_of_element_located((By.XPATH, '//*[@id="acrPopover"] | //*[@id="averageCustomerReviews"] | //span[contains(text(), "No customer reviews")]'))
                )
            except:
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
            # Use po-display.size class to find Screen Size row (most reliable)
            xpaths = [
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
            # Find model year - priority order
            xpaths = [
                # Highest priority: Technical Details - Model Year
                '//tr[.//th[contains(text(), "Model Year")]]/td[@class="a-size-base prodDetAttrValue"]',
                '//table[@id="productDetails_techSpec_section_1"]//tr[.//th[contains(text(), "Model Year")]]/td',
                '//table//tr[.//th[contains(text(), "Model Year")]]/td',
                '//*[@id="productDetails_expanderTables_depthRightSections"]//tr[.//th[contains(text(), "Model Year")]]/td',

                # Fallback: Compare table - RELEASE YEAR (when Model Year not found)
                '//div[@id="compare"]//tr[.//th//span[contains(text(), "RELEASE YEAR")]]//td[contains(@class, "page-asin")]//span[@class="ucc-v2-widget__table__col__container__attribute__value"]',
                '//div[@id="compare"]//table//tr[th//span[text()="RELEASE YEAR"]]//td[contains(@class, "page-asin")]//span',
                '//div[@id="compare"]//tr[th[contains(., "RELEASE YEAR")]]//td[contains(@class, "page-asin")]//span'
            ]

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
            xpaths = [
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
            # Try multiple XPaths for star rating
            star_rating_xpaths = [
                self.xpaths.get('star_rating'),  # DB XPath
                '//*[@id="acrPopover"]/@title',
                '//*[@id="averageCustomerReviews"]//span[@class="a-icon-alt"]',
                '//span[@data-hook="rating-out-of-text"]',
                '//*[@id="acrPopover"]/span[1]/a/span'
            ]

            # PRIORITY 1: Try to extract actual star rating first
            for xpath in star_rating_xpaths:
                if not xpath:
                    continue
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
            no_reviews_xpaths = [
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
            zero_reviews_xpaths = [
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

    def extract_count_of_star_ratings(self, tree):
        """Extract total star rating count (sum of all star ratings)
        Returns: integer (e.g., 2449) or None
        """
        try:
            # Primary: Get total count from "2,449 global ratings" in histogram section
            xpaths = [
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
                    # Fallback: return original if no price pattern found
                    return price_text.strip()

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
            xpaths = [
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
            summary = self.extract_text_safe(tree, '//*[@id="product-summary"]/p[1]/span')
            return summary if summary else None
        except Exception as e:
            return None

    def extract_detailed_reviews(self, product_url):
        """Extract detailed reviews from product detail page"""
        try:
            # Get current page HTML
            tree = html.fromstring(self.driver.page_source)

            # Extract reviews from detail page review section
            # Container: <ul id="cm-cr-dp-review-list" data-hook="top-customer-reviews-widget">
            # Each review: <li data-hook="review">
            # Review body: <span data-hook="review-body"> inner <span>
            review_xpath = '//ul[@id="cm-cr-dp-review-list"]//li[@data-hook="review"]//span[@data-hook="review-body"]//span'
            review_elements = tree.xpath(review_xpath)

            all_reviews = []
            if review_elements:
                for elem in review_elements:
                    review_text = elem.text_content().strip() if hasattr(elem, 'text_content') else str(elem).strip()
                    if review_text:
                        all_reviews.append(review_text)

            # Format as "1-review, 2-review, ..."
            if all_reviews:
                formatted_reviews = []
                for idx, review in enumerate(all_reviews, 1):
                    formatted_reviews.append(f"{idx}-{review}")
                return ", ".join(formatted_reviews)
            else:
                return None

        except Exception as e:
            print(f"  [WARNING] Failed to extract detailed reviews: {e}")
            return None

    def extract_detailed_reviews_from_review_page(self, product_url):
        """Extract up to 20 detailed reviews and count_of_reviews from review pages

        Returns:
            tuple: (detailed_review_content, count_of_reviews)
        """
        # 재시도 포함 최대 2회 시도
        for attempt in range(2):
            try:
                if attempt > 0:
                    print(f"  [INFO] Retrying review extraction (attempt {attempt + 1}/2)...")
                    # 크롬 종료 후 재시작
                    self.driver.quit()
                    self.setup_driver()
                    self.load_cookies()
                    self.driver.get(product_url)
                    time.sleep(random.uniform(4, 5))
                    # 대기 후 로드 안되면 새로고침
                    tree = html.fromstring(self.driver.page_source)
                    if not tree.xpath('//a[contains(@href, "product-reviews")]/@href'):
                        print(f"  [INFO] Page not loaded properly, refreshing...")
                        self.driver.refresh()
                        time.sleep(random.uniform(4, 5))

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
                    print("  [WARNING] Could not find review page link, falling back to detail page reviews")
                    return self.extract_detailed_reviews(product_url), None

                # Extract ASIN from review link
                asin_match = re.search(r'/product-reviews/([A-Z0-9]{10})', review_link)
                if asin_match:
                    asin = asin_match.group(1)
                    print(f"  [DEBUG] Extracted ASIN from review link: {asin}")
                else:
                    asin = None
                    print(f"  [WARNING] Could not extract ASIN from review link")

                # Use original review link (Amazon requires specific ref parameter for pagination)
                if review_link.startswith('http'):
                    review_url = review_link
                else:
                    review_url = "https://www.amazon.com" + review_link

                print(f"  [INFO] Navigating to review page: {review_url}")
                self.driver.get(review_url)
                time.sleep(random.uniform(3, 4))
                print(f"  [DEBUG] Actual URL after navigation: {self.driver.current_url}")

                # Wait for count_of_reviews element to load, with sorry page retry (max 10 times)
                count_of_reviews = None
                count_xpaths = [
                    '//*[@id="filter-info-section"]/div',
                    '//div[@data-hook="cr-filter-info-review-rating-count"]',
                    '//div[contains(@data-hook, "review-rating-count")]',
                    '//*[@id="filter-info-section"]'
                ]

                for refresh_attempt in range(10):
                    try:
                        # Sorry page 감지
                        page_source_lower = self.driver.page_source.lower()
                        if ('sorry' in page_source_lower and 'review' not in page_source_lower) or 'something went wrong' in page_source_lower:
                            print(f"  [WARNING] Sorry page detected on review page, refreshing ({refresh_attempt + 1}/10)...")
                            self.driver.refresh()
                            time.sleep(5)
                            continue

                        # 요소 대기
                        WebDriverWait(self.driver, 10).until(
                            EC.presence_of_element_located((By.XPATH, '//*[@id="filter-info-section"] | //div[@data-hook="cr-filter-info-review-rating-count"]'))
                        )

                        # 텍스트가 있을 때까지 추출 시도
                        tree = html.fromstring(self.driver.page_source)
                        for xpath in count_xpaths:
                            count_elements = tree.xpath(xpath)
                            if count_elements:
                                count_text = count_elements[0].text_content().strip() if hasattr(count_elements[0], 'text_content') else str(count_elements[0]).strip()
                                if count_text:
                                    print(f"  [DEBUG] count_text found: {count_text[:100]}...")
                                    # Try multiple patterns
                                    match = re.search(r'([\d,]+)\s*customer\s*reviews?', count_text, re.IGNORECASE)
                                    if not match:
                                        match = re.search(r'([\d,]+)\s*with\s*reviews?', count_text, re.IGNORECASE)
                                    if not match:
                                        match = re.search(r'([\d,]+)\s*reviews?', count_text, re.IGNORECASE)
                                    if not match:
                                        # Fallback: "1,234 total ratings, 567 with reviews" pattern
                                        match = re.search(r'([\d,]+)\s*total\s*ratings?,\s*([\d,]+)', count_text, re.IGNORECASE)
                                        if match:
                                            count_of_reviews = match.group(2)  # Second number is reviews
                                            print(f"  [OK] Extracted count_of_reviews from review page (fallback): {count_of_reviews}")
                                            break
                                    if match:
                                        count_of_reviews = match.group(1)
                                        print(f"  [OK] Extracted count_of_reviews from review page: {count_of_reviews}")
                                        break

                        # 추출 성공하면 루프 종료
                        if count_of_reviews:
                            break

                        # 텍스트 없으면 새로고침 후 재시도
                        print(f"  [WARNING] count_of_reviews text not found, refreshing ({refresh_attempt + 1}/10)...")
                        self.driver.refresh()
                        time.sleep(5)

                    except Exception as e:
                        print(f"  [WARNING] count_of_reviews element not loaded ({refresh_attempt + 1}/10) - {str(e)[:50]}")
                        self.driver.refresh()
                        time.sleep(5)

                # Collect reviews from first page (max 10 reviews per page)
                all_reviews = []

                # Debug: Print current URL to verify we're on review page
                current_url = self.driver.current_url
                print(f"  [DEBUG] Current URL before extracting reviews: {current_url[:100]}...")

                tree = html.fromstring(self.driver.page_source)

                # Extract reviews from first page
                review_xpath = '//span[@data-hook="review-body"]/span'
                review_elements = tree.xpath(review_xpath)

                if review_elements:
                    for elem in review_elements[:10]:  # Max 10 from first page
                        review_text = elem.text_content().strip() if hasattr(elem, 'text_content') else str(elem).strip()
                        if review_text:
                            all_reviews.append(review_text)

                print(f"  [INFO] Review page 1: collected {len(all_reviews)} reviews")

                # Check if we need to go to next page (count_of_reviews > 10)
                count_int = 0
                if count_of_reviews:
                    try:
                        count_int = int(str(count_of_reviews).replace(',', ''))
                    except:
                        count_int = 0

                # Store collected reviews for duplicate check
                collected_reviews = set(all_reviews)

                # If more reviews exist than collected, go to next pages (up to 3 pages)
                # Changed from count_int >= 20 to count_int > len(all_reviews) to handle cases like 11 reviews with only 10 collected
                current_page = 1
                max_pages = 3

                while len(all_reviews) < 20 and current_page < max_pages and count_int > len(all_reviews):
                    try:
                        # Scroll to bottom of page to ensure Next button is visible
                        self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                        time.sleep(1)

                        # Find and click Next page button
                        next_button = self.driver.find_element(By.XPATH, '//li[@class="a-last"]/a')
                        print(f"  [INFO] Clicking Next page button (page {current_page} -> {current_page + 1})...")
                        self.driver.execute_script("arguments[0].click();", next_button)
                        time.sleep(random.uniform(3, 5))

                        current_page += 1

                        # Verify we're on expected page
                        current_url = self.driver.current_url
                        if f'pageNumber={current_page}' not in current_url:
                            print(f"  [WARNING] Page {current_page} not loaded properly, current URL: {current_url[:80]}...")
                        else:
                            print(f"  [DEBUG] Confirmed on page {current_page}: {current_url[:80]}...")

                        # Extract reviews from current page
                        tree = html.fromstring(self.driver.page_source)
                        review_elements = tree.xpath(review_xpath)

                        print(f"  [DEBUG] Review page {current_page}: found {len(review_elements)} review elements")

                        # Collect reviews with duplicate check
                        page_count = 0
                        duplicates = 0
                        if review_elements:
                            for elem in review_elements[:10]:  # Max 10 per page
                                if len(all_reviews) >= 20:
                                    break
                                review_text = elem.text_content().strip() if hasattr(elem, 'text_content') else str(elem).strip()
                                if review_text:
                                    # Skip if duplicate
                                    if review_text in collected_reviews:
                                        duplicates += 1
                                        continue
                                    all_reviews.append(review_text)
                                    collected_reviews.add(review_text)
                                    page_count += 1

                        if duplicates > 0:
                            print(f"  [WARNING] Found {duplicates} duplicate reviews on page {current_page}")
                        print(f"  [INFO] Review page {current_page}: added {page_count} reviews, total {len(all_reviews)} reviews")

                        # 리뷰가 0개일 때만 Sorry page 감지 및 새로고침 (최대 10회, 5초 간격)
                        if page_count == 0:
                            sorry_page_detected = False
                            for refresh_attempt in range(10):
                                page_source_lower = self.driver.page_source.lower()
                                # Sorry page 패턴 확인 (리뷰 텍스트가 아닌 페이지 구조로 판단)
                                if ('sorry' in page_source_lower and 'review' not in page_source_lower) or 'something went wrong' in page_source_lower:
                                    print(f"  [WARNING] Sorry page detected on page {current_page}, refreshing ({refresh_attempt + 1}/10)...")
                                    self.driver.refresh()
                                    time.sleep(5)
                                    # 새로고침 후 리뷰 다시 추출 시도
                                    tree = html.fromstring(self.driver.page_source)
                                    review_elements = tree.xpath(review_xpath)
                                    if review_elements:
                                        print(f"  [INFO] Reviews found after refresh, continuing...")
                                        for elem in review_elements[:10]:
                                            if len(all_reviews) >= 20:
                                                break
                                            review_text = elem.text_content().strip() if hasattr(elem, 'text_content') else str(elem).strip()
                                            if review_text and review_text not in collected_reviews:
                                                all_reviews.append(review_text)
                                                collected_reviews.add(review_text)
                                                page_count += 1
                                        break
                                else:
                                    break
                            else:
                                print(f"  [ERROR] Sorry page persists after 10 refreshes on page {current_page}")
                                sorry_page_detected = True

                            if sorry_page_detected or page_count == 0:
                                print(f"  [INFO] No new reviews on page {current_page}, stopping pagination")
                                break

                    except Exception as e:
                        print(f"  [WARNING] Could not navigate to page {current_page + 1}: {e}")
                        break

                # Navigate back to product page
                print(f"  [INFO] Navigating back to product page...")
                self.driver.get(product_url)
                time.sleep(random.uniform(2, 3))

                # 3페이지까지 갔는데 20개 미만이면 재시도
                if count_int >= 20 and len(all_reviews) < 20 and attempt == 0:
                    print(f"  [INFO] Only collected {len(all_reviews)} reviews (expected 20), will retry...")
                    continue

                # Limit to 20 reviews and format as "1-review, 2-review, ..."
                reviews = all_reviews[:20]
                if reviews:
                    formatted_reviews = []
                    for idx, review in enumerate(reviews, 1):
                        formatted_reviews.append(f"{idx}-{review}")
                    print(f"  [OK] Collected {len(reviews)} detailed reviews from review page")
                    return ", ".join(formatted_reviews), count_of_reviews
                else:
                    print("  [WARNING] No reviews found on review page, falling back to detail page reviews")
                    return self.extract_detailed_reviews(product_url), count_of_reviews

            except Exception as e:
                print(f"  [WARNING] Failed to extract detailed reviews from review page: {e}")
                if attempt == 0:
                    continue
                return self.extract_detailed_reviews(product_url), None

        print(f"  [WARNING] No reviews extracted after retries")
        return self.extract_detailed_reviews(product_url), None

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

            # Location 1: Primary delivery message
            xpath1 = '//*[@id="mir-layout-DELIVERY_BLOCK-slot-PRIMARY_DELIVERY_MESSAGE_LARGE"]/span'
            text1 = self.extract_text_safe(tree, xpath1)
            text1 = self.clean_shipping_text(text1)
            if text1:
                shipping_parts.append(text1)

            # Location 2: Secondary delivery message
            xpath2 = '//*[@id="mir-layout-DELIVERY_BLOCK-slot-SECONDARY_DELIVERY_MESSAGE_LARGE"]/span'
            text2 = self.extract_text_safe(tree, xpath2)
            text2 = self.clean_shipping_text(text2)
            if text2:
                shipping_parts.append(text2)

            # Location 3: Holiday delivery message
            xpath3 = '//*[@id="mir-layout-DELIVERY_BLOCK-slot-HOLIDAY_DELIVERY_MESSAGE"]/b/font'
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
            xpath = '//*[@id="availability"]/span'
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

    def extract_discount_type(self, tree):
        """Extract discount type (e.g., 'Limited time deal')"""
        try:
            xpath = '//*[@id="dealBadgeSupportingText"]'
            text = self.extract_text_safe(tree, xpath)
            if text:
                return text.strip()
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

            self.driver.get(url)
            time.sleep(random.uniform(3, 5))

            # Verify page is properly loaded before extraction (with retry logic)
            page_loaded = self.verify_page_loaded(wait_for_price=True)
            skip_sku_name = False  # Flag to skip retailer_sku_name extraction

            if not page_loaded:
                # 1차 재시도: URL 재접속
                print(f"  [RETRY 1/2] Page verification failed - retrying with URL re-access...")
                time.sleep(3)
                self.driver.get(url)
                time.sleep(random.uniform(3, 5))
                page_loaded = self.verify_page_loaded(wait_for_price=True)

                if not page_loaded:
                    # 2차 재시도: 쿠키 재로드 후 URL 재접속
                    print(f"  [RETRY 2/2] Still failed - reloading cookies and retrying...")
                    self.load_cookies()
                    self.driver.get(url)
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
                item_details_button = self.driver.find_element("xpath", '//span[contains(text(), "Item details")]/ancestor::a[contains(@class, "a-expander-header")]')
                if item_details_button:
                    # Check if already expanded
                    aria_expanded = item_details_button.get_attribute("aria-expanded")
                    if aria_expanded != "true":
                        self.driver.execute_script("arguments[0].click();", item_details_button)
                        time.sleep(1)
                        print("  [INFO] Expanded 'Item details' section")
                    else:
                        print("  [INFO] 'Item details' already expanded")
            except Exception as e:
                print(f"  [WARNING] Could not find/click 'Item details': {e}")

            page_source = self.driver.page_source
            tree = html.fromstring(page_source)

            # Extract data
            if skip_sku_name:
                retailer_sku_name = None  # Skip extraction due to page load failure
                print(f"  [INFO] Skipping retailer_sku_name extraction (page load failed)")
            else:
                retailer_sku_name = self.extract_text_safe(tree, self.xpaths.get('product_name'))
            star_rating = self.extract_star_rating(tree)

            # SKU_Popularity - only collect if "Amazon's Choice"
            sku_popularity_raw = self.extract_text_safe(tree, self.xpaths.get('sku_popularity'))
            sku_popularity = sku_popularity_raw if sku_popularity_raw and "Amazon's" in sku_popularity_raw and "Choice" in sku_popularity_raw else None

            # Retailer_Membership_Discounts - clean Prime text
            membership_discount_raw = self.extract_text_safe(tree, self.xpaths.get('membership_discount'))
            membership_discount = self.clean_membership_discount(membership_discount_raw)

            # Item - Extract ASIN from final URL (after redirect)
            # sspa/click URL redirects to actual product page with /dp/ASIN/
            final_url = self.driver.current_url
            item = self.extract_asin(final_url)

            if item and len(item) == 10:
                print(f"  [OK] Extracted item from URL (ASIN): {item}")
            else:
                print(f"  [WARNING] Could not extract valid ASIN from URL: {final_url}")
                item = None

            # Ranks - try multiple approaches
            rank_1_raw = self.extract_text_safe(tree, self.xpaths.get('rank_1'))
            if not rank_1_raw:
                # Find by th text "Best Sellers Rank" - first rank
                rank_1_raw = self.extract_text_safe(tree, '//th[contains(text(), "Best Sellers Rank")]/following-sibling::td//li[1]//span[@class="a-list-item"]/span')
            if not rank_1_raw:
                # Alternative: look in Item details section
                rank_1_raw = self.extract_text_safe(tree, '//*[@id="productDetails_expanderTables_depthRightSections"]//th[contains(text(), "Best Sellers Rank")]/following-sibling::td//li[1]')
            if not rank_1_raw:
                # Old fallback XPath for different page structure
                rank_1_raw = self.extract_text_safe(tree, '//*[@id="detailBullets_feature_div"]/ul/li[7]/span/text()[1]')
            rank_1 = self.clean_rank(rank_1_raw)

            rank_2_raw = self.extract_text_safe(tree, self.xpaths.get('rank_2'))
            if not rank_2_raw:
                # Find by th text "Best Sellers Rank" - second rank
                rank_2_raw = self.extract_text_safe(tree, '//th[contains(text(), "Best Sellers Rank")]/following-sibling::td//li[2]//span[@class="a-list-item"]/span')
            if not rank_2_raw:
                # Alternative: look in Item details section
                rank_2_raw = self.extract_text_safe(tree, '//*[@id="productDetails_expanderTables_depthRightSections"]//th[contains(text(), "Best Sellers Rank")]/following-sibling::td//li[2]')
            if not rank_2_raw:
                # Old fallback XPath for different page structure
                rank_2_raw = self.extract_text_safe(tree, '//*[@id="detailBullets_feature_div"]/ul/li[7]/span/ul')
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
                wait = WebDriverWait(self.driver, 10)
                summary_element = wait.until(
                    EC.presence_of_element_located((By.XPATH, '//div[@data-testid="overall-summary"]//span[contains(@class, "__SAR2l0zNyyuZ")]'))
                )
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

            # Extract detailed review content and count_of_reviews from review page (up to 20 reviews)
            # Skip review page navigation if star_rating == "No customer reviews"
            # This avoids collecting wrong reviews from bundle products (e.g., Asurion warranty reviews)

            # First check for "0 customer reviews" pattern on detail page
            # e.g., "There are 0 customer reviews and 2 customer ratings."
            zero_reviews_detected = False
            zero_reviews_xpaths = [
                '//*[@id="reviewsMedley"]//div[@class="a-box-inner"]',
                '//*[@id="reviewsMedley"]/div/div[2]/div/div[2]/div[3]/div[2]/div/div',
                '//div[contains(text(), "customer reviews and")]'
            ]
            for xpath in zero_reviews_xpaths:
                zero_text = self.extract_text_safe(tree, xpath)
                if zero_text:
                    match = re.search(r'(\d+)\s*customer\s*reviews?', zero_text, re.IGNORECASE)
                    if match and match.group(1) == '0':
                        print(f"  [INFO] Detected '0 customer reviews' on detail page: {zero_text[:80]}...")
                        zero_reviews_detected = True
                        break

            if star_rating == "No customer reviews" or zero_reviews_detected:
                print(f"  [INFO] Skipping review page - No customer reviews (count_of_reviews=0)")
                detailed_review_content = None
                count_of_reviews = "0"
            else:
                detailed_review_content, count_of_reviews = self.extract_detailed_reviews_from_review_page(url)

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
                'number_of_units_purchased_past_month': url_data.get('number_of_units_purchased_past_month'),  # From main_crawled
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
                self.driver.get(url)

                # Wait for all key elements to load with extended timeout
                print(f"  [INFO] Waiting for product title element...")
                try:
                    WebDriverWait(self.driver, 30).until(
                        EC.presence_of_element_located((By.ID, 'productTitle'))
                    )
                    print(f"  [OK] Product title element loaded")
                except:
                    print(f"  [WARNING] Product title element not loaded within 30s")

                print(f"  [INFO] Waiting for star rating element...")
                try:
                    WebDriverWait(self.driver, 30).until(
                        EC.presence_of_element_located((By.XPATH, '//*[@id="acrPopover"] | //*[@id="averageCustomerReviews"]'))
                    )
                    print(f"  [OK] Star rating element loaded")
                except:
                    print(f"  [WARNING] Star rating element not loaded within 30s")

                print(f"  [INFO] Waiting for count of star ratings element...")
                try:
                    WebDriverWait(self.driver, 30).until(
                        EC.presence_of_element_located((By.ID, 'acrCustomerReviewText'))
                    )
                    print(f"  [OK] Count of star ratings element loaded")
                except:
                    print(f"  [WARNING] Count of star ratings element not loaded within 30s")

                print(f"  [INFO] Waiting for price element...")
                try:
                    WebDriverWait(self.driver, 30).until(
                        EC.presence_of_element_located((By.XPATH, '//*[@id="corePriceDisplay_desktop_feature_div"] | //*[@id="corePrice_feature_div"]'))
                    )
                    print(f"  [OK] Price element loaded")
                except:
                    print(f"  [WARNING] Price element not loaded within 30s")

                print(f"  [INFO] All element waits complete, re-extracting data...")

                # Re-parse page
                page_source = self.driver.page_source
                tree = html.fromstring(page_source)

                # Re-extract key fields
                retailer_sku_name = self.extract_text_safe(tree, self.xpaths.get('product_name'))
                star_rating = self.extract_star_rating(tree)
                count_of_star_ratings = self.extract_count_of_star_ratings(tree)
                final_sku_price = self.extract_final_sku_price(tree)

                # Re-extract count_of_reviews from review page (only if has reviews)
                # First check for "0 customer reviews" pattern on detail page
                zero_reviews_detected = False
                zero_reviews_xpaths = [
                    '//*[@id="reviewsMedley"]//div[@class="a-box-inner"]',
                    '//*[@id="reviewsMedley"]/div/div[2]/div/div[2]/div[3]/div[2]/div/div',
                    '//div[contains(text(), "customer reviews and")]'
                ]
                for xpath in zero_reviews_xpaths:
                    zero_text = self.extract_text_safe(tree, xpath)
                    if zero_text:
                        match = re.search(r'(\d+)\s*customer\s*reviews?', zero_text, re.IGNORECASE)
                        if match and match.group(1) == '0':
                            print(f"  [INFO] Retry: Detected '0 customer reviews' on detail page")
                            zero_reviews_detected = True
                            break

                if star_rating == "No customer reviews" or zero_reviews_detected:
                    detailed_review_content = None
                    count_of_reviews = "0"
                else:
                    detailed_review_content, count_of_reviews = self.extract_detailed_reviews_from_review_page(url)

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
                    has_cosr_null = cosr_is_null  # count_of_star_ratings is null
                    has_fsp_null = fsp_is_null  # final_sku_price is null
                    has_rv_detail_null = cor_int > 0 and drc_is_null  # reviews > 0 but content null
                    has_src_null = cor_int > 0 and src_is_null  # reviews > 0 but summarized review null

                    # 수집된 리뷰 개수 계산 (count가 수집된 개수보다 크면 재시도, 최대 20개까지)
                    collected_review_count = 0
                    if drc and isinstance(drc, str):
                        collected_review_count = len([r for r in drc.split(', ') if r and '-' in r])
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
                    """Check if any error condition exists"""
                    return (errors['has_sr_null'] or errors['has_cor_null'] or
                            errors['has_cosr_null'] or errors['has_fsp_null'] or
                            errors['has_rv_detail_null'] or errors['has_rv_insufficient'] or
                            errors['has_src_null'])

                def re_extract_summarized_review():
                    """Re-extract summarized review content with WebDriverWait"""
                    try:
                        wait = WebDriverWait(self.driver, 10)
                        summary_element = wait.until(
                            EC.presence_of_element_located((By.XPATH, '//div[@data-testid="overall-summary"]//span[contains(@class, "__SAR2l0zNyyuZ")]'))
                        )
                        return summary_element.text.strip() if summary_element.text else None
                    except:
                        return None

                def re_extract_fields(errors, url, data):
                    """Re-extract fields based on error type"""
                    tree = html.fromstring(self.driver.page_source)

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

                    if errors['has_cor_null'] or errors['has_rv_detail_null'] or errors['has_rv_insufficient']:
                        # Check for "0 customer reviews" pattern
                        sr = data.get('Star_Rating')
                        zero_reviews_detected = False
                        zero_reviews_xpaths = [
                            '//*[@id="reviewsMedley"]//div[@class="a-box-inner"]',
                            '//*[@id="reviewsMedley"]/div/div[2]/div/div[2]/div[3]/div[2]/div/div',
                            '//div[contains(text(), "customer reviews and")]'
                        ]
                        for xpath in zero_reviews_xpaths:
                            zero_text = self.extract_text_safe(tree, xpath)
                            if zero_text:
                                match = re.search(r'(\d+)\s*customer\s*reviews?', zero_text, re.IGNORECASE)
                                if match and match.group(1) == '0':
                                    zero_reviews_detected = True
                                    break

                        if sr == "No customer reviews" or zero_reviews_detected:
                            data['Detailed_Review_Content'] = None
                            data['count_of_reviews'] = "0"
                            print(f"    - count_of_reviews: 0 (no customer reviews)")
                        else:
                            if errors['has_rv_insufficient']:
                                print(f"    - rv_insufficient: count={errors['cor_int']}, collected={errors['collected_review_count']}, retrying...")
                            drc, cor = self.extract_detailed_reviews_from_review_page(url)
                            data['Detailed_Review_Content'] = drc
                            data['count_of_reviews'] = cor
                            # 재수집 후 리뷰 개수 계산
                            new_collected = len([r for r in drc.split(', ') if r and '-' in r]) if drc else 0
                            print(f"    - count_of_reviews: {cor}, detailed_review: {new_collected} collected")

                # Initial error check
                errors = check_error_conditions(data)

                # Retry up to 3 times if any error detected
                if has_any_error(errors):
                    for retry_num in range(1, 4):
                        print(f"  [RETRY {retry_num}/3] Errors: sr={errors['has_sr_null']}, cor={errors['has_cor_null']}, cosr={errors['has_cosr_null']}, fsp={errors['has_fsp_null']}, rv={errors['has_rv_detail_null']}, rv_insuf={errors['has_rv_insufficient']}, src={errors['has_src_null']}")

                        if retry_num == 1:
                            # 1st retry: Refresh page
                            print(f"  [RETRY 1/3] Refreshing page...")
                            self.driver.refresh()
                            time.sleep(3)
                        elif retry_num == 2:
                            # 2nd retry: Reload cookies and re-access URL
                            print(f"  [RETRY 2/3] Reloading cookies and re-accessing URL...")
                            self.load_cookies()
                            self.driver.get(url)
                            time.sleep(random.uniform(3, 5))
                        else:
                            # 3rd retry: Re-access URL with extended wait
                            print(f"  [RETRY 3/3] Re-accessing URL with extended wait...")
                            self.driver.get(url)
                            time.sleep(5)
                            # Wait for key elements
                            try:
                                WebDriverWait(self.driver, 15).until(
                                    EC.presence_of_element_located((By.ID, 'productTitle'))
                                )
                            except:
                                pass

                        # Re-extract fields
                        print(f"  [RETRY {retry_num}/3] Re-extracting fields...")
                        re_extract_fields(errors, url, data)

                        # Check if errors are resolved
                        errors = check_error_conditions(data)
                        if not has_any_error(errors):
                            print(f"  [OK] All errors resolved after retry {retry_num}")
                            break
                    else:
                        print(f"  [WARNING] Some errors persist after 3 retries")

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
                print(f"       Rank1: {rank_1 or 'N/A'} | Rank2: {rank_2 or 'N/A'}")
                print(f"       Main Rank: {data['main_rank'] or 'N/A'} | BSR Rank: {data['bsr_rank'] or 'N/A'}")
                print(f"       Screen Size: {screen_size or 'N/A'} | Reviews Count: {count_of_reviews or 'N/A'}")
                print(f"       Star Counts: {count_of_star_ratings or 'N/A'}")
                print(f"       Review Summary: {summarized_review_content[:80] + '...' if summarized_review_content and len(summarized_review_content) > 80 else summarized_review_content or 'N/A'}")

                # Show detailed review count (use data dict for retry-updated value)
                final_drc = data.get('Detailed_Review_Content')
                if final_drc:
                    try:
                        # Count reviews by counting "N-" patterns
                        review_count = len([r for r in final_drc.split(', ') if r and '-' in r])
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
            print(f"       Product: {data.get('Retailer_SKU_Name', 'N/A')[:60]}...")
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

            # Insert into tv_item_mst (with duplicate check on item, update screen_size on conflict)
            if data.get('item'):
                cursor.execute("""
                    INSERT INTO tv_item_mst (item, product_url, sku, account_name, screen_size)
                    VALUES (%s, %s, %s, %s, %s)
                    ON CONFLICT (item) DO UPDATE SET
                        screen_size = COALESCE(tv_item_mst.screen_size, EXCLUDED.screen_size)
                """, (
                    data['item'],
                    data['product_url'],
                    data.get('sku', 'no sku'),
                    'Amazon',
                    data.get('screen_size')
                ))
                print(f"  [DB] ✓ tv_item_mst upsert (item: {data['item']}, sku: {data.get('sku', 'no sku')}, screen_size: {data.get('screen_size')})")

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

    def run(self):
        """Main execution"""
        try:
            print("="*80)
            print(f"Amazon TV Detail1 Crawler (Price Collection from Detail Pages) - Starting (Batch ID: {self.batch_id})")
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

            # Step 4: Setup WebDriver
            print("\n[STEP 4/5] Setting up WebDriver...")
            self.setup_driver()
            print("[OK] WebDriver ready")

            # Step 5: Scrape each detail page
            print("\n[STEP 5/5] Starting to scrape detail pages...")
            print(f"[INFO] Total pages to scrape: {len(product_urls)}")

            for idx, url_data in enumerate(product_urls, 1):
                # Check if we've reached the maximum SKU limit
                if self.total_collected >= self.max_skus:
                    print(f"\n{'='*80}")
                    print(f"[INFO] Reached maximum SKU limit ({self.max_skus})")
                    print(f"[INFO] Stopping collection. Total collected: {self.total_collected}")
                    break

                print(f"\n{'='*80}")
                print(f"Processing {idx}/{len(product_urls)}")

                self.scrape_detail_page(url_data)

                # Random delay between requests
                delay = random.uniform(2, 4)
                print(f"[INFO] Waiting {delay:.1f} seconds before next request...")
                time.sleep(delay)

            print("\n" + "="*80)
            print(f"Detail Crawling completed!")
            print(f"Total collected: {self.total_collected} (max limit: {self.max_skus})")
            print(f"URLs processed: {min(idx, len(product_urls))}/{len(product_urls)}")
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

                monitor_and_alert('amazon', len(product_urls), results_df,
                                 rv_detail_null_records=self.rv_detail_null_records,
                                 reviews_equals_ratings_records=self.reviews_equals_ratings_records,
                                 fsp_null_records=self.fsp_null_records,
                                 cosr_null_records=self.cosr_null_records,
                                 screen_size_mismatch_records=self.screen_size_mismatch_records)
            except Exception as e:
                print(f"[WARNING] Failed to send alert: {e}")

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
    try:
        crawler = AmazonDetailCrawler()
        crawler.run()
    except Exception as e:
        print(f"\n[FATAL ERROR] {e}")
        import traceback
        traceback.print_exc()

    print("\n[INFO] Crawler terminated. Exiting...")
