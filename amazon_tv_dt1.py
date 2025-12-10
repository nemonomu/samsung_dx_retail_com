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

# Cookie file path
COOKIE_FILE = 'amazon_cookies.pkl'

# Import database configuration
from config import DB_CONFIG
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
        """Extract count of reviews (format: '1,484' or '0' for no reviews)"""
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
                    if review_text and len(review_text) > 10:
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
                print("  [WARNING] Could not find review page link, falling back to detail page reviews")
                return self.extract_detailed_reviews(product_url), None

            # Navigate to review page
            if review_link.startswith('http'):
                review_url = review_link
            else:
                review_url = "https://www.amazon.com" + review_link

            print(f"  [INFO] Navigating to review page: {review_url[:80]}...")
            self.driver.get(review_url)
            time.sleep(random.uniform(3, 4))

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

            # Collect reviews from first page (max 10 reviews per page)
            all_reviews = []
            tree = html.fromstring(self.driver.page_source)

            # Extract reviews from first page
            review_xpath = '//span[@data-hook="review-body"]/span'
            review_elements = tree.xpath(review_xpath)

            if review_elements:
                for elem in review_elements[:10]:  # Max 10 from first page
                    review_text = elem.text_content().strip() if hasattr(elem, 'text_content') else str(elem).strip()
                    if review_text and len(review_text) > 10:
                        all_reviews.append(review_text)

            print(f"  [INFO] Review page 1: collected {len(all_reviews)} reviews")

            # Check if we need to go to next page (count_of_reviews > 10)
            count_int = 0
            if count_of_reviews:
                try:
                    count_int = int(str(count_of_reviews).replace(',', ''))
                except:
                    count_int = 0

            # If more than 10 reviews exist, go to next page for more
            if count_int > 10 and len(all_reviews) > 0:
                # Find next page link - multiple approaches
                next_button_xpaths = [
                    '//*[@id="cm_cr-pagination_bar"]/ul/li[2]/a/@href',  # Exact structure from Amazon
                    '//a[contains(text(), "Next page")]/@href',  # Text-based search
                    '//li[contains(@class, "a-last")]/a/@href',
                    '//*[@id="cm_cr-pagination_bar"]//li[contains(@class, "a-last")]/a/@href',
                    '//ul[@class="a-pagination"]//li[@class="a-last"]/a/@href'
                ]

                next_link = None
                for idx, xpath in enumerate(next_button_xpaths, 1):
                    result = tree.xpath(xpath)
                    if result:
                        next_link = result[0]
                        print(f"  [DEBUG] Found next page link with XPath #{idx}: {next_link[:80]}...")
                        break
                    else:
                        print(f"  [DEBUG] Next page XPath #{idx} not found")

                if next_link:
                    # Verify the link contains pageNumber=2
                    if 'pageNumber=' not in next_link:
                        print(f"  [WARNING] Next link doesn't contain pageNumber, skipping")
                    else:
                        if next_link.startswith('http'):
                            next_url = next_link
                        else:
                            next_url = "https://www.amazon.com" + next_link

                        print(f"  [INFO] Navigating to review page 2: {next_url[:100]}...")
                        self.driver.get(next_url)
                        time.sleep(random.uniform(3, 5))  # Increased wait time for page load

                        # Verify we're on page 2
                        current_url = self.driver.current_url
                        if 'pageNumber=2' not in current_url:
                            print(f"  [WARNING] Page 2 not loaded properly, current URL: {current_url[:80]}...")
                        else:
                            print(f"  [DEBUG] Confirmed on page 2: {current_url[:80]}...")

                        # Extract reviews from second page
                        tree = html.fromstring(self.driver.page_source)
                        review_elements = tree.xpath(review_xpath)

                        print(f"  [DEBUG] Review page 2: found {len(review_elements)} review elements")

                        # Store first page reviews for duplicate check
                        first_page_reviews = set(all_reviews)

                        # Collect reviews from second page with duplicate check
                        page2_count = 0
                        duplicates = 0
                        if review_elements:
                            for elem in review_elements[:10]:  # Max 10 from second page
                                if len(all_reviews) >= 20:
                                    break
                                review_text = elem.text_content().strip() if hasattr(elem, 'text_content') else str(elem).strip()
                                if review_text and len(review_text) > 10:
                                    # Skip if duplicate from first page
                                    if review_text in first_page_reviews:
                                        duplicates += 1
                                        continue
                                    all_reviews.append(review_text)
                                    page2_count += 1

                        if duplicates > 0:
                            print(f"  [WARNING] Found {duplicates} duplicate reviews on page 2")
                        print(f"  [INFO] Review page 2: added {page2_count} reviews, total {len(all_reviews)} reviews")
                else:
                    print(f"  [WARNING] Could not find next page link")

            # Navigate back to product page
            print(f"  [INFO] Navigating back to product page...")
            self.driver.get(product_url)
            time.sleep(random.uniform(2, 3))

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
            return self.extract_detailed_reviews(product_url), None

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

            print(f"  [INFO] Page loaded, extracting data...")

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

            # Extract screen_size (NEW) - pass retailer_sku_name as fallback
            screen_size = self.extract_screen_size(tree, retailer_sku_name)

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

            # Extract detailed review content and count_of_reviews from review page (up to 20 reviews)
            detailed_review_content, count_of_reviews = self.extract_detailed_reviews_from_review_page(url)

            # Re-parse page source after returning from review page
            tree = html.fromstring(self.driver.page_source)

            # Fallback: If count_of_reviews not found from review page, try detail page
            if not count_of_reviews:
                count_of_reviews = self.extract_count_of_reviews(tree)

            # Extract prices from detail page
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
            savings = self.calculate_savings(final_sku_price, original_sku_price)

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
                'savings': savings  # Calculated from prices
            }

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

                # Show detailed review count
                if detailed_review_content:
                    try:
                        # Count reviews by counting "N-" patterns
                        review_count = len([r for r in detailed_review_content.split(', ') if r and '-' in r])
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

            # If "No customer reviews", set count_of_star_ratings to 0
            if data['Star_Rating'] == "No customer reviews":
                data['Count_of_Star_Ratings'] = 0

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
                None,  # discount_type (Amazon doesn't have this in detail)
                None,  # offer (Amazon doesn't have this)
                None,  # pick_up_availability (Amazon doesn't have this)
                None,  # shipping_availability (Amazon doesn't have this)
                None,  # delivery_availability (Amazon doesn't have this)
                None,  # shipping_info (Amazon doesn't have this in detail)
                None,  # available_quantity_for_purchase (Amazon doesn't have this in detail)
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

            # Insert into tv_item_mst (with duplicate check on item)
            if data.get('item'):
                cursor.execute("""
                    INSERT INTO tv_item_mst (item, product_url, sku, account_name)
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT (item) DO NOTHING
                """, (
                    data['item'],
                    data['product_url'],
                    data.get('sku', 'no sku'),
                    'Amazon'
                ))
                print(f"  [DB] ✓ tv_item_mst insert attempted (item: {data['item']}, sku: {data.get('sku', 'no sku')})")

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
                    SELECT final_sku_price, count_of_reviews, count_of_star_ratings,
                           star_rating, retailer_sku_name, screen_size
                    FROM tv_retail_com
                    WHERE account_name = 'Amazon'
                    AND crawl_datetime >= NOW() - INTERVAL '1 day'
                """)
                rows = cursor.fetchall()
                columns = ['final_sku_price', 'count_of_reviews', 'count_of_star_ratings',
                          'star_rating', 'retailer_sku_name', 'screen_size']
                results_df = pd.DataFrame(rows, columns=columns)
                cursor.close()

                monitor_and_alert('amazon', len(product_urls), results_df)
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
