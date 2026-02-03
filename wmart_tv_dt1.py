"""
Walmart TV Detail Page Crawler (Part 1)
Collects detailed product information from URLs stored in:
- wmart_tv_main_1 (mother='main', pages 1-5)
- wmart_tv_main_2 (mother='main', pages 6-10)
- wmart_tv_bsr_crawl (mother='bsr')
"""
import time
import random
import sys
import os
import psycopg2
from datetime import datetime, timedelta
from DrissionPage import ChromiumPage, ChromiumOptions
from lxml import html
import re


class Tee:
    """stdout을 콘솔과 파일 둘 다에 출력"""
    def __init__(self, filename):
        self.terminal = sys.stdout
        self.log = open(filename, 'w', encoding='utf-8')

    def write(self, message):
        self.terminal.write(message)
        self.log.write(message)
        self.log.flush()

    def flush(self):
        self.terminal.flush()
        self.log.flush()

    def close(self):
        self.log.close()

# Import database configuration
from config import DB_CONFIG
from wmart_config_loader import get_wmart_config
import pandas as pd
from alert_monitor import monitor_and_alert

class WalmartDetailCrawler:
    def __init__(self):
        self.page = None
        self.db_conn = None
        self.xpaths = {}
        self.total_collected = 0
        # Load config from DB
        self.config = get_wmart_config()
        self.max_skus = self.config.get_constant_int('max_skus', 'wmart_tv_dt1', default=300)
        # Error tracking for alert email
        self.drv_20_error_records = []  # count_of_reviews <= 20 but collected fewer reviews
        self.screen_size_mismatch_records = []  # screen_size mismatch between extracted and tv_item_mst

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
            cursor = self.db_conn.cursor()
            cursor.execute("""
                SELECT data_field, xpath
                FROM xpath_selectors
                WHERE mall_name = 'Walmart' AND page_type = 'detail_page' AND is_active = TRUE
            """)

            for row in cursor.fetchall():
                self.xpaths[row[0]] = row[1]

            cursor.close()
            print(f"[OK] Loaded {len(self.xpaths)} XPath selectors")
            return True

        except Exception as e:
            print(f"[ERROR] Failed to load XPaths: {e}")
            return False

    def load_product_urls(self):
        """Load product URLs from wmart_tv_main_1, wmart_tv_main_2, and wmart_tv_bsr_crawl tables (latest batch only)
        Merge rank information for duplicate URLs"""
        try:
            cursor = self.db_conn.cursor()

            # Get latest batch_id from wmart_tv_main_1
            cursor.execute("""
                SELECT batch_id
                FROM wmart_tv_main_1
                WHERE batch_id IS NOT NULL
                ORDER BY batch_id DESC
                LIMIT 1
            """)
            main1_batch_result = cursor.fetchone()
            main1_batch_id = main1_batch_result[0] if main1_batch_result else None

            # Get latest batch_id from wmart_tv_main_2
            cursor.execute("""
                SELECT batch_id
                FROM wmart_tv_main_2
                WHERE batch_id IS NOT NULL
                ORDER BY batch_id DESC
                LIMIT 1
            """)
            main2_batch_result = cursor.fetchone()
            main2_batch_id = main2_batch_result[0] if main2_batch_result else None

            # Get latest batch_id from wmart_tv_bsr_crawl
            cursor.execute("""
                SELECT batch_id
                FROM wmart_tv_bsr_crawl
                WHERE batch_id IS NOT NULL
                ORDER BY batch_id DESC
                LIMIT 1
            """)
            bsr_batch_result = cursor.fetchone()
            bsr_batch_id = bsr_batch_result[0] if bsr_batch_result else None

            print(f"[INFO] Latest batch_id - Main1: {main1_batch_id}, Main2: {main2_batch_id}, BSR: {bsr_batch_id}")

            # Dictionary to store merged URL data: {url: {page_type, main_rank, bsr_rank, ...}}
            url_data_map = {}

            # Load from wmart_tv_main_1 (main part 1) - latest batch only
            if main1_batch_id:
                cursor.execute("""
                    SELECT main_rank, Product_url,
                           Pick_Up_Availability, Shipping_Availability, Delivery_Availability,
                           SKU_Status, Retailer_Membership_Discounts, Available_Quantity_for_Purchase,
                           Inventory_Status
                    FROM wmart_tv_main_1
                    WHERE batch_id = %s
                      AND Product_url IS NOT NULL
                      AND Product_url != ''
                    ORDER BY main_rank
                """, (main1_batch_id,))
                main1_rows = cursor.fetchall()
                for row in main1_rows:
                    url = row[1]
                    if url not in url_data_map:
                        url_data_map[url] = {
                            'page_type': 'main',
                            'url': url,
                            'main_rank': row[0],
                            'bsr_rank': None,
                            'pick_up_availability': row[2],
                            'shipping_availability': row[3],
                            'delivery_availability': row[4],
                            'sku_status': row[5],
                            'retailer_membership_discounts': row[6],
                            'available_quantity_for_purchase': row[7],
                            'inventory_status': row[8]
                        }
                print(f"[OK] Loaded {len(main1_rows)} URLs from wmart_tv_main_1")

            # Load from wmart_tv_main_2 (main part 2) - latest batch only
            if main2_batch_id:
                cursor.execute("""
                    SELECT main_rank, Product_url,
                           Pick_Up_Availability, Shipping_Availability, Delivery_Availability,
                           SKU_Status, Retailer_Membership_Discounts, Available_Quantity_for_Purchase,
                           Inventory_Status
                    FROM wmart_tv_main_2
                    WHERE batch_id = %s
                      AND Product_url IS NOT NULL
                      AND Product_url != ''
                    ORDER BY main_rank
                """, (main2_batch_id,))
                main2_rows = cursor.fetchall()
                for row in main2_rows:
                    url = row[1]
                    if url not in url_data_map:
                        url_data_map[url] = {
                            'page_type': 'main',
                            'url': url,
                            'main_rank': row[0],
                            'bsr_rank': None,
                            'pick_up_availability': row[2],
                            'shipping_availability': row[3],
                            'delivery_availability': row[4],
                            'sku_status': row[5],
                            'retailer_membership_discounts': row[6],
                            'available_quantity_for_purchase': row[7],
                            'inventory_status': row[8]
                        }
                print(f"[OK] Loaded {len(main2_rows)} URLs from wmart_tv_main_2")

            # Load from wmart_tv_bsr_crawl (bsr) - latest batch only
            if bsr_batch_id:
                cursor.execute("""
                    SELECT bsr_rank, Product_url,
                           Pick_Up_Availability, Shipping_Availability, Delivery_Availability,
                           SKU_Status, Retailer_Membership_Discounts, Available_Quantity_for_Purchase,
                           Inventory_Status
                    FROM wmart_tv_bsr_crawl
                    WHERE batch_id = %s
                      AND Product_url IS NOT NULL
                      AND Product_url != ''
                    ORDER BY bsr_rank
                """, (bsr_batch_id,))
                bsr_rows = cursor.fetchall()
                for row in bsr_rows:
                    url = row[1]
                    if url in url_data_map:
                        # URL already exists in main - just add bsr_rank
                        url_data_map[url]['bsr_rank'] = row[0]
                    else:
                        # New URL from bsr
                        url_data_map[url] = {
                            'page_type': 'bsr',
                            'url': url,
                            'main_rank': None,
                            'bsr_rank': row[0],
                            'pick_up_availability': row[2],
                            'shipping_availability': row[3],
                            'delivery_availability': row[4],
                            'sku_status': row[5],
                            'retailer_membership_discounts': row[6],
                            'available_quantity_for_purchase': row[7],
                            'inventory_status': row[8]
                        }
                print(f"[OK] Loaded {len(bsr_rows)} BSR URLs")

            cursor.close()

            # Convert dictionary to list (maintains insertion order: main first, then bsr)
            all_urls = list(url_data_map.values())

            # Count duplicates from source tables
            total_loaded = 0
            if main1_batch_id:
                cursor = self.db_conn.cursor()
                cursor.execute("SELECT COUNT(*) FROM wmart_tv_main_1 WHERE batch_id = %s", (main1_batch_id,))
                total_loaded += cursor.fetchone()[0]
                cursor.close()
            if main2_batch_id:
                cursor = self.db_conn.cursor()
                cursor.execute("SELECT COUNT(*) FROM wmart_tv_main_2 WHERE batch_id = %s", (main2_batch_id,))
                total_loaded += cursor.fetchone()[0]
                cursor.close()
            if bsr_batch_id:
                cursor = self.db_conn.cursor()
                cursor.execute("SELECT COUNT(*) FROM wmart_tv_bsr_crawl WHERE batch_id = %s", (bsr_batch_id,))
                total_loaded += cursor.fetchone()[0]
                cursor.close()

            duplicates = total_loaded - len(all_urls)
            if duplicates > 0:
                print(f"[INFO] Found {duplicates} duplicate URLs - rank information merged")

            print(f"[OK] Total unique URLs from main1/main2/bsr: {len(all_urls)}")

            # Filter out already processed URLs from current session (based on main1 batch start time)
            print("[INFO] Checking for already processed URLs (current session)...")
            cursor = self.db_conn.cursor()

            # Use main1 batch_id as session start time (main1 is always first)
            if main1_batch_id:
                session_start_time = datetime.strptime(main1_batch_id, '%Y%m%d_%H%M%S')
                session_start_str = session_start_time.strftime('%Y-%m-%d %H:%M:%S')

                print(f"[INFO] Session start time (from main1 batch): {session_start_str}")

                # Get all distinct processed URLs from current session in Walmart_tv_detail_crawled
                cursor.execute("""
                    SELECT DISTINCT product_url
                    FROM Walmart_tv_detail_crawled
                    WHERE product_url IS NOT NULL
                      AND crawl_datetime >= %s
                """, (session_start_str,))

                already_processed_urls = {row[0] for row in cursor.fetchall()}
                print(f"[INFO] Found {len(already_processed_urls)} already processed URLs in current session")
            else:
                already_processed_urls = set()
                print(f"[WARNING] No main1 batch_id found, skipping duplicate check")

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
                    print("[ERROR] No URLs found!")

            return new_urls

        except Exception as e:
            print(f"[ERROR] Failed to load product URLs: {e}")
            import traceback
            traceback.print_exc()
            return []

    def setup_driver(self):
        """Setup DrissionPage browser with image loading disabled"""
        try:
            co = ChromiumOptions()
            co.no_imgs(True)  # Disable image loading for faster page load
            self.page = ChromiumPage(co)
            print("[OK] Browser setup complete (DrissionPage, images disabled)")
            return True
        except Exception as e:
            print(f"[ERROR] Browser setup failed: {e}")
            import traceback
            traceback.print_exc()
            return False

    def check_robot_page(self, page_source):
        """Check if page is showing 'Robot or human?' challenge"""
        robot_texts = self.config.get_robot_check_texts()
        for text in robot_texts:
            if text in page_source:
                return True
        return False

    def handle_captcha(self):
        """Handle 'PRESS & HOLD' CAPTCHA if present"""
        try:
            print("[INFO] Checking for CAPTCHA...")
            page_content = self.page.html.lower()
            captcha_keywords = self.config.get_captcha_keywords()
            if any(keyword in page_content for keyword in captcha_keywords):
                print("[WARNING] CAPTCHA keywords found in page")
                captcha_wait = self.config.get_float('timing', 'captcha_wait', default=60)
                print(f"[INFO] CAPTCHA detection - waiting {int(captcha_wait)} seconds for manual intervention...")
                time.sleep(captcha_wait)
                print("[INFO] Continuing after wait...")
                return True
            else:
                print("[INFO] No CAPTCHA detected")
                return True
        except Exception as e:
            print(f"[WARNING] CAPTCHA check failed: {e}")
            return True

    def initialize_session(self):
        """Initialize session with natural browsing pattern to avoid bot detection"""
        try:
            print("[INFO] Initializing session - navigating to Walmart homepage...")

            # Get timing and scroll config values
            homepage_wait = self.config.get_timing_range('homepage_wait') or (8, 12)
            browse_wait = self.config.get_timing_range('browse_wait') or (10, 15)
            scroll_wait_range = self.config.get_timing_range('scroll_wait') or (1, 2)
            robot_recovery_wait = self.config.get_float('timing', 'robot_recovery_wait', default=30)
            random_scroll_range = self.config.get_scroll_range('random_amount') or (150, 300)
            recovery_scroll_range = self.config.get_scroll_range('recovery_amount') or (200, 500)
            homepage_url = self.config.get_url('homepage') or "https://www.walmart.com"

            self.page.get(homepage_url)
            time.sleep(random.uniform(*homepage_wait))

            # Check for robot detection
            if self.check_robot_page(self.page.html):
                print("[WARNING] Robot detection on homepage. Handling CAPTCHA...")
                self.handle_captcha()
                captcha_after_wait = self.config.get_timing_range('captcha_after_wait') or (3, 5)
                time.sleep(random.uniform(*captcha_after_wait))

                if self.check_robot_page(self.page.html):
                    print("[WARNING] Still showing robot detection, trying recovery...")
                    # Slow scroll down
                    scroll_between_wait = self.config.get_timing_range('scroll_between_wait') or (1.5, 2.5)
                    for i in range(5):
                        scroll_amount = random.randint(*random_scroll_range)
                        self.page.run_js(f"window.scrollBy(0, {scroll_amount})")
                        time.sleep(random.uniform(*scroll_between_wait))
                    self.page.run_js("window.scrollBy(0, -200)")
                    time.sleep(random.uniform(*scroll_wait_range))

                print(f"[INFO] Waiting {int(robot_recovery_wait)} seconds...")
                time.sleep(robot_recovery_wait)

                print("[INFO] Reloading page...")
                self.page.refresh()
                time.sleep(random.uniform(*browse_wait))

                if self.check_robot_page(self.page.html):
                    print("[ERROR] Still getting robot detection after recovery")
                    print("[INFO] Attempting to continue anyway...")

            # Simple homepage exploration
            print("[INFO] Exploring homepage...")
            time.sleep(random.uniform(*scroll_wait_range) * 2)

            # Random scrolling
            for _ in range(random.randint(2, 4)):
                scroll_amount = random.randint(*recovery_scroll_range)
                self.page.run_js(f"window.scrollBy(0, {scroll_amount})")
                time.sleep(random.uniform(*scroll_wait_range))

            # Scroll back to top
            self.page.run_js("window.scrollTo(0, 0)")
            time.sleep(random.uniform(*scroll_wait_range) + 1)

            print("[OK] Session initialized")
            return True

        except Exception as e:
            print(f"[ERROR] Failed to initialize session: {e}")
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

    def extract_star_rating(self, tree, page_source=None):
        """Extract star rating number from '4.4 out of 5' format or 'No ratings yet'

        Priority:
        1. JSON data: roundedAverageOverallRating (rounded value like 4.4)
        2. XPath fallback (text parsing)

        Note: averageOverallRating may contain long decimals like 4.534883720930233
              which is invalid, so we use roundedAverageOverallRating instead.
        """
        try:
            # Method 1: Extract from JSON data (use rounded value)
            if page_source:
                # Try roundedAverageOverallRating first (clean value like 4.4)
                match = re.search(r'"roundedAverageOverallRating":([\d.]+)', page_source)
                if match:
                    rating = match.group(1)
                    # Validate: should be 1 decimal place max (e.g., 4.4, 3.7)
                    if len(rating.split('.')[-1]) <= 1 if '.' in rating else True:
                        print(f"  [INFO] Extracted roundedAverageOverallRating from JSON: {rating}")
                        return rating

            # Method 2: XPath fallback
            rating_text = self.extract_text_safe(tree, self.xpaths.get('star_rating'))
            if rating_text:
                # Check for "No ratings yet" first
                if "No ratings yet" in rating_text:
                    return "No ratings yet"

                # Extract number before "out of"
                # Handles: "4.4 out of 5" or "3.9 stars out of 69 reviews"
                match = re.search(r'([\d.]+)\s*(?:stars?\s*)?out of', rating_text, re.IGNORECASE)
                if match:
                    return match.group(1)

            # If no rating found, check for "No ratings yet" at specific location (from DB)
            no_ratings_xpaths = [
                self.xpaths.get('no_ratings_yet_1'),
                self.xpaths.get('no_ratings_yet_2'),
                self.xpaths.get('no_ratings_yet_3'),
                self.xpaths.get('no_ratings_yet_4'),
            ]

            for xpath in [x for x in no_ratings_xpaths if x]:
                result = tree.xpath(xpath)
                if result:
                    text = result[0].text_content().strip() if hasattr(result[0], 'text_content') else str(result[0]).strip()
                    if "No ratings yet" in text:
                        return "No ratings yet"

            return None
        except Exception as e:
            return None

    def get_price_container(self, tree):
        """Get the main price container to avoid picking prices from other sections like 'top deals'
        Returns: price container element or None
        """
        try:
            # Try to find the main product price container (from DB)
            container_xpaths = [
                self.xpaths.get('price_container_1'),
                self.xpaths.get('price_container_2'),
                self.xpaths.get('price_container_3'),
                self.xpaths.get('price_container_4'),
                self.xpaths.get('price_container_5'),
            ]

            for xpath in [x for x in container_xpaths if x]:
                containers = tree.xpath(xpath)
                if containers:
                    return containers[0]

            # If no specific container found, return None
            # This will fall back to searching entire page
            return None
        except:
            return None

    def extract_final_price(self, tree):
        """Extract final price from detail page (container-based)
        Example: <span itemprop="price" data-seo-id="hero-price">Now $238.00</span>
        Returns: $238.00 or "See price in cart"
        """
        try:
            # Try to get price container first
            price_container = self.get_price_container(tree)
            search_context = price_container if price_container is not None else tree

            # Try multiple XPath strategies (relative to container if available) - from DB
            xpaths = [
                self.xpaths.get('final_price_1'),
                self.xpaths.get('final_price_2'),
                self.xpaths.get('final_price_3'),
                self.xpaths.get('final_price_4'),
            ]

            for xpath in [x for x in xpaths if x]:
                try:
                    elements = search_context.xpath(xpath)
                    if elements:
                        text = elements[0].text_content().strip()

                        # Check for "See price in cart" first
                        if "See price in cart" in text:
                            print(f"       Final Price: See price in cart")
                            return "See price in cart"

                        # Check for "Not Available"
                        if "Not Available" in text:
                            print(f"       Final Price: Not Available")
                            return "Not Available"

                        # Extract dollar price (e.g., "Now $238.00" -> "$238.00")
                        price_match = re.search(r'\$[\d,]+\.?\d*', text)
                        if price_match:
                            print(f"       Final Price: {price_match.group(0)}")
                            return price_match.group(0)
                except:
                    continue

            # Fallback 1: Look for "See price in cart" at specific locations (from DB)
            see_price_xpaths = [
                self.xpaths.get('see_price_in_cart_1'),
                self.xpaths.get('see_price_in_cart_2'),
                self.xpaths.get('see_price_in_cart_3'),
                self.xpaths.get('see_price_in_cart_4'),
            ]

            for xpath in [x for x in see_price_xpaths if x]:
                try:
                    elements = tree.xpath(xpath)
                    if elements:
                        text = elements[0].text_content().strip()
                        if "See price in cart" in text:
                            print(f"       Final Price: See price in cart")
                            return "See price in cart"
                except:
                    continue

            # Fallback 2: Look for "Starting from $X" at specific locations (from DB)
            starting_from_xpaths = [
                self.xpaths.get('starting_from_1'),
                self.xpaths.get('starting_from_2'),
                self.xpaths.get('starting_from_3'),
            ]

            for xpath in [x for x in starting_from_xpaths if x]:
                try:
                    elements = tree.xpath(xpath)
                    if elements:
                        text = elements[0].text_content().strip()
                        # Extract price from "Starting from $1,995.00" -> "$1,995.00"
                        price_match = re.search(r'\$[\d,]+\.?\d*', text)
                        if price_match:
                            print(f"       Final Price: {price_match.group(0)}")
                            return price_match.group(0)
                except:
                    continue

            # Fallback 3: Look for "Not Available" at specific locations (from DB)
            not_available_xpaths = [
                self.xpaths.get('not_available_1'),
                self.xpaths.get('not_available_2'),
                self.xpaths.get('not_available_3'),
            ]

            for xpath in [x for x in not_available_xpaths if x]:
                try:
                    elements = tree.xpath(xpath)
                    if elements:
                        text = elements[0].text_content().strip()
                        if "Not Available" in text:
                            print(f"       Final Price: Not Available")
                            return "Not Available"
                except:
                    continue

            return None
        except Exception as e:
            return None

    def extract_original_price(self, tree, savings):
        """Extract original/strike-through price from detail page using DB xpath
        Only extract if savings exists (to avoid picking prices from other sections)

        Args:
            tree: HTML tree
            savings: savings value from extract_text_safe (e.g., "$60.00")

        Returns: $298.00 or None
        """
        try:
            # Only extract original price if there's a savings/discount
            if not savings:
                return None

            # Use DB xpath for original_price (strike-through price)
            xpath = self.xpaths.get('original_price')
            if not xpath:
                print(f"       Original Price: xpath not found in DB")
                return None

            text = self.extract_text_safe(tree, xpath)

            if text:
                # Extract price (e.g., "$4,999.99")
                price_match = re.search(r'\$[\d,]+\.?\d*', text)
                if price_match:
                    print(f"       Original Price: {price_match.group(0)} (with savings: {savings})")
                    return price_match.group(0)

            # If savings exists but no strike-through price found, log it
            print(f"       Original Price: Not found (but savings exists: {savings})")
            return None
        except Exception as e:
            return None

    def parse_number_format(self, text):
        """Parse numbers like '100+', '10k', '1,000+', '1000+' to integer"""
        if not text:
            return None
        try:
            # Remove any non-numeric characters except 'k' and '+'
            text = text.strip().lower()

            # Remove commas for easier parsing
            text_no_comma = text.replace(',', '')

            # Handle 'k' (thousands)
            if 'k' in text_no_comma:
                number = re.search(r'([\d.]+)k', text_no_comma)
                if number:
                    return int(float(number.group(1)) * 1000)

            # Handle '+' or regular numbers (now without commas)
            number = re.search(r'(\d+)', text_no_comma)
            if number:
                return int(number.group(1))

            return None
        except Exception as e:
            return None

    def extract_count_of_star_ratings(self, tree):
        """Extract total star rating count from star rating span
        New method: Extract from "4.4 stars out of 50630 reviews" text
        Returns: integer (e.g., 50630) or None
        """
        try:
            # ===== NEW METHOD: Extract from w_iUH7 span (from DB) =====
            # <span class="w_iUH7">4.4 stars out of 50630 reviews</span>
            count_star_xpaths = [
                self.xpaths.get('count_star_ratings_1'),
                self.xpaths.get('count_star_ratings_2'),
                self.xpaths.get('count_star_ratings_3'),
                self.xpaths.get('count_star_ratings_4'),
            ]

            for xpath in [x for x in count_star_xpaths if x]:
                elements = tree.xpath(xpath)
                if elements:
                    text = elements[0].text_content().strip()
                    match = re.search(r'out of\s*([\d,]+)\s*reviews?', text)
                    if match:
                        return int(match.group(1).replace(',', ''))

            # ===== NEW: Extract from "X ratings" link/span (from DB) =====
            ratings_link_xpaths = [
                self.xpaths.get('reviews_link_1'),
                self.xpaths.get('reviews_link_2'),
                self.xpaths.get('reviews_link_3'),
            ]
            for xpath in [x for x in ratings_link_xpaths if x]:
                elements = tree.xpath(xpath)
                if elements:
                    text = elements[0].text_content().strip()
                    match = re.search(r'([\d,]+)\s*ratings?', text, re.IGNORECASE)
                    if match:
                        count = int(match.group(1).replace(',', ''))
                        print(f"  [INFO] Extracted count_of_star_ratings from ratings link: {count}")
                        return count

            # Method 6: span with "X ratings" in item-review-section (from DB)
            ratings_span_xpaths = [
                self.xpaths.get('total_ratings_1'),
                self.xpaths.get('total_ratings_2'),
            ]
            for xpath in [x for x in ratings_span_xpaths if x]:
                elements = tree.xpath(xpath)
                if elements:
                    text = elements[0].text_content().strip()
                    match = re.search(r'([\d,]+)\s*ratings?', text, re.IGNORECASE)
                    if match:
                        count = int(match.group(1).replace(',', ''))
                        print(f"  [INFO] Extracted count_of_star_ratings from ratings span: {count}")
                        return count

            # ===== FALLBACK: Old method using star button breakdown =====
            # Get total ratings count for fallback calculation
            total_text = self.extract_text_safe(tree, self.xpaths.get('total_ratings'))
            total_count = None
            if total_text:
                total_match = re.search(r'(\d+)', total_text.replace(',', ''))
                if total_match:
                    total_count = int(total_match.group(1))

            star_counts = {}

            # Extract count for each star (5 to 1)
            for star_num in range(5, 0, -1):
                count = None

                # Old Method 1: Extract from "X% (Y)" pattern in span text
                try:
                    star_button_xpath = f"//button[@aria-label[contains(., '{star_num} star')]]"
                    star_buttons = tree.xpath(star_button_xpath)

                    if star_buttons:
                        percentage_spans = star_buttons[0].xpath(".//span[contains(text(), '% (')]")
                        if percentage_spans:
                            text = percentage_spans[0].text_content().strip()
                            match = re.search(r'\((\d+)\)', text)
                            if match:
                                count = int(match.group(1))
                except:
                    pass

                # Old Method 2: Extract from aria-label
                if count is None:
                    try:
                        aria_xpath = f"//button[@aria-label[contains(., '{star_num} star')]]/@aria-label"
                        aria_labels = tree.xpath(aria_xpath)
                        if aria_labels:
                            aria_text = aria_labels[0]
                            match = re.search(r'(\d+)\s+ratings?\s+are\s+rated', aria_text)
                            if match:
                                count = int(match.group(1))
                    except:
                        pass

                # Old Method 3: Calculate from percentage if total_count available
                if count is None and total_count:
                    try:
                        star_button_xpath = f"//button[@aria-label[contains(., '{star_num} star')]]"
                        star_buttons = tree.xpath(star_button_xpath)

                        if star_buttons:
                            percentage_spans = star_buttons[0].xpath(".//span[contains(text(), '%')]")
                            if percentage_spans:
                                text = percentage_spans[0].text_content().strip()
                                match = re.search(r'(\d+)%', text)
                                if match:
                                    percentage = int(match.group(1))
                                    count = round(total_count * percentage / 100.0)
                    except:
                        pass

                # Store the count
                if count is not None:
                    star_counts[star_num] = count

            # Return total sum of all star ratings as integer
            if star_counts:
                return sum(star_counts.values())

            return None

        except Exception as e:
            print(f"  [WARNING] Failed to extract star rating counts: {e}")
            return None

    def extract_shipping_info(self, tree):
        """Combine two shipping info parts (from DB XPaths)"""
        try:
            # XPaths from DB
            xpath_part1 = self.xpaths.get('shipping_part1')  # e.g., "Arrives Dec 23"
            xpath_part2 = self.xpaths.get('shipping_part2')  # e.g., "Free"

            part1 = self.extract_text_safe(tree, xpath_part1) if xpath_part1 else None
            part2 = self.extract_text_safe(tree, xpath_part2) if xpath_part2 else None

            parts = []
            if part1:
                parts.append(part1)
            if part2:
                parts.append(part2)

            if parts:
                return ', '.join(parts)
            return None
        except Exception as e:
            return None

    def extract_badges(self, tree):
        """
        Extract all badges and classify them:
        - "bought since yesterday" -> purchased_yesterday (number only)
        - "people's carts" -> added_to_carts (number only)
        - Others ("Best seller", "Popular pick", etc.) -> sku_popularity (text)
        """
        try:
            # Find all badge elements - ONLY from main product info section
            # Restrict to the top product info area to avoid similar products section
            # XPaths from DB
            badge_xpaths = [
                self.xpaths.get('badges_1'),
                self.xpaths.get('badges_2'),
                self.xpaths.get('badges_3'),
                self.xpaths.get('badges_4'),
            ]
            badge_xpaths = [x for x in badge_xpaths if x]  # Filter out None values

            all_badges = []
            for xpath in badge_xpaths:
                badges = tree.xpath(xpath)
                if badges:
                    for badge in badges:
                        text = badge.text_content().strip() if hasattr(badge, 'text_content') else str(badge).strip()
                        if text and text not in all_badges:
                            all_badges.append(text)
                    # If we found badges with this xpath, stop searching (don't accumulate from other xpaths)
                    if all_badges:
                        print(f"  [INFO] Found {len(all_badges)} badges: {all_badges}")
                        break

            # Classify badges
            purchased_yesterday = None
            added_to_carts = None
            sku_popularity = None
            sku_status_badge = None  # For Rollback

            for badge_text in all_badges:
                badge_lower = badge_text.lower()

                # Check for "bought since yesterday"
                if 'bought since yesterday' in badge_lower:
                    purchased_yesterday = self.parse_number_format(badge_text)

                # Check for "people's carts"
                elif "people's carts" in badge_lower or 'peoples carts' in badge_lower:
                    added_to_carts = self.parse_number_format(badge_text)

                # Check for "Rollback" -> goes to sku_status, NOT sku_popularity
                elif 'rollback' in badge_lower:
                    sku_status_badge = "Rollback"

                # Everything else is sku_popularity
                else:
                    # Collect popularity badges (Best seller, Popular pick, etc.)
                    if not sku_popularity:
                        sku_popularity = badge_text
                    else:
                        sku_popularity += f", {badge_text}"

            return {
                'purchased_yesterday': purchased_yesterday,
                'added_to_carts': added_to_carts,
                'sku_popularity': sku_popularity,
                'sku_status_badge': sku_status_badge  # New: Rollback
            }

        except Exception as e:
            print(f"  [WARNING] Failed to extract badges: {e}")
            return {
                'purchased_yesterday': None,
                'added_to_carts': None,
                'sku_popularity': None,
                'sku_status_badge': None
            }

    def extract_similar_products(self, tree):
        """Extract all similar product names and join with comma"""
        try:
            similar_xpath = self.xpaths.get('similar_products')
            if not similar_xpath:
                return None

            # Get all similar product containers
            containers = tree.xpath(similar_xpath)
            if not containers:
                return None

            product_names = []
            for container in containers:
                # Extract product name from each container (only product name, no price)
                # Use data-automation-id="product-title" to get clean product name
                name_xpath = './/h3[@data-automation-id="product-title"]'
                name_elem = container.xpath(name_xpath)
                if name_elem:
                    name = name_elem[0].text_content().strip() if hasattr(name_elem[0], 'text_content') else str(name_elem[0]).strip()
                    if name:
                        product_names.append(name)

            if product_names:
                return ', '.join(product_names)
            return None

        except Exception as e:
            print(f"  [WARNING] Failed to extract similar products: {e}")
            return None

    def extract_model_year(self, retailer_sku_name):
        """Extract model year from product name

        Patterns:
        - (2025) or (2025 Model) - year in parentheses
        - 2025 Model - year followed by Model
        - Smart TV 2025 - year at end of product name

        Examples:
        - 'Samsung 65" The Frame Pro ... Smart TV (2025)' -> 2025
        - 'Hisense 55" Class U7 Series... (55U75QG, 2025 Model)' -> 2025
        - 'SAMSUNG 85" Class QN90D Neo QLED 4K Smart TV 2024' -> 2024
        """
        if not retailer_sku_name:
            return None

        try:
            # Pattern 1: (2025) or (2025 Model)
            match = re.search(r'\((\d{4})(?:\s*Model)?\)', retailer_sku_name)
            if match:
                year = int(match.group(1))
                if 2015 <= year <= 2030:
                    print(f"  [INFO] Model year extracted (pattern 1): {year}")
                    return year

            # Pattern 2: 2025 Model (without parentheses)
            match = re.search(r'(\d{4})\s*Model', retailer_sku_name)
            if match:
                year = int(match.group(1))
                if 2015 <= year <= 2030:
                    print(f"  [INFO] Model year extracted (pattern 2): {year}")
                    return year

            # Pattern 3: year at end of product name (Smart TV 2025, 4K 2024, etc.)
            match = re.search(r'\b(20[12]\d)\s*$', retailer_sku_name.strip())
            if match:
                year = int(match.group(1))
                print(f"  [INFO] Model year extracted (pattern 3): {year}")
                return year

            return None

        except Exception as e:
            print(f"  [WARNING] Failed to extract model year: {e}")
            return None

    def extract_screen_size(self, tree, retailer_sku_name=None):
        """Extract screen size from product name or 'Specifications at a glance' section
        Example: 'SAMSUNG 77" Class...' -> '77 inches', '65 in' -> '65 inches'

        Priority:
        1. Product name (most reliable - avoids Resolution like 1280x720 being extracted)
        2. Specifications at a glance (fallback)
        """
        try:
            # Method 1 (Primary): Extract from retailer_sku_name (product name)
            # This avoids extracting Resolution values like 1280 from "1280 x 720"
            if retailer_sku_name:
                # Look for patterns: "32"", "50-Inch", "98" Q Series", "77" Class", etc.
                # Matches: number + (space/hyphen + inch/inches OR various quote characters)
                # Support: " (standard), " " (unicode quotes), ″ (double prime)
                match = re.search(r'(\d+\.?\d*)(?:[\s-]*inch(?:es)?|["\u201c\u201d\u2033])', retailer_sku_name, re.IGNORECASE)
                if match:
                    size_number = match.group(1)
                    print(f"  [INFO] Screen size extracted from product name: {size_number} inches")
                    return f"{size_number} inches"

            # Method 2 (Fallback): Try XPath from Specifications at a glance (from DB)
            xpaths = [
                self.xpaths.get('screen_size_5'),  # Definition list structure
                self.xpaths.get('screen_size_6'),  # Definition list - direct sibling
                self.xpaths.get('screen_size_7'),  # Table structure
                self.xpaths.get('screen_size_8'),  # Alternative table structure
                self.xpaths.get('screen_size_1'),  # aria-label
                self.xpaths.get('screen_size_2'),  # Screen size text sibling div
                self.xpaths.get('screen_size_3'),  # Direct XPath
                self.xpaths.get('screen_size_4'),  # Specifications at a glance container
            ]
            xpaths = [x for x in xpaths if x]  # Filter out None values

            screen_size_text = None
            for xpath in xpaths:
                result = tree.xpath(xpath)
                if result:
                    if isinstance(result[0], str):
                        screen_size_text = result[0].strip()
                    else:
                        screen_size_text = result[0].text_content().strip() if hasattr(result[0], 'text_content') else str(result[0]).strip()

                    if screen_size_text:
                        break

            if screen_size_text:
                # Extract number from text (including decimal)
                # Supports: "24 in", "24 inch", "24 inches", '24"', "43"
                match = re.search(r'([\d.]+)\s*(?:in(?:ch(?:es)?)?|")?', screen_size_text, re.IGNORECASE)
                if match:
                    size_number = match.group(1)
                    # Validate: TV screen size should be reasonable (10-150 inches)
                    if 10 <= float(size_number) <= 150:
                        print(f"  [INFO] Screen size extracted from XPath: {size_number} inches")
                        return f"{size_number} inches"

            return None

        except Exception as e:
            print(f"  [WARNING] Failed to extract screen size: {e}")
            return None

    def extract_offer(self, tree):
        """Extract offer count (e.g., '5' from '5 free offers, including Apple TV up to 3 months free')
        Only extracts if 'free offer' text is found, returns the number only
        """
        try:
            # Use DB xpath for offer
            xpath = self.xpaths.get('offer')
            if not xpath:
                print(f"  [DEBUG] extract_offer - xpath not found in DB")
                return None

            text = self.extract_text_safe(tree, xpath)
            print(f"  [DEBUG] extract_offer - raw text: {repr(text)}")
            if text and 'free offer' in text.lower():
                # Extract the number at the beginning (e.g., "5" from "5 free offers...")
                match = re.search(r'^(\d+)', text.strip())
                if match:
                    print(f"  [DEBUG] extract_offer - extracted number: {match.group(1)}")
                    return match.group(1)
            return None

        except Exception as e:
            print(f"  [WARNING] Failed to extract offer: {e}")
            return None

    def extract_count_of_reviews(self, tree, star_rating=None, page_source=None, count_of_star_ratings=None):
        """Extract total number of reviews from main page
        Example: '248 reviews' -> 248, '43 ratings' -> 43, 'No ratings yet' -> 0

        Priority:
        1. Check "No ratings yet" first -> return 0
        2. "Showing 1-3 of 18,552 reviews" pattern (most accurate)
        3. "View all reviews (4,686)" button

        Args:
            tree: HTML tree
            star_rating: Star rating value (if "No ratings yet", return 0)
            page_source: Raw HTML page source for JSON extraction
            count_of_star_ratings: Star ratings count for validation (skip if extracted value equals this and >= 10)
        """
        def is_likely_ratings_count(value):
            """Check if extracted value is likely the ratings count (not actual reviews)"""
            if count_of_star_ratings is None:
                return False
            # If value equals count_of_star_ratings and >= 10, it's likely wrong
            if value == count_of_star_ratings and count_of_star_ratings >= 10:
                print(f"  [WARNING] Extracted count_of_reviews ({value}) equals count_of_star_ratings - skipping this source")
                return True
            return False

        try:
            # Check 1: If star_rating is "No ratings yet", return 0 immediately
            if star_rating and "No ratings yet" in str(star_rating):
                print(f"  [INFO] Star rating is 'No ratings yet', setting count_of_reviews to 0")
                return 0

            # Check 2: Look for "0 reviews" in page source -> return 0 (from DB)
            if page_source and "0 reviews" in page_source:
                zero_reviews_xpaths = [
                    self.xpaths.get('zero_reviews_1'),
                    self.xpaths.get('zero_reviews_2'),
                ]
                zero_reviews_xpaths = [x for x in zero_reviews_xpaths if x]
                for xpath in zero_reviews_xpaths:
                    result = tree.xpath(xpath)
                    if result:
                        text = result[0].text_content().strip() if hasattr(result[0], 'text_content') else str(result[0]).strip()
                        if "0 reviews" in text:
                            print(f"  [INFO] Found '0 reviews' on page, setting count_of_reviews to 0")
                            return 0

            # Check 3: Look for "No ratings yet" in page source (from DB)
            if page_source and "No ratings yet" in page_source:
                no_ratings_xpaths = [
                    self.xpaths.get('no_ratings_yet_3'),
                    self.xpaths.get('no_ratings_yet_4'),
                ]
                no_ratings_xpaths = [x for x in no_ratings_xpaths if x]
                for xpath in no_ratings_xpaths:
                    result = tree.xpath(xpath)
                    if result:
                        print(f"  [INFO] Found 'No ratings yet' on page, setting count_of_reviews to 0")
                        return 0

            # Priority 1: Extract from "Showing 1-3 of 18,552 reviews" pattern (most accurate, from DB)
            showing_xpaths = [
                self.xpaths.get('showing_reviews_1'),
                self.xpaths.get('showing_reviews_2'),
                self.xpaths.get('showing_reviews_3'),
                self.xpaths.get('showing_reviews_4'),
                self.xpaths.get('showing_reviews_5'),
            ]
            showing_xpaths = [x for x in showing_xpaths if x]
            for xpath in showing_xpaths:
                result = tree.xpath(xpath)
                if result:
                    text = result[0].text_content().strip() if hasattr(result[0], 'text_content') else str(result[0]).strip()
                    # Pattern: "Showing 1-3 of 4,686 reviews" -> extract 4,686
                    match = re.search(r'of\s+([\d,]+)\s+reviews?', text, re.IGNORECASE)
                    if match:
                        count_str = match.group(1).replace(',', '')
                        count = int(count_str)
                        if not is_likely_ratings_count(count):
                            print(f"  [INFO] Extracted count from 'Showing X of Y reviews': {count}")
                            return count

            # Priority 2: Extract from "View all reviews (4,686)" button (from DB)
            view_all_xpaths = [
                self.xpaths.get('view_all_reviews_count_1'),
                self.xpaths.get('view_all_reviews_count_2'),
                self.xpaths.get('view_all_reviews_count_3'),
                self.xpaths.get('view_all_reviews_count_4'),
                self.xpaths.get('view_all_reviews_count_5'),
            ]
            view_all_xpaths = [x for x in view_all_xpaths if x]
            for xpath in view_all_xpaths:
                result = tree.xpath(xpath)
                if result:
                    text = result[0].text_content().strip() if hasattr(result[0], 'text_content') else str(result[0]).strip()
                    # Pattern: "View all reviews (4,686)" -> extract 4,686
                    match = re.search(r'\(([\d,]+)\)', text)
                    if match:
                        count_str = match.group(1).replace(',', '')
                        count = int(count_str)
                        if not is_likely_ratings_count(count):
                            print(f"  [INFO] Extracted count from 'View all reviews' button: {count}")
                            return count

            # Fallback: Check for "No ratings yet" -> return 0 (from DB)
            no_ratings_xpaths = [
                self.xpaths.get('no_ratings_yet_3'),
                self.xpaths.get('no_ratings_yet_4'),
            ]
            no_ratings_xpaths = [x for x in no_ratings_xpaths if x]
            for xpath in no_ratings_xpaths:
                result = tree.xpath(xpath)
                if result:
                    return 0

            return None

        except Exception as e:
            print(f"  [WARNING] Failed to extract count of reviews: {e}")
            return None

    def is_invalid_sku(self, sku):
        """Check if SKU is invalid (generic values that are not actual model numbers)"""
        if not sku:
            return True

        sku_clean = sku.strip()
        sku_lower = sku_clean.lower()
        # Remove all spaces for pattern matching
        sku_no_space = sku_clean.replace(' ', '').lower()

        # Too short (just numbers or very short strings)
        if len(sku_clean) < 4:
            return True

        # Exact match invalid values
        invalid_values = [
            '4K UHD', '4K (2160P)', '3840 x 2160', '1920 x 1080',
            '1080p', '1080i', '720p', '480p', '480i', '2160p',
            'Samsung', 'Hisense', 'LG', 'TCL', 'Philips', 'Vizio',
            'UHD', 'FHD', 'HD', 'QLED', 'OLED', 'LED'
        ]
        if sku_clean in invalid_values or sku_clean.upper() in invalid_values:
            return True

        # Contains invalid keywords (resolution/spec terms)
        invalid_keywords = [
            '1080p', '720p', '480p', '2160p', '4k', '8k',
            'hz', 'nits', 'uhd', 'fhd', 'qled', 'oled',
            'skip to main', 'sign in', 'pickup', 'delivery',
            'department', 'close', 'refresh rate', 'resolution'
        ]
        for keyword in invalid_keywords:
            if keyword in sku_lower:
                return True

        # Contains semicolon (multiple resolutions listed)
        if ';' in sku_clean:
            return True

        # Pattern 1: Refresh rate (60Hz, 120Hz, 144Hz, 60 Hz, 120 hz, etc.)
        # Check both with and without spaces
        if re.search(r'^\d+\s*hz$', sku_no_space, re.IGNORECASE):
            return True

        # Pattern 2: Resolution with x (3,840 x 2,160 or 3840 x 2160 or 1920 x 1080)
        if re.search(r'\d{1,3}(,\d{3})?\s*x\s*\d{1,3}(,\d{3})?', sku_clean, re.IGNORECASE):
            return True

        # Pattern 3: Resolution format (480i, 480p, 720p, 1080i, 1080p, 2160p, etc.)
        # Also catch with spaces: "1080 p", "720 i"
        if re.search(r'^\d{3,4}\s*[ip]$', sku_no_space, re.IGNORECASE):
            return True

        # Pattern 4: Contains parentheses with resolution like (2160p), (1080p)
        if '(' in sku_clean and ')' in sku_clean:
            if re.search(r'\(\d+[ip]\)', sku_clean):
                return True

        # Pattern 5: Just numbers (like "75", "65", etc. - likely screen size)
        if sku_clean.isdigit():
            return True

        # Pattern 6: Number followed by unit (catch variations)
        # 60hz, 120Hz, 4k, 8K, etc.
        if re.search(r'^\d+\s*(hz|khz|mhz|k|p|i)$', sku_no_space, re.IGNORECASE):
            return True

        # Pattern 7: Contains "refresh" or common spec terms
        if any(term in sku_lower for term in ['refresh', 'hertz', 'resolution', 'display']):
            return True

        return False

    def extract_sku_from_url(self, url):
        """Extract SKU from product URL"""
        try:
            # URL pattern: https://www.walmart.com/ip/{product-name}/{model}/{id}
            # Or: https://www.walmart.com/ip/{model}-{suffix}/{id}

            # Extract path from URL
            from urllib.parse import urlparse
            parsed = urlparse(url)
            path_parts = parsed.path.strip('/').split('/')

            if len(path_parts) < 2 or path_parts[0] != 'ip':
                return None

            # Get the second part (product name/model part)
            product_part = path_parts[1]

            # Pattern 1: Simple model at end (e.g., "55UA7500ZUA-AUS")
            if len(path_parts) == 2:
                # This is the model itself
                # Remove "-AUS" or similar suffix
                model = product_part.replace('-AUS', '')
                if model and len(model) > 3:
                    return model

            # Pattern 2: Model within product name (e.g., "TCL-43-Class-S3-43S310R-1080p-...")
            # Look for pattern: capital letters + numbers (like 43S310R, UN55U7900FFXZA)
            parts = product_part.split('-')

            # Check if last part is pure numeric model (8+ digits, like 100012589)
            if parts and parts[-1].isdigit() and len(parts[-1]) >= 8:
                return parts[-1]

            # Find parts that look like model numbers (contain both letters and numbers)
            potential_models = []
            for i, part in enumerate(parts):
                # Skip pure numbers, pure letters, or common words
                if not part or part.isdigit() or part.isalpha():
                    continue
                if part.lower() in ['class', 'inch', 'hd', 'uhd', 'led', 'lcd', 'smart', 'tv', 'new', 'with']:
                    continue

                # Check if it contains both letters and numbers
                has_letter = any(c.isalpha() for c in part)
                has_number = any(c.isdigit() for c in part)

                if has_letter and has_number and len(part) >= 5:
                    # Check if next part is a short number suffix (like "08", "84", "0809")
                    model = part
                    if i + 1 < len(parts):
                        next_part = parts[i + 1]
                        # If next part is 2-4 digit number, append it
                        if next_part.isdigit() and 2 <= len(next_part) <= 4:
                            model = f"{part}-{next_part}"

                    potential_models.append(model)

            # Return the longest potential model (usually the most specific)
            if potential_models:
                return max(potential_models, key=len)

            return None

        except Exception as e:
            print(f"  [DEBUG] Failed to extract SKU from URL: {e}")
            return None

    def extract_sku_from_product_name(self, product_name):
        """Extract SKU (model number) from retailer_sku_name

        Priority:
        1. Parentheses pattern: (VQD50M-08), (70PUL7553/F7), (43Q651G)
        2. After dash at end: - OLED55C4PUA, - 43QNED80TUC
        3. After comma (model only): K-50S30, 2024 Model -> K-50S30
        4. Last word if looks like model: QN50LS03BAFXZA, 58R6E3
        5. Series name fallback: QLED Q7F, Select Series 4K
        """
        try:
            if not product_name:
                return None

            import re

            # Exclude words that are NOT model numbers
            exclude_words = ['HD', 'UHD', 'LED', 'LCD', 'OLED', 'QLED', '4K', '8K', 'TV',
                           'Model', 'Series', 'Class', 'Smart', 'NEW', 'Inch', 'Refurbished',
                           '2024', '2025', '2023', '2022', '2021', '2020']

            def is_valid_model(text):
                """Check if text looks like a valid model number"""
                if not text or len(text) < 4:
                    return False
                # Should not be in exclude list
                if text.upper() in [w.upper() for w in exclude_words]:
                    return False
                # Should contain both letters and numbers OR be alphanumeric with special chars
                has_letter = any(c.isalpha() for c in text)
                has_number = any(c.isdigit() for c in text)
                # Model numbers typically have letters+numbers or just numbers (8+ digits)
                if has_letter and has_number:
                    return True
                if text.isdigit() and len(text) >= 8:  # Pure numeric like 100150805
                    return True
                return False

            # Pattern 1: In parentheses (highest priority)
            # e.g., (VQD50M-08), (70PUL7553/F7), (43Q651G), (100150805)
            paren_matches = re.findall(r'\(([A-Za-z0-9/_-]+)\)', product_name)
            for match in paren_matches:
                # Skip year patterns like (NEW 2024)
                if match.upper() in exclude_words or re.match(r'^NEW\s*\d{4}$', match, re.IGNORECASE):
                    continue
                if is_valid_model(match):
                    return match

            # Pattern 2: After " - " at the end
            # e.g., "... - OLED55C4PUA", "... - 43QNED80TUC"
            if ' - ' in product_name:
                after_dash = product_name.split(' - ')[-1].strip()
                # Get first word after dash (model is usually first)
                first_word = after_dash.split()[0] if after_dash.split() else after_dash
                first_word = first_word.strip('.,;:')
                if is_valid_model(first_word):
                    return first_word

            # Pattern 3: After comma (check if it's a model, not "2024 Model")
            # e.g., "Sony 50" ... K-50S30, 2024 Model" -> K-50S30
            if ',' in product_name:
                parts = product_name.split(',')
                # Check second-to-last part if last part contains "Model" or year
                for i in range(len(parts) - 1, -1, -1):
                    part = parts[i].strip()
                    # Skip if contains "Model" or is just a year
                    if 'Model' in part or re.match(r'^\d{4}$', part):
                        continue
                    # Get first word of this part
                    first_word = part.split()[0] if part.split() else part
                    first_word = first_word.strip('.,;:')
                    if is_valid_model(first_word):
                        return first_word

            # Pattern 4: Last word if looks like model number
            # e.g., "SAMSUNG 50" ... QN50LS03BAFXZA"
            words = product_name.split()
            if words:
                last_word = words[-1].strip('.,;:')
                if is_valid_model(last_word):
                    return last_word

            # Pattern 5: Series name fallback (when no model number found)
            # e.g., "Samsung 43" Class QLED Q7F 4K..." -> "QLED Q7F"
            # Look for patterns like "Q7F", "S3", "C4", "R6" etc.
            series_match = re.search(r'\b([A-Z]\d+[A-Z]?)\b', product_name)
            if series_match:
                series = series_match.group(1)
                # Check if there's a prefix like QLED, OLED
                prefix_match = re.search(r'(QLED|OLED|LED|UHD)\s+' + re.escape(series), product_name)
                if prefix_match:
                    return f"{prefix_match.group(1)} {series}"
                return series

            # No SKU found
            return "no sku"

        except Exception as e:
            print(f"  [DEBUG] Failed to extract SKU from product name: {e}")
            return None

    def extract_sku_from_specifications(self):
        """Extract SKU from Specifications dialog - Model field (from DB)"""
        try:
            page_source = self.page.html
            tree = html.fromstring(page_source)

            # Try multiple XPaths for Model field (from DB)
            model_xpaths = [
                self.xpaths.get('specifications_model_1'),
                self.xpaths.get('specifications_model_2'),
                self.xpaths.get('specifications_model_3'),
                self.xpaths.get('specifications_model_4'),
            ]
            model_xpaths = [x for x in model_xpaths if x]

            for xpath in model_xpaths:
                result = tree.xpath(xpath)
                if result:
                    model = result[0].text_content().strip() if hasattr(result[0], 'text_content') else str(result[0]).strip()
                    if model and len(model) >= 4:
                        print(f"  [INFO] Extracted SKU from Specifications: {model}")
                        return model

            return None

        except Exception as e:
            print(f"  [DEBUG] Failed to extract SKU from Specifications: {e}")
            return None

    def extract_sku(self, product_name):
        """Extract SKU - try product name first, then Specifications dialog

        Args:
            product_name: Retailer SKU Name (product title)

        Returns:
            SKU string or "no sku" if not found
        """
        # Priority 1: Extract from product name
        sku = self.extract_sku_from_product_name(product_name)
        if sku and sku != "no sku":
            print(f"  [INFO] Extracted SKU from product name: {sku}")
            return sku

        # Priority 2: Extract from Specifications dialog
        sku = self.extract_sku_from_specifications()
        if sku:
            return sku

        # No SKU found
        return "no sku"

    def extract_sku_from_lg_xpath(self):
        """Extract SKU using LG-specific XPath (from main page, from DB)"""
        try:
            page_source = self.page.html
            tree = html.fromstring(page_source)

            # Try multiple LG-specific XPaths (from DB)
            lg_xpaths = [
                self.xpaths.get('model_name_1'),
                self.xpaths.get('model_name_2'),
                self.xpaths.get('model_name_3'),
            ]
            lg_xpaths = [x for x in lg_xpaths if x]

            for xpath in lg_xpaths:
                sku = self.extract_text_safe(tree, xpath)
                if sku and 5 <= len(sku) <= 20:
                    print(f"  [INFO] Extracted SKU from LG XPath: {sku}")
                    return sku

            return None

        except Exception as e:
            print(f"  [DEBUG] Failed to extract SKU from LG XPath: {e}")
            return None

    def calculate_similarity(self, text1, text2):
        """Calculate similarity between two texts based on common words
        Returns: similarity ratio (0.0 to 1.0)
        """
        if not text1 or not text2:
            return 0.0

        # Normalize: lowercase, remove special chars, split into words
        import re
        words1 = set(re.findall(r'\w+', text1.lower()))
        words2 = set(re.findall(r'\w+', text2.lower()))

        # Remove common words that don't help distinguish (like "class", "smart", "tv", etc.)
        common_stopwords = {'class', 'smart', 'tv', 'led', 'hd', 'uhd', 'series', 'new', 'inches', 'inch', 'in'}
        words1 = words1 - common_stopwords
        words2 = words2 - common_stopwords

        if not words1 or not words2:
            return 0.0

        # Calculate Jaccard similarity (intersection / union)
        intersection = len(words1 & words2)
        union = len(words1 | words2)

        return intersection / union if union > 0 else 0.0

    def click_specifications_and_get_model(self):
        """Extract item from product URL - last segment after final slash

        Example:
        https://www.walmart.com/ip/TCL-50-Class-Q6-50Q651G-4K-UHD-HDR-QLED-Smart-TV-with-Google-TV-NEW-2024/5373842535
        -> Returns: 5373842535
        """
        try:
            print(f"  [INFO] Extracting item from product URL...")

            # Get current URL
            current_url = self.page.url

            # Extract last segment after final "/"
            item = current_url.rstrip('/').split('/')[-1]

            if item:
                print(f"  [OK] Extracted item from URL: {item}")
                return item
            else:
                print(f"  [WARNING] Could not extract item from URL: {current_url}")
                return None

        except Exception as e:
            print(f"  [ERROR] Failed to extract item from URL: {e}")
            import traceback
            traceback.print_exc()
            return None

    def extract_detailed_reviews(self, count_of_reviews=None):
        """Click 'View all reviews' and extract up to 20 reviews"""
        try:
            # 상품 페이지 URL 저장 (재시도용)
            product_url = self.page.url

            # 기본 2페이지, count_of_reviews >= 20이면 3페이지까지 수집 가능
            cor_int = int(str(count_of_reviews).replace(',', '')) if count_of_reviews else 0
            max_pages = 3 if cor_int >= 20 else 2

            # 재시도 포함 최대 2회 시도
            for attempt in range(2):
                if attempt > 0:
                    print(f"  [INFO] Retrying review extraction (attempt {attempt + 1}/2)...")
                    # 크롬 종료 후 재시작
                    self.page.quit()
                    self.setup_driver()
                    self.page.get(product_url)
                    scroll_step_wait = self.config.get_timing_range('scroll_step_wait', 'wmart_tv_dt1') or (3, 4)
                    time.sleep(random.uniform(*scroll_step_wait))

                # Find and click "View all reviews" button
                try:
                    # Scroll to reviews section first
                    self.page.run_js("window.scrollTo(0, document.body.scrollHeight);")
                    time.sleep(2)

                    # Try multiple XPaths to find the button (from DB)
                    view_all_xpaths = [
                        self.xpaths.get('detail_view_all_1'),
                        self.xpaths.get('detail_view_all_2'),
                        self.xpaths.get('detail_view_all_3'),
                        self.xpaths.get('detail_view_all_4'),
                        self.xpaths.get('detail_view_all_5'),
                        self.xpaths.get('view_all_reviews_button'),
                    ]
                    view_all_xpaths = [x for x in view_all_xpaths if x]

                    view_all_btn = None
                    for xpath in view_all_xpaths:
                        if xpath:
                            try:
                                buttons = self.page.eles(f'xpath:{xpath}')
                                # If multiple buttons found, prefer the one with number in parentheses
                                for btn in buttons:
                                    if '(' in btn.text and ')' in btn.text:
                                        view_all_btn = btn
                                        break
                                # If no button with number, use the first one found
                                if not view_all_btn and buttons:
                                    view_all_btn = buttons[0]
                                if view_all_btn:
                                    break
                            except:
                                continue

                    if not view_all_btn:
                        print(f"  [WARNING] Could not find View all reviews button")
                        # if attempt == 0:
                        #     continue  # 재시도
                        return None

                    # Scroll to button with offset to avoid header
                    view_all_btn.scroll.to_see()
                    time.sleep(1)

                    # Log button info before click
                    btn_text = view_all_btn.text if view_all_btn else "N/A"
                    print(f"  [DEBUG] Found View all reviews button: '{btn_text[:50]}...' ")

                    # Get URL before click
                    url_before = self.page.url

                    # Click button
                    view_all_btn.click()
                    scroll_step_wait = self.config.get_timing_range('scroll_step_wait', 'wmart_tv_dt1') or (3, 4)
                    time.sleep(random.uniform(*scroll_step_wait))

                    # Check if URL changed (page navigated)
                    url_after = self.page.url
                    if url_before == url_after:
                        print(f"  [WARNING] URL did not change after clicking View all reviews button")
                        print(f"  [DEBUG] URL: {url_after[:80]}...")
                    else:
                        print(f"  [DEBUG] Navigated to reviews page: {url_after[:80]}...")

                except Exception as e:
                    print(f"  [WARNING] Could not click View all reviews: {e}")
                    # if attempt == 0:
                    #     continue  # 재시도
                    return None

                # Extract reviews from multiple pages (up to 20 reviews)
                reviews = []
                page_num = 1

                while len(reviews) < 20 and page_num <= max_pages:
                    # Wait for review content to load (dynamic rendering)
                    time.sleep(2)

                    # Get xpaths from DB (multiple options with priority)
                    review_text_xpaths = [
                        self.xpaths.get('review_text_1'),
                        self.xpaths.get('review_text_2'),
                        self.xpaths.get('review_text_3'),
                    ]
                    review_text_xpaths = [x for x in review_text_xpaths if x]
                    review_content_wait_xpath = self.xpaths.get('review_content_wait')

                    if not review_text_xpaths:
                        print(f"  [WARNING] No review_text xpaths found in DB")
                        break

                    # Method 1: Use DrissionPage eles() to find review spans directly (most reliable for dynamic content)
                    try:
                        # Wait for review content element to appear (if xpath exists in DB)
                        if review_content_wait_xpath:
                            self.page.ele(f'xpath:{review_content_wait_xpath}', timeout=5)

                        # Try each review_text xpath in priority order
                        review_spans = None
                        used_xpath = None
                        for xpath in review_text_xpaths:
                            review_spans = self.page.eles(f'xpath:{xpath}')
                            if review_spans:
                                used_xpath = xpath
                                break

                        if review_spans:
                            print(f"  [DEBUG] Found {len(review_spans)} review spans on page {page_num} using: {used_xpath[:50]}...")
                            for span in review_spans:
                                if len(reviews) >= 20:
                                    break
                                review_text = span.text.strip() if span.text else ''
                                if review_text and len(review_text) > 5:  # Skip very short texts
                                    reviews.append(review_text)
                        else:
                            print(f"  [WARNING] No review content found on page {page_num}")
                            break
                    except Exception as e:
                        print(f"  [WARNING] DrissionPage method failed: {e}")
                        # Fallback: Try lxml parsing with priority xpaths
                        page_source = self.page.html
                        tree = html.fromstring(page_source)

                        review_spans_lxml = None
                        used_xpath_lxml = None
                        for xpath in review_text_xpaths:
                            review_spans_lxml = tree.xpath(xpath)
                            if review_spans_lxml:
                                used_xpath_lxml = xpath
                                break

                        if review_spans_lxml:
                            print(f"  [DEBUG] Found {len(review_spans_lxml)} review spans via lxml on page {page_num} using: {used_xpath_lxml[:50]}...")
                            for span in review_spans_lxml:
                                if len(reviews) >= 20:
                                    break
                                review_text = span.text_content().strip() if hasattr(span, 'text_content') else str(span).strip()
                                if review_text and len(review_text) > 5:
                                    reviews.append(review_text)
                        else:
                            print(f"  [WARNING] No review content found on page {page_num} (lxml fallback)")
                            break

                    # If we need more reviews and haven't reached max pages, click Next Page
                    if len(reviews) < 20 and page_num < max_pages:
                        try:
                            # Try multiple XPaths to find Next Page button (from DB)
                            next_page_xpaths = [
                                self.xpaths.get('review_next_page_1'),
                                self.xpaths.get('review_next_page_2'),
                                self.xpaths.get('review_next_page_3'),
                            ]
                            # Filter out None values
                            next_page_xpaths = [x for x in next_page_xpaths if x]

                            next_page_btn = None
                            for xpath in next_page_xpaths:
                                try:
                                    next_page_btn = self.page.ele(f'xpath:{xpath}', timeout=2)
                                    if next_page_btn:
                                        break
                                except:
                                    continue

                            if not next_page_btn:
                                print(f"  [WARNING] Could not find Next Page button")
                                break

                            # Scroll to button
                            next_page_btn.scroll.to_see()
                            time.sleep(1)

                            # Click Next Page
                            next_page_btn.click()

                            # Wait for next page to load
                            scroll_step_wait = self.config.get_timing_range('scroll_step_wait', 'wmart_tv_dt1') or (3, 4)
                            time.sleep(random.uniform(*scroll_step_wait))
                            page_num += 1
                        except Exception as e:
                            print(f"  [WARNING] Could not find or click Next Page button: {e}")
                            break
                    else:
                        break

                # 3페이지까지 갔는데 20개 미만이면 재시도
                if max_pages == 3 and len(reviews) < 20 and attempt == 0:
                    print(f"  [INFO] Only collected {len(reviews)} reviews after {page_num} pages, will retry...")
                    continue

                # Format as "review1-content, review2-content, ..."
                if reviews:
                    print(f"  [INFO] Extracted {len(reviews)} reviews from {page_num} page(s)")
                    formatted = []
                    for idx, review in enumerate(reviews[:20], 1):
                        formatted.append(f"review{idx}-{review}")
                    return ', '.join(formatted)

            print(f"  [WARNING] No reviews extracted")
            return None

        except Exception as e:
            print(f"  [WARNING] Failed to extract detailed reviews: {e}")
            import traceback
            traceback.print_exc()
            return None

    def scrape_detail_page(self, url_data):
        """Scrape detail page and extract information"""
        try:
            page_type = url_data['page_type']
            main_rank = url_data['main_rank']
            bsr_rank = url_data['bsr_rank']
            url = url_data['url']

            # Get additional columns from main/bsr tables (excluding prices)
            pick_up_availability = url_data.get('pick_up_availability')
            shipping_availability = url_data.get('shipping_availability')
            delivery_availability = url_data.get('delivery_availability')
            sku_status_from_main = url_data.get('sku_status')  # May contain Rollback
            retailer_membership_discounts = url_data.get('retailer_membership_discounts')
            available_quantity_for_purchase = url_data.get('available_quantity_for_purchase')
            inventory_status = url_data.get('inventory_status')

            print(f"\n{'='*80}")
            print(f"[{page_type.upper()}] Accessing: {url[:80]}...")
            print(f"[INFO] Page type: {page_type} | Main rank: {main_rank if main_rank else 'N/A'} | BSR rank: {bsr_rank if bsr_rank else 'N/A'}")

            # Check if window is still alive, restart if crashed
            try:
                _ = self.page.url
            except Exception as e:
                print(f"  [WARNING] Browser window crashed, restarting driver...")
                try:
                    self.page.quit()
                except:
                    pass
                self.setup_driver()
                print(f"  [OK] Driver restarted successfully")

            print(f"  [INFO] Loading page...")
            self.page.get(url)

            # Wait for product_name element instead of fixed time (DOM optimization)
            try:
                product_name_xpath = self.xpaths.get('product_name')
                if product_name_xpath:
                    self.page.ele(f'xpath:{product_name_xpath}', timeout=10)
                    print(f"  [OK] Product name element loaded")
                else:
                    # Fallback: wait for any h1 element
                    self.page.ele('xpath://h1', timeout=10)
                    print(f"  [OK] Page title element loaded")
            except:
                print(f"  [WARNING] Element wait timeout, continuing anyway...")

            # Minimum wait for page stability (CAPTCHA prevention)
            time.sleep(random.uniform(2, 3))

            print(f"  [INFO] Page loaded, extracting data...")

            page_source = self.page.html
            tree = html.fromstring(page_source)

            # Extract basic data using XPaths (from initial page load)
            retailer_sku_name = self.extract_text_safe(tree, self.xpaths.get('product_name'))
            star_rating = self.extract_star_rating(tree, page_source)

            # Extract discount_type and savings using DB xpaths
            discount_type = self.extract_text_safe(tree, self.xpaths.get('discount_type'))
            savings = self.extract_text_safe(tree, self.xpaths.get('savings'))

            # Extract prices from detail page (original_price only if savings exists)
            final_sku_price = self.extract_final_price(tree)
            original_sku_price = self.extract_original_price(tree, savings)

            # Extract and classify all badges (BEFORE Model extraction)
            badges = self.extract_badges(tree)
            purchased_yesterday = badges['purchased_yesterday']
            added_to_carts = badges['added_to_carts']
            sku_popularity = badges['sku_popularity']
            sku_status_badge = badges['sku_status_badge']  # Rollback from badges

            # Determine final sku_status (from main/bsr table or from badges)
            sku_status = sku_status_from_main if sku_status_from_main else sku_status_badge

            # Process discount_type
            # discount_type may contain "Flash Deal", "Reduced price", "Price when purchased online"
            # If Flash Deal or Reduced price exists, put it first, then add "Price when purchased online" if present
            discount_type_list = []
            if discount_type:
                discount_parts = [p.strip() for p in discount_type.split(',')]
                # Separate Flash Deal/Reduced price from Price when purchased online
                priority_types = []  # Flash Deal, Reduced price
                online_price = None  # Price when purchased online

                for part in discount_parts:
                    part_lower = part.lower()
                    if 'flash deal' in part_lower or 'reduced price' in part_lower:
                        priority_types.append(part)
                    elif 'price when purchased online' in part_lower:
                        online_price = part

                # Build final discount_type: priority types first, then online price
                discount_type_list.extend(priority_types)
                if online_price:
                    discount_type_list.append(online_price)

            final_discount_type = ', '.join(discount_type_list) if discount_type_list else discount_type

            # Extract shipping info (combine 2 parts)
            shipping_info = self.extract_shipping_info(tree)

            # Extract offer info
            offer = self.extract_offer(tree)

            # Extract similar products
            similar_products = self.extract_similar_products(tree)

            # Extract screen size (from main page, with fallback to product name)
            extracted_screen_size = self.extract_screen_size(tree, retailer_sku_name)

            # Scroll to review section for lazy loading content
            try:
                self.page.run_js("window.scrollTo(0, document.body.scrollHeight * 0.5);")
                time.sleep(2)
                # Update page_source and tree after scroll
                page_source = self.page.html
                tree = html.fromstring(page_source)
            except:
                pass

            # Extract count of star ratings (from review section)
            count_of_star_ratings = self.extract_count_of_star_ratings(tree)

            # Wait for count_of_reviews element to load, then extract with retry
            # count_of_star_ratings: "4.4 stars out of 50630 reviews" -> 50630 (ratings count)
            # count_of_reviews: "986 reviews" link -> 986 (actual written reviews count)
            count_of_reviews = None
            max_cor_retries = 3
            for cor_retry in range(max_cor_retries):
                try:
                    # Wait for review section element with DrissionPage
                    review_elem = self.page.ele("xpath://*[@id='item-review-section']/div[7]/h3 | //button[contains(text(), 'View all reviews')] | //span[contains(text(), 'No ratings yet')]", timeout=5)
                    # Element loaded - re-parse and extract
                    page_source = self.page.html
                    tree = html.fromstring(page_source)
                    count_of_reviews = self.extract_count_of_reviews(tree, star_rating, page_source, count_of_star_ratings)
                except:
                    print("  [WARNING] count_of_reviews element not loaded - trying extraction anyway")
                    count_of_reviews = self.extract_count_of_reviews(tree, star_rating, page_source, count_of_star_ratings)

                if count_of_reviews is not None:
                    break
                elif cor_retry < max_cor_retries - 1:
                    print(f"  [RETRY {cor_retry + 1}/{max_cor_retries}] count_of_reviews is None, refreshing page...")
                    self.page.refresh()
                    captcha_after_wait = self.config.get_timing_range('captcha_after_wait') or (3, 5)
                    time.sleep(random.uniform(*captcha_after_wait))
                    page_source = self.page.html
                    tree = html.fromstring(page_source)

            # Click Specifications and get Model (after static content extraction)
            sku_model = self.click_specifications_and_get_model()

            # tv_item_mst fallback for screen_size
            item_mst_data = self.get_item_mst_data(sku_model)
            mst_screen_size = item_mst_data.get('screen_size') if item_mst_data else None

            if extracted_screen_size and mst_screen_size:
                if extracted_screen_size != mst_screen_size:
                    print(f"  [WARNING] screen_size mismatch: extracted='{extracted_screen_size}', tv_item_mst='{mst_screen_size}'")
                    self.screen_size_mismatch_records.append({
                        'item': sku_model,
                        'url': url,
                        'extracted': extracted_screen_size,
                        'mst_value': mst_screen_size
                    })
                screen_size = extracted_screen_size
            elif extracted_screen_size:
                screen_size = extracted_screen_size
            elif mst_screen_size:
                screen_size = mst_screen_size
                print(f"  [INFO] Using screen_size from tv_item_mst: {mst_screen_size}")
            else:
                screen_size = None

            # Extract detailed reviews with retry if count_of_reviews >= 1 but no content
            detailed_review_content = self.extract_detailed_reviews(count_of_reviews)

            # Retry logic: if count_of_reviews >= 1 but detailed_review_content is empty
            cor_int_check = int(str(count_of_reviews).replace(',', '')) if count_of_reviews else 0
            if cor_int_check >= 1 and not detailed_review_content:
                max_drv_retries = 3
                for drv_retry in range(max_drv_retries):
                    print(f"  [RETRY {drv_retry + 1}/{max_drv_retries}] count_of_reviews={cor_int_check} but detailed_review_content is empty, retrying...")
                    # Navigate back to product page and retry
                    self.page.get(url)
                    captcha_after_wait = self.config.get_timing_range('captcha_after_wait') or (3, 5)
                    time.sleep(random.uniform(*captcha_after_wait))
                    detailed_review_content = self.extract_detailed_reviews(count_of_reviews)
                    if detailed_review_content:
                        break

            data = {
                'page_type': page_type,
                'product_url': url,
                'Retailer_SKU_Name': retailer_sku_name,
                'item': sku_model,  # Changed from 'Sku' to 'item'
                'Star_Rating': star_rating,
                'Number_of_ppl_purchased_yesterday': purchased_yesterday,
                'Number_of_ppl_added_to_carts': added_to_carts,
                'SKU_Popularity': sku_popularity,
                'Savings': savings,
                'Discount_Type': final_discount_type,  # Changed to use final_discount_type
                'Shipping_Info': shipping_info,
                'offer': offer,
                'Count_of_Star_Ratings': count_of_star_ratings,
                'Retailer_SKU_Name_similar': similar_products,
                'Detailed_Review_Content': detailed_review_content,
                # 11 additional columns from main/bsr tables
                'final_sku_price': final_sku_price,
                'original_sku_price': original_sku_price,
                'pick_up_availability': pick_up_availability,
                'shipping_availability': shipping_availability,
                'delivery_availability': delivery_availability,
                'sku_status': sku_status,
                'retailer_membership_discounts': retailer_membership_discounts,
                'available_quantity_for_purchase': available_quantity_for_purchase,
                'inventory_status': inventory_status,
                'main_rank': main_rank,
                'bsr_rank': bsr_rank,
                'screen_size': screen_size,
                'count_of_reviews': count_of_reviews
            }

            # Check for drv_20_error: detailed reviews under-collected
            # If count_of_reviews <= 20: should collect all (collected_count == count_of_reviews)
            # If count_of_reviews > 20: should collect at least 20 (collected_count >= 20)
            try:
                cor_int = int(str(count_of_reviews).replace(',', '')) if count_of_reviews else 0
                collected_count = 0
                if detailed_review_content:
                    collected_count = len([r for r in detailed_review_content.split(', ') if r.startswith('review')])

                has_error = False
                expected_count = 0

                if cor_int > 0 and cor_int <= 20:
                    # Should collect all reviews
                    expected_count = cor_int
                    if collected_count < cor_int:
                        has_error = True
                elif cor_int > 20:
                    # Should collect at least 20 reviews
                    expected_count = 20
                    if collected_count < 20:
                        has_error = True

                if has_error:
                    print(f"  [WARNING] drv_20_error detected: expected={expected_count}, collected={collected_count}, total_reviews={cor_int}")
                    print(f"            URL: {url}")
                    self.drv_20_error_records.append({
                        'url': url,
                        'count_of_reviews': cor_int,
                        'collected_count': collected_count,
                        'expected_count': expected_count
                    })
            except Exception as e:
                print(f"  [WARNING] drv_20_error check failed: {str(e)[:100]}")

            # Save to database with retry
            max_db_retries = 3
            db_saved = False
            for db_retry in range(max_db_retries):
                if self.save_to_db(data):
                    db_saved = True
                    break
                elif db_retry < max_db_retries - 1:
                    print(f"  [RETRY {db_retry + 1}/{max_db_retries}] DB save failed, retrying...")
                    time.sleep(1)

            if db_saved:
                self.total_collected += 1
                print(f"  [OK] Collected: {retailer_sku_name[:50] if retailer_sku_name else '[NO NAME]'}...")
                print(f"       Model: {sku_model or 'N/A'} | Screen: {screen_size or 'N/A'} | Star: {star_rating or 'N/A'}")
                print(f"       Ratings: {count_of_star_ratings or 'N/A'} | Reviews: {count_of_reviews or 'N/A'} | Purchased Yesterday: {purchased_yesterday or 'N/A'} | Added to Carts: {added_to_carts or 'N/A'}")
                print(f"       Savings: {savings or 'N/A'} | Discount: {discount_type or 'N/A'}")
                print(f"       Popularity: {sku_popularity or 'N/A'}")

                if detailed_review_content:
                    review_count = len([r for r in detailed_review_content.split(', ') if r.startswith('review')])
                    print(f"       Reviews: {review_count} collected")

                return True
            else:
                print(f"  [FAILED] Could not save data after {max_db_retries} retries")
                return False

        except Exception as e:
            print(f"  [ERROR] Failed to scrape detail page: {e}")
            import traceback
            traceback.print_exc()
            return False

    def save_to_db(self, data):
        """Save collected data to database"""
        cursor = None
        try:
            # If star_rating is "No ratings yet", set count_of_reviews and count_of_star_ratings to 0
            if data.get('Star_Rating') == "No ratings yet":
                data['count_of_reviews'] = 0
                data['Count_of_Star_Ratings'] = 0

            print(f"  [DB] Saving to database...")
            print(f"       Product: {(data.get('Retailer_SKU_Name') or 'N/A')[:60]}...")
            print(f"       Item (SKU): {data.get('item', 'N/A')}")

            # Get account_name from config
            account_name = self.config.get_constant('account_name', default='Walmart')

            # Temporarily disable autocommit for transaction
            self.db_conn.autocommit = False

            cursor = self.db_conn.cursor()

            # Calculate calendar week
            calendar_week = f"w{datetime.now().isocalendar().week}"

            # Calculate crawl_datetime (format: 2025-11-05 11:00:55)
            now = datetime.now()
            crawl_datetime = now.strftime('%Y-%m-%d %H:%M:%S')

            # Insert to Walmart_tv_detail_crawled
            cursor.execute("""
                INSERT INTO Walmart_tv_detail_crawled
                (page_type, product_url, Retailer_SKU_Name, item, Star_Rating,
                 Number_of_ppl_purchased_yesterday, Number_of_ppl_added_to_carts,
                 SKU_Popularity, Savings, Discount_Type, Shipping_Info,
                 Count_of_Star_Ratings, Retailer_SKU_Name_similar, Detailed_Review_Content,
                 calendar_week, crawl_datetime,
                 final_sku_price, original_sku_price, pick_up_availability,
                 shipping_availability, delivery_availability, sku_status,
                 retailer_membership_discounts, available_quantity_for_purchase,
                 inventory_status, main_rank, bsr_rank, screen_size, count_of_reviews)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                data['page_type'],  # Changed from 'mother'
                data['product_url'],
                data['Retailer_SKU_Name'],
                data['item'],  # Changed from 'Sku'
                data['Star_Rating'],
                data['Number_of_ppl_purchased_yesterday'],
                data['Number_of_ppl_added_to_carts'],
                data['SKU_Popularity'],
                data['Savings'],
                data['Discount_Type'],
                data['Shipping_Info'],
                data['Count_of_Star_Ratings'],
                data['Retailer_SKU_Name_similar'],
                data['Detailed_Review_Content'],
                calendar_week,
                crawl_datetime,  # New field
                # 11 additional columns
                data['final_sku_price'],
                data['original_sku_price'],
                data['pick_up_availability'],
                data['shipping_availability'],
                data['delivery_availability'],
                data['sku_status'],
                data['retailer_membership_discounts'],
                data['available_quantity_for_purchase'],
                data['inventory_status'],
                data['main_rank'],
                data['bsr_rank'],
                data['screen_size'],
                data['count_of_reviews']
            ))

            # Also insert into unified tv_retail_com table
            # Ensure count_of_reviews is integer
            count_of_reviews_int = None
            if data['count_of_reviews'] is not None:
                try:
                    count_of_reviews_int = int(data['count_of_reviews']) if isinstance(data['count_of_reviews'], int) else int(str(data['count_of_reviews']).replace(',', ''))
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
                 main_rank, bsr_rank, trend_rank, rank_1, rank_2, promotion_position,
                 number_of_ppl_purchased_yesterday, number_of_ppl_added_to_carts, number_of_units_purchased_past_month, retailer_sku_name_similar,
                 estimated_annual_electricity_use, promotion_type, model_year,
                 calendar_week, crawl_datetime)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                data['item'],
                account_name,
                data['page_type'],
                count_of_reviews_int,  # Converted to integer
                data['Retailer_SKU_Name'],
                data['product_url'],
                data['Star_Rating'],
                data['Count_of_Star_Ratings'],  # Now integer (total count)
                data['screen_size'],
                data['SKU_Popularity'],
                data['final_sku_price'],
                data['original_sku_price'],
                data['Savings'],
                data['Discount_Type'],
                data.get('offer'),  # offer
                data['pick_up_availability'],
                data['shipping_availability'],
                data['delivery_availability'],
                data['Shipping_Info'],
                data['available_quantity_for_purchase'],
                data['inventory_status'],
                data['sku_status'],
                data['retailer_membership_discounts'],
                data['Detailed_Review_Content'],
                None,  # summarized_review_content (Walmart doesn't have this)
                None,  # top_mentions (Walmart doesn't have this)
                None,  # recommendation_intent (Walmart doesn't have this)
                data['main_rank'],
                data['bsr_rank'],
                None,  # trend_rank (Walmart doesn't have this)
                None,  # rank_1 (Walmart doesn't have this)
                None,  # rank_2 (Walmart doesn't have this)
                None,  # promotion_position (Walmart doesn't have this)
                data['Number_of_ppl_purchased_yesterday'],
                data['Number_of_ppl_added_to_carts'],
                None,  # number_of_units_purchased_past_month (Walmart doesn't have this)
                data['Retailer_SKU_Name_similar'],
                None,  # estimated_annual_electricity_use (Walmart doesn't have this)
                None,  # promotion_type (Walmart doesn't have this)
                self.extract_model_year(data['Retailer_SKU_Name']),  # model_year from product name
                calendar_week,
                crawl_datetime
            ))

            # Insert into tv_item_mst (update screen_size on conflict)
            # Extract SKU from product name
            sku = self.extract_sku(data['Retailer_SKU_Name'])

            if data.get('item'):
                cursor.execute("""
                    INSERT INTO tv_item_mst (item, product_url, sku, account_name, screen_size)
                    VALUES (%s, %s, %s, %s, %s)
                    ON CONFLICT (item) DO UPDATE SET
                        screen_size = COALESCE(tv_item_mst.screen_size, EXCLUDED.screen_size)
                """, (
                    data['item'],
                    data['product_url'],
                    sku,
                    account_name,
                    data.get('screen_size')
                ))
                print(f"  [DB] ✓ tv_item_mst upsert (item: {data['item']}, sku: {sku}, screen_size: {data.get('screen_size')})")

            # Commit transaction
            self.db_conn.commit()

            cursor.close()

            # Re-enable autocommit
            self.db_conn.autocommit = True

            print(f"  [DB] ✓ Successfully saved to Walmart_tv_detail_crawled + tv_retail_com + tv_item_mst")
            print(f"       SKU: {sku}")

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
            print("Walmart TV Detail Page Crawler - Starting")
            print("="*80)

            # Connect to database
            if not self.connect_db():
                return

            # Load XPaths
            if not self.load_xpaths():
                return

            # Load product URLs
            product_urls = self.load_product_urls()
            if not product_urls:
                print("[ERROR] No product URLs found")
                return

            print(f"[INFO] Loaded {len(product_urls)} product URLs to process")

            # Setup DrissionPage browser
            self.setup_driver()

            # Initialize session (visit Walmart homepage first to avoid bot detection)
            if not self.initialize_session():
                print("[WARNING] Session initialization had issues, continuing anyway...")

            # Scrape each detail page
            for idx, url_data in enumerate(product_urls, 1):
                # Check if we've reached the maximum SKU limit
                if self.total_collected >= self.max_skus:
                    print(f"\n{'='*80}")
                    print(f"[INFO] Reached maximum SKU limit ({self.max_skus})")
                    print(f"[INFO] Stopping collection. Total collected: {self.total_collected}")
                    break

                print(f"\n{'='*80}")
                print(f"Processing {idx}/{len(product_urls)}")

                # Retry logic: try up to 3 times
                max_retries = 3
                success = False
                for attempt in range(max_retries):
                    result = self.scrape_detail_page(url_data)
                    if result:  # Success
                        success = True
                        break
                    else:  # Failed
                        if attempt < max_retries - 1:  # Not the last attempt
                            print(f"  [RETRY] Attempt {attempt + 1} failed, retrying... ({attempt + 2}/{max_retries})")
                            api_retry_wait = self.config.get_timing_range('api_retry_wait', 'wmart_tv_dt1') or (5, 8)
                            time.sleep(random.uniform(*api_retry_wait))  # Wait before retry
                        else:
                            print(f"  [FAILED] All {max_retries} attempts failed for this URL")

                # Random delay between requests
                captcha_after_wait = self.config.get_timing_range('captcha_after_wait') or (3, 5)
                time.sleep(random.uniform(*captcha_after_wait))

            print("\n" + "="*80)
            print(f"Detail Crawling completed! Total collected: {self.total_collected}/{len(product_urls)}")
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
                    WHERE account_name = 'Walmart'
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

                monitor_and_alert('walmart', len(product_urls), results_df,
                                 drv_20_error_records=self.drv_20_error_records,
                                 screen_size_mismatch_records=self.screen_size_mismatch_records)
            except Exception as e:
                print(f"[WARNING] Failed to send alert: {e}")

        except Exception as e:
            print(f"[ERROR] Crawler failed: {e}")
            import traceback
            traceback.print_exc()

        finally:
            if self.page:
                self.page.quit()
            if self.db_conn:
                self.db_conn.close()
            print("\n[INFO] Crawler terminated")


if __name__ == "__main__":
    # 30일 지난 로그 파일 삭제
    log_dir = "C:\\samsung_dx_retail_com\\log"
    if os.path.exists(log_dir):
        for filename in os.listdir(log_dir):
            if filename.endswith(".txt"):
                try:
                    file_date = datetime.strptime(filename[:8], "%Y%m%d")
                    if (datetime.now() - file_date).days > 30:
                        os.remove(os.path.join(log_dir, filename))
                        print(f"[INFO] Deleted old log: {filename}")
                except:
                    pass

    # 로그 파일 설정 (실행 시작 일시)
    log_filename = "C:\\samsung_dx_retail_com\\log\\" + datetime.now().strftime("%Y%m%d_%H%M%S") + ".txt"
    tee = Tee(log_filename)
    sys.stdout = tee

    try:
        crawler = WalmartDetailCrawler()
        crawler.run()
    except Exception as e:
        print(f"\n[FATAL ERROR] {e}")
        import traceback
        traceback.print_exc()

    print("\n[INFO] Crawler terminated. Exiting...")
    print(f"[INFO] Log saved to: {log_filename}")

    # 로그 파일 닫기
    sys.stdout = tee.terminal
    tee.close()
