"""
Walmart TV Detail Page Crawler (Part 1)
Collects detailed product information from URLs stored in:
- wmart_tv_main_1 (mother='main', pages 1-5)
- wmart_tv_main_2 (mother='main', pages 6-10)
- wmart_tv_bsr_crawl (mother='bsr')
"""
import time
import random
import psycopg2
from datetime import datetime, timedelta
import undetected_chromedriver as uc
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.action_chains import ActionChains
from lxml import html
import re

# Import database configuration
from config import DB_CONFIG

class WalmartDetailCrawler:
    def __init__(self):
        self.driver = None
        self.wait = None
        self.db_conn = None
        self.xpaths = {}
        self.total_collected = 0
        self.max_skus = 300  # Maximum SKUs to collect (final limit)

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
        """Setup Chrome WebDriver with undetected-chromedriver"""
        options = uc.ChromeOptions()

        # Set page load strategy to 'none' - don't wait for full page load
        options.page_load_strategy = 'none'

        # Basic options
        options.add_argument('--disable-dev-shm-usage')
        options.add_argument('--no-sandbox')
        options.add_argument('--disable-gpu')
        options.add_argument('--window-size=1920,1080')
        options.add_argument('--lang=en-US,en;q=0.9')

        # Preferences
        prefs = {
            "profile.default_content_setting_values.notifications": 2,
            "credentials_enable_service": False,
            "profile.password_manager_enabled": False,
        }
        options.add_experimental_option("prefs", prefs)

        # Use undetected_chromedriver
        self.driver = uc.Chrome(options=options)
        self.driver.set_page_load_timeout(120)  # Increased to 120 seconds as backup
        self.wait = WebDriverWait(self.driver, 20)

        print("[OK] WebDriver setup complete")

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

            # If no rating found, check for "No ratings yet" at specific location
            no_ratings_xpaths = [
                "//*[@id='maincontent']/section/main/div[2]/div[2]/div/div[2]/div/div[2]/div/div/div[2]/div/div/span",
                "//span[@class='gray f7 ph1 pt1'][contains(text(), 'No ratings yet')]",
                "//span[contains(text(), 'No ratings yet')]",
                "//*[@id='item-review-section']//span[contains(text(), 'No ratings yet')]"
            ]

            for xpath in no_ratings_xpaths:
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
            # Try to find the main product price container
            # Common containers: product details section, hero section, main content
            container_xpaths = [
                '//div[@data-testid="price-wrap"]',
                '//div[contains(@class, "prod-PriceSection")]',
                '//div[contains(@class, "price-section")]',
                '//section[contains(@class, "product-price")]',
                '//div[@id="product-details"]//div[contains(@class, "price")]'
            ]

            for xpath in container_xpaths:
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

            # Try multiple XPath strategies (relative to container if available)
            # NOTE: "Starting from" is checked in Fallback 2 as LAST RESORT
            xpaths = [
                './/span[@itemprop="price"][@data-seo-id="hero-price"]',
                './/span[@itemprop="price"]',
                './/span[@data-seo-id="hero-price"]',
                './/span[contains(@class, "price-wrap")]//span[contains(text(), "$")]'
            ]

            for xpath in xpaths:
                try:
                    elements = search_context.xpath(xpath)
                    if elements:
                        text = elements[0].text_content().strip()

                        # Check for "See price in cart" first
                        if "See price in cart" in text:
                            print(f"       Final Price: See price in cart")
                            return "See price in cart"

                        # Extract dollar price (e.g., "Now $238.00" -> "$238.00")
                        price_match = re.search(r'\$[\d,]+\.?\d*', text)
                        if price_match:
                            print(f"       Final Price: {price_match.group(0)}")
                            return price_match.group(0)
                except:
                    continue

            # Fallback 1: Look for "See price in cart" at specific locations
            see_price_xpaths = [
                '//*[@id="maincontent"]/section/main/div[2]/div[2]/div/div[3]/div/div[1]/div/div[2]/div/div/div[1]/span[2]/span[2]/span',
                '//span[@itemprop="price"][@data-seo-id="hero-price"][contains(text(), "See price in cart")]',
                '//span[@data-seo-id="hero-price"][contains(text(), "See price in cart")]',
                '//span[contains(text(), "See price in cart")]'
            ]

            for xpath in see_price_xpaths:
                try:
                    elements = tree.xpath(xpath)
                    if elements:
                        text = elements[0].text_content().strip()
                        if "See price in cart" in text:
                            print(f"       Final Price: See price in cart")
                            return "See price in cart"
                except:
                    continue

            # Fallback 2: Look for "Starting from $X" at specific locations
            starting_from_xpaths = [
                '//*[@id="maincontent"]/section/main/div[2]/div[2]/div/div[3]/div/div[2]/div/div/div[1]/section/div/div/div/div/div[1]/span[1]',
                '//span[contains(text(), "Starting from")]',
                '//div[contains(@class, "flex")]//span[contains(text(), "Starting from")]'
            ]

            for xpath in starting_from_xpaths:
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

            return None
        except Exception as e:
            return None

    def extract_original_price(self, tree, savings):
        """Extract original/strike-through price from detail page (container-based)
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

            # Try to get price container first
            price_container = self.get_price_container(tree)
            search_context = price_container if price_container is not None else tree

            # Try multiple XPath strategies (relative to container if available)
            xpaths = [
                './/span[@data-seo-id="strike-through-price"]',
                './/span[contains(@class, "strike")]',
                './/span[@aria-hidden="true"][contains(@class, "strike")]',
                './/del//span[contains(text(), "$")]'
            ]

            for xpath in xpaths:
                try:
                    elements = search_context.xpath(xpath)
                    if elements:
                        text = elements[0].text_content().strip()
                        # Extract price (e.g., "$298.00")
                        price_match = re.search(r'\$[\d,]+\.?\d*', text)
                        if price_match:
                            print(f"       Original Price: {price_match.group(0)} (with savings: {savings})")
                            return price_match.group(0)
                except:
                    continue

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
        """Extract star rating counts in format '5star:489, 4star:102, 3star:28, 2star:19, 1star:47'"""
        try:
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

                # Method 1: Extract from "X% (Y)" pattern in span text
                # Example: <span>71% (489)</span>
                try:
                    # Find the button for this star rating
                    star_button_xpath = f"//button[@aria-label[contains(., '{star_num} star')]]"
                    star_buttons = tree.xpath(star_button_xpath)

                    if star_buttons:
                        # Look for pattern "X% (Y)" inside this button
                        percentage_spans = star_buttons[0].xpath(".//span[contains(text(), '% (')]")
                        if percentage_spans:
                            text = percentage_spans[0].text_content().strip()
                            # Extract number from "71% (489)"
                            match = re.search(r'\((\d+)\)', text)
                            if match:
                                count = int(match.group(1))
                except:
                    pass

                # Method 2: Extract from aria-label
                # Example: "489 ratings are rated 5 stars, 71% of all ratings"
                if count is None:
                    try:
                        aria_xpath = f"//button[@aria-label[contains(., '{star_num} star')]]/@aria-label"
                        aria_labels = tree.xpath(aria_xpath)
                        if aria_labels:
                            aria_text = aria_labels[0]
                            # Extract first number (which is the count)
                            match = re.search(r'(\d+)\s+ratings?\s+are\s+rated', aria_text)
                            if match:
                                count = int(match.group(1))
                    except:
                        pass

                # Method 3: Calculate from percentage if total_count available
                if count is None and total_count:
                    try:
                        star_button_xpath = f"//button[@aria-label[contains(., '{star_num} star')]]"
                        star_buttons = tree.xpath(star_button_xpath)

                        if star_buttons:
                            # Look for percentage in span text
                            percentage_spans = star_buttons[0].xpath(".//span[contains(text(), '%')]")
                            if percentage_spans:
                                text = percentage_spans[0].text_content().strip()
                                # Extract percentage from "71% (489)" or just "71%"
                                match = re.search(r'(\d+)%', text)
                                if match:
                                    percentage = int(match.group(1))
                                    count = round(total_count * percentage / 100.0)
                    except:
                        pass

                # Store the count
                if count is not None:
                    star_counts[star_num] = count

            # Format as "5star:X, 4star:X, ..."
            if star_counts:
                result_parts = []
                for star_num in range(5, 0, -1):
                    if star_num in star_counts:
                        result_parts.append(f"{star_num}star:{star_counts[star_num]}")

                if result_parts:
                    return ', '.join(result_parts)

            return None

        except Exception as e:
            print(f"  [WARNING] Failed to extract star rating counts: {e}")
            return None

    def extract_shipping_info(self, tree):
        """Combine two shipping info parts"""
        try:
            part1 = self.extract_text_safe(tree, self.xpaths.get('shipping_info_1'))
            part2 = self.extract_text_safe(tree, self.xpaths.get('shipping_info_2'))

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
            badge_xpaths = [
                # Main product badges section (most reliable)
                '//main//div[@data-testid="module-2-badges"]//span[@data-testid="badgeTagComponent"]//span',
                # Fallback 1: Restrict to main product div (before similar products)
                '//main/div[1]//div[@data-testid="badgeTagComponent"]//span',
                # Fallback 2: Only badges in the product title/info area (not in similar items grid)
                '//section[@data-testid="product-info-section"]//span[@data-testid="badgeTagComponent"]//span',
                # Fallback 3: Exclude similar items by using ancestor check
                '//main/section[1]//div[@data-testid="badgeTagComponent"]//span'
            ]

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

            # Method 2 (Fallback): Try XPath from Specifications at a glance
            xpaths = [
                # Method 1: Definition list structure - <dl><dt>Screen size</dt><dd>75 in</dd></dl> (main page)
                "//dl[.//dt[contains(., 'Screen size')]]//dd",
                # Method 2: Definition list - direct sibling (main page)
                "//dt[contains(., 'Screen size')]/following-sibling::dd",
                # Method 3: Table structure - <tr><th><dt>Screen size</dt></th><td><dd>75 in</dd></td></tr>
                "//tr[.//dt[contains(text(), 'Screen size')]]//dd",
                # Method 4: Alternative table structure
                "//dt[contains(text(), 'Screen size')]/ancestor::tr//dd",
                # Method 5: Use aria-label (most reliable for div structure)
                "//div[@aria-label[contains(., 'Screen size:')]]/@aria-label",
                # Method 6: Find "Screen size" text and get the next sibling div
                "//div[contains(@class, 'b') and contains(., 'Screen size')]/following-sibling::div//span",
                # Method 7: Direct XPath provided by user (old structure)
                "//*[@id='ip-prod-desc-atf-div-1']/section/section[2]/div/div/div[1]/div[1]/div/div/div[2]/span",
                # Method 8: Find within "Specifications at a glance" container
                "//h3[contains(text(), 'Specifications at a glance')]/parent::div//div[@aria-label[contains(., 'Screen size')]]/@aria-label"
            ]

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

    def extract_count_of_reviews(self, tree, star_rating=None, page_source=None):
        """Extract total number of reviews from main page
        Example: '248 reviews' -> 248, '43 ratings' -> 43, 'No ratings yet' -> 0

        Priority:
        1. Check "No ratings yet" first -> return 0
        2. JSON data: totalReviewCount (exact value, not displayed on screen)
        3. JSON data: numberOfReviews (alternative exact value)
        4. XPath fallback (may get approximate values like '28K')

        Args:
            tree: HTML tree
            star_rating: Star rating value (if "No ratings yet", return 0)
            page_source: Raw HTML page source for JSON extraction
        """
        try:
            # Check 1: If star_rating is "No ratings yet", return 0 immediately
            if star_rating and "No ratings yet" in str(star_rating):
                print(f"  [INFO] Star rating is 'No ratings yet', setting count_of_reviews to 0")
                return 0

            # Check 2: Look for "No ratings yet" in page source before JSON extraction
            if page_source and "No ratings yet" in page_source:
                # Verify it's actually displayed (not just in some config)
                no_ratings_xpaths = [
                    "//span[contains(text(), 'No ratings yet')]",
                    "//*[@id='item-review-section']//span[contains(text(), 'No ratings yet')]"
                ]
                for xpath in no_ratings_xpaths:
                    result = tree.xpath(xpath)
                    if result:
                        print(f"  [INFO] Found 'No ratings yet' on page, setting count_of_reviews to 0")
                        return 0

            # Method 1: Extract from JSON data (most accurate - gets exact count like 28040)
            if page_source:
                # Try totalReviewCount first (exact total review count)
                match = re.search(r'"totalReviewCount":(\d+)', page_source)
                if match:
                    count = int(match.group(1))
                    print(f"  [INFO] Extracted totalReviewCount from JSON: {count}")
                    return count

                # Try numberOfReviews as alternative
                match = re.search(r'"numberOfReviews":(\d+)', page_source)
                if match:
                    count = int(match.group(1))
                    print(f"  [INFO] Extracted numberOfReviews from JSON: {count}")
                    return count

            # Method 2: XPath fallback for pages without JSON data
            xpaths = [
                # Method 1: Direct XPath provided by user
                "//*[@id='item-review-section']/div[2]/div[1]/div[1]/div/a",
                # Method 2: Find link with 'seeAllReviewsStarRating' identifier
                "//a[@link-identifier='seeAllReviewsStarRating']",
                # Method 3: Find link with 'reviewsLink' identifier (for ratings)
                "//a[@link-identifier='reviewsLink']",
                # Method 4: Find link containing 'reviews' text in item-review-section
                "//*[@id='item-review-section']//a[contains(text(), 'reviews')]",
                # Method 5: Find link containing 'ratings' text in item-review-section
                "//*[@id='item-review-section']//a[contains(text(), 'ratings')]",
                # Method 6: Any link in review section containing 'review' in text
                "//div[@id='item-review-section']//a[contains(., 'review')]",
                # Method 7: Any link in review section containing 'rating' in text
                "//div[@id='item-review-section']//a[contains(., 'rating')]"
            ]

            review_text = None
            for xpath in xpaths:
                result = tree.xpath(xpath)
                if result:
                    if isinstance(result[0], str):
                        review_text = result[0].strip()
                    else:
                        review_text = result[0].text_content().strip() if hasattr(result[0], 'text_content') else str(result[0]).strip()

                    if review_text:
                        break

            # Check for "No ratings yet" first -> return 0
            if review_text and "No ratings yet" in review_text:
                return 0

            if not review_text:
                # Try to find "No ratings yet" at specific location -> return 0
                no_ratings_xpaths = [
                    "//*[@id='maincontent']/section/main/div[2]/div[2]/div/div[2]/div/div[2]/div/div/div[2]/div/div/span",
                    "//span[@class='gray f7 ph1 pt1'][contains(text(), 'No ratings yet')]",
                    "//span[contains(text(), 'No ratings yet')]",
                    "//*[@id='item-review-section']//span[contains(text(), 'No ratings yet')]"
                ]

                for xpath in no_ratings_xpaths:
                    result = tree.xpath(xpath)
                    if result:
                        text = result[0].text_content().strip() if hasattr(result[0], 'text_content') else str(result[0]).strip()
                        if "No ratings yet" in text:
                            return 0

                return None

            # Extract number from text (fallback when JSON extraction fails)
            match = re.search(r'([\d,.k]+)\s*(reviews?|ratings?)', review_text, re.IGNORECASE)
            if match:
                number_str = match.group(1)
                print(f"  [INFO] Using XPath fallback for review count: {number_str}")
                # Use parse_number_format to handle commas, decimals, and 'k' suffix
                return self.parse_number_format(number_str)

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
        """Extract SKU from retailer_sku_name"""
        try:
            if not product_name:
                return None

            # Pattern 1: Comma-separated at the end (e.g., "... , S32VAFW")
            if ',' in product_name:
                parts = product_name.split(',')
                last_part = parts[-1].strip()
                # Check if last part looks like a model (letters + numbers, no spaces)
                if last_part and not ' ' in last_part:
                    has_letter = any(c.isalpha() for c in last_part)
                    has_number = any(c.isdigit() for c in last_part)
                    if has_letter and has_number and 5 <= len(last_part) <= 20:
                        return last_part

            # Pattern 2: In parentheses (e.g., "... (85QD6N)")
            import re
            paren_match = re.search(r'\(([A-Z0-9]+)\)', product_name)
            if paren_match:
                model = paren_match.group(1)
                if 5 <= len(model) <= 20:
                    return model

            # Pattern 3: At the end after space (e.g., "... UN65DU8000")
            words = product_name.split()
            if words:
                last_word = words[-1].strip('.,;:')
                # Check if it looks like a model
                has_letter = any(c.isalpha() for c in last_word)
                has_number = any(c.isdigit() for c in last_word)
                if has_letter and has_number and 5 <= len(last_word) <= 20:
                    # Make sure it's not a common word
                    if last_word.upper() not in ['HD', 'UHD', 'LED', 'LCD', '4K', 'TV']:
                        return last_word

            return None

        except Exception as e:
            print(f"  [DEBUG] Failed to extract SKU from product name: {e}")
            return None

    def extract_sku_from_lg_xpath(self):
        """Extract SKU using LG-specific XPath (from main page)"""
        try:
            page_source = self.driver.page_source
            tree = html.fromstring(page_source)

            # Try multiple LG-specific XPaths
            lg_xpaths = [
                '//div[@class="flix-model-name"]',  # New: flix-model-name class
                '//*[@id="inpage_container"]/div[2]/div/div/div/div[1]',  # Original
                '//div[contains(@class, "flix-model-name")]'  # Flexible
            ]

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
            current_url = self.driver.current_url

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

    def extract_detailed_reviews(self):
        """Click 'View all reviews' and extract up to 20 reviews"""
        try:
            # Find and click "View all reviews" button
            try:
                # Scroll to reviews section first
                self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                time.sleep(2)

                # Try multiple XPaths to find the button (there might be 2 on the page)
                view_all_xpaths = [
                    # Button with review count in text
                    "//button[contains(text(), 'View all reviews') and @data-dca-intent='select']",
                    # Any button with "View all reviews" text
                    "//button[contains(text(), 'View all reviews')]",
                    # Database XPath as fallback
                    self.xpaths.get('view_all_reviews_button')
                ]

                view_all_btn = None
                for xpath in view_all_xpaths:
                    if xpath:
                        try:
                            buttons = self.driver.find_elements(By.XPATH, xpath)
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
                    return None

                # Scroll to button with offset to avoid header
                self.driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", view_all_btn)
                time.sleep(1)

                # Use JavaScript click to avoid interception
                self.driver.execute_script("arguments[0].click();", view_all_btn)
                time.sleep(random.uniform(3, 4))
            except Exception as e:
                print(f"  [WARNING] Could not click View all reviews: {e}")
                return None

            # Extract reviews from multiple pages (up to 20 reviews)
            reviews = []
            page_num = 1
            max_pages = 2  # We'll collect from 2 pages to get 20 reviews

            while len(reviews) < 20 and page_num <= max_pages:
                # Get current page HTML
                page_source = self.driver.page_source
                tree = html.fromstring(page_source)

                # Get review containers
                # Find all review containers using data-testid attribute
                review_content_divs = tree.xpath('//div[@data-testid="enhanced-review-content"]')

                if not review_content_divs:
                    print(f"  [WARNING] No review content divs found on page {page_num}")
                    break

                # Extract reviews from current page
                for idx, content_div in enumerate(review_content_divs):
                    if len(reviews) >= 20:
                        break

                    # Extract review text
                    review_xpath = './/p/span[@class="tl-m db-m"]'
                    review_elem = content_div.xpath(review_xpath)

                    if review_elem:
                        review_text = review_elem[0].text_content().strip() if hasattr(review_elem[0], 'text_content') else str(review_elem[0]).strip()
                        if review_text and len(review_text) > 10:
                            reviews.append(review_text)

                # If we need more reviews and haven't reached max pages, click Next Page
                if len(reviews) < 20 and page_num < max_pages:
                    try:
                        # Find Next Page button using data-testid
                        next_page_btn = self.driver.find_element(By.XPATH, "//a[@data-testid='NextPage']")

                        # Scroll to button
                        self.driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", next_page_btn)
                        time.sleep(1)

                        # Click Next Page
                        self.driver.execute_script("arguments[0].click();", next_page_btn)

                        # Wait for next page to load
                        time.sleep(random.uniform(3, 4))
                        page_num += 1
                    except Exception as e:
                        print(f"  [WARNING] Could not find or click Next Page button: {e}")
                        break
                else:
                    break

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
                _ = self.driver.current_url
            except Exception as e:
                print(f"  [WARNING] Browser window crashed, restarting driver...")
                try:
                    self.driver.quit()
                except:
                    pass
                self.setup_driver()
                print(f"  [OK] Driver restarted successfully")

            print(f"  [INFO] Loading page...")
            self.driver.get(url)
            time.sleep(random.uniform(4, 6))

            print(f"  [INFO] Page loaded, extracting data...")

            page_source = self.driver.page_source
            tree = html.fromstring(page_source)

            # Extract basic data using XPaths (from initial page load)
            retailer_sku_name = self.extract_text_safe(tree, self.xpaths.get('product_name'))
            star_rating = self.extract_star_rating(tree, page_source)
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

            # Extract count of star ratings
            count_of_star_ratings = self.extract_count_of_star_ratings(tree)

            # Extract similar products
            similar_products = self.extract_similar_products(tree)

            # Extract screen size (from main page, with fallback to product name)
            screen_size = self.extract_screen_size(tree, retailer_sku_name)

            # Extract count of reviews (from main page, BEFORE navigating to reviews)
            # Pass star_rating and page_source for JSON extraction
            count_of_reviews = self.extract_count_of_reviews(tree, star_rating, page_source)

            # Click Specifications and get Model (after static content extraction)
            sku_model = self.click_specifications_and_get_model()

            # Extract detailed reviews (this will navigate to reviews page) - LAST
            detailed_review_content = self.extract_detailed_reviews()

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

            # Save to database
            if self.save_to_db(data):
                self.total_collected += 1
                print(f"  [OK] Collected: {retailer_sku_name[:50] if retailer_sku_name else '[NO NAME]'}...")
                print(f"       Model: {sku_model or 'N/A'} | Screen: {screen_size or 'N/A'} | Star: {star_rating or 'N/A'}")
                print(f"       Total Reviews: {count_of_reviews or 'N/A'} | Purchased Yesterday: {purchased_yesterday or 'N/A'} | Added to Carts: {added_to_carts or 'N/A'}")
                print(f"       Savings: {savings or 'N/A'} | Discount: {discount_type or 'N/A'}")
                print(f"       Popularity: {sku_popularity or 'N/A'}")

                if detailed_review_content:
                    review_count = len([r for r in detailed_review_content.split(', ') if r.startswith('review')])
                    print(f"       Reviews: {review_count} collected")

                return True
            else:
                print(f"  [FAILED] Could not save data")
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
            print(f"  [DB] Saving to database...")
            print(f"       Product: {data.get('Retailer_SKU_Name', 'N/A')[:60]}...")
            print(f"       Item (SKU): {data.get('item', 'N/A')}")

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
            if data['count_of_reviews']:
                try:
                    count_of_reviews_int = int(data['count_of_reviews']) if isinstance(data['count_of_reviews'], int) else int(str(data['count_of_reviews']).replace(',', ''))
                except:
                    count_of_reviews_int = None

            # Parse count_of_star_ratings to get total count
            # Example: "5star:142, 4star:14, 3star:7, 2star:2, 1star:4" -> 169
            count_of_star_ratings_int = None
            if data['Count_of_Star_Ratings']:
                try:
                    import re
                    # Extract numbers after colons (handle both comma and space separators)
                    numbers = re.findall(r':(\d+)', str(data['Count_of_Star_Ratings']))
                    if numbers:
                        count_of_star_ratings_int = sum(int(n) for n in numbers)
                except:
                    count_of_star_ratings_int = None

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
                'Walmart',  # account_name
                data['page_type'],
                count_of_reviews_int,  # Converted to integer
                data['Retailer_SKU_Name'],
                data['product_url'],
                data['Star_Rating'],
                count_of_star_ratings_int,  # Parsed from star ratings string
                data['screen_size'],
                data['SKU_Popularity'],
                data['final_sku_price'],
                data['original_sku_price'],
                data['Savings'],
                data['Discount_Type'],
                None,  # offer (Walmart doesn't have this)
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
                None,  # model_year (Walmart doesn't have this)
                calendar_week,
                crawl_datetime
            ))

            # Commit transaction
            self.db_conn.commit()

            cursor.close()

            # Re-enable autocommit
            self.db_conn.autocommit = True

            print(f"  [DB] ✓ Successfully saved to Walmart_tv_detail_crawled + tv_retail_com")

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

            # Setup WebDriver
            self.setup_driver()

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
                            time.sleep(random.uniform(5, 8))  # Wait before retry
                        else:
                            print(f"  [FAILED] All {max_retries} attempts failed for this URL")

                # Random delay between requests
                time.sleep(random.uniform(3, 5))

            print("\n" + "="*80)
            print(f"Detail Crawling completed! Total collected: {self.total_collected}/{len(product_urls)}")
            print("="*80)

        except Exception as e:
            print(f"[ERROR] Crawler failed: {e}")
            import traceback
            traceback.print_exc()

        finally:
            if self.driver:
                self.driver.quit()
            if self.db_conn:
                self.db_conn.close()
            print("\n[INFO] Crawler terminated")


if __name__ == "__main__":
    try:
        crawler = WalmartDetailCrawler()
        crawler.run()
    except Exception as e:
        print(f"\n[FATAL ERROR] {e}")
        import traceback
        traceback.print_exc()

    print("\n[INFO] Crawler terminated. Exiting...")
