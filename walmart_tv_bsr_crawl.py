import time
import random
import psycopg2
from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError
from lxml import html
import re
from urllib.parse import urlparse, parse_qs, unquote
import json
import os

# Try to import playwright_stealth (optional)
try:
    from playwright_stealth import stealth_sync
    HAS_STEALTH = True
except ImportError:
    print("[WARNING] playwright_stealth not installed, running without stealth")
    HAS_STEALTH = False

# Database configuration
DB_CONFIG = {
    'host': 'samsung-dx-crawl.csnixzmkuppn.ap-northeast-2.rds.amazonaws.com',
    'port': 5432,
    'database': 'postgres',
    'user': 'postgres',
    'password': 'admin2025!'
}

class WalmartTVBSRCrawler:
    def __init__(self):
        self.playwright = None
        self.browser = None
        self.db_conn = None
        self.xpaths = {}
        self.total_collected = 0
        self.max_skus = 100  # BSR 1-100
        self.sequential_id = 1  # ID counter for 1-100

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
                WHERE mall_name = 'Walmart' AND page_type = 'bsr' AND is_active = TRUE
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
                FROM wmart_tv_bsr_page_url
                WHERE page_type = 'bsr' AND is_active = TRUE
                ORDER BY page_number
            """)

            urls = cursor.fetchall()
            cursor.close()
            print(f"[OK] Loaded {len(urls)} page URLs")
            return urls

        except Exception as e:
            print(f"[ERROR] Failed to load page URLs: {e}")
            return []

    def setup_browser(self):
        """Setup Playwright browser"""
        try:
            print("[INFO] Setting up Playwright browser...")
            self.playwright = sync_playwright().start()

            # Launch browser with anti-detection settings
            # headless=True for AWS, headless=False for local testing
            self.browser = self.playwright.chromium.launch(
                headless=True,
                args=[
                    '--disable-blink-features=AutomationControlled',
                    '--no-sandbox',
                    '--disable-dev-shm-usage',
                    '--disable-gpu'
                ]
            )

            print("[OK] Playwright browser setup complete")
            return True

        except Exception as e:
            print(f"[ERROR] Failed to setup browser: {e}")
            return False

    def create_new_context(self):
        """Create a new browser context (like a new incognito window)"""
        # Check if storage state file exists (includes cookies + localStorage)
        storage_state_file = 'walmart_storage_state.json'

        if os.path.exists(storage_state_file):
            print(f"[INFO] Loading saved session from {storage_state_file}")
            context = self.browser.new_context(
                storage_state=storage_state_file,
                viewport={'width': 1920, 'height': 1080},
                user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
                locale='en-US',
                timezone_id='America/New_York'
            )
        else:
            print(f"[WARNING] {storage_state_file} not found, creating context without cookies")
            context = self.browser.new_context(
                viewport={'width': 1920, 'height': 1080},
                user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
                locale='en-US',
                timezone_id='America/New_York'
            )

        # Create a new page in this context
        page = context.new_page()

        # Apply playwright-stealth for better bot detection bypass (if available)
        if HAS_STEALTH:
            try:
                stealth_sync(page)
            except Exception as e:
                print(f"[WARNING] Failed to apply stealth: {e}")

        # Add additional anti-detection scripts
        page.add_init_script("""
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
        """)

        return context, page

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

    def check_robot_page(self, page_source):
        """Check if page is showing 'Robot or human?' challenge"""
        if "Robot or human?" in page_source or "Enter the characters you see below" in page_source:
            return True
        return False

    def scrape_page(self, page, url, page_number):
        """Scrape a single page"""
        try:
            print(f"\n[PAGE {page_number}] Accessing: {url[:80]}...")

            # Directly access the best_seller URL
            page.goto(url, wait_until='domcontentloaded', timeout=60000)
            time.sleep(random.uniform(8, 12))

            # Check for robot detection (just log, don't stop)
            page_source = page.content()
            if self.check_robot_page(page_source):
                print(f"[WARNING] Robot detection on page {page_number}, but continuing anyway...")
                # Save debug info
                with open(f'walmart_robot_page_{page_number}.html', 'w', encoding='utf-8') as f:
                    f.write(page_source)
                # Continue anyway - maybe we can still extract some data

            # Wait for page to load
            print("[INFO] Waiting for products to load...")
            time.sleep(random.uniform(5, 8))

            # Scroll to load all products
            print("[INFO] Scrolling to load all products...")
            last_height = page.evaluate("document.body.scrollHeight")

            for scroll_round in range(2):
                page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                time.sleep(3)

                new_height = page.evaluate("document.body.scrollHeight")
                if new_height == last_height:
                    break
                last_height = new_height

            # Scroll back to top
            page.evaluate("window.scrollTo(0, 0)")
            time.sleep(2)

            # Save screenshot for verification
            screenshot_path = f"walmart_bsr_page_{page_number}_screenshot.png"
            page.screenshot(path=screenshot_path, full_page=True)
            print(f"[INFO] Screenshot saved: {screenshot_path}")

            # Get page source and parse with lxml
            page_source = page.content()
            tree = html.fromstring(page_source)

            # Find all product containers
            base_xpath = self.xpaths['base_container']['xpath']
            products = tree.xpath(base_xpath)

            print(f"[INFO] Found {len(products)} product containers")

            # Process each product
            collected_count = 0
            for idx, product in enumerate(products, 1):
                if self.total_collected >= self.max_skus:
                    print(f"[INFO] Reached maximum SKU limit ({self.max_skus})")
                    return False

                # Extract product name (required field)
                product_name = self.extract_text_safe(product, self.xpaths['product_name']['xpath'])

                if not product_name:
                    print(f"  [{idx}/{len(products)}] SKIP: No product name found")
                    continue

                # Extract product URL and normalize it
                product_url_raw = self.extract_text_safe(product, self.xpaths['product_url']['xpath'])
                product_url = self.normalize_product_url(product_url_raw) if product_url_raw else None

                # Extract Final_SKU_Price
                final_price_raw = self.extract_text_safe(product, self.xpaths['final_price']['xpath'])
                final_price = self.clean_price_text(final_price_raw) if final_price_raw else None

                # Extract Original_SKU_Price
                original_price_raw = self.extract_text_safe(product, self.xpaths['original_price']['xpath'])
                original_price = original_price_raw if original_price_raw else None

                # Extract Offer
                offer = self.extract_text_safe(product, self.xpaths['offer']['xpath'])

                # Extract Pick-Up_Availability
                pickup_raw = self.extract_text_safe(product, self.xpaths['pickup_availability']['xpath'])
                pickup = pickup_raw if pickup_raw else None

                # Extract Shipping_Availability
                shipping_raw = self.extract_text_safe(product, self.xpaths['shipping_availability']['xpath'])
                shipping = shipping_raw if shipping_raw else None

                # Extract Delivery_Availability
                delivery_raw = self.extract_text_safe(product, self.xpaths['delivery_availability']['xpath'])
                delivery = delivery_raw if delivery_raw else None

                # Extract SKU_Status (check both Rollback and Sponsored)
                rollback = self.extract_text_safe(product, self.xpaths['sku_status_rollback']['xpath'])
                sponsored = self.extract_text_safe(product, self.xpaths['sku_status_sponsored']['xpath'])

                sku_status = None
                if rollback:
                    sku_status = "Rollback"
                elif sponsored:
                    sku_status = "Sponsored"

                # Extract Retailer_Membership_Discounts
                membership_discount_elem = self.extract_text_safe(product, self.xpaths['membership_discount']['xpath'])
                membership_discount = "Walmart Plus" if membership_discount_elem else None

                # Extract Available_Quantity_for_Purchase
                available_quantity = self.extract_text_safe(product, self.xpaths['available_quantity']['xpath'])

                # Extract Inventory_Status
                inventory_status = self.extract_text_safe(product, self.xpaths['inventory_status']['xpath'])

                data = {
                    'page_type': 'bsr',
                    'Retailer_SKU_Name': product_name,
                    'Final_SKU_Price': final_price,
                    'Original_SKU_Price': original_price,
                    'Offer': offer,
                    'Pick_Up_Availability': pickup,
                    'Shipping_Availability': shipping,
                    'Delivery_Availability': delivery,
                    'SKU_Status': sku_status,
                    'Retailer_Membership_Discounts': membership_discount,
                    'Available_Quantity_for_Purchase': available_quantity,
                    'Inventory_Status': inventory_status,
                    'Rank': None,  # To be added later
                    'Product_url': product_url
                }

                # Save to database
                if self.save_to_db(data):
                    collected_count += 1
                    self.total_collected += 1
                    print(f"  [{idx}/{len(products)}] Collected: {data['Retailer_SKU_Name'][:50]}... | Price: {final_price or 'N/A'}")

            print(f"[PAGE {page_number}] Collected {collected_count} products (Total: {self.total_collected}/{self.max_skus})")
            return True

        except Exception as e:
            print(f"[ERROR] Failed to scrape page {page_number}: {e}")
            import traceback
            traceback.print_exc()
            return True  # Continue to next page

    def normalize_product_url(self, raw_url):
        """Normalize product URL to clean format"""
        if not raw_url:
            return None

        # Type 1: Tracking URL (/sp/track?...rd=encoded_url)
        if '/sp/track?' in raw_url:
            try:
                parsed = urlparse(raw_url)
                query_params = parse_qs(parsed.query)

                # Extract 'rd' parameter (redirect URL)
                if 'rd' in query_params:
                    redirect_url = query_params['rd'][0]
                    # Decode URL-encoded string
                    decoded_url = unquote(redirect_url)

                    # Extract clean /ip/... path from decoded URL
                    if '/ip/' in decoded_url:
                        ip_path = decoded_url.split('/ip/')[1]
                        # Remove extra parameters after product ID
                        clean_path = '/ip/' + ip_path.split('?')[0]
                        return f"https://www.walmart.com{clean_path}"
            except Exception as e:
                pass  # Fall through to Type 2 handling

        # Type 2: Relative path (/ip/...)
        if raw_url.startswith('/ip/'):
            # Remove query parameters after product ID
            clean_path = raw_url.split('?')[0]
            return f"https://www.walmart.com{clean_path}"

        # Type 3: Already full URL
        if raw_url.startswith('http'):
            # Clean up query parameters if needed
            if '/ip/' in raw_url:
                base_url = raw_url.split('?')[0]
                return base_url
            return raw_url

        return raw_url

    def clean_price_text(self, price_text):
        """Extract clean price from complex price HTML text"""
        if not price_text:
            return None

        # Remove extra whitespace and newlines
        price_text = ' '.join(price_text.split())

        # Try to extract price pattern like "$1,797 99" or "$238 00"
        # Look for dollar sign followed by numbers
        match = re.search(r'\$\s*(\d[\d,]*)\s*(\d{2})', price_text)
        if match:
            dollars = match.group(1).replace(',', '')
            cents = match.group(2)
            return f"${dollars}.{cents}"

        # Fallback: just return cleaned text
        return price_text

    def save_to_db(self, data):
        """Save collected data with collection order (1-100)"""
        try:
            cursor = self.db_conn.cursor()

            # Use sequential_id (1-100) for collection order
            collection_order = self.sequential_id

            cursor.execute("""
                INSERT INTO wmart_tv_bsr_crawl
                ("order", page_type, Retailer_SKU_Name, Final_SKU_Price, Original_SKU_Price,
                 Offer, Pick_Up_Availability, Shipping_Availability, Delivery_Availability,
                 SKU_Status, Retailer_Membership_Discounts, Available_Quantity_for_Purchase,
                 Inventory_Status, Rank, Product_url)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                RETURNING id
            """, (
                collection_order,
                data['page_type'],
                data['Retailer_SKU_Name'],
                data['Final_SKU_Price'],
                data['Original_SKU_Price'],
                data['Offer'],
                data['Pick_Up_Availability'],
                data['Shipping_Availability'],
                data['Delivery_Availability'],
                data['SKU_Status'],
                data['Retailer_Membership_Discounts'],
                data['Available_Quantity_for_Purchase'],
                data['Inventory_Status'],
                data['Rank'],
                data['Product_url']
            ))

            result = cursor.fetchone()

            if result:
                # Increment sequential ID for next product
                self.sequential_id += 1

            self.db_conn.commit()
            cursor.close()

            return result is not None

        except Exception as e:
            print(f"[ERROR] Failed to save to DB: {e}")
            return False

    def initialize_session(self, page):
        """Initialize session by visiting Walmart homepage first (optional)"""
        try:
            print("[INFO] Warming up session...")
            page.goto("https://www.walmart.com", wait_until='domcontentloaded', timeout=30000)
            time.sleep(random.uniform(3, 5))
            print("[OK] Session warmed up")
            return True
        except Exception as e:
            print(f"[WARNING] Session warmup failed: {e}, continuing anyway...")
            return True  # Don't fail, just continue

    def run(self):
        """Main execution"""
        try:
            print("="*80)
            print("Walmart TV BSR Crawler - Playwright Version")
            print("Pages will remain open for verification")
            print("="*80)

            # Connect to database
            if not self.connect_db():
                return

            # Load XPaths and URLs
            if not self.load_xpaths():
                return

            page_urls = self.load_page_urls()
            if not page_urls:
                print("[ERROR] No page URLs found")
                return

            # Setup browser
            if not self.setup_browser():
                return

            print(f"\n[INFO] Will crawl {len(page_urls)} pages")
            print(f"[INFO] Screenshots will be saved for each page")
            print(f"[INFO] Starting crawler...")

            # Process each page
            for page_number, url in page_urls:
                if self.total_collected >= self.max_skus:
                    print(f"[INFO] Reached maximum SKU limit ({self.max_skus}), stopping...")
                    break

                print(f"\n{'#'*80}")
                print(f"Processing PAGE {page_number}")
                print(f"{'#'*80}")

                # Create new context and page for this page
                context, page = self.create_new_context()

                # Warm up session only for first page
                if page_number == 1:
                    self.initialize_session(page)

                # Scrape this page
                self.scrape_page(page, url, page_number)

                # Close context after scraping (don't keep windows open)
                try:
                    context.close()
                except:
                    pass

                # Delay between pages
                time.sleep(random.uniform(5, 8))

            # All pages done
            print("\n" + "="*80)
            print(f"[INFO] Crawling complete!")
            print(f"[INFO] Total collected: {self.total_collected} SKUs")
            print(f"[INFO] Screenshots saved in current directory")
            print("="*80)

        except Exception as e:
            print(f"[ERROR] Crawler failed: {e}")
            import traceback
            traceback.print_exc()

        finally:
            # Close browser
            if self.browser:
                try:
                    self.browser.close()
                except:
                    pass

            # Stop playwright
            if self.playwright:
                try:
                    self.playwright.stop()
                except:
                    pass

            if self.db_conn:
                self.db_conn.close()


if __name__ == "__main__":
    try:
        crawler = WalmartTVBSRCrawler()
        crawler.run()
    except Exception as e:
        print(f"\n[FATAL ERROR] {e}")
        import traceback
        traceback.print_exc()

    print("\n[INFO] BSR Crawler completed. Window will close automatically...")
