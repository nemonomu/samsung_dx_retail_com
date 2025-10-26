import time
import random
import re
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

# Database configuration
DB_CONFIG = {
    'host': 'samsung-dx-crawl.csnixzmkuppn.ap-northeast-2.rds.amazonaws.com',
    'port': 5432,
    'database': 'postgres',
    'user': 'postgres',
    'password': 'admin2025!'
}

class AmazonTVCrawler:
    def __init__(self):
        self.driver = None
        self.wait = None
        self.db_conn = None
        self.xpaths = {}
        self.total_collected = 0
        self.max_skus = 300
        self.sequential_id = 1  # ID counter for 1-300
        self.batch_id = None  # Batch ID for this crawling session

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

    def setup_driver(self):
        """Setup Chrome WebDriver"""
        chrome_options = Options()
        chrome_options.add_argument('--disable-blink-features=AutomationControlled')
        chrome_options.add_argument('--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36')
        chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
        chrome_options.add_experimental_option('useAutomationExtension', False)

        service = Service(ChromeDriverManager().install())
        self.driver = webdriver.Chrome(service=service, options=chrome_options)
        self.wait = WebDriverWait(self.driver, 10)

        self.driver.execute_cdp_cmd('Page.addScriptToEvaluateOnNewDocument', {
            'source': '''
                Object.defineProperty(navigator, 'webdriver', {
                    get: () => undefined
                })
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

    def convert_purchase_count(self, text):
        """Convert purchase count format: '10K+ bought in past month' -> '10,000'"""
        if not text:
            return None

        try:
            # Extract number part (e.g., "10K+" from "10K+ bought in past month")
            match = re.search(r'([\d.]+)K?\+?', text, re.IGNORECASE)
            if not match:
                return None

            number_str = match.group(1)
            number = float(number_str)

            # Check if K/k is present
            if 'K' in text.upper():
                number = number * 1000

            # Convert to integer and format with comma
            return f"{int(number):,}"

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

    def scrape_page(self, url, page_number):
        """Scrape a single page"""
        try:
            print(f"\n[PAGE {page_number}] Accessing: {url[:80]}...")
            self.driver.get(url)

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
                debug_asin = debug_product.get('data-asin', 'N/A')
                debug_name = self.extract_product_name(debug_product)
                debug_url_path = self.extract_text_safe(debug_product, self.xpaths['product_url']['xpath'])
                print(f"  {debug_idx}. ASIN: {debug_asin} | Name: {debug_name[:50] if debug_name else 'NULL'}... | URL: {debug_url_path[:50] if debug_url_path else 'NULL'}...")

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

                # Extract ASIN
                asin = product.get('data-asin', None)
                if not asin or asin.strip() == '':
                    asin = None

                # Initialize _seen_asins if not exists
                if not hasattr(self, '_seen_asins'):
                    self._seen_asins = {}

                # Check if we've seen this ASIN before (skip duplicates)
                if asin and asin in self._seen_asins:
                    prev_page = self._seen_asins[asin]
                    print(f"  [{idx}] SKIP: Duplicate ASIN {asin} (already collected from page {prev_page})")
                    print(f"         Product: {product_name[:60]}...")
                    continue

                # Extract price
                final_price = self.extract_text_safe(product, self.xpaths['final_price']['xpath'])

                # Extract and convert purchase count
                purchase_count_raw = self.extract_text_safe(product, self.xpaths['purchase_history']['xpath'])
                purchase_count = self.convert_purchase_count(purchase_count_raw)

                data = {
                    'mall_name': 'Amazon',
                    'page_number': page_number,
                    'Retailer_SKU_Name': product_name,
                    'Number_of_units_purchased_past_month': purchase_count,
                    'Final_SKU_Price': final_price,
                    'Original_SKU_Price': self.extract_text_safe(product, self.xpaths['original_price']['xpath']),
                    'Shipping_Info': self.extract_text_safe(product, self.xpaths['shipping_info']['xpath']),
                    'Available_Quantity_for_Purchase': self.extract_text_safe(product, self.xpaths['stock_availability']['xpath']),
                    'Discount_Type': discount_type,
                    'Product_URL': product_url,
                    'ASIN': asin
                }

                # Save to database
                if self.save_to_db(data):
                    collected_count += 1
                    self.total_collected += 1

                    # Track this ASIN
                    if asin:
                        self._seen_asins[asin] = page_number

                    # DEBUG: Show detailed saved data
                    print(f"  [{idx}] ✓ SAVED (Order #{self.sequential_id - 1}):")
                    print(f"           Name: {data['Retailer_SKU_Name'][:60] if data['Retailer_SKU_Name'] else '[NO NAME]'}...")
                    print(f"           ASIN: {asin or 'N/A'}")
                    print(f"           Price: {final_price or 'N/A'}")
                    print(f"           URL: {product_url[:60] if product_url else 'NULL'}...")
                else:
                    print(f"  [{idx}] ✗ FAILED: {data['Retailer_SKU_Name'][:40]}... (ASIN: {asin or 'N/A'}) - DB save error")

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
        """Save collected data with collection order (1-300)"""
        cursor = None
        try:
            # Temporarily disable autocommit for transaction
            self.db_conn.autocommit = False

            # Use sequential_id (1-300) for collection order
            collection_order = self.sequential_id

            cursor = self.db_conn.cursor()

            # Try INSERT to amazon_tv_main_raw_data
            cursor.execute("""
                INSERT INTO amazon_tv_main_raw_data
                ("order", mall_name, page_number, Retailer_SKU_Name, Number_of_units_purchased_past_month,
                 Final_SKU_Price, Original_SKU_Price, Shipping_Info,
                 Available_Quantity_for_Purchase, Discount_Type, Product_URL, ASIN, batch_id)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                RETURNING id
            """, (
                collection_order,
                data['mall_name'],
                data['page_number'],
                data['Retailer_SKU_Name'],
                data['Number_of_units_purchased_past_month'],
                data['Final_SKU_Price'],
                data['Original_SKU_Price'],
                data['Shipping_Info'],
                data['Available_Quantity_for_Purchase'],
                data['Discount_Type'],
                data['Product_URL'],
                data['ASIN'],
                self.batch_id
            ))
            raw_data_result = cursor.fetchone()

            # Try INSERT to Amazon_tv_main_crawled
            cursor.execute("""
                INSERT INTO Amazon_tv_main_crawled
                ("order", mall_name, Retailer_SKU_Name, Number_of_units_purchased_past_month,
                 Final_SKU_Price, Original_SKU_Price, Shipping_Info,
                 Available_Quantity_for_Purchase, Discount_Type, ASIN)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                collection_order,
                data['mall_name'],
                data['Retailer_SKU_Name'],
                data['Number_of_units_purchased_past_month'],
                data['Final_SKU_Price'],
                data['Original_SKU_Price'],
                data['Shipping_Info'],
                data['Available_Quantity_for_Purchase'],
                data['Discount_Type'],
                data['ASIN']
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
            print("Amazon TV Crawler - Starting")
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

            # Generate batch_id for this session (Korea timezone)
            korea_tz = pytz.timezone('Asia/Seoul')
            self.batch_id = datetime.now(korea_tz).strftime('%Y%m%d_%H%M%S')
            print(f"[OK] Batch ID: {self.batch_id}")

            # Load XPaths and URLs
            if not self.load_xpaths():
                return

            page_urls = self.load_page_urls()
            if not page_urls:
                print("[ERROR] No page URLs found")
                return

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

            # DEBUG: Show duplicate statistics
            if hasattr(self, '_seen_asins'):
                print(f"[DEBUG] Unique ASINs collected: {len(self._seen_asins)}")
                if len(self._seen_asins) != self.total_collected:
                    print(f"[WARNING] Mismatch! Total collected ({self.total_collected}) != Unique ASINs ({len(self._seen_asins)})")
                    print(f"[WARNING] This suggests duplicate products were collected!")

            print("="*80)

        except Exception as e:
            print(f"[ERROR] Crawler failed: {e}")

        finally:
            if self.driver:
                self.driver.quit()
            if self.db_conn:
                self.db_conn.close()


if __name__ == "__main__":
    try:
        crawler = AmazonTVCrawler()
        crawler.run()
    except Exception as e:
        print(f"\n[FATAL ERROR] {e}")
        import traceback
        traceback.print_exc()

    print("\n[INFO] Crawler completed. Window will close automatically...")
