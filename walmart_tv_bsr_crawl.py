import time
import random
import psycopg2
from playwright.sync_api import sync_playwright
from lxml import html
import re
from urllib.parse import urlparse, parse_qs, unquote

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
                WHERE mall_name = 'Walmart' AND page_type = 'main' AND is_active = TRUE
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
            self.browser = self.playwright.chromium.launch(headless=False)
            print("[OK] Playwright browser setup complete")
            return True
        except Exception as e:
            print(f"[ERROR] Failed to setup browser: {e}")
            return False

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

    def scrape_page(self, url, page_number):
        """Scrape a single page"""
        try:
            print(f"\n[PAGE {page_number}] Accessing: {url[:80]}...")

            # Create new context and page
            context = self.browser.new_context(
                viewport={'width': 1920, 'height': 1080},
                user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36'
            )
            page = context.new_page()

            # Access URL
            page.goto(url, wait_until='domcontentloaded', timeout=60000)
            time.sleep(random.uniform(5, 8))

            # Scroll to load all products
            print("[INFO] Scrolling to load products...")
            for _ in range(2):
                page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                time.sleep(2)
            page.evaluate("window.scrollTo(0, 0)")
            time.sleep(2)

            # Get page source and parse
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
                    context.close()
                    return False

                # Extract product name
                product_name = self.extract_text_safe(product, self.xpaths['product_name']['xpath'])
                if not product_name:
                    continue

                # Extract product URL
                product_url_raw = self.extract_text_safe(product, self.xpaths['product_url']['xpath'])
                product_url = self.normalize_product_url(product_url_raw) if product_url_raw else None

                # Extract prices
                final_price_raw = self.extract_text_safe(product, self.xpaths['final_price']['xpath'])
                final_price = self.clean_price_text(final_price_raw) if final_price_raw else None

                original_price_raw = self.extract_text_safe(product, self.xpaths['original_price']['xpath'])
                original_price = original_price_raw if original_price_raw else None

                # Extract other fields
                offer = self.extract_text_safe(product, self.xpaths['offer']['xpath'])
                pickup = self.extract_text_safe(product, self.xpaths['pickup_availability']['xpath'])
                shipping = self.extract_text_safe(product, self.xpaths['shipping_availability']['xpath'])
                delivery = self.extract_text_safe(product, self.xpaths['delivery_availability']['xpath'])

                # Extract SKU status
                rollback = self.extract_text_safe(product, self.xpaths['sku_status_rollback']['xpath'])
                sponsored = self.extract_text_safe(product, self.xpaths['sku_status_sponsored']['xpath'])
                sku_status = "Rollback" if rollback else ("Sponsored" if sponsored else None)

                # Extract membership discount
                membership_discount_elem = self.extract_text_safe(product, self.xpaths['membership_discount']['xpath'])
                membership_discount = "Walmart Plus" if membership_discount_elem else None

                # Extract availability
                available_quantity = self.extract_text_safe(product, self.xpaths['available_quantity']['xpath'])
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
                    'Rank': None,
                    'Product_url': product_url
                }

                # Save to database
                if self.save_to_db(data):
                    collected_count += 1
                    self.total_collected += 1
                    print(f"  [{idx}/{len(products)}] Collected: {product_name[:50]}... | Price: {final_price or 'N/A'}")

            print(f"[PAGE {page_number}] Collected {collected_count} products (Total: {self.total_collected}/{self.max_skus})")
            context.close()
            return True

        except Exception as e:
            print(f"[ERROR] Failed to scrape page {page_number}: {e}")
            import traceback
            traceback.print_exc()
            return True

    def normalize_product_url(self, raw_url):
        """Normalize product URL to clean format"""
        if not raw_url:
            return None

        if '/sp/track?' in raw_url:
            try:
                parsed = urlparse(raw_url)
                query_params = parse_qs(parsed.query)
                if 'rd' in query_params:
                    redirect_url = query_params['rd'][0]
                    decoded_url = unquote(redirect_url)
                    if '/ip/' in decoded_url:
                        ip_path = decoded_url.split('/ip/')[1]
                        clean_path = '/ip/' + ip_path.split('?')[0]
                        return f"https://www.walmart.com{clean_path}"
            except:
                pass

        if raw_url.startswith('/ip/'):
            clean_path = raw_url.split('?')[0]
            return f"https://www.walmart.com{clean_path}"

        if raw_url.startswith('http'):
            if '/ip/' in raw_url:
                return raw_url.split('?')[0]
            return raw_url

        return raw_url

    def clean_price_text(self, price_text):
        """Extract clean price from complex price HTML text"""
        if not price_text:
            return None

        price_text = ' '.join(price_text.split())
        match = re.search(r'\$\s*(\d[\d,]*)\s*(\d{2})', price_text)
        if match:
            dollars = match.group(1).replace(',', '')
            cents = match.group(2)
            return f"${dollars}.{cents}"

        return price_text

    def save_to_db(self, data):
        """Save collected data with collection order (1-100)"""
        try:
            cursor = self.db_conn.cursor()
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
                self.sequential_id += 1

            self.db_conn.commit()
            cursor.close()
            return result is not None

        except Exception as e:
            print(f"[ERROR] Failed to save to DB: {e}")
            return False

    def run(self):
        """Main execution"""
        try:
            print("="*80)
            print("Walmart TV BSR Crawler")
            print("="*80)

            if not self.connect_db():
                return

            if not self.load_xpaths():
                return

            page_urls = self.load_page_urls()
            if not page_urls:
                print("[ERROR] No page URLs found")
                return

            if not self.setup_browser():
                return

            # Scrape each page
            for page_number, url in page_urls:
                if self.total_collected >= self.max_skus:
                    break

                if not self.scrape_page(url, page_number):
                    break

                time.sleep(random.uniform(3, 5))

            print("\n" + "="*80)
            print(f"Crawling completed! Total collected: {self.total_collected} SKUs")
            print("="*80)

        except Exception as e:
            print(f"[ERROR] Crawler failed: {e}")
            import traceback
            traceback.print_exc()

        finally:
            if self.browser:
                try:
                    self.browser.close()
                except:
                    pass

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

    print("\n[INFO] BSR Crawler completed.")
