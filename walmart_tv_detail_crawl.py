"""
Walmart TV Detail Page Crawler
Collects detailed product information from URLs stored in:
- wmart_tv_main_crawl (mother='main')
- wmart_tv_bsr_crawl (mother='bsr')
"""
import time
import random
import psycopg2
import undetected_chromedriver as uc
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from lxml import html
import re

# Database configuration
DB_CONFIG = {
    'host': 'samsung-dx-crawl.csnixzmkuppn.ap-northeast-2.rds.amazonaws.com',
    'port': 5432,
    'database': 'postgres',
    'user': 'postgres',
    'password': 'admin2025!'
}

class WalmartDetailCrawler:
    def __init__(self):
        self.driver = None
        self.wait = None
        self.db_conn = None
        self.xpaths = {}
        self.total_collected = 0

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
        """Load product URLs from wmart_tv_main_crawl and wmart_tv_bsr_crawl tables"""
        try:
            cursor = self.db_conn.cursor()

            # Load from wmart_tv_main_crawl (main)
            cursor.execute("""
                SELECT "order", Product_url
                FROM wmart_tv_main_crawl
                WHERE Product_url IS NOT NULL
                  AND Product_url != ''
                ORDER BY "order"
            """)
            main_urls = [{'mother': 'main', 'order': row[0], 'url': row[1]} for row in cursor.fetchall()]

            # Load from wmart_tv_bsr_crawl (bsr)
            cursor.execute("""
                SELECT "order", Product_url
                FROM wmart_tv_bsr_crawl
                WHERE Product_url IS NOT NULL
                  AND Product_url != ''
                ORDER BY "order"
            """)
            bsr_urls = [{'mother': 'bsr', 'order': row[0], 'url': row[1]} for row in cursor.fetchall()]

            cursor.close()

            all_urls = main_urls + bsr_urls
            print(f"[OK] Loaded {len(main_urls)} main URLs + {len(bsr_urls)} bsr URLs = {len(all_urls)} total")
            return all_urls

        except Exception as e:
            print(f"[ERROR] Failed to load product URLs: {e}")
            import traceback
            traceback.print_exc()
            return []

    def setup_driver(self):
        """Setup Chrome WebDriver with undetected-chromedriver"""
        options = uc.ChromeOptions()

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
        self.driver.set_page_load_timeout(60)
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

    def extract_detailed_reviews(self, tree):
        """Extract up to 20 detailed reviews"""
        try:
            # TODO: Implement review extraction based on Walmart's review structure
            # This is a placeholder - needs actual XPath for Walmart reviews
            return None
        except Exception as e:
            print(f"  [WARNING] Failed to extract detailed reviews: {e}")
            return None

    def scrape_detail_page(self, url_data):
        """Scrape detail page and extract information"""
        try:
            mother = url_data['mother']
            order = url_data['order']
            url = url_data['url']

            print(f"\n[{mother.upper()}][{order}] Accessing: {url[:80]}...")

            self.driver.get(url)
            time.sleep(random.uniform(3, 5))

            page_source = self.driver.page_source
            tree = html.fromstring(page_source)

            # Extract data using XPaths from database
            retailer_sku_name = self.extract_text_safe(tree, self.xpaths.get('product_name'))
            star_rating = self.extract_text_safe(tree, self.xpaths.get('star_rating'))
            purchased_yesterday = self.extract_text_safe(tree, self.xpaths.get('purchased_yesterday'))
            added_to_carts = self.extract_text_safe(tree, self.xpaths.get('added_to_carts'))
            sku_popularity = self.extract_text_safe(tree, self.xpaths.get('sku_popularity'))
            savings = self.extract_text_safe(tree, self.xpaths.get('savings'))
            discount_type = self.extract_text_safe(tree, self.xpaths.get('discount_type'))
            shipping_info = self.extract_text_safe(tree, self.xpaths.get('shipping_info'))
            count_of_star_ratings = self.extract_text_safe(tree, self.xpaths.get('count_of_star_ratings'))

            # Extract detailed reviews
            detailed_review_content = self.extract_detailed_reviews(tree)

            data = {
                'mother': mother,
                'order': order,
                'product_url': url,
                'Retailer_SKU_Name': retailer_sku_name,
                'Star_Rating': star_rating,
                'Number_of_ppl_purchased_yesterday': purchased_yesterday,
                'Number_of_ppl_added_to_carts': added_to_carts,
                'SKU_Popularity': sku_popularity,
                'Savings': savings,
                'Discount_Type': discount_type,
                'Shipping_Info': shipping_info,
                'Count_of_Star_Ratings': count_of_star_ratings,
                'Detailed_Review_Content': detailed_review_content
            }

            # Save to database
            if self.save_to_db(data):
                self.total_collected += 1
                print(f"  [OK] Collected: {retailer_sku_name[:50] if retailer_sku_name else '[NO NAME]'}...")
                print(f"       Star: {star_rating or 'N/A'} | Ratings: {count_of_star_ratings or 'N/A'}")
                print(f"       Purchased Yesterday: {purchased_yesterday or 'N/A'} | Added to Carts: {added_to_carts or 'N/A'}")
                print(f"       Savings: {savings or 'N/A'} | Discount: {discount_type or 'N/A'}")
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
            # Temporarily disable autocommit for transaction
            self.db_conn.autocommit = False

            cursor = self.db_conn.cursor()

            # Insert to Walmart_tv_detail_crawled
            cursor.execute("""
                INSERT INTO Walmart_tv_detail_crawled
                (mother, "order", product_url, Retailer_SKU_Name, Star_Rating,
                 Number_of_ppl_purchased_yesterday, Number_of_ppl_added_to_carts,
                 SKU_Popularity, Savings, Discount_Type, Shipping_Info,
                 Count_of_Star_Ratings, Detailed_Review_Content)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                data['mother'],
                data['order'],
                data['product_url'],
                data['Retailer_SKU_Name'],
                data['Star_Rating'],
                data['Number_of_ppl_purchased_yesterday'],
                data['Number_of_ppl_added_to_carts'],
                data['SKU_Popularity'],
                data['Savings'],
                data['Discount_Type'],
                data['Shipping_Info'],
                data['Count_of_Star_Ratings'],
                data['Detailed_Review_Content']
            ))

            # Commit transaction
            self.db_conn.commit()

            cursor.close()

            # Re-enable autocommit
            self.db_conn.autocommit = True

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

            if 'duplicate key' not in str(e):
                print(f"[ERROR] Failed to save to DB: {e}")

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

            # Setup WebDriver
            self.setup_driver()

            # Scrape each detail page
            for idx, url_data in enumerate(product_urls, 1):
                print(f"\n{'='*80}")
                print(f"Processing {idx}/{len(product_urls)}")

                self.scrape_detail_page(url_data)

                # Random delay between requests
                time.sleep(random.uniform(2, 4))

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


if __name__ == "__main__":
    try:
        crawler = WalmartDetailCrawler()
        crawler.run()
    except Exception as e:
        print(f"\n[FATAL ERROR] {e}")
        import traceback
        traceback.print_exc()

    print("\n[INFO] Crawler terminated. Exiting...")
