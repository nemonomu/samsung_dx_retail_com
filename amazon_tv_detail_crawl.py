import time
import random
import psycopg2
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager
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

class AmazonDetailCrawler:
    def __init__(self):
        self.driver = None
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
                WHERE mall_name = 'Amazon' AND page_type = 'detail_page' AND is_active = TRUE
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
        """Load product URLs from raw_data and amazon_tv_bsr tables (latest batch only)"""
        try:
            cursor = self.db_conn.cursor()

            # Get latest batch_id from raw_data
            cursor.execute("""
                SELECT batch_id
                FROM raw_data
                WHERE mall_name = 'Amazon' AND batch_id IS NOT NULL
                ORDER BY batch_id DESC
                LIMIT 1
            """)
            main_batch_result = cursor.fetchone()
            main_batch_id = main_batch_result[0] if main_batch_result else None

            # Get latest batch_id from amazon_tv_bsr
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

            # Load from raw_data (main) - latest batch only
            main_urls = []
            if main_batch_id:
                cursor.execute("""
                    SELECT "order", product_url
                    FROM raw_data
                    WHERE mall_name = 'Amazon'
                      AND batch_id = %s
                      AND product_url IS NOT NULL
                      AND product_url != ''
                    ORDER BY "order"
                """, (main_batch_id,))
                main_urls = [{'mother': 'main', 'order': row[0], 'url': row[1]} for row in cursor.fetchall()]

            # Load from amazon_tv_bsr (bsr) - latest batch only
            bsr_urls = []
            if bsr_batch_id:
                cursor.execute("""
                    SELECT rank, product_url
                    FROM amazon_tv_bsr
                    WHERE batch_id = %s
                      AND product_url IS NOT NULL
                      AND product_url != ''
                    ORDER BY rank
                """, (bsr_batch_id,))
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
        """Setup Chrome WebDriver"""
        chrome_options = Options()
        chrome_options.add_argument('--disable-blink-features=AutomationControlled')
        chrome_options.add_argument('--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36')
        chrome_options.add_argument('--disable-dev-shm-usage')
        chrome_options.add_argument('--no-sandbox')
        chrome_options.add_argument('--window-size=1920,1080')
        chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
        chrome_options.add_experimental_option('useAutomationExtension', False)

        service = Service(ChromeDriverManager().install())
        self.driver = webdriver.Chrome(service=service, options=chrome_options)

        # Anti-detection scripts
        self.driver.execute_cdp_cmd('Page.addScriptToEvaluateOnNewDocument', {
            'source': '''
                Object.defineProperty(navigator, 'webdriver', {
                    get: () => undefined
                });
            '''
        })

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

    def clean_rank(self, rank_text):
        """Remove parentheses content from rank text"""
        if not rank_text:
            return None
        # Remove content in parentheses: "#8,565 in Electronics (See Top 100 in Electronics)" -> "#8,565 in Electronics"
        cleaned = re.sub(r'\s*\([^)]*\)', '', rank_text)
        return cleaned.strip()

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

            # Extract data
            retailer_sku_name = self.extract_text_safe(tree, self.xpaths.get('product_name'))
            star_rating = self.extract_text_safe(tree, self.xpaths.get('star_rating'))

            # SKU_Popularity - only collect if "Amazon's Choice"
            sku_popularity_raw = self.extract_text_safe(tree, self.xpaths.get('sku_popularity'))
            sku_popularity = sku_popularity_raw if sku_popularity_raw and "Amazon's Choice" in sku_popularity_raw else None

            membership_discount = self.extract_text_safe(tree, self.xpaths.get('membership_discount'))
            samsung_sku_name = self.extract_text_safe(tree, self.xpaths.get('samsung_sku_name'))

            # Ranks - remove parentheses
            rank_1_raw = self.extract_text_safe(tree, self.xpaths.get('rank_1'))
            rank_1 = self.clean_rank(rank_1_raw)

            rank_2_raw = self.extract_text_safe(tree, self.xpaths.get('rank_2'))
            rank_2 = self.clean_rank(rank_2_raw)

            data = {
                'mother': mother,
                'order': order,
                'product_url': url,
                'Retailer_SKU_Name': retailer_sku_name,
                'Star_Rating': star_rating,
                'SKU_Popularity': sku_popularity,
                'Retailer_Membership_Discounts': membership_discount,
                'Samsung_SKU_Name': samsung_sku_name,
                'Rank_1': rank_1,
                'Rank_2': rank_2,
                'Count_of_Star_Ratings': None,  # Not collecting yet
                'Summarized_Review_Content': None,  # Not collecting yet
                'Detailed_Review_Content': None  # Not collecting yet
            }

            # Save to database
            if self.save_to_db(data):
                self.total_collected += 1
                print(f"  [OK] Collected: {retailer_sku_name[:50] if retailer_sku_name else '[NO NAME]'}...")
                print(f"       Star: {star_rating or 'N/A'} | Popularity: {sku_popularity or 'N/A'}")
                print(f"       Rank1: {rank_1 or 'N/A'} | Rank2: {rank_2 or 'N/A'}")
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
            # Temporarily disable autocommit for transaction
            self.db_conn.autocommit = False

            cursor = self.db_conn.cursor()

            # Insert to Amazon_tv_detail_crawled
            cursor.execute("""
                INSERT INTO Amazon_tv_detail_crawled
                (mother, "order", product_url, Retailer_SKU_Name, Star_Rating,
                 SKU_Popularity, Retailer_Membership_Discounts, Samsung_SKU_Name,
                 Rank_1, Rank_2, Count_of_Star_Ratings, Summarized_Review_Content,
                 Detailed_Review_Content)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                data['mother'],
                data['order'],
                data['product_url'],
                data['Retailer_SKU_Name'],
                data['Star_Rating'],
                data['SKU_Popularity'],
                data['Retailer_Membership_Discounts'],
                data['Samsung_SKU_Name'],
                data['Rank_1'],
                data['Rank_2'],
                data['Count_of_Star_Ratings'],
                data['Summarized_Review_Content'],
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
            print("Amazon TV Detail Page Crawler - Starting")
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
        crawler = AmazonDetailCrawler()
        crawler.run()
    except Exception as e:
        print(f"\n[FATAL ERROR] {e}")
        import traceback
        traceback.print_exc()

    print("\n[INFO] Crawler terminated. Exiting...")
