import time
import random
import psycopg2
import pickle
import json
import os
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager
from lxml import html
import re

# Cookie file path
COOKIE_FILE = 'amazon_cookies.pkl'

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

        # Load cookies for login
        self.load_cookies()

    def load_cookies(self):
        """Load cookies from file for authenticated access"""
        if not os.path.exists(COOKIE_FILE):
            print(f"[WARNING] Cookie file not found: {COOKIE_FILE}")
            print("[WARNING] Review collection may fail without login. Run amazon_login.py first.")
            return False

        try:
            self.driver.get("https://www.amazon.com")
            time.sleep(2)

            with open(COOKIE_FILE, 'rb') as f:
                cookies = pickle.load(f)
                for cookie in cookies:
                    self.driver.add_cookie(cookie)

            self.driver.refresh()
            time.sleep(2)
            print(f"[OK] Cookies loaded - logged in")
            return True

        except Exception as e:
            print(f"[WARNING] Failed to load cookies: {e}")
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

    def extract_count_of_star_ratings(self, tree):
        """Extract star ratings count (format: 5star:1788, 4star:318, ...)"""
        try:
            # Get total count from "2,449 global ratings"
            total_text = self.extract_text_safe(tree, '//*[@id="cm_cr_dp_d_rating_histogram"]/div[3]')
            if not total_text:
                return None

            # Extract number from "2,449 global ratings"
            total_match = re.search(r'([\d,]+)\s*global ratings', total_text)
            if not total_match:
                return None

            total_count = int(total_match.group(1).replace(',', ''))

            # Extract percentages for each star rating (5 to 1) from aria-label
            star_counts = []
            for i in range(1, 6):  # li[1] to li[5]
                xpath = f'//*[@id="histogramTable"]/li[{i}]/span/a/@aria-label'
                aria_label = self.extract_text_safe(tree, xpath)

                if aria_label:
                    # Extract percentage from "73 percent of reviews have 5 stars"
                    percent_match = re.search(r'(\d+)\s*percent', aria_label)
                    if percent_match:
                        percentage = int(percent_match.group(1))
                        count = int(total_count * percentage / 100)
                        star_counts.append(count)
                    else:
                        star_counts.append(0)
                else:
                    star_counts.append(0)

            # Format: 5star:count, 4star:count, ...
            if len(star_counts) == 5:
                result = f"5star:{star_counts[0]}, 4star:{star_counts[1]}, 3star:{star_counts[2]}, 2star:{star_counts[3]}, 1star:{star_counts[4]}"
                return result

            return None

        except Exception as e:
            print(f"  [WARNING] Failed to extract star ratings count: {e}")
            return None

    def extract_summarized_review(self, tree):
        """Extract AI-generated review summary (may not exist on all pages)"""
        try:
            summary = self.extract_text_safe(tree, '//*[@id="product-summary"]/p[1]/span')
            return summary if summary else None
        except Exception as e:
            return None

    def extract_detailed_reviews(self, product_url):
        """Extract up to 20 detailed reviews from review pages"""
        try:
            # Get current page HTML
            tree = html.fromstring(self.driver.page_source)

            # Extract "See more reviews" link
            review_link_xpaths = [
                '//*[@id="reviews-medley-footer"]/div[2]/a/@href',
                '//a[@data-hook="see-all-reviews-link-foot"]/@href',
                '//a[contains(text(), "See more reviews")]/@href'
            ]

            review_link = None
            for xpath in review_link_xpaths:
                result = tree.xpath(xpath)
                if result:
                    review_link = result[0]
                    break

            if not review_link:
                print("  [WARNING] Could not find review page link")
                return None

            # Navigate to review page
            if review_link.startswith('http'):
                review_url = review_link
            else:
                review_url = "https://www.amazon.com" + review_link

            self.driver.get(review_url)
            time.sleep(random.uniform(3, 4))

            # Collect reviews from multiple pages
            all_reviews = []
            page_num = 1
            max_pages = 3  # Max 3 pages to get 20+ reviews

            while len(all_reviews) < 20 and page_num <= max_pages:
                tree = html.fromstring(self.driver.page_source)

                # Extract reviews from current page
                review_xpath = '//span[@data-hook="review-body"]/span'
                review_elements = tree.xpath(review_xpath)

                if review_elements:
                    for elem in review_elements:
                        review_text = elem.text_content().strip() if hasattr(elem, 'text_content') else str(elem).strip()
                        if review_text and len(review_text) > 10:
                            all_reviews.append(review_text)

                # Check if we have enough reviews
                if len(all_reviews) >= 20:
                    break

                # Find next page link
                next_button_xpaths = [
                    '//a[contains(text(), "Next page")]/@href',
                    '//*[@id="cm_cr-pagination_bar"]//li[@class="a-last"]/a/@href',
                    '//ul[@class="a-pagination"]//li[@class="a-last"]/a/@href'
                ]

                next_link = None
                for xpath in next_button_xpaths:
                    result = tree.xpath(xpath)
                    if result:
                        next_link = result[0]
                        break

                if next_link:
                    if next_link.startswith('http'):
                        next_url = next_link
                    else:
                        next_url = "https://www.amazon.com" + next_link

                    self.driver.get(next_url)
                    time.sleep(random.uniform(2, 3))
                    page_num += 1
                else:
                    break

            # Limit to 20 reviews and format as "1-review, 2-review, ..."
            reviews = all_reviews[:20]
            if reviews:
                formatted_reviews = []
                for idx, review in enumerate(reviews, 1):
                    formatted_reviews.append(f"{idx}-{review}")
                return ", ".join(formatted_reviews)
            else:
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

            # Extract data
            retailer_sku_name = self.extract_text_safe(tree, self.xpaths.get('product_name'))
            star_rating = self.extract_text_safe(tree, self.xpaths.get('star_rating'))

            # SKU_Popularity - only collect if "Amazon's Choice"
            sku_popularity_raw = self.extract_text_safe(tree, self.xpaths.get('sku_popularity'))
            sku_popularity = sku_popularity_raw if sku_popularity_raw and "Amazon's" in sku_popularity_raw and "Choice" in sku_popularity_raw else None

            # Retailer_Membership_Discounts - clean Prime text
            membership_discount_raw = self.extract_text_safe(tree, self.xpaths.get('membership_discount'))
            membership_discount = self.clean_membership_discount(membership_discount_raw)

            # Samsung SKU Name - try multiple XPaths
            samsung_sku_name = self.extract_text_safe(tree, self.xpaths.get('samsung_sku_name'))
            if not samsung_sku_name:
                # Fallback XPath for different page structure
                samsung_sku_name = self.extract_text_safe(tree, '//*[@id="detailBullets_feature_div"]/ul/li[2]/span/span[2]')

            # Ranks - remove parentheses, try multiple XPaths
            rank_1_raw = self.extract_text_safe(tree, self.xpaths.get('rank_1'))
            if not rank_1_raw:
                # Fallback XPath for different page structure
                rank_1_raw = self.extract_text_safe(tree, '//*[@id="detailBullets_feature_div"]/ul/li[7]/span/text()[1]')
            rank_1 = self.clean_rank(rank_1_raw)

            rank_2_raw = self.extract_text_safe(tree, self.xpaths.get('rank_2'))
            if not rank_2_raw:
                # Fallback XPath for different page structure
                rank_2_raw = self.extract_text_safe(tree, '//*[@id="detailBullets_feature_div"]/ul/li[7]/span/ul')
            rank_2 = self.clean_rank(rank_2_raw)

            # Extract count of star ratings
            count_of_star_ratings = self.extract_count_of_star_ratings(tree)

            # Extract summarized review content
            summarized_review_content = self.extract_summarized_review(tree)

            # Extract detailed review content (20 reviews in JSON format)
            detailed_review_content = self.extract_detailed_reviews(url)

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
                'Count_of_Star_Ratings': count_of_star_ratings,
                'Summarized_Review_Content': summarized_review_content,
                'Detailed_Review_Content': detailed_review_content
            }

            # Save to database
            if self.save_to_db(data):
                self.total_collected += 1
                print(f"  [OK] Collected: {retailer_sku_name[:50] if retailer_sku_name else '[NO NAME]'}...")
                print(f"       Star: {star_rating or 'N/A'} | Popularity: {sku_popularity or 'N/A'}")
                print(f"       Rank1: {rank_1 or 'N/A'} | Rank2: {rank_2 or 'N/A'}")
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
