import time
import random
import psycopg2
import pickle
import json
import os
from datetime import datetime
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

# Cookie file path
COOKIE_FILE = 'amazon_cookies.pkl'

# Import database configuration
from config import DB_CONFIG

class AmazonDetailCrawler:
    def __init__(self):
        self.driver = None
        self.db_conn = None
        self.xpaths = {}
        self.total_collected = 0
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

    def load_product_urls(self):
        """Load product URLs from amazon_tv_main_crawled and amazon_tv_bsr tables (latest batch only)"""
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

            # Load from amazon_tv_main_crawled (main) - latest batch only
            main_urls = []
            if main_batch_id:
                print(f"[INFO] Loading main URLs from batch {main_batch_id}...")
                cursor.execute("""
                    SELECT product_url
                    FROM amazon_tv_main_crawled
                    WHERE batch_id = %s
                      AND product_url IS NOT NULL
                      AND product_url != ''
                    ORDER BY main_rank
                """, (main_batch_id,))
                main_urls = [{'page_type': 'main', 'url': row[0]} for row in cursor.fetchall()]
                print(f"[OK] Loaded {len(main_urls)} main URLs")
            else:
                print("[WARNING] No main batch_id found in amazon_tv_main_crawled")

            # Load from amazon_tv_bsr (bsr) - latest batch only
            bsr_urls = []
            if bsr_batch_id:
                print(f"[INFO] Loading BSR URLs from batch {bsr_batch_id}...")
                cursor.execute("""
                    SELECT product_url
                    FROM amazon_tv_bsr
                    WHERE batch_id = %s
                      AND product_url IS NOT NULL
                      AND product_url != ''
                    ORDER BY bsr_rank
                """, (bsr_batch_id,))
                bsr_urls = [{'page_type': 'bsr', 'url': row[0]} for row in cursor.fetchall()]
                print(f"[OK] Loaded {len(bsr_urls)} BSR URLs")
            else:
                print("[WARNING] No BSR batch_id found in amazon_tv_bsr")

            cursor.close()

            all_urls = main_urls + bsr_urls
            print(f"[OK] Total URLs loaded: {len(all_urls)}")

            if len(all_urls) == 0:
                print("[ERROR] No product URLs found! Please check:")
                print("  1. amazon_tv_main_crawled table has data with valid batch_id")
                print("  2. amazon_tv_bsr table has data with valid batch_id")
                print("  3. Product_URL/product_url columns are not empty")

            return all_urls

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

    def extract_screen_size(self, tree):
        """Extract screen size (format: '32 inches')"""
        try:
            # XPath: //*[@id="poExpander"]/div[1]/div/table/tbody/tr[1]/td[2]/span
            xpaths = [
                '//*[@id="poExpander"]/div[1]/div/table/tbody/tr[1]/td[2]/span',
                '//table[@class="a-normal a-spacing-small"]//tr[contains(@class, "po-display.size")]//td[@class="a-span9"]//span[@class="a-size-base po-break-word"]',
                '//tr[contains(@class, "po-display.size")]//span[@class="a-size-base po-break-word"]'
            ]

            for xpath in xpaths:
                size_text = self.extract_text_safe(tree, xpath)
                if size_text:
                    # "32 Inches" -> "32 inches"
                    return size_text.lower()

            return None

        except Exception as e:
            print(f"  [WARNING] Failed to extract screen size: {e}")
            return None

    def extract_count_of_reviews(self, tree):
        """Extract count of reviews (format: '1,484')"""
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
                    # "1,484 ratings" -> "1,484"
                    # Extract only numbers and comma
                    match = re.search(r'([\d,]+)', reviews_text)
                    if match:
                        return match.group(1)

            return None

        except Exception as e:
            print(f"  [WARNING] Failed to extract count of reviews: {e}")
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
                        # Check if we already have 20 reviews
                        if len(all_reviews) >= 20:
                            break

                        review_text = elem.text_content().strip() if hasattr(elem, 'text_content') else str(elem).strip()
                        if review_text and len(review_text) > 10:
                            all_reviews.append(review_text)

                # Check if we have enough reviews after this page
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
            page_type = url_data['page_type']
            url = url_data['url']

            print(f"\n[{page_type.upper()}] Accessing: {url[:80]}...")

            self.driver.get(url)
            time.sleep(random.uniform(3, 5))

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
            star_rating = self.extract_text_safe(tree, self.xpaths.get('star_rating'))

            # SKU_Popularity - only collect if "Amazon's Choice"
            sku_popularity_raw = self.extract_text_safe(tree, self.xpaths.get('sku_popularity'))
            sku_popularity = sku_popularity_raw if sku_popularity_raw and "Amazon's" in sku_popularity_raw and "Choice" in sku_popularity_raw else None

            # Retailer_Membership_Discounts - clean Prime text
            membership_discount_raw = self.extract_text_safe(tree, self.xpaths.get('membership_discount'))
            membership_discount = self.clean_membership_discount(membership_discount_raw)

            # Item (formerly Samsung_SKU_Name) - try multiple approaches
            item = self.extract_text_safe(tree, self.xpaths.get('samsung_sku_name'))
            if not item:
                # Find by th text "Model Number" in Item details section
                item = self.extract_text_safe(tree, '//th[contains(text(), "Model Number")]/following-sibling::td')
            if not item:
                # Try fixed XPath for Item details section
                item = self.extract_text_safe(tree, '//*[@id="productDetails_expanderTables_depthRightSections"]//th[contains(text(), "Model Number")]/following-sibling::td')
            if not item:
                # Old fallback XPath for different page structure
                item = self.extract_text_safe(tree, '//*[@id="detailBullets_feature_div"]/ul/li[2]/span/span[2]')

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

            # Extract screen_size (NEW)
            screen_size = self.extract_screen_size(tree)

            # Extract count_of_reviews (NEW)
            count_of_reviews = self.extract_count_of_reviews(tree)

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

            # Extract detailed review content (20 reviews in JSON format)
            detailed_review_content = self.extract_detailed_reviews(url)

            data = {
                'page_type': page_type,
                'product_url': url,
                'Retailer_SKU_Name': retailer_sku_name,
                'Star_Rating': star_rating,
                'SKU_Popularity': sku_popularity,
                'Retailer_Membership_Discounts': membership_discount,
                'item': item,
                'Rank_1': rank_1,
                'Rank_2': rank_2,
                'screen_size': screen_size,
                'count_of_reviews': count_of_reviews,
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
            # Temporarily disable autocommit for transaction
            self.db_conn.autocommit = False

            cursor = self.db_conn.cursor()

            # Calculate calendar week
            calendar_week = f"w{datetime.now().isocalendar().week}"

            # Calculate crawl_strdatetime (format: 202511040300559260)
            now = datetime.now()
            crawl_strdatetime = now.strftime('%Y%m%d%H%M%S') + now.strftime('%f')[:4]

            # Insert to amazon_tv_detail_crawled
            cursor.execute("""
                INSERT INTO amazon_tv_detail_crawled
                (account_name, batch_id, page_type, product_url, Retailer_SKU_Name, Star_Rating,
                 SKU_Popularity, Retailer_Membership_Discounts, item,
                 Rank_1, Rank_2, screen_size, count_of_reviews, Count_of_Star_Ratings,
                 Summarized_Review_Content, Detailed_Review_Content, calendar_week, crawl_strdatetime)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
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
                crawl_strdatetime
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
            print(f"Amazon TV Detail Page Crawler - Starting (Batch ID: {self.batch_id})")
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
                print(f"\n{'='*80}")
                print(f"Processing {idx}/{len(product_urls)}")

                self.scrape_detail_page(url_data)

                # Random delay between requests
                delay = random.uniform(2, 4)
                print(f"[INFO] Waiting {delay:.1f} seconds before next request...")
                time.sleep(delay)

            print("\n" + "="*80)
            print(f"Detail Crawling completed! Total collected: {self.total_collected}/{len(product_urls)}")
            print("="*80)

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
