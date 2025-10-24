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
from selenium.webdriver.common.action_chains import ActionChains
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
        """Load product URLs from wmart_tv_main_crawl and wmart_tv_bsr_crawl tables (latest batch only)"""
        try:
            cursor = self.db_conn.cursor()

            # Get latest batch_id from wmart_tv_main_crawl
            cursor.execute("""
                SELECT batch_id
                FROM wmart_tv_main_crawl
                WHERE batch_id IS NOT NULL
                ORDER BY batch_id DESC
                LIMIT 1
            """)
            main_batch_result = cursor.fetchone()
            main_batch_id = main_batch_result[0] if main_batch_result else None

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

            print(f"[INFO] Latest batch_id - Main: {main_batch_id}, BSR: {bsr_batch_id}")

            # Load from wmart_tv_main_crawl (main) - latest batch only
            main_urls = []
            if main_batch_id:
                cursor.execute("""
                    SELECT "order", Product_url
                    FROM wmart_tv_main_crawl
                    WHERE batch_id = %s
                      AND Product_url IS NOT NULL
                      AND Product_url != ''
                    ORDER BY "order"
                """, (main_batch_id,))
                main_urls = [{'mother': 'main', 'order': row[0], 'url': row[1]} for row in cursor.fetchall()]

            # Load from wmart_tv_bsr_crawl (bsr) - latest batch only
            bsr_urls = []
            if bsr_batch_id:
                cursor.execute("""
                    SELECT "order", Product_url
                    FROM wmart_tv_bsr_crawl
                    WHERE batch_id = %s
                      AND Product_url IS NOT NULL
                      AND Product_url != ''
                    ORDER BY "order"
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

    def extract_star_rating(self, tree):
        """Extract star rating number from '4.4 out of 5' format"""
        try:
            rating_text = self.extract_text_safe(tree, self.xpaths.get('star_rating'))
            if rating_text:
                # Extract number before "out of"
                match = re.search(r'([\d.]+)\s*out of', rating_text)
                if match:
                    return match.group(1)
            return None
        except Exception as e:
            return None

    def parse_number_format(self, text):
        """Parse numbers like '100+', '10k', '200+' to integer"""
        if not text:
            return None
        try:
            # Remove any non-numeric characters except 'k' and '+'
            text = text.strip().lower()

            # Handle 'k' (thousands)
            if 'k' in text:
                number = re.search(r'([\d.]+)k', text)
                if number:
                    return int(float(number.group(1)) * 1000)

            # Handle '+' or regular numbers
            number = re.search(r'(\d+)', text)
            if number:
                return int(number.group(1))

            return None
        except Exception as e:
            return None

    def extract_count_of_star_ratings(self, tree):
        """Extract star rating counts in format '5star:516, 4star:94, ...'"""
        try:
            # Get total ratings count
            total_text = self.extract_text_safe(tree, self.xpaths.get('total_ratings'))
            if not total_text:
                return None

            # Extract number from "685 ratings"
            total_match = re.search(r'(\d+)', total_text.replace(',', ''))
            if not total_match:
                return None

            total_count = int(total_match.group(1))

            # Try to extract percentage for each star (5 to 1)
            # Looking for pattern like "71% (489)"
            star_counts = {}

            # Try to find star rating buttons with percentage info
            for star in range(5, 0, -1):
                # Look for pattern "X% (Y)" where X is percentage and Y is count
                # This is a simplified approach - may need adjustment based on actual HTML
                try:
                    # Try to extract from aria-label or visible text
                    # Format: "489 ratings are rated 5 stars, 71% of all ratings"
                    pass
                except:
                    pass

            # If we can't find individual counts, calculate from percentages
            # This is placeholder - might need to scroll or click to see the breakdown
            # For now, return None if detailed breakdown not available
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
            # Find all badge elements - try multiple approaches
            # Look for the badge container first
            badge_xpaths = [
                '//*[@id="maincontent"]/section/main/div[2]/div[2]/div/div[2]/div/div[2]/div/div/div[1]/div/span',
                '//div[@data-testid="module-2-badges"]//span[@data-testid="badgeTagComponent"]//span',
                '//div[contains(@class, "flex items-start")]//span[contains(@class, "w_yTSq")]'
            ]

            all_badges = []
            for xpath in badge_xpaths:
                badges = tree.xpath(xpath)
                if badges:
                    for badge in badges:
                        text = badge.text_content().strip() if hasattr(badge, 'text_content') else str(badge).strip()
                        if text and text not in all_badges:
                            all_badges.append(text)

            # Classify badges
            purchased_yesterday = None
            added_to_carts = None
            sku_popularity = None

            for badge_text in all_badges:
                badge_lower = badge_text.lower()

                # Check for "bought since yesterday"
                if 'bought since yesterday' in badge_lower:
                    purchased_yesterday = self.parse_number_format(badge_text)

                # Check for "people's carts"
                elif "people's carts" in badge_lower or 'peoples carts' in badge_lower:
                    added_to_carts = self.parse_number_format(badge_text)

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
                'sku_popularity': sku_popularity
            }

        except Exception as e:
            print(f"  [WARNING] Failed to extract badges: {e}")
            return {
                'purchased_yesterday': None,
                'added_to_carts': None,
                'sku_popularity': None
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
                # Extract product name from each container
                name_xpath = './/div/a/span/h3'
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

    def click_specifications_and_get_model(self):
        """Click Specifications > More details > Extract Model"""
        try:
            print(f"  [INFO] Attempting to extract Model from Specifications...")

            # Scroll down to load Specifications section
            self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight / 2);")
            time.sleep(2)

            # Find Specifications section - try multiple ways
            specs_found = False
            try:
                # Try to find by h2 text
                specs_button = self.driver.find_element(By.XPATH, "//h2[contains(text(), 'Specifications')]")
                self.driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", specs_button)
                time.sleep(1)
                specs_found = True
                print(f"  [OK] Found Specifications section")
            except:
                try:
                    # Try alternative: find by button text
                    specs_button = self.driver.find_element(By.XPATH, "//button[contains(., 'Specifications')]")
                    self.driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", specs_button)
                    time.sleep(1)
                    specs_found = True
                    print(f"  [OK] Found Specifications button")
                except Exception as e:
                    print(f"  [WARNING] Could not find Specifications section: {e}")
                    return None

            if not specs_found:
                return None

            # Look for "More details" button - try multiple XPaths
            more_details_clicked = False
            more_details_xpaths = [
                "//button[contains(text(), 'More details')]",
                "//button[contains(., 'More details')]",
                "//button[@aria-label='More details']"
            ]

            for xpath in more_details_xpaths:
                try:
                    more_details_btn = self.driver.find_element(By.XPATH, xpath)
                    self.driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", more_details_btn)
                    time.sleep(1)
                    more_details_btn.click()
                    time.sleep(3)  # Increased wait time
                    more_details_clicked = True
                    print(f"  [OK] Clicked More details button")
                    break
                except:
                    continue

            if not more_details_clicked:
                print(f"  [WARNING] Could not find or click More details button")
                # Try to extract Model anyway (might be visible without clicking)

            # Extract Model - try multiple XPaths
            page_source = self.driver.page_source
            tree = html.fromstring(page_source)

            model_xpaths = [
                "//h3[text()='Model']/following-sibling::div//span",
                "//h3[contains(text(), 'Model')]/following-sibling::div//span",
                "//div[contains(@class, 'pb2')]//h3[text()='Model']/following-sibling::div/span",
                "//h3[normalize-space()='Model']/parent::div/following-sibling::div//span",
                self.xpaths.get('sku_model'),  # From database (last resort)
            ]

            for xpath in model_xpaths:
                if xpath:
                    model = self.extract_text_safe(tree, xpath)
                    # Validate: Model should be relatively short (< 50 chars typically)
                    if model and len(model) > 0 and len(model) < 100:
                        # Check it doesn't contain common page elements
                        model_lower = model.lower()
                        if not any(keyword in model_lower for keyword in ['skip to main', 'sign in', 'pickup', 'delivery', 'department']):
                            print(f"  [OK] Extracted Model: {model}")
                            return model

            print(f"  [WARNING] Could not extract valid Model from any XPath")
            return None

        except Exception as e:
            print(f"  [ERROR] Failed to extract model: {e}")
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

                view_all_btn = self.driver.find_element(By.XPATH, self.xpaths.get('view_all_reviews_button'))

                # Scroll to button with offset to avoid header
                self.driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", view_all_btn)
                time.sleep(1)

                # Use JavaScript click to avoid interception
                self.driver.execute_script("arguments[0].click();", view_all_btn)
                time.sleep(random.uniform(3, 4))
            except Exception as e:
                print(f"  [WARNING] Could not click View all reviews: {e}")
                return None

            # Extract reviews from the reviews page
            page_source = self.driver.page_source
            tree = html.fromstring(page_source)

            # Get review containers
            review_container_xpath = self.xpaths.get('review_container')
            if not review_container_xpath:
                return None

            review_containers = tree.xpath(review_container_xpath + '/div')

            if not review_containers:
                print(f"  [WARNING] No review containers found")
                return None

            reviews = []
            for idx, container in enumerate(review_containers):
                if len(reviews) >= 20:
                    break

                # Extract review text - looking for the actual review content
                # Based on HTML structure: find <p> with review text
                review_xpath = './/div[@data-testid="enhanced-review-content"]//p/span'
                review_elem = container.xpath(review_xpath)

                if review_elem:
                    review_text = review_elem[0].text_content().strip() if hasattr(review_elem[0], 'text_content') else str(review_elem[0]).strip()
                    if review_text and len(review_text) > 10:
                        reviews.append(review_text)

            # Format as "review1-content, review2-content, ..."
            if reviews:
                formatted = []
                for idx, review in enumerate(reviews[:20], 1):
                    formatted.append(f"review{idx}-{review}")
                return ', '.join(formatted)

            return None

        except Exception as e:
            print(f"  [WARNING] Failed to extract detailed reviews: {e}")
            import traceback
            traceback.print_exc()
            return None

    def scrape_detail_page(self, url_data):
        """Scrape detail page and extract information"""
        try:
            mother = url_data['mother']
            order = url_data['order']
            url = url_data['url']

            print(f"\n[{mother.upper()}][{order}] Accessing: {url[:80]}...")

            self.driver.get(url)
            time.sleep(random.uniform(4, 6))

            page_source = self.driver.page_source
            tree = html.fromstring(page_source)

            # Extract basic data using XPaths
            retailer_sku_name = self.extract_text_safe(tree, self.xpaths.get('product_name'))
            star_rating = self.extract_star_rating(tree)
            discount_type = self.extract_text_safe(tree, self.xpaths.get('discount_type'))
            savings = self.extract_text_safe(tree, self.xpaths.get('savings'))

            # Click Specifications and get Model - FIRST (before other interactions)
            sku_model = self.click_specifications_and_get_model()

            # Extract and classify all badges
            badges = self.extract_badges(tree)
            purchased_yesterday = badges['purchased_yesterday']
            added_to_carts = badges['added_to_carts']
            sku_popularity = badges['sku_popularity']

            # Extract shipping info (combine 2 parts)
            shipping_info = self.extract_shipping_info(tree)

            # Extract count of star ratings
            count_of_star_ratings = self.extract_count_of_star_ratings(tree)

            # Extract similar products
            similar_products = self.extract_similar_products(tree)

            # Extract detailed reviews (this will navigate to reviews page) - LAST
            detailed_review_content = self.extract_detailed_reviews()

            data = {
                'mother': mother,
                'order': order,
                'product_url': url,
                'Retailer_SKU_Name': retailer_sku_name,
                'Sku': sku_model,
                'Star_Rating': star_rating,
                'Number_of_ppl_purchased_yesterday': purchased_yesterday,
                'Number_of_ppl_added_to_carts': added_to_carts,
                'SKU_Popularity': sku_popularity,
                'Savings': savings,
                'Discount_Type': discount_type,
                'Shipping_Info': shipping_info,
                'Count_of_Star_Ratings': count_of_star_ratings,
                'Retailer_SKU_Name_similar': similar_products,
                'Detailed_Review_Content': detailed_review_content
            }

            # Save to database
            if self.save_to_db(data):
                self.total_collected += 1
                print(f"  [OK] Collected: {retailer_sku_name[:50] if retailer_sku_name else '[NO NAME]'}...")
                print(f"       Model: {sku_model or 'N/A'} | Star: {star_rating or 'N/A'}")
                print(f"       Purchased Yesterday: {purchased_yesterday or 'N/A'} | Added to Carts: {added_to_carts or 'N/A'}")
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
            # Temporarily disable autocommit for transaction
            self.db_conn.autocommit = False

            cursor = self.db_conn.cursor()

            # Insert to Walmart_tv_detail_crawled
            cursor.execute("""
                INSERT INTO Walmart_tv_detail_crawled
                (mother, "order", product_url, Retailer_SKU_Name, Sku, Star_Rating,
                 Number_of_ppl_purchased_yesterday, Number_of_ppl_added_to_carts,
                 SKU_Popularity, Savings, Discount_Type, Shipping_Info,
                 Count_of_Star_Ratings, Retailer_SKU_Name_similar, Detailed_Review_Content)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                data['mother'],
                data['order'],
                data['product_url'],
                data['Retailer_SKU_Name'],
                data['Sku'],
                data['Star_Rating'],
                data['Number_of_ppl_purchased_yesterday'],
                data['Number_of_ppl_added_to_carts'],
                data['SKU_Popularity'],
                data['Savings'],
                data['Discount_Type'],
                data['Shipping_Info'],
                data['Count_of_Star_Ratings'],
                data['Retailer_SKU_Name_similar'],
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

            # TEST MODE: Only process first product
            product_urls = product_urls[:1]
            print(f"[TEST MODE] Processing only first product")

            # Setup WebDriver
            self.setup_driver()

            # Scrape each detail page
            for idx, url_data in enumerate(product_urls, 1):
                print(f"\n{'='*80}")
                print(f"Processing {idx}/{len(product_urls)}")

                self.scrape_detail_page(url_data)

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


if __name__ == "__main__":
    try:
        crawler = WalmartDetailCrawler()
        crawler.run()
    except Exception as e:
        print(f"\n[FATAL ERROR] {e}")
        import traceback
        traceback.print_exc()

    print("\n[INFO] Crawler terminated. Exiting...")
