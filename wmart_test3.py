"""
Walmart Star Ratings Count Test Script
- Extract count_of_star_ratings only from test URLs
- No DB save, just log output
- XPaths loaded from database
- Bot detection bypass logic from wmart_tv_main1.py
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

from config import DB_CONFIG


# Test URLs
TEST_URLS = [
    "https://www.walmart.com/ip/Philips-75-Class-4K-Ultra-HD-2160p-Google-Smart-LED-TV-75PUL7552-F7-New/873651804",
    "https://www.walmart.com/ip/onn-32-Class-HD-720P-LED-Roku-Smart-Television-100012589/314022535",
    "https://www.walmart.com/ip/Hisense-43-inch-Full-HD-LED-TV/17310022307",
    "https://www.walmart.com/ip/Samsung-QN75Q7FBAFXZA/14331173819",
    "https://www.walmart.com/ip/VIZIO-75-Class-Quantum-4K-QLED-HDR-Smart-TV-NEW-VQD75M-0804/7772412357",
    "https://www.walmart.com/ip/Philips-32-Class-HD-720p-LED-TV-32PFL3453-F7-New/2587374896",
    "https://www.walmart.com/ip/75UA7500ZUA-AUSQ/14354358485",
    "https://www.walmart.com/ip/TCL-Google-TV-43Q51K/14574454877",
    "https://www.walmart.com/ip/Samsung-QN55Q7FBAFXZA/14356852276",
    "https://www.walmart.com/ip/Philips-70-Class-4K-Ultra-HD-2160p-Google-Smart-LED-Television-70PUL7553-F7/5200041769",
    "https://www.walmart.com/ip/VIZIO-65-Class-Quantum-Pro-4K-QLED-HDR-120Hz-Smart-TV-NEW-VQP65C-84/1110721814",
    "https://www.walmart.com/ip/LG-48-Class-4K-UHD-OLED-Web-OS-Smart-TV-with-Dolby-Vision-C4-Series-OLED48C4PUA/5446374202",
    "https://www.walmart.com/ip/Samsung-32-inch-Class-HD-H5000F-4K-Smart-TV-2025/15974661488",
    "https://www.walmart.com/ip/Philips-85-4K-UHD-2160p-Google-Smart-TV/17255214013",
    "https://www.walmart.com/ip/LG-77-Class-4K-UHD-OLED-C4-Series-120-Hz-Web-OS-24-Smart-TV-with-Dolby-Vision-OLED77C4PUA/5332753048",
    "https://www.walmart.com/ip/TCL-85-Q-Series-4K-UHD-HDR-QD-Mini-LED-Smart-TV-with-Google-TV-85QM7K/15118916244",
    "https://www.walmart.com/ip/SAMSUNG-50-Class-LS03B-The-Frame-QLED-4K-Smart-TV-QN50LS03BAFXZA/821644919",
    "https://www.walmart.com/ip/Samsung-QN65Q8FAAF-65-Smart-LED-LCD-TV-4K-UHDTV-High-Dynamic-Range-HDR-qn65q8faafxza/16272668792",
    "https://www.walmart.com/ip/Onn-65-Class-4K-2160p-Smart-LED-TV-100012587/18378210936",
    "https://www.walmart.com/ip/ONN-55-Class-4K-2160P-Roku-Smart-LED-TV-100012586/18402308641",
    "https://www.walmart.com/ip/onn-32-Class-HD-720P-Smart-LED-TV-100012589/18375223072",
    "https://www.walmart.com/ip/TCL-32Q2K-32-inch-Class-Q2K-Series-1080P-FHD-QLED-Smart-TV/17115511113",
    "https://www.walmart.com/ip/SAMSUNG-50-Class-DU8000B-Crystal-UHD-4K-Smart-TV-UN50DU8000BXZA-2024/5150283705",
    "https://www.walmart.com/ip/Supersonic-19-Class-LED-HDTV-with-USB-and-HDMI-Inputs/23142954",
    "https://www.walmart.com/ip/Samsung-QN85QN80F-85-inch-Class-Neo-QN80F-Series-QLED-4K-Smart-TV/15943420383",
    "https://www.walmart.com/ip/LG-55QNED70A-55-inch-Class-QNED70A-Series-4K-QNED-Smart-TV/17760415929",
    "https://www.walmart.com/ip/GPX-TE1982B-19-720p-60Hz-LED-HDTV/37451841",
    "https://www.walmart.com/ip/Emerson-24-720p-LED-TV-with-DVD-Player-ETD-2450/1905212031",
    "https://www.walmart.com/ip/55-Roku-Plus-Series/15942718986",
    "https://www.walmart.com/ip/Sony-BRAVIA-5-65-inch-Class-Mini-LED-4K-HDR-Google-TV-2025/16347258322",
    "https://www.walmart.com/ip/SYLVOX-Smart-Kitchen-TV-15-6-Android-Small-TV-1080P-FHD-Support-Rotated-Foldable-WiFi-Bluetooth-Cabinet-TV-Kitchen-Bedroom-RV-Camper-White/12403568804",
    "https://www.walmart.com/ip/Hisense-75-Class-U8-Series-Mini-LED-ULED-4K-UHD-Google-Smart-TV-75U8QG-2025-Model-QLED-Native-165Hz-5000-Nit-Dolby-Vision-IQ-Full-Array-Local-Dimming/16115514527",
    "https://www.walmart.com/ip/Onn-50-Class-4K-2160P-Roku-Smart-LED-TV-100012585/18299759637",
    "https://www.walmart.com/ip/JVC-LT-32MAR205-32-inch-Class-Roku-LED-Smart-TV/290868918",
    "https://www.walmart.com/ip/Element-55-4K-UHD-HDR-Google-TV-Frameless-E450AD55G/17434323142",
]


class WalmartStarRatingsTest:
    def __init__(self):
        self.driver = None
        self.wait = None
        self.db_conn = None
        self.xpaths = {}

    def connect_db(self):
        """Connect to PostgreSQL database"""
        try:
            self.db_conn = psycopg2.connect(**DB_CONFIG)
            self.db_conn.autocommit = True
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

    def setup_driver(self):
        """Setup Chrome WebDriver with undetected-chromedriver (bot detection bypass)"""
        options = uc.ChromeOptions()
        options.add_argument('--disable-blink-features=AutomationControlled')
        options.add_argument('--disable-dev-shm-usage')
        options.add_argument('--no-sandbox')
        options.add_argument('--disable-setuid-sandbox')
        options.add_argument('--start-maximized')
        options.add_argument('--disable-infobars')
        options.add_argument('--window-size=1920,1080')
        options.add_argument('--lang=en-US,en;q=0.9')

        prefs = {
            "profile.default_content_setting_values.notifications": 2,
            "credentials_enable_service": False,
            "profile.password_manager_enabled": False,
        }
        options.add_experimental_option("prefs", prefs)

        self.driver = uc.Chrome(options=options, use_subprocess=True)
        self.driver.set_page_load_timeout(120)
        self.wait = WebDriverWait(self.driver, 20)
        print("[OK] WebDriver setup complete (bot detection bypass enabled)")

    def check_robot_page(self, page_source):
        """Check if page is showing 'Robot or human?' challenge"""
        if "Robot or human?" in page_source or "Enter the characters you see below" in page_source:
            return True
        return False

    def handle_captcha(self):
        """Handle 'PRESS & HOLD' CAPTCHA if present"""
        try:
            print("[INFO] Checking for CAPTCHA...")

            page_content = self.driver.page_source.lower()
            if any(keyword in page_content for keyword in ['press & hold', 'press and hold', 'captcha', 'human verification']):
                print("[WARNING] CAPTCHA keywords found in page")
                print("[INFO] CAPTCHA detection - waiting 60 seconds for manual intervention...")
                print("[INFO] Please solve CAPTCHA manually if present")

                try:
                    self.driver.save_screenshot(f"captcha_screen_{int(time.time())}.png")
                    print("[INFO] Screenshot saved for debugging")
                except:
                    pass

                time.sleep(60)
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

            self.driver.get("https://www.walmart.com")
            time.sleep(random.uniform(8, 12))

            # Check for robot detection
            if self.check_robot_page(self.driver.page_source):
                print("[WARNING] Robot detection on homepage. Handling CAPTCHA...")
                self.handle_captcha()
                time.sleep(random.uniform(3, 5))

                if self.check_robot_page(self.driver.page_source):
                    print("[WARNING] Still showing robot detection, trying recovery...")

                    # Slow scroll down
                    print("[INFO] Scrolling slowly...")
                    for i in range(5):
                        scroll_amount = random.randint(150, 300)
                        self.driver.execute_script(f"window.scrollBy(0, {scroll_amount})")
                        time.sleep(random.uniform(1.5, 2.5))

                    self.driver.execute_script("window.scrollBy(0, -200)")
                    time.sleep(random.uniform(1, 2))

                print("[INFO] Waiting 30 seconds...")
                time.sleep(30)

                print("[INFO] Reloading page...")
                self.driver.refresh()
                time.sleep(random.uniform(10, 15))

                if self.check_robot_page(self.driver.page_source):
                    print("[ERROR] Still getting robot detection after recovery")
                    print("[INFO] Attempting to continue anyway...")

            # Simple homepage exploration
            print("[INFO] Exploring homepage...")
            time.sleep(random.uniform(2, 4))

            # Random scrolling
            for _ in range(random.randint(2, 4)):
                scroll_amount = random.randint(200, 500)
                self.driver.execute_script(f"window.scrollBy(0, {scroll_amount})")
                time.sleep(random.uniform(1, 2))

            # Scroll back to top
            print("[INFO] Scrolling back to top...")
            self.driver.execute_script("window.scrollTo(0, 0)")
            time.sleep(random.uniform(2, 3))

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
        except Exception:
            return None

    def extract_count_of_star_ratings(self, tree):
        """Extract total star rating count from star rating span
        New method: Extract from "4.4 stars out of 50630 reviews" text
        Returns: integer (e.g., 50630) or None
        """
        try:
            # ===== NEW METHOD: Extract from w_iUH7 span =====
            # <span class="w_iUH7">4.4 stars out of 50630 reviews</span>

            # Method 1: class attribute
            new_xpath_1 = "//span[@class='w_iUH7']"
            elements = tree.xpath(new_xpath_1)
            if elements:
                text = elements[0].text_content().strip()
                print(f"  [DEBUG] NEW Method 1 (w_iUH7): '{text}'")
                # Extract number before "reviews" - "4.4 stars out of 50630 reviews"
                match = re.search(r'out of\s*([\d,]+)\s*reviews?', text)
                if match:
                    count = int(match.group(1).replace(',', ''))
                    print(f"  [DEBUG] NEW Method 1 extracted: {count}")
                    return count

            # Method 2: contains class (for partial match)
            new_xpath_2 = "//span[contains(@class, 'w_iUH7')]"
            elements = tree.xpath(new_xpath_2)
            if elements:
                text = elements[0].text_content().strip()
                print(f"  [DEBUG] NEW Method 2 (contains w_iUH7): '{text}'")
                match = re.search(r'out of\s*([\d,]+)\s*reviews?', text)
                if match:
                    count = int(match.group(1).replace(',', ''))
                    print(f"  [DEBUG] NEW Method 2 extracted: {count}")
                    return count

            # Method 3: text pattern search
            new_xpath_3 = "//span[contains(text(), 'stars out of')]"
            elements = tree.xpath(new_xpath_3)
            if elements:
                text = elements[0].text_content().strip()
                print(f"  [DEBUG] NEW Method 3 (text pattern): '{text}'")
                match = re.search(r'out of\s*([\d,]+)\s*reviews?', text)
                if match:
                    count = int(match.group(1).replace(',', ''))
                    print(f"  [DEBUG] NEW Method 3 extracted: {count}")
                    return count

            # Method 4: absolute xpath from user
            new_xpath_4 = "//*[@id='maincontent']/section/main/div[2]/div[2]/div/div[2]/div/div[2]/div/div/div[2]/div/div/span"
            elements = tree.xpath(new_xpath_4)
            if elements:
                text = elements[0].text_content().strip()
                print(f"  [DEBUG] NEW Method 4 (absolute xpath): '{text}'")
                match = re.search(r'out of\s*([\d,]+)\s*reviews?', text)
                if match:
                    count = int(match.group(1).replace(',', ''))
                    print(f"  [DEBUG] NEW Method 4 extracted: {count}")
                    return count

            # ===== FALLBACK: Old method using star button breakdown =====
            print(f"  [DEBUG] NEW methods failed, trying old method...")

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
                except Exception:
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
                    except Exception:
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
                    except Exception:
                        pass

                # Store the count
                if count is not None:
                    star_counts[star_num] = count

            # Return total sum of all star ratings as integer
            if star_counts:
                total_star_ratings = sum(star_counts.values())
                print(f"  [DEBUG] OLD method - Star counts breakdown: {star_counts}")
                return total_star_ratings

            return None

        except Exception as e:
            print(f"  [WARNING] Failed to extract star rating counts: {e}")
            return None

    def extract_count_of_reviews(self, tree, page_source=None):
        """Extract total number of reviews from main page
        Example: '248 reviews' -> 248, '43 ratings' -> 43, 'No ratings yet' -> 0
        """
        try:
            # Check for "No ratings yet" in page source
            if page_source and "No ratings yet" in page_source:
                no_ratings_xpaths = [
                    "//span[contains(text(), 'No ratings yet')]",
                    "//*[@id='item-review-section']//span[contains(text(), 'No ratings yet')]"
                ]
                for xpath in no_ratings_xpaths:
                    result = tree.xpath(xpath)
                    if result:
                        print(f"  [DEBUG] Found 'No ratings yet' on page, setting count_of_reviews to 0")
                        return 0

            # Priority 1: Extract from "Showing 1-3 of 4,686 reviews" pattern
            showing_xpaths = [
                '//*[@id="item-review-section"]/div[7]/div[1]',
                '//*[@id="item-review-section"]//div[@role="heading" and contains(text(), "Showing")]',
                '//div[@role="heading" and contains(text(), "Showing") and contains(text(), "reviews")]'
            ]
            for xpath in showing_xpaths:
                result = tree.xpath(xpath)
                if result:
                    text = result[0].text_content().strip() if hasattr(result[0], 'text_content') else str(result[0]).strip()
                    match = re.search(r'of\s+([\d,]+)\s+reviews?', text, re.IGNORECASE)
                    if match:
                        count_str = match.group(1).replace(',', '')
                        count = int(count_str)
                        print(f"  [DEBUG] Extracted count from 'Showing X of Y reviews': {count}")
                        return count

            # Priority 2: Extract from "View all reviews (4,686)" button
            view_all_xpaths = [
                '//*[@id="item-review-section"]/div[8]/button',
                '//button[contains(text(), "View all reviews")]',
                '//*[@id="item-review-section"]//button[contains(text(), "View all reviews")]'
            ]
            for xpath in view_all_xpaths:
                result = tree.xpath(xpath)
                if result:
                    text = result[0].text_content().strip() if hasattr(result[0], 'text_content') else str(result[0]).strip()
                    match = re.search(r'\(([\d,]+)\)', text)
                    if match:
                        count_str = match.group(1).replace(',', '')
                        count = int(count_str)
                        print(f"  [DEBUG] Extracted count from 'View all reviews' button: {count}")
                        return count

            # Priority 3: Extract from "4,686 reviews" link
            reviews_link_xpaths = [
                "//*[@id='item-review-section']/div[2]/div[1]/div[1]/div/a",
                "//a[@link-identifier='seeAllReviewsStarRating']"
            ]
            for xpath in reviews_link_xpaths:
                result = tree.xpath(xpath)
                if result:
                    text = result[0].text_content().strip() if hasattr(result[0], 'text_content') else str(result[0]).strip()
                    match = re.search(r'([\d,]+)\s+reviews?', text, re.IGNORECASE)
                    if match:
                        count_str = match.group(1).replace(',', '')
                        count = int(count_str)
                        print(f"  [DEBUG] Extracted count from reviews link: {count}")
                        return count

            # Fallback: Check for "No ratings yet" -> return 0
            no_ratings_xpaths = [
                "//span[contains(text(), 'No ratings yet')]",
                "//*[@id='item-review-section']//span[contains(text(), 'No ratings yet')]"
            ]
            for xpath in no_ratings_xpaths:
                result = tree.xpath(xpath)
                if result:
                    return 0

            return None

        except Exception as e:
            print(f"  [WARNING] Failed to extract count of reviews: {e}")
            return None

    def scrape_url(self, url, retry_count=0):
        """Scrape single URL and extract count_of_star_ratings"""
        max_retries = 2

        try:
            print(f"\n{'='*80}")
            print(f"[INFO] Accessing: {url[:80]}...")

            # Check if window is still alive
            try:
                _ = self.driver.current_url
            except Exception:
                print(f"  [WARNING] Browser window crashed, restarting driver...")
                try:
                    self.driver.quit()
                except Exception:
                    pass
                self.setup_driver()
                self.initialize_session()
                print(f"  [OK] Driver restarted successfully")

            print(f"  [INFO] Loading page...")
            self.driver.get(url)
            time.sleep(random.uniform(6, 10))

            # Check for robot detection
            page_source = self.driver.page_source
            if self.check_robot_page(page_source):
                print(f"  [WARNING] Robot detection page detected.")

                if self.handle_captcha():
                    print("  [OK] CAPTCHA handled, checking page again...")
                    time.sleep(random.uniform(3, 5))
                    page_source = self.driver.page_source

                    if not self.check_robot_page(page_source):
                        print("  [OK] Robot detection bypassed after CAPTCHA")
                    else:
                        print("  [WARNING] Robot detection still present after CAPTCHA")

                # If still robot detected, retry
                if self.check_robot_page(self.driver.page_source):
                    if retry_count < max_retries:
                        print(f"  [WARNING] Retrying... {retry_count + 1}/{max_retries}")
                        wait_time = 30 + retry_count * 15
                        print(f"  [INFO] Waiting {wait_time} seconds before retry...")
                        time.sleep(wait_time)

                        print("  [INFO] Refreshing page...")
                        self.driver.refresh()
                        time.sleep(random.uniform(10, 15))

                        return self.scrape_url(url, retry_count + 1)
                    else:
                        print(f"  [ERROR] Failed to bypass robot detection after {max_retries} retries")
                        return None

            print(f"  [INFO] Page loaded, extracting data...")

            page_source = self.driver.page_source
            tree = html.fromstring(page_source)

            # Scroll to review section for lazy loading content
            try:
                self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight * 0.7);")
                time.sleep(2)
                page_source = self.driver.page_source
                tree = html.fromstring(page_source)
            except Exception:
                pass

            # Extract count of star ratings
            count_of_star_ratings = self.extract_count_of_star_ratings(tree)

            # Extract count of reviews
            count_of_reviews = self.extract_count_of_reviews(tree, page_source)

            print(f"  [RESULT] count_of_reviews = {count_of_reviews}, product_url = {url}")

            return {
                'count_of_star_ratings': count_of_star_ratings,
                'count_of_reviews': count_of_reviews,
                'product_url': url
            }

        except Exception as e:
            print(f"  [ERROR] Failed to scrape: {e}")
            import traceback
            traceback.print_exc()
            return None

    def run(self):
        """Main execution"""
        try:
            print("=" * 80)
            print("Walmart Star Ratings Count Test - Starting (Bot Detection Bypass)")
            print("=" * 80)

            # Connect to database and load xpaths
            if not self.connect_db():
                print("[ERROR] Database connection failed, exiting...")
                return

            if not self.load_xpaths():
                print("[ERROR] XPath loading failed, exiting...")
                return

            # Setup WebDriver
            self.setup_driver()

            # Initialize session (visit Walmart homepage first to avoid bot detection)
            if not self.initialize_session():
                print("[WARNING] Session initialization had issues, continuing anyway...")

            results = []

            # Scrape each URL
            for idx, url in enumerate(TEST_URLS, 1):
                print(f"\n{'='*80}")
                print(f"Processing {idx}/{len(TEST_URLS)}")

                result = self.scrape_url(url)
                results.append(result)

                # Random delay between requests
                if idx < len(TEST_URLS):
                    time.sleep(random.uniform(3, 5))

            # Print summary
            print("\n" + "=" * 80)
            print("SUMMARY (count_of_reviews, product_url)")
            print("=" * 80)

            success_count = 0

            for idx, result in enumerate(results, 1):
                if result is not None:
                    count_of_reviews = result.get('count_of_reviews')
                    product_url = result.get('product_url', '')
                    url_short = product_url.split('/')[-1][:40] if product_url else 'N/A'
                    print(f"  [{idx}] count_of_reviews = {count_of_reviews}, url = {url_short}")
                    success_count += 1
                else:
                    print(f"  [{idx}] None (failed)")

            print(f"\n  Total URLs: {len(TEST_URLS)}")
            print(f"  Success: {success_count}")
            print(f"  Failed: {len(TEST_URLS) - success_count}")
            print("=" * 80)

        except Exception as e:
            print(f"[ERROR] Test failed: {e}")
            import traceback
            traceback.print_exc()

        finally:
            if self.driver:
                self.driver.quit()
            if self.db_conn:
                self.db_conn.close()
                print("[OK] Database connection closed")
            print("\n[INFO] Test terminated")


if __name__ == "__main__":
    try:
        tester = WalmartStarRatingsTest()
        tester.run()
    except Exception as e:
        print(f"\n[FATAL ERROR] {e}")
        import traceback
        traceback.print_exc()

    print("\n[INFO] Test terminated. Exiting...")
