"""
Walmart Star Ratings Count Test Script
- Extract count_of_star_ratings only from test URLs
- No DB save, just log output
- XPaths loaded from database
"""
import time
import random
import psycopg2
import undetected_chromedriver as uc
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from lxml import html
import re

from config import DB_CONFIG


# Test URLs
TEST_URLS = [
    "https://www.walmart.com/ip/Samsung-65-Class-Crystal-UHD-U7900F-4K-Smart-TV-2025-UN65U7900FFXZA/16815105564",
    "https://www.walmart.com/ip/Philips-55-Class-4K-UHD-2160p-Google-Gaming-TV/17233061084",
    "https://www.walmart.com/ip/onn-32-Class-HD-720P-LED-Roku-Smart-Television-100012589/314022535",
    "https://www.walmart.com/ip/onn-50-Class-4K-UHD-2160P-LED-Roku-Smart-Television-HDR-100012585/300694285",
    "https://www.walmart.com/ip/Samsung-75-Class-Crystal-UHD-U7900F-4K-Smart-TV-2025-UN75U7900FFXZA/16785918403",
    "https://www.walmart.com/ip/Philips-50-Class-4K-UHD-2160p-Google-Gaming-TV/17216450897",
    "https://www.walmart.com/ip/Philips-55-Class-4K-Ultra-HD-2160p-Google-Smart-LED-TV-55PUL7552-F7-New/811665748",
    "https://www.walmart.com/ip/Hisense-40-Class-FHD-1080P-Roku-Smart-LED-TV-40H4030F1/470905078",
    "https://www.walmart.com/ip/GPX-40-DLED-TV-TE4019BP/688968343",
    "https://www.walmart.com/ip/86UA7500ZUA-AUSQ/14363012836",
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
        """Setup Chrome WebDriver with undetected-chromedriver"""
        options = uc.ChromeOptions()
        options.page_load_strategy = 'none'
        options.add_argument('--disable-dev-shm-usage')
        options.add_argument('--no-sandbox')
        options.add_argument('--disable-gpu')
        options.add_argument('--window-size=1920,1080')
        options.add_argument('--lang=en-US,en;q=0.9')

        prefs = {
            "profile.default_content_setting_values.notifications": 2,
            "credentials_enable_service": False,
            "profile.password_manager_enabled": False,
        }
        options.add_experimental_option("prefs", prefs)

        self.driver = uc.Chrome(options=options)
        self.driver.set_page_load_timeout(120)
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

    def scrape_url(self, url):
        """Scrape single URL and extract count_of_star_ratings"""
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
                print(f"  [OK] Driver restarted successfully")

            print(f"  [INFO] Loading page...")
            self.driver.get(url)
            time.sleep(random.uniform(4, 6))

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

            print(f"  [RESULT] count_of_star_ratings = {count_of_star_ratings}")

            return count_of_star_ratings

        except Exception as e:
            print(f"  [ERROR] Failed to scrape: {e}")
            import traceback
            traceback.print_exc()
            return None

    def run(self):
        """Main execution"""
        try:
            print("=" * 80)
            print("Walmart Star Ratings Count Test - Starting")
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

            results = []

            # Scrape each URL
            for idx, url in enumerate(TEST_URLS, 1):
                print(f"\n{'='*80}")
                print(f"Processing {idx}/{len(TEST_URLS)}")

                count = self.scrape_url(url)
                results.append({
                    'url': url,
                    'count_of_star_ratings': count
                })

                # Random delay between requests
                if idx < len(TEST_URLS):
                    time.sleep(random.uniform(3, 5))

            # Print summary
            print("\n" + "=" * 80)
            print("SUMMARY")
            print("=" * 80)

            total_star_ratings = 0
            success_count = 0

            for idx, result in enumerate(results, 1):
                url_short = result['url'].split('/')[-1][:30]
                count = result['count_of_star_ratings']

                if count is not None:
                    print(f"  [{idx}] {url_short}... -> {count}")
                    total_star_ratings += count
                    success_count += 1
                else:
                    print(f"  [{idx}] {url_short}... -> None (failed)")

            print(f"\n  Total URLs: {len(TEST_URLS)}")
            print(f"  Success: {success_count}")
            print(f"  Failed: {len(TEST_URLS) - success_count}")
            print(f"  Total Star Ratings Sum: {total_star_ratings}")
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
