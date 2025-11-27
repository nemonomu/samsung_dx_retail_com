"""
Amazon TV Review Test - detailed_review_content extraction only
"""

import time
import random
import sys
import os
import pickle
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager
from lxml import html

# Configure stdout encoding for Windows console
if sys.stdout.encoding != 'utf-8':
    sys.stdout.reconfigure(encoding='utf-8', errors='replace')

# Cookie file path
COOKIE_FILE = 'amazon_cookies.pkl'


class AmazonReviewTest:
    def __init__(self):
        self.driver = None

    def setup_driver(self):
        """Setup Chrome driver"""
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
            return False

    def extract_detailed_reviews(self, product_url):
        """Extract detailed reviews from product detail page"""
        try:
            # Get current page HTML
            tree = html.fromstring(self.driver.page_source)

            # Extract reviews from detail page review section
            # Container: <ul id="cm-cr-dp-review-list" data-hook="top-customer-reviews-widget">
            # Each review: <li data-hook="review">
            # Review body: <span data-hook="review-body"> inner <span>
            review_xpath = '//ul[@id="cm-cr-dp-review-list"]//li[@data-hook="review"]//span[@data-hook="review-body"]//span'
            review_elements = tree.xpath(review_xpath)

            all_reviews = []
            if review_elements:
                for elem in review_elements:
                    review_text = elem.text_content().strip() if hasattr(elem, 'text_content') else str(elem).strip()
                    if review_text and len(review_text) > 10:
                        all_reviews.append(review_text)

            # Format as "1-review, 2-review, ..."
            if all_reviews:
                formatted_reviews = []
                for idx, review in enumerate(all_reviews, 1):
                    formatted_reviews.append(f"{idx}-{review}")
                return ", ".join(formatted_reviews)
            else:
                return None

        except Exception as e:
            print(f"  [WARNING] Failed to extract detailed reviews: {e}")
            return None

    # def extract_detailed_reviews_from_review_page(self, product_url):
    #     """Extract up to 20 detailed reviews from review pages (DISABLED - use detail page instead)"""
    #     try:
    #         # Get current page HTML
    #         tree = html.fromstring(self.driver.page_source)
    #
    #         # Extract "See more reviews" link
    #         review_link_xpaths = [
    #             '//*[@id="reviews-medley-footer"]/div[2]/a/@href',
    #             '//a[@data-hook="see-all-reviews-link-foot"]/@href',
    #             '//a[contains(text(), "See more reviews")]/@href'
    #         ]
    #
    #         review_link = None
    #         for xpath in review_link_xpaths:
    #             result = tree.xpath(xpath)
    #             if result:
    #                 review_link = result[0]
    #                 break
    #
    #         if not review_link:
    #             print("  [WARNING] Could not find review page link")
    #             return None
    #
    #         # Navigate to review page
    #         if review_link.startswith('http'):
    #             review_url = review_link
    #         else:
    #             review_url = "https://www.amazon.com" + review_link
    #
    #         self.driver.get(review_url)
    #         time.sleep(random.uniform(3, 4))
    #
    #         # Collect reviews from multiple pages
    #         all_reviews = []
    #         page_num = 1
    #         max_pages = 3  # Max 3 pages to get 20+ reviews
    #
    #         while len(all_reviews) < 20 and page_num <= max_pages:
    #             tree = html.fromstring(self.driver.page_source)
    #
    #             # Extract reviews from current page
    #             review_xpath = '//span[@data-hook="review-body"]/span'
    #             review_elements = tree.xpath(review_xpath)
    #
    #             if review_elements:
    #                 for elem in review_elements:
    #                     # Check if we already have 20 reviews
    #                     if len(all_reviews) >= 20:
    #                         break
    #
    #                     review_text = elem.text_content().strip() if hasattr(elem, 'text_content') else str(elem).strip()
    #                     if review_text and len(review_text) > 10:
    #                         all_reviews.append(review_text)
    #
    #             # Check if we have enough reviews after this page
    #             if len(all_reviews) >= 20:
    #                 break
    #
    #             # Find next page link
    #             next_button_xpaths = [
    #                 '//a[contains(text(), "Next page")]/@href',
    #                 '//*[@id="cm_cr-pagination_bar"]//li[@class="a-last"]/a/@href',
    #                 '//ul[@class="a-pagination"]//li[@class="a-last"]/a/@href'
    #             ]
    #
    #             next_link = None
    #             for xpath in next_button_xpaths:
    #                 result = tree.xpath(xpath)
    #                 if result:
    #                     next_link = result[0]
    #                     break
    #
    #             if next_link:
    #                 if next_link.startswith('http'):
    #                     next_url = next_link
    #                 else:
    #                     next_url = "https://www.amazon.com" + next_link
    #
    #                 self.driver.get(next_url)
    #                 time.sleep(random.uniform(2, 3))
    #                 page_num += 1
    #             else:
    #                 break
    #
    #         # Limit to 20 reviews and format as "1-review, 2-review, ..."
    #         reviews = all_reviews[:20]
    #         if reviews:
    #             formatted_reviews = []
    #             for idx, review in enumerate(reviews, 1):
    #                 formatted_reviews.append(f"{idx}-{review}")
    #             return ", ".join(formatted_reviews)
    #         else:
    #             return None
    #
    #     except Exception as e:
    #         print(f"  [WARNING] Failed to extract detailed reviews: {e}")
    #         return None

    def test_url(self, url):
        """Test a single URL"""
        print(f"\n{'='*80}")
        print(f"[TEST] URL: {url}")
        print(f"{'='*80}")

        self.driver.get(url)
        time.sleep(random.uniform(3, 5))

        detailed_review_content = self.extract_detailed_reviews(url)

        print(f"\n[RESULT] detailed_review_content:")
        if detailed_review_content:
            print(detailed_review_content)
        else:
            print("None")

    def run(self):
        """Run the test"""
        test_urls = [
            "https://www.amazon.com/Sony-Processor-Technology-Television-K-65XR80M2/dp/B0DYK7Y2YB/ref=zg_bs_g_172659_d_sccl_81/141-2609762-2663958?psc=1",
            "https://www.amazon.com/toshiba-fire-tv-32-inch-class-v35-series-hd-smart-tv/dp/B09L2DTGLJ/ref=zg_bs_g_172659_d_sccl_30/141-2609762-2663958?psc=1",
            "https://www.amazon.com/Fire-TV-Omni-QLED-Series-55-inch/dp/B0DD2F6VXT/ref=zg_bs_g_172659_d_sccl_93/141-2609762-2663958?psc=1"
        ]

        try:
            self.setup_driver()

            for url in test_urls:
                self.test_url(url)

        finally:
            if self.driver:
                self.driver.quit()
                print("\n[INFO] Driver closed")


if __name__ == "__main__":
    tester = AmazonReviewTest()
    tester.run()
