"""
Best Buy TV Test - Extract product name, star rating, review count, screen size
"""

import time
import random
import re
import sys
import undetected_chromedriver as uc
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException
from lxml import html

# Configure stdout encoding for Windows console
if sys.stdout.encoding != 'utf-8':
    sys.stdout.reconfigure(encoding='utf-8', errors='replace')


class BestBuyTest:
    def __init__(self):
        self.driver = None

    def setup_driver(self):
        """Setup Chrome driver"""
        print("[INFO] Setting up Chrome driver...")
        options = uc.ChromeOptions()
        options.page_load_strategy = 'eager'

        self.driver = uc.Chrome(options=options)
        self.driver.set_page_load_timeout(120)
        self.driver.maximize_window()
        print("[OK] Driver setup complete")

    def extract_retailer_sku_name(self, tree):
        """Retailer_SKU_Name extraction"""
        try:
            xpaths = [
                '//h1[contains(@class, "h4")]',
                '//div[@class="sku-title"]//h1'
            ]
            for xpath in xpaths:
                elem = tree.xpath(xpath)
                if elem:
                    return elem[0].text_content().strip()
            return None
        except Exception as e:
            print(f"  [ERROR] Retailer_SKU_Name extraction failed: {e}")
            return None

    def extract_star_rating(self, tree):
        """Star Rating extraction"""
        try:
            # Method 1: Container-based extraction
            container_xpaths = [
                '/html/body/div[5]/div[4]/div[1]',
                '//div[@class="order-2 t3V0AOwowrTfUzPn "]',
                '//div[contains(@class, "order-2")]'
            ]

            price_container = None
            for xpath in container_xpaths:
                containers = tree.xpath(xpath)
                if containers:
                    price_container = containers[0]
                    break

            if price_container is not None:
                rating_xpaths = [
                    './/div/div[3]/a/div/span[1]',
                    './/span[@class="font-weight-medium  font-weight-bold order-1"]',
                    './/span[contains(@class, "font-weight-bold") and contains(@class, "order-1")]',
                    './/span[@aria-hidden="true"][contains(@class, "order-1")]'
                ]

                for xpath in rating_xpaths:
                    elem = price_container.xpath(xpath)
                    if elem:
                        rating = elem[0].text_content().strip()
                        if rating and re.match(r'^\d+\.\d+$', rating):
                            return rating

            # Method 2: visually-hidden p tag (new)
            hidden_xpaths = [
                '//p[@class="visually-hidden"][contains(text(), "Rating")]',
                '//p[contains(@class, "visually-hidden")][contains(text(), "out of 5 stars")]'
            ]
            for xpath in hidden_xpaths:
                elem = tree.xpath(xpath)
                if elem:
                    text = elem[0].text_content().strip()
                    # "Rating 4.8 out of 5 stars with 286 reviews"
                    match = re.search(r'Rating\s+([\d.]+)\s+out of', text)
                    if match:
                        return match.group(1)

            return None
        except Exception as e:
            print(f"  [ERROR] Star Rating extraction failed: {e}")
            return None

    def extract_count_of_reviews(self, tree):
        """Count of Reviews extraction"""
        try:
            # Method 1: Container-based extraction
            container_xpaths = [
                '/html/body/div[5]/div[4]/div[1]',
                '//div[@class="order-2 t3V0AOwowrTfUzPn "]',
                '//div[contains(@class, "order-2")]'
            ]

            price_container = None
            for xpath in container_xpaths:
                containers = tree.xpath(xpath)
                if containers:
                    price_container = containers[0]
                    break

            if price_container is not None:
                review_xpaths = [
                    './/div/div[3]/a/div/span[2]',
                    './/span[@class="c-reviews order-2"]',
                    './/span[contains(@class, "c-reviews")]'
                ]

                for xpath in review_xpaths:
                    elem = price_container.xpath(xpath)
                    if elem:
                        text = elem[0].text_content().strip()
                        match = re.search(r'\(?([\d,]+)\s*reviews?\)?', text, re.IGNORECASE)
                        if match:
                            return match.group(1).replace(',', '')

            # Method 2: visually-hidden p tag (new)
            hidden_xpaths = [
                '//p[@class="visually-hidden"][contains(text(), "reviews")]',
                '//p[contains(@class, "visually-hidden")][contains(text(), "out of 5 stars")]'
            ]
            for xpath in hidden_xpaths:
                elem = tree.xpath(xpath)
                if elem:
                    text = elem[0].text_content().strip()
                    # "Rating 4.8 out of 5 stars with 286 reviews"
                    match = re.search(r'with\s+([\d,]+)\s+reviews', text)
                    if match:
                        return match.group(1).replace(',', '')

            return None
        except Exception as e:
            print(f"  [ERROR] Count of Reviews extraction failed: {e}")
            return None

    def extract_screen_size(self, tree):
        """Screen Size extraction"""
        try:
            xpaths = [
                '/html/body/div[5]/div[4]/div[2]/div/div[3]/div[1]/button[2]/div/div/div[2]',
                '//div[contains(text(), "Screen Size Class")]/following-sibling::div[@class="flex font-500 items-center"]',
                '//div[text()="Screen Size Class"]/..//div[contains(@class, "flex font-500")]'
            ]

            for xpath in xpaths:
                elem = tree.xpath(xpath)
                if elem:
                    screen_size_text = elem[0].text_content().strip()
                    match = re.search(r'(\d+\s*inches)', screen_size_text)
                    if match:
                        return match.group(1)
                    return screen_size_text
            return None
        except Exception as e:
            print(f"  [ERROR] Screen Size extraction failed: {e}")
            return None

    def test_url(self, url):
        """Test a single URL"""
        print(f"\n{'='*80}")
        print(f"[TEST] URL: {url}")
        print(f"{'='*80}")

        try:
            self.driver.get(url)
            time.sleep(random.uniform(3, 5))

            # Wait for page load
            try:
                wait = WebDriverWait(self.driver, 20)
                wait.until(EC.presence_of_element_located(
                    (By.XPATH, '//h1[contains(@class, "h4") or contains(@class, "heading")]')
                ))
                print("[OK] Page load complete")
            except TimeoutException:
                print("[WARNING] Page load timeout - continuing anyway")

            # Get page source
            page_source = self.driver.page_source
            tree = html.fromstring(page_source)

            # Extract data
            retailer_sku_name = self.extract_retailer_sku_name(tree)
            star_rating = self.extract_star_rating(tree)
            count_of_reviews = self.extract_count_of_reviews(tree)
            screen_size = self.extract_screen_size(tree)

            print(f"\n[RESULTS]")
            print(f"  retailer_sku_name: {retailer_sku_name}")
            print(f"  star_rating: {star_rating}")
            print(f"  count_of_reviews: {count_of_reviews}")
            print(f"  screen_size: {screen_size}")

        except Exception as e:
            print(f"[ERROR] Failed to test URL: {e}")

    def run(self):
        """Run the test"""
        test_urls = [
            "https://www.bestbuy.com/product/samsung-75-class-qn90f-series-neo-qled-mini-led-4k-uhd-samsungvision-ai-smart-tizen-tv-2025/J3ZYG2FZ87",
            "https://www.bestbuy.com/product/samsung-55-class-q60d-series-qled-4k-smart-tizen-tv-2024/J3ZYG2HLCK",
            "https://www.bestbuy.com/product/hisense-65-class-a6-4k-uhd-series-led-hdr-smart-fire-tv-2025/J3Z9Z42923/sku/6647936"
        ]

        try:
            self.setup_driver()

            for url in test_urls:
                self.test_url(url)
                time.sleep(random.uniform(2, 3))

        finally:
            if self.driver:
                self.driver.quit()
                print("\n[INFO] Driver closed")


if __name__ == "__main__":
    tester = BestBuyTest()
    tester.run()
