"""
Amazon TV count_of_reviews Test Script
- URL 접속 후 count_of_reviews만 추출하여 로그 출력
- DB 저장 없음
"""
import time
import random
import re
from lxml import html
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager


class CountOfReviewsTest:
    def __init__(self):
        self.driver = None
        self.setup_driver()

    def setup_driver(self):
        """Setup Chrome WebDriver"""
        options = Options()
        options.add_argument('--disable-blink-features=AutomationControlled')
        options.add_argument('--no-sandbox')
        options.add_argument('--disable-dev-shm-usage')
        options.add_argument('--disable-gpu')
        options.add_argument('--window-size=1920,1080')
        options.add_argument('user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36')

        service = Service(ChromeDriverManager().install())
        self.driver = webdriver.Chrome(service=service, options=options)
        self.driver.implicitly_wait(10)
        print("[OK] WebDriver initialized")

    def extract_count_of_star_ratings(self, tree):
        """Extract total star rating count from detail page"""
        try:
            xpaths = [
                '//*[@id="acrCustomerReviewText"]',
                '//span[@id="acrCustomerReviewText"]',
                '//span[contains(@data-hook, "total-review-count")]'
            ]
            for xpath in xpaths:
                elements = tree.xpath(xpath)
                if elements:
                    text = elements[0].text_content().strip() if hasattr(elements[0], 'text_content') else str(elements[0]).strip()
                    match = re.search(r'([\d,]+)', text)
                    if match:
                        return match.group(1)
            return None
        except Exception as e:
            print(f"  [ERROR] extract_count_of_star_ratings failed: {e}")
            return None

    def extract_count_of_reviews_from_review_page(self, product_url):
        """Extract count_of_reviews from review page (dt1.py 로직 그대로)"""
        try:
            tree = html.fromstring(self.driver.page_source)

            # Extract "See more reviews" link
            review_link_xpaths = [
                '//a[@data-hook="see-all-reviews-link-foot"]/@href',
                '//*[@id="reviews-medley-footer"]//a[contains(@href, "product-reviews")]/@href',
                '//*[@id="reviews-medley-footer"]/div[2]/a/@href',
                '//a[contains(text(), "See more reviews")]/@href',
                '//a[contains(text(), "See all reviews")]/@href',
                '//a[contains(@href, "product-reviews")]/@href'
            ]

            review_link = None
            for idx, xpath in enumerate(review_link_xpaths, 1):
                result = tree.xpath(xpath)
                if result:
                    review_link = result[0]
                    print(f"  [DEBUG] Found review link with XPath #{idx}: {review_link[:80]}...")
                    break
                else:
                    print(f"  [DEBUG] XPath #{idx} not found")

            if not review_link:
                print("  [WARNING] Could not find review page link")
                return None, None

            # Build review URL
            if review_link.startswith('http'):
                review_url = review_link
            else:
                review_url = "https://www.amazon.com" + review_link

            print(f"  [INFO] Navigating to review page: {review_url}")
            self.driver.get(review_url)
            time.sleep(random.uniform(3, 4))
            print(f"  [DEBUG] Actual URL after navigation: {self.driver.current_url}")

            # Wait for count_of_reviews element to load, then extract
            count_of_reviews = None
            extraction_source = None
            try:
                WebDriverWait(self.driver, 5).until(
                    EC.presence_of_element_located((By.XPATH, '//*[@id="filter-info-section"] | //div[@data-hook="cr-filter-info-review-rating-count"]'))
                )
                # Element loaded - extract count_of_reviews
                tree = html.fromstring(self.driver.page_source)
                count_xpaths = [
                    ('//*[@id="filter-info-section"]/div', 'filter-info-section'),
                    ('//div[@data-hook="cr-filter-info-review-rating-count"]', 'cr-filter-info-review-rating-count'),
                    ('//div[contains(@data-hook, "review-rating-count")]', 'review-rating-count')
                ]
                for xpath, xpath_name in count_xpaths:
                    count_elements = tree.xpath(xpath)
                    if count_elements:
                        count_text = count_elements[0].text_content().strip() if hasattr(count_elements[0], 'text_content') else str(count_elements[0]).strip()
                        if count_text:
                            print(f"  [DEBUG] count_text found: {count_text}")
                            print(f"  [DEBUG] XPath used: {xpath_name}")

                            # Try different regex patterns
                            regex_patterns = [
                                (r'([\d,]+)\s*customer\s*reviews?', 'customer reviews'),
                                (r'([\d,]+)\s*with\s*reviews?', 'with reviews'),
                                (r'([\d,]+)\s*reviews?', 'reviews')
                            ]
                            for pattern, pattern_name in regex_patterns:
                                match = re.search(pattern, count_text, re.IGNORECASE)
                                if match:
                                    count_of_reviews = match.group(1)
                                    extraction_source = f"review_page/{xpath_name}/{pattern_name}"
                                    print(f"  [OK] Extracted count_of_reviews: {count_of_reviews}")
                                    print(f"  [SOURCE] {extraction_source}")
                                    break
                            if count_of_reviews:
                                break
            except Exception as e:
                print(f"  [WARNING] count_of_reviews element not loaded: {e}")

            return count_of_reviews, extraction_source

        except Exception as e:
            print(f"  [ERROR] Failed to extract count_of_reviews: {e}")
            return None, None

    def test_url(self, url):
        """Test single URL - extract count_of_reviews and count_of_star_ratings"""
        print(f"\n{'='*80}")
        print(f"[TEST] URL: {url}")
        print(f"{'='*80}")

        try:
            # Navigate to product page
            print(f"  [INFO] Navigating to product page...")
            self.driver.get(url)
            time.sleep(random.uniform(3, 5))

            # Extract count_of_star_ratings from detail page
            tree = html.fromstring(self.driver.page_source)
            count_of_star_ratings = self.extract_count_of_star_ratings(tree)
            print(f"  [RESULT] count_of_star_ratings (from detail page): {count_of_star_ratings}")

            # Extract count_of_reviews from review page
            count_of_reviews, extraction_source = self.extract_count_of_reviews_from_review_page(url)

            # Navigate back to product page
            print(f"  [INFO] Navigating back to product page...")
            self.driver.get(url)
            time.sleep(2)

            # Final output
            print(f"\n  {'='*60}")
            print(f"  [FINAL RESULT]")
            print(f"  product_url: {url}")
            print(f"  count_of_star_ratings: {count_of_star_ratings} (source: detail_page)")
            print(f"  count_of_reviews: {count_of_reviews} (source: {extraction_source or 'N/A'})")

            # Compare values
            if count_of_star_ratings and count_of_reviews:
                cosr_int = int(str(count_of_star_ratings).replace(',', ''))
                cor_int = int(str(count_of_reviews).replace(',', ''))
                if cor_int > cosr_int:
                    print(f"  [WARNING] count_of_reviews({cor_int}) > count_of_star_ratings({cosr_int}) - INVALID!")
                elif cor_int == cosr_int:
                    print(f"  [INFO] count_of_reviews == count_of_star_ratings")
                else:
                    print(f"  [OK] count_of_reviews({cor_int}) <= count_of_star_ratings({cosr_int})")
            print(f"  {'='*60}\n")

            return {
                'url': url,
                'count_of_star_ratings': count_of_star_ratings,
                'count_of_reviews': count_of_reviews,
                'extraction_source': extraction_source
            }

        except Exception as e:
            print(f"  [ERROR] Test failed: {e}")
            return None

    def run(self, urls):
        """Run test for multiple URLs"""
        print(f"\n{'#'*80}")
        print(f"# Amazon TV count_of_reviews Test")
        print(f"# Testing {len(urls)} URLs")
        print(f"{'#'*80}\n")

        results = []
        for url in urls:
            result = self.test_url(url)
            if result:
                results.append(result)
            time.sleep(2)

        # Summary
        print(f"\n{'#'*80}")
        print(f"# SUMMARY")
        print(f"{'#'*80}")
        for r in results:
            cosr = r['count_of_star_ratings'] or 'N/A'
            cor = r['count_of_reviews'] or 'N/A'
            src = r.get('extraction_source') or 'N/A'
            print(f"  URL: {r['url']}")
            print(f"    count_of_star_ratings: {cosr} (source: detail_page)")
            print(f"    count_of_reviews: {cor} (source: {src})")
            print()

        self.driver.quit()
        print("[OK] Test completed")


def main():
    # Test URLs - 문제된 URL들 (ASIN 기준 중복 제거)
    test_urls = [
        # B0CB21YDMM - Samsung Frame
        "https://www.amazon.com/dp/B0CB21YDMM",
        # B0CFVQ8F3F - HOW PRIME ACCOUNT (ebook)
        "https://www.amazon.com/dp/B0CFVQ8F3F",
        # B0CSYYR7S8 - Samsung Commercial
        "https://www.amazon.com/dp/B0CSYYR7S8",
        # B0CWMLLQMK - Samsung QN43QN90D Renewed
        "https://www.amazon.com/dp/B0CWMLLQMK",
        # B0DF95Y41F - Sony K65XR70 Bravia
        "https://www.amazon.com/dp/B0DF95Y41F",
        # B0DPY5FXVW - VIZIO V4K75M
        "https://www.amazon.com/dp/B0DPY5FXVW",
        # B0FBDG3FGN - Sony K55XR80M2 QD-OLED
        "https://www.amazon.com/dp/B0FBDG3FGN",
        # B0FL9TWGLM - Samsung QN48S90FAEXZA
        "https://www.amazon.com/dp/B0FL9TWGLM",
        # B0FMRHHC7T - 100 Best All Time (DVD)
        "https://www.amazon.com/dp/B0FMRHHC7T",
    ]

    if not test_urls:
        print("[INFO] No test URLs provided.")
        print("[INFO] Edit this file and add URLs to test_urls list, or enter URL below:")
        url = input("Enter product URL (or press Enter to exit): ").strip()
        if url:
            test_urls = [url]
        else:
            print("[EXIT] No URL provided")
            return

    tester = CountOfReviewsTest()
    tester.run(test_urls)


if __name__ == "__main__":
    main()
