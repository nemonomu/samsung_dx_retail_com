"""
Amazon Review Test Crawler
- 수집 항목: count_of_reviews, detailed_review_content
- DB 저장: 없음
- 출력: count_of_reviews, detailed_review_content, collected_review_count, product_url
"""

import time
import random
import sys
import pickle
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

# Configure stdout encoding for Windows console
if sys.stdout.encoding != 'utf-8':
    sys.stdout.reconfigure(encoding='utf-8', errors='replace')

# Import account configuration
from config import AMAZON_ACCOUNTS

# Cookie file path (uses unsandev0004 - same as amazon_tv_dt1.py)
COOKIE_FILE = AMAZON_ACCOUNTS['unsandev0004']['cookie_file']

# Test URLs
TEST_URLS = [
    "https://www.amazon.com/Feihe-Television-Kitchen-Bedroom-Entertainment/dp/B0F8C3VTVY/ref=zg_bs_g_172659_d_sccl_29/145-0631719-6469732?psc=1",
    "https://www.amazon.com/VIZIO-Bluetooth-Compatible-Chromecast-VFD43M-0804/dp/B0D92TBJX2/ref=zg_bs_g_172659_d_sccl_81/145-0631719-6469732?psc=1",
    "https://www.amazon.com/Westinghouse-Smart-Compatible-Google-Assistant/dp/B0B3JMQG8Q/ref=zg_bs_g_172659_d_sccl_84/145-0631719-6469732?th=1",
    "https://www.amazon.com/Panasonic-Newest-55-inch-Adaptive-Refresh/dp/B0D97GB5HM/ref=zg_bs_g_172659_d_sccl_85/145-0631719-6469732?th=1",
    "https://www.amazon.com/TCL-43S450R-Assistant-Compatibility-Television/dp/B0C1J14MZT/ref=zg_bs_g_172659_d_sccl_86/145-0631719-6469732?th=1",
    "https://www.amazon.com/LG-Upscaling-Filmmaker-Orchestra-86QNED85AUA/dp/B0DYR35J2G/ref=zg_bs_g_172659_d_sccl_87/145-0631719-6469732?th=1",
    "https://www.amazon.com/VIZIO-720P-Smart-Dual-Band-WiFi/dp/B0D81P3D79/ref=zg_bs_g_172659_d_sccl_90/145-0631719-6469732?psc=1",
    "https://www.amazon.com/amazon-fire-tv-65-inch-omni-series-4k-smart-tv/dp/B08T6J1HG8/ref=zg_bs_g_172659_d_sccl_91/145-0631719-6469732?th=1",
    "https://www.amazon.com/Samsung-UN40F6000FFXZA-Essentials-Enhanced-Protection/dp/B0FDS8LJDW/ref=zg_bs_g_172659_d_sccl_92/145-0631719-6469732?th=1",
    "https://www.amazon.com/SAMSUNG-65-Inch-Tracking-Xcelerator-UN65DU8000/dp/B0CV9G5ST8/ref=zg_bs_g_172659_d_sccl_93/145-0631719-6469732?th=1",
    "https://www.amazon.com/insignia-fire-tv-43-inch-class-f30-series-4k-smart-tv/dp/B0CMDJ8TK3/ref=zg_bs_g_172659_d_sccl_95/145-0631719-6469732?psc=1",
    "https://www.amazon.com/toshiba-fire-tv-50-inch-class-c350-series-4k-smart-tv/dp/B0BMK5B4TF/ref=zg_bs_g_172659_d_sccl_96/145-0631719-6469732?psc=1",
    "https://www.amazon.com/Feihe-Screen-Kitchen-Camper-Bedroom/dp/B0FM8FM7MG/ref=zg_bs_g_172659_d_sccl_97/145-0631719-6469732?psc=1",
    "https://www.amazon.com/TCL-32S350G-Assistant-Compatible-Television/dp/B0C1HZ9HCM/ref=zg_bs_g_172659_d_sccl_98/145-0631719-6469732?th=1",
    "https://www.amazon.com/Sony-Exclusive-Features-PlayStation-K-85XR90/dp/B0CVPNFL8Z/ref=zg_bs_g_172659_d_sccl_99/145-0631719-6469732?th=1",
    "https://www.amazon.com/Sony-Exclusive-Features-PlayStation-K-65XR80/dp/B0CVQ6YLH7/ref=zg_bs_g_172659_d_sccl_100/145-0631719-6469732?th=1",
    "https://www.amazon.com/INSIGNIA-50-inch-4K-UHD-Fire-TV/dp/B0F19KLHG3/ref=sr_1_1?crid=RY3XOG3VC795&dib=eyJ2IjoiMSJ9.5JPdYN3krY1uOzwl96ODr07meUoShy-5mDAzm--YUJnluT2NrL3xbjVwjV_6NnK5t-H226x5VzJLQtos6gA74-WFGO-NHvxpb8-Peff1Ldz47pPGIzk4vldf0o2n8HXNxASoke8Xvk2LguObRwkWa72ufRIOsoubcrJTcZ8huOJ3LRjv6waAipVHt4_VE5nxlKmPJy3QmXYkBA4F3PexEgAv2BwrS9YISUxVRfeVAcs.mUHmg6gJCyQPy_6HRHgxdh_RCBqTE_cBR3nNL2VEfds&dib_tag=se&keywords=TV&qid=1767398422&sprefix=tv%2Caps%2C287&sr=8-1&th=1",
    "https://www.amazon.com/SAMSUNG-65-Inch-Processor-Upscaling-Xcelerator/dp/B0DXMJGQWC/ref=sr_1_2?crid=RY3XOG3VC795&dib=eyJ2IjoiMSJ9.5JPdYN3krY1uOzwl96ODr07meUoShy-5mDAzm--YUJnluT2NrL3xbjVwjV_6NnK5t-H226x5VzJLQtos6gA74-WFGO-NHvxpb8-Peff1Ldz47pPGIzk4vldf0o2n8HXNxASoke8Xvk2LguObRwkWa72ufRIOsoubcrJTcZ8huOJ3LRjv6waAipVHt4_VE5nxlKmPJy3QmXYkBA4F3PexEgAv2BwrS9YISUxVRfeVAcs.mUHmg6gJCyQPy_6HRHgxdh_RCBqTE_cBR3nNL2VEfds&dib_tag=se&keywords=TV&qid=1767398422&sprefix=tv%2Caps%2C287&sr=8-2&th=1"
]


class AmazonReviewTestCrawler:
    """Test crawler for count_of_reviews and detailed_review_content only"""

    def __init__(self):
        self.driver = None
        self.total_collected = 0
        korea_tz = pytz.timezone('Asia/Seoul')
        self.batch_id = datetime.now(korea_tz).strftime('%Y%m%d_%H%M%S')

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

            # Verify login status
            self.verify_login_status()

            print(f"[OK] Cookies loaded successfully")
            return True

        except Exception as e:
            print(f"[WARNING] Failed to load cookies: {e}")
            import traceback
            traceback.print_exc()
            return False

    def verify_login_status(self):
        """Verify if logged in by checking for account name or sign-in element"""
        try:
            tree = html.fromstring(self.driver.page_source)

            # Check for account name (logged in)
            account_xpaths = [
                '//*[@id="nav-link-accountList-nav-line-1"]',
                '//span[@id="nav-link-accountList-nav-line-1"]',
                '//a[@id="nav-link-accountList"]//span'
            ]

            for xpath in account_xpaths:
                account_text = self.extract_text_safe(tree, xpath)
                if account_text:
                    if "Sign in" in account_text or "Hello, sign in" in account_text:
                        print(f"[WARNING] NOT LOGGED IN - Found: '{account_text}'")
                        return False
                    else:
                        print(f"[OK] LOGGED IN - Account: '{account_text}'")
                        return True

            # Fallback: check page source for login indicators
            page_source = self.driver.page_source
            if "Hello, sign in" in page_source:
                print(f"[WARNING] NOT LOGGED IN - 'Hello, sign in' found in page")
                return False

            print(f"[WARNING] Could not determine login status")
            return None

        except Exception as e:
            print(f"[WARNING] Failed to verify login status: {e}")
            return None

    def verify_review_page_access(self):
        """Verify if review page is accessible (not blocked)"""
        try:
            tree = html.fromstring(self.driver.page_source)

            # Check for sign-in redirect or blocked page
            page_source = self.driver.page_source

            if "ap/signin" in self.driver.current_url:
                print(f"  [ERROR] REVIEW PAGE BLOCKED - Redirected to sign-in page")
                return False

            if "robot" in page_source.lower() or "captcha" in page_source.lower():
                print(f"  [ERROR] REVIEW PAGE BLOCKED - Robot/Captcha detected")
                return False

            # Check for review elements
            review_elements = tree.xpath('//span[@data-hook="review-body"]')
            if review_elements:
                print(f"  [OK] REVIEW PAGE ACCESSIBLE - Found {len(review_elements)} review elements")
                return True

            # Check for "no reviews" message
            if "no customer reviews" in page_source.lower():
                print(f"  [OK] REVIEW PAGE ACCESSIBLE - No reviews for this product")
                return True

            print(f"  [WARNING] REVIEW PAGE STATUS UNKNOWN - No review elements found")
            return None

        except Exception as e:
            print(f"  [WARNING] Failed to verify review page access: {e}")
            return None

    def verify_page_loaded(self):
        """Verify page is properly loaded before extraction"""
        try:
            # Wait for product title element (required)
            try:
                WebDriverWait(self.driver, 10).until(
                    EC.presence_of_element_located((By.ID, 'productTitle'))
                )
            except:
                print("  [WARNING] Product title element not found within timeout")
                return False

            return True

        except Exception as e:
            print(f"  [ERROR] Page verification failed: {e}")
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

    def extract_count_of_reviews(self, tree):
        """Extract count of reviews (format: '1,484' or '0' for no reviews)
        From detail page - fallback when review page extraction fails
        """
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
                    # Check for "No customer reviews" first -> return 0
                    if "No customer reviews" in reviews_text:
                        return "0"

                    # "1,484 ratings" -> "1,484"
                    # Extract only numbers and comma
                    match = re.search(r'([\d,]+)', reviews_text)
                    if match:
                        return match.group(1)

            # Fallback: Check for "No customer reviews" at specific location -> return 0
            no_reviews_xpaths = [
                '//*[@id="cm-cr-dp-review-header"]/h3/span',
                '//span[@data-hook="top-customer-reviews-title"]',
                '//div[@id="cm-cr-dp-review-header"]//span[contains(text(), "No customer reviews")]',
                '//span[contains(text(), "No customer reviews")]'
            ]

            for xpath in no_reviews_xpaths:
                text = self.extract_text_safe(tree, xpath)
                if text and "No customer reviews" in text:
                    return "0"

            return None

        except Exception as e:
            print(f"  [WARNING] Failed to extract count of reviews: {e}")
            return None

    def extract_detailed_reviews(self, product_url):
        """Extract detailed reviews from product detail page (fallback method)"""
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

    def extract_detailed_reviews_from_review_page(self, product_url):
        """Extract up to 20 detailed reviews and count_of_reviews from review pages

        Returns:
            tuple: (detailed_review_content, count_of_reviews, total_duplicates)

        Note: Duplicate detection is based on exact content match (entire review text)
        """
        try:
            # Get current page HTML
            tree = html.fromstring(self.driver.page_source)

            # Extract "See more reviews" link
            # Priority: data-hook attribute is most reliable
            review_link_xpaths = [
                '//a[@data-hook="see-all-reviews-link-foot"]/@href',  # Most reliable - data-hook attribute
                '//*[@id="reviews-medley-footer"]//a[contains(@href, "product-reviews")]/@href',  # Footer container
                '//*[@id="reviews-medley-footer"]/div[2]/a/@href',  # Legacy structure
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
                print("  [WARNING] Could not find review page link, falling back to detail page reviews")
                return self.extract_detailed_reviews(product_url), None, 0

            # Extract ASIN from review link
            asin_match = re.search(r'/product-reviews/([A-Z0-9]{10})', review_link)
            if asin_match:
                asin = asin_match.group(1)
                print(f"  [DEBUG] Extracted ASIN from review link: {asin}")
            else:
                asin = None
                print(f"  [WARNING] Could not extract ASIN from review link")

            # Use original review link (Amazon requires specific ref parameter for pagination)
            if review_link.startswith('http'):
                review_url = review_link
            else:
                review_url = "https://www.amazon.com" + review_link

            print(f"  [INFO] Navigating to review page: {review_url[:100]}...")
            self.driver.get(review_url)
            time.sleep(random.uniform(3, 4))
            print(f"  [DEBUG] Actual URL after navigation: {self.driver.current_url[:100]}...")

            # Verify review page access (check login/block status)
            review_access = self.verify_review_page_access()
            if review_access is False:
                print(f"  [ERROR] Review page access failed - cookie may be expired")
                return self.extract_detailed_reviews(product_url), None, 0

            # Wait for count_of_reviews element to load, then extract
            count_of_reviews = None
            try:
                WebDriverWait(self.driver, 5).until(
                    EC.presence_of_element_located((By.XPATH, '//*[@id="filter-info-section"] | //div[@data-hook="cr-filter-info-review-rating-count"]'))
                )
                # Element loaded - extract count_of_reviews
                tree = html.fromstring(self.driver.page_source)
                count_xpaths = [
                    '//*[@id="filter-info-section"]/div',
                    '//div[@data-hook="cr-filter-info-review-rating-count"]',
                    '//div[contains(@data-hook, "review-rating-count")]'
                ]
                for xpath in count_xpaths:
                    count_elements = tree.xpath(xpath)
                    if count_elements:
                        count_text = count_elements[0].text_content().strip() if hasattr(count_elements[0], 'text_content') else str(count_elements[0]).strip()
                        if count_text:
                            print(f"  [DEBUG] count_text found: {count_text[:100]}...")
                            match = re.search(r'([\d,]+)\s*customer\s*reviews?', count_text, re.IGNORECASE)
                            if not match:
                                match = re.search(r'([\d,]+)\s*with\s*reviews?', count_text, re.IGNORECASE)
                            if not match:
                                match = re.search(r'([\d,]+)\s*reviews?', count_text, re.IGNORECASE)
                            if match:
                                count_of_reviews = match.group(1)
                                print(f"  [OK] Extracted count_of_reviews from review page: {count_of_reviews}")
                                break
            except:
                print("  [WARNING] count_of_reviews element not loaded - skipping extraction")

            # Collect reviews from first page (max 10 reviews per page)
            all_reviews = []

            # Debug: Print current URL to verify we're on review page
            current_url = self.driver.current_url
            print(f"  [DEBUG] Current URL before extracting reviews: {current_url[:100]}...")

            tree = html.fromstring(self.driver.page_source)

            # Extract reviews from first page
            review_xpath = '//span[@data-hook="review-body"]/span'
            review_elements = tree.xpath(review_xpath)

            if review_elements:
                for elem in review_elements[:10]:  # Max 10 from first page
                    review_text = elem.text_content().strip() if hasattr(elem, 'text_content') else str(elem).strip()
                    if review_text and len(review_text) > 10:
                        all_reviews.append(review_text)

            print(f"  [INFO] Review page 1: collected {len(all_reviews)} reviews")

            # Check if we need to go to next page (count_of_reviews > 10)
            count_int = 0
            if count_of_reviews:
                try:
                    count_int = int(str(count_of_reviews).replace(',', ''))
                except:
                    count_int = 0

            # Store collected reviews for duplicate check
            collected_reviews = set(all_reviews)

            # Track total duplicates across all pages
            total_duplicates = 0

            # If 20+ reviews exist and we have less than 20, go to next pages (up to 3 pages)
            current_page = 1
            max_pages = 3

            while len(all_reviews) < 20 and current_page < max_pages and count_int >= 20:
                try:
                    # Scroll to bottom of page to ensure Next button is visible
                    self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                    time.sleep(1)

                    # Find and click Next page button
                    next_button = self.driver.find_element(By.XPATH, '//li[@class="a-last"]/a')
                    print(f"  [INFO] Clicking Next page button (page {current_page} -> {current_page + 1})...")
                    self.driver.execute_script("arguments[0].click();", next_button)
                    time.sleep(random.uniform(3, 5))

                    current_page += 1

                    # Verify we're on expected page
                    current_url = self.driver.current_url
                    if f'pageNumber={current_page}' not in current_url:
                        print(f"  [WARNING] Page {current_page} not loaded properly, current URL: {current_url[:80]}...")
                    else:
                        print(f"  [DEBUG] Confirmed on page {current_page}: {current_url[:80]}...")

                    # Extract reviews from current page
                    tree = html.fromstring(self.driver.page_source)
                    review_elements = tree.xpath(review_xpath)

                    print(f"  [DEBUG] Review page {current_page}: found {len(review_elements)} review elements")

                    # Collect reviews with duplicate check
                    page_count = 0
                    duplicates = 0
                    if review_elements:
                        for elem in review_elements[:10]:  # Max 10 per page
                            if len(all_reviews) >= 20:
                                break
                            review_text = elem.text_content().strip() if hasattr(elem, 'text_content') else str(elem).strip()
                            if review_text and len(review_text) > 10:
                                # Skip if duplicate
                                if review_text in collected_reviews:
                                    duplicates += 1
                                    continue
                                all_reviews.append(review_text)
                                collected_reviews.add(review_text)
                                page_count += 1

                    if duplicates > 0:
                        print(f"  [WARNING] Found {duplicates} duplicate reviews on page {current_page}")
                        total_duplicates += duplicates
                    print(f"  [INFO] Review page {current_page}: added {page_count} reviews, total {len(all_reviews)} reviews")

                    # If no new reviews found, stop pagination
                    if page_count == 0:
                        print(f"  [INFO] No new reviews on page {current_page}, stopping pagination")
                        break

                except Exception as e:
                    print(f"  [WARNING] Could not navigate to page {current_page + 1}: {e}")
                    break

            # Navigate back to product page
            print(f"  [INFO] Navigating back to product page...")
            self.driver.get(product_url)
            time.sleep(random.uniform(2, 3))

            # Limit to 20 reviews and format as "1-review, 2-review, ..."
            reviews = all_reviews[:20]
            if reviews:
                formatted_reviews = []
                for idx, review in enumerate(reviews, 1):
                    formatted_reviews.append(f"{idx}-{review}")
                print(f"  [OK] Collected {len(reviews)} detailed reviews from review page (duplicates: {total_duplicates})")
                return ", ".join(formatted_reviews), count_of_reviews, total_duplicates
            else:
                print("  [WARNING] No reviews found on review page, falling back to detail page reviews")
                return self.extract_detailed_reviews(product_url), count_of_reviews, total_duplicates

        except Exception as e:
            print(f"  [WARNING] Failed to extract detailed reviews from review page: {e}")
            import traceback
            traceback.print_exc()
            return self.extract_detailed_reviews(product_url), None, 0

    def scrape_page(self, url):
        """Scrape a single page for reviews"""
        try:
            print(f"\n{'='*80}")
            print(f"[URL] {url[:100]}...")

            self.driver.get(url)
            time.sleep(random.uniform(3, 5))

            # Verify page is properly loaded
            page_loaded = self.verify_page_loaded()

            if not page_loaded:
                # Retry once
                print("  [RETRY] Page verification failed - retrying...")
                time.sleep(3)
                self.driver.get(url)
                time.sleep(random.uniform(3, 5))
                page_loaded = self.verify_page_loaded()

                if not page_loaded:
                    print("  [ERROR] Page failed to load after retry")
                    return False

            print("  [INFO] Page loaded, extracting reviews...")

            # Get final URL (after any redirects)
            final_url = self.driver.current_url

            # Extract detailed reviews and count_of_reviews from review page
            detailed_review_content, count_of_reviews, duplicate_count = self.extract_detailed_reviews_from_review_page(url)

            # Verify page is properly loaded after returning from review page
            if not self.verify_page_loaded():
                print("  [WARNING] Page verification failed after review page return")

            # Fallback: If count_of_reviews not found from review page, try detail page
            if not count_of_reviews:
                tree = html.fromstring(self.driver.page_source)
                count_of_reviews = self.extract_count_of_reviews(tree)
                if count_of_reviews:
                    print(f"  [INFO] Fallback - extracted count_of_reviews from detail page: {count_of_reviews}")

            # Count collected reviews from detailed_review_content
            collected_review_count = 0
            if detailed_review_content:
                try:
                    # Count reviews by counting "N-" patterns
                    collected_review_count = len([r for r in detailed_review_content.split(', ') if r and '-' in r])
                except:
                    collected_review_count = 0

            # Print results
            print(f"\n{'='*80}")
            print(f"[RESULT]")
            print(f"  product_url: {final_url}")
            print(f"  count_of_reviews: {count_of_reviews or 'N/A'}")
            print(f"  collected_review_count: {collected_review_count}")
            print(f"  duplicate_review_count: {duplicate_count}")
            if detailed_review_content:
                # Truncate for display
                display_content = detailed_review_content[:500] + '...' if len(detailed_review_content) > 500 else detailed_review_content
                print(f"  detailed_review_content: {display_content}")
            else:
                print(f"  detailed_review_content: N/A")
            print(f"{'='*80}")

            self.total_collected += 1
            return True

        except Exception as e:
            print(f"  [ERROR] Failed to scrape page: {e}")
            import traceback
            traceback.print_exc()
            return False

    def run(self):
        """Main execution"""
        try:
            print("="*80)
            print(f"Amazon Review Test Crawler - Starting (Batch ID: {self.batch_id})")
            print(f"Collecting: count_of_reviews, detailed_review_content only")
            print(f"Account: unsandev0004")
            print(f"Test URLs: {len(TEST_URLS)}")
            print("="*80)

            # Step 1: Setup WebDriver
            print("\n[STEP 1/2] Setting up WebDriver...")
            self.setup_driver()
            print("[OK] WebDriver ready")

            # Step 2: Scrape each URL
            print(f"\n[STEP 2/2] Starting to scrape {len(TEST_URLS)} URLs...")

            for idx, url in enumerate(TEST_URLS, 1):
                print(f"\n[{idx}/{len(TEST_URLS)}] Processing...")
                self.scrape_page(url)

                # Random delay between requests
                if idx < len(TEST_URLS):
                    delay = random.uniform(2, 4)
                    print(f"[INFO] Waiting {delay:.1f} seconds before next request...")
                    time.sleep(delay)

            print("\n" + "="*80)
            print(f"Test Crawling completed!")
            print(f"Total processed: {self.total_collected}/{len(TEST_URLS)}")
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


if __name__ == "__main__":
    try:
        crawler = AmazonReviewTestCrawler()
        crawler.run()
    except Exception as e:
        print(f"\n[FATAL ERROR] {e}")
        import traceback
        traceback.print_exc()

    print("\n[INFO] Test crawler terminated.")
