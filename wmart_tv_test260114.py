"""
Walmart TV Test Crawler - Extract only key fields
Extracts: count_of_reviews, detailed_review_content, product_url
"""

import sys
import time
import random
import re
from datetime import datetime

import undetected_chromedriver as uc
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from lxml import html

# UTF-8 encoding for Windows console
if sys.platform == 'win32':
    sys.stdout.reconfigure(encoding='utf-8', errors='replace')

# Test URLs
TEST_URLS = [
    "https://www.walmart.com/ip/TCL-55-Class-S4-55S41BR-4K-UHD-HDR-LED-Smart-TV-with-Roku-TV-NEW-2024/6832762774",
    "https://www.walmart.com/ip/TCL-55-Class-S4-55S41BR-4K-UHD-HDR-LED-Smart-TV-with-Roku-TV-NEW-2024/6832762774",
    "https://www.walmart.com/ip/onn-65-Class-4K-UHD-2160P-LED-Roku-Smart-Television-HDR-100012587/476550098",
    "https://www.walmart.com/ip/Naxa-Electronics-NT-3206-32-Class-Widescreen-HD-TV-Black/1030187073",
    "https://www.walmart.com/ip/TCL-55-Class-S4-55S451-4K-UHD-HDR-Smart-TV-with-Roku-TV/533225197",
    "https://www.walmart.com/ip/Hisense-58-Class-4K-UHD-LED-LCD-Roku-Smart-TV-HDR-R6-Series-58R6E3/587182688",
    "https://www.walmart.com/ip/TCL-65-Class-S4-65S451-4K-UHD-HDR-Smart-TV-with-Roku-TV/799842411",
    "https://www.walmart.com/ip/TCL-75-Class-S4-75S451-4K-UHD-HDR-Smart-TV-with-Roku-TV/822836388",
    "https://www.walmart.com/ip/TCL-32-Class-S-Class-720p-HD-LED-Smart-TV-with-Google-TV-32S250G-New/5084872018",
    "https://www.walmart.com/ip/VIZIO-50-Class-V-Series-4K-UHD-LED-SmartCast-Smart-TV-V505-J/378837297",
    "https://www.walmart.com/ip/VIZIO-40-Class-D-Series-FHD-LED-Smart-TV-D40f-J/223245823",
    "https://www.walmart.com/ip/VIZIO-65-Class-V-Series-4K-UHD-LED-SmartCast-Smart-TV-HDR-V655-J/292825097",
    "https://www.walmart.com/ip/Samsung-98-Class-QLED-Q7F-4K-Samsung-Vision-AI-Smart-TV-2025/17552161828",
    "https://www.walmart.com/ip/VIZIO-43-Class-D-Series-FHD-LED-Smart-TV-D43f-J04/940656766",
    "https://www.walmart.com/ip/LG-55-Class-4K-UHDTV-2160p-Smart-LED-LCD-TV-55QNED82AUA/16399700878",
    "https://www.walmart.com/ip/Samsung-Crystal-UN58U8000FF-58-Smart-LED-LCD-TV-4K-UHDTV-High-Dynamic-Range-HDR-un58u8000ffxza/16390322152",
    "https://www.walmart.com/ip/Samsung-QN75QN80FAF-75-Smart-LED-LCD-TV-4K-UHDTV-qn75qn80fafxza/16335308420",
]


class WalmartTestCrawler:
    def __init__(self):
        self.driver = None

    def setup_driver(self):
        """Setup Chrome WebDriver with undetected-chromedriver"""
        print("[INFO] Setting up WebDriver...")

        options = uc.ChromeOptions()
        options.page_load_strategy = 'none'
        options.add_argument('--disable-blink-features=AutomationControlled')
        options.add_argument('--disable-dev-shm-usage')
        options.add_argument('--no-sandbox')
        options.add_argument('--start-maximized')
        options.add_argument('--disable-gpu')
        options.add_argument('--window-size=1920,1080')

        self.driver = uc.Chrome(options=options, use_subprocess=True)
        self.driver.set_page_load_timeout(120)

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
        except:
            return None

    def extract_count_of_reviews(self, tree, page_source=None):
        """Extract total number of reviews"""
        try:
            # Check for "No ratings yet"
            if page_source and "No ratings yet" in page_source:
                no_ratings_xpaths = [
                    "//span[contains(text(), 'No ratings yet')]",
                    "//*[@id='item-review-section']//span[contains(text(), 'No ratings yet')]"
                ]
                for xpath in no_ratings_xpaths:
                    result = tree.xpath(xpath)
                    if result:
                        print(f"  [INFO] Found 'No ratings yet', setting count_of_reviews to 0")
                        return 0

            # Priority 1: "View all reviews (4,686)" button
            view_all_xpaths = [
                "//button[contains(text(), 'View all reviews') and @data-dca-intent='select' and contains(@class, 'tc')]",
                '//*[@id="item-review-section"]/div[6]/button',
                '//*[@id="item-review-section"]/div[8]/button',
                "//button[contains(text(), 'View all reviews') and @data-dca-intent='select']",
                '//button[contains(text(), "View all reviews")]'
            ]
            for xpath in view_all_xpaths:
                result = tree.xpath(xpath)
                if result:
                    text = result[0].text_content().strip() if hasattr(result[0], 'text_content') else str(result[0]).strip()
                    match = re.search(r'\(([\d,]+)\)', text)
                    if match:
                        count = int(match.group(1).replace(',', ''))
                        print(f"  [INFO] Extracted count from 'View all reviews' button: {count}")
                        return count

            # Priority 2: "6,602 reviews" link
            reviews_link_xpaths = [
                '//*[@id="item-review-section"]/div[2]/div[1]/div[1]/div/a',
                "//a[@link-identifier='seeAllReviewsStarRating']",
                "//span[@link-identifier='seeAllReviewsStarRating']"
            ]
            for xpath in reviews_link_xpaths:
                result = tree.xpath(xpath)
                if result:
                    text = result[0].text_content().strip() if hasattr(result[0], 'text_content') else str(result[0]).strip()

                    if re.match(r'^0\s+reviews?$', text, re.IGNORECASE):
                        print(f"  [INFO] Extracted count from reviews link: 0")
                        return 0

                    # Handle "23.9K reviews" format
                    match_k = re.search(r'([\d.]+)K\s+reviews?', text, re.IGNORECASE)
                    if match_k:
                        count = int(float(match_k.group(1)) * 1000)
                        print(f"  [INFO] Extracted count from reviews link (K format): {count}")
                        return count

                    match = re.search(r'([\d,]+)\s+reviews?', text, re.IGNORECASE)
                    if match:
                        count = int(match.group(1).replace(',', ''))
                        print(f"  [INFO] Extracted count from reviews link: {count}")
                        return count

            # Priority 3: "Showing 1-3 of 4,686 reviews"
            showing_xpaths = [
                '//*[@id="item-review-section"]/div[7]/h3',
                '//*[@id="item-review-section"]//h3[contains(text(), "Showing")]',
                '//h3[contains(text(), "Showing") and contains(text(), "reviews")]'
            ]
            for xpath in showing_xpaths:
                result = tree.xpath(xpath)
                if result:
                    text = result[0].text_content().strip() if hasattr(result[0], 'text_content') else str(result[0]).strip()
                    match = re.search(r'of\s+([\d,]+)\s+reviews?', text, re.IGNORECASE)
                    if match:
                        count = int(match.group(1).replace(',', ''))
                        print(f"  [INFO] Extracted count from 'Showing X of Y reviews': {count}")
                        return count

            # Priority 4: JSON extraction
            if page_source:
                match = re.search(r'"numberOfReviews"\s*:\s*(\d+)', page_source)
                if match:
                    count = int(match.group(1))
                    print(f"  [INFO] Extracted numberOfReviews from JSON: {count}")
                    return count

                match = re.search(r'"totalReviewCount"\s*:\s*(\d+)', page_source)
                if match:
                    count = int(match.group(1))
                    print(f"  [INFO] Extracted totalReviewCount from JSON: {count}")
                    return count

            return None

        except Exception as e:
            print(f"  [WARNING] Failed to extract count of reviews: {e}")
            return None

    def extract_detailed_reviews(self):
        """Click 'View all reviews' and extract up to 20 reviews"""
        try:
            # Scroll to reviews section
            self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            time.sleep(2)

            # Find and click "View all reviews" button
            view_all_xpaths = [
                "//button[contains(text(), 'View all reviews') and @data-dca-intent='select']",
                '//*[@id="item-review-section"]/div[6]/button',
                '//*[@id="item-review-section"]/div[8]/button',
                "//button[contains(text(), 'View all reviews')]"
            ]

            view_all_btn = None
            for xpath in view_all_xpaths:
                try:
                    buttons = self.driver.find_elements(By.XPATH, xpath)
                    for btn in buttons:
                        if '(' in btn.text and ')' in btn.text:
                            view_all_btn = btn
                            break
                    if not view_all_btn and buttons:
                        view_all_btn = buttons[0]
                    if view_all_btn:
                        break
                except:
                    continue

            if not view_all_btn:
                print(f"  [WARNING] Could not find View all reviews button")
                return None

            # Scroll to button and click
            self.driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", view_all_btn)
            time.sleep(1)

            btn_text = view_all_btn.text if view_all_btn else "N/A"
            print(f"  [DEBUG] Found View all reviews button: '{btn_text[:50]}...'")

            self.driver.execute_script("arguments[0].click();", view_all_btn)
            time.sleep(random.uniform(3, 4))

            # Extract reviews from pages
            reviews = []
            page_num = 1
            max_pages = 2

            while len(reviews) < 20 and page_num <= max_pages:
                page_source = self.driver.page_source
                tree = html.fromstring(page_source)

                # Find review containers
                review_content_divs = tree.xpath('//div[@data-testid="enhanced-review-content"]')
                if not review_content_divs:
                    review_content_divs = tree.xpath('//div[contains(@class, "review-content")]')
                if not review_content_divs:
                    review_content_divs = tree.xpath('//div[@data-testid="review-card"]')

                if not review_content_divs:
                    print(f"  [WARNING] No review content divs found on page {page_num}")
                    break

                # Extract reviews
                for content_div in review_content_divs:
                    if len(reviews) >= 20:
                        break

                    review_xpath = './/p/span[@class="tl-m db-m"]'
                    review_elem = content_div.xpath(review_xpath)

                    if review_elem:
                        review_text = review_elem[0].text_content().strip() if hasattr(review_elem[0], 'text_content') else str(review_elem[0]).strip()
                        if review_text and len(review_text) > 10:
                            reviews.append(review_text)

                # Click Next Page if needed
                if len(reviews) < 20 and page_num < max_pages:
                    try:
                        next_page_xpaths = [
                            '//*[@id="maincontent"]/main/nav/ul/li[4]/a',
                            "//a[@data-testid='NextPage']",
                            "//a[@aria-label='Next Page']"
                        ]

                        next_page_btn = None
                        for xpath in next_page_xpaths:
                            try:
                                next_page_btn = self.driver.find_element(By.XPATH, xpath)
                                if next_page_btn:
                                    break
                            except:
                                continue

                        if not next_page_btn:
                            break

                        self.driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", next_page_btn)
                        time.sleep(1)
                        self.driver.execute_script("arguments[0].click();", next_page_btn)
                        time.sleep(random.uniform(3, 4))
                        page_num += 1
                    except:
                        break
                else:
                    break

            # Format reviews
            if reviews:
                print(f"  [INFO] Extracted {len(reviews)} reviews from {page_num} page(s)")
                formatted = []
                for idx, review in enumerate(reviews[:20], 1):
                    formatted.append(f"review{idx}-{review}")
                return ', '.join(formatted)

            return None

        except Exception as e:
            print(f"  [WARNING] Failed to extract detailed reviews: {e}")
            return None

    def scrape_url(self, url, index):
        """Scrape single URL and extract key fields"""
        try:
            print(f"\n{'='*80}")
            print(f"[{index+1}/{len(TEST_URLS)}] Accessing: {url[:80]}...")

            self.driver.get(url)

            # Wait for page to load
            try:
                WebDriverWait(self.driver, 30).until(
                    EC.presence_of_element_located((By.ID, 'maincontent'))
                )
            except:
                print(f"  [WARNING] Page load timeout, continuing...")

            time.sleep(random.uniform(3, 5))

            # Get final URL
            final_url = self.driver.current_url

            # Parse page content
            page_source = self.driver.page_source
            tree = html.fromstring(page_source)

            # Extract count_of_reviews
            count_of_reviews = self.extract_count_of_reviews(tree, page_source)

            # Extract detailed_review_content (navigates to review page)
            detailed_review_content = None
            if count_of_reviews and count_of_reviews > 0:
                detailed_review_content = self.extract_detailed_reviews()

                # Navigate back to product page for next iteration
                self.driver.get(url)
                time.sleep(2)

            # Print results
            review_count = 0
            if detailed_review_content:
                review_count = len([r for r in detailed_review_content.split(', ') if r.startswith('review')])

            print(f"\n  [RESULT]")
            print(f"    product_url: {final_url}")
            print(f"    count_of_reviews: {count_of_reviews}")
            print(f"    detailed_review_content: {review_count} reviews collected")

            return {
                'product_url': final_url,
                'count_of_reviews': count_of_reviews,
                'detailed_review_content': detailed_review_content
            }

        except Exception as e:
            print(f"  [ERROR] Failed to scrape URL: {e}")
            import traceback
            traceback.print_exc()
            return None

    def run(self):
        """Run test crawler"""
        print("="*80)
        print("Walmart TV Test Crawler")
        print(f"Start time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"Total URLs: {len(TEST_URLS)}")
        print("="*80)

        try:
            self.setup_driver()

            results = []
            for i, url in enumerate(TEST_URLS):
                result = self.scrape_url(url, i)
                if result:
                    results.append(result)

            # Print summary
            print("\n" + "="*80)
            print("SUMMARY")
            print("="*80)
            print(f"Total URLs: {len(TEST_URLS)}")
            print(f"Successfully scraped: {len(results)}")
            print(f"Failed: {len(TEST_URLS) - len(results)}")

            # Count null values
            null_counts = {
                'count_of_reviews': sum(1 for r in results if r['count_of_reviews'] is None),
                'detailed_review_content': sum(1 for r in results if not r['detailed_review_content'])
            }

            print(f"\nNull counts:")
            for field, count in null_counts.items():
                print(f"  {field}: {count}")

            print(f"\nEnd time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
            print("="*80)

        except Exception as e:
            print(f"[ERROR] Fatal error: {e}")
            import traceback
            traceback.print_exc()

        finally:
            if self.driver:
                self.driver.quit()
                print("[INFO] WebDriver closed")


if __name__ == "__main__":
    crawler = WalmartTestCrawler()
    crawler.run()
