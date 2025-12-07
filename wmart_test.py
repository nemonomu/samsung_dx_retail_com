"""
Walmart TV Test Crawler
- count_of_star_ratings, count_of_reviews 만 수집
- DB 저장 없음
"""
import time
import random
import undetected_chromedriver as uc
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from lxml import html
import re


class WalmartTestCrawler:
    def __init__(self):
        self.driver = None
        self.wait = None
        self.xpaths = {
            'star_rating': "//*[@id='item-review-section']/div[2]/div[1]/div[1]/div/a",
            'total_ratings': "//a[@link-identifier='seeAllReviewsStarRating']"
        }

    def setup_driver(self):
        """Setup Chrome WebDriver"""
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
        except Exception as e:
            return None

    def extract_star_rating(self, tree, page_source=None):
        """Extract star rating"""
        try:
            if page_source:
                match = re.search(r'"roundedAverageOverallRating":([\d.]+)', page_source)
                if match:
                    rating = match.group(1)
                    if len(rating.split('.')[-1]) <= 1 if '.' in rating else True:
                        return rating

            rating_text = self.extract_text_safe(tree, self.xpaths.get('star_rating'))
            if rating_text:
                if "No ratings yet" in rating_text:
                    return "No ratings yet"
                match = re.search(r'([\d.]+)\s*(?:stars?\s*)?out of', rating_text, re.IGNORECASE)
                if match:
                    return match.group(1)

            no_ratings_xpaths = [
                "//span[contains(text(), 'No ratings yet')]",
                "//*[@id='item-review-section']//span[contains(text(), 'No ratings yet')]"
            ]
            for xpath in no_ratings_xpaths:
                result = tree.xpath(xpath)
                if result:
                    text = result[0].text_content().strip() if hasattr(result[0], 'text_content') else str(result[0]).strip()
                    if "No ratings yet" in text:
                        return "No ratings yet"

            return None
        except Exception as e:
            return None

    def extract_count_of_star_ratings(self, tree):
        """Extract total star rating count (sum of all star ratings)"""
        try:
            total_text = self.extract_text_safe(tree, self.xpaths.get('total_ratings'))
            total_count = None
            if total_text:
                total_match = re.search(r'(\d+)', total_text.replace(',', ''))
                if total_match:
                    total_count = int(total_match.group(1))

            star_counts = {}

            for star_num in range(5, 0, -1):
                count = None

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
                except:
                    pass

                if count is None:
                    try:
                        aria_xpath = f"//button[@aria-label[contains(., '{star_num} star')]]/@aria-label"
                        aria_labels = tree.xpath(aria_xpath)
                        if aria_labels:
                            aria_text = aria_labels[0]
                            match = re.search(r'(\d+)\s+ratings?\s+are\s+rated', aria_text)
                            if match:
                                count = int(match.group(1))
                    except:
                        pass

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
                    except:
                        pass

                if count is not None:
                    star_counts[star_num] = count

            if star_counts:
                return sum(star_counts.values())

            return None

        except Exception as e:
            print(f"  [WARNING] Failed to extract star rating counts: {e}")
            return None

    def extract_count_of_reviews(self, tree, star_rating=None, page_source=None):
        """Extract total number of reviews"""
        try:
            if star_rating and "No ratings yet" in str(star_rating):
                return 0

            if page_source and "No ratings yet" in page_source:
                no_ratings_xpaths = [
                    "//span[contains(text(), 'No ratings yet')]",
                    "//*[@id='item-review-section']//span[contains(text(), 'No ratings yet')]"
                ]
                for xpath in no_ratings_xpaths:
                    result = tree.xpath(xpath)
                    if result:
                        return 0

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
                        return int(count_str)

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
                        return int(count_str)

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
                        return int(count_str)

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

    def scrape_page(self, url):
        """Scrape page and extract star ratings and reviews count"""
        try:
            print(f"\n{'='*80}")
            print(f"[INFO] Accessing: {url[:80]}...")

            self.driver.get(url)
            time.sleep(random.uniform(5, 7))

            page_source = self.driver.page_source
            tree = html.fromstring(page_source)

            # Extract star rating
            star_rating = self.extract_star_rating(tree, page_source)

            # Extract count of star ratings
            count_of_star_ratings = self.extract_count_of_star_ratings(tree)

            # Extract count of reviews
            count_of_reviews = self.extract_count_of_reviews(tree, star_rating, page_source)

            print(f"  [RESULT] Star Rating: {star_rating}")
            print(f"  [RESULT] Count of Star Ratings (총 별점 수): {count_of_star_ratings}")
            print(f"  [RESULT] Count of Reviews (총 리뷰 수): {count_of_reviews}")

            return {
                'url': url,
                'star_rating': star_rating,
                'count_of_star_ratings': count_of_star_ratings,
                'count_of_reviews': count_of_reviews
            }

        except Exception as e:
            print(f"  [ERROR] Failed to scrape page: {e}")
            import traceback
            traceback.print_exc()
            return None

    def run(self, urls):
        """Main execution"""
        try:
            print("="*80)
            print("Walmart TV Test Crawler - Starting")
            print("="*80)

            self.setup_driver()

            results = []
            for idx, url in enumerate(urls, 1):
                print(f"\n[{idx}/{len(urls)}] Processing...")
                result = self.scrape_page(url)
                if result:
                    results.append(result)
                time.sleep(random.uniform(2, 4))

            # Summary
            print("\n" + "="*80)
            print("SUMMARY")
            print("="*80)
            total_star_ratings = 0
            total_reviews = 0

            for r in results:
                star_ratings = r['count_of_star_ratings'] or 0
                reviews = r['count_of_reviews'] or 0
                total_star_ratings += star_ratings
                total_reviews += reviews
                print(f"URL: {r['url'][:60]}...")
                print(f"  Star Rating: {r['star_rating']}, Star Ratings Count: {star_ratings}, Reviews Count: {reviews}")

            print("\n" + "-"*40)
            print(f"총 별점 수 (Total Star Ratings): {total_star_ratings}")
            print(f"총 리뷰 수 (Total Reviews): {total_reviews}")
            print("="*80)

        except Exception as e:
            print(f"[ERROR] Crawler failed: {e}")
            import traceback
            traceback.print_exc()

        finally:
            if self.driver:
                self.driver.quit()
            print("\n[INFO] Crawler terminated")


if __name__ == "__main__":
    test_urls = [
        "https://www.walmart.com/ip/FPD-32-Inch-Smart-TV-Tizen-OS-2025-Model-AT32-P1-720p-HD-HDR10-Dolby-Audio-Built-in-WiFi-Bluetooth-2-HDMI-Voice-Remote-Google-Cast-Ready-Flat-Screen/18089368235",
        "https://www.walmart.com/ip/LG-55-Class-4K-UHD-OLED-Web-OS-Smart-TV-with-Dolby-Vision-C4-Series-OLED55C4PUA/5193228936",
        "https://www.walmart.com/ip/VIZIO-32-Class-Full-HD-1080p-LED-Smart-TV-New-VFD32M-0807/5337847390",
        "https://www.walmart.com/ip/Element-43-Class-4K-UHD-2160p-LED-XUMO-Smart-Television-HDR-E550AE43C-G/5558619060",
        "https://www.walmart.com/ip/LG-55-Class-4K-UHD-QNED-Web-OS-Smart-TV-55QNED85TUA/5446374200",
        "https://www.walmart.com/ip/SAMSUNG-98-Class-DU9000-Crystal-UHD-Smart-TV-UN98DU9000FXZA-2024/5347546902",
        "https://www.walmart.com/ip/Sansui-32-VA-Series-Full-HD-Frameless-Design-Smart-TV-with-Dolby-Vision-HDR-Dolby-Atmos-WebOS/14337862799",
        "https://www.walmart.com/ip/LG-65-Class-4K-UHD-QNED-Web-OS-Smart-TV-65QNED85TUA/5446374201",
        "https://www.walmart.com/ip/Hisense-43-Inch-Class-QD6G-Series-Mini-LED-QLED-Google-Smart-TV-43QD6030G-QLED-600-Nit-Dolby-Vision-IQ-Dolby-Atmos-Full-Array-Local-Dimming/13640354576",
        "https://www.walmart.com/ip/50-Roku-Select-Series-4K/15947423237",
        "https://www.walmart.com/ip/55-Roku-Select-Series-4K/15988470169",
        "https://www.walmart.com/ip/Samsung-QN65QN70FAF-65-Smart-LED-LCD-TV-4K-UHDTV-qn65qn70fafxza/16300115318",
        "https://www.walmart.com/ip/Hisense-75-Class-4K-UHDTV-2160p-HDR-Smart-LED-LCD-TV-75QD6N/13500824517",
        "https://www.walmart.com/ip/Sony-43-Class-BRAVIA-3-LED-4K-HDR-Smart-Google-TV-K-43S30-2024-Model/5558569811",
        "https://www.walmart.com/ip/Sony-55-Class-BRAVIA-3-LED-4K-HDR-Smart-Google-TV-K-55S30-2024-Model/5534421160"
    ]

    crawler = WalmartTestCrawler()
    crawler.run(test_urls)
