"""
Walmart TV Star Rating & Reviews Count Test
Tests the extract_star_rating and extract_count_of_reviews functions
"""
import time
import random
import undetected_chromedriver as uc
from lxml import html
import re


class WalmartReviewsTest:
    def __init__(self):
        self.driver = None

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

    def parse_number_format(self, text):
        """Parse numbers like '100+', '10k', '1,000+', '1000+' to integer"""
        if not text:
            return None
        try:
            text = text.strip().lower()
            text_no_comma = text.replace(',', '')

            if 'k' in text_no_comma:
                number = re.search(r'([\d.]+)k', text_no_comma)
                if number:
                    return int(float(number.group(1)) * 1000)

            number = re.search(r'(\d+)', text_no_comma)
            if number:
                return int(number.group(1))

            return None
        except Exception as e:
            return None

    def extract_star_rating(self, tree, page_source=None):
        """Extract star rating number from '4.4 out of 5' format or 'No ratings yet'

        Priority:
        1. JSON data: roundedAverageOverallRating (rounded value like 4.4)
        2. XPath fallback (text parsing)

        Note: averageOverallRating may contain long decimals like 4.534883720930233
              which is invalid, so we use roundedAverageOverallRating instead.
        """
        try:
            # Method 1: Extract from JSON data (use rounded value)
            if page_source:
                # Try roundedAverageOverallRating first (clean value like 4.4)
                match = re.search(r'"roundedAverageOverallRating":([\d.]+)', page_source)
                if match:
                    rating = match.group(1)
                    # Validate: should be 1 decimal place max (e.g., 4.4, 3.7)
                    if len(rating.split('.')[-1]) <= 1 if '.' in rating else True:
                        print(f"  [INFO] Extracted roundedAverageOverallRating from JSON: {rating}")
                        return rating

            # Method 2: XPath fallback
            star_rating_xpaths = [
                "//span[@class='w_iUH7']",
                "//*[@id='maincontent']/section/main/div[2]/div[2]/div/div[2]/div/div[2]/div/div/div[2]/div/div/span",
                "//span[contains(text(), 'stars out of')]",
                "//span[contains(text(), 'out of 5')]"
            ]

            for xpath in star_rating_xpaths:
                result = tree.xpath(xpath)
                if result:
                    rating_text = result[0].text_content().strip() if hasattr(result[0], 'text_content') else str(result[0]).strip()

                    if rating_text:
                        # Check for "No ratings yet" first
                        if "No ratings yet" in rating_text:
                            return "No ratings yet"

                        # Extract number before "out of"
                        match = re.search(r'([\d.]+)\s*(?:stars?\s*)?out of', rating_text, re.IGNORECASE)
                        if match:
                            return match.group(1)

            # If no rating found, check for "No ratings yet" at specific locations
            no_ratings_xpaths = [
                "//*[@id='maincontent']/section/main/div[2]/div[2]/div/div[2]/div/div[2]/div/div/div[2]/div/div/span",
                "//span[@class='gray f7 ph1 pt1'][contains(text(), 'No ratings yet')]",
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
            print(f"  [WARNING] Failed to extract star rating: {e}")
            return None

    def extract_screen_size(self, tree, retailer_sku_name=None):
        """Extract screen size from product name or 'Specifications at a glance' section
        Example: 'SAMSUNG 77" Class...' -> '77 inches', '65 in' -> '65 inches'

        Priority:
        1. Product name (most reliable - avoids Resolution like 1280x720 being extracted)
        2. Specifications at a glance (fallback)
        """
        try:
            # Method 1 (Primary): Extract from retailer_sku_name (product name)
            # This avoids extracting Resolution values like 1280 from "1280 x 720"
            if retailer_sku_name:
                # Support various quote characters: " (standard), " " (unicode quotes), ″ (double prime)
                match = re.search(r'(\d+\.?\d*)(?:[\s-]*inch(?:es)?|["\u201c\u201d\u2033])', retailer_sku_name, re.IGNORECASE)
                if match:
                    size_number = match.group(1)
                    print(f"  [INFO] Screen size extracted from product name: {size_number} inches")
                    return f"{size_number} inches"

            # Method 2 (Fallback): Try XPath from Specifications at a glance
            xpaths = [
                "//dl[.//dt[contains(., 'Screen size')]]//dd",
                "//dt[contains(., 'Screen size')]/following-sibling::dd",
                "//tr[.//dt[contains(text(), 'Screen size')]]//dd",
                "//dt[contains(text(), 'Screen size')]/ancestor::tr//dd",
                "//div[@aria-label[contains(., 'Screen size:')]]/@aria-label",
                "//div[contains(@class, 'b') and contains(., 'Screen size')]/following-sibling::div//span",
                "//*[@id='ip-prod-desc-atf-div-1']/section/section[2]/div/div/div[1]/div[1]/div/div/div[2]/span",
                "//h3[contains(text(), 'Specifications at a glance')]/parent::div//div[@aria-label[contains(., 'Screen size')]]/@aria-label"
            ]

            screen_size_text = None
            for xpath in xpaths:
                result = tree.xpath(xpath)
                if result:
                    if isinstance(result[0], str):
                        screen_size_text = result[0].strip()
                    else:
                        screen_size_text = result[0].text_content().strip() if hasattr(result[0], 'text_content') else str(result[0]).strip()

                    if screen_size_text:
                        break

            if screen_size_text:
                match = re.search(r'([\d.]+)\s*(?:in(?:ch(?:es)?)?|")?', screen_size_text, re.IGNORECASE)
                if match:
                    size_number = match.group(1)
                    # Validate: TV screen size should be reasonable (10-150 inches)
                    if 10 <= float(size_number) <= 150:
                        print(f"  [INFO] Screen size extracted from XPath: {size_number} inches")
                        return f"{size_number} inches"

            return None

        except Exception as e:
            print(f"  [WARNING] Failed to extract screen size: {e}")
            return None

    def extract_product_name(self, tree):
        """Extract product name from page"""
        try:
            xpaths = [
                "//h1[@itemprop='name']",
                "//h1[@id='main-title']",
                "//h1[contains(@class, 'prod-ProductTitle')]"
            ]
            for xpath in xpaths:
                result = tree.xpath(xpath)
                if result:
                    return result[0].text_content().strip() if hasattr(result[0], 'text_content') else str(result[0]).strip()
            return None
        except:
            return None

    def extract_count_of_reviews(self, tree, star_rating=None, page_source=None):
        """Extract total number of reviews from main page

        Priority:
        1. Check "No ratings yet" first -> return 0
        2. JSON data: totalReviewCount (exact value, not displayed on screen)
        3. JSON data: numberOfReviews (alternative exact value)
        4. XPath fallback (may get approximate values like '28K')
        """
        try:
            # Check 1: If star_rating is "No ratings yet", return 0 immediately
            if star_rating and "No ratings yet" in str(star_rating):
                print(f"  [INFO] Star rating is 'No ratings yet', setting count_of_reviews to 0")
                return 0

            # Check 2: Look for "No ratings yet" in page source before JSON extraction
            if page_source and "No ratings yet" in page_source:
                # Verify it's actually displayed (not just in some config)
                no_ratings_xpaths = [
                    "//span[contains(text(), 'No ratings yet')]",
                    "//*[@id='item-review-section']//span[contains(text(), 'No ratings yet')]"
                ]
                for xpath in no_ratings_xpaths:
                    result = tree.xpath(xpath)
                    if result:
                        print(f"  [INFO] Found 'No ratings yet' on page, setting count_of_reviews to 0")
                        return 0

            # Method 1: Extract from JSON data (most accurate - gets exact count like 28040)
            if page_source:
                # Try totalReviewCount first (exact total review count)
                match = re.search(r'"totalReviewCount":(\d+)', page_source)
                if match:
                    count = int(match.group(1))
                    print(f"  [INFO] Extracted totalReviewCount from JSON: {count}")
                    return count

                # Try numberOfReviews as alternative
                match = re.search(r'"numberOfReviews":(\d+)', page_source)
                if match:
                    count = int(match.group(1))
                    print(f"  [INFO] Extracted numberOfReviews from JSON: {count}")
                    return count

            # Method 2: XPath fallback for pages without JSON data
            xpaths = [
                "//*[@id='item-review-section']/div[2]/div[1]/div[1]/div/a",
                "//a[@link-identifier='seeAllReviewsStarRating']",
                "//a[@link-identifier='reviewsLink']",
                "//*[@id='item-review-section']//a[contains(text(), 'reviews')]",
                "//*[@id='item-review-section']//a[contains(text(), 'ratings')]",
                "//div[@id='item-review-section']//a[contains(., 'review')]",
                "//div[@id='item-review-section']//a[contains(., 'rating')]"
            ]

            review_text = None
            for xpath in xpaths:
                result = tree.xpath(xpath)
                if result:
                    if isinstance(result[0], str):
                        review_text = result[0].strip()
                    else:
                        review_text = result[0].text_content().strip() if hasattr(result[0], 'text_content') else str(result[0]).strip()

                    if review_text:
                        break

            # Check for "No ratings yet" first -> return 0
            if review_text and "No ratings yet" in review_text:
                return 0

            if not review_text:
                # Try to find "No ratings yet" at specific location -> return 0
                no_ratings_xpaths = [
                    "//*[@id='maincontent']/section/main/div[2]/div[2]/div/div[2]/div/div[2]/div/div/div[2]/div/div/span",
                    "//span[@class='gray f7 ph1 pt1'][contains(text(), 'No ratings yet')]",
                    "//span[contains(text(), 'No ratings yet')]",
                    "//*[@id='item-review-section']//span[contains(text(), 'No ratings yet')]"
                ]

                for xpath in no_ratings_xpaths:
                    result = tree.xpath(xpath)
                    if result:
                        text = result[0].text_content().strip() if hasattr(result[0], 'text_content') else str(result[0]).strip()
                        if "No ratings yet" in text:
                            return 0

                return None

            # Extract number from text (fallback when JSON extraction fails)
            match = re.search(r'([\d,.k]+)\s*(reviews?|ratings?)', review_text, re.IGNORECASE)
            if match:
                number_str = match.group(1)
                print(f"  [INFO] Using XPath fallback for review count: {number_str}")
                # Use parse_number_format to handle commas, decimals, and 'k' suffix
                return self.parse_number_format(number_str)

            return None

        except Exception as e:
            print(f"  [WARNING] Failed to extract count of reviews: {e}")
            return None

    def test_url(self, url):
        """Test a single URL and extract star_rating, count_of_reviews, and screen_size"""
        try:
            print(f"\n{'='*80}")
            print(f"[TEST] URL: {url}")
            print(f"{'='*80}")

            self.driver.get(url)
            time.sleep(random.uniform(4, 6))

            page_source = self.driver.page_source
            tree = html.fromstring(page_source)

            # Extract product name for screen_size fallback
            retailer_sku_name = self.extract_product_name(tree)

            # Extract star_rating (pass page_source for JSON extraction)
            star_rating = self.extract_star_rating(tree, page_source)

            # Extract count_of_reviews (pass star_rating and page_source for JSON extraction)
            count_of_reviews = self.extract_count_of_reviews(tree, star_rating, page_source)

            # Extract screen_size (with fallback to product name)
            screen_size = self.extract_screen_size(tree, retailer_sku_name)

            print(f"\n  [RESULT]")
            print(f"  ├─ product_url: {url}")
            print(f"  ├─ screen_size: {screen_size}")
            print(f"  ├─ star_rating: {star_rating}")
            print(f"  └─ count_of_reviews: {count_of_reviews}")

            return {
                'url': url,
                'star_rating': star_rating,
                'count_of_reviews': count_of_reviews,
                'screen_size': screen_size
            }

        except Exception as e:
            print(f"  [ERROR] Failed to test URL: {e}")
            return {
                'url': url,
                'star_rating': None,
                'count_of_reviews': None,
                'screen_size': None,
                'error': str(e)
            }

    def run(self):
        """Run tests on all URLs"""
        test_urls = [
            "https://www.walmart.com/ip/Westinghouse-43-inch-Smart-TV-4K-UHD-XUMO-TV-w-HDR10-Voice-Remote-Dolby-Vision-Edgeless-Flat-Screen-LED-Television-w-Apple-Home-kit-Wi-Fi-Mobile-Conn/16766210956",
            "https://www.walmart.com/ip/FPD-40-Inch-Smart-LED-TV-Tizen-OS-2025-Model-AT40-P1-1080p-Full-HD-HDR10-Dolby-Audio-Built-in-WiFi-Bluetooth-2-HDMI-Voice-Remote-Google-Cast-Flat-Scr/18085052914",
            "https://www.walmart.com/ip/LG-43-4K-UHD-UA70-AI-Smart-TV-43UA7000/16746214961",
            "https://www.walmart.com/ip/TCL-32-Q31K-Series-1080P-FHD-QLED-Smart-TV-with-Google-TV-32Q31K/15775051070",
            "https://www.walmart.com/ip/Hisense-55-Class-U7-Series-Mini-LED-ULED-4K-UHD-Google-Smart-TV-55U75Q-2025-Model-QLED-Native-165Hz-1000-Nit-Dolby-Vision-IQ-Full-Array-Local-Dimming/16031006937",
            "https://www.walmart.com/ip/Restored-Westinghouse-32-720P-HD-Smart-Roku-TV-WR32HT2212-Refurbished/1576486772",
            "https://www.walmart.com/ip/Restored-VIZIO-D-Series-24-Class-1080p-Full-Array-LED-HD-Smart-TV-D24f-J09-Refurbished/981750578",
            "https://www.walmart.com/ip/Sony-55-class-BRAVIA-2-II-LED-4K-HDR-Smart-Google-TV-K-55S20-2025-Model/16539074131",
            "https://www.walmart.com/ip/Restored-Westinghouse-24-720p-LED-Roku-Smart-TV-WR24HT2212-Refurbished/3110948754",
            "https://www.walmart.com/ip/Supersonic-19-In-Class-VIDAA-LED-Smart-TV-AC-DC-Compatible-SC-1920VTV/5890920417",
            "https://www.walmart.com/ip/OLED65B5PUA/16998272236",
            "https://www.walmart.com/ip/Westinghouse-24-inch-Smart-TV-HD-Xumo-TV-with-Voice-Remote-Flat-Screen-LED-Television-w-Apple-Home-kit-Wi-Fi-Mobile-Connectivity/16752513510",
            "https://www.walmart.com/ip/RCA-43-1080p-FHD-Smart-LED-TV-TC-LE43K-AN2401-Android-TV/14588607914",
            "https://www.walmart.com/ip/LG-75-Inch-4K-HDR-Smart-Quantum-Dot-NanoCell-Mini-LED-TV-2024/5513854593",
        ]

        print("="*80)
        print("Walmart TV Star Rating & Reviews Count Test")
        print(f"Total URLs to test: {len(test_urls)}")
        print("="*80)

        try:
            self.setup_driver()

            results = []
            for idx, url in enumerate(test_urls, 1):
                print(f"\n[{idx}/{len(test_urls)}] Testing...")
                result = self.test_url(url)
                results.append(result)

                # Random delay between requests
                if idx < len(test_urls):
                    delay = random.uniform(3, 5)
                    print(f"  [INFO] Waiting {delay:.1f}s before next request...")
                    time.sleep(delay)

            # Print summary
            print("\n" + "="*120)
            print("TEST SUMMARY")
            print("="*120)
            print(f"{'No':<4} {'star_rating':<15} {'count_of_reviews':<18} {'screen_size':<15} {'URL':<60}")
            print("-"*120)

            success_count = 0
            for idx, r in enumerate(results, 1):
                star = r.get('star_rating', 'N/A')
                count = r.get('count_of_reviews', 'N/A')
                screen = r.get('screen_size', 'N/A')
                url_short = r['url'].split('/')[-1][:50] + "..." if len(r['url'].split('/')[-1]) > 50 else r['url'].split('/')[-1]

                if star is not None or count is not None or screen is not None:
                    success_count += 1

                print(f"{idx:<4} {str(star):<15} {str(count):<18} {str(screen):<15} {url_short:<60}")

            print("-"*120)
            print(f"Success: {success_count}/{len(results)}")
            print("="*120)

        except Exception as e:
            print(f"[FATAL ERROR] {e}")
            import traceback
            traceback.print_exc()

        finally:
            if self.driver:
                self.driver.quit()
                print("\n[INFO] WebDriver closed")


if __name__ == "__main__":
    tester = WalmartReviewsTest()
    tester.run()
