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

    def extract_star_rating(self, tree):
        """Extract star rating number from '4.4 out of 5' format or 'No ratings yet'"""
        try:
            # Try multiple XPaths to find star rating
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

    def extract_count_of_reviews(self, tree, star_rating=None):
        """Extract total number of reviews from main page"""
        try:
            # If star_rating is "No ratings yet", return 0 immediately
            if star_rating and "No ratings yet" in str(star_rating):
                print(f"  [INFO] Star rating is 'No ratings yet', setting count_of_reviews to 0")
                return 0

            # Try multiple XPath strategies to find review count
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

            # Extract number from text
            match = re.search(r'([\d,.k]+)\s*(reviews?|ratings?)', review_text, re.IGNORECASE)
            if match:
                number_str = match.group(1)
                # Check if 'k' is in the number (e.g., "27.9K ratings")
                if 'k' in number_str.lower():
                    # Fallback: Try to extract exact count from "X stars out of Y reviews" pattern
                    stars_out_of_xpaths = [
                        "//span[@class='w_iUH7']",
                        "//*[@id='maincontent']/section/main/div[2]/div[2]/div/div[2]/div/div[2]/div/div/div[2]/div/div/span",
                        "//span[contains(text(), 'stars out of')]"
                    ]

                    for xpath in stars_out_of_xpaths:
                        result = tree.xpath(xpath)
                        if result:
                            text = result[0].text_content().strip() if hasattr(result[0], 'text_content') else str(result[0]).strip()
                            # Extract from "4.4 stars out of 27980 reviews" -> 27980
                            out_of_match = re.search(r'out of\s+([\d,]+)\s*reviews?', text, re.IGNORECASE)
                            if out_of_match:
                                exact_count = out_of_match.group(1).replace(',', '')
                                print(f"  [INFO] Extracted exact review count from 'stars out of' pattern: {exact_count}")
                                return int(exact_count)

                    # If no exact count found, use the 'k' approximation
                    print(f"  [INFO] Using 'k' approximation for review count: {number_str}")

                # Use parse_number_format to handle commas, decimals, and 'k' suffix
                return self.parse_number_format(number_str)

            return None

        except Exception as e:
            print(f"  [WARNING] Failed to extract count of reviews: {e}")
            return None

    def test_url(self, url):
        """Test a single URL and extract star_rating and count_of_reviews"""
        try:
            print(f"\n{'='*80}")
            print(f"[TEST] URL: {url}")
            print(f"{'='*80}")

            self.driver.get(url)
            time.sleep(random.uniform(4, 6))

            page_source = self.driver.page_source
            tree = html.fromstring(page_source)

            # Extract star_rating
            star_rating = self.extract_star_rating(tree)

            # Extract count_of_reviews (pass star_rating for "No ratings yet" handling)
            count_of_reviews = self.extract_count_of_reviews(tree, star_rating)

            print(f"\n  [RESULT]")
            print(f"  ├─ star_rating: {star_rating}")
            print(f"  └─ count_of_reviews: {count_of_reviews}")

            return {
                'url': url,
                'star_rating': star_rating,
                'count_of_reviews': count_of_reviews
            }

        except Exception as e:
            print(f"  [ERROR] Failed to test URL: {e}")
            return {
                'url': url,
                'star_rating': None,
                'count_of_reviews': None,
                'error': str(e)
            }

    def run(self):
        """Run tests on all URLs"""
        test_urls = [
            # Group 1
            "https://www.walmart.com/ip/TCL-98-Google-Smart-TV-98Q51CG/13621223713",
            "https://www.walmart.com/ip/Hisense-40-Class-FHD-1080P-Roku-Smart-LED-TV-40H4030F1/470905078",
            "https://www.walmart.com/ip/VIZIO-43-Class-Full-HD-1080p-LED-Smart-TV-New-VFD43M-0804/5197667451",
            "https://www.walmart.com/ip/Hisense-58-Class-4K-UHD-LED-LCD-Roku-Smart-TV-HDR-R6-Series-58R6E3/587182688",
            "https://www.walmart.com/ip/TCL-55-Class-S4-55S451-4K-UHD-HDR-Smart-TV-with-Roku-TV/533225197",
            "https://www.walmart.com/ip/VIZIO-24-Class-HD-720p-LED-Smart-TV-NEW-VHD24M-08/5197667452",
            "https://www.walmart.com/ip/LG-77-Class-4K-UHD-OLED-C4-Series-120-Hz-Web-OS-24-Smart-TV-with-Dolby-Vision-OLED77C4PUA/5332753048",
            "https://www.walmart.com/ip/65UA7500ZUA-AUS/14365163951",
            "https://www.walmart.com/ip/43UA7500ZUA-AUS/14350017890",
            "https://www.walmart.com/ip/SAMSUNG-77-Class-S90D-OLED-Smart-TV-QN77S90DAFXZA-2024/5337847611",
            "https://www.walmart.com/ip/Hisense-43-Class-4K-UHD-LED-LCD-Smart-Roku-TV-HDR-R6-Series-43R6E3/213300402",
            "https://www.walmart.com/ip/TCL-98-Class-Q6-98Q651G-4K-UHD-HDR-QLED-Smart-TV-with-Google-TV-NEW-2024/5378490189",
            "https://www.walmart.com/ip/LG-86-4K-UHD-Smart-TV-120-Hz-webOS-86UQ7070ZUD/1902385530",
            "https://www.walmart.com/ip/Element-43-Class-4K-UHD-2160p-LED-XUMO-Smart-Television-HDR-E550AE43C-G/5558619060",
            "https://www.walmart.com/ip/VIZIO-43-Class-Quantum-4K-QLED-HDR-Smart-TV-NEW-VQD43M-0801/5378440008",
            "https://www.walmart.com/ip/LG-75-Inch-4K-HDR-Smart-Quantum-Dot-NanoCell-Mini-LED-TV-2024/5513854593",
            "https://www.walmart.com/ip/TCL-50-Class-Q6-50Q651G-4K-UHD-HDR-QLED-Smart-TV-with-Google-TV-NEW-2024/5373842535",
            "https://www.walmart.com/ip/Supersonic-15-In-Class-VIDAA-LED-Smart-TV-AC-DC-Compatible-SC-1520VTV/5810310139",
            "https://www.walmart.com/ip/SAMSUNG-85-Class-Q60D-QLED-4K-Smart-TV-QN85Q60DAFXZA-2024/5340366860",
            "https://www.walmart.com/ip/VIZIO-43-Class-D-Series-FHD-LED-Smart-TV-D43f-J04/940656766",
            "https://www.walmart.com/ip/TCL-32-Class-S-Class-720p-HD-LED-Smart-TV-with-Google-TV-32S250G-New/5084872018",
            "https://www.walmart.com/ip/SAMSUNG-55-Class-LS03B-The-Frame-QLED-4K-Smart-TV-QN55LS03BAFXZA/944779027",
            "https://www.walmart.com/ip/Sony-BRAVIA-5-65-inch-Class-Mini-LED-4K-HDR-Google-TV-2025/16347258322",
            "https://www.walmart.com/ip/SAMSUNG-85-Class-QN800D-Neo-QLED-8K-Smart-TV-QN85QN800DFXZA-2024/5369461009",
            "https://www.walmart.com/ip/LG-86-Class-4K-UHD-QNED-Wed-OS-Smart-TV-86QNED85TUA/5440505126",
            "https://www.walmart.com/ip/RitaRita-22-1080p-Portable-Touch-Screen-Monitor-Television-Type/5676260062",
            "https://www.walmart.com/ip/Hisense-65-Inch-Class-U6-Series-Mini-LED-QLED-Google-Smart-TV-65U75QG-QLED-1000-Nit-Dolby-Vision-IQ-Dolby-Atmos-Full-Array-Local-Dimming/16047264086",
            # Group 2
            "https://www.walmart.com/ip/onn-32-Class-HD-720P-Smart-LED-TV-100012589/18375223072",
            "https://www.walmart.com/ip/Hiro-32-720p-HD-Smart-TV-Flat-Screen-LED-Television-with-Roku-TV-and-Dolby-Audio-for-Streaming-H32C2C4/18053369522",
            "https://www.walmart.com/ip/RCA-85-Pro-Idiom-4K-LED-TV/15194715050",
            "https://www.walmart.com/ip/SuperSonic-43-High-Definition-Smart-TV-SC-4316STV/881809037",
            "https://www.walmart.com/ip/Hiro-Roku-24-720p-HD-Smart-Flat-Screen-LED-Roku-TV-with-Dolby-Audio-for-Streaming-H24C2C4/18096203989",
            "https://www.walmart.com/ip/Westinghouse-Roku-TV-40-Inch-Smart-TV-FHD-QLED-Television-w-Dolby-Digital-Wi-Fi-Mobile-App-Connectivity-Flat-Screen-Bluetooth-Compatible-w-Apple-AirP/17842772710",
            "https://www.walmart.com/ip/VIZIO-65-Class-4K-UHD-LED-HDR-Limited-Edition-Smart-TV-NEW-V4K65X-08/7772412359",
            "https://www.walmart.com/ip/VIZIO-75-Class-4K-UHD-LED-HDR-Limited-Edition-Smart-TV-NEW-V4K75X-08/7788374178",
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
            print("\n" + "="*80)
            print("TEST SUMMARY")
            print("="*80)
            print(f"{'No':<4} {'star_rating':<20} {'count_of_reviews':<20} {'URL':<60}")
            print("-"*104)

            success_count = 0
            for idx, r in enumerate(results, 1):
                star = r.get('star_rating', 'N/A')
                count = r.get('count_of_reviews', 'N/A')
                url_short = r['url'].split('/')[-1][:50] + "..." if len(r['url'].split('/')[-1]) > 50 else r['url'].split('/')[-1]

                if star is not None or count is not None:
                    success_count += 1

                print(f"{idx:<4} {str(star):<20} {str(count):<20} {url_short:<60}")

            print("-"*104)
            print(f"Success: {success_count}/{len(results)}")
            print("="*80)

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
