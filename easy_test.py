"""
Simple test script to extract count_of_reviews, star_rating, final_sku_price from specific URLs
No database operations - just print results
"""
import time
import re
from lxml import html
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager

class SimpleExtractor:
    def __init__(self):
        self.setup_driver()

    def setup_driver(self):
        """Setup Chrome driver"""
        chrome_options = Options()
        chrome_options.add_argument('--disable-blink-features=AutomationControlled')
        chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
        chrome_options.add_experimental_option('useAutomationExtension', False)

        self.driver = webdriver.Chrome(
            service=Service(ChromeDriverManager().install()),
            options=chrome_options
        )

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

    def extract_count_of_reviews_amazon(self, tree):
        """Extract count of reviews for Amazon"""
        try:
            # Primary XPaths
            xpaths = [
                '//*[@id="acrCustomerReviewText"]',
                '//span[@id="acrCustomerReviewText"]',
                '//a[@id="acrCustomerReviewLink"]//span'
            ]

            for xpath in xpaths:
                reviews_text = self.extract_text_safe(tree, xpath)
                if reviews_text:
                    if "No customer reviews" in reviews_text:
                        return "0"
                    match = re.search(r'([\d,]+)', reviews_text)
                    if match:
                        return match.group(1)

            # Fallback XPaths
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
            return f"ERROR: {e}"

    def extract_star_rating_amazon(self, tree):
        """Extract star rating for Amazon"""
        try:
            # Primary XPath (from DB)
            star_rating_text = self.extract_text_safe(tree, '//*[@id="acrPopover"]/span[1]/a/span')

            if star_rating_text:
                if "No customer reviews" in star_rating_text:
                    return "No customer reviews"
                if re.search(r'\d', star_rating_text):
                    return star_rating_text

            # Fallback XPaths
            no_reviews_xpaths = [
                '//*[@id="cm-cr-dp-review-header"]/h3/span',
                '//span[@data-hook="top-customer-reviews-title"]',
                '//div[@id="cm-cr-dp-review-header"]//span[contains(text(), "No customer reviews")]',
                '//span[contains(text(), "No customer reviews")]'
            ]

            for xpath in no_reviews_xpaths:
                text = self.extract_text_safe(tree, xpath)
                if text and "No customer reviews" in text:
                    return "No customer reviews"

            return None
        except Exception as e:
            return f"ERROR: {e}"

    def extract_final_sku_price_amazon(self, tree):
        """Extract final SKU price for Amazon"""
        try:
            # PRIORITY 1: Currently unavailable
            currently_unavailable_xpaths = [
                '//*[@id="outOfStock"]/div/div[1]/span[1]',
                '//*[@id="availability"]/span[2]/span',
                '//span[@class="a-color-price a-text-bold"]'
            ]

            for xpath in currently_unavailable_xpaths:
                text = self.extract_text_safe(tree, xpath)
                if text and 'currently unavailable' in text.lower():
                    return "Currently unavailable."

            # PRIORITY 2: Price higher than typical
            price_higher_xpaths = [
                '//*[@id="fod-cx-message-with-learn-more"]/span[1]',
                '//span[contains(text(), "Price higher than typical")]'
            ]

            for xpath in price_higher_xpaths:
                text = self.extract_text_safe(tree, xpath)
                if text and 'price higher than typical' in text.lower():
                    return "Price higher than typical"

            # PRIORITY 3: No featured offers available
            no_offers_xpaths = [
                '//*[@id="fod-cx-message-with-learn-more"]/span[1]',
                '//span[contains(text(), "No featured offers available")]'
            ]

            for xpath in no_offers_xpaths:
                text = self.extract_text_safe(tree, xpath)
                if text and 'no featured offers available' in text.lower():
                    return "No featured offers available"

            # PRIORITY 4: See price in cart
            see_price_xpaths = [
                '//*[@id="corePriceDisplay_desktop_feature_div"]/table/tbody/tr/td[2]/span/a',
                '//a[contains(text(), "See price in cart")]'
            ]

            for xpath in see_price_xpaths:
                text = self.extract_text_safe(tree, xpath)
                if text and 'see price in cart' in text.lower():
                    return "See price in cart"

            # PRIORITY 5: To see our price, add this item to your cart
            add_to_cart_xpaths = [
                '//*[@id="corePriceDisplay_desktop_feature_div"]/table/tbody/tr/td[2]',
                '//td[contains(text(), "To see our price, add this item to your cart")]'
            ]

            for xpath in add_to_cart_xpaths:
                text = self.extract_text_safe(tree, xpath)
                if text and 'to see our price, add this item to your cart' in text.lower():
                    return "To see our price, add this item to your cart."

            # NORMAL EXTRACTION: Regular price
            xpaths = [
                '//*[@id="corePriceDisplay_desktop_feature_div"]/div[1]/span[1]',
                '//*[@id="corePriceDisplay_desktop_feature_div"]/div[1]/span[3]/span[2]',
                '//span[@class="a-price aok-align-center reinventPricePriceToPayMargin priceToPay"]//span[@class="a-offscreen"]',
                '//*[@id="corePrice_feature_div"]/div/div/span[1]/span[1]'
            ]

            for xpath in xpaths:
                price_text = self.extract_text_safe(tree, xpath)
                if price_text:
                    match = re.search(r'\$[\d,]+\.?\d*', price_text)
                    if match:
                        return match.group()
                    return price_text.strip()

            return None
        except Exception as e:
            return f"ERROR: {e}"

    def extract_count_of_reviews_walmart(self, tree):
        """Extract count of reviews for Walmart"""
        try:
            # Check for "No ratings yet" first
            no_ratings_xpaths = [
                '//*[@id="maincontent"]/section/main/div[2]/div[2]/div/div[2]/div/div[2]/div/div/div[2]/div/div/span',
                '//span[@class="gray f7 ph1 pt1"][contains(text(), "No ratings yet")]'
            ]

            for xpath in no_ratings_xpaths:
                result = self.extract_text_safe(tree, xpath)
                if result and "No ratings yet" in result:
                    return "0"

            # Try to extract review count
            xpaths = [
                '//a[@class="fw5 underline-hover w_EU"][@href="#reviews"]',
                '//a[@href="#reviews"][contains(text(), "reviews")]',
                '//a[@href="#reviews"][contains(text(), "ratings")]'
            ]

            for xpath in xpaths:
                review_text = self.extract_text_safe(tree, xpath)
                if review_text:
                    # Extract number: "248 reviews" -> "248"
                    match = re.search(r'([\d,]+)', review_text)
                    if match:
                        return match.group(1).replace(',', '')

            return None
        except Exception as e:
            return f"ERROR: {e}"

    def extract_star_rating_walmart(self, tree):
        """Extract star rating for Walmart"""
        try:
            # Check for "No ratings yet"
            no_ratings_xpaths = [
                '//*[@id="maincontent"]/section/main/div[2]/div[2]/div/div[2]/div/div[2]/div/div/div[2]/div/div/span',
                '//span[@class="gray f7 ph1 pt1"][contains(text(), "No ratings yet")]'
            ]

            for xpath in no_ratings_xpaths:
                result = self.extract_text_safe(tree, xpath)
                if result and "No ratings yet" in result:
                    return "No ratings yet"

            # Try to extract star rating
            xpaths = [
                '//span[@class="w_iUH7"][@itemprop="ratingValue"]',
                '//span[@itemprop="ratingValue"]',
                '//div[@class="flex items-center"]//span[contains(text(), "out of")]'
            ]

            for xpath in xpaths:
                rating_text = self.extract_text_safe(tree, xpath)
                if rating_text:
                    # "3.9 stars out of 69 reviews" -> "3.9"
                    match = re.search(r'([\d.]+)\s*(?:stars?\s*)?out of', rating_text, re.IGNORECASE)
                    if match:
                        return match.group(1)
                    # Direct rating value
                    if re.match(r'^\d+\.?\d*$', rating_text):
                        return rating_text

            return None
        except Exception as e:
            return f"ERROR: {e}"

    def extract_final_sku_price_walmart(self, tree):
        """Extract final SKU price for Walmart"""
        try:
            # Check for "See price in cart"
            see_price_xpaths = [
                '//*[@id="maincontent"]/section/main/div[2]/div[2]/div/div[2]/div/div[2]/div/div/div[1]/div/span[2]',
                '//span[contains(text(), "See price in cart")]'
            ]

            for xpath in see_price_xpaths:
                text = self.extract_text_safe(tree, xpath)
                if text and "See price in cart" in text:
                    return "See price in cart"

            # Normal price extraction (try regular prices first)
            xpaths = [
                '//span[@itemprop="price"]',
                '//*[@id="maincontent"]/section/main/div[2]/div[2]/div/div[2]/div/div[2]/div/div/div[1]/div/span[1]',
                '//div[@data-testid="price-wrap"]//span[@itemprop="price"]'
            ]

            for xpath in xpaths:
                price_text = self.extract_text_safe(tree, xpath)
                if price_text:
                    # Skip if it contains "Starting from" (will be handled in fallback)
                    if "Starting from" in price_text:
                        continue
                    match = re.search(r'\$[\d,]+\.?\d*', price_text)
                    if match:
                        return match.group()

            # LAST RESORT: Check for "Starting from $X"
            starting_from_xpaths = [
                '//*[@id="maincontent"]/section/main/div[2]/div[2]/div/div[2]/div/div[2]/div/div/div[1]/div/span[1]',
                '//span[contains(text(), "Starting from")]'
            ]

            for xpath in starting_from_xpaths:
                text = self.extract_text_safe(tree, xpath)
                if text and "Starting from" in text:
                    # Extract price: "Starting from $1,995.00" -> "$1,995.00"
                    match = re.search(r'\$[\d,]+\.?\d*', text)
                    if match:
                        return match.group()

            return None
        except Exception as e:
            return f"ERROR: {e}"

    def test_url(self, url):
        """Test a single URL"""
        print("=" * 100)
        print(f"Testing: {url}")
        print("=" * 100)

        try:
            self.driver.get(url)
            time.sleep(5)  # Wait for page load

            page_source = self.driver.page_source
            tree = html.fromstring(page_source)

            # Detect site
            is_amazon = 'amazon.com' in url
            is_walmart = 'walmart.com' in url

            if is_amazon:
                count_of_reviews = self.extract_count_of_reviews_amazon(tree)
                star_rating = self.extract_star_rating_amazon(tree)
                final_sku_price = self.extract_final_sku_price_amazon(tree)
            elif is_walmart:
                count_of_reviews = self.extract_count_of_reviews_walmart(tree)
                star_rating = self.extract_star_rating_walmart(tree)
                final_sku_price = self.extract_final_sku_price_walmart(tree)
            else:
                print("Unknown site!")
                return

            print(f"count_of_reviews: {count_of_reviews}")
            print(f"star_rating: {star_rating}")
            print(f"final_sku_price: {final_sku_price}")
            print()

        except Exception as e:
            print(f"ERROR: {e}")
            import traceback
            traceback.print_exc()

    def run_tests(self, urls):
        """Test all URLs"""
        for url in urls:
            self.test_url(url)

        self.driver.quit()
        print("=" * 100)
        print("ALL TESTS COMPLETED")
        print("=" * 100)


if __name__ == '__main__':
    urls = [
        "https://www.amazon.com/75-UHD-Hisense-Roku-HDR/dp/B09JVHWJS4/ref=sr_1_302?crid=RY3XOG3VC795&dib=eyJ2IjoiMSJ9.p0XO6n7rnSEC60H0oDaPZA_SCeNY16VZsysust8QGkq1gkMKVytQ8Ykq9jXtxkiGfyRgEZ27f-WOQbPavKaOoEJH5BLIUPJGU7meN-F0rn3o0iFpW2sN0ggBkxO6QPpoyghrZZ12Nrc2aIL2MFkDqh2u4Pw4bLJVq286RGYH49aLcJWrpRhM9s3e97rV8SywgkB6vAIyIvkn2PlLNP5FFeR-P_UdIx3-ecEmKZPcpjQ.fssstYNAbTks4e8c4EuoQ9cEB8v7Mnwh-41hNxXT9A8&dib_tag=se&keywords=TV&qid=1763427228&sprefix=tv%2Caps%2C287&sr=8-302&xpid=Cg0_BApgal-UM",
        "https://www.amazon.com/SAMSUNG-QN98QN90AA-7S-8KHD2-2-5M-Premium-Braided/dp/B0CLC94N45/ref=sr_1_300?crid=RY3XOG3VC795&dib=eyJ2IjoiMSJ9.p0XO6n7rnSEC60H0oDaPZA_SCeNY16VZsysust8QGkq1gkMKVytQ8Ykq9jXtxkiGfyRgEZ27f-WOQbPavKaOoEJH5BLIUPJGU7meN-F0rn3o0iFpW2sN0ggBkxO6QPpoyghrZZ12Nrc2aIL2MFkDqh2u4Pw4bLJVq286RGYH49aLcJWrpRhM9s3e97rV8SywgkB6vAIyIvkn2PlLNP5FFeR-P_UdIx3-ecEmKZPcpjQ.fssstYNAbTks4e8c4EuoQ9cEB8v7Mnwh-41hNxXT9A8&dib_tag=se&keywords=TV&qid=1763427228&sprefix=tv%2Caps%2C287&sr=8-300&xpid=Cg0_BApgal-UM",
        "https://www.amazon.com/Samsung-QN75QN80FAFXZA-Supreme-PMX700-Motion/dp/B0FKCM7JVN/ref=sr_1_268?crid=RY3XOG3VC795&dib=eyJ2IjoiMSJ9.Dk2V6G2PIn-jtUSswktR-E1GVvrJ7F7ZhJm9SaXwVnwn2O1wX98PaO_RrXAKIA0_pn-wEn2PuQ6uYquET8-_dWGQ9qZOSlSWCXKAjOrZzhWls-XjGUH7rk41T6voyy9nZNJ-3ieOcqiEw5-NHnvcePCB-EtXyOKdL2lPQj6It8J0hOGxW98nSLHjbv6qH__C2wd3I4muF08pDq_JO5PPFc2A1ge-BkZtrrcejffSkV4.3hb0hUC2oYsCwvYXVe_NgvGWQCygJdiFTUWnLAyLPfY&dib_tag=se&keywords=TV&qid=1763427186&sprefix=tv%2Caps%2C287&sr=8-268&xpid=Cg0_BApgal-UM",
        "https://www.walmart.com/ip/Hisense-110-Inch-Class-Mini-LED-Premium-ULED-X-Series-4K-Google-Smart-TV-110UX-2024-QLED-144Hz-Dolby-Vision-Atmos-HDR10-4-2-2-Ch-Audio-Alexa-Compatib/11945451040",
        "https://www.amazon.com/Samsung-QN85Q7F-Class-Vision-Smart/dp/B0F2QGCM4V/ref=sr_1_210?crid=RY3XOG3VC795&dib=eyJ2IjoiMSJ9.71JDC5fI_LcsqrhinyqTPVbtOcTSJ_BUTfHl9s8OAnF86aMTbozABN-STt0X3YPSUTFu-9sd5C4XBj8Yu44a_J7OYSY8ws44gZlTarqvqihDrpLCGloHnkcOFP9Lv0M5R5Rdko8WYqK6XKxydmWWAUDzS9xoxPakz4J2ZJFpsi7eDumjQR2d9MYEriKTorB3fqmz8mue1pKI5AQL-135z7uvnynzVtFYn9UbipcZZFw.7nQd_SfOasFXpx_G8TM8c2B0_oNP4MvN8nuPFW-TRtc&dib_tag=se&keywords=TV&qid=1763427128&sprefix=tv%2Caps%2C287&sr=8-210&xpid=Cg0_BApgal-UM"
    ]

    extractor = SimpleExtractor()
    extractor.run_tests(urls)
