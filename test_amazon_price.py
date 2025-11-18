"""
Test script to extract final_sku_price from Amazon URLs
Logs: URL, extraction steps, and final output value
No database operations - just print results
"""
import time
import re
import sys
from lxml import html
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager

# Set UTF-8 encoding for Windows console
if sys.platform == 'win32':
    sys.stdout.reconfigure(encoding='utf-8')

class AmazonPriceExtractor:
    def __init__(self):
        self.setup_driver()

    def setup_driver(self):
        """Setup Chrome driver with anti-detection"""
        print("[INFO] Setting up Chrome WebDriver with anti-detection...")

        chrome_options = Options()
        chrome_options.add_argument('--disable-blink-features=AutomationControlled')
        chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
        chrome_options.add_experimental_option('useAutomationExtension', False)
        chrome_options.add_argument('--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36')
        chrome_options.add_argument('--disable-dev-shm-usage')
        chrome_options.add_argument('--no-sandbox')
        chrome_options.add_argument('--window-size=1920,1080')

        self.driver = webdriver.Chrome(
            service=Service(ChromeDriverManager().install()),
            options=chrome_options
        )

        # Anti-detection script
        self.driver.execute_cdp_cmd('Page.addScriptToEvaluateOnNewDocument', {
            'source': '''
                Object.defineProperty(navigator, 'webdriver', {
                    get: () => undefined
                });
            '''
        })

        print("[OK] WebDriver setup complete\n")

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

    def extract_final_sku_price(self, tree):
        """Extract final SKU price from detail page
        Priority order: Special cases first, then normal price extraction
        """
        try:
            # PRIORITY 1: Check for "Currently unavailable."
            print("  [Step 1] Checking for 'Currently unavailable'...")
            currently_unavailable_xpaths = [
                '//*[@id="outOfStock"]/div/div[1]/span[1]',
                '//*[@id="availability"]/span[2]/span',
                '//span[@class="a-color-price a-text-bold"]',
                '//span[@class="a-size-medium a-color-success"]'
            ]

            for i, xpath in enumerate(currently_unavailable_xpaths, 1):
                text = self.extract_text_safe(tree, xpath)
                if text and 'currently unavailable' in text.lower():
                    print(f"    [MATCH] XPath #{i}: '{text}'")
                    return "Currently unavailable."

            # PRIORITY 2: Check for "Price higher than typical"
            print("  [Step 2] Checking for 'Price higher than typical'...")
            price_higher_xpaths = [
                '//*[@id="fod-cx-message-with-learn-more"]/span[1]',
                '//span[@id="fod-cx-message-with-learn-more"]/span[1]',
                '//span[contains(text(), "Price higher than typical")]'
            ]

            for i, xpath in enumerate(price_higher_xpaths, 1):
                text = self.extract_text_safe(tree, xpath)
                if text and 'price higher than typical' in text.lower():
                    print(f"    [MATCH] XPath #{i}: '{text}'")
                    return "Price higher than typical"

            # PRIORITY 3: Check for "No featured offers available"
            print("  [Step 3] Checking for 'No featured offers available'...")
            no_offers_xpaths = [
                '//*[@id="fod-cx-message-with-learn-more"]/span[1]',
                '//span[@id="fod-cx-message-with-learn-more"]/span[1]',
                '//span[contains(text(), "No featured offers available")]'
            ]

            for i, xpath in enumerate(no_offers_xpaths, 1):
                text = self.extract_text_safe(tree, xpath)
                if text and 'no featured offers available' in text.lower():
                    print(f"    [MATCH] XPath #{i}: '{text}'")
                    return "No featured offers available"

            # PRIORITY 4: Check for "See price in cart"
            print("  [Step 4] Checking for 'See price in cart'...")
            see_price_xpaths = [
                '//*[@id="corePriceDisplay_desktop_feature_div"]/table/tbody/tr/td[2]/span/a',
                '//a[contains(text(), "See price in cart")]',
                '//span[@class="a-declarative"]//a[contains(text(), "See price in cart")]'
            ]

            for i, xpath in enumerate(see_price_xpaths, 1):
                text = self.extract_text_safe(tree, xpath)
                if text and 'see price in cart' in text.lower():
                    print(f"    [MATCH] XPath #{i}: '{text}'")
                    return "See price in cart"

            # PRIORITY 5: Check for "To see our price, add this item to your cart."
            print("  [Step 5] Checking for 'To see our price, add this item to your cart'...")
            add_to_cart_xpaths = [
                '//*[@id="corePriceDisplay_desktop_feature_div"]/table/tbody/tr/td[2]',
                '//table[@class="a-lineitem"]//td[contains(text(), "To see our price")]',
                '//td[contains(text(), "To see our price, add this item to your cart")]'
            ]

            for i, xpath in enumerate(add_to_cart_xpaths, 1):
                text = self.extract_text_safe(tree, xpath)
                if text and 'to see our price, add this item to your cart' in text.lower():
                    print(f"    [MATCH] XPath #{i}: '{text}'")
                    return "To see our price, add this item to your cart."

            # NORMAL EXTRACTION: Try to extract regular price
            print("  [Step 6] Extracting regular price...")
            xpaths = [
                '//*[@id="corePriceDisplay_desktop_feature_div"]/div[1]/span[1]',
                '//*[@id="corePriceDisplay_desktop_feature_div"]/div[1]/span[3]/span[2]',
                '//*[@id="corePriceDisplay_desktop_feature_div"]/div[1]/span[3]/span[2]/span[1]',
                '//span[@class="a-price aok-align-center reinventPricePriceToPayMargin priceToPay"]//span[@class="a-offscreen"]',
                '//*[@id="corePrice_feature_div"]/div/div/span[1]/span[1]',
                '//*[@id="corePrice_feature_div"]//span[@class="a-offscreen"]'
            ]

            for i, xpath in enumerate(xpaths, 1):
                price_text = self.extract_text_safe(tree, xpath)
                if price_text:
                    print(f"    [FOUND] XPath #{i}: '{price_text}'")
                    # Extract only "$XXX.XX" or "$X,XXX.XX" format
                    match = re.search(r'\$[\d,]+\.?\d*', price_text)
                    if match:
                        result = match.group()
                        print(f"    [OK] Extracted price: '{result}'")
                        return result
                    # Fallback: return original if no price pattern found
                    print(f"    [OK] Returning raw text: '{price_text.strip()}'")
                    return price_text.strip()

            print("    [NONE] No price found")
            return None

        except Exception as e:
            print(f"  [ERROR] Failed to extract final SKU price: {e}")
            import traceback
            traceback.print_exc()
            return None

    def extract_retailer_sku_name(self, tree):
        """Extract product title"""
        try:
            xpaths = [
                '//*[@id="productTitle"]',
                '//span[@id="productTitle"]',
                '//h1[@id="productTitle"]//span'
            ]

            for xpath in xpaths:
                title = self.extract_text_safe(tree, xpath)
                if title:
                    return title.strip()

            return None
        except Exception as e:
            print(f"  [ERROR] Failed to extract product title: {e}")
            return None

    def test_url(self, url, index=None):
        """Test a single URL"""
        print("=" * 120)
        if index is not None:
            print(f"URL #{index}")
        print(f"URL: {url}")
        print("=" * 120)

        try:
            self.driver.get(url)
            time.sleep(5)  # Wait for page load

            page_source = self.driver.page_source
            tree = html.fromstring(page_source)

            # Extract product name
            retailer_sku_name = self.extract_retailer_sku_name(tree)
            if retailer_sku_name:
                print(f"\nProduct Title: {retailer_sku_name}\n")

            # Extract final_sku_price
            final_sku_price = self.extract_final_sku_price(tree)

            print("\n" + "-" * 120)
            print(f"FINAL RESULT: final_sku_price = '{final_sku_price}'")
            print("-" * 120 + "\n")

        except Exception as e:
            print(f"ERROR: {e}")
            import traceback
            traceback.print_exc()

    def run_tests(self, urls):
        """Test all URLs"""
        import random

        total = len(urls)
        for i, url in enumerate(urls, 1):
            print(f"\n\n{'#' * 120}")
            print(f"TESTING {i}/{total}")
            print(f"{'#' * 120}\n")
            self.test_url(url, index=i)

            # Add random delay between requests
            if i < total:
                delay = random.uniform(2, 4)
                print(f"\n[INFO] Waiting {delay:.1f} seconds before next request...\n")
                time.sleep(delay)

        self.driver.quit()
        print("\n" + "=" * 120)
        print(f"ALL TESTS COMPLETED: {total} URLs tested")
        print("=" * 120)


if __name__ == '__main__':
    urls = [
        "https://www.amazon.com/SAMSUNG-Tracking-Xcelerator-Q-Symphony-UN65DU7200/dp/B0CVS1XHJL/ref=zg_bs_g_172659_d_sccl_24/130-4968780-3185654?psc=1",
        "https://www.amazon.com/Samsung-QN55QN80FAFXZA-HW-B630F-Soundbar-Subwoofer/dp/B0FC72Z6DQ/ref=sr_1_158?crid=RY3XOG3VC795&dib=eyJ2IjoiMSJ9.UV9ARwpwLYcJHWXJkLrlc3Ri6GvBdetmoGRynBOOBgQMpq15gJyqLxT_HIMEqqCRjXPh39bWwIc0aE2Xd1wrym4qcn8Q98g37bLiQjY6ggTBCSeU98ke5RMqzpoDZV8Uy76DQLemaCuHvJfXGfJ49YgwBSDzY7MOG0Ys6Vlsv6FNK7IXF37OpSGxWOqfEfHxl6kcSToHGEc2RjEx0-FXL8x-qoi3nvxFP3j0DhIAHU8.i_SpvSzbY907LsqXOt1ioldQQNIOjVBkWlFU8fOrmjM&dib_tag=se&keywords=TV&qid=1763427047&sprefix=tv%2Caps%2C287&sr=8-158&xpid=Cg0_BApgal-UM",
        "https://www.amazon.com/LG-Upscaling-Filmmaker-Compatible-86UA7700PUA/dp/B0DYQDVSHM/ref=sr_1_33?crid=RY3XOG3VC795&dib=eyJ2IjoiMSJ9.9nc3MYmNXwz6ub_4_oklgmyASeJwSnkdjf6r8yL9eiY2MIvhIp2ivAB_UR_E3ZuNPbflHvDtzvUlgkFyjXLxMamiul4-XA7n-7HshjP5N5-m5SDN9nG4LsC2eY9Ncz0_mCtlZ7WWnrbF3rxSRkXyrUkAISOsU6DaEXOjENL5vfweXaoQ-vMad471buAjyIs4a1C-UPiQyOF_DpNyTtx2ft4FihORKExWvKKR1rvieUg.ls4c2C-MMjYvEcU_u75_z_dNQwE0ZDK1-w_m2xPZyCU&dib_tag=se&keywords=TV&qid=1763426902&sprefix=tv%2Caps%2C287&sr=8-33&xpid=Cg0_BApgal-UM",
        "https://www.amazon.com/SAMSUNG-QN55QN80FAFXZA-QN80F-Supreme-Protection/dp/B0F4YR3LJ1/ref=sr_1_108?crid=RY3XOG3VC795&dib=eyJ2IjoiMSJ9.fsaR9bAYJTIZMwxuD5CKb-wyZ8OfXmspWHQSF14wYqwSWYFRTdw25Zp6NiqEswCt7Np8cnMxDzt_dloux8PFQIvfG5Kq49wnlqF-0z38myGzCHGD7yOmptUXpLFNH033xts9axsgUuUb9qtRlYhjSc73N0GCht75wPLz_fsNGZuwVOkQcoz7wo3Q5dmC-ZdODFoo_rdqxP56SJsJg0uDxzAZyKDIOQXlD9u5FSyQk-s.NEVCWOVs1jp17HLZwIQxlO1LCnnQRjuLJZEymHE9zTk&dib_tag=se&keywords=TV&qid=1763426986&sprefix=tv%2Caps%2C287&sr=8-108&xpid=Cg0_BApgal-UM",
        "https://www.amazon.com/Hisense-65-Inch-CanvasTVTM-Vision-65S7N/dp/B0D2PNVJFT/ref=zg_bs_g_172659_d_sccl_1/130-4968780-3185654?psc=1",
        "https://www.amazon.com/75-UHD-Hisense-Roku-HDR/dp/B09JVHWJS4/ref=sr_1_302?crid=RY3XOG3VC795&dib=eyJ2IjoiMSJ9.p0XO6n7rnSEC60H0oDaPZA_SCeNY16VZsysust8QGkq1gkMKVytQ8Ykq9jXtxkiGfyRgEZ27f-WOQbPavKaOoEJH5BLIUPJGU7meN-F0rn3o0iFpW2sN0ggBkxO6QPpoyghrZZ12Nrc2aIL2MFkDqh2u4Pw4bLJVq286RGYH49aLcJWrpRhM9s3e97rV8SywgkB6vAIyIvkn2PlLNP5FFeR-P_UdIx3-ecEmKZPcpjQ.fssstYNAbTks4e8c4EuoQ9cEB8v7Mnwh-41hNxXT9A8&dib_tag=se&keywords=TV&qid=1763427228&sprefix=tv%2Caps%2C287&sr=8-302&xpid=Cg0_BApgal-UM",
        "https://www.amazon.com/Roku-Smart-2025-Television-Entertainment/dp/B0DWG6TBHT/ref=zg_bs_g_172659_d_sccl_21/130-4968780-3185654?psc=1",
        "https://www.amazon.com/SAMSUNG-65-Inch-Class-Serif-Built/dp/B0B3KZZG2B/ref=sr_1_141?crid=RY3XOG3VC795&dib=eyJ2IjoiMSJ9.uyAkNhYGjw_aHPyyvFZQCj_rivOb9tGj8qlfdFwL9m4jCi5glX4G3evUBuViiKmc9AQwaekVRAP8LveXyM6ltgNIEtjWOtyop3Dxh6dEUvCy95xVYIB8t6gXWghh3THfkLY_S8_qTTrx_ZOk1PCUr3lHuk9HTp4phO3XX_r9wJoKmIgKeqz7B2nQrxs02p5dF4ujEvi_i4O6Fhv26eUAzzwXSlk6XIPlTBoyfECFfw0.AB4l4m84CRTQS1AS6h2BWQ7pIsfwKgbvhR8l_w6it5I&dib_tag=se&keywords=TV&qid=1763427025&sprefix=tv%2Caps%2C287&sr=8-141&xpid=Cg0_BApgal-UM"
    ]

    extractor = AmazonPriceExtractor()
    extractor.run_tests(urls)
