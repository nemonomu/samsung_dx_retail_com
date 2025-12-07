"""
Amazon TV Test Crawler
- final_sku_price, screen_size만 추출
- DB 저장 없음
"""
import time
import random
import sys
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


class AmazonTestCrawler:
    def __init__(self):
        self.driver = None

    def setup_driver(self):
        """Setup Chrome WebDriver"""
        options = Options()
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

        service = Service(ChromeDriverManager().install())
        self.driver = webdriver.Chrome(service=service, options=options)
        self.driver.set_page_load_timeout(60)
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

    def extract_final_sku_price(self, tree):
        """Extract final SKU price from detail page"""
        try:
            # PRIORITY 1: Check for "Currently unavailable."
            currently_unavailable_xpaths = [
                '//*[@id="outOfStock"]/div/div[1]/span[1]',
                '//*[@id="availability"]/span[2]/span',
                '//span[@class="a-color-price a-text-bold"]',
                '//span[@class="a-size-medium a-color-success"]'
            ]

            for xpath in currently_unavailable_xpaths:
                text = self.extract_text_safe(tree, xpath)
                if text and 'currently unavailable' in text.lower():
                    return "Currently unavailable."

            # PRIORITY 2: Check for "Price higher than typical"
            price_higher_xpaths = [
                '//*[@id="fod-cx-message-with-learn-more"]/span[1]',
                '//span[@id="fod-cx-message-with-learn-more"]/span[1]',
                '//span[contains(text(), "Price higher than typical")]'
            ]

            for xpath in price_higher_xpaths:
                text = self.extract_text_safe(tree, xpath)
                if text and 'price higher than typical' in text.lower():
                    return "Price higher than typical"

            # PRIORITY 3: Check for "No featured offers available"
            no_offers_xpaths = [
                '//*[@id="fod-cx-message-with-learn-more"]/span[1]',
                '//span[@id="fod-cx-message-with-learn-more"]/span[1]',
                '//span[contains(text(), "No featured offers available")]'
            ]

            for xpath in no_offers_xpaths:
                text = self.extract_text_safe(tree, xpath)
                if text and 'no featured offers available' in text.lower():
                    return "No featured offers available"

            # PRIORITY 4: Check for "See price in cart"
            see_price_xpaths = [
                '//*[@id="corePriceDisplay_desktop_feature_div"]/table/tbody/tr/td[2]/span/a',
                '//a[contains(text(), "See price in cart")]',
                '//span[@class="a-declarative"]//a[contains(text(), "See price in cart")]'
            ]

            for xpath in see_price_xpaths:
                text = self.extract_text_safe(tree, xpath)
                if text and 'see price in cart' in text.lower():
                    return "See price in cart"

            # PRIORITY 5: Check for "To see our price, add this item to your cart."
            add_to_cart_xpaths = [
                '//*[@id="corePriceDisplay_desktop_feature_div"]/table/tbody/tr/td[2]',
                '//table[@class="a-lineitem"]//td[contains(text(), "To see our price")]',
                '//td[contains(text(), "To see our price, add this item to your cart")]'
            ]

            for xpath in add_to_cart_xpaths:
                text = self.extract_text_safe(tree, xpath)
                if text and 'to see our price, add this item to your cart' in text.lower():
                    return "To see our price, add this item to your cart."

            # NORMAL EXTRACTION: Try to extract regular price
            xpaths = [
                '//*[@id="corePriceDisplay_desktop_feature_div"]/div[1]/span[1]',
                '//*[@id="corePriceDisplay_desktop_feature_div"]/div[1]/span[3]/span[2]',
                '//*[@id="corePriceDisplay_desktop_feature_div"]/div[1]/span[3]/span[2]/span[1]',
                '//span[@class="a-price aok-align-center reinventPricePriceToPayMargin priceToPay"]//span[@class="a-offscreen"]',
                '//*[@id="corePrice_feature_div"]/div/div/span[1]/span[1]',
                '//*[@id="corePrice_feature_div"]//span[@class="a-offscreen"]'
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
            print(f"  [WARNING] Failed to extract final SKU price: {e}")
            return None

    def extract_screen_size(self, tree, retailer_sku_name=None):
        """Extract screen size (format: '32 inches')"""
        try:
            xpaths = [
                '//tr[contains(@class, "po-display.size")]//td[@class="a-span9"]//span[@class="a-size-base po-break-word"]',
                '//table[@class="a-normal a-spacing-small"]//tr[contains(@class, "po-display.size")]//td[@class="a-span9"]//span[@class="a-size-base po-break-word"]',
                '//*[@id="poExpander"]/div[1]/div/table/tbody/tr[2]/td[2]/span',
                '//tr[contains(@class, "po-display.size")]//span[@class="a-size-base po-break-word"]',
                '//*[@id="inline-twister-expanded-dimension-text-size_name"]'
            ]

            for xpath in xpaths:
                size_text = self.extract_text_safe(tree, xpath)
                if size_text:
                    size_text = size_text.strip()
                    print(f"  [DEBUG] screen_size found: '{size_text}'")

                    if not re.search(r'\d', size_text):
                        continue

                    if '-inch' in size_text.lower():
                        match = re.search(r'(\d+\.?\d*)-inch', size_text.lower())
                        if match:
                            return f"{match.group(1)} inches"
                        continue

                    if re.match(r'^\d+\.?\d*"?$', size_text):
                        size_number = re.search(r'(\d+\.?\d*)', size_text).group(1)
                        return f"{size_number} inches"

                    match = re.search(r'(\d+\.?\d*)\s*inch', size_text.lower())
                    if match:
                        return f"{match.group(1)} inches"

                    continue

            # Fallback: Extract from retailer_sku_name if provided
            if retailer_sku_name:
                match = re.search(r'(\d+\.?\d*)(?:[\s-]*inch(?:es)?|")', retailer_sku_name, re.IGNORECASE)
                if match:
                    size_number = match.group(1)
                    print(f"  [INFO] Extracted screen_size from product name: {size_number} inches")
                    return f"{size_number} inches"

            return None

        except Exception as e:
            print(f"  [WARNING] Failed to extract screen size: {e}")
            return None

    def extract_product_name(self, tree):
        """Extract product name"""
        try:
            xpaths = [
                '//*[@id="productTitle"]',
                '//span[@id="productTitle"]',
                '//h1[@id="title"]//span'
            ]
            for xpath in xpaths:
                name = self.extract_text_safe(tree, xpath)
                if name:
                    return name
            return None
        except:
            return None

    def scrape_page(self, url):
        """Scrape page and extract final_sku_price and screen_size"""
        try:
            print(f"\n{'='*80}")
            print(f"[INFO] Accessing: {url[:80]}...")

            self.driver.get(url)
            time.sleep(random.uniform(4, 6))

            page_source = self.driver.page_source
            tree = html.fromstring(page_source)

            # Extract product name (for screen_size fallback)
            product_name = self.extract_product_name(tree)

            # Extract final_sku_price
            final_sku_price = self.extract_final_sku_price(tree)

            # Extract screen_size
            screen_size = self.extract_screen_size(tree, product_name)

            print(f"  [RESULT] Product: {product_name[:60] if product_name else 'N/A'}...")
            print(f"  [RESULT] Final SKU Price: {final_sku_price}")
            print(f"  [RESULT] Screen Size: {screen_size}")

            return {
                'url': url,
                'product_name': product_name,
                'final_sku_price': final_sku_price,
                'screen_size': screen_size
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
            print("Amazon TV Test Crawler - Starting")
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

            for r in results:
                print(f"URL: {r['url'][:60]}...")
                print(f"  Product: {r['product_name'][:50] if r['product_name'] else 'N/A'}...")
                print(f"  Final SKU Price: {r['final_sku_price']}")
                print(f"  Screen Size: {r['screen_size']}")
                print()

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
        "https://www.amazon.com/SYLVOX-Standing-Removable-Television-Scenarios/dp/B0FMK86LB1/ref=sr_1_7?crid=RY3XOG3VC795&dib=eyJ2IjoiMSJ9.SyCoOkM7uhmlyv61ekfFtE6Z9CJxSmZlW3HSxoFjK0ReyuNtfBFalFjByx5PlOqUjO0yCiYuXVo5MGwrj-FF9GbUsHXySj1TACmj0M17shrjoUdU1R-aSCYDDQrm2W2rapFu1g1zmOmYfTYiVrtppxZ8h5uiB3_YwPvk-rhLK_yqEKH5-a5vRai134NRaN6GEoYxWukcw2yqbfo-7verhlNl2wZqwgaQpYECuhtmvzs.f28Jq7iaJZ7sUriyk1Q7NT7iI0ojaR4ccs65hrA-fas&dib_tag=se&keywords=TV&qid=1764892820&sprefix=tv%2Caps%2C287&sr=8-7",
        "https://www.amazon.com/amazon-fire-tv-43-inch-4-series-4k-smart-tv/dp/B08SWD2SCK/ref=sr_1_48?crid=RY3XOG3VC795&dib=eyJ2IjoiMSJ9.F9yU1zgYAKiS1wbto5UNDr2BxyedErJw-FbuIYg_uvvAPWOJgPoLdAQ_eY2S6L_6zRAkWXFr7OlNJf-ATJO-DMn8Xvz0Vv3KfLe1WdIJ6BLQ2pJL-ASoBfO0e_4sCgwKFahBWTjN7Kz24BWetTKFGhxjzWdAnWP1HW7gok47ipK7_iqrJU39wDUb95eNfAsQic-P5enjYOKItHqFteeYJH59RV0MZ1cgQRtanDveeys.MbRSARs03o2u5PlJ1sbUQuuggnzHZPqyaTSmDDqMJGY&dib_tag=se&keywords=TV&qid=1764892863&sprefix=tv%2Caps%2C287&sr=8-48&xpid=Cg0_BApgal-UM&th=1",
        "https://www.amazon.com/amazon-fire-tv-65-inch-omni-series-4k-smart-tv/dp/B08T6J1HG8/ref=sr_1_53?crid=RY3XOG3VC795&dib=eyJ2IjoiMSJ9.M4-BGz5BtnoPXdFDibZhmb7NJH9v9b2Jz2x3eQHI3hcZ0iIH7CZtB2trNJGFePMFai4J0fPDo4PuJrq9h2HSCeBhRexdbZZzpDNejJ7qh9MLlLd54sdLlZQ6htB09MVKpzWj5MX0JqGpONLpeEphqKYFcsO7W_EWxa560sfM_N2OXAkpmtiebVixPwSSjKXeY7Q-cHITimoQNw3naHDh739TuiJ8rvDkaS9VjOxl-Ic.B65c7O4lOWKBeVXLfy2z_nURTO258vXGwlH5tOHpB2Q&dib_tag=se&keywords=TV&qid=1764892885&sprefix=tv%2Caps%2C287&sr=8-53&xpid=Cg0_BApgal-UM&th=1",
        "https://www.amazon.com/Introducing-Amazon-Fire-TV-without/dp/B09WGTLG4N/ref=sr_1_81?crid=RY3XOG3VC795&dib=eyJ2IjoiMSJ9.ZrcKFx1RCuFXwTh-mlO9ZCfWu5EC2T76ieBbopjxS6ptjo9Jez3OqCZsB1akcaJFMrhnuLxNYsGBNkonHQ-DXsdPLpU8M951bCsxaxNcl4jenYQF0s_d5BO4BRfQXhIK7dMJ4ljFWggpOisZO0uZGDt7OCGtdPVmcaMzQc4Aa-Qzb67iOA7k3N9SLCHUNTLPmjlwiBtDbpsgXBrcbEP1GBOw9W46sy1X-KAPUUPokOQ.0ZsARWpUxc_E-fNOrQ7xY8Mg_IZHWCXrKoJ1XibjDT4&dib_tag=se&keywords=TV&qid=1764892926&sprefix=tv%2Caps%2C287&sr=8-81&xpid=Cg0_BApgal-UM&th=1",
        "https://www.amazon.com/Dungeon-Core-Complete-Boxed-Set-ebook/dp/B0DNVK9RW3/ref=sr_1_252?crid=RY3XOG3VC795&dib=eyJ2IjoiMSJ9.s8cL8WmkJl_jKm_2LvtEm7KminlQmGwoFqGOKyADlnpUebJZvNLy3DuRNKEyQQSZwF0WhEmkSVp5UbO4lFO_b1cs0lS9rIfkZqHe-NHciPcPWFtQsLin_08J1wTP_tux7JiKPhv_yPdjtfS9-BQ6TFHPzs6FLOikO4qEdYB_k5BQgx12ziSSSd_N6uYaTrMQKHjcU8D74NCEAcFqluF118k9QVzCEvzwqwzA9PFOp7w.q7732-9mZ6wNVjdKUkzrdvcmPO95GmPC5Df9qfBCHJY&dib_tag=se&keywords=TV&qid=1764893124&sprefix=tv%2Caps%2C287&sr=8-252&xpid=Cg0_BApgal-UM",
        "https://www.amazon.com/SYLVOX-Standing-Removable-Television-Scenarios/dp/B0FMK86LB1/ref=sr_1_3?crid=RY3XOG3VC795&dib=eyJ2IjoiMSJ9.-tHQkHUzN_-0XHCCI10ejaTOOiJElnkOXRxLiuE8hE5EYA_tZHolc3c8Kxn4tILP0PbP6yQUIwQEZ8oJrmTjetBJVIX9vr8AeROuYLHczsP2dRa5_zB9ZQQdp-Pcpa87CCLrQ2l4ipUd3rCMoTaxE1nP0AlI8zxErMLD8LJcGbNHVLpZsgOFqvdWkI8iJnvy7zWCibqHfgIWY6FQ9JlI_bqM7eb78FHbNJoUX1jFHbg.PsBiSN6aG7o_3dnztfZu8k4uREYS2nqI8SJsX2qEDe4&dib_tag=se&keywords=TV&qid=1764936036&sprefix=tv%2Caps%2C287&sr=8-3",
        "https://www.amazon.com/amazon-fire-tv-43-inch-4-series-4k-smart-tv/dp/B08SWD2SCK/ref=sr_1_47?crid=RY3XOG3VC795&dib=eyJ2IjoiMSJ9.M9DVckYBZJko2kKKs2o5sJG0Gu_pryENiG3hYd6wiPPrc63h3iIpZq9x0t0dtBTxEWPXOnYGYOAzeZNdYU-G_U0E7qI6uDSz53qYn_NtltFjgAkXKiMStUu6NlmLopZ21_mbxc4INnNjTkFmenyK9Izc37w-KCyVDj3VeKIFz1iHFbwbE_NH-Xsr2LnornPz0i70uuQQl5TNx3xqmPLlXZPYyrFambOets2Czi0A9PY.cMKq6HDgAqnrI3M5X-VYyiV4bYKAafpt4yv_9JhMGmk&dib_tag=se&keywords=TV&qid=1764936082&sprefix=tv%2Caps%2C287&sr=8-47&xpid=Cg0_BApgal-UM&th=1",
        "https://www.amazon.com/Dungeon-Core-Complete-Boxed-Set-ebook/dp/B0DNVK9RW3/ref=sr_1_251?crid=RY3XOG3VC795&dib=eyJ2IjoiMSJ9.itjLj09Vtj0ML_pFH8eqne8xYuaSah6p0hlm48px5_wrWF48I-on0oOFo-Q1i4gBraclDSXbzKqE1KVCiJI-4IHqPGx_8j5G54YlCB2ZtymCsYuWOhvQ51yFUd2SjA5gg7zerF4-_eavJZWzen2ix5CzdKO8kY5WQcPhLcdMfkSYnUl-u8GKhjsPWuNtTpFFDLTpOl_3lCzW9At64mDaSOx5D48_Tz1t9H5cnhz_p2k.seOL4cyRonYqNGr1AMAinWoz66Uh15hpMIedYaatdvw&dib_tag=se&keywords=TV&qid=1764936338&sprefix=tv%2Caps%2C287&sr=8-251&xpid=Cg0_BApgal-UM",
        "https://www.amazon.com/Monoprice-Screens-Between-32in-65in-Patterns/dp/B07ZWNWZPC/ref=sr_1_298?crid=RY3XOG3VC795&dib=eyJ2IjoiMSJ9.Ioh8sDahgwHw29SagZVo0KZ57-AaT4JT9wzvQlkcpOoHydp2QPwa_1KykPBmT5dVyB5rlDqYYbWG13rDmKFQWcsU1H4zSMNkrpMKEkT81KaTRexKn2Pm7U8zPLKu5azzWX5ypujyZcjA5gM-NimeuYXyektC2YzLJXcFk9Ec2VGtBbBil2KsiWeb4aIj5xUymagV8rj1AL0afqtSoDqXmWGQ-JGsvXHnvmv9ULFP4dM.1YLW59Mo6HbQ-TdivRr3z-NgsnZUSHc5Am0POCJLP4U&dib_tag=se&keywords=TV&qid=1764936401&sprefix=tv%2Caps%2C287&sr=8-298&xpid=Cg0_BApgal-UM",
        "https://www.amazon.com/Introducing-Amazon-Fire-TV-without/dp/B09WGTLG4N/ref=zg_bs_g_172659_d_sccl_99/136-9156030-1933102?th=1"
    ]

    crawler = AmazonTestCrawler()
    crawler.run(test_urls)
