"""
Amazon TV Test Crawler - Extract only key fields
Extracts: final_sku_price, star_rating, count_of_reviews, count_of_star_ratings, product_url
"""

import os
import sys
import time
import random
import pickle
import re
from datetime import datetime

from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager
from lxml import html

# UTF-8 encoding for Windows console
if sys.platform == 'win32':
    sys.stdout.reconfigure(encoding='utf-8', errors='replace')

# Import config for cookie file path
from config import AMAZON_ACCOUNTS

COOKIE_FILE = AMAZON_ACCOUNTS['unsandev0004']['cookie_file']

# Test URLs
TEST_URLS = [
    "https://www.amazon.com/amazon-fire-tv-43-inch-4-series-4k-smart-tv/dp/B0CZ9WV2ZX/ref=sr_1_83?crid=RY3XOG3VC795&dib=eyJ2IjoiMSJ9.PmtAy2SThn5GY5K9Gm-pyHPyIDtEJw3rdxmy3kvm4cchMIJfhoSMEcmPpcZzXE6k3iZFFvvus0e5oj4-cPrQZB6STBrS_12JJTK8HwuM7bEF1yKWO3dkM1tpNhiJpQ7Xqr-2tdbRDGNlXpC3gCzjaSLkWvFt7hIvdY22uD8HKhpiZMjm2SdspqoZ87MStDjUVaTq5u3MEkMsft2EqmRv3r3Ebh8QgCDxd9Zzermjqts.Io7ne168Xn8AJM81UHAP9dTJxIQh2rDfSQT3byfb1mc&dib_tag=se&keywords=TV&qid=1768280531&sprefix=tv%2Caps%2C287&sr=8-83&xpid=Cg0_BApgal-UM&th=1",
    "https://www.amazon.com/LG-Upscaling-Filmmaker-Orchestra-OLED97G5WUA/dp/B0DYQRL1YX/ref=sr_1_84?crid=RY3XOG3VC795&dib=eyJ2IjoiMSJ9.PmtAy2SThn5GY5K9Gm-pyHPyIDtEJw3rdxmy3kvm4cchMIJfhoSMEcmPpcZzXE6k3iZFFvvus0e5oj4-cPrQZB6STBrS_12JJTK8HwuM7bEF1yKWO3dkM1tpNhiJpQ7Xqr-2tdbRDGNlXpC3gCzjaSLkWvFt7hIvdY22uD8HKhpiZMjm2SdspqoZ87MStDjUVaTq5u3MEkMsft2EqmRv3r3Ebh8QgCDxd9Zzermjqts.Io7ne168Xn8AJM81UHAP9dTJxIQh2rDfSQT3byfb1mc&dib_tag=se&keywords=TV&qid=1768280531&sprefix=tv%2Caps%2C287&sr=8-84&xpid=Cg0_BApgal-UM",
    "https://www.amazon.com/Sony-Exclusive-Features-PlayStation-K-85XR90/dp/B0CVPNFL8Z/ref=sr_1_85?crid=RY3XOG3VC795&dib=eyJ2IjoiMSJ9.PmtAy2SThn5GY5K9Gm-pyHPyIDtEJw3rdxmy3kvm4cchMIJfhoSMEcmPpcZzXE6k3iZFFvvus0e5oj4-cPrQZB6STBrS_12JJTK8HwuM7bEF1yKWO3dkM1tpNhiJpQ7Xqr-2tdbRDGNlXpC3gCzjaSLkWvFt7hIvdY22uD8HKhpiZMjm2SdspqoZ87MStDjUVaTq5u3MEkMsft2EqmRv3r3Ebh8QgCDxd9Zzermjqts.Io7ne168Xn8AJM81UHAP9dTJxIQh2rDfSQT3byfb1mc&dib_tag=se&keywords=TV&qid=1768280531&sprefix=tv%2Caps%2C287&sr=8-85&xpid=Cg0_BApgal-UM",
    "https://www.amazon.com/VIZIO-Bluetooth-Headphone-Compatibility-V555M-K01/dp/B09VCZVX7M/ref=sr_1_87?crid=RY3XOG3VC795&dib=eyJ2IjoiMSJ9.PmtAy2SThn5GY5K9Gm-pyHPyIDtEJw3rdxmy3kvm4cchMIJfhoSMEcmPpcZzXE6k3iZFFvvus0e5oj4-cPrQZB6STBrS_12JJTK8HwuM7bEF1yKWO3dkM1tpNhiJpQ7Xqr-2tdbRDGNlXpC3gCzjaSLkWvFt7hIvdY22uD8HKhpiZMjm2SdspqoZ87MStDjUVaTq5u3MEkMsft2EqmRv3r3Ebh8QgCDxd9Zzermjqts.Io7ne168Xn8AJM81UHAP9dTJxIQh2rDfSQT3byfb1mc&dib_tag=se&keywords=TV&qid=1768280531&sprefix=tv%2Caps%2C287&sr=8-87&xpid=Cg0_BApgal-UM",
    "https://www.amazon.com/ApoloSign-Portable-EDLA-Certified-15000mAh-Rotation/dp/B0F2YD3VV5/ref=sr_1_90?crid=RY3XOG3VC795&dib=eyJ2IjoiMSJ9.PmtAy2SThn5GY5K9Gm-pyHPyIDtEJw3rdxmy3kvm4cchMIJfhoSMEcmPpcZzXE6k3iZFFvvus0e5oj4-cPrQZB6STBrS_12JJTK8HwuM7bEF1yKWO3dkM1tpNhiJpQ7Xqr-2tdbRDGNlXpC3gCzjaSLkWvFt7hIvdY22uD8HKhpiZMjm2SdspqoZ87MStDjUVaTq5u3MEkMsft2EqmRv3r3Ebh8QgCDxd9Zzermjqts.Io7ne168Xn8AJM81UHAP9dTJxIQh2rDfSQT3byfb1mc&dib_tag=se&keywords=TV&qid=1768280531&sprefix=tv%2Caps%2C287&sr=8-90&xpid=Cg0_BApgal-UM",
    "https://www.amazon.com/SAMSUNG-75-Inch-Weatherproof-Anti-Reflection-QN75LST9DAFXZA/dp/B0DK2JLVWP/ref=sr_1_91?crid=RY3XOG3VC795&dib=eyJ2IjoiMSJ9.PmtAy2SThn5GY5K9Gm-pyHPyIDtEJw3rdxmy3kvm4cchMIJfhoSMEcmPpcZzXE6k3iZFFvvus0e5oj4-cPrQZB6STBrS_12JJTK8HwuM7bEF1yKWO3dkM1tpNhiJpQ7Xqr-2tdbRDGNlXpC3gCzjaSLkWvFt7hIvdY22uD8HKhpiZMjm2SdspqoZ87MStDjUVaTq5u3MEkMsft2EqmRv3r3Ebh8QgCDxd9Zzermjqts.Io7ne168Xn8AJM81UHAP9dTJxIQh2rDfSQT3byfb1mc&dib_tag=se&keywords=TV&qid=1768280531&sprefix=tv%2Caps%2C287&sr=8-91&xpid=Cg0_BApgal-UM",
    "https://www.amazon.com/LG-Upscaling-Filmmaker-Orchestra-OLED55B5PUA-AUSZ/dp/B0FFXN5PN9/ref=zg_bs_g_172659_d_sccl_83/145-5513661-8696414?th=1",
    "https://www.amazon.com/SAMSUNG-Tracking-Xcelerator-Enhancer-QN75QN90D/dp/B0CV9DSFXM/ref=zg_bs_g_172659_d_sccl_84/145-5513661-8696414?psc=1",
    "https://www.amazon.com/Monster-Portable-Entertainment-Playtime-Resistant/dp/B082349G1C/ref=zg_bs_g_172659_d_sccl_86/145-5513661-8696414?psc=1",
    "https://www.amazon.com/TCL-65-Inch-NXTVISION-Google-Canvas/dp/B0DB6HJDJL/ref=zg_bs_g_172659_d_sccl_97/145-5513661-8696414?th=1",
    "https://www.amazon.com/SAMSUNG-32-Inch-Tracking-Xcelerator-QN32Q60D/dp/B0CV9MGX22/ref=zg_bs_g_172659_d_sccl_98/145-5513661-8696414?psc=1",
    "https://www.amazon.com/TCL-65QM8K-120HZ-144HZ-Reflective-Television/dp/B0F53CZ4WT/ref=zg_bs_g_172659_d_sccl_99/145-5513661-8696414?psc=1",
    "https://www.amazon.com/VIZIO-50-inch-Premium-Compatibility-M50QXM-K01/dp/B09VCZCR1W/ref=zg_bs_g_172659_d_sccl_100/145-5513661-8696414?psc=1",
    "https://www.amazon.com/ApoloSign-Portable-EDLA-Certified-15000mAh-Rotation/dp/B0F2YD3VV5/ref=sr_1_91?crid=RY3XOG3VC795&dib=eyJ2IjoiMSJ9.7NgWirXi3B7cL-H_cj9lbNpH8xkKuf0-bT1QuaP1rpuSiVZy9AqlQBK1hhSe8SPtLbt4J5PloWLlejJpGIwzS66JacVmX2JYi2awsT0MZG3ra45BPyVSAH73kosCfJ3z9NWFFu-LWN-L1Aa0OxsI6LAJ4p0oTrrTsMLk3S01sGLC3cJfATzsTYMK5Q1R8oV3NOBmkN9up3ElxLZMrtcRrI00nCHK33RH4LksM0lDJ0w.cs1VeH0KaSjJ8lO0m0Ipub4wWPnhU9VDX2IXKhjuNV4&dib_tag=se&keywords=TV&qid=1768323727&sprefix=tv%2Caps%2C287&sr=8-91&xpid=Cg0_BApgal-UM",
    "https://www.amazon.com/Supersonic-SC-3226SDVD-Language-Compatible-Included/dp/B0D7YFRKTV/ref=sr_1_92?crid=RY3XOG3VC795&dib=eyJ2IjoiMSJ9.7NgWirXi3B7cL-H_cj9lbNpH8xkKuf0-bT1QuaP1rpuSiVZy9AqlQBK1hhSe8SPtLbt4J5PloWLlejJpGIwzS66JacVmX2JYi2awsT0MZG3ra45BPyVSAH73kosCfJ3z9NWFFu-LWN-L1Aa0OxsI6LAJ4p0oTrrTsMLk3S01sGLC3cJfATzsTYMK5Q1R8oV3NOBmkN9up3ElxLZMrtcRrI00nCHK33RH4LksM0lDJ0w.cs1VeH0KaSjJ8lO0m0Ipub4wWPnhU9VDX2IXKhjuNV4&dib_tag=se&keywords=TV&qid=1768323727&sprefix=tv%2Caps%2C287&sr=8-92&xpid=Cg0_BApgal-UM",
    "https://www.amazon.com/SAMSUNG-75-Inch-Weatherproof-Anti-Reflection-QN75LST9DAFXZA/dp/B0DK2JLVWP/ref=sr_1_94?crid=RY3XOG3VC795&dib=eyJ2IjoiMSJ9.7NgWirXi3B7cL-H_cj9lbNpH8xkKuf0-bT1QuaP1rpuSiVZy9AqlQBK1hhSe8SPtLbt4J5PloWLlejJpGIwzS66JacVmX2JYi2awsT0MZG3ra45BPyVSAH73kosCfJ3z9NWFFu-LWN-L1Aa0OxsI6LAJ4p0oTrrTsMLk3S01sGLC3cJfATzsTYMK5Q1R8oV3NOBmkN9up3ElxLZMrtcRrI00nCHK33RH4LksM0lDJ0w.cs1VeH0KaSjJ8lO0m0Ipub4wWPnhU9VDX2IXKhjuNV4&dib_tag=se&keywords=TV&qid=1768323727&sprefix=tv%2Caps%2C287&sr=8-94&xpid=Cg0_BApgal-UM",
    "https://www.amazon.com/insignia-fire-tv-43-inch-class-f30-series-4k-smart-tv/dp/B0CMDJ8TK3/ref=zg_bs_g_172659_d_sccl_86/147-5053841-4237206?psc=1",
    "https://www.amazon.com/Monster-Portable-Entertainment-Playtime-Resistant/dp/B082349G1C/ref=zg_bs_g_172659_d_sccl_87/147-5053841-4237206?psc=1",
    "https://www.amazon.com/SAMSUNG-Tracking-Xcelerator-Enhancer-QN75QN90D/dp/B0CV9DSFXM/ref=zg_bs_g_172659_d_sccl_88/147-5053841-4237206?psc=1",
    "https://www.amazon.com/Sony-Exclusive-Features-PlayStation-K-65XR80/dp/B0CVQ6YLH7/ref=zg_bs_g_172659_d_sccl_89/147-5053841-4237206?psc=1",
]


class AmazonTestCrawler:
    def __init__(self):
        self.driver = None

    def setup_driver(self):
        """Setup Chrome WebDriver with options"""
        print("[INFO] Setting up WebDriver...")

        options = Options()
        options.add_argument('--disable-gpu')
        options.add_argument('--no-sandbox')
        options.add_argument('--disable-dev-shm-usage')
        options.add_argument('--window-size=1920,1080')
        options.add_argument('--disable-blink-features=AutomationControlled')
        options.add_experimental_option('excludeSwitches', ['enable-automation'])
        options.add_experimental_option('useAutomationExtension', False)
        options.add_argument('--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36')

        service = Service(ChromeDriverManager().install())
        self.driver = webdriver.Chrome(service=service, options=options)

        # Stealth settings
        self.driver.execute_cdp_cmd('Page.addScriptToEvaluateOnNewDocument', {
            'source': '''
                Object.defineProperty(navigator, 'webdriver', {get: () => undefined});
                Object.defineProperty(navigator, 'plugins', {get: () => [1, 2, 3, 4, 5]});
            '''
        })

        print("[OK] WebDriver setup complete")
        self.load_cookies()

    def load_cookies(self):
        """Load cookies from file for authenticated access"""
        print(f"[INFO] Loading cookies from {COOKIE_FILE}...")

        if not os.path.exists(COOKIE_FILE):
            print(f"[WARNING] Cookie file not found: {COOKIE_FILE}")
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
                        pass

            print("[INFO] Refreshing page with cookies...")
            self.driver.refresh()
            time.sleep(2)
            print("[OK] Cookies loaded successfully")
            return True

        except Exception as e:
            print(f"[WARNING] Failed to load cookies: {e}")
            return False

    def extract_text_safe(self, tree, xpath):
        """Safely extract text from xpath"""
        try:
            elements = tree.xpath(xpath)
            if elements:
                text = elements[0].text_content() if hasattr(elements[0], 'text_content') else str(elements[0])
                return text.strip() if text else None
            return None
        except:
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

    def extract_star_rating(self, tree):
        """Extract star rating"""
        try:
            xpaths = [
                '//*[@id="acrPopover"]/@title',
                '//*[@id="averageCustomerReviews"]//span[@class="a-icon-alt"]',
                '//span[@data-hook="rating-out-of-text"]'
            ]

            for xpath in xpaths:
                text = self.extract_text_safe(tree, xpath)
                if text:
                    match = re.search(r'(\d+\.?\d*)\s*out of\s*5', text)
                    if match:
                        return match.group(1)
                    match = re.search(r'(\d+\.?\d*)', text)
                    if match:
                        return match.group(1)
            return None

        except Exception as e:
            print(f"  [WARNING] Failed to extract star rating: {e}")
            return None

    def extract_count_of_reviews(self, tree):
        """Extract count of reviews"""
        try:
            xpaths = [
                '//*[@id="acrCustomerReviewLink"]/span',
                '//a[@id="acrCustomerReviewLink"]/span',
                '//span[@data-hook="total-review-count"]'
            ]

            for xpath in xpaths:
                text = self.extract_text_safe(tree, xpath)
                if text:
                    text = text.replace(',', '').strip()
                    match = re.search(r'(\d+)', text)
                    if match:
                        return match.group(1)
            return None

        except Exception as e:
            print(f"  [WARNING] Failed to extract count of reviews: {e}")
            return None

    def extract_count_of_star_ratings(self, tree):
        """Extract count of star ratings"""
        try:
            xpaths = [
                '//*[@id="acrCustomerReviewText"]',
                '//span[@id="acrCustomerReviewText"]'
            ]

            for xpath in xpaths:
                text = self.extract_text_safe(tree, xpath)
                if text:
                    text = text.replace(',', '').strip()
                    match = re.search(r'(\d+)', text)
                    if match:
                        return match.group(1)
            return None

        except Exception as e:
            print(f"  [WARNING] Failed to extract count of star ratings: {e}")
            return None

    def scrape_url(self, url, index):
        """Scrape single URL and extract key fields"""
        try:
            print(f"\n{'='*80}")
            print(f"[{index+1}/{len(TEST_URLS)}] Accessing: {url[:80]}...")

            self.driver.get(url)
            time.sleep(random.uniform(3, 5))

            # Get final URL after redirects
            final_url = self.driver.current_url

            # Parse page content
            page_source = self.driver.page_source
            tree = html.fromstring(page_source)

            # Extract fields
            final_sku_price = self.extract_final_sku_price(tree)
            star_rating = self.extract_star_rating(tree)
            count_of_reviews = self.extract_count_of_reviews(tree)
            count_of_star_ratings = self.extract_count_of_star_ratings(tree)

            # Print results
            print(f"\n  [RESULT]")
            print(f"    product_url: {final_url}")
            print(f"    final_sku_price: {final_sku_price}")
            print(f"    star_rating: {star_rating}")
            print(f"    count_of_reviews: {count_of_reviews}")
            print(f"    count_of_star_ratings: {count_of_star_ratings}")

            return {
                'product_url': final_url,
                'final_sku_price': final_sku_price,
                'star_rating': star_rating,
                'count_of_reviews': count_of_reviews,
                'count_of_star_ratings': count_of_star_ratings
            }

        except Exception as e:
            print(f"  [ERROR] Failed to scrape URL: {e}")
            return None

    def run(self):
        """Run test crawler"""
        print("="*80)
        print("Amazon TV Test Crawler")
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
                'final_sku_price': sum(1 for r in results if not r['final_sku_price']),
                'star_rating': sum(1 for r in results if not r['star_rating']),
                'count_of_reviews': sum(1 for r in results if not r['count_of_reviews']),
                'count_of_star_ratings': sum(1 for r in results if not r['count_of_star_ratings'])
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
    crawler = AmazonTestCrawler()
    crawler.run()
