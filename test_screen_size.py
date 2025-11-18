"""
Test script to extract screen_size from specific Amazon URLs
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

class ScreenSizeExtractor:
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

    def extract_retailer_sku_name(self, tree):
        """Extract product title (retailer_sku_name)"""
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
            print(f"  [ERROR] Failed to extract retailer_sku_name: {e}")
            return None

    def extract_screen_size(self, tree, retailer_sku_name=None):
        """Extract screen size (format: '32 inches')
        Ensures proper formatting with 'inches' suffix
        Falls back to extracting from retailer_sku_name if not found in tree
        """
        try:
            # Use po-display.size class to find Screen Size row (most reliable)
            xpaths = [
                '//tr[contains(@class, "po-display.size")]//td[@class="a-span9"]//span[@class="a-size-base po-break-word"]',
                '//table[@class="a-normal a-spacing-small"]//tr[contains(@class, "po-display.size")]//td[@class="a-span9"]//span[@class="a-size-base po-break-word"]',
                '//*[@id="poExpander"]/div[1]/div/table/tbody/tr[2]/td[2]/span',  # tr[2] typically contains Screen Size
                '//tr[contains(@class, "po-display.size")]//span[@class="a-size-base po-break-word"]',
                # Lowest priority: inline-twister Size selector (format: "50-inch")
                '//*[@id="inline-twister-expanded-dimension-text-size_name"]'
            ]

            for i, xpath in enumerate(xpaths, 1):
                size_text = self.extract_text_safe(tree, xpath)
                if size_text:
                    size_text = size_text.strip()
                    print(f"  [DEBUG] XPath #{i} found: '{size_text}' (repr: {repr(size_text)})")

                    # Skip invalid values (like "tilt", "fix", etc.)
                    # Screen size must contain a number
                    if not re.search(r'\d', size_text):
                        print(f"  [DEBUG] Skipped - no digit found")
                        continue

                    # Handle "50-inch" format -> "50 inches"
                    if '-inch' in size_text.lower():
                        # Extract number from "50-inch" or "50-Inch"
                        match = re.search(r'(\d+\.?\d*)-inch', size_text.lower())
                        if match:
                            result = f"{match.group(1)} inches"
                            print(f"  [OK] Matched '-inch' pattern -> '{result}'")
                            return result
                        continue

                    # Check if it's a number (with optional " symbol like "43", "15.6", or "50"") -> "X inches"
                    if re.match(r'^\d+\.?\d*"?$', size_text):
                        # Extract just the number, removing " if present
                        size_number = re.search(r'(\d+\.?\d*)', size_text).group(1)
                        result = f"{size_number} inches"
                        print(f"  [OK] Matched number pattern -> '{result}'")
                        return result

                    # Handle "32 Inches" or "15.6 inches" format
                    # Extract number + "inch/inches" pattern
                    match = re.search(r'(\d+\.?\d*)\s*inch', size_text.lower())
                    if match:
                        result = f"{match.group(1)} inches"
                        print(f"  [OK] Matched 'X inch(es)' pattern -> '{result}'")
                        return result

                    # If no valid screen size format found, continue to next XPath
                    print(f"  [DEBUG] No valid pattern matched, trying next XPath...")
                    continue

            # Fallback: Extract from retailer_sku_name if provided
            if retailer_sku_name:
                print(f"  [INFO] Trying fallback from retailer_sku_name")
                # Look for patterns: "25 inch", "55-Inch", "50"", etc.
                # Matches: number + (space/hyphen + inch/inches OR double quote)
                match = re.search(r'(\d+\.?\d*)(?:[\s-]*inch(?:es)?|")', retailer_sku_name, re.IGNORECASE)
                if match:
                    size_number = match.group(1)
                    result = f"{size_number} inches"
                    print(f"  [OK] Extracted from retailer_sku_name: '{result}'")
                    return result

            print(f"  [NONE] No screen_size found")
            return None

        except Exception as e:
            print(f"  [ERROR] Failed to extract screen size: {e}")
            return None

    def test_url(self, url, index=None):
        """Test a single URL"""
        print("=" * 100)
        if index is not None:
            print(f"URL #{index}: {url}")
        else:
            print(f"Testing: {url}")
        print("=" * 100)

        try:
            self.driver.get(url)
            time.sleep(5)  # Wait for page load

            page_source = self.driver.page_source
            tree = html.fromstring(page_source)

            # Extract retailer_sku_name first
            retailer_sku_name = self.extract_retailer_sku_name(tree)
            print(f"\nProduct Title: {retailer_sku_name or 'N/A'}\n")

            # Extract screen_size
            screen_size = self.extract_screen_size(tree, retailer_sku_name)

            print("\n" + "=" * 100)
            print(f"RESULT: screen_size = '{screen_size}'")
            print("=" * 100 + "\n")

        except Exception as e:
            print(f"ERROR: {e}")
            import traceback
            traceback.print_exc()

    def run_tests(self, urls):
        """Test all URLs"""
        for i, url in enumerate(urls, 1):
            self.test_url(url, index=i)

        self.driver.quit()
        print("=" * 100)
        print("ALL TESTS COMPLETED")
        print("=" * 100)


if __name__ == '__main__':
    urls = [
        "https://www.amazon.com/sspa/click?ie=UTF8&spc=MToxNzE5NDY1NDA0MTYyMTE0OjE3NjM0MjY5NjM6c3BfYnRmOjMwMTA2NDU2NDkxMDgwMjo6MDo6&url=%2FARISKEEN-Cable-2-Pack-48Gbps-Support%2Fdp%2FB0CNGS4GVQ%2Fref%3Dsr_1_101_sspa%3Fcrid%3DRY3XOG3VC795%26dib%3DeyJ2IjoiMSJ9.9FpiCg5q8FbTOq7CcGSgycIeEOsS-vByQBqEj3kIYp4Lq7ubACucp-ryTaSPYMDZBkN_qcKaWd5rXbnZePblK0Se6yrmJkvYwuixdZBwOo8FE15XuZQ2j7ysinVaDWeHQNlaZWUrt2QP0jF0gl6FF_6icKHOBU7NN67sKid5ctqlQbsEmWC0D6lxo006PFLoo45uUqqA2dvDKQj2FAGZnUpJc1k_70MWILnzyjjP1Nc.Yg735AFmv7psAILxb-F6iNUofS-BHYlWWzQcZOdalYU%26dib_tag%3Dse%26keywords%3DTV%26qid%3D1763426963%26sprefix%3Dtv%252Caps%252C287%26sr%3D8-101-spons%26xpid%3DCg0_BApgal-UM%26sp_csd%3Dd2lkZ2V0TmFtZT1zcF9idGY%26psc%3D1",
        "https://www.amazon.com/Amazon-Ambient-Experience-dimming-hands-free/dp/B0CJDJWMDV/ref=sr_1_14?crid=RY3XOG3VC795&dib=eyJ2IjoiMSJ9.QUBdY5Brt_-FoPq2hlnmmenbkf1K3WrVgcoGzxHQ_wcHaRw0hZfsigLRcbmTyvzN742KoqvQ1eqV7r8WCOUWFoGpTTPufE84sQw2_OWxePD6NAn6EzAbgwMClrIjjJg6YX502cjiCn62oSva24Vne0sIHDPxhUA3rmhbG5hgQGGOmnleMdrHbQD9e5iIMZZSlwEXQ4mcsbe2ZLuT-t5B1Ei9vZaAR36oEfP-lOgsg_U.Nvc8WPtE1Lb1lJeRSv-Raz-yeUQYAym-cSlGmnFMo7w&dib_tag=se&keywords=TV&qid=1763426858&sprefix=tv%2Caps%2C287&sr=8-14",
        "https://www.amazon.com/sspa/click?ie=UTF8&spc=MToxNzE5NDY1NDA0MTYyMTE0OjE3NjM0MjY5NjM6c3BfYXRmX25leHQ6MzAxMDY2Mjc0NzcyMTAyOjowOjo&url=%2FProjector-Bluetooth-Portable-Proyector-Sovboi%2Fdp%2FB0G13VRCW2%2Fref%3Dsr_1_82_sspa%3Fcrid%3DRY3XOG3VC795%26dib%3DeyJ2IjoiMSJ9.9FpiCg5q8FbTOq7CcGSgycIeEOsS-vByQBqEj3kIYp4Lq7ubACucp-ryTaSPYMDZBkN_qcKaWd5rXbnZePblK0Se6yrmJkvYwuixdZBwOo8FE15XuZQ2j7ysinVaDWeHQNlaZWUrt2QP0jF0gl6FF_6icKHOBU7NN67sKid5ctqlQbsEmWC0D6lxo006PFLoo45uUqqA2dvDKQj2FAGZnUpJc1k_70MWILnzyjjP1Nc.Yg735AFmv7psAILxb-F6iNUofS-BHYlWWzQcZOdalYU%26dib_tag%3Dse%26keywords%3DTV%26qid%3D1763426963%26sprefix%3Dtv%252Caps%252C287%26sr%3D8-82-spons%26xpid%3DCg0_BApgal-UM%26sp_csd%3Dd2lkZ2V0TmFtZT1zcF9hdGZfbmV4dA%26psc%3D1",
        "https://www.amazon.com/NORTHING-Portable-Antenna-Monitor-Television/dp/B0FQ5M3TPF/ref=sr_1_207?crid=RY3XOG3VC795&dib=eyJ2IjoiMSJ9.WV6TZikrGT8k-4N279g55bQ_nDGJeDep6uBAJVpinps5sQFswGAcyRZJtFIpKvwFx5VRphgx8cN-shDva_BUOFkkmPBrLTVl-_mU1NyzYdKoXQNazPMZasBjNsSKqpEC6B9cgOOz1EKbqePP7wPekswYF6cZRr_W_LASp80yv9mY0xH4XWXJ-gIXlaSD7-r-9lzG8vZRXVSubZYTDqK0A7uvnynzVtFYn9UbipcZZFw.uuegQx4FD7Cw1KFWRZMsk-1TQOZIDkF6kLOTN4FvN6o&dib_tag=se&keywords=TV&qid=1763427108&sprefix=tv%2Caps%2C287&sr=8-207&xpid=Cg0_BApgal-UM",
        "https://www.amazon.com/Kiyomo-Portable-Rotation-Snapdragon-Certified/dp/B0FNQSBWCL/ref=sr_1_256?crid=RY3XOG3VC795&dib=eyJ2IjoiMSJ9.69AS0Ial7qhRDPyGxNlR9fiH0VLBJLbFLdwfVd5JyqMHJUb0jTAxrnbHn3FEugQtUdt4DnBF30m8ZT5ag8M5DDDg4_-NDLMFunaUBtE6bD8ev0c6XIldnASt2GFkhvPOoALTyJfP6Q68KfkiHKhJzL9KM_tt6ahT2nt85Z07ONhOseFvVOCLt4-MWzeOTspsdzpaXLLrPJH9CrN2bnWIbt1PMK0J2SgWOwBKCUcX0_Y.2RbHDae0y5SOa_0HIOE2cPrxXBabenMnsMOSRRgAjyI&dib_tag=se&keywords=TV&qid=1763427167&sprefix=tv%2Caps%2C287&sr=8-256&xpid=Cg0_BApgal-UM",
        "https://www.amazon.com/BRAVIA-K-65XR50-PlayStation%C2%AE5-Digital-Bundle/dp/B0FW82F6Y9/ref=sr_1_76?crid=RY3XOG3VC795&dib=eyJ2IjoiMSJ9.RQR8oDEsna4T_lv0qfgNCWdYf9EgHyS5sqxj3gsAbDVKF7F7Edm5sc-pWgn_2CYpvphKAJlcXgcs9vb--uRTrcBcOf5U7YeQStNnDkI5PyzBc-FJ68Jo-KVYiQppqvepQMyOSxs1XHGCJ0zQ_zPJkxvXEbEQB8kCOO5z7PS7ODQ-_xA7E8W2NO0TCwg1Z_l1PDgmFFj9rrSn_h-tW2vZNlG0kuqcuOtZag8lA-7GR7M.PR2V3Wt2uccAKrL73xjVS3kbsN_Sjwzl1WItVX8GJwI&dib_tag=se&keywords=TV&qid=1763426942&sprefix=tv%2Caps%2C287&sr=8-76&xpid=Cg0_BApgal-UM"
    ]

    extractor = ScreenSizeExtractor()
    extractor.run_tests(urls)
