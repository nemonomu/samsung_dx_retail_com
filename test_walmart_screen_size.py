"""
Test script for Walmart screen_size extraction
Tests the extract_screen_size() function from wmart_tv_dt1.py
with NULL-causing URLs to identify extraction issues
"""
import time
import random
import undetected_chromedriver as uc
from lxml import html
import re

# Test URLs (6 URLs provided by user)
TEST_URLS = [
    "https://www.walmart.com/ip/SuperSonic-43-High-Definition-Smart-TV-SC-4316STV/881809037",
    "https://www.walmart.com/ip/Restored-Westinghouse-24-720p-LED-Roku-Smart-TV-WR24HT2212-Refurbished/3110948754",
    "https://www.walmart.com/ip/Supersonic-SC-1926SDVD-19-inch-AC-DC-LED-SMART-TV-Built-in-DVD-Powered-by-VIDAA-LED/14071706329",
    "https://www.walmart.com/ip/Norcent-24-Inch-720P-LED-HD-Backlight-Flat-TV-DVD-Combo-with-Multimedia-Access/539101623",
    "https://www.walmart.com/ip/Restored-Westinghouse-32-720P-HD-Smart-Roku-TV-WR32HT2212-Refurbished/1576486772",
    "https://www.walmart.com/ip/Restored-Westinghouse-24-720p-LED-Roku-Smart-TV-WR24HT2212-Refurbished/3110948754"
]

# Expected screen sizes for each URL (based on product names)
EXPECTED_SCREEN_SIZES = {
    "881809037": "43 inches",
    "3110948754": "24 inches",
    "14071706329": "19 inches",
    "539101623": "24 inches",
    "1576486772": "32 inches"
}


class ScreenSizeTester:
    def __init__(self):
        self.driver = None
        self.test_results = []

    def setup_driver(self):
        """Setup Chrome WebDriver"""
        print("\n[SETUP] Initializing Chrome WebDriver...")
        options = uc.ChromeOptions()
        options.page_load_strategy = 'none'
        options.add_argument('--disable-dev-shm-usage')
        options.add_argument('--no-sandbox')
        options.add_argument('--disable-gpu')
        options.add_argument('--window-size=1920,1080')
        options.add_argument('--lang=en-US,en;q=0.9')

        self.driver = uc.Chrome(options=options)
        self.driver.set_page_load_timeout(120)
        print("[SETUP] ✅ WebDriver initialized successfully\n")

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

    def extract_screen_size_with_detailed_log(self, tree, retailer_sku_name=None, url=None):
        """Extract screen size with detailed logging for each XPath attempt
        This is a copy of extract_screen_size() from wmart_tv_dt1.py with enhanced logging
        """
        print(f"\n{'='*80}")
        print(f"[TEST] Extracting screen_size...")
        print(f"[INFO] Product Name: {retailer_sku_name or 'N/A'}")
        print(f"[INFO] URL: {url or 'N/A'}")
        print(f"{'='*80}")

        # Extract item ID from URL for expected value lookup
        item_id = url.rstrip('/').split('/')[-1] if url else None
        expected_value = EXPECTED_SCREEN_SIZES.get(item_id, "Unknown")
        print(f"[EXPECTED] Screen Size: {expected_value}")
        print(f"{'-'*80}")

        try:
            # Try multiple XPath strategies to find Screen size
            xpaths = [
                # Method 1: Definition list structure
                ("Method 1: Definition list", "//dl[.//dt[contains(., 'Screen size')]]//dd"),
                # Method 2: Definition list - direct sibling
                ("Method 2: Sibling dd", "//dt[contains(., 'Screen size')]/following-sibling::dd"),
                # Method 3: Table structure
                ("Method 3: Table dd", "//tr[.//dt[contains(text(), 'Screen size')]]//dd"),
                # Method 4: Alternative table structure
                ("Method 4: Table ancestor", "//dt[contains(text(), 'Screen size')]/ancestor::tr//dd"),
                # Method 5: Use aria-label
                ("Method 5: aria-label", "//div[@aria-label[contains(., 'Screen size:')]]/@aria-label"),
                # Method 6: Find "Screen size" text and get the next sibling div
                ("Method 6: Next sibling", "//div[contains(@class, 'b') and contains(., 'Screen size')]/following-sibling::div//span"),
                # Method 7: Direct XPath
                ("Method 7: Direct XPath", "//*[@id='ip-prod-desc-atf-div-1']/section/section[2]/div/div/div[1]/div[1]/div/div/div[2]/span"),
                # Method 8: Within "Specifications at a glance" container
                ("Method 8: Specs container", "//h3[contains(text(), 'Specifications at a glance')]/parent::div//div[@aria-label[contains(., 'Screen size')]]/@aria-label")
            ]

            screen_size_text = None
            successful_method = None

            print("[XPath Tests]")
            for method_name, xpath in xpaths:
                result = tree.xpath(xpath)
                if result:
                    if isinstance(result[0], str):
                        screen_size_text = result[0].strip()
                    else:
                        screen_size_text = result[0].text_content().strip() if hasattr(result[0], 'text_content') else str(result[0]).strip()

                    if screen_size_text:
                        print(f"  ✅ {method_name}: Found '{screen_size_text}'")
                        successful_method = method_name
                        break
                    else:
                        print(f"  ⚠️ {method_name}: Found element but empty text")
                else:
                    print(f"  ❌ {method_name}: Not found")

            print(f"{'-'*80}")

            if not screen_size_text:
                print("[Fallback] XPath extraction failed, trying regex from product name...")
                # Fallback: Extract from retailer_sku_name (product name)
                if retailer_sku_name:
                    # Look for patterns: "32"", "50-Inch", "98" Q Series", etc.
                    match = re.search(r'(\d+\.?\d*)(?:[\s-]*inch(?:es)?|")', retailer_sku_name, re.IGNORECASE)
                    if match:
                        size_number = match.group(1)
                        final_value = f"{size_number} inches"
                        print(f"  ✅ Regex Pattern: Found '{size_number}' from product name")
                        print(f"  ✅ Extracted Value: {final_value}")

                        # Compare with expected
                        status = "✅ MATCH" if final_value == expected_value else "❌ MISMATCH"
                        print(f"\n[RESULT] {status}")
                        print(f"  Extracted: {final_value}")
                        print(f"  Expected:  {expected_value}")
                        print(f"  Method:    Regex from product name")

                        return {
                            'extracted': final_value,
                            'expected': expected_value,
                            'method': 'Regex from product name',
                            'status': 'match' if final_value == expected_value else 'mismatch'
                        }
                    else:
                        print(f"  ❌ Regex Pattern: No match in product name")
                else:
                    print(f"  ❌ Product name is None")

                print(f"\n[RESULT] ❌ FAILED - Could not extract screen size")
                print(f"  Extracted: NULL")
                print(f"  Expected:  {expected_value}")
                return {
                    'extracted': None,
                    'expected': expected_value,
                    'method': 'Failed',
                    'status': 'failed'
                }

            # Extract number from text (including decimal)
            print(f"[Parse] Parsing '{screen_size_text}'...")
            match = re.search(r'([\d.]+)\s*in', screen_size_text, re.IGNORECASE)
            if match:
                size_number = match.group(1)
                final_value = f"{size_number} inches"
                print(f"  ✅ Parsed: {final_value}")

                # Compare with expected
                status = "✅ MATCH" if final_value == expected_value else "❌ MISMATCH"
                print(f"\n[RESULT] {status}")
                print(f"  Extracted: {final_value}")
                print(f"  Expected:  {expected_value}")
                print(f"  Method:    {successful_method}")

                return {
                    'extracted': final_value,
                    'expected': expected_value,
                    'method': successful_method,
                    'status': 'match' if final_value == expected_value else 'mismatch'
                }
            else:
                print(f"  ❌ Parse Failed: Could not extract number from '{screen_size_text}'")
                print(f"\n[RESULT] ❌ FAILED - Parse error")
                print(f"  Extracted: NULL")
                print(f"  Expected:  {expected_value}")
                return {
                    'extracted': None,
                    'expected': expected_value,
                    'method': 'Parse failed',
                    'status': 'failed'
                }

        except Exception as e:
            print(f"\n[ERROR] Exception occurred: {e}")
            print(f"[RESULT] ❌ FAILED - Exception")
            print(f"  Extracted: NULL")
            print(f"  Expected:  {expected_value}")
            import traceback
            traceback.print_exc()
            return {
                'extracted': None,
                'expected': expected_value,
                'method': 'Exception',
                'status': 'failed'
            }

    def extract_product_name(self, tree):
        """Extract product name from page"""
        try:
            # Try multiple XPaths to find product name
            xpaths = [
                "//h1[@itemprop='name']",
                "//h1[contains(@class, 'prod-ProductTitle')]",
                "//*[@id='maincontent']//h1",
                "//h1"
            ]

            for xpath in xpaths:
                result = tree.xpath(xpath)
                if result:
                    text = result[0].text_content().strip() if hasattr(result[0], 'text_content') else str(result[0]).strip()
                    if text:
                        return text
            return None
        except:
            return None

    def test_url(self, url, test_num, total):
        """Test screen_size extraction for a single URL"""
        print(f"\n\n{'#'*80}")
        print(f"# TEST {test_num}/{total}")
        print(f"{'#'*80}")
        print(f"[URL] {url}")

        try:
            # Check if browser is still alive (crash detection from wmart_tv_dt1.py)
            try:
                _ = self.driver.current_url
            except Exception as e:
                print(f"  [WARNING] Browser window crashed, restarting driver...")
                try:
                    self.driver.quit()
                except:
                    pass
                self.setup_driver()
                print(f"  [OK] Driver restarted successfully")

            # Load page
            print(f"[LOADING] Accessing page...")
            self.driver.get(url)
            time.sleep(random.uniform(4, 6))  # Random wait from wmart_tv_dt1.py
            print(f"[LOADING] ✅ Page loaded")

            # Get page source and parse
            page_source = self.driver.page_source
            tree = html.fromstring(page_source)

            # Extract product name
            product_name = self.extract_product_name(tree)

            # Extract screen size with detailed logging
            result = self.extract_screen_size_with_detailed_log(tree, product_name, url)

            # Store result
            self.test_results.append({
                'url': url,
                'product_name': product_name,
                **result
            })

            return True  # Success

        except Exception as e:
            print(f"\n[ERROR] Test failed: {e}")
            import traceback
            traceback.print_exc()

            self.test_results.append({
                'url': url,
                'product_name': None,
                'extracted': None,
                'expected': EXPECTED_SCREEN_SIZES.get(url.rstrip('/').split('/')[-1], "Unknown"),
                'method': 'Exception',
                'status': 'failed'
            })

            return False  # Failed

    def print_summary(self):
        """Print summary of all test results"""
        print(f"\n\n{'='*80}")
        print(f"SUMMARY - Test Results for {len(self.test_results)} URLs")
        print(f"{'='*80}\n")

        match_count = sum(1 for r in self.test_results if r['status'] == 'match')
        mismatch_count = sum(1 for r in self.test_results if r['status'] == 'mismatch')
        failed_count = sum(1 for r in self.test_results if r['status'] == 'failed')

        print(f"✅ Match:    {match_count}/{len(self.test_results)}")
        print(f"❌ Mismatch: {mismatch_count}/{len(self.test_results)}")
        print(f"❌ Failed:   {failed_count}/{len(self.test_results)}")
        print(f"{'-'*80}\n")

        for idx, result in enumerate(self.test_results, 1):
            status_icon = "✅" if result['status'] == 'match' else "❌"
            print(f"{status_icon} Test {idx}:")
            print(f"   URL:       {result['url'][:70]}...")
            print(f"   Product:   {result['product_name'][:70] if result['product_name'] else 'N/A'}...")
            print(f"   Extracted: {result['extracted']}")
            print(f"   Expected:  {result['expected']}")
            print(f"   Method:    {result['method']}")
            print(f"   Status:    {result['status'].upper()}")
            print()

    def run(self):
        """Run all tests"""
        try:
            self.setup_driver()

            total = len(TEST_URLS)
            for idx, url in enumerate(TEST_URLS, 1):
                # Retry logic from wmart_tv_dt1.py: try up to 3 times
                max_retries = 3
                success = False
                for attempt in range(max_retries):
                    result = self.test_url(url, idx, total)
                    if result:  # Success
                        success = True
                        break
                    else:  # Failed
                        if attempt < max_retries - 1:  # Not the last attempt
                            print(f"  [RETRY] Attempt {attempt + 1} failed, retrying... ({attempt + 2}/{max_retries})")
                            time.sleep(random.uniform(5, 8))  # Wait before retry
                        else:
                            print(f"  [FAILED] All {max_retries} attempts failed for this URL")

                # Wait between tests (from wmart_tv_dt1.py)
                if idx < total:
                    wait_time = random.uniform(3, 5)
                    print(f"\n[WAIT] Waiting {wait_time:.1f}s before next test...")
                    time.sleep(wait_time)

            # Print summary
            self.print_summary()

        except Exception as e:
            print(f"\n[FATAL ERROR] Test runner failed: {e}")
            import traceback
            traceback.print_exc()

        finally:
            if self.driver:
                self.driver.quit()
                print("\n[CLEANUP] WebDriver closed")


if __name__ == "__main__":
    print(f"\n{'='*80}")
    print("Walmart Screen Size Extraction Test")
    print("Testing screen_size extraction logic from wmart_tv_dt1.py")
    print(f"{'='*80}")

    tester = ScreenSizeTester()
    tester.run()

    print("\n[DONE] All tests completed!")
