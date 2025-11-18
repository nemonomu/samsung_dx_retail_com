"""
Test script to extract screen_size from Walmart URLs
Logs: URL, extracted value, and output value
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

class WalmartScreenSizeExtractor:
    def __init__(self):
        self.setup_driver()

    def setup_driver(self):
        """Setup Chrome driver"""
        chrome_options = Options()
        chrome_options.add_argument('--disable-blink-features=AutomationControlled')
        chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
        chrome_options.add_experimental_option('useAutomationExtension', False)
        chrome_options.add_argument('--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36')

        self.driver = webdriver.Chrome(
            service=Service(ChromeDriverManager().install()),
            options=chrome_options
        )

    def extract_screen_size(self, tree):
        """Extract screen size from 'Specifications at a glance' section
        Example: '65 in' -> '65 inches'
        """
        try:
            # Try multiple XPath strategies to find Screen size
            xpaths = [
                # Method 1: Definition list structure - <dl><dt>Screen size</dt><dd>75 in</dd></dl> (main page)
                "//dl[.//dt[contains(., 'Screen size')]]//dd",
                # Method 2: Definition list - direct sibling (main page)
                "//dt[contains(., 'Screen size')]/following-sibling::dd",
                # Method 3: Table structure - <tr><th><dt>Screen size</dt></th><td><dd>75 in</dd></td></tr>
                "//tr[.//dt[contains(text(), 'Screen size')]]//dd",
                # Method 4: Alternative table structure
                "//dt[contains(text(), 'Screen size')]/ancestor::tr//dd",
                # Method 5: Use aria-label (most reliable for div structure)
                "//div[@aria-label[contains(., 'Screen size:')]]/@aria-label",
                # Method 6: Find "Screen size" text and get the next sibling div
                "//div[contains(@class, 'b') and contains(., 'Screen size')]/following-sibling::div//span",
                # Method 7: Direct XPath provided by user (old structure)
                "//*[@id='ip-prod-desc-atf-div-1']/section/section[2]/div/div/div[1]/div[1]/div/div/div[2]/span",
                # Method 8: Find within "Specifications at a glance" container
                "//h3[contains(text(), 'Specifications at a glance')]/parent::div//div[@aria-label[contains(., 'Screen size')]]/@aria-label"
            ]

            screen_size_text = None
            matched_xpath_num = None

            for i, xpath in enumerate(xpaths, 1):
                result = tree.xpath(xpath)
                if result:
                    if isinstance(result[0], str):
                        screen_size_text = result[0].strip()
                    else:
                        screen_size_text = result[0].text_content().strip() if hasattr(result[0], 'text_content') else str(result[0]).strip()

                    if screen_size_text:
                        matched_xpath_num = i
                        print(f"  [XPATH #{i}] Matched: '{screen_size_text}'")
                        break

            if not screen_size_text:
                print(f"  [NONE] No screen_size text found in any XPath")
                return None

            # Extract number from text (including decimal)
            # Examples: "Screen size: 65 in" -> "65", "64.5 in" -> "64.5"
            match = re.search(r'([\d.]+)\s*in', screen_size_text, re.IGNORECASE)
            if match:
                size_number = match.group(1)
                result = f"{size_number} inches"
                print(f"  [OK] Extracted: '{size_number}' -> Output: '{result}'")
                return result
            else:
                print(f"  [FAIL] No 'X in' pattern found in: '{screen_size_text}'")
                return None

        except Exception as e:
            print(f"  [ERROR] Failed to extract screen size: {e}")
            import traceback
            traceback.print_exc()
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

            # Extract screen_size
            screen_size = self.extract_screen_size(tree)

            print("\n" + "-" * 120)
            print(f"FINAL RESULT: screen_size = '{screen_size}'")
            print("-" * 120 + "\n")

        except Exception as e:
            print(f"ERROR: {e}")
            import traceback
            traceback.print_exc()

    def run_tests(self, urls):
        """Test all URLs"""
        total = len(urls)
        for i, url in enumerate(urls, 1):
            print(f"\n\n{'#' * 120}")
            print(f"TESTING {i}/{total}")
            print(f"{'#' * 120}\n")
            self.test_url(url, index=i)

        self.driver.quit()
        print("\n" + "=" * 120)
        print(f"ALL TESTS COMPLETED: {total} URLs tested")
        print("=" * 120)


if __name__ == '__main__':
    urls = [
        "https://www.walmart.com/ip/Hisense-75-Class-U7-Series-Mini-LED-ULED-4K-UHD-Roku-Smart-TV-75H5QBR-2025-Model-QLED-Native-165Hz-1000-Nit-Dolby-Vision-IQ-Full-Array-Local-Dimming/16732218557",
        "https://www.walmart.com/ip/Samsung-65-Class-QLED-Q6F-4K-Smart-TV-2025-QN65Q6FAAFXZA/16784312617",
        "https://www.walmart.com/ip/VIZIO-86-Class-4K-UHD-LED-HDR-Smart-TV-New-V4K86C-0804/5195359733",
        "https://www.walmart.com/ip/Samsung-65-Class-Crystal-UHD-U7900F-4K-Smart-TV-2025-UN65U7900FFXZA/16815105564",
        "https://www.walmart.com/ip/onn-32-Class-HD-720P-LED-Roku-Smart-Television-100012589/314022535",
        "https://www.walmart.com/ip/50-HISENSE-4K-GOOGLE-TV/17309421750",
        "https://www.walmart.com/ip/Hisense-40-Class-FHD-1080P-Roku-Smart-LED-TV-40H4030F1/470905078",
        "https://www.walmart.com/ip/onn-50-Class-4K-UHD-2160P-LED-Roku-Smart-Television-HDR-100012585/300694285",
        "https://www.walmart.com/ip/TCL-98-Google-Smart-TV-98Q51CG/13621223713",
        "https://www.walmart.com/ip/Samsung-55-Class-Crystal-UHD-U7900F-4K-Smart-TV-2025-UN55U7900FFXZA/16436012780",
        "https://www.walmart.com/ip/Hisense-58-Class-4K-UHD-LED-LCD-Roku-Smart-TV-HDR-R6-Series-58R6E3/587182688",
        "https://www.walmart.com/ip/onn-24-Class-HD-720P-LED-Roku-Smart-Television-100012590/959853005",
        "https://www.walmart.com/ip/VIZIO-40-Class-Full-HD-1080p-LED-Smart-TV-New-VFD40M-08/5195359732",
        "https://www.walmart.com/ip/VIZIO-55-Class-4K-UHD-LED-HDR-Smart-TV-New-V4K55M-08/5197667450",
        "https://www.walmart.com/ip/SAMSUNG-77-Class-S90D-OLED-Smart-TV-QN77S90DAFXZA-2024/5337847611",
        "https://www.walmart.com/ip/onn-55-Class-4K-UHD-2160P-LED-Roku-Smart-Television-HDR-100012586/201216466",
        "https://www.walmart.com/ip/Hisense-32-Class-FHD-1080p-Smart-LED-TV-32A45K/8703258904",
        "https://www.walmart.com/ip/Hisense-65-Class-4K-UHD-LCD-Roku-Smart-TV-HDR-R6-Series-65R6E4/771452270",
        "https://www.walmart.com/ip/VIZIO-70-Class-4K-UHD-LED-HDR-Smart-TV-New-V4K70M-08/5197667453",
        "https://www.walmart.com/ip/VIZIO-75-Class-4K-UHD-LED-HDR-Smart-TV-New-V4K75M-08/5195359735",
        "https://www.walmart.com/ip/Samsung-QN65Q7FBAFXZA/14316322212",
        "https://www.walmart.com/ip/Samsung-UN50U8000F/15083853099",
        "https://www.walmart.com/ip/Samsung-UN65U8000F/15073169446",
        "https://www.walmart.com/ip/Samsung-UN43U8000F/15093811177",
        "https://www.walmart.com/ip/Samsung-75-Class-QLED-Q6F-4K-Smart-TV-2025-QN75Q6FAAFXZA/16721466651",
        "https://www.walmart.com/ip/Hisense-75-Class-4K-UHD-LED-LCD-Roku-Smart-TV-HDR-R6-Series-75R6E4/953460510",
        "https://www.walmart.com/ip/FPD-50-4K-UHD-LED-Google-Smart-Television-HDR/17459850344",
        "https://www.walmart.com/ip/onn-43-Class-4K-UHD-2160P-LED-Roku-Smart-Television-HDR-100012584/428114216",
        "https://www.walmart.com/ip/Samsung-UN85U8000F/15031766627",
        "https://www.walmart.com/ip/Hisense-85-Class-4K-UHD-LED-LCD-Roku-Smart-TV-HDR-R6-Series-85R6E4/3491792460",
        "https://www.walmart.com/ip/VIZIO-32-Class-Full-HD-1080p-LED-Smart-TV-New-VFD32M-0807/5337847390",
        "https://www.walmart.com/ip/Samsung-UN70U8000F/15077158707",
        "https://www.walmart.com/ip/Hisense-43-Class-4K-UHD-LED-LCD-Smart-Roku-TV-HDR-R6-Series-43R6E3/213300402",
        "https://www.walmart.com/ip/Hisense-70-Class-4K-UHD-LED-LCD-Roku-Smart-TV-HDR-R6-Series-70R6E4/2484260597",
        "https://www.walmart.com/ip/VIZIO-100-Class-Quantum-4K-QLED-HDR-Smart-TV-NEW-VQD100M-0804/7763919612",
        "https://www.walmart.com/ip/FPD-55-4K-UHD-LED-Google-Smart-Television-HDR/17429409326",
        "https://www.walmart.com/ip/onn-32-Class-HD-720P-Smart-LED-TV-100012589/18375223072",
        "https://www.walmart.com/ip/Samsung-QN65QN70FAF-65-Smart-LED-LCD-TV-4K-UHDTV-qn65qn70fafxza/16300115318",
        "https://www.walmart.com/ip/TCL-65-Class-Q6-65Q651G-4K-UHD-HDR-QLED-Smart-TV-with-Google-TV-NEW-2024/5378490185",
        "https://www.walmart.com/ip/Hisense-55-Class-U7-Series-Mini-LED-ULED-4K-UHD-Google-Smart-TV-55U75Q-2025-Model-QLED-Native-165Hz-1000-Nit-Dolby-Vision-IQ-Full-Array-Local-Dimming/16031006937",
        "https://www.walmart.com/ip/Vizio-D24FM-K01-24-in-D-Series-Full-HD-Smart-TV/145006565",
        "https://www.walmart.com/ip/TCL-98-QM6K-Series-QD-Mini-LED-QLED-4K-UHD-Smart-TV-with-Google-TV-NEW-2025-98QM6K/15085365747",
        "https://www.walmart.com/ip/LG-65-Inch-4K-HDR-Smart-Quantum-Dot-NanoCell-Mini-LED-TV-2024/5519716535",
        "https://www.walmart.com/ip/LG-75-Inch-4K-HDR-Smart-Quantum-Dot-NanoCell-Mini-LED-TV-2024/5513854593",
        "https://www.walmart.com/ip/Restored-Westinghouse-32-720P-HD-Smart-Roku-TV-WR32HT2212-Refurbished/1576486772",
        "https://www.walmart.com/ip/TCL-32-Class-S-Class-720p-HD-LED-Smart-TV-with-Google-TV-32S250G-New/5084872018",
        "https://www.walmart.com/ip/FPD-32-inch-Palette-Series-HD-720p-Smart-Google-TV-Dolby-Atmos-Hdr-10-Bluetooth/13368304057",
        "https://www.walmart.com/ip/FPD-43-Inch-Palette-Series-1080p-Full-HD-LED-Television-Single-Piece-with-Smart-TV-Accessories/13337215414",
        "https://www.walmart.com/ip/SYLVOX-Kitchen-TV-15-6-inch-Smart-TV-Google-System-1080P-FHD-Small-TV-Rotated-Foldable-Support-Google-Assistant-WiFi-Bluetooth-Cabinet-TV-Kitchen-Bed/5324670810",
        "https://www.walmart.com/ip/VIZIO-43-Class-D-Series-FHD-LED-Smart-TV-2023-Online-Only-D43fM-K04/1777915686",
        "https://www.walmart.com/ip/SAMSUNG-40-Class-N5200-Series-Full-HD-1080P-LED-Smart-Television-UN40N5200AFXZA/697559735",
        "https://www.walmart.com/ip/Westinghouse-40-inch-Smart-TV-FHD-1080P-Xumo-TV-w-Voice-Remote-Flat-Screen-LED-Television-w-Apple-Home-kit-Wi-Fi-Mobile-Connectivity/16748318973",
        "https://www.walmart.com/ip/Supersonic-SC-1926SDVD-19-inch-AC-DC-LED-SMART-TV-Built-in-DVD-Powered-by-VIDAA-LED/14071706329",
        "https://www.walmart.com/ip/OLED77C5PUA-AUS/14340574575",
        "https://www.walmart.com/ip/55-Roku-Plus-Series/15942718986",
        "https://www.walmart.com/ip/Sceptre-50-Class-4K-UHD-LED-TV-U515CV-U/44829924",
        "https://www.walmart.com/ip/Sony-32-Class-W830K-720p-HD-LED-HDR-TV-with-Google-TV-and-Google-Assistant-2022-Model/718083895",
        "https://www.walmart.com/ip/VIZIO-65-Class-V-Series-4K-UHD-LED-SmartCast-Smart-TV-HDR-V655-J/292825097",
        "https://www.walmart.com/ip/SAMSUNG-55-Class-S90C-OLED-4K-Smart-TV-QN55S90CAFXZA-2023/2472636061",
        "https://www.walmart.com/ip/Westinghouse-QX-Series-43-Edgeless-QLED-4K-UHD-Roku-TV-WR43QX400-2024/5534601509",
        "https://www.walmart.com/ip/SYLVOX-22-inch-Smart-RV-T-12-Vot-V-with-DVD-Player-1080P-FHD-Smart-Android-Tv-Free-Download-Apps-Support-Wif-Bluetoot-Limo-Series/3055314192",
        "https://www.walmart.com/ip/65-Roku-Pro-Series/15941352037",
        "https://www.walmart.com/ip/43-in-CU8000-Crystal-UHD-2160p-120-Hz-4K-HDR-Smart-LED-TV/5187633671",
        "https://www.walmart.com/ip/RCA-32-720p-HD-Smart-LED-TV-TC-LE32K-AN2401-Android-TV/14586820804",
        "https://www.walmart.com/ip/Norcent-24-Inch-720P-LED-HD-Backlight-Flat-TV-DVD-Combo-with-Multimedia-Access/539101623",
        "https://www.walmart.com/ip/SuperSonic-43-High-Definition-Smart-TV-SC-4316STV/881809037",
        "https://www.walmart.com/ip/Restored-Westinghouse-24-720p-LED-Roku-Smart-TV-WR24HT2212-Refurbished/3110948754",
        "https://www.walmart.com/ip/SAMSUNG-50-Class-LS03B-The-Frame-QLED-4K-Smart-TV-QN50LS03BAFXZA/821644919",
        "https://www.walmart.com/ip/SAMSUNG-55-Class-LS03B-The-Frame-QLED-4K-Smart-TV-QN55LS03BAFXZA/944779027",
        "https://www.walmart.com/ip/SAMSUNG-65-Class-LS03B-The-Frame-QLED-4K-Smart-TV-QN65LS03BAFXZA/876051375"
    ]

    extractor = WalmartScreenSizeExtractor()
    extractor.run_tests(urls)
