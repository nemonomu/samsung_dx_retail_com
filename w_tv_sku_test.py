"""
Walmart TV SKU Extraction Test
제품명, URL, SKU만 추출하여 로그 출력 (DB 저장 없음)
"""
import time
import random
import psycopg2
import re
from datetime import datetime
import undetected_chromedriver as uc
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from lxml import html

# Database configuration (URL 로드용)
DB_CONFIG = {
    'host': 'samsung-dx-crawl.csnixzmkuppn.ap-northeast-2.rds.amazonaws.com',
    'port': 5432,
    'database': 'postgres',
    'user': 'postgres',
    'password': 'admin2025!'
}

class WalmartSKUTester:
    def __init__(self):
        self.driver = None
        self.db_conn = None
        self.xpaths = {}
        self.total_tested = 0

    def connect_db(self):
        """Connect to PostgreSQL database"""
        try:
            self.db_conn = psycopg2.connect(**DB_CONFIG)
            print("[OK] Database connected")
            return True
        except Exception as e:
            print(f"[ERROR] Database connection failed: {e}")
            return False

    def load_xpaths(self):
        """Load XPath selectors from database"""
        try:
            cursor = self.db_conn.cursor()
            cursor.execute("""
                SELECT data_field, xpath, css_selector
                FROM xpath_selectors
                WHERE mall_name = 'Walmart' AND page_type = 'detail' AND is_active = TRUE
            """)

            for row in cursor.fetchall():
                self.xpaths[row[0]] = {
                    'xpath': row[1],
                    'css': row[2]
                }

            cursor.close()
            print(f"[OK] Loaded {len(self.xpaths)} XPath selectors\n")
            return True

        except Exception as e:
            print(f"[ERROR] Failed to load XPaths: {e}")
            return False

    def load_product_urls(self):
        """Load product URLs from wmart_tv_main_crawl and wmart_tv_bsr_crawl"""
        try:
            cursor = self.db_conn.cursor()

            # Get latest batch from main
            cursor.execute("""
                SELECT batch_id FROM wmart_tv_main_crawl
                WHERE batch_id IS NOT NULL
                ORDER BY batch_id DESC LIMIT 1
            """)
            main_batch_result = cursor.fetchone()
            main_batch_id = main_batch_result[0] if main_batch_result else None

            # Get latest batch from bsr
            cursor.execute("""
                SELECT batch_id FROM wmart_tv_bsr_crawl
                WHERE batch_id IS NOT NULL
                ORDER BY batch_id DESC LIMIT 1
            """)
            bsr_batch_result = cursor.fetchone()
            bsr_batch_id = bsr_batch_result[0] if bsr_batch_result else None

            # Load main URLs
            main_urls = []
            if main_batch_id:
                cursor.execute("""
                    SELECT "order", Product_url
                    FROM wmart_tv_main_crawl
                    WHERE batch_id = %s
                      AND Product_url IS NOT NULL
                      AND Product_url != ''
                    ORDER BY "order"
                    LIMIT 10
                """, (main_batch_id,))
                main_urls = [{'mother': 'main', 'order': row[0], 'url': row[1]} for row in cursor.fetchall()]

            # Load BSR URLs
            bsr_urls = []
            if bsr_batch_id:
                cursor.execute("""
                    SELECT "order", Product_url
                    FROM wmart_tv_bsr_crawl
                    WHERE batch_id = %s
                      AND Product_url IS NOT NULL
                      AND Product_url != ''
                    ORDER BY "order"
                    LIMIT 10
                """, (bsr_batch_id,))
                bsr_urls = [{'mother': 'bsr', 'order': row[0], 'url': row[1]} for row in cursor.fetchall()]

            cursor.close()

            # Remove duplicates
            main_url_set = {item['url'] for item in main_urls}
            unique_bsr_urls = [item for item in bsr_urls if item['url'] not in main_url_set]

            all_urls = main_urls + unique_bsr_urls
            print(f"[OK] Loaded {len(main_urls)} main URLs + {len(unique_bsr_urls)} unique bsr URLs = {len(all_urls)} total\n")
            return all_urls

        except Exception as e:
            print(f"[ERROR] Failed to load URLs: {e}")
            return []

    def setup_driver(self):
        """Setup Chrome WebDriver"""
        options = uc.ChromeOptions()
        options.add_argument('--disable-dev-shm-usage')
        options.add_argument('--no-sandbox')
        options.add_argument('--disable-gpu')
        options.add_argument('--window-size=1920,1080')

        self.driver = uc.Chrome(options=options)
        self.driver.set_page_load_timeout(60)
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
        except:
            return None

    def is_invalid_sku(self, sku):
        """Check if SKU is invalid"""
        if not sku:
            return True

        sku_clean = sku.strip()

        # Exact match invalid values
        invalid_values = ['4K UHD', '4K (2160P)', '3840 x 2160', '1920 x 1080', '1080p', '1080i', '720p', '480p', '480i', 'Samsung', 'Hisense']
        if sku_clean in invalid_values:
            return True

        # Contains semicolon (multiple resolutions listed)
        if ';' in sku_clean:
            return True

        # Pattern 1: Refresh rate (60Hz, 120Hz, 144Hz, etc.)
        if re.search(r'^\d+Hz$', sku_clean, re.IGNORECASE):
            return True

        # Pattern 2: Resolution with x (3,840 x 2,160 or 3840 x 2160 or 1920 x 1080)
        if re.search(r'\d{1,3}(,\d{3})?\s*x\s*\d{1,3}(,\d{3})?', sku_clean, re.IGNORECASE):
            return True

        # Pattern 3: Resolution format (480i, 480p, 720p, 1080i, 1080p, 2160p, etc.)
        if re.search(r'^\d{3,4}[ip]$', sku_clean, re.IGNORECASE):
            return True

        # Pattern 4: Contains parentheses with resolution like (2160p), (1080p)
        if '(' in sku_clean and ')' in sku_clean:
            if re.search(r'\(\d+[ip]\)', sku_clean):
                return True

        return False

    def extract_sku_from_url(self, url):
        """Extract SKU from product URL"""
        try:
            from urllib.parse import urlparse
            parsed = urlparse(url)
            path_parts = parsed.path.strip('/').split('/')

            if len(path_parts) < 2 or path_parts[0] != 'ip':
                return None

            product_part = path_parts[1]

            # Pattern 1: Simple model at end
            if len(path_parts) == 2:
                model = product_part.replace('-AUS', '')
                if model and len(model) > 3:
                    return model

            # Pattern 2: Model within product name
            parts = product_part.split('-')

            # Check if last part is pure numeric model (8+ digits, like 100012589)
            if parts and parts[-1].isdigit() and len(parts[-1]) >= 8:
                return parts[-1]

            potential_models = []

            for i, part in enumerate(parts):
                if not part or part.isdigit() or part.isalpha():
                    continue
                if part.lower() in ['class', 'inch', 'hd', 'uhd', 'led', 'lcd', 'smart', 'tv', 'new', 'with']:
                    continue

                has_letter = any(c.isalpha() for c in part)
                has_number = any(c.isdigit() for c in part)

                if has_letter and has_number and len(part) >= 5:
                    model = part
                    # Check for numeric suffix (2-4 digits)
                    if i + 1 < len(parts):
                        next_part = parts[i + 1]
                        if next_part.isdigit() and 2 <= len(next_part) <= 4:
                            model = f"{part}-{next_part}"
                    potential_models.append(model)

            if potential_models:
                return max(potential_models, key=len)

            return None
        except:
            return None

    def extract_sku_from_product_name(self, product_name):
        """Extract SKU from product name"""
        try:
            if not product_name:
                return None

            # Pattern 1: Comma-separated at the end
            if ',' in product_name:
                parts = product_name.split(',')
                last_part = parts[-1].strip()
                if last_part and not ' ' in last_part:
                    has_letter = any(c.isalpha() for c in last_part)
                    has_number = any(c.isdigit() for c in last_part)
                    if has_letter and has_number and 5 <= len(last_part) <= 20:
                        return last_part

            # Pattern 2: In parentheses
            paren_match = re.search(r'\(([A-Z0-9]+)\)', product_name)
            if paren_match:
                model = paren_match.group(1)
                if 5 <= len(model) <= 20:
                    return model

            # Pattern 3: At the end after space
            words = product_name.split()
            if words:
                last_word = words[-1].strip('.,;:')
                has_letter = any(c.isalpha() for c in last_word)
                has_number = any(c.isdigit() for c in last_word)
                if has_letter and has_number and 5 <= len(last_word) <= 20:
                    if last_word.upper() not in ['HD', 'UHD', 'LED', 'LCD', '4K', 'TV']:
                        return last_word

            return None
        except:
            return None

    def extract_sku_from_lg_xpath(self):
        """Extract SKU using LG-specific XPath"""
        try:
            page_source = self.driver.page_source
            tree = html.fromstring(page_source)

            lg_xpath = '//*[@id="inpage_container"]/div[2]/div/div/div/div[1]'
            sku = self.extract_text_safe(tree, lg_xpath)

            if sku and 5 <= len(sku) <= 20:
                return sku
            return None
        except:
            return None

    def click_specifications_and_get_model(self):
        """Extract SKU from Specifications dialog with fallback methods"""
        try:
            # Scroll to Specifications
            self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight / 2);")
            time.sleep(2)

            # Click Specifications arrow
            specs_arrow_xpaths = [
                "//button[@aria-label='Specifications']",
                "//button[@aria-label='Specifications']//i[contains(@class, 'ChevronDown')]",
                "//h2[contains(text(), 'Specifications')]/parent::*/following-sibling::div//button"
            ]

            specs_clicked = False
            for xpath in specs_arrow_xpaths:
                try:
                    arrow_btn = self.driver.find_element(By.XPATH, xpath)
                    self.driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", arrow_btn)
                    time.sleep(1)
                    self.driver.execute_script("arguments[0].click();", arrow_btn)
                    time.sleep(2)
                    specs_clicked = True
                    break
                except:
                    continue

            if not specs_clicked:
                return None

            # Click More details
            more_details_xpaths = [
                "//button[@aria-label='More details']",
                "//button[contains(text(), 'More details')]"
            ]

            details_clicked = False
            for xpath in more_details_xpaths:
                try:
                    btn = self.driver.find_element(By.XPATH, xpath)
                    self.driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", btn)
                    time.sleep(1)
                    self.driver.execute_script("arguments[0].click();", btn)
                    time.sleep(5)
                    details_clicked = True
                    break
                except:
                    continue

            if not details_clicked:
                return None

            # Extract Model
            page_source = self.driver.page_source
            tree = html.fromstring(page_source)

            model_xpaths = [
                "//h3[text()='Model']/following-sibling::div//span",
                "//h3[contains(text(), 'Model')]/following-sibling::div/div/span",
                "/html/body/div[2]/div/div[2]/div[1]/div/div[2]/div/div/div[7]/div/span"
            ]

            model = None
            for xpath in model_xpaths:
                extracted = self.extract_text_safe(tree, xpath)
                if extracted and 3 < len(extracted) < 50:
                    model_lower = extracted.lower()
                    if not any(kw in model_lower for kw in ['skip', 'sign in', 'pickup', 'delivery', 'close']):
                        model = extracted
                        # Remove parentheses if model is entirely wrapped: "(SC-1311)" -> "SC-1311"
                        if model.startswith('(') and model.endswith(')'):
                            model = model[1:-1]
                        break

            # Close dialog
            try:
                close_xpaths = [
                    "//button[@aria-label='Close dialog']",
                    "//button[contains(@aria-label, 'Close')]"
                ]
                for xpath in close_xpaths:
                    try:
                        close_btn = self.driver.find_element(By.XPATH, xpath)
                        self.driver.execute_script("arguments[0].click();", close_btn)
                        time.sleep(1)
                        break
                    except:
                        continue
            except:
                pass

            # Validate and try fallback if needed
            if self.is_invalid_sku(model):
                current_url = self.driver.current_url
                page_source = self.driver.page_source
                tree = html.fromstring(page_source)
                product_name = self.extract_text_safe(tree, self.xpaths.get('product_name'))

                # Fallback 1: URL
                url_sku = self.extract_sku_from_url(current_url)
                if url_sku:
                    return url_sku

                # Fallback 2: Product name
                name_sku = self.extract_sku_from_product_name(product_name)
                if name_sku:
                    return name_sku

                # Fallback 3: LG XPath
                if product_name and 'LG' in product_name.upper():
                    lg_sku = self.extract_sku_from_lg_xpath()
                    if lg_sku:
                        return lg_sku

                return None

            return model

        except Exception as e:
            return None

    def test_product(self, url_data):
        """Test a single product - extract name, URL, SKU"""
        try:
            mother = url_data['mother']
            order = url_data['order']
            url = url_data['url']

            self.driver.get(url)
            time.sleep(random.uniform(4, 6))

            page_source = self.driver.page_source
            tree = html.fromstring(page_source)

            # Extract product name
            product_name = self.extract_text_safe(tree, self.xpaths.get('product_name'))

            # Extract SKU
            sku = self.click_specifications_and_get_model()

            self.total_tested += 1

            # Print result
            print(f"[{self.total_tested}] [{mother.upper()}][{order}]")
            print(f"  Product: {product_name[:70] if product_name else 'N/A'}...")
            print(f"  URL:     {url}")
            print(f"  SKU:     {sku or 'N/A'}")
            print()

            return True

        except Exception as e:
            print(f"  [ERROR] Failed: {e}\n")
            return False

    def run(self):
        """Main execution"""
        try:
            print("="*100)
            print("Walmart TV SKU Extraction Test")
            print("="*100)
            print()

            if not self.connect_db():
                return

            if not self.load_xpaths():
                return

            product_urls = self.load_product_urls()
            if not product_urls:
                print("[ERROR] No URLs found")
                return

            self.setup_driver()

            print("="*100)
            print("Starting SKU extraction test...")
            print("="*100)
            print()

            for url_data in product_urls:
                self.test_product(url_data)
                time.sleep(random.uniform(2, 4))

            print("="*100)
            print(f"Test completed! Total tested: {self.total_tested} products")
            print("="*100)

        except Exception as e:
            print(f"[ERROR] Test failed: {e}")
            import traceback
            traceback.print_exc()

        finally:
            if self.driver:
                try:
                    self.driver.quit()
                except:
                    pass
            if self.db_conn:
                try:
                    self.db_conn.close()
                except:
                    pass

            print("\n[INFO] Press Enter to exit...")
            input()


if __name__ == "__main__":
    try:
        tester = WalmartSKUTester()
        tester.run()
    except Exception as e:
        print(f"\n[FATAL ERROR] {e}")
        import traceback
        traceback.print_exc()

    print("\n[INFO] Test completed.")
