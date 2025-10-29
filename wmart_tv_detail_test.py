"""
Walmart TV Detail Data Quality Test
비어있는 필드가 하나라도 있는 제품 URL을 크롤링하여 실제 데이터 존재 여부 확인
"""
import time
import random
import psycopg2
from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeout
from lxml import html
import re

# Database configuration
DB_CONFIG = {
    'host': 'samsung-dx-crawl.csnixzmkuppn.ap-northeast-2.rds.amazonaws.com',
    'port': 5432,
    'database': 'postgres',
    'user': 'postgres',
    'password': 'admin2025!'
}

class WalmartDetailTester:
    def __init__(self):
        self.db_conn = None
        self.browser = None
        self.context = None
        self.page = None
        self.xpaths = {}
        self.tested_count = 0

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
            print(f"[OK] Loaded {len(self.xpaths)} XPath selectors")
            return True

        except Exception as e:
            print(f"[ERROR] Failed to load XPaths: {e}")
            return False

    def get_products_with_null_fields(self):
        """Get products that have at least one NULL field"""
        try:
            cursor = self.db_conn.cursor()

            # Get latest batch
            cursor.execute("""
                SELECT DISTINCT batch_id
                FROM walmart_tv_detail_crawled
                ORDER BY batch_id DESC
                LIMIT 1
            """)
            batch_result = cursor.fetchone()

            if not batch_result:
                print("[ERROR] No batch found")
                return []

            batch_id = batch_result[0]
            print(f"[INFO] Latest batch: {batch_id}")

            # Get products with at least one NULL field
            cursor.execute("""
                SELECT
                    id,
                    product_url,
                    retailer_sku_name,
                    star_rating,
                    number_of_ppl_purchased_yesterday,
                    number_of_ppl_added_to_carts,
                    sku_popularity,
                    savings,
                    count_of_star_ratings,
                    detailed_review_content,
                    sku
                FROM walmart_tv_detail_crawled
                WHERE batch_id = %s
                  AND (
                      star_rating IS NULL OR star_rating = '' OR
                      number_of_ppl_purchased_yesterday IS NULL OR number_of_ppl_purchased_yesterday = '' OR
                      number_of_ppl_added_to_carts IS NULL OR number_of_ppl_added_to_carts = '' OR
                      sku_popularity IS NULL OR sku_popularity = '' OR
                      savings IS NULL OR savings = '' OR
                      count_of_star_ratings IS NULL OR count_of_star_ratings = '' OR
                      detailed_review_content IS NULL OR detailed_review_content = '' OR
                      sku IS NULL OR sku = ''
                  )
                ORDER BY id
            """, (batch_id,))

            products = []
            for row in cursor.fetchall():
                products.append({
                    'id': row[0],
                    'url': row[1],
                    'name': row[2],
                    'db_data': {
                        'star_rating': row[3],
                        'number_of_ppl_purchased_yesterday': row[4],
                        'number_of_ppl_added_to_carts': row[5],
                        'sku_popularity': row[6],
                        'savings': row[7],
                        'count_of_star_ratings': row[8],
                        'detailed_review_content': row[9],
                        'sku': row[10]
                    }
                })

            cursor.close()
            print(f"[OK] Found {len(products)} products with NULL fields")
            return products

        except Exception as e:
            print(f"[ERROR] Failed to get products: {e}")
            import traceback
            traceback.print_exc()
            return []

    def setup_browser(self):
        """Setup Playwright browser"""
        try:
            print("[INFO] Setting up Playwright browser...")
            playwright = sync_playwright().start()

            self.browser = playwright.chromium.launch(
                headless=False,  # Show browser
                args=[
                    '--disable-blink-features=AutomationControlled',
                    '--disable-dev-shm-usage',
                    '--no-sandbox'
                ]
            )

            self.context = self.browser.new_context(
                viewport={'width': 1920, 'height': 1080},
                user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
            )

            self.page = self.context.new_page()
            self.page.set_default_timeout(30000)

            print("[OK] Browser setup complete")
            return True

        except Exception as e:
            print(f"[ERROR] Failed to setup browser: {e}")
            return False

    def extract_text_safe(self, tree, xpath):
        """Safely extract text from XPath"""
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

    def test_product_url(self, product):
        """Test a single product URL to check if NULL fields actually exist on page"""
        try:
            url = product['url']
            product_name = product['name'][:50]

            print(f"\n{'='*100}")
            print(f"[TEST #{self.tested_count + 1}] ID: {product['id']}")
            print(f"Product: {product_name}...")
            print(f"URL: {url}")

            # Navigate to page
            self.page.goto(url, wait_until='domcontentloaded', timeout=30000)
            time.sleep(random.uniform(3, 5))

            # Get page source
            page_source = self.page.content()
            tree = html.fromstring(page_source)

            # Extract all fields
            actual_data = {}
            actual_data['star_rating'] = self.extract_text_safe(tree, self.xpaths.get('star_rating', {}).get('xpath'))
            actual_data['number_of_ppl_purchased_yesterday'] = self.extract_text_safe(tree, self.xpaths.get('number_of_ppl_purchased_yesterday', {}).get('xpath'))
            actual_data['number_of_ppl_added_to_carts'] = self.extract_text_safe(tree, self.xpaths.get('number_of_ppl_added_to_carts', {}).get('xpath'))
            actual_data['sku_popularity'] = self.extract_text_safe(tree, self.xpaths.get('sku_popularity', {}).get('xpath'))
            actual_data['savings'] = self.extract_text_safe(tree, self.xpaths.get('savings', {}).get('xpath'))
            actual_data['count_of_star_ratings'] = self.extract_text_safe(tree, self.xpaths.get('count_of_star_ratings', {}).get('xpath'))
            actual_data['detailed_review_content'] = self.extract_text_safe(tree, self.xpaths.get('detailed_review', {}).get('xpath'))
            actual_data['sku'] = self.extract_text_safe(tree, self.xpaths.get('samsung_sku_name', {}).get('xpath'))

            # Compare DB data with actual data
            print(f"\n{'Field':<40} | {'DB Value':<30} | {'Actual Page Value':<30} | Status")
            print("-" * 140)

            issues_found = []
            fields_ok = []

            for field in product['db_data'].keys():
                db_value = product['db_data'][field]
                actual_value = actual_data.get(field)

                # Check if DB has NULL but page has data
                db_empty = not db_value or db_value.strip() == ''
                actual_empty = not actual_value or actual_value.strip() == ''

                if db_empty and not actual_empty:
                    # ISSUE: DB is NULL but data exists on page
                    status = "XPATH ISSUE"
                    issues_found.append(field)
                    db_str = "NULL/EMPTY"
                    actual_str = (actual_value[:27] + '...') if len(str(actual_value)) > 30 else str(actual_value)
                    print(f"{field:<40} | {db_str:<30} | {actual_str:<30} | {status}")
                elif db_empty and actual_empty:
                    # OK: Both NULL (data doesn't exist)
                    status = "OK (No Data)"
                    fields_ok.append(field)
                    print(f"{field:<40} | {'NULL/EMPTY':<30} | {'NULL/EMPTY':<30} | {status}")
                else:
                    # OK: DB has data
                    status = "OK"
                    fields_ok.append(field)
                    # Don't print OK fields to reduce clutter

            # Summary
            print(f"\n[SUMMARY]")
            print(f"  - Fields OK: {len(fields_ok)}/8")
            print(f"  - XPath Issues Found: {len(issues_found)}/8")

            if issues_found:
                print(f"  - Problematic Fields: {', '.join(issues_found)}")
            else:
                print(f"  - All NULL fields are correctly identified (no data on page)")

            self.tested_count += 1

            return {
                'id': product['id'],
                'url': url,
                'issues': issues_found,
                'ok_fields': fields_ok
            }

        except Exception as e:
            print(f"\n[ERROR] Failed to test product: {e}")
            import traceback
            traceback.print_exc()
            return None

    def run(self):
        """Main execution"""
        try:
            print("="*100)
            print("Walmart TV Detail Data Quality Test")
            print("="*100)

            # Connect to database
            if not self.connect_db():
                return

            # Load XPaths
            if not self.load_xpaths():
                return

            # Get products with NULL fields
            products = self.get_products_with_null_fields()
            if not products:
                print("[INFO] No products with NULL fields found. All data is complete!")
                return

            print(f"\n[INFO] Testing {len(products)} products...")

            # Setup browser
            if not self.setup_browser():
                return

            # Test each product
            all_results = []
            total_xpath_issues = 0

            for idx, product in enumerate(products, 1):
                print(f"\n{'#'*100}")
                print(f"Progress: {idx}/{len(products)}")
                print(f"{'#'*100}")

                result = self.test_product_url(product)
                if result:
                    all_results.append(result)
                    if result['issues']:
                        total_xpath_issues += len(result['issues'])

                # Random delay
                time.sleep(random.uniform(2, 4))

            # Final summary
            print("\n\n")
            print("="*100)
            print("FINAL SUMMARY")
            print("="*100)
            print(f"\nTotal Products Tested: {len(all_results)}")
            print(f"Total XPath Issues Found: {total_xpath_issues}")

            if total_xpath_issues > 0:
                print(f"\n[WARNING] {total_xpath_issues} XPath issues detected!")
                print("These are cases where DB has NULL but data exists on the actual page.")
                print("XPath selectors may need to be updated.")

                # Group by field
                field_issue_count = {}
                for result in all_results:
                    for field in result['issues']:
                        field_issue_count[field] = field_issue_count.get(field, 0) + 1

                print(f"\nIssue breakdown by field:")
                for field, count in sorted(field_issue_count.items(), key=lambda x: x[1], reverse=True):
                    pct = (count / len(products) * 100) if len(products) > 0 else 0
                    print(f"  - {field}: {count} products ({pct:.1f}%)")
            else:
                print("\n[OK] No XPath issues found!")
                print("All NULL values in DB are correct (data doesn't exist on page).")

            print("\n" + "="*100)

        except Exception as e:
            print(f"\n[ERROR] Test failed: {e}")
            import traceback
            traceback.print_exc()

        finally:
            print("\n[INFO] Test completed. Browser will remain open.")
            print("Press Enter to close...")
            input()

            if self.browser:
                self.browser.close()
            if self.db_conn:
                self.db_conn.close()


if __name__ == "__main__":
    try:
        tester = WalmartDetailTester()
        tester.run()
    except Exception as e:
        print(f"\n[FATAL ERROR] {e}")
        import traceback
        traceback.print_exc()

    print("\n[INFO] Press Enter to exit...")
    input()
