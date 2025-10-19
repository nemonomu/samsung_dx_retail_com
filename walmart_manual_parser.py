"""
Walmart TV Manual HTML Parser
사람이 직접 저장한 HTML 파일들을 읽어서 데이터를 추출합니다.

사용 방법:
1. 브라우저로 https://www.walmart.com/search?q=TV 접속
2. 페이지 소스 저장 (Ctrl+S) -> walmart_page_1.html
3. 다음 페이지들도 동일하게 저장 (page_2.html, page_3.html...)
4. 이 스크립트 실행: python walmart_manual_parser.py
"""

import psycopg2
from lxml import html
import re
import os
import glob

# Database configuration
DB_CONFIG = {
    'host': 'samsung-dx-crawl.csnixzmkuppn.ap-northeast-2.rds.amazonaws.com',
    'port': 5432,
    'database': 'postgres',
    'user': 'postgres',
    'password': 'admin2025!'
}

class WalmartManualParser:
    def __init__(self):
        self.db_conn = None
        self.xpaths = {}
        self.total_collected = 0
        self.sequential_id = 1

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
                SELECT data_field, xpath
                FROM xpath_selectors
                WHERE mall_name = 'Walmart' AND page_type = 'main' AND is_active = TRUE
            """)

            for row in cursor.fetchall():
                self.xpaths[row[0]] = row[1]

            cursor.close()
            print(f"[OK] Loaded {len(self.xpaths)} XPath selectors")
            return True

        except Exception as e:
            print(f"[ERROR] Failed to load XPaths: {e}")
            return False

    def extract_text_safe(self, element, xpath):
        """Safely extract text from element using xpath"""
        try:
            result = element.xpath(xpath)
            if result:
                if isinstance(result[0], str):
                    return result[0].strip()
                else:
                    return result[0].text_content().strip()
            return None
        except Exception as e:
            return None

    def clean_price_text(self, price_text):
        """Extract clean price from complex price HTML text"""
        if not price_text:
            return None

        price_text = ' '.join(price_text.split())
        match = re.search(r'\$\s*(\d[\d,]*)\s*(\d{2})', price_text)
        if match:
            dollars = match.group(1).replace(',', '')
            cents = match.group(2)
            return f"${dollars}.{cents}"

        return price_text

    def parse_html_file(self, file_path, page_number):
        """Parse a single HTML file"""
        try:
            print(f"\n[PAGE {page_number}] Parsing: {file_path}")

            with open(file_path, 'r', encoding='utf-8') as f:
                page_source = f.read()

            tree = html.fromstring(page_source)

            # Find all product containers
            base_xpath = self.xpaths['base_container']
            products = tree.xpath(base_xpath)

            print(f"[INFO] Found {len(products)} product containers")

            collected_count = 0
            for idx, product in enumerate(products, 1):
                # Extract product name
                product_name = self.extract_text_safe(product, self.xpaths['product_name'])

                if not product_name:
                    print(f"  [{idx}/{len(products)}] SKIP: No product name found")
                    continue

                # Extract product URL
                product_url_raw = self.extract_text_safe(product, self.xpaths['product_url'])
                product_url = product_url_raw if product_url_raw else None

                # Extract Final_SKU_Price
                final_price_raw = self.extract_text_safe(product, self.xpaths['final_price'])
                final_price = self.clean_price_text(final_price_raw) if final_price_raw else None

                # Extract Original_SKU_Price
                original_price_raw = self.extract_text_safe(product, self.xpaths['original_price'])
                original_price = original_price_raw if original_price_raw else None

                # Extract Offer
                offer = self.extract_text_safe(product, self.xpaths['offer'])

                # Extract Pick-Up_Availability
                pickup_raw = self.extract_text_safe(product, self.xpaths['pickup_availability'])
                pickup = pickup_raw if pickup_raw else None

                # Extract Shipping_Availability
                shipping_raw = self.extract_text_safe(product, self.xpaths['shipping_availability'])
                shipping = shipping_raw if shipping_raw else None

                # Extract Delivery_Availability
                delivery_raw = self.extract_text_safe(product, self.xpaths['delivery_availability'])
                delivery = delivery_raw if delivery_raw else None

                # Extract SKU_Status
                rollback = self.extract_text_safe(product, self.xpaths['sku_status_rollback'])
                sponsored = self.extract_text_safe(product, self.xpaths['sku_status_sponsored'])

                sku_status = None
                if rollback:
                    sku_status = "Rollback"
                elif sponsored:
                    sku_status = "Sponsored"

                # Extract Retailer_Membership_Discounts
                membership_discount_elem = self.extract_text_safe(product, self.xpaths['membership_discount'])
                membership_discount = "Walmart Plus" if membership_discount_elem else None

                # Extract Available_Quantity_for_Purchase
                available_quantity = self.extract_text_safe(product, self.xpaths['available_quantity'])

                # Extract Inventory_Status
                inventory_status = self.extract_text_safe(product, self.xpaths['inventory_status'])

                data = {
                    'page_type': 'main',
                    'Retailer_SKU_Name': product_name,
                    'Final_SKU_Price': final_price,
                    'Original_SKU_Price': original_price,
                    'Offer': offer,
                    'Pick_Up_Availability': pickup,
                    'Shipping_Availability': shipping,
                    'Delivery_Availability': delivery,
                    'SKU_Status': sku_status,
                    'Retailer_Membership_Discounts': membership_discount,
                    'Available_Quantity_for_Purchase': available_quantity,
                    'Inventory_Status': inventory_status,
                    'Rank': None,
                    'Product_url': product_url
                }

                if self.save_to_db(data):
                    collected_count += 1
                    self.total_collected += 1
                    print(f"  [{idx}/{len(products)}] Collected: {data['Retailer_SKU_Name'][:50]}... | Price: {final_price or 'N/A'}")

            print(f"[PAGE {page_number}] Collected {collected_count} products (Total: {self.total_collected})")
            return True

        except Exception as e:
            print(f"[ERROR] Failed to parse file {file_path}: {e}")
            import traceback
            traceback.print_exc()
            return False

    def save_to_db(self, data):
        """Save collected data with collection order"""
        try:
            cursor = self.db_conn.cursor()

            collection_order = self.sequential_id

            cursor.execute("""
                INSERT INTO wmart_tv_main_crawl
                ("order", page_type, Retailer_SKU_Name, Final_SKU_Price, Original_SKU_Price,
                 Offer, Pick_Up_Availability, Shipping_Availability, Delivery_Availability,
                 SKU_Status, Retailer_Membership_Discounts, Available_Quantity_for_Purchase,
                 Inventory_Status, Rank, Product_url)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                RETURNING id
            """, (
                collection_order,
                data['page_type'],
                data['Retailer_SKU_Name'],
                data['Final_SKU_Price'],
                data['Original_SKU_Price'],
                data['Offer'],
                data['Pick_Up_Availability'],
                data['Shipping_Availability'],
                data['Delivery_Availability'],
                data['SKU_Status'],
                data['Retailer_Membership_Discounts'],
                data['Available_Quantity_for_Purchase'],
                data['Inventory_Status'],
                data['Rank'],
                data['Product_url']
            ))

            result = cursor.fetchone()

            if result:
                self.sequential_id += 1

            self.db_conn.commit()
            cursor.close()

            return result is not None

        except Exception as e:
            print(f"[ERROR] Failed to save to DB: {e}")
            return False

    def run(self):
        """Main execution"""
        try:
            print("="*80)
            print("Walmart TV Manual HTML Parser - Starting")
            print("="*80)

            if not self.connect_db():
                return

            if not self.load_xpaths():
                return

            # Find all HTML files matching pattern walmart_page_*.html
            html_files = sorted(glob.glob("walmart_page_*.html"))

            if not html_files:
                print("[ERROR] No HTML files found!")
                print("[INFO] Please save HTML files as: walmart_page_1.html, walmart_page_2.html, etc.")
                return

            print(f"[OK] Found {len(html_files)} HTML files to parse")

            for idx, file_path in enumerate(html_files, 1):
                self.parse_html_file(file_path, idx)

                if self.total_collected >= 300:
                    print(f"[INFO] Reached 300 products limit")
                    break

            print("\n" + "="*80)
            print(f"Parsing completed! Total collected: {self.total_collected} SKUs")
            print("="*80)

        except Exception as e:
            print(f"[ERROR] Parser failed: {e}")
            import traceback
            traceback.print_exc()

        finally:
            if self.db_conn:
                self.db_conn.close()


if __name__ == "__main__":
    parser = WalmartManualParser()
    parser.run()
    print("\n[INFO] Parser completed.")
