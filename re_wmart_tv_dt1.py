"""
wmart_tv_dt1 Recovery Script
- tv_retail_com 에서 Walmart + discount_type IS NULL 레코드 복구
- 시간 범위: 2026-04-17 12:17:50 ~ 2026-04-17 15:50:08
- 재크롤링하여 4개 필드 UPDATE: savings, original_sku_price, offer, discount_type

UPDATE 분기:
  - new_final_sku_price == existing_final_sku_price (가격 변동 없음)
      → 4개 모두 UPDATE (savings, original_sku_price, offer, discount_type)
  - new_final_sku_price != existing_final_sku_price (가격 변동 있음)
      → 2개만 UPDATE (offer, discount_type) — savings/original_sku_price는 NULL 유지

xpath는 DB (xpath_selectors, mall_name='Walmart', page_type='detail_page') 에서 로드.

사용법:
    python re_wmart_tv_dt1.py
"""
import time
import re
import random
import psycopg2
from DrissionPage import ChromiumPage, ChromiumOptions
from lxml import html

from config import DB_CONFIG


DATE_FROM = '2026-04-17 12:17:50'
DATE_TO = '2026-04-17 15:50:08'


class WmartTvRecovery:
    def __init__(self):
        self.page = None
        self.db_conn = None
        self.xpaths = {}

    # ================================================================
    # 연결 / 로드
    # ================================================================
    def connect_db(self):
        try:
            self.db_conn = psycopg2.connect(**DB_CONFIG)
            self.db_conn.autocommit = True
            print("[OK] Database connected")
            return True
        except Exception as e:
            print(f"[ERROR] DB connection failed: {e}")
            return False

    def setup_browser(self):
        try:
            co = ChromiumOptions()
            co.no_imgs(True)
            self.page = ChromiumPage(co)
            print("[OK] Browser ready (images disabled)")
            return True
        except Exception as e:
            print(f"[ERROR] Browser setup failed: {e}")
            return False

    def load_xpaths(self):
        try:
            cur = self.db_conn.cursor()
            cur.execute("""
                SELECT data_field, xpath
                FROM xpath_selectors
                WHERE mall_name = 'Walmart'
                  AND page_type = 'detail_page'
                  AND is_active = TRUE
            """)
            for row in cur.fetchall():
                self.xpaths[row[0]] = row[1]
            cur.close()
            print(f"[OK] Loaded {len(self.xpaths)} xpaths")
            return True
        except Exception as e:
            print(f"[ERROR] xpath load failed: {e}")
            return False

    # ================================================================
    # 대상 조회
    # ================================================================
    def get_null_records(self, date_from, date_to):
        """discount_type IS NULL 대상 조회"""
        query = """
        SELECT product_url, crawl_datetime, final_sku_price
        FROM tv_retail_com
        WHERE account_name = 'Walmart'
          AND crawl_datetime >= %s
          AND crawl_datetime <= %s
          AND discount_type IS NULL
        ORDER BY crawl_datetime ASC
        """
        cur = self.db_conn.cursor()
        cur.execute(query, (date_from, date_to))
        rows = cur.fetchall()
        cur.close()
        return [
            {'product_url': r[0], 'crawl_datetime': r[1], 'final_sku_price': r[2]}
            for r in rows
        ]

    # ================================================================
    # 추출 메서드 (wmart_tv_dt1.py 동일)
    # ================================================================
    def extract_text_safe(self, tree, xpath):
        if not xpath:
            return None
        try:
            elements = tree.xpath(xpath)
            if elements:
                if isinstance(elements[0], str):
                    return elements[0].strip()
                return elements[0].text_content().strip()
            return None
        except Exception:
            return None

    def get_price_container(self, tree):
        """wmart_tv_dt1.py:521-544"""
        try:
            container_xpaths = [
                self.xpaths.get('price_container_1'),
                self.xpaths.get('price_container_2'),
                self.xpaths.get('price_container_3'),
                self.xpaths.get('price_container_4'),
                self.xpaths.get('price_container_5'),
            ]
            for xpath in [x for x in container_xpaths if x]:
                containers = tree.xpath(xpath)
                if containers:
                    return containers[0]
            return None
        except Exception:
            return None

    def extract_final_price(self, tree):
        """wmart_tv_dt1.py:546-647"""
        try:
            price_container = self.get_price_container(tree)
            search_context = price_container if price_container is not None else tree

            xpaths = [
                self.xpaths.get('final_price_1'),
                self.xpaths.get('final_price_2'),
                self.xpaths.get('final_price_3'),
                self.xpaths.get('final_price_4'),
            ]
            for xpath in [x for x in xpaths if x]:
                try:
                    elements = search_context.xpath(xpath)
                    if elements:
                        text = elements[0].text_content().strip()
                        if "See price in cart" in text:
                            return "See price in cart"
                        if "Not Available" in text:
                            return "Not Available"
                        price_match = re.search(r'\$[\d,]+\.?\d*', text)
                        if price_match:
                            return price_match.group(0)
                except Exception:
                    continue

            see_price_xpaths = [
                self.xpaths.get('see_price_in_cart_1'),
                self.xpaths.get('see_price_in_cart_2'),
                self.xpaths.get('see_price_in_cart_3'),
                self.xpaths.get('see_price_in_cart_4'),
            ]
            for xpath in [x for x in see_price_xpaths if x]:
                try:
                    elements = tree.xpath(xpath)
                    if elements:
                        text = elements[0].text_content().strip()
                        if "See price in cart" in text:
                            return "See price in cart"
                except Exception:
                    continue

            starting_from_xpaths = [
                self.xpaths.get('starting_from_1'),
                self.xpaths.get('starting_from_2'),
                self.xpaths.get('starting_from_3'),
            ]
            for xpath in [x for x in starting_from_xpaths if x]:
                try:
                    elements = tree.xpath(xpath)
                    if elements:
                        text = elements[0].text_content().strip()
                        price_match = re.search(r'\$[\d,]+\.?\d*', text)
                        if price_match:
                            return price_match.group(0)
                except Exception:
                    continue

            not_available_xpaths = [
                self.xpaths.get('not_available_1'),
                self.xpaths.get('not_available_2'),
                self.xpaths.get('not_available_3'),
            ]
            for xpath in [x for x in not_available_xpaths if x]:
                try:
                    elements = tree.xpath(xpath)
                    if elements:
                        text = elements[0].text_content().strip()
                        if "Not Available" in text:
                            return "Not Available"
                except Exception:
                    continue

            return None
        except Exception:
            return None

    def extract_original_price(self, tree, savings):
        """wmart_tv_dt1.py:649-683 (savings 있을 때만)"""
        try:
            if not savings:
                return None
            xpath = self.xpaths.get('original_price')
            if not xpath:
                return None
            text = self.extract_text_safe(tree, xpath)
            if text:
                price_match = re.search(r'\$[\d,]+\.?\d*', text)
                if price_match:
                    return price_match.group(0)
            return None
        except Exception:
            return None

    def extract_offer(self, tree):
        """wmart_tv_dt1.py:1078-1101"""
        try:
            xpath = self.xpaths.get('offer')
            if not xpath:
                return None
            text = self.extract_text_safe(tree, xpath)
            if text and 'free offer' in text.lower():
                match = re.search(r'^(\d+)', text.strip())
                if match:
                    return match.group(1)
            return None
        except Exception:
            return None

    # ================================================================
    # 페이지 로드 + 5개 필드 추출
    # ================================================================
    def load_page_and_extract(self, url):
        try:
            self.page.get(url)
            try:
                product_name_xpath = self.xpaths.get('product_name')
                if product_name_xpath:
                    self.page.ele(f'xpath:{product_name_xpath}', timeout=10)
                else:
                    self.page.ele('xpath://h1', timeout=10)
            except Exception:
                pass
            time.sleep(random.uniform(2, 3))

            tree = html.fromstring(self.page.html)

            discount_type = self.extract_text_safe(tree, self.xpaths.get('discount_type'))
            savings = self.extract_text_safe(tree, self.xpaths.get('savings'))
            original_sku_price = self.extract_original_price(tree, savings)
            offer = self.extract_offer(tree)
            final_sku_price = self.extract_final_price(tree)

            # 빈 문자열 → None 정규화 (DB 저장 일관성)
            if savings == '':
                savings = None
            if discount_type == '':
                discount_type = None

            return {
                'discount_type': discount_type,
                'savings': savings,
                'original_sku_price': original_sku_price,
                'offer': offer,
                'final_sku_price': final_sku_price,
            }
        except Exception as e:
            print(f"  [ERROR] extract failed: {e}")
            return None

    # ================================================================
    # UPDATE (분기 로직)
    # ================================================================
    def update_db(self, product_url, crawl_datetime, new_data, existing_final):
        new_final = new_data['final_sku_price']
        same_price = (new_final == existing_final)

        cur = self.db_conn.cursor()
        try:
            if same_price:
                cur.execute("""
                    UPDATE tv_retail_com
                    SET savings = %s,
                        original_sku_price = %s,
                        offer = %s,
                        discount_type = %s
                    WHERE product_url = %s
                      AND crawl_datetime = %s
                      AND account_name = 'Walmart'
                """, (
                    new_data['savings'],
                    new_data['original_sku_price'],
                    new_data['offer'],
                    new_data['discount_type'],
                    product_url,
                    crawl_datetime,
                ))
            else:
                cur.execute("""
                    UPDATE tv_retail_com
                    SET offer = %s,
                        discount_type = %s
                    WHERE product_url = %s
                      AND crawl_datetime = %s
                      AND account_name = 'Walmart'
                """, (
                    new_data['offer'],
                    new_data['discount_type'],
                    product_url,
                    crawl_datetime,
                ))
            rowcount = cur.rowcount
            cur.close()
            return rowcount, same_price
        except Exception as e:
            print(f"  [ERROR] UPDATE failed: {e}")
            try:
                cur.close()
            except Exception:
                pass
            return 0, same_price

    # ================================================================
    # MAIN
    # ================================================================
    def run(self):
        print("=" * 80)
        print("  re_wmart_tv_dt1 Recovery")
        print(f"  범위: {DATE_FROM} ~ {DATE_TO}")
        print("  대상: account_name='Walmart' AND discount_type IS NULL")
        print("=" * 80)

        if not self.connect_db():
            return
        if not self.load_xpaths():
            return

        records = self.get_null_records(DATE_FROM, DATE_TO)
        total = len(records)
        print(f"\n[INFO] Target: {total} records")

        if total == 0:
            print("[INFO] 대상 없음. 종료.")
            return

        confirm = input(f"\n{total}건 복구 진행하시겠습니까? (y/N): ").strip().lower()
        if confirm != 'y':
            print("[INFO] 중단.")
            return

        if not self.setup_browser():
            return

        # 세션 웜업
        print("\n[INFO] Walmart 홈페이지 웜업...")
        self.page.get("https://www.walmart.com")
        time.sleep(random.uniform(5, 8))

        success_same = 0
        success_diff = 0
        failed = 0

        for idx, rec in enumerate(records, 1):
            url = rec['product_url']
            ct = rec['crawl_datetime']
            ef = rec['final_sku_price']

            print(f"\n[{idx}/{total}] {url}")
            print(f"  Existing final_sku_price: {ef!r}")

            new_data = self.load_page_and_extract(url)
            if new_data is None:
                failed += 1
                time.sleep(random.uniform(3, 5))
                continue

            print(f"  New: final={new_data['final_sku_price']!r} | savings={new_data['savings']!r} | original={new_data['original_sku_price']!r} | offer={new_data['offer']!r} | discount_type={new_data['discount_type']!r}")

            rowcount, same_price = self.update_db(url, ct, new_data, ef)
            if rowcount > 0:
                if same_price:
                    success_same += 1
                    print(f"  [OK] UPDATED 4 fields (price same)")
                else:
                    success_diff += 1
                    print(f"  [OK] UPDATED 2 fields (price changed: {ef!r} -> {new_data['final_sku_price']!r}, savings/original_sku_price NULL 유지)")
            else:
                failed += 1
                print(f"  [FAIL] UPDATE 0 rows")

            time.sleep(random.uniform(3, 5))

        print("\n" + "=" * 80)
        print(f"  결과: 총 {total}건")
        print(f"    - 성공 (가격 동일, 4필드): {success_same}건")
        print(f"    - 성공 (가격 변동, 2필드): {success_diff}건")
        print(f"    - 실패: {failed}건")
        print("=" * 80)

        try:
            self.page.quit()
        except Exception:
            pass


if __name__ == "__main__":
    recovery = WmartTvRecovery()
    recovery.run()
