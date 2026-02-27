"""
bby_tv_dt1 Recovery Script
- tv_retail_com / bby_tv_crawl 에서 original_sku_price, savings 모두 NULL인 레코드 복구
- 해당 URL을 재크롤링하여 DB UPDATE

사용법:
    python re_bby_tv_dt1.py
"""
import time
import re
import sys
import psycopg2
from datetime import datetime
import pytz
from DrissionPage import ChromiumPage, ChromiumOptions
from lxml import html

from config import DB_CONFIG
from bby_config_loader import get_config


class BbyTvRecovery:
    def __init__(self):
        self.page = None
        self.db_conn = None
        self.korea_tz = pytz.timezone('Asia/Seoul')
        self.config = get_config()
        self.file_name = 'bby_tv_dt1'

    def connect_db(self):
        """DB 연결"""
        try:
            self.db_conn = psycopg2.connect(**DB_CONFIG)
            self.db_conn.autocommit = True
            print("[OK] Database connected")
            return True
        except Exception as e:
            print(f"[ERROR] Database connection failed: {e}")
            return False

    def setup_browser(self):
        """브라우저 설정"""
        try:
            print("[INFO] Setting up browser...")
            co = ChromiumOptions()
            co.no_imgs(True)
            self.page = ChromiumPage(co)
            print("[OK] Browser ready")
            return True
        except Exception as e:
            print(f"[ERROR] Browser setup failed: {e}")
            return False

    def get_sessions(self):
        """세션 목록 조회 (bby_tv_crawl 기준, 시간대별 그룹)"""
        query = """
        SELECT
            SUBSTRING(crawl_datetime, 1, 10) as crawl_date,
            MIN(crawl_datetime) as session_start,
            MAX(crawl_datetime) as session_end,
            COUNT(*) as total_count,
            SUM(CASE WHEN original_sku_price IS NULL AND savings IS NULL THEN 1 ELSE 0 END) as both_null_count,
            SUM(CASE WHEN original_sku_price IS NULL THEN 1 ELSE 0 END) as orig_null_count,
            SUM(CASE WHEN savings IS NULL THEN 1 ELSE 0 END) as savings_null_count
        FROM bby_tv_crawl
        WHERE account_name = 'Bestbuy'
        GROUP BY SUBSTRING(crawl_datetime, 1, 10), SUBSTRING(crawl_datetime, 12, 2)
        ORDER BY crawl_date DESC, session_start DESC
        LIMIT 20
        """
        try:
            cursor = self.db_conn.cursor()
            cursor.execute(query)
            rows = cursor.fetchall()
            cursor.close()
            return rows
        except Exception as e:
            print(f"[ERROR] Session query failed: {e}")
            return None

    def get_null_records(self, session_start, session_end):
        """original_sku_price AND savings 모두 NULL인 레코드 조회 (bby_tv_crawl)"""
        query = """
        SELECT product_url, crawl_datetime, retailer_sku_name, item, final_sku_price
        FROM bby_tv_crawl
        WHERE account_name = 'Bestbuy'
          AND crawl_datetime >= %s
          AND crawl_datetime <= %s
          AND original_sku_price IS NULL
          AND savings IS NULL
          AND final_sku_price IS NOT NULL
          AND final_sku_price != 'no longer available'
        ORDER BY crawl_datetime ASC
        """
        try:
            cursor = self.db_conn.cursor()
            cursor.execute(query, (session_start, session_end))
            rows = cursor.fetchall()
            columns = [desc[0] for desc in cursor.description]
            cursor.close()
            return [dict(zip(columns, row)) for row in rows]
        except Exception as e:
            print(f"[ERROR] NULL records query failed: {e}")
            return None

    def load_page_and_extract(self, url):
        """URL 접속 후 가격 정보 재추출"""
        try:
            self.page.get(url)

            # h1 로딩 대기
            h1_elem = self.page.ele(
                'xpath://h1[contains(@class, "h4") or contains(@class, "heading")]',
                timeout=20
            )
            if not h1_elem:
                print(f"    [ERROR] page load failed - h1 not found")
                return None

            # 가격 컨테이너 로딩 대기
            try:
                self.page.ele(
                    'xpath://div[@data-testid="price-block-customer-price"] | //div[@data-testid="price-restricted-price-tap-for-price"] | //div[contains(text(), "no longer available in new condition")]',
                    timeout=10
                )
                # 로딩 후 tree 갱신
            except Exception:
                pass

            page_source = self.page.html
            tree = html.fromstring(page_source)

            # extract
            final_sku_price = self.extract_final_sku_price(tree)
            savings = self.extract_savings(tree)
            original_sku_price = self.extract_original_sku_price(tree, savings, final_sku_price)

            return {
                'final_sku_price': final_sku_price,
                'savings': savings,
                'original_sku_price': original_sku_price
            }
        except Exception as e:
            print(f"    [ERROR] Page load/extract failed: {e}")
            return None

    # ====================================================================
    # extraction methods (bby_tv_dt1.py와 동일)
    # ====================================================================

    def extract_final_sku_price(self, tree):
        """Final SKU Price extraction"""
        try:
            no_longer_available_xpaths = self.config.get_xpath_list('no_longer_available', self.file_name) or [
                '//div[@class="text-danger text-4 font-500 leading-4"]',
                '//div[contains(@class, "text-danger")][contains(text(), "no longer available")]',
                '//div[contains(text(), "This item is no longer available in new condition")]'
            ]
            for xpath in no_longer_available_xpaths:
                elem = tree.xpath(xpath)
                if elem:
                    text = elem[0].text_content().strip()
                    if "no longer available" in text.lower():
                        return "no longer available"

            container_xpaths = self.config.get_xpath_list('price_block_container', self.file_name) or [
                '//div[@data-testid="price-block"]',
                '//div[contains(@class, "order-2")]'
            ]
            price_container = None
            for xpath in container_xpaths:
                containers = tree.xpath(xpath)
                if containers:
                    price_container = containers[0]
                    break
            if price_container is None:
                return None

            price_xpaths = self.config.get_xpath_list('final_price_inner', self.file_name) or [
                './/div[@data-testid="price-block-customer-price"]//span',
                './/div[@data-lu-target="customer_price"]//span',
                './/span[@class="font-sans text-default text-style-body-md-400 font-500 text-7 leading-7"]'
            ]
            for xpath in price_xpaths:
                elem = price_container.xpath(xpath)
                if elem:
                    price = elem[0].text_content().strip()
                    if price and '$' in price:
                        return price

            see_price_xpaths = self.config.get_xpath_list('see_price_in_cart', self.file_name) or [
                './/div[@data-testid="price-restricted-price-tap-for-price"]//span',
                './/span[contains(text(), "See price in cart")]',
                './/span[contains(text(), "See details in checkout")]'
            ]
            for xpath in see_price_xpaths:
                elem = price_container.xpath(xpath)
                if elem:
                    text = elem[0].text_content().strip()
                    if "See price in cart" in text:
                        return "See price in cart"
                    if "See details in checkout" in text:
                        return "See details in checkout"

            return None
        except Exception as e:
            print(f"    [ERROR] final_sku_price extraction failed: {e}")
            return None

    def extract_savings(self, tree):
        """Savings extraction"""
        try:
            container_xpaths = self.config.get_xpath_list('price_block_container', self.file_name) or [
                '//div[@data-testid="price-block"]',
                '//div[contains(@class, "order-2")]'
            ]
            price_container = None
            for xpath in container_xpaths:
                containers = tree.xpath(xpath)
                if containers:
                    price_container = containers[0]
                    break
            if price_container is None:
                return None

            savings_xpaths = self.config.get_xpath_list('savings_inner', self.file_name) or [
                './/span[@data-testid="price-block-total-savings-text"]',
                './/div[@data-testid="price-block-total-savings"]//span',
                './/span[contains(@style, "color: rgb(232, 30, 37)") and contains(., "Save")]'
            ]
            for xpath in savings_xpaths:
                elem = price_container.xpath(xpath)
                if elem:
                    text = elem[0].text_content().strip()
                    match = re.search(r'\$[\d,]+(?:\.\d{2})?', text)
                    if match:
                        return match.group()
            return None
        except Exception as e:
            print(f"    [ERROR] savings extraction failed: {e}")
            return None

    def extract_original_sku_price(self, tree, savings=None, final_sku_price=None):
        """Original SKU Price extraction"""
        try:
            container_xpaths = self.config.get_xpath_list('price_block_container', self.file_name) or [
                '//div[@data-testid="price-block"]',
                '//div[contains(@class, "order-2")]'
            ]
            price_container = None
            for xpath in container_xpaths:
                containers = tree.xpath(xpath)
                if containers:
                    price_container = containers[0]
                    break
            if price_container is None:
                return None

            price_xpaths = self.config.get_xpath_list('original_price_inner', self.file_name) or [
                './/span[@data-lu-target="comp_value"]',
                './/span[@data-testid="price-block-regular-price-message-text"]//span[@data-lu-target="comp_value"]',
                './/span[contains(@style, "color: rgb(108, 111, 117)") and contains(., "$")]'
            ]
            for xpath in price_xpaths:
                elem = price_container.xpath(xpath)
                if elem:
                    price = elem[0].text_content().strip()
                    if price and '$' in price:
                        return price

            if savings and final_sku_price:
                buy_new_xpaths = self.config.get_xpath_list('buy_new_price', self.file_name) or [
                    '//a[@data-testid="price-block-regular-price-message-link"]//span',
                    '//div[@data-testid="price-block-regular-price-link-text-wrapper"]//a//span'
                ]
                for xpath in buy_new_xpaths:
                    elem = tree.xpath(xpath)
                    if elem:
                        price = elem[0].text_content().strip()
                        if price and '$' in price:
                            return price

            return None
        except Exception as e:
            print(f"    [ERROR] original_sku_price extraction failed: {e}")
            return None

    def update_db(self, product_url, original_crawl_datetime, new_data):
        """bby_tv_crawl + tv_retail_com 양쪽 UPDATE"""
        try:
            cursor = self.db_conn.cursor()
            updated = 0

            # 1) bby_tv_crawl UPDATE
            cursor.execute("""
                UPDATE bby_tv_crawl
                SET final_sku_price = %s,
                    savings = %s,
                    original_sku_price = %s
                WHERE product_url = %s
                  AND crawl_datetime = %s
            """, (
                new_data['final_sku_price'],
                new_data['savings'],
                new_data['original_sku_price'],
                product_url,
                original_crawl_datetime
            ))
            updated += cursor.rowcount

            # 2) tv_retail_com UPDATE
            cursor.execute("""
                UPDATE tv_retail_com
                SET final_sku_price = %s,
                    savings = %s,
                    original_sku_price = %s
                WHERE product_url = %s
                  AND crawl_datetime = %s
                  AND account_name = 'Bestbuy'
            """, (
                new_data['final_sku_price'],
                new_data['savings'],
                new_data['original_sku_price'],
                product_url,
                original_crawl_datetime
            ))
            updated += cursor.rowcount

            cursor.close()
            return updated
        except Exception as e:
            print(f"    [ERROR] DB update failed: {e}")
            return 0

    def run(self):
        """메인 실행"""
        print("=" * 80)
        print("  bby_tv_dt1 Recovery Script")
        print("  - original_sku_price, savings NULL 복구")
        print("=" * 80)

        # DB 연결
        if not self.connect_db():
            return

        while True:
            # 1. 세션 선택
            sessions = self.get_sessions()
            if not sessions:
                print("[ERROR] No sessions found")
                return

            print(f"\n{'='*90}")
            print(f"{'No':>3} | {'Session Start':<22} | {'Total':>6} | {'Both NULL':>10} | {'Orig NULL':>10} | {'Save NULL':>10}")
            print("-" * 90)

            for i, row in enumerate(sessions):
                crawl_date, session_start, session_end, total, both_null, orig_null, save_null = row
                print(f"{i+1:>3} | {str(session_start):<22} | {total:>6} | {both_null:>10} | {orig_null:>10} | {save_null:>10}")

            print("\n0. Exit")
            choice = input("\nSelect session number: ").strip()
            if choice == '0':
                print("Exit.")
                return

            try:
                idx = int(choice) - 1
                if idx < 0 or idx >= len(sessions):
                    print("Invalid number.")
                    continue
            except ValueError:
                print("Enter a number.")
                continue

            selected = sessions[idx]
            session_start = selected[1]
            session_end = selected[2]

            # 2. NULL 레코드 조회
            null_records = self.get_null_records(session_start, session_end)
            if not null_records:
                print("[INFO] No recovery targets found (both NULL records = 0)")
                continue

            print(f"\n[INFO] Recovery targets: {len(null_records)} records")
            print(f"[INFO] Session: {session_start} ~ {session_end}")

            # 미리보기
            for i, rec in enumerate(null_records[:5]):
                print(f"  [{i+1}] {rec['product_url'][:70]}... | price: {rec['final_sku_price']}")
            if len(null_records) > 5:
                print(f"  ... and {len(null_records) - 5} more")

            confirm = input(f"\nProceed with recovery? (y/n): ").strip().lower()
            if confirm != 'y':
                print("Cancelled.")
                continue

            # 3. 브라우저 시작
            if not self.setup_browser():
                return

            # 4. 재크롤링 + DB UPDATE
            success_count = 0
            fail_count = 0
            skip_count = 0

            try:
                for i, rec in enumerate(null_records):
                    url = rec['product_url']
                    original_dt = rec['crawl_datetime']
                    print(f"\n[{i+1}/{len(null_records)}] {url[:70]}...")

                    result = self.load_page_and_extract(url)

                    if result is None:
                        print(f"    [FAIL] extraction failed")
                        fail_count += 1
                        time.sleep(2)
                        continue

                    # savings 또는 original_sku_price 중 하나라도 있으면 성공
                    if result['savings'] is None and result['original_sku_price'] is None:
                        print(f"    [SKIP] still NULL (no sale?) - price: {result['final_sku_price']}")
                        skip_count += 1
                        time.sleep(2)
                        continue

                    print(f"    [OK] final: {result['final_sku_price']} | savings: {result['savings']} | original: {result['original_sku_price']}")

                    # DB UPDATE
                    updated = self.update_db(url, original_dt, result)
                    if updated > 0:
                        print(f"    [DB] Updated {updated} rows")
                        success_count += 1
                    else:
                        print(f"    [DB] No rows updated")
                        fail_count += 1

                    time.sleep(2)

            except KeyboardInterrupt:
                print("\n[INFO] Interrupted by user")
            except Exception as e:
                print(f"[ERROR] Recovery failed: {e}")
                import traceback
                traceback.print_exc()
            finally:
                if self.page:
                    self.page.quit()
                    print("[INFO] Browser closed")

            # 5. 결과 요약
            print(f"\n{'='*80}")
            print(f"[Recovery Result]")
            print(f"  Total targets: {len(null_records)}")
            print(f"  Success (DB updated): {success_count}")
            print(f"  Skip (still NULL - no sale): {skip_count}")
            print(f"  Fail: {fail_count}")
            print(f"{'='*80}")

            cont = input("\nContinue with another session? (y/n): ").strip().lower()
            if cont != 'y':
                break

        # cleanup
        if self.db_conn:
            self.db_conn.close()
            print("[INFO] Database disconnected")
        print("Done.")


def main():
    recovery = BbyTvRecovery()
    recovery.run()


if __name__ == '__main__':
    main()
