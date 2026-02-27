"""
bby_tv_dt1 Recovery Script
- tv_retail_com / bby_tv_crawl 에서 original_sku_price, savings 모두 NULL인 레코드 복구
- 해당 URL을 재크롤링하여 DB UPDATE

사용법:
    python re_bby_tv_dt1.py                          # 날짜 입력 모드
    python re_bby_tv_dt1.py 2026-02-27               # 특정 날짜 전체 복구
    python re_bby_tv_dt1.py 2026-02-27 2026-02-28    # 날짜 범위 복구
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

    def get_null_records(self, date_from, date_to):
        """날짜 범위에서 original_sku_price AND savings 모두 NULL인 레코드 조회"""
        query = """
        SELECT product_url, crawl_datetime, retailer_sku_name, item, final_sku_price
        FROM bby_tv_crawl
        WHERE account_name = 'Bestbuy'
          AND crawl_datetime >= %s
          AND crawl_datetime < %s
          AND original_sku_price IS NULL
          AND savings IS NULL
          AND final_sku_price IS NOT NULL
          AND final_sku_price != 'no longer available'
        ORDER BY crawl_datetime ASC
        """
        try:
            cursor = self.db_conn.cursor()
            cursor.execute(query, (f"{date_from} 00:00:00", f"{date_to} 23:59:59"))
            rows = cursor.fetchall()
            columns = [desc[0] for desc in cursor.description]
            cursor.close()
            return [dict(zip(columns, row)) for row in rows]
        except Exception as e:
            print(f"[ERROR] NULL records query failed: {e}")
            return None

    def get_summary(self, date_from, date_to):
        """날짜 범위의 요약 정보 조회"""
        query = """
        SELECT
            COUNT(*) as total_count,
            SUM(CASE WHEN original_sku_price IS NULL AND savings IS NULL THEN 1 ELSE 0 END) as both_null_count,
            SUM(CASE WHEN original_sku_price IS NULL THEN 1 ELSE 0 END) as orig_null_count,
            SUM(CASE WHEN savings IS NULL THEN 1 ELSE 0 END) as savings_null_count
        FROM bby_tv_crawl
        WHERE account_name = 'Bestbuy'
          AND crawl_datetime >= %s
          AND crawl_datetime < %s
        """
        try:
            cursor = self.db_conn.cursor()
            cursor.execute(query, (f"{date_from} 00:00:00", f"{date_to} 23:59:59"))
            row = cursor.fetchone()
            cursor.close()
            return row
        except Exception as e:
            print(f"[ERROR] Summary query failed: {e}")
            return None

    def load_page_and_extract(self, url, debug=False):
        """URL 접속 후 가격 정보 재추출 (bby_tv_dt1.py crawl_detail_page와 동일한 로딩 로직)"""
        try:
            self.page.get(url)

            # ERR_HTTP2_PROTOCOL_ERROR 감지
            try:
                page_text = self.page.html[:2000] if self.page.html else ''
                if 'ERR_HTTP2_PROTOCOL_ERROR' in page_text or "This site can't be reached" in page_text:
                    print(f"    [BLOCKED] ERR_HTTP2_PROTOCOL_ERROR")
                    return None
            except Exception:
                pass

            # h1 로딩 대기
            h1_elem = self.page.ele(
                'xpath://h1[contains(@class, "h4") or contains(@class, "heading")]',
                timeout=20
            )
            if not h1_elem:
                print(f"    [ERROR] page load failed - h1 not found")
                return None

            # DOM 체크 - 핵심 요소 존재 확인
            page_source = self.page.html
            tree = html.fromstring(page_source)
            key_elements = {
                'price': tree.xpath('//div[@data-testid="price-block-customer-price"] | //span[contains(text(), "See price in cart")]'),
                'rating': tree.xpath('//p[contains(@class, "visually-hidden")][contains(text(), "Rating")]'),
                'savings': tree.xpath('//span[@data-testid="price-block-total-savings-text"] | //div[@data-testid="price-block-total-savings"]'),
            }
            elements_found = sum(1 for v in key_elements.values() if v)

            # 핵심 요소가 부족하면 스크롤 수행 (lazy loading 대응)
            if elements_found < 2:
                for pos in [1000, 3000, 5000, 8000]:
                    self.page.run_js(f"window.scrollTo(0, {pos})")
                    time.sleep(0.3)
                self.page.run_js("window.scrollTo(0, 0)")
                time.sleep(0.5)

            # 가격 컨테이너 로딩 대기
            try:
                self.page.ele(
                    'xpath://div[@data-testid="price-block-customer-price"] | //div[@data-testid="price-restricted-price-tap-for-price"] | //div[contains(text(), "no longer available in new condition")]',
                    timeout=10
                )
            except Exception:
                pass

            # 로딩 완료 후 최종 HTML 가져오기
            page_source = self.page.html
            tree = html.fromstring(page_source)

            # debug: 첫 번째 URL에서 컨테이너 내부 확인
            if debug:
                from lxml import etree
                container_xpaths = self.config.get_xpath_list('price_block_container', self.file_name) or [
                    '//div[@data-testid="price-block"]',
                    '//div[contains(@class, "order-2")]'
                ]
                for xpath in container_xpaths:
                    containers = tree.xpath(xpath)
                    if containers:
                        c = containers[0]
                        inner = etree.tostring(c, encoding='unicode', method='html')
                        print(f"    [DEBUG] container({xpath}) inner HTML (1500chars):")
                        print(f"    {inner[:1500]}")
                        print(f"    [DEBUG] data-testid list:")
                        for el in c.xpath('.//*[@data-testid]'):
                            t = el.text_content().strip()[:40]
                            print(f"      <{el.tag} data-testid=\"{el.get('data-testid')}\"> text=\"{t}\"")
                        break

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
        try:
            container_xpaths = self.config.get_xpath_list('price_container', self.file_name) or [
                '//div[contains(@class, "order-2")]',
                '//div[@data-testid="price-block"]'
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
        try:
            container_xpaths = self.config.get_xpath_list('price_container', self.file_name) or [
                '//div[contains(@class, "order-2")]',
                '//div[@data-testid="price-block"]'
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
                './/div[@data-testid="price-block-regular-price-message-text"]//span[contains(@style, "line-through")]',
            ]
            for xpath in price_xpaths:
                elem = price_container.xpath(xpath)
                if elem:
                    text = elem[0].text_content().strip()
                    match = re.search(r'\$[\d,]+(?:\.\d{2})?', text)
                    if match:
                        return match.group()

            return None
        except Exception as e:
            print(f"    [ERROR] original_sku_price extraction failed: {e}")
            return None

    def update_db(self, product_url, original_crawl_datetime, new_data):
        """bby_tv_crawl + tv_retail_com 양쪽 UPDATE"""
        try:
            cursor = self.db_conn.cursor()
            updated = 0

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

    def run(self, date_from=None, date_to=None):
        """메인 실행"""
        print("=" * 80)
        print("  bby_tv_dt1 Recovery Script")
        print("  - original_sku_price, savings NULL 복구")
        print("=" * 80)

        # DB 연결
        if not self.connect_db():
            return

        # 날짜 결정
        if not date_from:
            date_from = input("Start date (YYYY-MM-DD): ").strip()
        if not date_to:
            date_to = date_from

        print(f"\n[INFO] Recovery range: {date_from} ~ {date_to}")

        # 요약 조회
        summary = self.get_summary(date_from, date_to)
        if not summary:
            print("[ERROR] Summary query failed")
            return

        total, both_null, orig_null, save_null = summary
        print(f"[INFO] Total records: {total}")
        print(f"[INFO] Both NULL (recovery target): {both_null}")
        print(f"[INFO] Original NULL: {orig_null} | Savings NULL: {save_null}")

        if both_null == 0:
            print("[INFO] No recovery targets. Done.")
            return

        # NULL 레코드 조회
        null_records = self.get_null_records(date_from, date_to)
        if not null_records:
            print("[INFO] No recovery targets found.")
            return

        print(f"\n[INFO] Recovery targets: {len(null_records)} records")
        for i, rec in enumerate(null_records[:5]):
            print(f"  [{i+1}] {rec['product_url'][:70]}... | price: {rec['final_sku_price']}")
        if len(null_records) > 5:
            print(f"  ... and {len(null_records) - 5} more")

        confirm = input(f"\nProceed? (y/n): ").strip().lower()
        if confirm != 'y':
            print("Cancelled.")
            return

        # 브라우저 시작
        if not self.setup_browser():
            return

        # 재크롤링 + DB UPDATE
        success_count = 0
        fail_count = 0
        skip_count = 0

        try:
            for i, rec in enumerate(null_records):
                url = rec['product_url']
                original_dt = rec['crawl_datetime']
                print(f"\n[{i+1}/{len(null_records)}] {url[:70]}...")

                # 첫 번째 URL은 debug 모드로 실행 (컨테이너 내부 확인)
                result = self.load_page_and_extract(url, debug=(i == 0))

                if result is None:
                    print(f"    [FAIL] extraction failed")
                    fail_count += 1
                    time.sleep(2)
                    continue

                if result['savings'] is None and result['original_sku_price'] is None:
                    print(f"    [SKIP] still NULL (no sale) - price: {result['final_sku_price']}")
                    skip_count += 1
                    time.sleep(2)
                    continue

                print(f"    [OK] final: {result['final_sku_price']} | savings: {result['savings']} | original: {result['original_sku_price']}")

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

        # 결과 요약
        print(f"\n{'='*80}")
        print(f"[Recovery Result]")
        print(f"  Range: {date_from} ~ {date_to}")
        print(f"  Total targets: {len(null_records)}")
        print(f"  Success (DB updated): {success_count}")
        print(f"  Skip (no sale - NULL is correct): {skip_count}")
        print(f"  Fail: {fail_count}")
        print(f"{'='*80}")

        # cleanup
        if self.db_conn:
            self.db_conn.close()
            print("[INFO] Database disconnected")
        print("Done.")


def main():
    recovery = BbyTvRecovery()

    if len(sys.argv) >= 3:
        recovery.run(date_from=sys.argv[1], date_to=sys.argv[2])
    elif len(sys.argv) == 2:
        recovery.run(date_from=sys.argv[1], date_to=sys.argv[1])
    else:
        recovery.run()


if __name__ == '__main__':
    main()
