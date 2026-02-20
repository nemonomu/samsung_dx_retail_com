"""
Best Buy TV Detail Page Crawler (Modified v1)
collected table: bby_tv_main1, bby_tv_bsr1, bby_tv_pmt1, bby_tv_Trend_crawl
save table: bby_tv_crawl, tv_retail_com

수정사항:
1. estimated_annual_electricity_use: 숫자만 extraction (예: "286 kilowatt hours" -> "286")
2. screen_size 컬럼 추가
3. samsung_sku_name -> item으로 변경, item -> retailer_sku_name으로 변경
4. 소스 table에서 13items 컬럼 추가 collected:
   - 9items data 컬럼: final_sku_price, savings, original_sku_price, offer,
     pick_up_availability, shipping_availability, delivery_availability, sku_status, star_rating
   - 4items rank/type 컬럼: promotion_type, promotion_rank, bsr_rank, main_rank
   - first 번째 found된 URL의 data 우선 (중복 URL은 first 소스 data 사용)
   - 소스 table에 없는 컬럼은 NULL 처리

v1 추가 수정사항 (2025-11-15):
5. page 로딩 불확실성 해결:
   - page_load_strategy를 'none'에서 'eager'로 변경
   - page load 후 핵심 element(제품명) wait 로직 추가
6. dialog timeout 처리 items선:
   - click_specifications_with_retry() 메서드 추가 (retry 1회)
   - timeout 발생 시 2배 증가 (15sec -> 30sec)
   - failed 원인 추적 가능
7. 가격 정보 직접 collected으로 변경 (컨테이너 기반):
   - final_sku_price, original_sku_price, savings를 소스 table에서 가져오지 않고 detail page에서 직접 crawling
   - extract_final_sku_price(), extract_original_sku_price(), extract_savings() 메서드 추가
   - 2단계 extraction: 1) 가격 컨테이너 찾기 (/html/body/div[5]/div[4]/div[1]) 2) 컨테이너 내부에서만 가격 extraction
   - data-testid 기반 XPath 사용 (price-block-customer-price, price-block-total-savings-text 등)
   - 다른 element와의 혼동 방지 (컨테이너 내부만 검색)
   - savings는 "Save $1,200" → "$1,200" 형식으로 정규식 파싱 (콤마 처리 포함)
8. star_rating 및 count_of_reviews 직접 collected으로 변경 (컨테이너 기반):
   - star_rating: 소스 table → 메인 page에서 직접 crawling (예: "4.7")
   - count_of_reviews: review page → 메인 page에서 직접 crawling (예: "(79 reviews)" → "79")
   - extract_star_rating(), extract_count_of_reviews_from_detail() 메서드 추가
   - 동일한 가격 컨테이너 사용 (/html/body/div[5]/div[4]/div[1])
   - 콤마 처리 포함 (예: "(1,234 reviews)" → "1234")
"""
import time
import random
import re
import os
import sys
import psycopg2
from datetime import datetime, timedelta
import pytz
from DrissionPage import ChromiumPage, ChromiumOptions
from lxml import html
from data_validator import DataValidator
from alert_monitor import send_review_url_error_alert

# Import database configuration
from config import DB_CONFIG
from bby_config_loader import get_config
import pandas as pd
from alert_monitor import monitor_and_alert


class Tee:
    """stdout을 콘솔과 파일 모두에 출력하는 클래스"""
    def __init__(self, file_path):
        self.file = open(file_path, 'w', encoding='utf-8')
        self.stdout = sys.stdout
        sys.stdout = self

    def write(self, data):
        self.stdout.write(data)
        self.file.write(data)
        self.file.flush()

    def flush(self):
        self.stdout.flush()
        self.file.flush()

    def close(self):
        sys.stdout = self.stdout
        self.file.close()


class BestBuyDetailCrawler:
    def __init__(self):
        self.page = None
        self.db_conn = None
        self.korea_tz = pytz.timezone('Asia/Seoul')
        self.batch_id = datetime.now(self.korea_tz).strftime('%Y%m%d_%H%M%S')
        self.order = 0
        self.total_collected = 0

        # Config loader 초기화
        self.config = get_config()
        self.file_name = 'bby_tv_dt1'

        self.max_skus = self.config.get_int('constant', 'max_products_detail', self.file_name, 300)

        # NULL detailed_review_content 로그 저장용
        self.null_review_logs = []

        # Mismatch tracking for tv_item_mst
        self.screen_size_mismatch_records = []
        self.electricity_use_mismatch_records = []

        # Data validator 초기화
        session_start_time = os.environ.get('SESSION_START_TIME', datetime.now().strftime('%Y%m%d%H%M'))
        self.validator = DataValidator(session_start_time)

        # 콘솔 로그 저장용
        self.tee = None

    def connect_db(self):
        """DB connection"""
        try:
            self.db_conn = psycopg2.connect(**DB_CONFIG)
            self.db_conn.autocommit = True
            print("[OK] Database connected")
            return True
        except Exception as e:
            print(f"[ERROR] Database connection failed: {e}")
            return False

    def get_item_mst_data(self, item):
        """Get screen_size and estimated_annual_electricity_use from tv_item_mst for given item"""
        try:
            if not self.db_conn or not item:
                return None
            cursor = self.db_conn.cursor()
            table_name = self.config.get_table('item_master') or 'tv_item_mst'
            cursor.execute(f"""
                SELECT screen_size, estimated_annual_electricity_use FROM {table_name} WHERE item = %s
            """, (item,))
            row = cursor.fetchone()
            cursor.close()
            if row:
                return {'screen_size': row[0], 'estimated_annual_electricity_use': row[1]}
            return None
        except Exception as e:
            print(f"  [WARNING] Failed to get item_mst data: {e}")
            return None

    def setup_browser(self):
        """Setup DrissionPage ChromiumPage - 이미지 비활성화로 속도 향상"""
        try:
            print("[INFO] Setting up DrissionPage browser...")
            co = ChromiumOptions()
            co.no_imgs(True)
            self.page = ChromiumPage(co)
            print("[OK] DrissionPage browser setup complete")
            return True
        except Exception as e:
            print(f"[ERROR] Browser setup failed: {e}")
            import traceback
            traceback.print_exc()
            return False

    def close_browser(self):
        """브라우저 안전 종료 + 잔여 프로세스 정리"""
        try:
            if self.page:
                self.page.quit()
                self.page = None
                print("[INFO] Browser closed")
        except Exception as e:
            print(f"[WARNING] Browser close error: {e}")
            self.page = None
        # 잔여 chrome 프로세스 강제 종료
        try:
            import subprocess
            subprocess.run(['taskkill', '/f', '/im', 'chrome.exe'],
                         capture_output=True, timeout=10)
        except Exception:
            pass

    def restart_browser(self):
        """브라우저 종료 후 재시작"""
        print("[INFO] Restarting browser...")
        self.close_browser()
        time.sleep(3)
        return self.setup_browser()

    def check_db_connection(self):
        """DB 커넥션 상태 확인 및 재연결"""
        try:
            cursor = self.db_conn.cursor()
            cursor.execute("SELECT 1")
            cursor.close()
            return True
        except Exception:
            print("[WARNING] DB connection lost, reconnecting...")
            try:
                self.db_conn = psycopg2.connect(**DB_CONFIG)
                self.db_conn.autocommit = True
                print("[OK] DB reconnected")
                return True
            except Exception as e:
                print(f"[ERROR] DB reconnection failed: {e}")
                return False

    def get_recent_urls(self):
        """최신 batch_id의 product URLs와 추가 data 가져오기"""
        try:
            # Helper function to extract item from URL for deduplication
            def _extract_item(url):
                """Extract item ID from URL for deduplication
                Example: /product/name/JJ8VPZW5KG/sku/123 -> JJ8VPZW5KG
                         /product/name/JJ8VPZW5KG -> JJ8VPZW5KG
                """
                try:
                    parts = url.rstrip('/').split('/')
                    if 'product' in parts:
                        product_index = parts.index('product')
                        if len(parts) > product_index + 2:
                            item = parts[product_index + 2]
                            if item and item.lower() != 'sku':
                                return item
                    return url  # Fallback to URL if item extraction fails
                except:
                    return url

            cursor = self.db_conn.cursor()
            urls = []

            # Config에서 테이블명 가져오기
            main_table = self.config.get_table('main_data') or 'bby_tv_main1'
            bsr_table = self.config.get_table('bsr_data') or 'bby_tv_bsr1'
            pmt_table = self.config.get_table('pmt_data') or 'bby_tv_pmt1'
            trend_table = self.config.get_table('trend_data') or 'bby_tv_Trend_crawl'

            # bestbuy_tv_main_crawl에서 최신 batch_id 가져오기
            cursor.execute(f"""
                SELECT batch_id
                FROM {main_table}
                WHERE batch_id IS NOT NULL
                ORDER BY batch_id DESC
                LIMIT 1
            """)
            main_batch_result = cursor.fetchone()
            main_batch_id = main_batch_result[0] if main_batch_result else None

            # bby_tv_Trend_crawl에서 최신 batch_id 가져오기
            cursor.execute(f"""
                SELECT batch_id
                FROM {trend_table}
                WHERE batch_id IS NOT NULL
                ORDER BY batch_id DESC
                LIMIT 1
            """)
            trend_batch_result = cursor.fetchone()
            trend_batch_id = trend_batch_result[0] if trend_batch_result else None

            # bby_tv_promotion_crawl에서 최신 batch_id 가져오기
            cursor.execute(f"""
                SELECT batch_id
                FROM {pmt_table}
                WHERE batch_id IS NOT NULL
                ORDER BY batch_id DESC
                LIMIT 1
            """)
            promo_batch_result = cursor.fetchone()
            promo_batch_id = promo_batch_result[0] if promo_batch_result else None

            # bby_tv_bsr_crawl에서 최신 batch_id 가져오기
            cursor.execute(f"""
                SELECT batch_id
                FROM {bsr_table}
                WHERE batch_id IS NOT NULL
                ORDER BY batch_id DESC
                LIMIT 1
            """)
            bsr_batch_result = cursor.fetchone()
            bsr_batch_id = bsr_batch_result[0] if bsr_batch_result else None

            print(f"[INFO] Latest batch_id - Main: {main_batch_id}, BSR: {bsr_batch_id}, Promotion: {promo_batch_id}, Trend: {trend_batch_id}")

            # collected 순서: main → bsr → promotion → trend (우선순위 순서)
            # 각 table의 rank 순서대로 정렬
            # 중복 item은 rank 정보 병합 (crawling은 한 번만)

            # Dictionary to store merged URL data: {item: {page_type, ranks, data...}}
            # Key는 item ID (URL에서 추출) - 같은 item이면 URL이 달라도 중복 처리
            url_data_map = {}

            # 1. bestbuy_tv_main_crawl에서 해당 batch의 URLs와 data 가져오기
            if main_batch_id:
                cursor.execute(f"""
                    SELECT DISTINCT product_url, offer,
                           pick_up_availability, shipping_availability, delivery_availability,
                           sku_status, main_rank, retailer_sku_name
                    FROM {main_table}
                    WHERE batch_id = %s
                    AND product_url IS NOT NULL
                    ORDER BY main_rank
                """, (main_batch_id,))
                main_urls = cursor.fetchall()
                for row in main_urls:
                    url = row[0]
                    item_key = _extract_item(url)  # item ID로 중복 체크
                    if item_key not in url_data_map:
                        url_data_map[item_key] = {
                            'page_type': 'main',
                            'product_url': url,
                            'retailer_sku_name': row[7],
                            'final_sku_price': None,
                            'savings': None,
                            'original_sku_price': None,
                            'offer': row[1],
                            'pick_up_availability': row[2],
                            'shipping_availability': row[3],
                            'delivery_availability': row[4],
                            'sku_status': row[5],
                            'star_rating': None,
                            'main_rank': row[6],
                            'bsr_rank': None,
                            'trend_rank': None,
                            'promotion_position': None,
                            'promotion_type': None
                        }
                print(f"[OK] Main URLs (batch {main_batch_id}): {len(main_urls)} items")

            # 2. bby_tv_bsr_crawl에서 해당 batch의 URLs와 data 가져오기
            if bsr_batch_id:
                cursor.execute(f"""
                    SELECT DISTINCT product_url, offer,
                           pick_up_availability, shipping_availability, delivery_availability,
                           sku_status, bsr_rank, retailer_sku_name
                    FROM {bsr_table}
                    WHERE batch_id = %s
                    AND product_url IS NOT NULL
                    ORDER BY bsr_rank
                """, (bsr_batch_id,))
                bsr_urls = cursor.fetchall()
                for row in bsr_urls:
                    url = row[0]
                    item_key = _extract_item(url)  # item ID로 중복 체크
                    if item_key in url_data_map:
                        # Item already exists - just add bsr_rank
                        url_data_map[item_key]['bsr_rank'] = row[6]
                    else:
                        # New item from bsr
                        url_data_map[item_key] = {
                            'page_type': 'bsr',
                            'product_url': url,
                            'retailer_sku_name': row[7],
                            'final_sku_price': None,
                            'savings': None,
                            'original_sku_price': None,
                            'offer': row[1],
                            'pick_up_availability': row[2],
                            'shipping_availability': row[3],
                            'delivery_availability': row[4],
                            'sku_status': row[5],
                            'star_rating': None,
                            'main_rank': None,
                            'bsr_rank': row[6],
                            'trend_rank': None,
                            'promotion_position': None,
                            'promotion_type': None
                        }
                print(f"[OK] BSR URLs (batch {bsr_batch_id}): {len(bsr_urls)} items")

            # 3. bby_tv_promotion_crawl에서 해당 batch의 URLs와 data 가져오기
            if promo_batch_id:
                cursor.execute(f"""
                    SELECT DISTINCT product_url, offer, promotion_type, promotion_rank, retailer_sku_name
                    FROM {pmt_table}
                    WHERE batch_id = %s
                    AND product_url IS NOT NULL
                    ORDER BY promotion_rank
                """, (promo_batch_id,))
                promo_urls = cursor.fetchall()
                for row in promo_urls:
                    url = row[0]
                    item_key = _extract_item(url)  # item ID로 중복 체크
                    if item_key in url_data_map:
                        # Item already exists - just add promotion_position and promotion_type
                        url_data_map[item_key]['promotion_position'] = row[3]  # promotion_rank -> promotion_position
                        url_data_map[item_key]['promotion_type'] = row[2]
                    else:
                        # New item from promotion
                        url_data_map[item_key] = {
                            'page_type': 'promotion',
                            'product_url': url,
                            'retailer_sku_name': row[4],
                            'final_sku_price': None,
                            'savings': None,
                            'original_sku_price': None,
                            'offer': row[1],
                            'pick_up_availability': None,
                            'shipping_availability': None,
                            'delivery_availability': None,
                            'sku_status': None,
                            'star_rating': None,
                            'main_rank': None,
                            'bsr_rank': None,
                            'trend_rank': None,
                            'promotion_position': row[3],  # promotion_rank -> promotion_position
                            'promotion_type': row[2]
                        }
                print(f"[OK] Promotion URLs (batch {promo_batch_id}): {len(promo_urls)} items")

            # 4. bby_tv_Trend_crawl에서 해당 batch의 URLs와 data 가져오기
            if trend_batch_id:
                cursor.execute(f"""
                    SELECT DISTINCT product_url, rank, product_name
                    FROM {trend_table}
                    WHERE batch_id = %s
                    AND product_url IS NOT NULL
                    ORDER BY rank
                """, (trend_batch_id,))
                trend_urls = cursor.fetchall()
                for row in trend_urls:
                    url = row[0]
                    item_key = _extract_item(url)  # item ID로 중복 체크
                    if item_key in url_data_map:
                        # Item already exists - just add trend_rank
                        url_data_map[item_key]['trend_rank'] = row[1]
                    else:
                        # New item from trend
                        url_data_map[item_key] = {
                            'page_type': 'Trend',
                            'product_url': url,
                            'retailer_sku_name': row[2],  # product_name -> retailer_sku_name
                            'final_sku_price': None,
                            'savings': None,
                            'original_sku_price': None,
                            'offer': None,
                            'pick_up_availability': None,
                            'shipping_availability': None,
                            'delivery_availability': None,
                            'sku_status': None,
                            'star_rating': None,
                            'main_rank': None,
                            'bsr_rank': None,
                            'trend_rank': row[1],
                            'promotion_position': None,
                            'promotion_type': None
                        }
                print(f"[OK] Trend URLs (batch {trend_batch_id}): {len(trend_urls)} items")

            cursor.close()

            # Convert dictionary to list (maintains insertion order: main, bsr, promotion, trend)
            all_urls = list(url_data_map.values())

            # Filter out Open Box products (URL에 "openbox" 포함된 제품 제외)
            before_openbox_filter = len(all_urls)
            all_urls = [u for u in all_urls if 'openbox' not in u['product_url'].lower()]
            openbox_filtered = before_openbox_filter - len(all_urls)
            if openbox_filtered > 0:
                print(f"[INFO] Filtered out {openbox_filtered} Open Box products")

            # Count duplicates from source tables
            total_loaded = 0
            if main_batch_id:
                cursor = self.db_conn.cursor()
                cursor.execute(f"SELECT COUNT(*) FROM {main_table} WHERE batch_id = %s", (main_batch_id,))
                total_loaded += cursor.fetchone()[0]
                cursor.close()
            if bsr_batch_id:
                cursor = self.db_conn.cursor()
                cursor.execute(f"SELECT COUNT(*) FROM {bsr_table} WHERE batch_id = %s", (bsr_batch_id,))
                total_loaded += cursor.fetchone()[0]
                cursor.close()
            if promo_batch_id:
                cursor = self.db_conn.cursor()
                cursor.execute(f"SELECT COUNT(*) FROM {pmt_table} WHERE batch_id = %s", (promo_batch_id,))
                total_loaded += cursor.fetchone()[0]
                cursor.close()
            if trend_batch_id:
                cursor = self.db_conn.cursor()
                cursor.execute(f"SELECT COUNT(*) FROM {trend_table} WHERE batch_id = %s", (trend_batch_id,))
                total_loaded += cursor.fetchone()[0]
                cursor.close()

            duplicates_count = total_loaded - len(all_urls)
            if duplicates_count > 0:
                print(f"[INFO] Found {duplicates_count} duplicate items - rank information merged")

            print(f"[OK] Total unique items from main/bsr/promo/trend: {len(all_urls)}")

            # Filter out already processed URLs (현재 실행 기준 - 같은 batch_id만)
            print("[INFO] Checking for already processed URLs (current run)...")
            cursor = self.db_conn.cursor()

            detail_table = self.config.get_table('detail_data') or 'bby_tv_crawl'
            print(f"[INFO] Checking batch_id = '{self.batch_id}' (current run)")

            # 현재 실행(batch_id)에서 수집된 URL 중 핵심 필드가 모두 있는 경우만 완료로 간주
            cursor.execute(f"""
                SELECT DISTINCT product_url
                FROM {detail_table}
                WHERE product_url IS NOT NULL
                  AND batch_id = %s
                  AND retailer_sku_name IS NOT NULL
                  AND final_sku_price IS NOT NULL
                  AND Detailed_Review_Content IS NOT NULL
            """, (self.batch_id,))

            already_processed_urls = {row[0] for row in cursor.fetchall()}
            print(f"[INFO] Found {len(already_processed_urls)} already processed URLs in current run")

            cursor.close()

            # Filter out already processed URLs
            new_urls = [url_data for url_data in all_urls
                        if url_data['product_url'] not in already_processed_urls]

            # Summary
            already_processed_count = len(all_urls) - len(new_urls)
            print(f"[INFO] Already processed (skipped): {already_processed_count}")
            print(f"[OK] New URLs to process: {len(new_urls)}")

            if len(new_urls) == 0:
                if len(all_urls) > 0:
                    print("[WARNING] All URLs have been processed already in current session!")
                else:
                    print("[ERROR] No URLs found!")

            return new_urls

        except Exception as e:
            print(f"[ERROR] Failed to load URLs: {e}")
            import traceback
            traceback.print_exc()
            return []

    def extract_retailer_sku_name(self, tree):
        """Retailer_SKU_Name extraction"""
        try:
            xpaths = self.config.get_xpath_list('retailer_sku_name', self.file_name) or [
                '//h1[contains(@class, "h4")]',
                '//div[@class="sku-title"]//h1'
            ]
            for xpath in xpaths:
                elem = tree.xpath(xpath)
                if elem:
                    return elem[0].text_content().strip()
            return None
        except Exception as e:
            print(f"  [ERROR] Retailer_SKU_Name extraction failed: {e}")
            return None

    def click_specifications(self):
        """Specification button click (DrissionPage)"""
        try:
            print("  [INFO] Specification button click...")
            # CSS/XPath를 사용한 여러 attempt
            selectors = self.config.get_selectors('specs_button', self.file_name) or [
                'xpath://button[contains(@class, "specs-accordion")]',
                'xpath://button[.//h3[text()="Specifications"]]',
                'css:button.specs-accordion'
            ]

            for selector in selectors:
                try:
                    spec_button = self.page.ele(selector, timeout=3)
                    if spec_button:
                        spec_button.scroll.to_see()
                        time.sleep(2)
                        spec_button.click()
                        print("  [OK] Specification click successful")
                        time.sleep(7)  # dialog 로딩 wait 증가
                        return True
                except:
                    continue

            print("  [WARNING] Specification button not found.")
            return False

        except Exception as e:
            print(f"  [ERROR] Specification click failed: {e}")
            return False

    def click_specifications_with_retry(self):
        """
        Specifications dialog 열기 (retry 포함) - DrissionPage

        Returns:
            (success, error):
                (True, None): successful
                (False, 'dialog_timeout'): timeout
                (False, 'click_failed'): click failed
        """
        max_retries = 1
        retry_count = 0
        base_timeout = 15

        while retry_count <= max_retries:
            # Specifications button click
            if self.click_specifications():
                try:
                    wait_time = base_timeout * (2 ** retry_count)  # 15s -> 30s
                    # DrissionPage wait for element
                    model_num_elem = self.page.ele('xpath://div[contains(text(), "Model Number")]', timeout=wait_time)
                    if model_num_elem:
                        print(f"  [OK] dialog load complete (wait: {wait_time}sec)")
                        return True, None
                    else:
                        raise Exception("Model Number element not found")

                except Exception as e:
                    if retry_count < max_retries:
                        print(f"  [WARNING] dialog timeout, retry {retry_count + 1}/{max_retries}...")
                        retry_count += 1
                        self.close_specifications_dialog()

                        # Page refresh to recover from stuck state
                        print(f"  [INFO] Refreshing page to recover from timeout...")
                        try:
                            self.page.refresh()
                            time.sleep(3)  # Wait for page reload
                            print(f"  [OK] Page refreshed successfully")
                        except Exception as e2:
                            print(f"  [WARNING] Page refresh failed: {e2}")

                        time.sleep(2)
                        continue
                    else:
                        print(f"  [ERROR] dialog timeout (retry failed)")
                        return False, 'dialog_timeout'
            else:
                print(f"  [WARNING] Specifications button click failed")
                return False, 'click_failed'

        return False, 'dialog_timeout'

    def extract_item_from_url(self, url):
        """Extract item from BestBuy product URL

        Examples:
        https://www.bestbuy.com/product/roku-32-class.../J3PFCJQRY8/sku/6644457 -> J3PFCJQRY8
        https://www.bestbuy.com/product/tcl-75-class.../J36QYTQ595 -> J36QYTQ595

        Pattern: /product/[product-name]/[ITEM_ID]
        """
        try:
            # Split URL by "/"
            parts = url.rstrip('/').split('/')

            # Find "product" in the URL
            if 'product' in parts:
                product_index = parts.index('product')
                # Item ID is 2 positions after "product" (product_index + 2)
                if len(parts) > product_index + 2:
                    item = parts[product_index + 2]
                    if item:
                        print(f"  [✓] Item extracted from URL: {item}")
                        return item

            print(f"  [WARNING] Could not extract item from URL: {url}")
            return None

        except Exception as e:
            print(f"  [ERROR] Item extraction from URL failed: {e}")
            import traceback
            traceback.print_exc()
            return None

    def extract_electricity_use(self, tree):
        """Estimated_Annual_Electricity_Use extraction (숫자만)"""
        try:
            # dialog에서 Estimated Annual Electricity Use 찾기 (여러 패턴 attempt)
            xpaths = self.config.get_xpath_list('electricity_use', self.file_name) or [
                '//div[contains(@class, "dB7j8sHUbncyf79K")]//div[contains(text(), "Estimated Annual Electricity Use")]/following-sibling::div[@class="grow basis-none pl-300"]',
                '//li[.//h4[text()="Power"]]//div[.//div[contains(text(), "Estimated Annual Electricity Use")]]//div[@class="grow basis-none pl-300"]',
                '//div[contains(text(), "Estimated Annual Electricity Use")]/following-sibling::div[@class="grow basis-none pl-300"]',
                '//div[contains(text(), "Estimated Annual Electricity Use")]/..//div[@class="grow basis-none pl-300"]',
                '//div[contains(., "Estimated Annual Electricity Use")]//div[contains(@class, "pl-300")]'
            ]
            for xpath in xpaths:
                elem = tree.xpath(xpath)
                if elem:
                    electricity = elem[0].text_content().strip()
                    if electricity:
                        # 숫자만 extraction (예: "286 kilowatt hours" -> "286")
                        match = re.search(r'(\d+)', electricity)
                        if match:
                            return match.group(1)
                        return electricity  # 숫자를 찾지 못하면 원본 반환
            return None
        except Exception as e:
            print(f"  [ERROR] Estimated_Annual_Electricity_Use extraction failed: {e}")
            return None

    def extract_screen_size(self, tree):
        """Screen Size extraction"""
        try:
            # XPath 패턴
            xpaths = self.config.get_xpath_list('screen_size', self.file_name) or [
                '/html/body/div[5]/div[4]/div[2]/div/div[3]/div[1]/button[2]/div/div/div[2]',
                '//div[contains(text(), "Screen Size Class")]/following-sibling::div[@class="flex font-500 items-center"]',
                '//div[text()="Screen Size Class"]/..//div[contains(@class, "flex font-500")]'
            ]

            for xpath in xpaths:
                elem = tree.xpath(xpath)
                if elem:
                    screen_size_text = elem[0].text_content().strip()
                    # "65 inches" 형태로 반환 (svg 텍스트 제거)
                    # 정규식으로 "숫자 + inches" extraction
                    match = re.search(r'(\d+\s*inches)', screen_size_text)
                    if match:
                        return match.group(1)
                    # 만약 매칭 안되면 원본 반환
                    return screen_size_text
            return None
        except Exception as e:
            print(f"  [ERROR] Screen Size extraction failed: {e}")
            return None

    def extract_model_year(self, tree):
        """Extract model year from specifications - general dialog (DB에서 xpath 로드)"""
        try:
            xpaths = self.config.get_xpath_list('model_year', self.file_name)

            if not xpaths:
                print(f"  [WARNING] No model_year xpath found in DB for {self.file_name}")
                return None

            for xpath in xpaths:
                elem = tree.xpath(xpath)
                if elem:
                    year_text = elem[0].text_content().strip()
                    # Validate it's a 4-digit year
                    if re.match(r'^\d{4}$', year_text):
                        return year_text

            return None
        except Exception as e:
            print(f"  [ERROR] Model Year extraction failed: {e}")
            return None

    def extract_sku(self, tree):
        """Extract SKU (Model Number) from Specifications dialog - General container

        HTML 구조:
        <div class="dB7j8sHUbncyf79K inline-flex w-full body-copy-lg">
          <div class="flex grow basis-none font-weight-medium gap-50 inline-align-middle items-center">Model Number</div>
          <div class="grow basis-none pl-300">32R3B5/32R3BX</div>
        </div>

        Returns:
            str: SKU (Model Number) or "no sku" if not found
        """
        try:
            # XPath 패턴 - Model Number 텍스트가 있는 div의 형제 div에서 값 추출
            xpaths = self.config.get_xpath_list('sku_model_number', self.file_name) or [
                '//div[contains(@class, "dB7j8sHUbncyf79K")][.//div[contains(text(), "Model Number")]]/div[contains(@class, "pl-300")]',
                '//div[contains(text(), "Model Number")]/following-sibling::div[contains(@class, "pl-300")]',
                '//div[contains(text(), "Model Number")]/../div[contains(@class, "pl-300")]',
                '//div[contains(text(), "Model Number")]/following-sibling::div'
            ]

            for xpath in xpaths:
                elem = tree.xpath(xpath)
                if elem:
                    sku = elem[0].text_content().strip()
                    if sku and len(sku) > 0:
                        print(f"  [✓] SKU (Model Number): {sku}")
                        return sku

            print(f"  [WARNING] SKU (Model Number) not found - using 'no sku'")
            return "no sku"

        except Exception as e:
            print(f"  [ERROR] SKU extraction failed: {e}")
            return "no sku"

    def extract_final_sku_price(self, tree):
        """Final SKU Price extraction (현재 판매 가격) - 컨테이너 기반"""
        try:
            # 0단계: "no longer available" 체크를 먼저 수행 (가격 컨테이너가 없을 수 있음)
            no_longer_available_xpaths = self.config.get_xpath_list('no_longer_available', self.file_name) or [
                '/html/body/div[5]/div[3]/div[1]/div/div[2]/div[4]',
                '//div[@class="text-danger text-4 font-500 leading-4"]',
                '//div[contains(@class, "text-danger")][contains(text(), "no longer available")]',
                '//div[contains(text(), "This item is no longer available in new condition")]'
            ]

            for xpath in no_longer_available_xpaths:
                elem = tree.xpath(xpath)
                if elem:
                    text = elem[0].text_content().strip()
                    if "no longer available" in text.lower():
                        print(f"  [INFO] Item no longer available in new condition")
                        return "no longer available"

            # 1단계: 가격 컨테이너 찾기
            container_xpaths = self.config.get_xpath_list('price_container', self.file_name) or [
                '/html/body/div[5]/div[4]/div[1]',
                '//div[@class="order-2 t3V0AOwowrTfUzPn "]',
                '//div[contains(@class, "order-2")]'
            ]

            price_container = None
            for xpath in container_xpaths:
                containers = tree.xpath(xpath)
                if containers:
                    price_container = containers[0]
                    break

            if price_container is None or len(price_container) == 0:
                print(f"  [WARNING] price container not found")
                return None

            # 2단계: 컨테이너 내부에서만 가격 extraction
            price_xpaths = self.config.get_xpath_list('final_price_inner', self.file_name) or [
                './/div[@data-testid="price-block-customer-price"]//span',
                './/div[@data-lu-target="customer_price"]//span',
                './/span[@class="font-sans text-default text-style-body-md-400 font-500 text-7 leading-7"]'
            ]

            for xpath in price_xpaths:
                elem = price_container.xpath(xpath)
                if elem:
                    price = elem[0].text_content().strip()
                    # "$" 기호가 포함되어 있고 유효한 가격인지 확인
                    if price and '$' in price:
                        return price  # "$89.99" 형식 반환

            # Fallback 1: "See price in cart" 또는 "See details in checkout" 패턴 찾기
            see_price_xpaths = self.config.get_xpath_list('see_price_in_cart', self.file_name) or [
                './/div[@data-testid="price-restricted-price-tap-for-price"]//span',
                './/span[contains(text(), "See price in cart")]',
                './/span[contains(text(), "See details in checkout")]',
                './/div[@data-testid="price-block"]//span[contains(text(), "See price in cart")]',
                './/div[@data-testid="price-block"]//span[contains(text(), "See details in checkout")]'
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
            print(f"  [ERROR] Final_SKU_Price extraction failed: {e}")
            return None

    def extract_original_sku_price(self, tree, savings=None, final_sku_price=None):
        """Original SKU Price extraction (세일 전 원가) - 컨테이너 기반

        Args:
            tree: HTML tree
            savings: savings value (for fallback condition check)
            final_sku_price: final price value (for fallback condition check)
        """
        try:
            # 1단계: 가격 컨테이너 찾기
            container_xpaths = self.config.get_xpath_list('price_container', self.file_name) or [
                '/html/body/div[5]/div[4]/div[1]',
                '//div[@class="order-2 t3V0AOwowrTfUzPn "]',
                '//div[contains(@class, "order-2")]'
            ]

            price_container = None
            for xpath in container_xpaths:
                containers = tree.xpath(xpath)
                if containers:
                    price_container = containers[0]
                    break

            if price_container is None or len(price_container) == 0:
                # 컨테이너 없으면 None 반환 (경고 none - 정상 케이스일 수 있음)
                return None

            # 2단계: 컨테이너 내부에서만 원가 extraction
            price_xpaths = self.config.get_xpath_list('original_price_inner', self.file_name) or [
                './/span[@data-lu-target="comp_value"]',
                './/span[@data-testid="price-block-regular-price-message-text"]//span[@data-lu-target="comp_value"]',
                './/span[contains(@style, "color: rgb(108, 111, 117)") and contains(., "$")]'
            ]

            for xpath in price_xpaths:
                elem = price_container.xpath(xpath)
                if elem:
                    price = elem[0].text_content().strip()
                    # "$" 기호가 포함되어 있고 유효한 가격인지 확인
                    if price and '$' in price:
                        return price  # "$149.99" 형식 반환

            # Fallback: "Buy New: $X,XXX.XX" 패턴 찾기 (전체 페이지에서)
            # 조건: savings와 final_sku_price를 모두 수집했을 때만 시도
            if savings and final_sku_price:
                buy_new_xpaths = self.config.get_xpath_list('buy_new_price', self.file_name) or [
                    '/html/body/div[4]/div[4]/div[1]/div/div[4]/div/div/div/div/div[1]/div[2]/div/div/div/div/div[2]/div[2]/div/a',
                    '//a[@data-testid="price-block-regular-price-message-link"]//span',
                    '//div[@data-testid="price-block-regular-price-link-text-wrapper"]//a//span'
                ]

                for xpath in buy_new_xpaths:
                    elem = tree.xpath(xpath)
                    if elem:
                        price = elem[0].text_content().strip()
                        if price and '$' in price:
                            print(f"  [INFO] Original price found via 'Buy New' fallback: {price}")
                            return price

            return None  # 세일이 아니면 None
        except Exception as e:
            print(f"  [ERROR] Original_SKU_Price extraction failed: {e}")
            return None

    def extract_savings(self, tree):
        """Savings extraction (할인 금액) - 컨테이너 기반"""
        try:
            # 1단계: 가격 컨테이너 찾기
            container_xpaths = self.config.get_xpath_list('price_container', self.file_name) or [
                '/html/body/div[5]/div[4]/div[1]',
                '//div[@class="order-2 t3V0AOwowrTfUzPn "]',
                '//div[contains(@class, "order-2")]'
            ]

            price_container = None
            for xpath in container_xpaths:
                containers = tree.xpath(xpath)
                if containers:
                    price_container = containers[0]
                    break

            if price_container is None or len(price_container) == 0:
                # 컨테이너 없으면 None 반환
                return None

            # 2단계: 컨테이너 내부에서만 할인 금액 extraction
            savings_xpaths = self.config.get_xpath_list('savings_inner', self.file_name) or [
                './/span[@data-testid="price-block-total-savings-text"]',
                './/div[@data-testid="price-block-total-savings"]//span',
                './/span[contains(@style, "color: rgb(232, 30, 37)") and contains(., "Save")]'
            ]

            for xpath in savings_xpaths:
                elem = price_container.xpath(xpath)
                if elem:
                    text = elem[0].text_content().strip()  # "Save $1,200"
                    # "$숫자" 패턴 extraction (콤마 포함 정규식 사용)
                    match = re.search(r'\$[\d,]+(?:\.\d{2})?', text)
                    if match:
                        return match.group()  # "$1,200.00" 형식 반환
            return None  # 세일이 아니면 None
        except Exception as e:
            print(f"  [ERROR] Savings extraction failed: {e}")
            return None

    def extract_star_rating(self, tree):
        """Star Rating extraction (평점 점수) - 실제 평점 먼저 찾고, 없으면 Not yet reviewed"""
        try:
            # Priority 1: visually-hidden p tag (가장 정확)
            hidden_xpaths = self.config.get_xpath_list('star_rating_hidden', self.file_name) or [
                '//p[@class="visually-hidden"][contains(text(), "Rating")]',
                '//p[contains(@class, "visually-hidden")][contains(text(), "out of 5 stars")]'
            ]
            for xpath in hidden_xpaths:
                elem = tree.xpath(xpath)
                if elem:
                    text = elem[0].text_content().strip()
                    match = re.search(r'Rating\s+([\d.]+)\s+out of', text)
                    if match:
                        return match.group(1)

            # Priority 2: 컨테이너 기반 extraction (fallback)
            container_xpaths = self.config.get_xpath_list('price_container', self.file_name) or [
                '/html/body/div[5]/div[4]/div[1]',
                '//div[@class="order-2 t3V0AOwowrTfUzPn "]',
                '//div[contains(@class, "order-2")]'
            ]

            price_container = None
            for xpath in container_xpaths:
                containers = tree.xpath(xpath)
                if containers:
                    price_container = containers[0]
                    break

            if price_container is None or len(price_container) == 0:
                return None

            rating_xpaths = self.config.get_xpath_list('star_rating_inner', self.file_name) or [
                './/div/div[4]/a/div/span[1]',
                './/div/div[3]/a/div/span[1]',
                './/span[@class="font-weight-medium  font-weight-bold order-1"]',
                './/span[contains(@class, "font-weight-bold") and contains(@class, "order-1")]',
                './/span[@aria-hidden="true"][contains(@class, "order-1")]',
                './/span[contains(@class, "X1oPXJyKAwAqyfx_")]',
                './/span[contains(@class, "heading-2") and contains(@class, "font-weight-medium")]',
            ]

            for xpath in rating_xpaths:
                elem = price_container.xpath(xpath)
                if elem:
                    rating = elem[0].text_content().strip()

                    if "Not yet reviewed" in rating:
                        return "Not yet reviewed"

                    if rating and re.match(r'^\d+\.\d+$', rating):
                        return rating

            return None
        except Exception as e:
            print(f"  [ERROR] Star_Rating extraction failed: {e}")
            return None

    def extract_count_of_reviews_from_detail(self, tree):
        """Count of Reviews extraction (메인 detail page에서) - DB에서 xpath 로드

        Example: '(79 reviews)' -> '79', 'Not yet reviewed' -> 0
        Note: '(45 reviews from Skyworth USA)' 같은 외부 리뷰는 'EXTERNAL_REVIEWS' 반환

        중요: Sponsored/Related Item 섹션(spotlight-ad)의 리뷰 수가 아닌
              현재 제품의 리뷰 수만 추출해야 함
        """
        try:
            # Step 1: "Not yet reviewed" 우선 감지 (Sponsored 섹션 제외)
            # 리뷰가 없는 제품을 먼저 처리하여 다른 섹션의 값이 잘못 추출되는 것을 방지
            not_reviewed_xpaths = self.config.get_xpath_list('not_yet_reviewed', self.file_name)
            if not not_reviewed_xpaths:
                print(f"  [WARNING] No not_yet_reviewed xpath in DB for {self.file_name}")
            else:
                for xpath in not_reviewed_xpaths:
                    elem = tree.xpath(xpath)
                    if elem:
                        text = elem[0].text_content().strip()
                        if "Not yet reviewed" in text:
                            return 0

            # Step 2: visually-hidden에서 리뷰 수 추출 (Sponsored 섹션 제외)
            hidden_xpaths = self.config.get_xpath_list('count_of_reviews_hidden', self.file_name)
            if not hidden_xpaths:
                print(f"  [WARNING] No count_of_reviews_hidden xpath in DB for {self.file_name}")
            else:
                for xpath in hidden_xpaths:
                    elem = tree.xpath(xpath)
                    if elem:
                        text = elem[0].text_content().strip()
                        # 외부 리뷰 감지: "(45 reviews from Skyworth USA)" 패턴
                        if re.search(r'reviews?\s+from\s+', text, re.IGNORECASE):
                            print(f"  [INFO] External reviews detected (hidden): {text}")
                            return 'EXTERNAL_REVIEWS'
                        # "Rating X.X out of 5 stars with N reviews" 패턴
                        match = re.search(r'with\s+([\d,]+)\s+reviews', text)
                        if match:
                            # hidden에서 숫자 추출 성공 → return 전에 visible에서 외부 리뷰 여부 확인
                            visible_xpaths_check = self.config.get_xpath_list('count_of_reviews_visible', self.file_name)
                            if visible_xpaths_check:
                                for vxpath in visible_xpaths_check:
                                    velem = tree.xpath(vxpath)
                                    if velem:
                                        vtext = velem[0].text_content().strip()
                                        if re.search(r'reviews?\s+from\s+', vtext, re.IGNORECASE):
                                            print(f"  [INFO] External reviews detected (visible cross-check): {vtext}")
                                            return 'EXTERNAL_REVIEWS'
                            return match.group(1).replace(',', '')

            # Step 3: visible span에서 리뷰 수 추출 (Sponsored 섹션 제외)
            visible_xpaths = self.config.get_xpath_list('count_of_reviews_visible', self.file_name)
            if not visible_xpaths:
                print(f"  [WARNING] No count_of_reviews_visible xpath in DB for {self.file_name}")
            else:
                for xpath in visible_xpaths:
                    elem = tree.xpath(xpath)
                    if elem:
                        text = elem[0].text_content().strip()
                        # "Not yet reviewed" 재확인
                        if "Not yet reviewed" in text:
                            return 0
                        # 외부 리뷰 감지
                        if re.search(r'reviews?\s+from\s+', text, re.IGNORECASE):
                            print(f"  [INFO] External reviews detected (visible): {text}")
                            return 'EXTERNAL_REVIEWS'
                        # "(N reviews)" 패턴
                        match = re.search(r'\(([\d,]+)\s*reviews?[^)]*\)', text, re.IGNORECASE)
                        if match:
                            return match.group(1).replace(',', '')

            return None
        except Exception as e:
            print(f"  [ERROR] Count_of_Reviews extraction failed: {e}")
            return None

    def close_specifications_dialog(self):
        """Specification dialog close (DrissionPage)"""
        try:
            print("  [INFO] Specification dialog close...")
            selectors = self.config.get_xpath_list('dialog_close_btn', self.file_name) or [
                'xpath://button[@data-testid="brix-sheet-closeButton"]',
                'xpath://button[@aria-label="Close Sheet"]',
                'xpath://div[@class="relative"]//button'
            ]

            for selector in selectors:
                try:
                    close_button = self.page.ele(selector, timeout=3)
                    if close_button:
                        close_button.click()
                        print("  [OK] dialog close successful")
                        time.sleep(2)
                        return True
                except:
                    continue

            print("  [WARNING] dialog close button not found.")
            return False

        except Exception as e:
            print(f"  [ERROR] dialog close failed: {e}")
            return False

    def extract_similar_products(self, tree):
        """Compare similar products data extraction"""
        try:
            similar_names = []
            pros_list = []
            cons_list = []

            # Retailer_SKU_Name_similar extraction
            name_xpaths = self.config.get_xpath_list('similar_product_names', self.file_name) or [
                '//span[@class="clamp" and starts-with(@id, "compare-title-")]'
            ]
            for xpath in name_xpaths:
                name_elements = tree.xpath(xpath)
                if name_elements:
                    for elem in name_elements[:4]:  # 최대 4items
                        similar_names.append(elem.text_content().strip())
                    break

            # Pros extraction
            pros_xpaths = self.config.get_xpath_list('similar_product_pros', self.file_name) or [
                '//tr[@class="flex"]//td[.//svg[@aria-label="Advantage Icon"]]//span[@class="text-3 min-w-0 flex flex-wrap"]'
            ]
            for xpath in pros_xpaths:
                pros_elements = tree.xpath(xpath)
                if pros_elements:
                    for elem in pros_elements[:4]:  # 최대 4items
                        pros_list.append(elem.text_content().strip())
                    break

            # Cons extraction
            cons_xpaths = self.config.get_xpath_list('similar_product_cons', self.file_name) or [
                '//tr[@class="flex"]//td[.//svg[@aria-label="Disadvantage Icon"]]//span[@class="text-3 min-w-0 flex flex-wrap"]'
            ]
            for xpath in cons_xpaths:
                cons_elements = tree.xpath(xpath)
                if cons_elements:
                    for elem in cons_elements[:4]:  # 최대 4items
                        text = elem.text_content().strip()
                        if text and text != '—':
                            cons_list.append(text)
                        else:
                            cons_list.append(None)
                    break

            # 부족한 경우 None으로 fill
            while len(similar_names) < 4:
                similar_names.append(None)
            while len(pros_list) < 4:
                pros_list.append(None)
            while len(cons_list) < 4:
                cons_list.append(None)

            return similar_names[:4], pros_list[:4], cons_list[:4]

        except Exception as e:
            print(f"  [ERROR] Similar products extraction failed: {e}")
            return [None]*4, [None]*4, [None]*4

    def extract_star_rating_from_reviews_page(self, tree):
        """리뷰 페이지에서 star_rating (평점) 추출 - lxml tree
        예: <div class="overall-rating">4.5</div>
        """
        try:
            xpaths = self.config.get_xpath_list('star_rating_reviews_page', self.file_name) or [
                '//div[@class="overall-rating"]',
                '//*[@id="reviews-accordion"]/section/div[1]/div[1]/div/div/div[1]/div/div[1]',
                '//div[contains(@class, "overall-rating")]',
            ]

            for xpath in xpaths:
                try:
                    elem = tree.xpath(xpath)
                    if elem:
                        text = elem[0].text_content().strip()
                        # "4.5" 형태의 숫자만 추출
                        match = re.search(r'(\d+\.?\d*)', text)
                        if match:
                            rating = match.group(1)
                            print(f"  [OK] Star_Rating from reviews page: {rating}")
                            return rating
                except:
                    continue

            return None

        except Exception as e:
            print(f"  [ERROR] Star_Rating extraction from reviews page failed: {e}")
            return None

    def extract_star_ratings_from_reviews_page(self):
        """Count_of_Star_Ratings extraction (See All Customer Reviews page에서) - DrissionPage
        Returns: integer (total count) or None
        """
        try:
            time.sleep(3)  # page 로딩 wait
            total_count = 0
            # XPath 패턴 (5점부터 1점까지) - config에서 로드
            star_config_keys = ['star_ratings_5', 'star_ratings_4', 'star_ratings_3', 'star_ratings_2', 'star_ratings_1']
            default_xpaths = [
                'xpath://*[@id="reviews-accordion"]/section/div[1]/div[1]/div/div/div[2]/div/fieldset/div[1]/div/label/span[5]',  # 5점
                'xpath://*[@id="reviews-accordion"]/section/div[1]/div[1]/div/div/div[2]/div/fieldset/div[2]/div/label/span[5]',  # 4점
                'xpath://*[@id="reviews-accordion"]/section/div[1]/div[1]/div/div/div[2]/div/fieldset/div[3]/div/label/span[5]',  # 3점
                'xpath://*[@id="reviews-accordion"]/section/div[1]/div[1]/div/div/div[2]/div/fieldset/div[4]/div/label/span[5]',  # 2점
                'xpath://*[@id="reviews-accordion"]/section/div[1]/div[1]/div/div/div[2]/div/fieldset/div[5]/div/label/span[5]'   # 1점
            ]

            # 5점부터 1점까지 순서로 extraction
            for idx, config_key in enumerate(star_config_keys):
                selectors = self.config.get_xpath_list(config_key, self.file_name) or [default_xpaths[idx]]
                for selector in selectors:
                    try:
                        elem = self.page.ele(selector, timeout=2)
                        if elem:
                            count_text = elem.text.strip()
                            count = int(count_text) if count_text.isdigit() else 0
                            total_count += count
                            break
                    except Exception:
                        continue

            return total_count if total_count > 0 else None

        except Exception as e:
            print(f"  [ERROR] Star ratings extraction failed: {e}")
            return None

    def extract_count_of_reviews(self):
        """Count_of_Reviews extraction (See All Customer Reviews page에서) - DrissionPage"""
        try:
            # Selector 패턴
            selectors = self.config.get_xpath_list('count_reviews_page', self.file_name) or [
                'xpath://span[@class="c-reviews order-2"]',
                'xpath://div[contains(@id, "user-generated-content-ugc-stats")]//span[@class="c-reviews order-2"]',
                'xpath://span[contains(@class, "c-reviews")]'
            ]

            for selector in selectors:
                try:
                    elem = self.page.ele(selector, timeout=3)
                    if elem:
                        text = elem.text.strip()
                        # 숫자만 extraction (예: "(84 Reviews)" -> "84")
                        match = re.search(r'\((\d+)\s*Reviews?\)', text)
                        if match:
                            return match.group(1)
                except Exception:
                    continue

            return None

        except Exception as e:
            print(f"  [ERROR] Count of reviews extraction failed: {e}")
            return None

    def extract_top_mentions_from_reviews_page(self, tree):
        """Top_Mentions extraction (See All Customer Reviews page에서) - lxml tree
        Returns: 콤마로 구분된 모든 mentions (예: "Picture Quality, Setup, Size")
        """
        try:
            # XPath 패턴 - 제공된 HTML 구조 기반
            xpaths = self.config.get_xpath_list('top_mentions', self.file_name) or [
                '/html/body/div[5]/div[8]/div[2]/aside/ul/li/a',
                '//ul[@class="list-unstyled"]/li/a[contains(@class, "v-text-tech-black")]',
                '//ul[@class="list-unstyled"]/li/a',
                '//div[contains(@class, "customer-review-pros-stats")]//span[@class="text-nowrap"]',
                '//div[contains(., "Highly rated by customers for")]//span[@class="text-nowrap"]'
            ]

            mentions = []
            for xpath in xpaths:
                try:
                    elements = tree.xpath(xpath)
                    if elements:
                        for elem in elements:
                            text = elem.text_content().strip()
                            if text:
                                # 숫자와 괄호 제거 (예: "Picture Quality (1014)" -> "Picture Quality")
                                # "&nbsp;" 처리 및 괄호+숫자 패턴 제거 (콤마 포함 숫자도 처리)
                                clean_text = re.sub(r'\s*\([\d,]+\)\s*$', '', text)
                                # 앞뒤 공백 및 특수문자 정리
                                clean_text = clean_text.replace('\xa0', ' ').strip()
                                if clean_text:
                                    mentions.append(clean_text)
                        break
                except Exception:
                    continue

            if mentions:
                # 모든 mentions를 콤마로 구분하여 반환
                return ', '.join(mentions)

            # Fallback: distillation-card에서 Pros + Cons mentioned 수집 (data-feature-name 속성 사용)
            try:
                fallback_mentions = []
                # Pros mentioned 먼저
                pros_elements = tree.xpath('//div[contains(@class, "pros-container")]//button[@data-feature-name]/@data-feature-name')
                for feature_name in pros_elements:
                    if feature_name:
                        fallback_mentions.append(feature_name.strip())
                # Cons mentioned 다음
                cons_elements = tree.xpath('//div[contains(@class, "cons-container")]//button[@data-feature-name]/@data-feature-name')
                for feature_name in cons_elements:
                    if feature_name:
                        fallback_mentions.append(feature_name.strip())
                if fallback_mentions:
                    return ', '.join(fallback_mentions)
            except Exception:
                pass

            return None

        except Exception as e:
            print(f"  [ERROR] Top mentions extraction failed: {e}")
            return None

    def extract_bestbuy_sku_number(self):
        """BestBuy SKU 번호 추출 (예: 6614066)"""
        try:
            # 방법 1: SKU div에서 추출
            sku_selectors = self.config.get_xpath_list('bestbuy_sku', self.file_name) or [
                'xpath://div[contains(text(), "SKU:")]',
                'xpath://div[@class="pr-150 inline-block"][contains(text(), "SKU")]',
            ]

            for selector in sku_selectors:
                try:
                    elem = self.page.ele(selector, timeout=2)
                    if elem:
                        text = elem.text.strip()
                        # "SKU: 6614066" -> "6614066"
                        match = re.search(r'SKU[:\s]+(\d+)', text)
                        if match:
                            return match.group(1)
                except:
                    continue

            # 방법 2: data-testid 속성에서 추출
            testid_selectors = self.config.get_xpath_list('bestbuy_sku_testid', self.file_name) or [
                'xpath://div[contains(@data-testid, "mbo-entrypoint-")]'
            ]
            for testid_selector in testid_selectors:
                try:
                    elem = self.page.ele(testid_selector, timeout=2)
                    if elem:
                        testid = elem.attr('data-testid')
                        # "mbo-entrypoint-6614066" -> "6614066"
                        match = re.search(r'mbo-entrypoint-(\d+)', testid)
                        if match:
                            return match.group(1)
                except:
                    continue

            # 방법 3: 페이지 소스에서 직접 추출
            page_html = self.page.html
            match = re.search(r'SKU[:\s]+<!--\s*-->(\d+)', page_html)
            if match:
                return match.group(1)

            match = re.search(r'mbo-entrypoint-(\d+)', page_html)
            if match:
                return match.group(1)

            return None

        except Exception as e:
            print(f"  [ERROR] BestBuy SKU number extraction failed: {e}")
            return None

    def extract_product_slug_from_url(self, url):
        """URL에서 제품 slug 추출
        예: https://www.bestbuy.com/product/insignia-40-class.../J2FPJKSFFJ
        -> insignia-40-class-f40-series-led-full-hd-1080p-smart-fire-tv
        """
        try:
            # /product/ 뒤의 slug 추출
            match = re.search(r'/product/([^/]+)/', url)
            if match:
                return match.group(1)
            return None
        except:
            return None

    def navigate_to_reviews_page(self, product_url):
        """리뷰 페이지로 직접 이동 (버튼 클릭 대신) - SKU 검증 포함"""
        try:
            print("  [INFO] Navigating to reviews page directly...")

            # SKU 번호 추출 (상세 페이지에서)
            expected_sku = self.extract_bestbuy_sku_number()
            if not expected_sku:
                print("  [WARNING] Could not extract BestBuy SKU number")
                return False
            print(f"  [OK] BestBuy SKU number (expected): {expected_sku}")

            # 제품 slug 추출
            product_slug = self.extract_product_slug_from_url(product_url)
            if not product_slug:
                print("  [WARNING] Could not extract product slug from URL")
                return False
            print(f"  [OK] Product slug: {product_slug}")

            # 리뷰 URL 생성
            reviews_url = f"https://www.bestbuy.com/site/reviews/{product_slug}/{expected_sku}"
            print(f"  [INFO] Reviews URL: {reviews_url}")

            # 리뷰 페이지 접근
            self.page.get(reviews_url)
            time.sleep(3)

            # SKU 검증: 현재 URL에서 SKU 추출하여 비교
            current_url = self.page.url
            actual_sku_match = re.search(r'/reviews/[^/]+/(\d+)', current_url)
            if actual_sku_match:
                actual_sku = actual_sku_match.group(1)
                if actual_sku != expected_sku:
                    print(f"  [ERROR] SKU mismatch! Expected: {expected_sku}, Actual: {actual_sku}")
                    print(f"  [ERROR] Sending alert email...")
                    send_review_url_error_alert(product_url, expected_sku, actual_sku)
                    return False
                else:
                    print(f"  [OK] SKU verified: {actual_sku}")
            else:
                print(f"  [WARNING] Could not extract SKU from review page URL for verification")

            # 페이지 로드 확인
            page_html = self.page.html
            if "reviews" in page_html.lower() or "rating" in page_html.lower():
                print("  [OK] Reviews page loaded successfully")
                return True
            else:
                print("  [WARNING] Reviews page may not have loaded correctly")
                return True  # 일단 진행

        except Exception as e:
            print(f"  [ERROR] Navigate to reviews page failed: {e}")
            return False

    def click_see_all_reviews(self, product_url=None):
        """See All Customer Reviews - 버튼 클릭 먼저, 실패 시 직접 URL 접근 (DrissionPage)"""
        try:
            print("  [INFO] See All Customer Reviews button searching...")

            selectors = self.config.get_xpath_list('see_all_reviews_btn', self.file_name) or [
                'xpath://button[contains(., "See All Customer Reviews")]',
                'xpath://button[contains(@class, "Op9coqeII1kYHR9Q")]',
                'css:button.Op9coqeII1kYHR9Q'
            ]

            # 1. DOM에서 바로 버튼 검색 (스크롤 없이)
            for selector in selectors:
                try:
                    button = self.page.ele(selector, timeout=2)
                    if button:
                        print("  [OK] See All Customer Reviews button found (in DOM)")
                        button.scroll.to_see()
                        time.sleep(0.5)
                        button.click()
                        print("  [OK] See All Customer Reviews click successful")
                        time.sleep(3)
                        return True
                except:
                    continue

            # 2. 리뷰 섹션으로 빠르게 스크롤 후 검색
            print("  [INFO] Scrolling to review section...")
            self.page.run_js("window.scrollTo(0, document.body.scrollHeight * 0.7)")
            time.sleep(1)

            for selector in selectors:
                try:
                    button = self.page.ele(selector, timeout=2)
                    if button:
                        print("  [OK] See All Customer Reviews button found (after scroll)")
                        button.scroll.to_see()
                        time.sleep(0.5)
                        button.click()
                        print("  [OK] See All Customer Reviews click successful")
                        time.sleep(3)
                        return True
                except:
                    continue

            # 3. 버튼 못 찾으면 직접 URL 접근 시도 (fallback)
            print("  [WARNING] See All Customer Reviews button not found. Trying direct URL...")
            if product_url and self.navigate_to_reviews_page(product_url):
                return True

            print("  [ERROR] Both button click and direct URL navigation failed.")
            return False

        except Exception as e:
            print(f"  [ERROR] See All Customer Reviews click failed: {e}")
            return False

    def extract_reviews(self):
        """review 20items collected (page네이션 포함) - DrissionPage
        저장 형식: "review 1- 내용 | review 2- 내용 | ... | review 20- 내용"
        """
        try:
            time.sleep(3)  # page 로딩 wait
            reviews = []
            collected = 0
            page_num = 1

            while collected < 20:
                # page 소스 가져오기
                page_source = self.page.html
                tree = html.fromstring(page_source)

                # review extraction
                review_xpaths = self.config.get_xpath_list('review_items', self.file_name) or [
                    '//li[@class="review-item"]//div[@class="ugc-review-body"]//p[@class="pre-white-space"]'
                ]
                review_elements = []
                for xpath in review_xpaths:
                    review_elements = tree.xpath(xpath)
                    if review_elements:
                        break

                for elem in review_elements:
                    if collected >= 20:
                        break
                    review_text = elem.text_content().strip()
                    if review_text:
                        collected += 1
                        # "review N- 내용" 형식으로 저장
                        formatted_review = f"review {collected}- {review_text}"
                        reviews.append(formatted_review)
                        print(f"    [review {collected}/20] {review_text[:50]}...")

                # 20items collected complete하면 closed
                if collected >= 20:
                    break

                # next page button 찾기
                try:
                    next_button = self.page.ele('xpath://li[contains(@class, "page next")]//a', timeout=3)
                    if next_button:
                        print(f"  [INFO] Navigating to next page... (Page {page_num + 1})")
                        next_button.scroll.to_see()
                        time.sleep(2)
                        next_button.click()
                        time.sleep(4)
                        page_num += 1
                    else:
                        print("  [INFO] next page button not found. collected closed.")
                        break
                except:
                    print("  [INFO] next page button not found. collected closed.")
                    break

            # review를 구분자로 connection (예: "review 1- 내용 | review 2- 내용 | ...")
            return " | ".join(reviews) if reviews else None

        except Exception as e:
            print(f"  [ERROR] review collected failed: {e}")
            return None

    def extract_summarized_review_content(self, tree):
        """Summarized_Review_Content extraction (AI 요약 리뷰) - 상세 페이지에서
        예: "Customers frequently mention the superb picture quality..."
        """
        try:
            xpaths = self.config.get_xpath_list('summarized_review', self.file_name) or [
                # review-summary 컨테이너 기반 (우선순위 높음)
                '//div[contains(@class, "review-summary")]//p[contains(@class, "body-copy-lg")]',
                # "Customers are saying" 헤더 기준
                '//h4[contains(text(), "Customers are saying")]/following-sibling::p',
                # 클래스 기반
                '//p[@class="mt-200 body-copy-lg mb-none"]',
                # 부분 클래스 매칭
                '//p[contains(@class, "body-copy-lg") and contains(@class, "mb-none")]',
                # 제공된 절대 경로 (fallback)
                '/html/body/div[5]/div[8]/div[2]/div/div[1]/div/p',
            ]

            for xpath in xpaths:
                try:
                    elem = tree.xpath(xpath)
                    if elem:
                        text = elem[0].text_content().strip()
                        if text:
                            print(f"  [OK] Summarized_Review_Content found: {text[:60]}...")
                            return text
                except:
                    continue

            return None

        except Exception as e:
            print(f"  [ERROR] Summarized_Review_Content extraction failed: {e}")
            return None

    def extract_summarized_review_content_from_reviews_page(self, tree):
        """Summarized_Review_Content extraction (리뷰 페이지에서) - lxml tree
        상세 페이지에서 추출 못했을 때 fallback으로 사용
        """
        try:
            # 리뷰 페이지의 AI 요약 위치
            xpaths = self.config.get_xpath_list('summarized_review_reviews_page', self.file_name) or [
                '//*[@id="reviews-accordion"]/div[1]/div/p[1]',
                '//p[@class="mb-200 mt-none"]',
                '//div[@id="reviews-accordion"]//p[contains(@class, "mb-200")]',
            ]

            for xpath in xpaths:
                try:
                    elem = tree.xpath(xpath)
                    if elem:
                        text = elem[0].text_content().strip()
                        if text:
                            return text
                except:
                    continue

            return None

        except Exception as e:
            print(f"  [ERROR] Summarized_Review_Content (reviews page) extraction failed: {e}")
            return None

    def extract_recommendation_intent_from_reviews_page(self, tree):
        """Recommendation_Intent extraction (See All Customer Reviews page에서) - lxml tree"""
        try:
            # XPath 패턴
            xpaths = [
                # 제공된 HTML 기준
                '//div[contains(@class, "recommendation-card-no-donut")]//span[@class="recommendation-percent v-fw-medium"]',
                # 더 넓은 패턴
                '//span[contains(@class, "recommendation-percent")]'
            ]

            percent = None
            for xpath in xpaths:
                try:
                    elem = tree.xpath(xpath)
                    if elem:
                        percent = elem[0].text_content().strip()
                        if percent:
                            break
                except Exception:
                    continue

            if percent:
                # "100% would recommend to a friend" 형식으로 반환
                return f"{percent} would recommend to a friend"

            return None

        except Exception as e:
            print(f"  [ERROR] Recommendation intent extraction failed: {e}")
            return None

    def extract_compare_similar_products(self, current_url):
        """Compare similar products section data extraction (first page 로딩 items선) - DrissionPage"""
        max_retries = 2

        for retry in range(max_retries):
            try:
                if retry > 0:
                    print(f"  [RETRY {retry}/{max_retries}] Compare similar products retry...")
                else:
                    print("  [INFO] Compare similar products section searching...")

                # page 상단으로 이동 후 30%까지 스크롤
                self.page.run_js("window.scrollTo(0, 0)")
                time.sleep(1)

                total_height = self.page.run_js("return document.body.scrollHeight")
                scroll_to = int(total_height * 0.3)
                self.page.run_js(f"window.scrollTo(0, {scroll_to})")

                # first page 여부 확인
                is_first_page = (self.order == 1)

                # timeout 설정: first page는 30sec, 나머지는 15sec
                timeout = 30 if is_first_page else 15

                if is_first_page:
                    print(f"  [INFO] first page detected - applying long wait time (max {timeout}sec)")

                # DrissionPage로 product-title element가 load될 때까지 명시적 wait
                try:
                    product_title_elem = self.page.ele(
                        'xpath://div[@class="product-title font-weight-normal pb-100 body-copy-lg min-h-600"]',
                        timeout=timeout
                    )
                    if product_title_elem:
                        print(f"  [OK] Compare similar products element load complete")
                        # element가 load된 후 안정화를 위한 추가 wait
                        additional_wait = 5 if is_first_page else 3
                        time.sleep(additional_wait)
                    else:
                        raise Exception("product-title element not found")

                except Exception as wait_error:
                    print(f"  [WARNING] element wait time exceeded: {wait_error}")
                    if retry < max_retries - 1:
                        # next retry를 위해 page refresh
                        print("  [INFO] page refresh and retry...")
                        self.page.refresh()
                        time.sleep(10)
                        continue
                    else:
                        # 마지막 attempt였다면 None 반환
                        return None

                # page 소스 가져오기
                page_source = self.page.html
                tree = html.fromstring(page_source)

                # 4items 제품 data save
                products = []

                # product-title div들 찾기
                product_divs = tree.xpath('//div[@class="product-title font-weight-normal pb-100 body-copy-lg min-h-600"]')

                if len(product_divs) < 4:
                    print(f"  [WARNING] insufficient products. (found items count: {len(product_divs)})")
                    if retry < max_retries - 1:
                        # retry
                        time.sleep(5)
                        continue
                    else:
                        return None

                # first 번째 제품 (현재 page)
                first_product = {
                    'product_url': current_url,
                    'product_name': None,
                    'pros': None,
                    'cons': None
                }

                # first 번째 제품명 extraction
                span_elem = product_divs[0].xpath('.//span[@class="clamp"]')
                if span_elem:
                    first_product['product_name'] = span_elem[0].text_content().strip()

                products.append(first_product)

                # 2-4번째 제품
                for i in range(1, 4):
                    if i < len(product_divs):
                        product = {
                            'product_url': None,
                            'product_name': None,
                            'pros': None,
                            'cons': None
                        }

                        # a 태그에서 URL과 제품명 extraction
                        a_elem = product_divs[i].xpath('.//a[@class="clamp"]')
                        if a_elem:
                            href = a_elem[0].get('href')
                            if href:
                                product['product_url'] = href
                            product['product_name'] = a_elem[0].text_content().strip()

                        products.append(product)

                # Pros extraction (tr[2]/td[1~4])
                for i in range(1, 5):
                    pros_xpath = f'/html/body/div[5]/div[6]/div/table/tbody/tr[2]/td[{i}]/span/span'
                    pros_elem = tree.xpath(pros_xpath)
                    if pros_elem and i-1 < len(products):
                        products[i-1]['pros'] = pros_elem[0].text_content().strip()

                # Cons extraction (tr[4]/td[1~4])
                for i in range(1, 5):
                    cons_xpath = f'/html/body/div[5]/div[6]/div/table/tbody/tr[4]/td[{i}]/span/span'
                    cons_elem = tree.xpath(cons_xpath)
                    if cons_elem and i-1 < len(products):
                        text = cons_elem[0].text_content().strip()
                        # '—' 같은 값은 None으로 처리
                        products[i-1]['cons'] = text if text and text != '—' else None

                print(f"  [OK] Compare similar products data extraction complete (4items)")
                return products

            except Exception as e:
                print(f"  [ERROR] Compare similar products extraction failed (attempt {retry + 1}/{max_retries}): {e}")
                if retry < max_retries - 1:
                    print("  [INFO] Retrying...")
                    time.sleep(5)
                    continue
                else:
                    import traceback
                    traceback.print_exc()
                    return None

        return None

    def get_item_by_product_name(self, product_name):
        """bby_tv_crawl에서 product_name으로 item 찾기"""
        try:
            if not product_name:
                return None

            cursor = self.db_conn.cursor()
            detail_table = self.config.get_table('detail_data') or 'bby_tv_crawl'

            # 가장 최근 data에서 retailer_sku_name과 product_name이 일치하는 것 찾기
            cursor.execute(f"""
                SELECT item
                FROM {detail_table}
                WHERE retailer_sku_name = %s
                AND item IS NOT NULL
                ORDER BY crawl_datetime DESC
                LIMIT 1
            """, (product_name,))

            result = cursor.fetchone()
            cursor.close()

            if result:
                return result[0]
            return None

        except Exception as e:
            print(f"  [ERROR] Item lookup failed ({product_name}): {e}")
            return None

    def scrape_detail_page(self, url_data):
        """detail page crawling (items선된 로딩 + dialog 처리) - DrissionPage"""
        try:
            self.order += 1
            page_type = url_data['page_type']
            product_url = url_data['product_url']

            print(f"\n{'='*80}")
            print(f"[{self.order}] [{page_type.upper()}] Accessing: {product_url[:80]}...")
            print(f"[INFO] Page type: {page_type} | Main rank: {url_data.get('main_rank', 'N/A')} | BSR rank: {url_data.get('bsr_rank', 'N/A')} | Trend rank: {url_data.get('trend_rank', 'N/A')}")

            # page 접속 (DrissionPage)
            print(f"  [INFO] Loading page...")
            self.page.get(product_url)

            # ERR_HTTP2_PROTOCOL_ERROR 감지
            try:
                page_title = self.page.title or ''
                page_text = self.page.html[:2000] if self.page.html else ''
                if 'ERR_HTTP2_PROTOCOL_ERROR' in page_text or "This site can't be reached" in page_text or 'ERR_HTTP2_PROTOCOL_ERROR' in page_title:
                    print(f"  [BLOCKED] ERR_HTTP2_PROTOCOL_ERROR detected - Best Buy blocked this request")
                    return 'blocked'
            except Exception:
                pass

            # 핵심 element load wait (최대 20sec) - DrissionPage
            try:
                h1_elem = self.page.ele('xpath://h1[contains(@class, "h4") or contains(@class, "heading")]', timeout=20)
                if h1_elem:
                    print(f"  [OK] page load complete")
                else:
                    print(f"  [ERROR] page loading timeout - h1 element not found")
                    return False
            except Exception as e:
                print(f"  [ERROR] page loading timeout: {e}")
                return False

            # DOM 우선 탐색 + 조건부 스크롤 (최적화)
            # 주요 요소들이 DOM에 이미 있으면 스크롤 생략
            print(f"  [INFO] Checking DOM for key elements...")
            page_source = self.page.html
            tree = html.fromstring(page_source)

            # 핵심 요소 존재 여부 확인
            key_elements = {
                'price': tree.xpath('//div[@data-testid="price-block-customer-price"] | //span[contains(text(), "See price in cart")]'),
                'rating': tree.xpath('//p[contains(@class, "visually-hidden")][contains(text(), "Rating")]'),
                'title': tree.xpath('//h1[contains(@class, "heading")]//span')
            }

            elements_found = sum(1 for v in key_elements.values() if v)
            print(f"  [INFO] DOM check: {elements_found}/3 key elements found")

            # 핵심 요소가 2개 이상 있으면 스크롤 생략
            if elements_found >= 2:
                print(f"  [OK] Key elements found in DOM - skipping full scroll")
            else:
                # 요소가 부족하면 스크롤 수행 (최적화된 버전)
                print(f"  [INFO] Starting optimized scroll for lazy loading...")
                scroll_positions = [1000, 3000, 5000, 8000]  # 주요 위치만 스크롤

                for pos in scroll_positions:
                    self.page.run_js(f"window.scrollTo(0, {pos})")
                    time.sleep(0.3)

                # Scroll back to top
                self.page.run_js("window.scrollTo(0, 0)")
                time.sleep(0.5)
                print(f"  [OK] Optimized scroll complete")

            # page 소스 가져오기
            page_source = self.page.html
            tree = html.fromstring(page_source)

            # 1. Retailer_SKU_Name - 소스 테이블에서 가져온 값 사용, 없으면 detail에서 추출
            retailer_sku_name = url_data.get('retailer_sku_name')
            if not retailer_sku_name or len(retailer_sku_name) < 3:
                print(f"  [INFO] retailer_sku_name 없음 - detail 페이지에서 추출 시도")
                retailer_sku_name = self.extract_retailer_sku_name(tree)
            print(f"  [✓] Retailer_SKU_Name: {retailer_sku_name}")

            # 2. Screen Size extraction (메인 page에서)
            extracted_screen_size = self.extract_screen_size(tree)
            print(f"  [✓] Screen Size (extracted): {extracted_screen_size}")

            # 2-0. Model Year - dialog에서 추출 (아래에서 처리)
            model_year = None

            # 2-1. Price 정보 extraction (메인 page에서 직접 collected)
            # 가격 컨테이너 로딩 대기 (최대 10초) - DrissionPage
            try:
                price_elem = self.page.ele(
                    'xpath://div[contains(@class, "order-2")]//div[@data-testid="price-block-customer-price"] | //div[contains(@class, "order-2")]//span[contains(text(), "See price in cart")] | //div[contains(text(), "no longer available in new condition")]',
                    timeout=10
                )
                if price_elem:
                    print(f"  [OK] price container load complete")
                # 가격 컨테이너 로딩 후 tree 다시 가져오기
                page_source = self.page.html
                tree = html.fromstring(page_source)
            except Exception:
                print(f"  [WARNING] price container loading timeout - attempting extraction anyway")

            final_sku_price = self.extract_final_sku_price(tree)
            print(f"  [✓] Final_SKU_Price: {final_sku_price}")

            savings = self.extract_savings(tree)
            print(f"  [✓] Savings: {savings}")

            # original_sku_price는 savings와 final_sku_price 추출 후에 시도
            # (fallback 로직이 두 값을 모두 확인하기 때문)
            original_sku_price = self.extract_original_sku_price(tree, savings, final_sku_price)
            print(f"  [✓] Original_SKU_Price: {original_sku_price}")

            # 2-2. Star Rating 및 Reviews 정보 extraction (메인 page에서 직접 collected)
            star_rating = self.extract_star_rating(tree)
            print(f"  [✓] Star_Rating: {star_rating}")

            count_of_reviews = self.extract_count_of_reviews_from_detail(tree)

            # 외부 리뷰 감지 시 (예: "reviews from Skyworth USA") 0으로 처리, star_rating도 변경
            is_external_reviews = (count_of_reviews == 'EXTERNAL_REVIEWS')
            if is_external_reviews:
                count_of_reviews = 0
                star_rating = "Not yet reviewed"  # 외부 리뷰는 BestBuy 리뷰가 아니므로
                print(f"  [✓] Count_of_Reviews: 0 (외부 리뷰 - BestBuy 자체 리뷰 아님)")
                print(f"  [✓] Star_Rating: Not yet reviewed (외부 리뷰)")
            else:
                print(f"  [✓] Count_of_Reviews: {count_of_reviews}")

            # 2-3. Summarized Review Content - 리뷰 페이지에서만 수집 (아래에서 처리)
            summarized_review_content = None

            # 3. Compare similar products extraction (실패 시 새로고침 후 재시도)
            similar_products = self.extract_compare_similar_products(product_url)
            if similar_products:
                print(f"  [✓] Similar Products: {len(similar_products)} items found")
            else:
                print(f"  [INFO] Similar Products 추출 실패, 페이지 새로고침 후 재시도...")
                self.page.refresh()
                time.sleep(5)
                similar_products = self.extract_compare_similar_products(product_url)
                if similar_products:
                    print(f"  [✓] Similar Products (재시도 성공): {len(similar_products)} items found")
                else:
                    print(f"  [✓] Similar Products: None (섹션 없음 또는 로드 실패)")

            # 4. Item extraction from URL (simplified - no dialog needed)
            item = self.extract_item_from_url(product_url)

            # Electricity use and SKU - need to open dialog for these fields
            extracted_electricity_use = None
            sku = "no sku"
            success, error = self.click_specifications_with_retry()

            if success:
                time.sleep(3)
                # dialog 소스 가져오기 (DrissionPage)
                dialog_source = self.page.html
                dialog_tree = html.fromstring(dialog_source)

                # Extract Estimated_Annual_Electricity_Use (숫자만)
                extracted_electricity_use = self.extract_electricity_use(dialog_tree)
                print(f"  [✓] Estimated_Annual_Electricity_Use (extracted): {extracted_electricity_use}")

                # Extract SKU (Model Number) from dialog
                sku = self.extract_sku(dialog_tree)

                # Extract Model Year from dialog
                model_year = self.extract_model_year(dialog_tree)
                print(f"  [✓] Model Year: {model_year}")

                # dialog close
                self.close_specifications_dialog()
            else:
                print(f"  [WARNING] Could not extract electricity_use/SKU (dialog failed): {error}")

            # tv_item_mst fallback for screen_size and electricity_use
            item_mst_data = self.get_item_mst_data(item)
            mst_screen_size = item_mst_data.get('screen_size') if item_mst_data else None
            mst_electricity_use = item_mst_data.get('estimated_annual_electricity_use') if item_mst_data else None

            # Determine final screen_size with fallback and mismatch tracking
            if extracted_screen_size and mst_screen_size:
                if extracted_screen_size != mst_screen_size:
                    print(f"  [WARNING] screen_size mismatch: extracted='{extracted_screen_size}', tv_item_mst='{mst_screen_size}'")
                    self.screen_size_mismatch_records.append({
                        'item': item,
                        'url': product_url,
                        'extracted': extracted_screen_size,
                        'mst_value': mst_screen_size
                    })
                screen_size = extracted_screen_size
            elif extracted_screen_size:
                screen_size = extracted_screen_size
            elif mst_screen_size:
                screen_size = mst_screen_size
                print(f"  [INFO] Using screen_size from tv_item_mst: {mst_screen_size}")
            else:
                screen_size = None

            # Determine final electricity_use with fallback and mismatch tracking
            if extracted_electricity_use and mst_electricity_use:
                if extracted_electricity_use != mst_electricity_use:
                    print(f"  [WARNING] electricity_use mismatch: extracted='{extracted_electricity_use}', tv_item_mst='{mst_electricity_use}'")
                    self.electricity_use_mismatch_records.append({
                        'item': item,
                        'url': product_url,
                        'extracted': extracted_electricity_use,
                        'mst_value': mst_electricity_use
                    })
                electricity_use = extracted_electricity_use
            elif extracted_electricity_use:
                electricity_use = extracted_electricity_use
            elif mst_electricity_use:
                electricity_use = mst_electricity_use
                print(f"  [INFO] Using electricity_use from tv_item_mst: {mst_electricity_use}")
            else:
                electricity_use = None

            # 8. See All Customer Reviews click 및 data collected
            # count_of_star_ratings는 count_of_reviews와 동일 값 사용
            count_of_star_ratings = count_of_reviews
            print(f"  [✓] count_of_star_ratings: {count_of_star_ratings} (= count_of_reviews)")

            top_mentions = None
            detailed_reviews = None
            recommendation_intent = None

            # 외부 리뷰인 경우 리뷰 페이지 접근 스킵
            if is_external_reviews:
                print(f"  [INFO] 외부 리뷰 - 리뷰 페이지 수집 스킵 (detailed_reviews, top_mentions 등 수집 안함)")
            elif self.click_see_all_reviews(product_url):
                # 리뷰 페이지 로드 대기 (요소 기반) 및 tree 파싱
                try:
                    review_elem = self.page.ele('xpath://div[@class="overall-rating"] | //div[@id="reviews-accordion"]', timeout=10)
                    if review_elem:
                        print(f"  [OK] Reviews page loaded")
                except:
                    print(f"  [WARNING] Reviews page element wait timeout")

                reviews_page_source = self.page.html
                reviews_tree = html.fromstring(reviews_page_source)

                # 9-0. 상세페이지에서 star_rating 못 찾았으면 리뷰 페이지에서 재추출
                if not star_rating or star_rating == "Not yet reviewed":
                    star_rating_from_reviews = self.extract_star_rating_from_reviews_page(reviews_tree)
                    if star_rating_from_reviews:
                        star_rating = star_rating_from_reviews
                        print(f"  [✓] Star_Rating (from reviews page): {star_rating}")

                # 9-1. Star ratings collected (review page에서 - 별점별 detail items count)
                # NOTE: count_of_star_ratings는 count_of_reviews와 동일하므로 별도 추출 불필요
                # count_of_star_ratings = self.extract_star_ratings_from_reviews_page()
                # print(f"  [✓] count_of_star_ratings: {count_of_star_ratings}")

                # 9-2. Top mentions collected (review page에서)
                top_mentions = self.extract_top_mentions_from_reviews_page(reviews_tree)
                print(f"  [✓] Top_Mentions: {top_mentions}")

                # 9-3. Recommendation intent collected (review page에서)
                recommendation_intent = self.extract_recommendation_intent_from_reviews_page(reviews_tree)
                print(f"  [✓] Recommendation_Intent: {recommendation_intent}")

                # 9-4. Detailed reviews collected
                detailed_reviews = self.extract_reviews()
                print(f"  [✓] Detailed_Reviews: {len(detailed_reviews) if detailed_reviews else 0} chars")

                # 9-5. Summarized_Review_Content (리뷰 페이지에서만 수집)
                summarized_review_content = self.extract_summarized_review_content_from_reviews_page(reviews_tree)
                print(f"  [✓] Summarized_Review_Content: {summarized_review_content[:50] if summarized_review_content else 'None'}...")

            # 9-4-1. detailed_reviews NULL 로그 기록 (count_of_reviews > 0인데 NULL인 경우)
            try:
                cor_int = int(str(count_of_reviews).replace(',', '')) if count_of_reviews else 0
            except:
                cor_int = 0

            if cor_int > 0 and not detailed_reviews:
                log_entry = {
                    'timestamp': datetime.now(self.korea_tz).strftime('%Y-%m-%d %H:%M:%S'),
                    'product_url': product_url,
                    'retailer_sku_name': retailer_sku_name[:80] if retailer_sku_name else 'N/A',
                    'count_of_reviews': count_of_reviews,
                    'star_rating': star_rating,
                    'reason': 'reviews_page_access_failed' if not top_mentions else 'extract_reviews_failed'
                }
                self.null_review_logs.append(log_entry)
                print(f"  [WARNING] detailed_reviews is NULL but count_of_reviews={count_of_reviews}")

            # 9-5. data 검증 (문제 감지 및 로깅)
            print(f"\n  [VALIDATION] Checking data quality...")
            self.validator.validate_item(item, product_url, 'bby_tv_dt1')
            self.validator.validate_screen_size(screen_size, product_url, 'bby_tv_dt1')
            self.validator.validate_price(final_sku_price, 'final_sku_price', product_url, 'bby_tv_dt1')
            if savings:  # savings는 없을 수도 있음
                self.validator.validate_price(savings, 'savings', product_url, 'bby_tv_dt1')
            if original_sku_price:  # original도 없을 수도 있음
                self.validator.validate_price(original_sku_price, 'original_sku_price', product_url, 'bby_tv_dt1')
            self.validator.validate_count(count_of_reviews, 'count_of_reviews', product_url, 'bby_tv_dt1')
            self.validator.validate_star_rating(star_rating, product_url, 'bby_tv_dt1')

            # 10. Detail DB save
            self.save_to_db(
                page_type=page_type,
                order=self.order,
                retailer_sku_name=retailer_sku_name,
                item=item,
                electricity_use=electricity_use,
                screen_size=screen_size,
                count_of_reviews=count_of_reviews,
                count_of_star_ratings=count_of_star_ratings,
                top_mentions=top_mentions,
                detailed_reviews=detailed_reviews,
                summarized_review_content=summarized_review_content,
                recommendation_intent=recommendation_intent,
                product_url=product_url,
                # 가격 정보는 crawling한 값 사용 (CHANGED)
                final_sku_price=final_sku_price,
                savings=savings,
                original_sku_price=original_sku_price,
                # 소스 table의 추가 data (가격 제외)
                offer=url_data['offer'],
                pick_up_availability=url_data['pick_up_availability'],
                shipping_availability=url_data['shipping_availability'],
                delivery_availability=url_data['delivery_availability'],
                sku_status=url_data['sku_status'],
                star_rating_source=star_rating,  # 메인 page에서 crawling한 값 사용 (CHANGED)
                promotion_type=url_data['promotion_type'],
                promotion_position=url_data['promotion_position'],
                bsr_rank=url_data['bsr_rank'],
                main_rank=url_data['main_rank'],
                trend_rank=url_data.get('trend_rank'),
                model_year=model_year,  # ADDED: model_year parameter
                sku=sku,  # ADDED: sku parameter for tv_item_mst
                similar_products=similar_products  # Similar products for retailer_sku_name_similar
            )

            # Increment total collected after successful save
            self.total_collected += 1

            return True

        except Exception as e:
            print(f"  [ERROR] detail page crawling failed: {e}")
            import traceback
            traceback.print_exc()
            return False

    def save_to_db(self, page_type, order, retailer_sku_name, item,
                   electricity_use, screen_size, count_of_reviews, count_of_star_ratings, top_mentions, detailed_reviews,
                   summarized_review_content, recommendation_intent, product_url,
                   final_sku_price, savings, original_sku_price, offer,
                   pick_up_availability, shipping_availability, delivery_availability,
                   sku_status, star_rating_source, promotion_type, promotion_position,
                   bsr_rank, main_rank, trend_rank, model_year, sku="no sku", similar_products=None):
        """DB에 save"""
        try:
            print(f"  [DB] Saving to database...")
            print(f"       Product: {retailer_sku_name[:60] if retailer_sku_name else 'N/A'}...")
            print(f"       Item (SKU): {item if item else 'N/A'}")

            cursor = self.db_conn.cursor()

            # Config에서 테이블명 가져오기
            detail_table = self.config.get_table('detail_data') or 'bby_tv_crawl'
            retail_com_table = self.config.get_table('retail_com') or 'tv_retail_com'
            item_mst_table = self.config.get_table('item_master') or 'tv_item_mst'
            account_name = self.config.get_constant('account_name', None, 'Bestbuy')

            # Calculate calendar week
            calendar_week = f"w{datetime.now().isocalendar().week}"

            # Calculate crawl_datetime (format: 2025-11-04 03:00:27)
            now = datetime.now()
            crawl_datetime = now.strftime('%Y-%m-%d %H:%M:%S')

            # If "Not yet reviewed", set both to 0
            if star_rating_source == "Not yet reviewed":
                count_of_star_ratings = 0
                count_of_reviews = 0

            # data 삽입
            insert_query = f"""
                INSERT INTO {detail_table}
                (account_name, batch_id, page_type, "order", retailer_sku_name, item,
                 Estimated_Annual_Electricity_Use, screen_size, count_of_reviews, Count_of_Star_Ratings, Top_Mentions,
                 Detailed_Review_Content, Recommendation_Intent, product_url, crawl_datetime, calendar_week,
                 final_sku_price, savings, original_sku_price, offer, pick_up_availability, shipping_availability,
                 delivery_availability, sku_status, star_rating, promotion_type, promotion_position,
                 bsr_rank, main_rank, trend_rank)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """

            cursor.execute(insert_query, (
                account_name,
                self.batch_id,
                page_type,
                order,
                retailer_sku_name,
                item,
                electricity_use,
                screen_size,
                count_of_reviews,
                count_of_star_ratings,
                top_mentions,
                detailed_reviews,
                recommendation_intent,
                product_url,
                crawl_datetime,
                calendar_week,
                final_sku_price,
                savings,
                original_sku_price,
                offer,
                pick_up_availability,
                shipping_availability,
                delivery_availability,
                sku_status,
                star_rating_source,
                promotion_type,
                promotion_position,
                bsr_rank,
                main_rank,
                trend_rank
            ))

            # Also insert into unified tv_retail_com table
            # Convert count_of_reviews to integer (remove commas if present)
            count_of_reviews_int = None
            if count_of_reviews is not None:
                try:
                    count_of_reviews_int = int(str(count_of_reviews).replace(',', ''))
                except:
                    count_of_reviews_int = None

            # Data validation: If star_rating is "Not yet reviewed", count_of_reviews must be 0
            if star_rating_source and "not yet reviewed" in str(star_rating_source).lower():
                if count_of_reviews_int != 0 and count_of_reviews_int is not None:
                    print(f"  [WARNING] Data inconsistency detected: star_rating='Not yet reviewed' but count_of_reviews={count_of_reviews_int}")
                    print(f"  [FIX] Setting count_of_reviews to 0")
                count_of_reviews_int = 0

            cursor.execute(f"""
                INSERT INTO {retail_com_table}
                (item, account_name, page_type, count_of_reviews, retailer_sku_name, product_url,
                 star_rating, count_of_star_ratings, screen_size, sku_popularity,
                 final_sku_price, original_sku_price, savings, discount_type, offer,
                 pick_up_availability, shipping_availability, delivery_availability, shipping_info,
                 available_quantity_for_purchase, inventory_status, sku_status, retailer_membership_discounts,
                 detailed_review_content, summarized_review_content, top_mentions, recommendation_intent,
                 main_rank, bsr_rank, trend_rank, rank_1, rank_2, promotion_position,
                 number_of_ppl_purchased_yesterday, number_of_ppl_added_to_carts, number_of_units_purchased_past_month, retailer_sku_name_similar,
                 estimated_annual_electricity_use, promotion_type, model_year,
                 calendar_week, crawl_datetime)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                item,
                account_name,  # account_name
                page_type,
                count_of_reviews_int,  # Converted to integer
                retailer_sku_name,
                product_url,
                star_rating_source,
                count_of_star_ratings,
                screen_size,
                None,  # sku_popularity (BestBuy doesn't have this)
                final_sku_price,
                original_sku_price,
                savings,
                None,  # discount_type (BestBuy doesn't have this)
                offer,
                pick_up_availability,
                shipping_availability,
                delivery_availability,
                None,  # shipping_info (BestBuy doesn't have this)
                None,  # available_quantity_for_purchase (BestBuy doesn't have this)
                None,  # inventory_status (BestBuy doesn't have this)
                sku_status,
                None,  # retailer_membership_discounts (BestBuy doesn't have this)
                detailed_reviews,
                summarized_review_content,  # AI 요약 리뷰
                top_mentions,
                recommendation_intent,
                main_rank,
                bsr_rank,
                trend_rank,
                None,  # rank_1 (BestBuy doesn't have this)
                None,  # rank_2 (BestBuy doesn't have this)
                promotion_position,
                None,  # number_of_ppl_purchased_yesterday (BestBuy doesn't have this)
                None,  # number_of_ppl_added_to_carts (BestBuy doesn't have this)
                None,  # number_of_units_purchased_past_month (BestBuy doesn't have this)
                (' ||| '.join([p.get('product_name', '') for p in similar_products if p.get('product_name')]) or None) if similar_products else None,  # retailer_sku_name_similar
                electricity_use,
                promotion_type,
                model_year,
                calendar_week,
                crawl_datetime
            ))

            # Insert into tv_item_mst (update screen_size and electricity_use on conflict)
            if item:
                cursor.execute(f"""
                    INSERT INTO {item_mst_table} (item, product_url, sku, account_name, screen_size, estimated_annual_electricity_use)
                    VALUES (%s, %s, %s, %s, %s, %s)
                    ON CONFLICT (item) DO UPDATE SET
                        screen_size = COALESCE({item_mst_table}.screen_size, EXCLUDED.screen_size),
                        estimated_annual_electricity_use = COALESCE({item_mst_table}.estimated_annual_electricity_use, EXCLUDED.estimated_annual_electricity_use)
                """, (item, product_url, sku, account_name, screen_size, electricity_use))
                print(f"  [DB] ✓ {item_mst_table} upsert (item: {item}, sku: {sku}, screen_size: {screen_size}, electricity: {electricity_use})")

            cursor.close()
            print(f"  [DB] ✓ Successfully saved to {detail_table} + {retail_com_table} + {item_mst_table}")
            return True

        except Exception as e:
            # 모든 에러를 명확하게 출력 (중복 키 포함)
            if 'duplicate key' in str(e):
                print(f"  [WARNING] Duplicate key - product already exists in DB")
                print(f"            URL: {product_url[:80] if product_url else 'N/A'}...")
            else:
                print(f"  [ERROR] DB save failed: {e}")
                import traceback
                traceback.print_exc()
            return False

    def cleanup_old_logs(self, log_dir, days=30):
        """30일 지난 로그 파일 삭제"""
        try:
            if not os.path.exists(log_dir):
                return
            cutoff_time = time.time() - (days * 24 * 60 * 60)
            deleted_count = 0
            for filename in os.listdir(log_dir):
                if filename.startswith('bby_tv_dt1_') and filename.endswith('.txt'):
                    file_path = os.path.join(log_dir, filename)
                    if os.path.getmtime(file_path) < cutoff_time:
                        os.remove(file_path)
                        deleted_count += 1
            if deleted_count > 0:
                print(f"[INFO] Deleted {deleted_count} old log files (older than {days} days)")
        except Exception as e:
            print(f"[WARNING] Failed to cleanup old logs: {e}")

    def run(self, start_from=0):
        """메인 execution. start_from: 0-based index to skip already-processed URLs"""
        # 콘솔 출력을 파일로도 저장
        log_dir = r"C:\samsung_dx_retail_com\log"
        if not os.path.exists(log_dir):
            os.makedirs(log_dir)
        self.cleanup_old_logs(log_dir)
        log_filename = f"bby_tv_dt1_{self.batch_id}.txt"
        log_path = os.path.join(log_dir, log_filename)
        self.tee = Tee(log_path)

        try:
            print("="*80)
            print(f"Best Buy TV Detail Page Crawler (DrissionPage) (Batch ID: {self.batch_id})")
            print("="*80)

            # DB connection
            if not self.connect_db():
                return

            # URLs 가져오기
            urls = self.get_recent_urls()
            if not urls:
                print("[ERROR] No URLs found")
                return

            # 브라우저 설정 (DrissionPage)
            if not self.setup_browser():
                return

            # 각 URL crawling
            success_count = 0
            if start_from > 0:
                urls = urls[start_from:]
                print(f"[INFO] Skipped first {start_from} URLs, remaining: {len(urls)}")

            # 자동 재시도 설정
            MAX_RETRIES = 5          # 최대 재시도 횟수
            INITIAL_WAIT = 600       # 초기 대기 시간 (10분)
            retry_count = 0          # 현재 재시도 횟수
            consecutive_fails = 0    # 연속 실패 횟수

            i = 0
            while i < len(urls):
                url_data = urls[i]
                idx = i + start_from + 1

                # Check if we've reached the maximum SKU limit
                if self.total_collected >= self.max_skus:
                    print(f"\n{'='*80}")
                    print(f"[INFO] Reached maximum SKU limit ({self.max_skus})")
                    print(f"[INFO] Stopping collection. Total collected: {self.total_collected}")
                    break

                result = self.scrape_detail_page(url_data)

                if result == 'blocked':
                    # ERR_HTTP2_PROTOCOL_ERROR 감지 - 재시도 로직
                    retry_count += 1
                    if retry_count > MAX_RETRIES:
                        print(f"\n{'='*80}")
                        print(f"[ERROR] Max retries ({MAX_RETRIES}) exceeded. Stopping.")
                        print(f"[INFO] Resume later with: python bby_tv_dt1.py {idx}")
                        break

                    wait_time = INITIAL_WAIT * retry_count  # 5분, 10분, 15분, 20분, 25분
                    print(f"\n{'='*80}")
                    print(f"[RETRY {retry_count}/{MAX_RETRIES}] Blocked by Best Buy. Waiting {wait_time // 60} minutes...")
                    print(f"[INFO] Will restart browser and retry item {idx}")
                    print(f"{'='*80}")

                    # 브라우저 종료
                    self.close_browser()

                    # 대기
                    time.sleep(wait_time)

                    # DB 커넥션 체크
                    if not self.check_db_connection():
                        print("[ERROR] DB reconnection failed. Stopping.")
                        break

                    # 브라우저 새로 시작
                    if not self.setup_browser():
                        print("[ERROR] Browser restart failed. Stopping.")
                        break

                    # 같은 URL 다시 시도 (i 증가 안 함)
                    self.order -= 1  # order 카운터 복원 (scrape_detail_page에서 증가했으므로)
                    continue

                elif result is True:
                    success_count += 1
                    consecutive_fails = 0
                    retry_count = 0  # 성공하면 재시도 카운터 리셋

                else:
                    # 일반 실패 (blocked 아님)
                    consecutive_fails += 1
                    # 1회 실패 시 바로 blocked로 판단
                    if consecutive_fails >= 1:
                        retry_count += 1
                        if retry_count > MAX_RETRIES:
                            print(f"\n{'='*80}")
                            print(f"[ERROR] Max retries ({MAX_RETRIES}) exceeded. Stopping.")
                            print(f"[INFO] Resume later with: python bby_tv_dt1.py {idx}")
                            break

                        wait_time = INITIAL_WAIT * retry_count
                        print(f"\n{'='*80}")
                        print(f"[RETRY {retry_count}/{MAX_RETRIES}] {consecutive_fails} consecutive failures. Possible block detected.")
                        print(f"[INFO] Waiting {wait_time // 60} minutes before retry...")
                        print(f"{'='*80}")

                        self.close_browser()
                        time.sleep(wait_time)

                        if not self.check_db_connection():
                            print("[ERROR] DB reconnection failed. Stopping.")
                            break

                        if not self.setup_browser():
                            print("[ERROR] Browser restart failed. Stopping.")
                            break

                        consecutive_fails = 0
                        # 실패한 항목 다시 시도 (i 증가 안 함)
                        self.order -= 1
                        continue

                # page 간 딜레이
                time.sleep(random.uniform(5, 10))
                i += 1

            print("\n" + "="*80)
            print(f"crawling complete! successful: {success_count}/{len(urls)}items")
            print("="*80)

            # NULL detailed_review_content 로그 파일 저장
            if self.null_review_logs:
                log_date = datetime.now(self.korea_tz).strftime('%y%m%d')
                log_filename = f"null_detailed_review_content_{log_date}.txt"
                log_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), log_filename)

                with open(log_path, 'w', encoding='utf-8') as f:
                    f.write(f"NULL Detailed Review Content Log - {datetime.now(self.korea_tz).strftime('%Y-%m-%d %H:%M:%S')}\n")
                    f.write(f"Total: {len(self.null_review_logs)} products\n")
                    f.write("="*100 + "\n\n")

                    for i, log in enumerate(self.null_review_logs, 1):
                        f.write(f"[{i}] {log['timestamp']}\n")
                        f.write(f"    URL: {log['product_url']}\n")
                        f.write(f"    Product: {log['retailer_sku_name']}\n")
                        f.write(f"    Count of Reviews: {log['count_of_reviews']}\n")
                        f.write(f"    Star Rating: {log['star_rating']}\n")
                        f.write(f"    Reason: {log['reason']}\n")
                        f.write("-"*100 + "\n")

                print(f"\n[INFO] NULL detailed_review_content log saved: {log_path}")
                print(f"       Total products with NULL reviews: {len(self.null_review_logs)}")

            # data 검증 요약 출력
            summary = self.validator.get_summary()
            if summary['total'] > 0:
                print("\n" + "="*80)
                print("DATA VALIDATION SUMMARY")
                print("="*80)
                print(f"Total Issues Detected: {summary['total']}")
                for issue_type, count in sorted(summary['by_type'].items()):
                    print(f"  {issue_type}: {count}")
                print(f"\nLog file: C:\\samsung_dx_retail_com\\problems\\{self.validator.session_start_time}.txt")
                print("="*80)
                self.validator.write_summary()
            else:
                print("\n[OK] No data quality issues detected")

            # Send alert email
            try:
                cursor = self.db_conn.cursor()
                retail_com_table = self.config.get_table('retail_com') or 'tv_retail_com'
                account_name = self.config.get_constant('account_name', None, 'Bestbuy')
                cursor.execute(f"""
                    SELECT retailer_sku_name, star_rating, count_of_star_ratings, count_of_reviews,
                           screen_size, sku_popularity, final_sku_price, original_sku_price,
                           savings, discount_type, offer, pick_up_availability,
                           shipping_availability, delivery_availability, shipping_info,
                           available_quantity_for_purchase, inventory_status, sku_status,
                           retailer_membership_discounts, detailed_review_content, summarized_review_content,
                           top_mentions, recommendation_intent, main_rank, bsr_rank, trend_rank,
                           rank_1, rank_2, promotion_position, number_of_ppl_purchased_yesterday,
                           number_of_ppl_added_to_carts, number_of_units_purchased_past_month,
                           retailer_sku_name_similar, estimated_annual_electricity_use, promotion_type, model_year
                    FROM {retail_com_table}
                    WHERE account_name = %s
                    AND crawl_datetime::timestamp >= NOW() - INTERVAL '4 hours'
                """, (account_name,))
                rows = cursor.fetchall()
                columns = [
                    'retailer_sku_name', 'star_rating', 'count_of_star_ratings', 'count_of_reviews',
                    'screen_size', 'sku_popularity', 'final_sku_price', 'original_sku_price',
                    'savings', 'discount_type', 'offer', 'pick_up_availability',
                    'shipping_availability', 'delivery_availability', 'shipping_info',
                    'available_quantity_for_purchase', 'inventory_status', 'sku_status',
                    'retailer_membership_discounts', 'detailed_review_content', 'summarized_review_content',
                    'top_mentions', 'recommendation_intent', 'main_rank', 'bsr_rank', 'trend_rank',
                    'rank_1', 'rank_2', 'promotion_position', 'number_of_ppl_purchased_yesterday',
                    'number_of_ppl_added_to_carts', 'number_of_units_purchased_past_month',
                    'retailer_sku_name_similar', 'estimated_annual_electricity_use', 'promotion_type', 'model_year'
                ]
                results_df = pd.DataFrame(rows, columns=columns)
                cursor.close()

                monitor_and_alert('bestbuy', len(urls), results_df,
                                 screen_size_mismatch_records=self.screen_size_mismatch_records,
                                 electricity_use_mismatch_records=self.electricity_use_mismatch_records)
            except Exception as e:
                print(f"[WARNING] Failed to send alert: {e}")

        except Exception as e:
            print(f"[ERROR] crawler execution error: {e}")
            import traceback
            traceback.print_exc()

        finally:
            self.close_browser()
            if self.db_conn:
                self.db_conn.close()
                print("[INFO] DB connection closed")
            if self.tee:
                log_path = self.tee.file.name
                print(f"\n[INFO] Console log saved: {log_path}")
                self.tee.close()

def main():
    # Parse start_from argument: python bby_tv_dt1.py 39
    start_from = 0
    if len(sys.argv) > 1:
        try:
            start_from = int(sys.argv[1]) - 1  # user provides 1-based, convert to 0-based
            if start_from < 0:
                start_from = 0
            print(f"[INFO] Start from: {start_from + 1} (skipping first {start_from})")
        except ValueError:
            print(f"[WARNING] Invalid start argument '{sys.argv[1]}', starting from 1")

    crawler = BestBuyDetailCrawler()
    crawler.run(start_from=start_from)

if __name__ == "__main__":
    main()
