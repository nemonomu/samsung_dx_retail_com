"""
Best Buy TV Detail Page Crawler (Modified v1)
collected table: bby_tv_main1, bby_tv_bsr1, bby_tv_pmt1 (trend_crawl NOT used)
save table: bby_tv_crawl, bby_tv_mst

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
import psycopg2
from datetime import datetime
import pytz
import undetected_chromedriver as uc
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException
from lxml import html
from data_validator import DataValidator

# Import database configuration
from config import DB_CONFIG

class BestBuyDetailCrawler:
    def __init__(self):
        self.driver = None
        self.db_conn = None
        self.korea_tz = pytz.timezone('Asia/Seoul')
        self.batch_id = datetime.now(self.korea_tz).strftime('%Y%m%d_%H%M%S')
        self.order = 0

        # Data validator sec기화
        session_start_time = os.environ.get('SESSION_START_TIME', datetime.now().strftime('%Y%m%d%H%M'))
        self.validator = DataValidator(session_start_time)

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

    def setup_driver(self):
        """Chrome driver setup"""
        try:
            print("[INFO] Setting up Chrome driver...")

            # Chrome options with page load strategy
            options = uc.ChromeOptions()
            options.page_load_strategy = 'eager'  # Wait for DOM load (CHANGED from 'none')

            self.driver = uc.Chrome(options=options)
            self.driver.set_page_load_timeout(120)  # Increased to 120 seconds
            self.driver.maximize_window()

            print("[OK] Driver setup complete (page_load_strategy=eager, timeout=120s)")
            return True
        except Exception as e:
            print(f"[ERROR] Driver setup failed: {e}")
            return False

    def get_recent_urls(self):
        """최신 batch_id의 product URLs와 추가 data 가져오기"""
        try:
            cursor = self.db_conn.cursor()
            urls = []

            # bestbuy_tv_main_crawl에서 최신 batch_id 가져오기
            cursor.execute("""
                SELECT batch_id
                FROM bby_tv_main1
                WHERE batch_id IS NOT NULL
                ORDER BY batch_id DESC
                LIMIT 1
            """)
            main_batch_result = cursor.fetchone()
            main_batch_id = main_batch_result[0] if main_batch_result else None

            # bby_tv_Trend_crawl 테이블은 사용하지 않음 (trend crawler 없음)
            # cursor.execute("""
            #     SELECT batch_id
            #     FROM bby_tv_Trend_crawl
            #     WHERE batch_id IS NOT NULL
            #     ORDER BY batch_id DESC
            #     LIMIT 1
            # """)
            # trend_batch_result = cursor.fetchone()
            # trend_batch_id = trend_batch_result[0] if trend_batch_result else None
            trend_batch_id = None  # Trend crawler not used

            # bby_tv_promotion_crawl에서 최신 batch_id 가져오기
            cursor.execute("""
                SELECT batch_id
                FROM bby_tv_pmt1
                WHERE batch_id IS NOT NULL
                ORDER BY batch_id DESC
                LIMIT 1
            """)
            promo_batch_result = cursor.fetchone()
            promo_batch_id = promo_batch_result[0] if promo_batch_result else None

            # bby_tv_bsr_crawl에서 최신 batch_id 가져오기
            cursor.execute("""
                SELECT batch_id
                FROM bby_tv_bsr1
                WHERE batch_id IS NOT NULL
                ORDER BY batch_id DESC
                LIMIT 1
            """)
            bsr_batch_result = cursor.fetchone()
            bsr_batch_id = bsr_batch_result[0] if bsr_batch_result else None

            print(f"[INFO] Latest batch_id - Main: {main_batch_id}, BSR: {bsr_batch_id}, Promotion: {promo_batch_id}")

            # collected 순서: main → bsr → promotion → trend (우선순위 순서)
            # 각 table의 rank 순서대로 정렬
            # 중복 URL은 rank 정보 병합 (crawling은 한 번만)

            # Dictionary to store merged URL data: {url: {page_type, ranks, data...}}
            url_data_map = {}

            # 1. bestbuy_tv_main_crawl에서 해당 batch의 URLs와 data 가져오기
            if main_batch_id:
                cursor.execute("""
                    SELECT DISTINCT product_url, offer,
                           pick_up_availability, shipping_availability, delivery_availability,
                           sku_status, main_rank
                    FROM bby_tv_main1
                    WHERE batch_id = %s
                    AND product_url IS NOT NULL
                    ORDER BY main_rank
                """, (main_batch_id,))
                main_urls = cursor.fetchall()
                for row in main_urls:
                    url = row[0]
                    if url not in url_data_map:
                        url_data_map[url] = {
                            'page_type': 'main',
                            'product_url': url,
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
                            'promotion_rank': None,
                            'promotion_type': None
                        }
                print(f"[OK] Main URLs (batch {main_batch_id}): {len(main_urls)} items")

            # 2. bby_tv_bsr_crawl에서 해당 batch의 URLs와 data 가져오기
            if bsr_batch_id:
                cursor.execute("""
                    SELECT DISTINCT product_url, offer,
                           pick_up_availability, shipping_availability, delivery_availability,
                           sku_status, bsr_rank
                    FROM bby_tv_bsr1
                    WHERE batch_id = %s
                    AND product_url IS NOT NULL
                    ORDER BY bsr_rank
                """, (bsr_batch_id,))
                bsr_urls = cursor.fetchall()
                for row in bsr_urls:
                    url = row[0]
                    if url in url_data_map:
                        # URL already exists - just add bsr_rank
                        url_data_map[url]['bsr_rank'] = row[6]
                    else:
                        # New URL from bsr
                        url_data_map[url] = {
                            'page_type': 'bsr',
                            'product_url': url,
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
                            'promotion_rank': None,
                            'promotion_type': None
                        }
                print(f"[OK] BSR URLs (batch {bsr_batch_id}): {len(bsr_urls)} items")

            # 3. bby_tv_promotion_crawl에서 해당 batch의 URLs와 data 가져오기
            if promo_batch_id:
                cursor.execute("""
                    SELECT DISTINCT product_url, offer, promotion_type, promotion_rank
                    FROM bby_tv_pmt1
                    WHERE batch_id = %s
                    AND product_url IS NOT NULL
                    ORDER BY promotion_rank
                """, (promo_batch_id,))
                promo_urls = cursor.fetchall()
                for row in promo_urls:
                    url = row[0]
                    if url in url_data_map:
                        # URL already exists - just add promotion_rank and promotion_type
                        url_data_map[url]['promotion_rank'] = row[3]
                        url_data_map[url]['promotion_type'] = row[2]
                    else:
                        # New URL from promotion
                        url_data_map[url] = {
                            'page_type': 'promotion',
                            'product_url': url,
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
                            'promotion_rank': row[3],
                            'promotion_type': row[2]
                        }
                print(f"[OK] Promotion URLs (batch {promo_batch_id}): {len(promo_urls)} items")

            # 4. bby_tv_Trend_crawl에서 해당 batch의 URLs와 data 가져오기 (DISABLED - no trend crawler)
            # if trend_batch_id:
            #     cursor.execute("""
            #         SELECT DISTINCT product_url, rank
            #         FROM bby_tv_Trend_crawl
            #         WHERE batch_id = %s
            #         AND product_url IS NOT NULL
            #         ORDER BY rank
            #     """, (trend_batch_id,))
            #     trend_urls = cursor.fetchall()
            #     for row in trend_urls:
            #         url = row[0]
            #         if url in url_data_map:
            #             # URL already exists - just add trend_rank
            #             url_data_map[url]['trend_rank'] = row[1]
            #         else:
            #             # New URL from trend
            #             url_data_map[url] = {
            #                 'page_type': 'Trend',
            #                 'product_url': url,
            #                 'final_sku_price': None,
            #                 'savings': None,
            #                 'original_sku_price': None,
            #                 'offer': None,
            #                 'pick_up_availability': None,
            #                 'shipping_availability': None,
            #                 'delivery_availability': None,
            #                 'sku_status': None,
            #                 'star_rating': None,
            #                 'main_rank': None,
            #                 'bsr_rank': None,
            #                 'trend_rank': row[1],
            #                 'promotion_rank': None,
            #                 'promotion_type': None
            #             }
            #     print(f"[OK] Trend URLs (batch {trend_batch_id}): {len(trend_urls)} items")

            cursor.close()

            # Convert dictionary to list (maintains insertion order: main, bsr, promotion, trend)
            unique_urls = list(url_data_map.values())

            # Count duplicates
            total_loaded = 0
            if main_batch_id:
                cursor = self.db_conn.cursor()
                cursor.execute("SELECT COUNT(*) FROM bby_tv_main1 WHERE batch_id = %s", (main_batch_id,))
                total_loaded += cursor.fetchone()[0]
                cursor.close()
            if bsr_batch_id:
                cursor = self.db_conn.cursor()
                cursor.execute("SELECT COUNT(*) FROM bby_tv_bsr1 WHERE batch_id = %s", (bsr_batch_id,))
                total_loaded += cursor.fetchone()[0]
                cursor.close()
            if promo_batch_id:
                cursor = self.db_conn.cursor()
                cursor.execute("SELECT COUNT(*) FROM bby_tv_pmt1 WHERE batch_id = %s", (promo_batch_id,))
                total_loaded += cursor.fetchone()[0]
                cursor.close()
            # Trend crawler not used
            # if trend_batch_id:
            #     cursor = self.db_conn.cursor()
            #     cursor.execute("SELECT COUNT(*) FROM bby_tv_Trend_crawl WHERE batch_id = %s", (trend_batch_id,))
            #     total_loaded += cursor.fetchone()[0]
            #     cursor.close()

            duplicates_count = total_loaded - len(unique_urls)
            if duplicates_count > 0:
                print(f"[INFO] Found {duplicates_count} duplicate URLs - rank information merged")

            print(f"[OK] Loaded {len(unique_urls)} unique URLs (before dedup: {total_loaded} items)")
            return unique_urls

        except Exception as e:
            print(f"[ERROR] Failed to load URLs: {e}")
            import traceback
            traceback.print_exc()
            return []

    def extract_retailer_sku_name(self, tree):
        """Retailer_SKU_Name extraction"""
        try:
            xpaths = [
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
        """Specification button click"""
        try:
            print("  [INFO] Specification button click...")
            # XPath를 사용한 여러 attempt
            xpaths = [
                "//button[@class='c-button-unstyled specs-accordion font-weight-medium w-full flex justify-content-between align-items-center CiN3vihE2Ub2POwD']",
                "//button[.//h3[text()='Specifications']]",
                "//button[contains(@class, 'specs-accordion')]"
            ]

            for xpath in xpaths:
                try:
                    spec_button = self.driver.find_element(By.XPATH, xpath)
                    self.driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", spec_button)
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
        Specifications dialog 열기 (retry 포함)

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
                    wait = WebDriverWait(self.driver, wait_time)
                    wait.until(EC.presence_of_element_located(
                        (By.XPATH, '//div[contains(text(), "Model Number")]')
                    ))
                    print(f"  [OK] dialog load complete (wait: {wait_time}sec)")
                    return True, None

                except TimeoutException:
                    if retry_count < max_retries:
                        print(f"  [WARNING] dialog timeout, retry {retry_count + 1}/{max_retries}...")
                        retry_count += 1
                        self.close_specifications_dialog()
                        time.sleep(2)
                        continue
                    else:
                        print(f"  [ERROR] dialog timeout (retry failed)")
                        return False, 'dialog_timeout'
            else:
                print(f"  [WARNING] Specifications button click failed")
                return False, 'click_failed'

        return False, 'dialog_timeout'

    def extract_item(self, tree):
        """Item (Model Number) extraction"""
        try:
            # dialog에서 Model Number 찾기 (여러 패턴 attempt)
            xpaths = [
                # 새로운 패턴
                '//div[contains(@class, "dB7j8sHUbncyf79K")]//div[contains(text(), "Model Number")]/following-sibling::div[@class="grow basis-none pl-300"]',
                # 기존 패턴
                '//li[.//h4[text()="General"]]//div[.//div[text()="Model Number"]]//div[@class="grow basis-none pl-300"]',
                '//div[contains(text(), "Model Number")]/following-sibling::div[@class="grow basis-none pl-300"]',
                # 더 넓은 패턴
                '//div[text()="Model Number"]/..//div[@class="grow basis-none pl-300"]',
                '//div[contains(., "Model Number")]//div[contains(@class, "pl-300")]'
            ]
            for xpath in xpaths:
                elem = tree.xpath(xpath)
                if elem:
                    model_number = elem[0].text_content().strip()
                    if model_number:
                        return model_number
            return None
        except Exception as e:
            print(f"  [ERROR] Item extraction failed: {e}")
            return None

    def extract_electricity_use(self, tree):
        """Estimated_Annual_Electricity_Use extraction (숫자만)"""
        try:
            # dialog에서 Estimated Annual Electricity Use 찾기 (여러 패턴 attempt)
            xpaths = [
                # 새로운 패턴
                '//div[contains(@class, "dB7j8sHUbncyf79K")]//div[contains(text(), "Estimated Annual Electricity Use")]/following-sibling::div[@class="grow basis-none pl-300"]',
                # 기존 패턴
                '//li[.//h4[text()="Power"]]//div[.//div[contains(text(), "Estimated Annual Electricity Use")]]//div[@class="grow basis-none pl-300"]',
                '//div[contains(text(), "Estimated Annual Electricity Use")]/following-sibling::div[@class="grow basis-none pl-300"]',
                # 더 넓은 패턴
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
            xpaths = [
                # 제공된 HTML 패턴
                '/html/body/div[5]/div[4]/div[2]/div/div[3]/div[1]/button[2]/div/div/div[2]',
                # 더 범용적인 패턴
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

    def extract_final_sku_price(self, tree):
        """Final SKU Price extraction (현재 판매 가격) - 컨테이너 기반"""
        try:
            # 1단계: 가격 컨테이너 찾기
            container_xpaths = [
                # 절대 경로 (제공된 것)
                '/html/body/div[5]/div[4]/div[1]',
                # class 기반
                '//div[@class="order-2 t3V0AOwowrTfUzPn "]',
                # 컨테이너 구조 기반 (더 넓은 패턴)
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
            price_xpaths = [
                # data-testid 기반 (가장 안정적)
                './/div[@data-testid="price-block-customer-price"]//span',
                './/div[@data-lu-target="customer_price"]//span',
                # class 기반 fallback
                './/span[@class="font-sans text-default text-style-body-md-400 font-500 text-7 leading-7"]'
            ]

            for xpath in price_xpaths:
                elem = price_container.xpath(xpath)
                if elem:
                    price = elem[0].text_content().strip()
                    # "$" 기호가 포함되어 있고 유효한 가격인지 확인
                    if price and '$' in price:
                        return price  # "$89.99" 형식 반환
            return None
        except Exception as e:
            print(f"  [ERROR] Final_SKU_Price extraction failed: {e}")
            return None

    def extract_original_sku_price(self, tree):
        """Original SKU Price extraction (세일 전 원가) - 컨테이너 기반"""
        try:
            # 1단계: 가격 컨테이너 찾기
            container_xpaths = [
                # 절대 경로 (제공된 것)
                '/html/body/div[5]/div[4]/div[1]',
                # class 기반
                '//div[@class="order-2 t3V0AOwowrTfUzPn "]',
                # 컨테이너 구조 기반
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
            price_xpaths = [
                # data-lu-target 기반 (가장 안정적)
                './/span[@data-lu-target="comp_value"]',
                # data-testid 기반
                './/span[@data-testid="price-block-regular-price-message-text"]//span[@data-lu-target="comp_value"]',
                # 회색 작은 글씨 (원가 특징)
                './/span[contains(@style, "color: rgb(108, 111, 117)") and contains(., "$")]'
            ]

            for xpath in price_xpaths:
                elem = price_container.xpath(xpath)
                if elem:
                    price = elem[0].text_content().strip()
                    # "$" 기호가 포함되어 있고 유효한 가격인지 확인
                    if price and '$' in price:
                        return price  # "$149.99" 형식 반환
            return None  # 세일이 아니면 None
        except Exception as e:
            print(f"  [ERROR] Original_SKU_Price extraction failed: {e}")
            return None

    def extract_savings(self, tree):
        """Savings extraction (할인 금액) - 컨테이너 기반"""
        try:
            # 1단계: 가격 컨테이너 찾기
            container_xpaths = [
                # 절대 경로 (제공된 것)
                '/html/body/div[5]/div[4]/div[1]',
                # class 기반
                '//div[@class="order-2 t3V0AOwowrTfUzPn "]',
                # 컨테이너 구조 기반
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
            savings_xpaths = [
                # data-testid 기반 (가장 안정적)
                './/span[@data-testid="price-block-total-savings-text"]',
                # data-testid 컨테이너 기반
                './/div[@data-testid="price-block-total-savings"]//span',
                # 빨간색 "Save" 텍스트
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
        """Star Rating extraction (평점 점수) - 컨테이너 기반"""
        try:
            # 1단계: 가격 컨테이너 찾기
            container_xpaths = [
                # 절대 경로
                '/html/body/div[5]/div[4]/div[1]',
                # class 기반
                '//div[@class="order-2 t3V0AOwowrTfUzPn "]',
                # 컨테이너 구조 기반
                '//div[contains(@class, "order-2")]'
            ]

            price_container = None
            for xpath in container_xpaths:
                containers = tree.xpath(xpath)
                if containers:
                    price_container = containers[0]
                    break

            if price_container is None or len(price_container) == 0:
                # 컨테이너 없으면 None 반환 (review가 없는 상품일 수 있음)
                return None

            # 2단계: 컨테이너 내부에서만 평점 extraction
            rating_xpaths = [
                # 절대 경로 기반 (컨테이너 내부)
                './/div/div[3]/a/div/span[1]',
                # class 기반 (가장 안정적)
                './/span[@class="font-weight-medium  font-weight-bold order-1"]',
                './/span[contains(@class, "font-weight-bold") and contains(@class, "order-1")]',
                # aria-hidden 속성 기반
                './/span[@aria-hidden="true"][contains(@class, "order-1")]'
            ]

            for xpath in rating_xpaths:
                elem = price_container.xpath(xpath)
                if elem:
                    rating = elem[0].text_content().strip()
                    # 평점 형식 검증 (숫자.숫자 형식)
                    if rating and re.match(r'^\d+\.\d+$', rating):
                        return rating  # "4.7" 형식 반환
            return None  # review가 없으면 None
        except Exception as e:
            print(f"  [ERROR] Star_Rating extraction failed: {e}")
            return None

    def extract_count_of_reviews_from_detail(self, tree):
        """Count of Reviews extraction (메인 detail page에서) - 컨테이너 기반"""
        try:
            # 1단계: 가격 컨테이너 찾기
            container_xpaths = [
                # 절대 경로
                '/html/body/div[5]/div[4]/div[1]',
                # class 기반
                '//div[@class="order-2 t3V0AOwowrTfUzPn "]',
                # 컨테이너 구조 기반
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

            # 2단계: 컨테이너 내부에서만 review items count extraction
            reviews_xpaths = [
                # 절대 경로 기반 (컨테이너 내부)
                './/div/div[3]/a/div/span[2]',
                # class 기반 (가장 안정적)
                './/span[@class="c-reviews order-2"]',
                './/span[contains(@class, "c-reviews")]',
                # aria-hidden 속성 기반
                './/span[@aria-hidden="true"][contains(@class, "order-2")]'
            ]

            for xpath in reviews_xpaths:
                elem = price_container.xpath(xpath)
                if elem:
                    text = elem[0].text_content().strip()  # "(79 reviews)" or "(1,234 reviews)"
                    # 숫자 extraction (콤마 제거)
                    # 패턴: (숫자,숫자 reviews) → 숫자만 extraction
                    match = re.search(r'\(([\d,]+)\s*reviews?\)', text, re.IGNORECASE)
                    if match:
                        # 콤마 제거 후 반환: "1,234" → "1234"
                        count = match.group(1).replace(',', '')
                        return count  # "79" or "1234" 형식 반환
            return None  # review가 없으면 None
        except Exception as e:
            print(f"  [ERROR] Count_of_Reviews extraction failed: {e}")
            return None

    def close_specifications_dialog(self):
        """Specification dialog close"""
        try:
            print("  [INFO] Specification dialog close...")
            xpaths = [
                '//button[@data-testid="brix-sheet-closeButton"]',
                '//button[@aria-label="Close Sheet"]',
                '//div[@class="relative"]//button'
            ]

            for xpath in xpaths:
                try:
                    close_button = self.driver.find_element(By.XPATH, xpath)
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
            name_elements = tree.xpath('//span[@class="clamp" and starts-with(@id, "compare-title-")]')
            for elem in name_elements[:4]:  # 최대 4items
                similar_names.append(elem.text_content().strip())

            # Pros extraction
            pros_elements = tree.xpath('//tr[@class="flex"]//td[.//svg[@aria-label="Advantage Icon"]]//span[@class="text-3 min-w-0 flex flex-wrap"]')
            for elem in pros_elements[:4]:  # 최대 4items
                pros_list.append(elem.text_content().strip())

            # Cons extraction
            cons_elements = tree.xpath('//tr[@class="flex"]//td[.//svg[@aria-label="Disadvantage Icon"]]//span[@class="text-3 min-w-0 flex flex-wrap"]')
            for elem in cons_elements[:4]:  # 최대 4items
                text = elem.text_content().strip()
                if text and text != '—':
                    cons_list.append(text)
                else:
                    cons_list.append(None)

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

    def extract_star_ratings_from_reviews_page(self):
        """Count_of_Star_Ratings extraction (See All Customer Reviews page에서)"""
        try:
            time.sleep(3)  # page 로딩 wait
            ratings = {}
            # XPath 패턴 (5점부터 1점까지)
            xpaths = [
                '//*[@id="reviews-accordion"]/section/div[1]/div[1]/div/div/div[2]/div/fieldset/div[1]/div/label/span[5]',  # 5점
                '//*[@id="reviews-accordion"]/section/div[1]/div[1]/div/div/div[2]/div/fieldset/div[2]/div/label/span[5]',  # 4점
                '//*[@id="reviews-accordion"]/section/div[1]/div[1]/div/div/div[2]/div/fieldset/div[3]/div/label/span[5]',  # 3점
                '//*[@id="reviews-accordion"]/section/div[1]/div[1]/div/div/div[2]/div/fieldset/div[4]/div/label/span[5]',  # 2점
                '//*[@id="reviews-accordion"]/section/div[1]/div[1]/div/div/div[2]/div/fieldset/div[5]/div/label/span[5]'   # 1점
            ]

            # 5점부터 1점까지 순서로 extraction
            for idx, xpath in enumerate(xpaths):
                star = 5 - idx  # 5, 4, 3, 2, 1
                try:
                    elem = self.driver.find_element(By.XPATH, xpath)
                    count = elem.text.strip()
                    # 1star는 단수형, 나머지는 복수형
                    key = f"{star}star" if star == 1 else f"{star}stars"
                    ratings[key] = count
                except Exception:
                    # 찾지 못하면 0으로 설정
                    key = f"{star}star" if star == 1 else f"{star}stars"
                    ratings[key] = "0"

            # 형식: "5stars:9 4stars:1 3stars:0 2stars:0 1star:2" (공백으로 구분)
            rating_str = " ".join([f"{k}:{v}" for k, v in ratings.items()])
            return rating_str if rating_str else None

        except Exception as e:
            print(f"  [ERROR] Star ratings extraction failed: {e}")
            return None

    def extract_count_of_reviews(self):
        """Count_of_Reviews extraction (See All Customer Reviews page에서)"""
        try:
            # XPath 패턴
            xpaths = [
                # 제공된 HTML 패턴
                '//span[@class="c-reviews order-2"]',
                # ID 기반 패턴 (동적 ID이므로 contains 사용)
                '//div[contains(@id, "user-generated-content-ugc-stats")]//span[@class="c-reviews order-2"]',
                # 더 범용적인 패턴
                '//span[contains(@class, "c-reviews")]'
            ]

            for xpath in xpaths:
                try:
                    elem = self.driver.find_element(By.XPATH, xpath)
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

    def extract_top_mentions_from_reviews_page(self):
        """Top_Mentions extraction (See All Customer Reviews page에서)"""
        try:
            # XPath 패턴 (ID가 동적이므로 class 기반으로 찾기)
            xpaths = [
                # "Highly rated by customers for" section의 span.text-nowrap들
                '//div[contains(@class, "customer-review-pros-stats")]//span[@class="text-nowrap"]',
                # 더 넓은 패턴
                '//div[contains(., "Highly rated by customers for")]//span[@class="text-nowrap"]'
            ]

            mentions = []
            for xpath in xpaths:
                try:
                    elements = self.driver.find_elements(By.XPATH, xpath)
                    if elements:
                        for elem in elements:
                            text = elem.text.strip()
                            # 콤마나 기타 불필요한 문자 제거
                            text = text.replace(',', '').strip()
                            if text:
                                mentions.append(text)
                        break
                except Exception:
                    continue

            if mentions:
                # first 번째 항목만 반환 (예: "Picture Quality")
                return mentions[0]

            return None

        except Exception as e:
            print(f"  [ERROR] Top mentions extraction failed: {e}")
            return None

    def click_see_all_reviews(self):
        """See All Customer Reviews button click"""
        try:
            print("  [INFO] See All Customer Reviews button searching...")

            # page를 천천히 스크롤하면서 button이 나타날 때까지 wait
            print("  [INFO] page starting scroll...")
            scroll_height = self.driver.execute_script("return document.body.scrollHeight")
            current_position = 0
            step = 400  # 400px씩 스크롤 (더 천천히)

            xpaths = [
                '//button[contains(., "See All Customer Reviews")]',
                '//button[@class="relative border-xs border-solid rounded-lg justify-center items-center self-start flex flex-col cursor-pointer px-300 py-100 border-comp-outline-primary-emphasis bg-comp-surface-primary-emphasis mr-200 Op9coqeII1kYHR9Q"]',
                '//button[contains(@class, "Op9coqeII1kYHR9Q")]'
            ]

            # 스크롤하면서 button 찾기
            while current_position < scroll_height:
                # 각 스크롤 위치에서 button 찾기 attempt
                for xpath in xpaths:
                    try:
                        button = self.driver.find_element(By.XPATH, xpath)
                        print("  [OK] See All Customer Reviews button found")
                        self.driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", button)
                        time.sleep(2)

                        # JavaScript로 click attempt
                        try:
                            self.driver.execute_script("arguments[0].click();", button)
                            print("  [OK] See All Customer Reviews click successful")
                            time.sleep(5)  # review page 로딩 wait
                            return True
                        except Exception as click_err:
                            print(f"  [WARNING] click failed (JS): {click_err}, trying regular click")
                            # trying regular click
                            button.click()
                            print("  [OK] See All Customer Reviews click successful (regular)")
                            time.sleep(5)
                            return True

                    except Exception as e:
                        # button을 찾지 못한 경우만 continue
                        if "no such element" not in str(e).lower():
                            print(f"  [DEBUG] button processing failed: {e}")
                        continue

                # button을 못 찾으면 계속 스크롤
                current_position += step
                self.driver.execute_script(f"window.scrollTo(0, {current_position});")
                time.sleep(1)  # 스크롤 후 wait 시간

            print("  [WARNING] See All Customer Reviews button not found.")
            return False

        except Exception as e:
            print(f"  [ERROR] See All Customer Reviews click failed: {e}")
            return False

    def extract_reviews(self):
        """review 20items collected (page네이션 포함)"""
        try:
            time.sleep(3)  # page 로딩 wait
            reviews = []
            collected = 0
            page = 1

            while collected < 20:
                # page 소스 가져오기
                page_source = self.driver.page_source
                tree = html.fromstring(page_source)

                # review extraction
                review_elements = tree.xpath('//li[@class="review-item"]//div[@class="ugc-review-body"]//p[@class="pre-white-space"]')

                for elem in review_elements:
                    if collected >= 20:
                        break
                    review_text = elem.text_content().strip()
                    if review_text:
                        reviews.append(review_text)
                        collected += 1
                        print(f"    [review {collected}/20] {review_text[:50]}...")

                # 20items collected complete하면 closed
                if collected >= 20:
                    break

                # next page button 찾기
                try:
                    next_button = self.driver.find_element(By.XPATH, '//li[contains(@class, "page next")]//a')
                    print(f"  [INFO] Navigating to next page... (Page {page + 1})")
                    self.driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", next_button)
                    time.sleep(2)
                    next_button.click()
                    time.sleep(4)
                    page += 1
                except:
                    print("  [INFO] next page button not found. collected closed.")
                    break

            # review를 구분자로 connection
            return " | ".join(reviews) if reviews else None

        except Exception as e:
            print(f"  [ERROR] review collected failed: {e}")
            return None

    def extract_recommendation_intent_from_reviews_page(self):
        """Recommendation_Intent extraction (See All Customer Reviews page에서)"""
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
                    elem = self.driver.find_element(By.XPATH, xpath)
                    percent = elem.text.strip()
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
        """Compare similar products section data extraction (first page 로딩 items선)"""
        max_retries = 2

        for retry in range(max_retries):
            try:
                if retry > 0:
                    print(f"  [RETRY {retry}/{max_retries}] Compare similar products retry...")
                else:
                    print("  [INFO] Compare similar products section searching...")

                # page 상단으로 이동 후 30%까지 스크롤
                self.driver.execute_script("window.scrollTo(0, 0);")
                time.sleep(1)

                total_height = self.driver.execute_script("return document.body.scrollHeight")
                scroll_to = int(total_height * 0.3)
                self.driver.execute_script(f"window.scrollTo(0, {scroll_to});")

                # first page 여부 확인
                is_first_page = (self.order == 1)

                # timeout 설정: first page는 30sec, 나머지는 15sec
                timeout = 30 if is_first_page else 15

                if is_first_page:
                    print(f"  [INFO] first page detected - applying long wait time (max {timeout}sec)")

                # WebDriverWait로 product-title element가 load될 때까지 명시적 wait
                try:
                    wait = WebDriverWait(self.driver, timeout)
                    wait.until(EC.presence_of_element_located(
                        (By.XPATH, '//div[@class="product-title font-weight-normal pb-100 body-copy-lg min-h-600"]')
                    ))
                    print(f"  [OK] Compare similar products element load complete")

                    # element가 load된 후 안정화를 위한 추가 wait
                    additional_wait = 5 if is_first_page else 3
                    time.sleep(additional_wait)

                except Exception as wait_error:
                    print(f"  [WARNING] element wait time exceeded: {wait_error}")
                    if retry < max_retries - 1:
                        # next retry를 위해 page refresh
                        print("  [INFO] page refresh and retry...")
                        self.driver.refresh()
                        time.sleep(10)
                        continue
                    else:
                        # 마지막 attempt였다면 None 반환
                        return None

                # page 소스 가져오기
                page_source = self.driver.page_source
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

            # 가장 최근 data에서 retailer_sku_name과 product_name이 일치하는 것 찾기
            cursor.execute("""
                SELECT item
                FROM bby_tv_crawl
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

    def save_to_mst_table(self, products, current_item):
        """bby_tv_mst table에 4items 제품 data save"""
        try:
            cursor = self.db_conn.cursor()

            # table 존재 확인 및 생성
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS bby_tv_mst (
                    id SERIAL PRIMARY KEY,
                    account_name VARCHAR(50),
                    item VARCHAR(255),
                    product_url TEXT,
                    pros TEXT,
                    cons TEXT,
                    product_name TEXT,
                    update_date VARCHAR(50),
                    calendar_week VARCHAR(10)
                )
            """)

            # current timestamp
            update_date = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

            # Calculate calendar week
            calendar_week = f"w{datetime.now().isocalendar().week}"

            # 각 제품 save
            for idx, product in enumerate(products):
                # item 결정
                if idx == 0:
                    # first 번째 제품은 현재 page의 item
                    mst_item = current_item
                else:
                    # 2-4번째 제품은 DB에서 찾기
                    mst_item = self.get_item_by_product_name(product['product_name'])

                # data 삽입
                insert_query = """
                    INSERT INTO bby_tv_mst
                    (account_name, item, product_url, pros, cons, product_name, update_date, calendar_week)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                """

                cursor.execute(insert_query, (
                    'Bestbuy',
                    mst_item,
                    product['product_url'],
                    product['pros'],
                    product['cons'],
                    product['product_name'],
                    update_date,
                    calendar_week
                ))

                print(f"    [MST {idx+1}/4] {product['product_name'][:50]}... (item: {mst_item})")

            cursor.close()
            print(f"  [✓] MST table save complete (4items)")
            return True

        except Exception as e:
            print(f"  [ERROR] MST table save failed: {e}")
            import traceback
            traceback.print_exc()
            return False

    def scrape_detail_page(self, url_data):
        """detail page crawling (items선된 로딩 + dialog 처리)"""
        try:
            self.order += 1
            page_type = url_data['page_type']
            product_url = url_data['product_url']
            print(f"\n[{self.order}] [{page_type}] {product_url[:80]}...")

            # page 접속
            self.driver.get(product_url)

            # ADDED: 핵심 element load wait (최대 20sec)
            try:
                wait = WebDriverWait(self.driver, 20)
                wait.until(EC.presence_of_element_located(
                    (By.XPATH, '//h1[contains(@class, "h4") or contains(@class, "heading")]')  # 제품명
                ))
                print(f"  [OK] page load complete")
            except TimeoutException:
                print(f"  [ERROR] page loading timeout")
                return False

            # page 소스 가져오기
            page_source = self.driver.page_source
            tree = html.fromstring(page_source)

            # 1. Retailer_SKU_Name extraction
            retailer_sku_name = self.extract_retailer_sku_name(tree)
            print(f"  [✓] Retailer_SKU_Name: {retailer_sku_name}")

            # 2. Screen Size extraction (메인 page에서)
            screen_size = self.extract_screen_size(tree)
            print(f"  [✓] Screen Size: {screen_size}")

            # 2-1. Price 정보 extraction (메인 page에서 직접 collected)
            final_sku_price = self.extract_final_sku_price(tree)
            print(f"  [✓] Final_SKU_Price: {final_sku_price}")

            original_sku_price = self.extract_original_sku_price(tree)
            print(f"  [✓] Original_SKU_Price: {original_sku_price}")

            savings = self.extract_savings(tree)
            print(f"  [✓] Savings: {savings}")

            # 2-2. Star Rating 및 Reviews 정보 extraction (메인 page에서 직접 collected)
            star_rating = self.extract_star_rating(tree)
            print(f"  [✓] Star_Rating: {star_rating}")

            count_of_reviews = self.extract_count_of_reviews_from_detail(tree)
            print(f"  [✓] Count_of_Reviews: {count_of_reviews}")

            # 3. Compare similar products extraction
            mst_products = self.extract_compare_similar_products(product_url)

            # 4. Specification button click (retry 로직 적용)
            item = None
            electricity_use = None

            success, error = self.click_specifications_with_retry()

            if success:
                time.sleep(3)
                # dialog 소스 가져오기
                dialog_source = self.driver.page_source
                dialog_tree = html.fromstring(dialog_source)

                # 5. Item extraction
                item = self.extract_item(dialog_tree)
                print(f"  [✓] Item: {item}")

                # 6. Estimated_Annual_Electricity_Use extraction (숫자만)
                electricity_use = self.extract_electricity_use(dialog_tree)
                print(f"  [✓] Estimated_Annual_Electricity_Use: {electricity_use}")

                # 7. dialog close
                self.close_specifications_dialog()
            else:
                print(f"  [ERROR] Specifications dialog failed: {error}")
                item = None

            # 8. MST table에 save (item이 있고 mst_products가 있을 때)
            if mst_products and item:
                self.save_to_mst_table(mst_products, item)

            # 9. See All Customer Reviews click 및 data collected
            star_ratings = None
            top_mentions = None
            detailed_reviews = None
            recommendation_intent = None

            if self.click_see_all_reviews():
                # 9-1. Star ratings collected (review page에서 - 별점별 detail items count)
                star_ratings = self.extract_star_ratings_from_reviews_page()
                print(f"  [✓] Star_Ratings: {star_ratings}")

                # 9-2. Top mentions collected (review page에서)
                top_mentions = self.extract_top_mentions_from_reviews_page()
                print(f"  [✓] Top_Mentions: {top_mentions}")

                # 9-3. Recommendation intent collected (review page에서)
                recommendation_intent = self.extract_recommendation_intent_from_reviews_page()
                print(f"  [✓] Recommendation_Intent: {recommendation_intent}")

                # 9-4. Detailed reviews collected
                detailed_reviews = self.extract_reviews()
                print(f"  [✓] Detailed_Reviews: {len(detailed_reviews) if detailed_reviews else 0} chars")

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
            self.validator.validate_star_rating(star_rating_source, product_url, 'bby_tv_dt1')

            # 10. Detail DB save
            self.save_to_db(
                page_type=page_type,
                order=self.order,
                retailer_sku_name=retailer_sku_name,
                item=item,
                electricity_use=electricity_use,
                screen_size=screen_size,
                count_of_reviews=count_of_reviews,
                star_ratings=star_ratings,
                top_mentions=top_mentions,
                detailed_reviews=detailed_reviews,
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
                promotion_rank=url_data['promotion_rank'],
                bsr_rank=url_data['bsr_rank'],
                main_rank=url_data['main_rank']
            )

            return True

        except Exception as e:
            print(f"  [ERROR] detail page crawling failed: {e}")
            import traceback
            traceback.print_exc()
            return False

    def save_to_db(self, page_type, order, retailer_sku_name, item,
                   electricity_use, screen_size, count_of_reviews, star_ratings, top_mentions, detailed_reviews,
                   recommendation_intent, product_url,
                   final_sku_price, savings, original_sku_price, offer,
                   pick_up_availability, shipping_availability, delivery_availability,
                   sku_status, star_rating_source, promotion_type, promotion_rank,
                   bsr_rank, main_rank):
        """DB에 save"""
        try:
            cursor = self.db_conn.cursor()

            # Calculate calendar week
            calendar_week = f"w{datetime.now().isocalendar().week}"

            # Calculate crawl_datetime (format: 2025-11-04 03:00:27)
            now = datetime.now()
            crawl_datetime = now.strftime('%Y-%m-%d %H:%M:%S')

            # data 삽입
            insert_query = """
                INSERT INTO bby_tv_crawl
                (account_name, batch_id, page_type, "order", retailer_sku_name, item,
                 Estimated_Annual_Electricity_Use, screen_size, count_of_reviews, Count_of_Star_Ratings, Top_Mentions,
                 Detailed_Review_Content, Recommendation_Intent, product_url, crawl_datetime, calendar_week,
                 final_sku_price, savings, original_sku_price, offer, pick_up_availability, shipping_availability,
                 delivery_availability, sku_status, star_rating, promotion_type, promotion_rank,
                 bsr_rank, main_rank)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """

            cursor.execute(insert_query, (
                'Bestbuy',
                self.batch_id,
                page_type,
                order,
                retailer_sku_name,
                item,
                electricity_use,
                screen_size,
                count_of_reviews,
                star_ratings,
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
                promotion_rank,
                bsr_rank,
                main_rank
            ))

            # Also insert into unified tv_retail_com table
            # Convert count_of_reviews to integer (remove commas if present)
            count_of_reviews_int = None
            if count_of_reviews:
                try:
                    count_of_reviews_int = int(str(count_of_reviews).replace(',', ''))
                except:
                    count_of_reviews_int = None

            # Parse star_ratings to get total count
            # Example: "5stars:231 4stars:19 3stars:2 2stars:1 1star:8" -> 261
            count_of_star_ratings_int = None
            if star_ratings:
                try:
                    import re
                    numbers = re.findall(r':(\d+)', star_ratings)
                    if numbers:
                        count_of_star_ratings_int = sum(int(n) for n in numbers)
                except:
                    count_of_star_ratings_int = None

            cursor.execute("""
                INSERT INTO tv_retail_com
                (item, account_name, page_type, count_of_reviews, retailer_sku_name, product_url,
                 star_rating, count_of_star_ratings, screen_size, sku_popularity,
                 final_sku_price, original_sku_price, savings, discount_type, offer,
                 pick_up_availability, shipping_availability, delivery_availability, shipping_info,
                 available_quantity_for_purchase, inventory_status, sku_status, retailer_membership_discounts,
                 detailed_review_content, summarized_review_content, top_mentions, recommendation_intent,
                 main_rank, bsr_rank, rank_1, rank_2, promotion_rank,
                 number_of_ppl_purchased_yesterday, number_of_ppl_added_to_carts, retailer_sku_name_similar,
                 estimated_annual_electricity_use, promotion_type,
                 calendar_week, crawl_datetime)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                item,
                'Bestbuy',  # account_name
                page_type,
                count_of_reviews_int,  # Converted to integer
                retailer_sku_name,
                product_url,
                star_rating_source,
                count_of_star_ratings_int,  # Parsed from star_ratings string
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
                None,  # summarized_review_content (BestBuy doesn't have this)
                top_mentions,
                recommendation_intent,
                main_rank,
                bsr_rank,
                None,  # rank_1 (BestBuy doesn't have this)
                None,  # rank_2 (BestBuy doesn't have this)
                promotion_rank,
                None,  # number_of_ppl_purchased_yesterday (BestBuy doesn't have this)
                None,  # number_of_ppl_added_to_carts (BestBuy doesn't have this)
                None,  # retailer_sku_name_similar (BestBuy doesn't have this)
                electricity_use,
                promotion_type,
                calendar_week,
                crawl_datetime
            ))

            cursor.close()
            print(f"  [✓] DB save complete (bby_tv_crawl + tv_retail_com)")
            return True

        except Exception as e:
            print(f"  [ERROR] DB save failed: {e}")
            import traceback
            traceback.print_exc()
            return False

    def fill_missing_items(self):
        """empty item을 이전 세션 data로 fill"""
        try:
            print("\n[INFO] empty item filling...")
            cursor = self.db_conn.cursor()

            # 현재 세션에서 item이 NULL인 레코드 찾기
            cursor.execute("""
                SELECT id, product_name
                FROM bby_tv_mst
                WHERE item IS NULL
                AND product_name IS NOT NULL
            """)

            empty_items = cursor.fetchall()

            if not empty_items:
                print("[OK] empty item none")
                cursor.close()
                return

            print(f"[INFO] empty item {len(empty_items)}items found")

            updated_count = 0
            for record_id, product_name in empty_items:
                # 이전 세션에서 같은 product_name을 가진 레코드의 item 찾기
                cursor.execute("""
                    SELECT item
                    FROM bby_tv_mst
                    WHERE product_name = %s
                    AND item IS NOT NULL
                    ORDER BY id DESC
                    LIMIT 1
                """, (product_name,))

                result = cursor.fetchone()
                if result:
                    item_value = result[0]
                    # UPDATE
                    cursor.execute("""
                        UPDATE bby_tv_mst
                        SET item = %s
                        WHERE id = %s
                    """, (item_value, record_id))
                    updated_count += 1
                    print(f"  [✓] Updated: {product_name[:50]}... → item: {item_value}")

            self.db_conn.commit()
            cursor.close()

            print(f"[OK] {updated_count}/{len(empty_items)}items item filled complete")
            return True

        except Exception as e:
            print(f"[ERROR] item fill failed: {e}")
            import traceback
            traceback.print_exc()
            return False

    def run(self):
        """메인 execution"""
        try:
            print("="*80)
            print(f"Best Buy TV Detail Page Crawler (Modified v1) (Batch ID: {self.batch_id})")
            print("="*80)

            # DB connection
            if not self.connect_db():
                return

            # URLs 가져오기
            urls = self.get_recent_urls()
            if not urls:
                print("[ERROR] No URLs found")
                return

            # 드라이버 설정
            if not self.setup_driver():
                return

            # 각 URL crawling
            success_count = 0
            for url_data in urls:
                if self.scrape_detail_page(url_data):
                    success_count += 1

                # page 간 딜레이
                time.sleep(random.uniform(3, 5))

            print("\n" + "="*80)
            print(f"crawling complete! successful: {success_count}/{len(urls)}items")
            print("="*80)

            # empty item fill
            self.fill_missing_items()

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

        except Exception as e:
            print(f"[ERROR] crawler execution error: {e}")
            import traceback
            traceback.print_exc()

        finally:
            if self.driver:
                self.driver.quit()
                print("\n[INFO] Driver closed")
            if self.db_conn:
                self.db_conn.close()
                print("[INFO] DB connection closed")

def main():
    crawler = BestBuyDetailCrawler()
    crawler.run()

if __name__ == "__main__":
    main()
