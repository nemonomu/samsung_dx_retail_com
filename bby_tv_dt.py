"""
Best Buy TV Detail Page Crawler (Modified)
수집 테이블: bestbuy_tv_main_crawl, bby_tv_trend_crawl, bby_tv_promotion_crawl, bby_tv_bsr_crawl
저장 테이블: bby_tv_detail_crawled, bby_tv_mst

수정사항:
1. estimated_annual_electricity_use: 숫자만 추출 (예: "286 kilowatt hours" -> "286")
2. screen_size 컬럼 추가
3. samsung_sku_name -> item으로 변경, item -> retailer_sku_name으로 변경
4. 소스 테이블에서 14개 컬럼 추가 수집:
   - 9개 데이터 컬럼: final_sku_price, savings, original_sku_price, offer,
     pick_up_availability, shipping_availability, delivery_availability, sku_status, star_rating
   - 5개 rank/type 컬럼: promotion_type, promotion_rank, bsr_rank, main_rank, trend_rank
   - 첫 번째 발견된 URL의 데이터 우선 (중복 URL은 첫 소스 데이터 사용)
   - 소스 테이블에 없는 컬럼은 NULL 처리
"""
import time
import random
import re
import psycopg2
from datetime import datetime
import pytz
import undetected_chromedriver as uc
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from lxml import html

# Import database configuration
from config import DB_CONFIG

class BestBuyDetailCrawler:
    def __init__(self):
        self.driver = None
        self.db_conn = None
        self.korea_tz = pytz.timezone('Asia/Seoul')
        self.batch_id = datetime.now(self.korea_tz).strftime('%Y%m%d_%H%M%S')
        self.order = 0

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

    def setup_driver(self):
        """Chrome 드라이버 설정"""
        try:
            print("[INFO] Chrome 드라이버 설정 중...")
            self.driver = uc.Chrome()
            self.driver.maximize_window()
            print("[OK] 드라이버 설정 완료")
            return True
        except Exception as e:
            print(f"[ERROR] 드라이버 설정 실패: {e}")
            return False

    def get_recent_urls(self):
        """최신 batch_id의 product URLs와 추가 데이터 가져오기"""
        try:
            cursor = self.db_conn.cursor()
            urls = []

            # bestbuy_tv_main_crawl에서 최신 batch_id 가져오기
            cursor.execute("""
                SELECT batch_id
                FROM bestbuy_tv_main_crawl
                WHERE batch_id IS NOT NULL
                ORDER BY batch_id DESC
                LIMIT 1
            """)
            main_batch_result = cursor.fetchone()
            main_batch_id = main_batch_result[0] if main_batch_result else None

            # bby_tv_Trend_crawl에서 최신 batch_id 가져오기
            cursor.execute("""
                SELECT batch_id
                FROM bby_tv_Trend_crawl
                WHERE batch_id IS NOT NULL
                ORDER BY batch_id DESC
                LIMIT 1
            """)
            trend_batch_result = cursor.fetchone()
            trend_batch_id = trend_batch_result[0] if trend_batch_result else None

            # bby_tv_promotion_crawl에서 최신 batch_id 가져오기
            cursor.execute("""
                SELECT batch_id
                FROM bby_tv_promotion_crawl
                WHERE batch_id IS NOT NULL
                ORDER BY batch_id DESC
                LIMIT 1
            """)
            promo_batch_result = cursor.fetchone()
            promo_batch_id = promo_batch_result[0] if promo_batch_result else None

            # bby_tv_bsr_crawl에서 최신 batch_id 가져오기
            cursor.execute("""
                SELECT batch_id
                FROM bby_tv_bsr_crawl
                WHERE batch_id IS NOT NULL
                ORDER BY batch_id DESC
                LIMIT 1
            """)
            bsr_batch_result = cursor.fetchone()
            bsr_batch_id = bsr_batch_result[0] if bsr_batch_result else None

            print(f"[INFO] Latest batch_id - Main: {main_batch_id}, Trend: {trend_batch_id}, Promotion: {promo_batch_id}, BSR: {bsr_batch_id}")

            # 수집 순서: main → bsr → promotion → trend (우선순위 순서)
            # 각 테이블의 rank 순서대로 정렬

            # 1. bestbuy_tv_main_crawl에서 해당 batch의 URLs와 데이터 가져오기
            if main_batch_id:
                cursor.execute("""
                    SELECT DISTINCT product_url, final_sku_price, savings, original_sku_price, offer,
                           pick_up_availability, shipping_availability, delivery_availability,
                           sku_status, star_rating, main_rank
                    FROM bestbuy_tv_main_crawl
                    WHERE batch_id = %s
                    AND product_url IS NOT NULL
                    ORDER BY main_rank
                """, (main_batch_id,))
                main_urls = cursor.fetchall()
                for row in main_urls:
                    urls.append({
                        'page_type': 'main',
                        'product_url': row[0],
                        'final_sku_price': row[1],
                        'savings': row[2],
                        'original_sku_price': row[3],
                        'offer': row[4],
                        'pick_up_availability': row[5],
                        'shipping_availability': row[6],
                        'delivery_availability': row[7],
                        'sku_status': row[8],
                        'star_rating': row[9],
                        'main_rank': row[10],
                        'bsr_rank': None,
                        'trend_rank': None,
                        'promotion_rank': None,
                        'promotion_type': None
                    })
                print(f"[OK] Main URLs (batch {main_batch_id}): {len(main_urls)}개")

            # 2. bby_tv_bsr_crawl에서 해당 batch의 URLs와 데이터 가져오기
            if bsr_batch_id:
                cursor.execute("""
                    SELECT DISTINCT product_url, final_sku_price, savings, original_sku_price, offer,
                           pick_up_availability, shipping_availability, delivery_availability,
                           sku_status, star_rating, bsr_rank
                    FROM bby_tv_bsr_crawl
                    WHERE batch_id = %s
                    AND product_url IS NOT NULL
                    ORDER BY bsr_rank
                """, (bsr_batch_id,))
                bsr_urls = cursor.fetchall()
                for row in bsr_urls:
                    urls.append({
                        'page_type': 'bsr',
                        'product_url': row[0],
                        'final_sku_price': row[1],
                        'savings': row[2],
                        'original_sku_price': row[3],
                        'offer': row[4],
                        'pick_up_availability': row[5],
                        'shipping_availability': row[6],
                        'delivery_availability': row[7],
                        'sku_status': row[8],
                        'star_rating': row[9],
                        'main_rank': None,
                        'bsr_rank': row[10],
                        'trend_rank': None,
                        'promotion_rank': None,
                        'promotion_type': None
                    })
                print(f"[OK] BSR URLs (batch {bsr_batch_id}): {len(bsr_urls)}개")

            # 3. bby_tv_promotion_crawl에서 해당 batch의 URLs와 데이터 가져오기
            if promo_batch_id:
                cursor.execute("""
                    SELECT DISTINCT product_url, final_sku_price, original_sku_price, offer, savings,
                           promotion_type, promotion_rank
                    FROM bby_tv_promotion_crawl
                    WHERE batch_id = %s
                    AND product_url IS NOT NULL
                    ORDER BY promotion_rank
                """, (promo_batch_id,))
                promo_urls = cursor.fetchall()
                for row in promo_urls:
                    urls.append({
                        'page_type': 'promotion',
                        'product_url': row[0],
                        'final_sku_price': row[1],
                        'savings': row[4],
                        'original_sku_price': row[2],
                        'offer': row[3],
                        'pick_up_availability': None,
                        'shipping_availability': None,
                        'delivery_availability': None,
                        'sku_status': None,
                        'star_rating': None,
                        'main_rank': None,
                        'bsr_rank': None,
                        'trend_rank': None,
                        'promotion_rank': row[6],
                        'promotion_type': row[5]
                    })
                print(f"[OK] Promotion URLs (batch {promo_batch_id}): {len(promo_urls)}개")

            # 4. bby_tv_Trend_crawl에서 해당 batch의 URLs와 데이터 가져오기
            if trend_batch_id:
                cursor.execute("""
                    SELECT DISTINCT product_url, rank
                    FROM bby_tv_Trend_crawl
                    WHERE batch_id = %s
                    AND product_url IS NOT NULL
                    ORDER BY rank
                """, (trend_batch_id,))
                trend_urls = cursor.fetchall()
                for row in trend_urls:
                    urls.append({
                        'page_type': 'Trend',
                        'product_url': row[0],
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
                        'promotion_rank': None,
                        'promotion_type': None
                    })
                print(f"[OK] Trend URLs (batch {trend_batch_id}): {len(trend_urls)}개")

            cursor.close()

            # Remove duplicate URLs across tables (keep first occurrence)
            seen_urls = set()
            unique_urls = []
            duplicates_count = 0

            for url_data in urls:
                url = url_data['product_url']
                if url not in seen_urls:
                    seen_urls.add(url)
                    unique_urls.append(url_data)
                else:
                    duplicates_count += 1

            if duplicates_count > 0:
                print(f"[INFO] Removed {duplicates_count} duplicate URLs across tables")

            print(f"[OK] 총 {len(unique_urls)}개 unique URLs 로드 완료 (중복 제거 전: {len(urls)}개)")
            return unique_urls

        except Exception as e:
            print(f"[ERROR] Failed to load URLs: {e}")
            import traceback
            traceback.print_exc()
            return []

    def extract_retailer_sku_name(self, tree):
        """Retailer_SKU_Name 추출"""
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
            print(f"  [ERROR] Retailer_SKU_Name 추출 실패: {e}")
            return None

    def click_specifications(self):
        """Specification 버튼 클릭"""
        try:
            print("  [INFO] Specification 버튼 클릭...")
            # XPath를 사용한 여러 시도
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
                    print("  [OK] Specification 클릭 성공")
                    time.sleep(7)  # 다이얼로그 로딩 대기 증가
                    return True
                except:
                    continue

            print("  [WARNING] Specification 버튼을 찾을 수 없습니다.")
            return False

        except Exception as e:
            print(f"  [ERROR] Specification 클릭 실패: {e}")
            return False

    def extract_item(self, tree):
        """Item (Model Number) 추출"""
        try:
            # 다이얼로그에서 Model Number 찾기 (여러 패턴 시도)
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
            print(f"  [ERROR] Item 추출 실패: {e}")
            return None

    def extract_electricity_use(self, tree):
        """Estimated_Annual_Electricity_Use 추출 (숫자만)"""
        try:
            # 다이얼로그에서 Estimated Annual Electricity Use 찾기 (여러 패턴 시도)
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
                        # 숫자만 추출 (예: "286 kilowatt hours" -> "286")
                        match = re.search(r'(\d+)', electricity)
                        if match:
                            return match.group(1)
                        return electricity  # 숫자를 찾지 못하면 원본 반환
            return None
        except Exception as e:
            print(f"  [ERROR] Estimated_Annual_Electricity_Use 추출 실패: {e}")
            return None

    def extract_screen_size(self, tree):
        """Screen Size 추출"""
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
                    # 정규식으로 "숫자 + inches" 추출
                    match = re.search(r'(\d+\s*inches)', screen_size_text)
                    if match:
                        return match.group(1)
                    # 만약 매칭 안되면 원본 반환
                    return screen_size_text
            return None
        except Exception as e:
            print(f"  [ERROR] Screen Size 추출 실패: {e}")
            return None

    def close_specifications_dialog(self):
        """Specification 다이얼로그 닫기"""
        try:
            print("  [INFO] Specification 다이얼로그 닫기...")
            xpaths = [
                '//button[@data-testid="brix-sheet-closeButton"]',
                '//button[@aria-label="Close Sheet"]',
                '//div[@class="relative"]//button'
            ]

            for xpath in xpaths:
                try:
                    close_button = self.driver.find_element(By.XPATH, xpath)
                    close_button.click()
                    print("  [OK] 다이얼로그 닫기 성공")
                    time.sleep(2)
                    return True
                except:
                    continue

            print("  [WARNING] 다이얼로그 닫기 버튼을 찾을 수 없습니다.")
            return False

        except Exception as e:
            print(f"  [ERROR] 다이얼로그 닫기 실패: {e}")
            return False

    def extract_similar_products(self, tree):
        """Compare similar products 데이터 추출"""
        try:
            similar_names = []
            pros_list = []
            cons_list = []

            # Retailer_SKU_Name_similar 추출
            name_elements = tree.xpath('//span[@class="clamp" and starts-with(@id, "compare-title-")]')
            for elem in name_elements[:4]:  # 최대 4개
                similar_names.append(elem.text_content().strip())

            # Pros 추출
            pros_elements = tree.xpath('//tr[@class="flex"]//td[.//svg[@aria-label="Advantage Icon"]]//span[@class="text-3 min-w-0 flex flex-wrap"]')
            for elem in pros_elements[:4]:  # 최대 4개
                pros_list.append(elem.text_content().strip())

            # Cons 추출
            cons_elements = tree.xpath('//tr[@class="flex"]//td[.//svg[@aria-label="Disadvantage Icon"]]//span[@class="text-3 min-w-0 flex flex-wrap"]')
            for elem in cons_elements[:4]:  # 최대 4개
                text = elem.text_content().strip()
                if text and text != '—':
                    cons_list.append(text)
                else:
                    cons_list.append(None)

            # 부족한 경우 None으로 채우기
            while len(similar_names) < 4:
                similar_names.append(None)
            while len(pros_list) < 4:
                pros_list.append(None)
            while len(cons_list) < 4:
                cons_list.append(None)

            return similar_names[:4], pros_list[:4], cons_list[:4]

        except Exception as e:
            print(f"  [ERROR] Similar products 추출 실패: {e}")
            return [None]*4, [None]*4, [None]*4

    def extract_star_ratings_from_reviews_page(self):
        """Count_of_Star_Ratings 추출 (See All Customer Reviews 페이지에서)"""
        try:
            time.sleep(3)  # 페이지 로딩 대기
            ratings = {}
            # XPath 패턴 (5점부터 1점까지)
            xpaths = [
                '//*[@id="reviews-accordion"]/section/div[1]/div[1]/div/div/div[2]/div/fieldset/div[1]/div/label/span[5]',  # 5점
                '//*[@id="reviews-accordion"]/section/div[1]/div[1]/div/div/div[2]/div/fieldset/div[2]/div/label/span[5]',  # 4점
                '//*[@id="reviews-accordion"]/section/div[1]/div[1]/div/div/div[2]/div/fieldset/div[3]/div/label/span[5]',  # 3점
                '//*[@id="reviews-accordion"]/section/div[1]/div[1]/div/div/div[2]/div/fieldset/div[4]/div/label/span[5]',  # 2점
                '//*[@id="reviews-accordion"]/section/div[1]/div[1]/div/div/div[2]/div/fieldset/div[5]/div/label/span[5]'   # 1점
            ]

            # 5점부터 1점까지 순서로 추출
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
            print(f"  [ERROR] Star ratings 추출 실패: {e}")
            return None

    def extract_count_of_reviews(self):
        """Count_of_Reviews 추출 (See All Customer Reviews 페이지에서)"""
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
                    # 숫자만 추출 (예: "(84 Reviews)" -> "84")
                    match = re.search(r'\((\d+)\s*Reviews?\)', text)
                    if match:
                        return match.group(1)
                except Exception:
                    continue

            return None

        except Exception as e:
            print(f"  [ERROR] Count of reviews 추출 실패: {e}")
            return None

    def extract_top_mentions_from_reviews_page(self):
        """Top_Mentions 추출 (See All Customer Reviews 페이지에서)"""
        try:
            # XPath 패턴 (ID가 동적이므로 class 기반으로 찾기)
            xpaths = [
                # "Highly rated by customers for" 섹션의 span.text-nowrap들
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
                # 첫 번째 항목만 반환 (예: "Picture Quality")
                return mentions[0]

            return None

        except Exception as e:
            print(f"  [ERROR] Top mentions 추출 실패: {e}")
            return None

    def click_see_all_reviews(self):
        """See All Customer Reviews 버튼 클릭"""
        try:
            print("  [INFO] See All Customer Reviews 버튼 찾는 중...")

            # 페이지를 천천히 스크롤하면서 버튼이 나타날 때까지 대기
            print("  [INFO] 페이지 스크롤 시작...")
            scroll_height = self.driver.execute_script("return document.body.scrollHeight")
            current_position = 0
            step = 400  # 400px씩 스크롤 (더 천천히)

            xpaths = [
                '//button[contains(., "See All Customer Reviews")]',
                '//button[@class="relative border-xs border-solid rounded-lg justify-center items-center self-start flex flex-col cursor-pointer px-300 py-100 border-comp-outline-primary-emphasis bg-comp-surface-primary-emphasis mr-200 Op9coqeII1kYHR9Q"]',
                '//button[contains(@class, "Op9coqeII1kYHR9Q")]'
            ]

            # 스크롤하면서 버튼 찾기
            while current_position < scroll_height:
                # 각 스크롤 위치에서 버튼 찾기 시도
                for xpath in xpaths:
                    try:
                        button = self.driver.find_element(By.XPATH, xpath)
                        print("  [OK] See All Customer Reviews 버튼 발견")
                        self.driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", button)
                        time.sleep(2)

                        # JavaScript로 클릭 시도
                        try:
                            self.driver.execute_script("arguments[0].click();", button)
                            print("  [OK] See All Customer Reviews 클릭 성공")
                            time.sleep(5)  # 리뷰 페이지 로딩 대기
                            return True
                        except Exception as click_err:
                            print(f"  [WARNING] 클릭 실패 (JS): {click_err}, 일반 클릭 시도")
                            # 일반 클릭 시도
                            button.click()
                            print("  [OK] See All Customer Reviews 클릭 성공 (일반)")
                            time.sleep(5)
                            return True

                    except Exception as e:
                        # 버튼을 찾지 못한 경우만 continue
                        if "no such element" not in str(e).lower():
                            print(f"  [DEBUG] 버튼 처리 실패: {e}")
                        continue

                # 버튼을 못 찾으면 계속 스크롤
                current_position += step
                self.driver.execute_script(f"window.scrollTo(0, {current_position});")
                time.sleep(1)  # 스크롤 후 대기 시간

            print("  [WARNING] See All Customer Reviews 버튼을 찾을 수 없습니다.")
            return False

        except Exception as e:
            print(f"  [ERROR] See All Customer Reviews 클릭 실패: {e}")
            return False

    def extract_reviews(self):
        """리뷰 20개 수집 (페이지네이션 포함)"""
        try:
            time.sleep(3)  # 페이지 로딩 대기
            reviews = []
            collected = 0
            page = 1

            while collected < 20:
                # 페이지 소스 가져오기
                page_source = self.driver.page_source
                tree = html.fromstring(page_source)

                # 리뷰 추출
                review_elements = tree.xpath('//li[@class="review-item"]//div[@class="ugc-review-body"]//p[@class="pre-white-space"]')

                for elem in review_elements:
                    if collected >= 20:
                        break
                    review_text = elem.text_content().strip()
                    if review_text:
                        reviews.append(review_text)
                        collected += 1
                        print(f"    [리뷰 {collected}/20] {review_text[:50]}...")

                # 20개 수집 완료하면 종료
                if collected >= 20:
                    break

                # 다음 페이지 버튼 찾기
                try:
                    next_button = self.driver.find_element(By.XPATH, '//li[contains(@class, "page next")]//a')
                    print(f"  [INFO] 다음 페이지로 이동 중... (Page {page + 1})")
                    self.driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", next_button)
                    time.sleep(2)
                    next_button.click()
                    time.sleep(4)
                    page += 1
                except:
                    print("  [INFO] 다음 페이지 버튼이 없습니다. 수집 종료.")
                    break

            # 리뷰를 구분자로 연결
            return " | ".join(reviews) if reviews else None

        except Exception as e:
            print(f"  [ERROR] 리뷰 수집 실패: {e}")
            return None

    def extract_recommendation_intent_from_reviews_page(self):
        """Recommendation_Intent 추출 (See All Customer Reviews 페이지에서)"""
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
            print(f"  [ERROR] Recommendation intent 추출 실패: {e}")
            return None

    def extract_compare_similar_products(self, current_url):
        """Compare similar products 섹션 데이터 추출"""
        try:
            print("  [INFO] Compare similar products 섹션 찾는 중...")

            # 페이지 상단으로 이동 후 30%까지 스크롤
            self.driver.execute_script("window.scrollTo(0, 0);")
            time.sleep(1)

            total_height = self.driver.execute_script("return document.body.scrollHeight")
            scroll_to = int(total_height * 0.3)
            self.driver.execute_script(f"window.scrollTo(0, {scroll_to});")
            time.sleep(3)

            # 페이지 소스 가져오기
            page_source = self.driver.page_source
            tree = html.fromstring(page_source)

            # 4개 제품 데이터 저장
            products = []

            # product-title div들 찾기
            product_divs = tree.xpath('//div[@class="product-title font-weight-normal pb-100 body-copy-lg min-h-600"]')

            if len(product_divs) < 4:
                print(f"  [WARNING] Compare similar products 섹션을 찾을 수 없거나 제품이 부족합니다. (찾은 개수: {len(product_divs)})")
                return None

            # 첫 번째 제품 (현재 페이지)
            first_product = {
                'product_url': current_url,
                'product_name': None,
                'pros': None,
                'cons': None
            }

            # 첫 번째 제품명 추출
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

                    # a 태그에서 URL과 제품명 추출
                    a_elem = product_divs[i].xpath('.//a[@class="clamp"]')
                    if a_elem:
                        href = a_elem[0].get('href')
                        if href:
                            product['product_url'] = href
                        product['product_name'] = a_elem[0].text_content().strip()

                    products.append(product)

            # Pros 추출 (tr[2]/td[1~4])
            for i in range(1, 5):
                pros_xpath = f'/html/body/div[5]/div[6]/div/table/tbody/tr[2]/td[{i}]/span/span'
                pros_elem = tree.xpath(pros_xpath)
                if pros_elem and i-1 < len(products):
                    products[i-1]['pros'] = pros_elem[0].text_content().strip()

            # Cons 추출 (tr[4]/td[1~4])
            for i in range(1, 5):
                cons_xpath = f'/html/body/div[5]/div[6]/div/table/tbody/tr[4]/td[{i}]/span/span'
                cons_elem = tree.xpath(cons_xpath)
                if cons_elem and i-1 < len(products):
                    text = cons_elem[0].text_content().strip()
                    # '—' 같은 값은 None으로 처리
                    products[i-1]['cons'] = text if text and text != '—' else None

            print(f"  [OK] Compare similar products 데이터 추출 완료 (4개)")
            return products

        except Exception as e:
            print(f"  [ERROR] Compare similar products 추출 실패: {e}")
            import traceback
            traceback.print_exc()
            return None

    def get_item_by_product_name(self, product_name):
        """bby_tv_detail_crawled에서 product_name으로 item 찾기"""
        try:
            if not product_name:
                return None

            cursor = self.db_conn.cursor()

            # 가장 최근 데이터에서 retailer_sku_name과 product_name이 일치하는 것 찾기
            cursor.execute("""
                SELECT item
                FROM bby_tv_detail_crawled
                WHERE retailer_sku_name = %s
                AND item IS NOT NULL
                ORDER BY crawl_strdatetime DESC
                LIMIT 1
            """, (product_name,))

            result = cursor.fetchone()
            cursor.close()

            if result:
                return result[0]
            return None

        except Exception as e:
            print(f"  [ERROR] Item 조회 실패 ({product_name}): {e}")
            return None

    def save_to_mst_table(self, products, current_item):
        """bby_tv_mst 테이블에 4개 제품 데이터 저장"""
        try:
            cursor = self.db_conn.cursor()

            # 테이블 존재 확인 및 생성
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

            # 각 제품 저장
            for idx, product in enumerate(products):
                # item 결정
                if idx == 0:
                    # 첫 번째 제품은 현재 페이지의 item
                    mst_item = current_item
                else:
                    # 2-4번째 제품은 DB에서 찾기
                    mst_item = self.get_item_by_product_name(product['product_name'])

                # 데이터 삽입
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
            print(f"  [✓] MST 테이블 저장 완료 (4개)")
            return True

        except Exception as e:
            print(f"  [ERROR] MST 테이블 저장 실패: {e}")
            import traceback
            traceback.print_exc()
            return False

    def scrape_detail_page(self, url_data):
        """상세 페이지 크롤링"""
        try:
            self.order += 1
            page_type = url_data['page_type']
            product_url = url_data['product_url']
            print(f"\n[{self.order}] [{page_type}] {product_url[:80]}...")

            # 페이지 접속
            self.driver.get(product_url)
            time.sleep(random.uniform(8, 12))

            # 페이지 소스 가져오기
            page_source = self.driver.page_source
            tree = html.fromstring(page_source)

            # 1. Retailer_SKU_Name 추출
            retailer_sku_name = self.extract_retailer_sku_name(tree)
            print(f"  [✓] Retailer_SKU_Name: {retailer_sku_name}")

            # 2. Screen Size 추출 (메인 페이지에서)
            screen_size = self.extract_screen_size(tree)
            print(f"  [✓] Screen Size: {screen_size}")

            # 3. Compare similar products 추출
            mst_products = self.extract_compare_similar_products(product_url)

            # 4. Specification 버튼 클릭
            item = None
            electricity_use = None

            if self.click_specifications():
                # 다이얼로그가 완전히 로드될 때까지 대기
                try:
                    # Model Number 요소가 나타날 때까지 최대 15초 대기
                    wait = WebDriverWait(self.driver, 15)
                    wait.until(EC.presence_of_element_located((By.XPATH, '//div[contains(text(), "Model Number")]')))
                    print("  [OK] 다이얼로그 로드 완료")
                except Exception as e:
                    print(f"  [WARNING] 다이얼로그 로딩 대기 타임아웃: {e}")

                time.sleep(3)
                # 다이얼로그 소스 가져오기
                dialog_source = self.driver.page_source
                dialog_tree = html.fromstring(dialog_source)

                # 5. Item 추출
                item = self.extract_item(dialog_tree)
                print(f"  [✓] Item: {item}")

                # 6. Estimated_Annual_Electricity_Use 추출 (숫자만)
                electricity_use = self.extract_electricity_use(dialog_tree)
                print(f"  [✓] Estimated_Annual_Electricity_Use: {electricity_use}")

                # 7. 다이얼로그 닫기
                self.close_specifications_dialog()

            # 8. MST 테이블에 저장 (item이 있고 mst_products가 있을 때)
            if mst_products and item:
                self.save_to_mst_table(mst_products, item)

            # 9. See All Customer Reviews 클릭 및 데이터 수집
            star_ratings = None
            count_of_reviews = None
            top_mentions = None
            detailed_reviews = None
            recommendation_intent = None

            if self.click_see_all_reviews():
                # 9-1. Count of reviews 수집 (리뷰 페이지에서)
                count_of_reviews = self.extract_count_of_reviews()
                print(f"  [✓] Count_of_Reviews: {count_of_reviews}")

                # 9-2. Star ratings 수집 (리뷰 페이지에서)
                star_ratings = self.extract_star_ratings_from_reviews_page()
                print(f"  [✓] Star_Ratings: {star_ratings}")

                # 9-3. Top mentions 수집 (리뷰 페이지에서)
                top_mentions = self.extract_top_mentions_from_reviews_page()
                print(f"  [✓] Top_Mentions: {top_mentions}")

                # 9-4. Recommendation intent 수집 (리뷰 페이지에서)
                recommendation_intent = self.extract_recommendation_intent_from_reviews_page()
                print(f"  [✓] Recommendation_Intent: {recommendation_intent}")

                # 9-5. Detailed reviews 수집
                detailed_reviews = self.extract_reviews()
                print(f"  [✓] Detailed_Reviews: {len(detailed_reviews) if detailed_reviews else 0} chars")

            # 10. Detail DB 저장
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
                # 소스 테이블의 추가 데이터
                final_sku_price=url_data['final_sku_price'],
                savings=url_data['savings'],
                original_sku_price=url_data['original_sku_price'],
                offer=url_data['offer'],
                pick_up_availability=url_data['pick_up_availability'],
                shipping_availability=url_data['shipping_availability'],
                delivery_availability=url_data['delivery_availability'],
                sku_status=url_data['sku_status'],
                star_rating_source=url_data['star_rating'],
                promotion_type=url_data['promotion_type'],
                promotion_rank=url_data['promotion_rank'],
                bsr_rank=url_data['bsr_rank'],
                main_rank=url_data['main_rank'],
                trend_rank=url_data['trend_rank']
            )

            return True

        except Exception as e:
            print(f"  [ERROR] 상세 페이지 크롤링 실패: {e}")
            import traceback
            traceback.print_exc()
            return False

    def save_to_db(self, page_type, order, retailer_sku_name, item,
                   electricity_use, screen_size, count_of_reviews, star_ratings, top_mentions, detailed_reviews,
                   recommendation_intent, product_url,
                   final_sku_price, savings, original_sku_price, offer,
                   pick_up_availability, shipping_availability, delivery_availability,
                   sku_status, star_rating_source, promotion_type, promotion_rank,
                   bsr_rank, main_rank, trend_rank):
        """DB에 저장"""
        try:
            cursor = self.db_conn.cursor()

            # Calculate calendar week
            calendar_week = f"w{datetime.now().isocalendar().week}"

            # Calculate crawl_strdatetime (format: 202511040300276863)
            now = datetime.now()
            crawl_strdatetime = now.strftime('%Y%m%d%H%M%S') + now.strftime('%f')[:4]

            # 데이터 삽입
            insert_query = """
                INSERT INTO bby_tv_detail_crawled
                (account_name, batch_id, page_type, "order", retailer_sku_name, item,
                 Estimated_Annual_Electricity_Use, screen_size, count_of_reviews, Count_of_Star_Ratings, Top_Mentions,
                 Detailed_Review_Content, Recommendation_Intent, product_url, crawl_strdatetime, calendar_week,
                 final_sku_price, savings, original_sku_price, offer, pick_up_availability, shipping_availability,
                 delivery_availability, sku_status, star_rating, promotion_type, promotion_rank,
                 bsr_rank, main_rank, trend_rank)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
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
                crawl_strdatetime,
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
                main_rank,
                trend_rank
            ))

            cursor.close()
            print(f"  [✓] DB 저장 완료")
            return True

        except Exception as e:
            print(f"  [ERROR] DB 저장 실패: {e}")
            import traceback
            traceback.print_exc()
            return False

    def fill_missing_items(self):
        """빈 item을 이전 세션 데이터로 채우기"""
        try:
            print("\n[INFO] 빈 item 채우는 중...")
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
                print("[OK] 빈 item 없음")
                cursor.close()
                return

            print(f"[INFO] 빈 item {len(empty_items)}개 발견")

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

            print(f"[OK] {updated_count}/{len(empty_items)}개 item 채움 완료")
            return True

        except Exception as e:
            print(f"[ERROR] item 채우기 실패: {e}")
            import traceback
            traceback.print_exc()
            return False

    def run(self):
        """메인 실행"""
        try:
            print("="*80)
            print(f"Best Buy TV Detail Page Crawler (Modified) (Batch ID: {self.batch_id})")
            print("="*80)

            # DB 연결
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

            # 각 URL 크롤링
            success_count = 0
            for url_data in urls:
                if self.scrape_detail_page(url_data):
                    success_count += 1

                # 페이지 간 딜레이
                time.sleep(random.uniform(3, 5))

            print("\n" + "="*80)
            print(f"크롤링 완료! 성공: {success_count}/{len(urls)}개")
            print("="*80)

            # 빈 item 채우기
            self.fill_missing_items()

        except Exception as e:
            print(f"[ERROR] 크롤러 실행 오류: {e}")
            import traceback
            traceback.print_exc()

        finally:
            if self.driver:
                self.driver.quit()
                print("\n[INFO] 드라이버 종료")
            if self.db_conn:
                self.db_conn.close()
                print("[INFO] DB 연결 종료")

def main():
    crawler = BestBuyDetailCrawler()
    crawler.run()

if __name__ == "__main__":
    main()
