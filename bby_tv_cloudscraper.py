"""
Best Buy TV Detail Page Crawler
수집 테이블: bestbuy_tv_main_crawl, bby_tv_trend_crawl, bby_tv_promotion_crawl
저장 테이블: bby_tv_detail_crawled, bby_tv_mst

[v3.0] 2025-01-15 개선사항:
- cloudscraper를 사용한 Cloudflare 우회
- while 루프 재시도 로직 추가 (Compare similar products 로딩 실패 시)
- undetected-chromedriver 옵션 강화 (EC2 환경 최적화)
- Compare similar products 스크롤/대기 로직 대폭 개선
- lazy-loading 컨텐츠 완전 로딩을 위한 천천히 스크롤 기능 추가
"""
import time
import random
import re
import os
import psycopg2
from datetime import datetime
import pytz
import cloudscraper
import undetected_chromedriver as uc
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.action_chains import ActionChains
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
        self.wait = None  # WebDriverWait 객체
        self.scraper = None  # cloudscraper 인스턴스
        self.cf_cookies = None  # Cloudflare 우회 쿠키

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

    def init_cloudscraper(self):
        """
        Cloudscraper 초기화 - Cloudflare 우회용

        cloudscraper는 Cloudflare의 JavaScript Challenge를 자동으로 해결하여
        필요한 쿠키(cf_clearance 등)를 획득합니다.
        """
        try:
            print("[INFO] Cloudscraper 초기화 중...")

            # cloudscraper 인스턴스 생성
            self.scraper = cloudscraper.create_scraper(
                browser={
                    'browser': 'chrome',
                    'platform': 'windows',
                    'desktop': True
                },
                delay=10  # JavaScript challenge 해결을 위한 대기 시간
            )

            print("[OK] Cloudscraper 초기화 완료")
            return True

        except Exception as e:
            print(f"[ERROR] Cloudscraper 초기화 실패: {e}")
            return False

    def get_cloudflare_cookies(self, url, max_retries=5):
        """
        Cloudflare 우회 쿠키 획득 - while 루프로 성공할 때까지 재시도

        Args:
            url: 접속할 URL
            max_retries: 최대 재시도 횟수

        Returns:
            성공 시 쿠키 dict, 실패 시 None
        """
        if not self.scraper:
            self.init_cloudscraper()

        retry_count = 0
        while retry_count < max_retries:
            try:
                retry_count += 1
                print(f"  [INFO] Cloudflare 우회 시도 {retry_count}/{max_retries}...")

                # cloudscraper로 페이지 접속
                response = self.scraper.get(url, timeout=30)

                # 응답 확인
                if response.status_code == 200:
                    # 쿠키 추출
                    cookies = self.scraper.cookies.get_dict()

                    # cf_clearance 쿠키가 있는지 확인
                    if 'cf_clearance' in cookies or len(cookies) > 0:
                        print(f"  [OK] Cloudflare 우회 성공! 쿠키 {len(cookies)}개 획득")
                        self.cf_cookies = cookies
                        return cookies
                    else:
                        print(f"  [WARNING] 쿠키가 비어있음, 재시도...")

                elif response.status_code == 403:
                    print(f"  [WARNING] 403 Forbidden - Cloudflare 차단, 재시도...")

                elif response.status_code == 503:
                    print(f"  [WARNING] 503 Service Unavailable - Cloudflare challenge, 재시도...")

                else:
                    print(f"  [WARNING] 응답 코드: {response.status_code}, 재시도...")

            except Exception as e:
                print(f"  [ERROR] Cloudflare 우회 실패: {e}")

            # 재시도 전 대기 (점점 증가)
            wait_time = 5 + (retry_count * 3)  # 8, 11, 14, 17, 20초
            print(f"  [INFO] {wait_time}초 대기 후 재시도...")
            time.sleep(wait_time)

        print(f"  [ERROR] {max_retries}회 시도 후에도 Cloudflare 우회 실패")
        return None

    def apply_cookies_to_driver(self):
        """
        Cloudscraper에서 획득한 쿠키를 Selenium 드라이버에 적용
        """
        try:
            if not self.cf_cookies:
                print("  [WARNING] 적용할 쿠키가 없습니다")
                return False

            # 먼저 bestbuy.com 도메인으로 이동 (쿠키 적용을 위해)
            self.driver.get("https://www.bestbuy.com")
            time.sleep(3)

            # 쿠키 적용
            for name, value in self.cf_cookies.items():
                try:
                    cookie = {
                        'name': name,
                        'value': value,
                        'domain': '.bestbuy.com',
                        'path': '/'
                    }
                    self.driver.add_cookie(cookie)
                    print(f"    [OK] 쿠키 적용: {name}")
                except Exception as e:
                    print(f"    [WARNING] 쿠키 적용 실패 ({name}): {e}")

            # 쿠키 적용 후 페이지 새로고침
            self.driver.refresh()
            time.sleep(5)

            print("  [OK] 쿠키 적용 완료")
            return True

        except Exception as e:
            print(f"  [ERROR] 쿠키 적용 실패: {e}")
            return False

    def setup_driver(self):
        """Chrome 드라이버 설정 - 사용자 프로필 유지 버전"""
        try:
            print("[INFO] Chrome 드라이버 설정 중 (User Profile Mode)...")

            # Chrome 옵션 설정
            options = uc.ChromeOptions()

            # ★ 핵심: Chrome 사용자 프로필 디렉토리 사용
            # 이렇게 하면 쿠키, 세션, 로컬스토리지가 유지됨
            import os
            user_data_dir = os.path.join(os.getcwd(), "chrome_profile_bestbuy")
            if not os.path.exists(user_data_dir):
                os.makedirs(user_data_dir)
            options.add_argument(f'--user-data-dir={user_data_dir}')
            print(f"    [INFO] Chrome 프로필 경로: {user_data_dir}")

            # 기본 옵션
            options.add_argument('--disable-dev-shm-usage')
            options.add_argument('--no-sandbox')
            options.add_argument('--disable-gpu')
            options.add_argument('--window-size=1920,1080')
            options.add_argument('--start-maximized')

            # 언어 설정 (미국 영어)
            options.add_argument('--lang=en-US,en;q=0.9')
            options.add_argument('--accept-lang=en-US,en;q=0.9')

            # 봇 감지 우회를 위한 추가 옵션
            options.add_argument('--disable-blink-features=AutomationControlled')
            options.add_argument('--disable-infobars')

            # User-Agent 설정 (실제 Chrome과 동일하게)
            options.add_argument('--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36')

            # 프로필 설정
            prefs = {
                "profile.default_content_setting_values.notifications": 2,
                "credentials_enable_service": False,
                "profile.password_manager_enabled": False,
                "profile.managed_default_content_settings.images": 1,  # 이미지 로드 허용
                "profile.default_content_setting_values.cookies": 1,  # 쿠키 허용
            }
            options.add_experimental_option("prefs", prefs)

            # undetected-chromedriver 초기화
            self.driver = uc.Chrome(
                options=options,
                use_subprocess=True,  # 서브프로세스 사용 (더 안정적)
            )

            # WebDriverWait 설정
            self.wait = WebDriverWait(self.driver, 20)

            # 페이지 로드 타임아웃 설정
            self.driver.set_page_load_timeout(60)

            # 창 최대화
            self.driver.maximize_window()

            # 추가 stealth 스크립트 실행
            self.driver.execute_script("""
                // navigator.webdriver 숨기기
                Object.defineProperty(navigator, 'webdriver', {
                    get: () => undefined
                });

                // plugins 배열 수정
                Object.defineProperty(navigator, 'plugins', {
                    get: () => [1, 2, 3, 4, 5]
                });

                // languages 수정
                Object.defineProperty(navigator, 'languages', {
                    get: () => ['en-US', 'en']
                });

                // Chrome 객체 추가
                window.chrome = {
                    runtime: {},
                    loadTimes: function() {},
                    csi: function() {},
                    app: {}
                };

                // permissions query 수정
                const originalQuery = window.navigator.permissions.query;
                window.navigator.permissions.query = (parameters) => (
                    parameters.name === 'notifications' ?
                        Promise.resolve({ state: Notification.permission }) :
                        originalQuery(parameters)
                );
            """)

            print("[OK] 드라이버 설정 완료 (Enhanced Stealth Mode)")
            return True

        except Exception as e:
            print(f"[ERROR] 드라이버 설정 실패: {e}")
            import traceback
            traceback.print_exc()
            return False

    def slow_scroll_page(self, scroll_pause=1.5, scroll_step=300):
        """
        페이지를 천천히 스크롤하여 모든 lazy-loading 컨텐츠 로드

        Args:
            scroll_pause: 각 스크롤 후 대기 시간 (초)
            scroll_step: 한 번에 스크롤할 픽셀 수
        """
        try:
            print("  [INFO] 페이지 전체 스크롤 시작 (lazy-loading 컨텐츠 로드)...")

            # 현재 페이지 높이
            last_height = self.driver.execute_script("return document.body.scrollHeight")
            current_position = 0
            scroll_count = 0

            while current_position < last_height:
                # 스크롤 수행
                current_position += scroll_step
                self.driver.execute_script(f"window.scrollTo(0, {current_position});")
                scroll_count += 1

                # 대기
                time.sleep(scroll_pause)

                # 새로운 컨텐츠가 로드되었는지 확인
                new_height = self.driver.execute_script("return document.body.scrollHeight")
                if new_height > last_height:
                    print(f"    [DEBUG] 새 컨텐츠 로드됨: {last_height} → {new_height}")
                    last_height = new_height

                # 진행 상황 표시 (10번마다)
                if scroll_count % 10 == 0:
                    progress = min(100, int(current_position / last_height * 100))
                    print(f"    [DEBUG] 스크롤 진행: {progress}%")

            # 페이지 맨 위로 돌아가기
            self.driver.execute_script("window.scrollTo(0, 0);")
            time.sleep(1)

            print(f"  [OK] 페이지 스크롤 완료 ({scroll_count}회 스크롤)")
            return True

        except Exception as e:
            print(f"  [WARNING] 페이지 스크롤 중 오류: {e}")
            return False

    def wait_for_element_with_retry(self, xpaths, timeout=10, retry=3):
        """
        여러 XPath 중 하나라도 찾을 때까지 재시도

        Args:
            xpaths: 찾을 XPath 목록
            timeout: 각 시도의 타임아웃 (초)
            retry: 재시도 횟수

        Returns:
            찾은 요소 또는 None
        """
        for attempt in range(retry):
            for xpath in xpaths:
                try:
                    element = WebDriverWait(self.driver, timeout).until(
                        EC.presence_of_element_located((By.XPATH, xpath))
                    )
                    return element
                except:
                    continue

            if attempt < retry - 1:
                print(f"    [DEBUG] 요소 찾기 재시도 ({attempt + 2}/{retry})...")
                time.sleep(2)

        return None

    def get_recent_urls(self):
        """최신 batch_id의 product URLs 가져오기"""
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

            print(f"[INFO] Latest batch_id - Main: {main_batch_id}, Trend: {trend_batch_id}, Promotion: {promo_batch_id}")

            # bestbuy_tv_main_crawl에서 해당 batch의 URLs 가져오기
            if main_batch_id:
                cursor.execute("""
                    SELECT DISTINCT product_url
                    FROM bestbuy_tv_main_crawl
                    WHERE batch_id = %s
                    AND product_url IS NOT NULL
                    ORDER BY product_url
                """, (main_batch_id,))
                main_urls = cursor.fetchall()
                urls.extend([('main', url[0]) for url in main_urls])
                print(f"[OK] Main URLs (batch {main_batch_id}): {len(main_urls)}개")

            # bby_tv_Trend_crawl에서 해당 batch의 URLs 가져오기
            if trend_batch_id:
                cursor.execute("""
                    SELECT DISTINCT product_url
                    FROM bby_tv_Trend_crawl
                    WHERE batch_id = %s
                    AND product_url IS NOT NULL
                    ORDER BY product_url
                """, (trend_batch_id,))
                trend_urls = cursor.fetchall()
                urls.extend([('Trend', url[0]) for url in trend_urls])
                print(f"[OK] Trend URLs (batch {trend_batch_id}): {len(trend_urls)}개")

            # bby_tv_promotion_crawl에서 해당 batch의 URLs 가져오기
            if promo_batch_id:
                cursor.execute("""
                    SELECT DISTINCT product_url
                    FROM bby_tv_promotion_crawl
                    WHERE batch_id = %s
                    AND product_url IS NOT NULL
                    ORDER BY product_url
                """, (promo_batch_id,))
                promo_urls = cursor.fetchall()
                urls.extend([('promotion', url[0]) for url in promo_urls])
                print(f"[OK] Promotion URLs (batch {promo_batch_id}): {len(promo_urls)}개")

            cursor.close()

            # Remove duplicate URLs across tables (keep first occurrence)
            seen_urls = set()
            unique_urls = []
            duplicates_count = 0

            for mother, url in urls:
                if url not in seen_urls:
                    seen_urls.add(url)
                    unique_urls.append((mother, url))
                else:
                    duplicates_count += 1

            if duplicates_count > 0:
                print(f"[INFO] Removed {duplicates_count} duplicate URLs across tables")

            print(f"[OK] 총 {len(unique_urls)}개 unique URLs 로드 완료 (중복 제거 전: {len(urls)}개)")
            return unique_urls

        except Exception as e:
            print(f"[ERROR] Failed to load URLs: {e}")
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

    def extract_samsung_sku_name(self, tree):
        """Samsung_SKU_Name (Model Number) 추출"""
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
            print(f"  [ERROR] Samsung_SKU_Name 추출 실패: {e}")
            return None

    def extract_electricity_use(self, tree):
        """Estimated_Annual_Electricity_Use 추출"""
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
                        return electricity
            return None
        except Exception as e:
            print(f"  [ERROR] Estimated_Annual_Electricity_Use 추출 실패: {e}")
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

    def verify_compare_section_loaded(self):
        """
        Compare similar products 섹션이 제대로 로드되었는지 검증

        Returns:
            True: 섹션이 정상 로드됨 (이미지와 데이터 포함)
            False: 섹션이 없거나 "(undefined)" 등 로드 실패
        """
        try:
            page_source = self.driver.page_source

            # 1. "Compare similar products" 텍스트가 있는지 확인
            if "Compare similar products" not in page_source:
                print("    [VERIFY] Compare similar products 섹션 없음")
                return False

            # 2. "(undefined)" 텍스트가 있으면 로드 실패
            if "(undefined)" in page_source:
                print("    [VERIFY] (undefined) 발견 - 로드 실패")
                return False

            # 3. 제품 이미지가 로드되었는지 확인 (img 태그에 src가 있는지)
            tree = html.fromstring(page_source)

            # Compare 섹션 내 이미지 찾기
            compare_images = tree.xpath('//div[contains(@class, "compare")]//img/@src')
            if not compare_images:
                compare_images = tree.xpath('//div[contains(@class, "product-title")]//ancestor::div[contains(@class, "compare")]//img/@src')

            # 이미지 src가 있고, placeholder가 아닌지 확인
            valid_images = [img for img in compare_images if img and 'placeholder' not in img.lower() and 'data:' not in img]

            if len(valid_images) < 2:
                print(f"    [VERIFY] 유효한 이미지 부족: {len(valid_images)}개")
                return False

            # 4. 제품명이 로드되었는지 확인
            product_names = tree.xpath('//span[@class="clamp" and starts-with(@id, "compare-title-")]')
            if len(product_names) < 2:
                print(f"    [VERIFY] 제품명 부족: {len(product_names)}개")
                return False

            print(f"    [VERIFY] 섹션 로드 성공! 이미지: {len(valid_images)}개, 제품명: {len(product_names)}개")
            return True

        except Exception as e:
            print(f"    [VERIFY] 검증 중 오류: {e}")
            return False

    def load_page_with_retry(self, url, max_retries=5):
        """
        페이지 로드 with Cloudflare 우회 재시도 로직

        Compare similar products 섹션이 제대로 로드될 때까지 재시도

        Args:
            url: 로드할 URL
            max_retries: 최대 재시도 횟수

        Returns:
            True: 성공, False: 실패
        """
        retry_count = 0

        while retry_count < max_retries:
            retry_count += 1
            print(f"\n  [RETRY {retry_count}/{max_retries}] 페이지 로드 시도...")

            try:
                # 1. 페이지 접속
                self.driver.get(url)

                # 2. 초기 로딩 대기
                initial_wait = random.uniform(8, 12)
                print(f"    [INFO] 초기 로딩 대기... ({initial_wait:.1f}초)")
                time.sleep(initial_wait)

                # 3. 페이지 전체 스크롤 (lazy-loading 트리거)
                self.slow_scroll_page(scroll_pause=1.5, scroll_step=400)

                # 4. Compare similar products 섹션으로 스크롤
                self.scroll_to_compare_section()

                # 5. 추가 대기 (이미지 로딩)
                time.sleep(5)

                # 6. 섹션 로드 검증
                if self.verify_compare_section_loaded():
                    print(f"  [OK] 페이지 로드 성공 (시도 {retry_count}회)")
                    return True
                else:
                    print(f"    [WARNING] Compare 섹션 로드 실패, 재시도...")

            except Exception as e:
                print(f"    [ERROR] 페이지 로드 오류: {e}")

            # 재시도 전 Cloudflare 쿠키 갱신 시도
            if retry_count < max_retries:
                wait_time = 10 + (retry_count * 5)  # 15, 20, 25, 30초
                print(f"    [INFO] {wait_time}초 대기 후 재시도...")
                time.sleep(wait_time)

                # Cloudflare 쿠키 갱신
                print("    [INFO] Cloudflare 쿠키 갱신 시도...")
                if self.get_cloudflare_cookies(url, max_retries=2):
                    self.apply_cookies_to_driver()

        print(f"  [ERROR] {max_retries}회 시도 후에도 페이지 로드 실패")
        return False

    def scroll_to_compare_section(self):
        """
        Compare similar products 섹션으로 스크롤
        """
        try:
            # 섹션 헤더 찾기
            compare_section_xpaths = [
                "//h2[contains(text(), 'Compare similar products')]",
                "//h2[contains(text(), 'Compare Similar Products')]",
                "//div[contains(@class, 'compare-similar')]//h2",
            ]

            for xpath in compare_section_xpaths:
                try:
                    compare_header = self.driver.find_element(By.XPATH, xpath)
                    if compare_header:
                        self.driver.execute_script(
                            "arguments[0].scrollIntoView({block: 'center', behavior: 'smooth'});",
                            compare_header
                        )
                        time.sleep(3)
                        self.driver.execute_script("window.scrollBy(0, -200);")
                        time.sleep(2)
                        print("    [OK] Compare 섹션으로 스크롤 완료")
                        return True
                except:
                    continue

            # 헤더를 못 찾으면 페이지 40~60% 위치로 스크롤
            total_height = self.driver.execute_script("return document.body.scrollHeight")
            for target_percent in [40, 50, 60]:
                scroll_to = int(total_height * target_percent / 100)
                self.driver.execute_script(f"window.scrollTo(0, {scroll_to});")
                time.sleep(2)

            return False

        except Exception as e:
            print(f"    [WARNING] Compare 섹션 스크롤 실패: {e}")
            return False

    def extract_compare_similar_products(self, current_url):
        """
        Compare similar products 섹션 데이터 추출 - 개선 버전

        [개선사항]
        - 페이지 전체를 천천히 스크롤하여 lazy-loading 컨텐츠 로드
        - "Compare similar products" 헤더를 찾아 해당 위치로 스크롤
        - 이미지/데이터 로딩을 위한 충분한 대기 시간
        - 재시도 로직 추가
        """
        try:
            print("  [INFO] Compare similar products 섹션 찾는 중...")

            # 1. 먼저 페이지 맨 위로
            self.driver.execute_script("window.scrollTo(0, 0);")
            time.sleep(2)

            # 2. 페이지를 천천히 스크롤하면서 lazy-loading 컨텐츠 로드
            total_height = self.driver.execute_script("return document.body.scrollHeight")
            print(f"    [DEBUG] 페이지 전체 높이: {total_height}px")

            # 단계별 스크롤 (10%, 20%, 30%, ..., 100%)
            for percent in range(10, 110, 10):
                scroll_to = int(total_height * percent / 100)
                self.driver.execute_script(f"window.scrollTo(0, {scroll_to});")
                time.sleep(1.5)  # lazy-loading 대기

                # 새로운 컨텐츠가 로드되면 높이 업데이트
                new_height = self.driver.execute_script("return document.body.scrollHeight")
                if new_height > total_height:
                    total_height = new_height
                    print(f"    [DEBUG] 새 컨텐츠 로드: 높이 {new_height}px")

            # 3. "Compare similar products" 섹션 헤더 찾기
            compare_section_xpaths = [
                "//h2[contains(text(), 'Compare similar products')]",
                "//h2[contains(text(), 'Compare Similar Products')]",
                "//div[contains(@class, 'compare-similar')]//h2",
                "//section[contains(@class, 'compare')]//h2",
            ]

            compare_header = None
            for xpath in compare_section_xpaths:
                try:
                    compare_header = self.driver.find_element(By.XPATH, xpath)
                    if compare_header:
                        print("    [OK] Compare similar products 헤더 발견")
                        break
                except:
                    continue

            # 4. 헤더를 찾았으면 해당 위치로 스크롤
            if compare_header:
                # 헤더 위로 스크롤 (center로 위치시킴)
                self.driver.execute_script(
                    "arguments[0].scrollIntoView({block: 'center', behavior: 'smooth'});",
                    compare_header
                )
                time.sleep(3)

                # 추가로 약간 위로 스크롤 (섹션 전체가 보이도록)
                self.driver.execute_script("window.scrollBy(0, -200);")
                time.sleep(2)
            else:
                # 헤더를 못 찾으면 페이지 40~60% 위치로 스크롤 (보통 그 위치에 있음)
                print("    [WARNING] Compare 헤더 못찾음, 페이지 중간으로 스크롤...")
                for target_percent in [40, 50, 60]:
                    scroll_to = int(total_height * target_percent / 100)
                    self.driver.execute_script(f"window.scrollTo(0, {scroll_to});")
                    time.sleep(3)

            # 5. 이미지 로딩을 위한 추가 대기
            print("    [INFO] 이미지 및 데이터 로딩 대기 중...")
            time.sleep(5)

            # 6. 페이지 소스 가져오기
            page_source = self.driver.page_source
            tree = html.fromstring(page_source)

            # 4개 제품 데이터 저장
            products = []

            # 7. product-title div들 찾기 (여러 패턴 시도)
            product_div_xpaths = [
                '//div[@class="product-title font-weight-normal pb-100 body-copy-lg min-h-600"]',
                '//div[contains(@class, "product-title") and contains(@class, "min-h-600")]',
                '//div[contains(@class, "compare-similar")]//div[contains(@class, "product-title")]',
            ]

            product_divs = []
            for xpath in product_div_xpaths:
                product_divs = tree.xpath(xpath)
                if len(product_divs) >= 4:
                    print(f"    [OK] 제품 div 찾음: {len(product_divs)}개 (xpath: {xpath[:50]}...)")
                    break

            if len(product_divs) < 4:
                # 재시도: 추가 대기 후 다시 시도
                print(f"    [WARNING] 제품 div 부족 ({len(product_divs)}개), 재시도 중...")
                time.sleep(5)

                page_source = self.driver.page_source
                tree = html.fromstring(page_source)

                for xpath in product_div_xpaths:
                    product_divs = tree.xpath(xpath)
                    if len(product_divs) >= 4:
                        break

            if len(product_divs) < 4:
                print(f"  [WARNING] Compare similar products 섹션을 찾을 수 없거나 제품이 부족합니다. (찾은 개수: {len(product_divs)})")
                # 디버깅: HTML 일부 출력
                debug_html = page_source[max(0, page_source.find('Compare similar')-100):page_source.find('Compare similar')+500] if 'Compare similar' in page_source else "섹션 없음"
                print(f"    [DEBUG] HTML snippet: {debug_html[:200]}...")
                return None

            # 8. 첫 번째 제품 (현재 페이지)
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

            # 9. 2-4번째 제품
            for i in range(1, 4):
                if i < len(product_divs):
                    product = {
                        'product_url': None,
                        'product_name': None,
                        'pros': None,
                        'cons': None
                    }

                    # a 태그에서 URL과 제품명 추출 (여러 패턴)
                    a_elem = product_divs[i].xpath('.//a[@class="clamp"]')
                    if not a_elem:
                        a_elem = product_divs[i].xpath('.//a[contains(@class, "clamp")]')
                    if not a_elem:
                        a_elem = product_divs[i].xpath('.//a')

                    if a_elem:
                        href = a_elem[0].get('href')
                        if href:
                            # 상대 경로면 절대 경로로 변환
                            if href.startswith('/'):
                                href = f"https://www.bestbuy.com{href}"
                            product['product_url'] = href
                        product['product_name'] = a_elem[0].text_content().strip()

                    products.append(product)

            # 10. Pros/Cons 추출 (여러 XPath 패턴 시도)
            # Pros 추출
            pros_xpaths_patterns = [
                '/html/body/div[5]/div[6]/div/table/tbody/tr[2]/td[{i}]/span/span',
                '//table//tr[2]/td[{i}]//span/span',
                '//tr[contains(@class, "pros")]//td[{i}]//span',
                '//div[contains(@class, "compare")]//table//tr[2]//td[{i}]//span',
            ]

            for pattern in pros_xpaths_patterns:
                pros_found = False
                for i in range(1, 5):
                    pros_xpath = pattern.format(i=i)
                    pros_elem = tree.xpath(pros_xpath)
                    if pros_elem and i-1 < len(products):
                        products[i-1]['pros'] = pros_elem[0].text_content().strip()
                        pros_found = True

                if pros_found:
                    break

            # Cons 추출
            cons_xpaths_patterns = [
                '/html/body/div[5]/div[6]/div/table/tbody/tr[4]/td[{i}]/span/span',
                '//table//tr[4]/td[{i}]//span/span',
                '//tr[contains(@class, "cons")]//td[{i}]//span',
                '//div[contains(@class, "compare")]//table//tr[4]//td[{i}]//span',
            ]

            for pattern in cons_xpaths_patterns:
                cons_found = False
                for i in range(1, 5):
                    cons_xpath = pattern.format(i=i)
                    cons_elem = tree.xpath(cons_xpath)
                    if cons_elem and i-1 < len(products):
                        text = cons_elem[0].text_content().strip()
                        # '—' 같은 값은 None으로 처리
                        products[i-1]['cons'] = text if text and text != '—' else None
                        cons_found = True

                if cons_found:
                    break

            print(f"  [OK] Compare similar products 데이터 추출 완료 ({len(products)}개)")
            for idx, p in enumerate(products):
                print(f"    [{idx+1}] {p['product_name'][:40] if p['product_name'] else 'N/A'}...")

            return products

        except Exception as e:
            print(f"  [ERROR] Compare similar products 추출 실패: {e}")
            import traceback
            traceback.print_exc()
            return None

    def get_samsung_sku_by_product_name(self, product_name):
        """bby_tv_detail_crawled에서 product_name으로 samsung_sku_name 찾기"""
        try:
            if not product_name:
                return None

            cursor = self.db_conn.cursor()

            # 가장 최근 데이터에서 retailer_sku_name과 product_name이 일치하는 것 찾기
            cursor.execute("""
                SELECT Samsung_SKU_Name
                FROM bby_tv_detail_crawled
                WHERE Retailer_SKU_Name = %s
                AND Samsung_SKU_Name IS NOT NULL
                ORDER BY crawl_datetime DESC
                LIMIT 1
            """, (product_name,))

            result = cursor.fetchone()
            cursor.close()

            if result:
                return result[0]
            return None

        except Exception as e:
            print(f"  [ERROR] Samsung SKU 조회 실패 ({product_name}): {e}")
            return None

    def save_to_mst_table(self, products, current_samsung_sku):
        """bby_tv_mst 테이블에 4개 제품 데이터 저장"""
        try:
            cursor = self.db_conn.cursor()

            # 테이블 존재 확인 및 생성
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS bby_tv_mst (
                    id SERIAL PRIMARY KEY,
                    sku VARCHAR(255),
                    product_url TEXT,
                    pros TEXT,
                    cons TEXT,
                    product_name TEXT,
                    update_date VARCHAR(50)
                )
            """)

            # current timestamp
            update_date = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

            # Calculate calendar week
            calendar_week = f"w{datetime.now().isocalendar().week}"

            # 각 제품 저장
            for idx, product in enumerate(products):
                # SKU 결정
                if idx == 0:
                    # 첫 번째 제품은 현재 페이지의 samsung_sku_name
                    sku = current_samsung_sku
                else:
                    # 2-4번째 제품은 DB에서 찾기
                    sku = self.get_samsung_sku_by_product_name(product['product_name'])

                # 데이터 삽입
                insert_query = """
                    INSERT INTO bby_tv_mst
                    (sku, product_url, pros, cons, product_name, update_date, calendar_week)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                """

                cursor.execute(insert_query, (
                    sku,
                    product['product_url'],
                    product['pros'],
                    product['cons'],
                    product['product_name'],
                    update_date,
                    calendar_week
                ))

                print(f"    [MST {idx+1}/4] {product['product_name'][:50]}... (SKU: {sku})")

            cursor.close()
            print(f"  [✓] MST 테이블 저장 완료 (4개)")
            return True

        except Exception as e:
            print(f"  [ERROR] MST 테이블 저장 실패: {e}")
            import traceback
            traceback.print_exc()
            return False

    def scrape_detail_page(self, page_type, product_url):
        """
        상세 페이지 크롤링 - v3.0 Cloudflare 우회 버전

        Compare similar products 섹션이 로드될 때까지 while 루프로 재시도
        """
        try:
            self.order += 1
            print(f"\n[{self.order}] [{page_type}] {product_url[:80]}...")

            # ★ Cloudflare 우회 + Compare 섹션 로드 재시도 로직
            page_loaded = self.load_page_with_retry(product_url, max_retries=5)

            if not page_loaded:
                print("  [WARNING] Compare 섹션 로드 실패, 기본 데이터만 수집 시도...")
                # 기본 페이지 로드만 시도
                self.driver.get(product_url)
                time.sleep(10)

            # 페이지 로드 완료 대기
            try:
                WebDriverWait(self.driver, 30).until(
                    lambda d: d.execute_script("return document.readyState") == "complete"
                )
                print("  [OK] 페이지 로드 완료")
            except:
                print("  [WARNING] 페이지 로드 타임아웃, 계속 진행...")

            # 랜덤 마우스 움직임 (봇 감지 우회)
            try:
                action = ActionChains(self.driver)
                action.move_by_offset(random.randint(100, 300), random.randint(100, 300)).perform()
                time.sleep(0.5)
                action.move_by_offset(random.randint(-50, 50), random.randint(-50, 50)).perform()
            except:
                pass

            # 페이지 소스 가져오기
            page_source = self.driver.page_source
            tree = html.fromstring(page_source)

            # 1. Retailer_SKU_Name 추출
            retailer_sku_name = self.extract_retailer_sku_name(tree)
            print(f"  [✓] Retailer_SKU_Name: {retailer_sku_name}")

            # 2. Compare similar products 추출 (개선된 함수 사용)
            mst_products = self.extract_compare_similar_products(product_url)

            # 3. Specification 버튼 클릭
            samsung_sku_name = None
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

                # 4. Samsung_SKU_Name 추출
                samsung_sku_name = self.extract_samsung_sku_name(dialog_tree)
                print(f"  [✓] Samsung_SKU_Name: {samsung_sku_name}")

                # 5. Estimated_Annual_Electricity_Use 추출
                electricity_use = self.extract_electricity_use(dialog_tree)
                print(f"  [✓] Estimated_Annual_Electricity_Use: {electricity_use}")

                # 6. 다이얼로그 닫기
                self.close_specifications_dialog()

            # 7. MST 테이블에 저장 (samsung_sku_name이 있고 mst_products가 있을 때)
            if mst_products and samsung_sku_name:
                self.save_to_mst_table(mst_products, samsung_sku_name)

            # 8. See All Customer Reviews 클릭 및 데이터 수집
            star_ratings = None
            top_mentions = None
            detailed_reviews = None
            recommendation_intent = None

            if self.click_see_all_reviews():
                # 8-1. Star ratings 수집 (리뷰 페이지에서)
                star_ratings = self.extract_star_ratings_from_reviews_page()
                print(f"  [✓] Star_Ratings: {star_ratings}")

                # 8-2. Top mentions 수집 (리뷰 페이지에서)
                top_mentions = self.extract_top_mentions_from_reviews_page()
                print(f"  [✓] Top_Mentions: {top_mentions}")

                # 8-3. Recommendation intent 수집 (리뷰 페이지에서)
                recommendation_intent = self.extract_recommendation_intent_from_reviews_page()
                print(f"  [✓] Recommendation_Intent: {recommendation_intent}")

                # 8-4. Detailed reviews 수집
                detailed_reviews = self.extract_reviews()
                print(f"  [✓] Detailed_Reviews: {len(detailed_reviews) if detailed_reviews else 0} chars")

            # 9. Detail DB 저장
            self.save_to_db(
                page_type=page_type,
                order=self.order,
                retailer_sku_name=retailer_sku_name,
                samsung_sku_name=samsung_sku_name,
                electricity_use=electricity_use,
                star_ratings=star_ratings,
                top_mentions=top_mentions,
                detailed_reviews=detailed_reviews,
                recommendation_intent=recommendation_intent,
                product_url=product_url
            )

            return True

        except Exception as e:
            print(f"  [ERROR] 상세 페이지 크롤링 실패: {e}")
            import traceback
            traceback.print_exc()
            return False

    def save_to_db(self, page_type, order, retailer_sku_name, samsung_sku_name,
                   electricity_use, star_ratings, top_mentions, detailed_reviews,
                   recommendation_intent, product_url):
        """DB에 저장"""
        try:
            cursor = self.db_conn.cursor()

            # Calculate calendar week
            calendar_week = f"w{datetime.now().isocalendar().week}"

            # 테이블 존재 확인 및 생성
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS bby_tv_detail_crawled (
                    id SERIAL PRIMARY KEY,
                    batch_id VARCHAR(50),
                    page_type VARCHAR(50),
                    "order" INTEGER,
                    Retailer_SKU_Name TEXT,
                    Samsung_SKU_Name TEXT,
                    Estimated_Annual_Electricity_Use TEXT,
                    Count_of_Star_Ratings TEXT,
                    Top_Mentions TEXT,
                    Detailed_Review_Content TEXT,
                    Recommendation_Intent TEXT,
                    product_url TEXT,
                    crawl_datetime TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)

            # 데이터 삽입
            insert_query = """
                INSERT INTO bby_tv_detail_crawled
                (batch_id, page_type, "order", Retailer_SKU_Name, Samsung_SKU_Name,
                 Estimated_Annual_Electricity_Use, Count_of_Star_Ratings, Top_Mentions,
                 Detailed_Review_Content, Recommendation_Intent, product_url, calendar_week)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """

            cursor.execute(insert_query, (
                self.batch_id,
                page_type,
                order,
                retailer_sku_name,
                samsung_sku_name,
                electricity_use,
                star_ratings,
                top_mentions,
                detailed_reviews,
                recommendation_intent,
                product_url,
                calendar_week
            ))

            cursor.close()
            print(f"  [✓] DB 저장 완료")
            return True

        except Exception as e:
            print(f"  [ERROR] DB 저장 실패: {e}")
            import traceback
            traceback.print_exc()
            return False

    def fill_missing_skus(self):
        """빈 SKU를 이전 세션 데이터로 채우기"""
        try:
            print("\n[INFO] 빈 SKU 채우는 중...")
            cursor = self.db_conn.cursor()

            # 현재 세션에서 sku가 NULL인 레코드 찾기
            cursor.execute("""
                SELECT id, product_name
                FROM bby_tv_mst
                WHERE sku IS NULL
                AND product_name IS NOT NULL
            """)

            empty_skus = cursor.fetchall()

            if not empty_skus:
                print("[OK] 빈 SKU 없음")
                cursor.close()
                return

            print(f"[INFO] 빈 SKU {len(empty_skus)}개 발견")

            updated_count = 0
            for record_id, product_name in empty_skus:
                # 이전 세션에서 같은 product_name을 가진 레코드의 sku 찾기
                cursor.execute("""
                    SELECT sku
                    FROM bby_tv_mst
                    WHERE product_name = %s
                    AND sku IS NOT NULL
                    ORDER BY id DESC
                    LIMIT 1
                """, (product_name,))

                result = cursor.fetchone()
                if result:
                    sku = result[0]
                    # UPDATE
                    cursor.execute("""
                        UPDATE bby_tv_mst
                        SET sku = %s
                        WHERE id = %s
                    """, (sku, record_id))
                    updated_count += 1
                    print(f"  [✓] Updated: {product_name[:50]}... → SKU: {sku}")

            self.db_conn.commit()
            cursor.close()

            print(f"[OK] {updated_count}/{len(empty_skus)}개 SKU 채움 완료")
            return True

        except Exception as e:
            print(f"[ERROR] SKU 채우기 실패: {e}")
            import traceback
            traceback.print_exc()
            return False

    def warmup_session(self):
        """
        BestBuy 세션 워밍업 - Cloudflare 우회 + 쿠키 초기화

        1. cloudscraper로 Cloudflare 우회 쿠키 획득
        2. Selenium에 쿠키 적용
        3. 홈페이지/카테고리 방문으로 세션 활성화
        """
        try:
            print("\n[INFO] 세션 워밍업 시작 (Cloudflare 우회)...")

            # ★ 1. Cloudscraper로 Cloudflare 우회 쿠키 획득
            print("  [1/4] Cloudflare 우회 쿠키 획득 중...")
            test_url = "https://www.bestbuy.com/site/samsung-65-class-cu7000-crystal-uhd-4k-smart-tizen-tv/6536733.p"

            retry_count = 0
            max_retries = 10  # 최대 10회 재시도

            while retry_count < max_retries:
                retry_count += 1
                print(f"    [시도 {retry_count}/{max_retries}] Cloudflare 우회 중...")

                if self.get_cloudflare_cookies(test_url, max_retries=3):
                    print("    [OK] Cloudflare 쿠키 획득 성공")
                    break
                else:
                    wait_time = 15 + (retry_count * 5)
                    print(f"    [WARNING] 실패, {wait_time}초 후 재시도...")
                    time.sleep(wait_time)

            # ★ 2. Selenium에 쿠키 적용
            print("  [2/4] Selenium에 쿠키 적용 중...")
            self.apply_cookies_to_driver()

            # 3. BestBuy 홈페이지 방문
            print("  [3/4] 홈페이지 방문 중...")
            self.driver.get("https://www.bestbuy.com")
            time.sleep(random.uniform(5, 8))

            # 쿠키 동의 팝업 처리 (있으면)
            try:
                accept_btn = self.driver.find_element(By.XPATH, "//button[contains(text(), 'Accept')]")
                accept_btn.click()
                time.sleep(2)
            except:
                pass

            # 페이지 스크롤 (사람처럼 행동)
            self.driver.execute_script("window.scrollTo(0, 500);")
            time.sleep(2)
            self.driver.execute_script("window.scrollTo(0, 1000);")
            time.sleep(2)
            self.driver.execute_script("window.scrollTo(0, 0);")
            time.sleep(1)

            # ★ 4. 테스트 제품 페이지에서 Compare similar products 로딩 테스트
            print("  [4/4] Compare similar products 로딩 테스트...")

            # while 루프로 Compare 섹션이 로드될 때까지 재시도
            test_retry = 0
            test_max_retries = 5

            while test_retry < test_max_retries:
                test_retry += 1
                print(f"    [테스트 {test_retry}/{test_max_retries}] 제품 페이지 로드 중...")

                self.driver.get(test_url)
                time.sleep(random.uniform(10, 15))

                # 전체 페이지 스크롤
                total_height = self.driver.execute_script("return document.body.scrollHeight")
                for percent in range(10, 110, 10):
                    scroll_to = int(total_height * percent / 100)
                    self.driver.execute_script(f"window.scrollTo(0, {scroll_to});")
                    time.sleep(1.5)

                # Compare 섹션으로 스크롤
                self.scroll_to_compare_section()
                time.sleep(5)

                # 검증
                if self.verify_compare_section_loaded():
                    print("  [OK] 워밍업 성공! Compare similar products 섹션 정상 로드")
                    return True
                else:
                    print(f"    [WARNING] Compare 섹션 로드 실패, 재시도...")

                    # Cloudflare 쿠키 재획득 시도
                    if test_retry < test_max_retries:
                        wait_time = 20 + (test_retry * 10)
                        print(f"    [INFO] {wait_time}초 대기 후 쿠키 재획득...")
                        time.sleep(wait_time)

                        if self.get_cloudflare_cookies(test_url, max_retries=3):
                            self.apply_cookies_to_driver()

            print("  [WARNING] 워밍업 후에도 Compare similar products 섹션 로드 실패")
            print("  [INFO] 그래도 크롤링을 계속 진행합니다...")
            return False

        except Exception as e:
            print(f"  [ERROR] 워밍업 실패: {e}")
            import traceback
            traceback.print_exc()
            return False

    def run(self):
        """메인 실행"""
        try:
            print("="*80)
            print(f"Best Buy TV Detail Page Crawler v3.0 (Batch ID: {self.batch_id})")
            print("Cloudflare 우회 + Compare similar products 재시도 로직 포함")
            print("="*80)

            # DB 연결
            if not self.connect_db():
                return

            # URLs 가져오기
            urls = self.get_recent_urls()
            if not urls:
                print("[ERROR] No URLs found")
                return

            # ★ Cloudscraper 초기화 (Cloudflare 우회용)
            print("\n[INFO] Cloudscraper 초기화...")
            self.init_cloudscraper()

            # 드라이버 설정
            if not self.setup_driver():
                return

            # ★ 세션 워밍업 (Cloudflare 우회 + 쿠키 초기화)
            self.warmup_session()

            # 각 URL 크롤링
            success_count = 0
            for page_type, url in urls:
                if self.scrape_detail_page(page_type, url):
                    success_count += 1

                # 페이지 간 딜레이
                time.sleep(random.uniform(3, 5))

            print("\n" + "="*80)
            print(f"크롤링 완료! 성공: {success_count}/{len(urls)}개")
            print("="*80)

            # 빈 SKU 채우기
            self.fill_missing_skus()

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
