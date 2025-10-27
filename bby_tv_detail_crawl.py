"""
Best Buy TV Detail Page Crawler
수집 테이블: bestbuy_tv_main_crawl, bby_tv_trend_crawl, bby_tv_promotion_crawl
저장 테이블: bby_tv_detail_crawled
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

# DB 설정
DB_CONFIG = {
    'host': 'samsung-dx-crawl.csnixzmkuppn.ap-northeast-2.rds.amazonaws.com',
    'port': 5432,
    'database': 'postgres',
    'user': 'postgres',
    'password': 'admin2025!'
}

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
                """, (promo_batch_id,))
                promo_urls = cursor.fetchall()
                urls.extend([('promotion', url[0]) for url in promo_urls])
                print(f"[OK] Promotion URLs (batch {promo_batch_id}): {len(promo_urls)}개")

            cursor.close()
            print(f"[OK] 총 {len(urls)}개 URLs 로드 완료")
            return urls

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
                    time.sleep(1)
                    spec_button.click()
                    print("  [OK] Specification 클릭 성공")
                    time.sleep(5)  # 다이얼로그 로딩 대기 (3초 -> 5초)
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
            # XPath 패턴
            xpath = '//*[@id="user-generated-content-ugc-stats-68760209"]/div/div/span/span/a/span'

            try:
                elem = self.driver.find_element(By.XPATH, xpath)
                text = elem.text.strip()
                if text:
                    return text
            except Exception:
                pass

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
                        # 버튼이 클릭 가능할 때까지 대기
                        wait = WebDriverWait(self.driver, 5)
                        wait.until(EC.element_to_be_clickable((By.XPATH, xpath)))
                        button.click()
                        print("  [OK] See All Customer Reviews 클릭 성공")
                        time.sleep(4)  # 리뷰 페이지 로딩 대기 (3초 -> 4초)
                        return True
                    except:
                        continue

                # 버튼을 못 찾으면 계속 스크롤
                current_position += step
                self.driver.execute_script(f"window.scrollTo(0, {current_position});")
                time.sleep(1.5)  # 스크롤 후 대기 시간 증가 (0.5초 -> 1.5초)

            print("  [WARNING] See All Customer Reviews 버튼을 찾을 수 없습니다.")
            return False

        except Exception as e:
            print(f"  [ERROR] See All Customer Reviews 클릭 실패: {e}")
            return False

    def extract_reviews(self):
        """리뷰 20개 수집 (페이지네이션 포함)"""
        try:
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
                    time.sleep(1)
                    next_button.click()
                    time.sleep(3)
                    page += 1
                except:
                    print("  [INFO] 다음 페이지 버튼이 없습니다. 수집 종료.")
                    break

            # 리뷰를 구분자로 연결
            return " | ".join(reviews) if reviews else None

        except Exception as e:
            print(f"  [ERROR] 리뷰 수집 실패: {e}")
            return None

    def extract_recommendation_intent(self, tree):
        """Recommendation_Intent 추출"""
        try:
            # 여러 XPath 패턴 시도
            xpaths = [
                # HTML 예시에 맞춘 패턴: svg + span.font-weight-bold를 포함하는 div
                '//div[contains(@class, "v-text-dark-gray") and .//svg and .//span[@class="font-weight-bold"]]',
                # svg와 span이 있는 div
                '//div[.//svg[contains(@class, "mr-50")] and .//span[@class="font-weight-bold"]]',
                # "would recommend" 텍스트를 포함하는 div
                '//div[contains(@class, "v-text-dark-gray") and contains(., "would recommend")]',
                # 더 넓은 패턴
                '//div[contains(., "would recommend to a friend")]'
            ]

            for xpath in xpaths:
                elem = tree.xpath(xpath)
                if elem:
                    text = elem[0].text_content().strip()
                    # "83% would recommend to a friend" 형식으로 추출
                    text = ' '.join(text.split())  # 여러 공백을 하나로
                    # 불필요한 줄바꿈 제거
                    text = text.replace('\n', ' ').replace('\r', ' ')
                    text = ' '.join(text.split())
                    return text

            return None

        except Exception as e:
            print(f"  [ERROR] Recommendation intent 추출 실패: {e}")
            return None

    def scrape_detail_page(self, page_type, product_url):
        """상세 페이지 크롤링"""
        try:
            self.order += 1
            print(f"\n[{self.order}] [{page_type}] {product_url[:80]}...")

            # 페이지 접속
            self.driver.get(product_url)
            time.sleep(random.uniform(5, 8))

            # 페이지 소스 가져오기
            page_source = self.driver.page_source
            tree = html.fromstring(page_source)

            # 1. Retailer_SKU_Name 추출
            retailer_sku_name = self.extract_retailer_sku_name(tree)
            print(f"  [✓] Retailer_SKU_Name: {retailer_sku_name}")

            # 2. Specification 버튼 클릭
            samsung_sku_name = None
            electricity_use = None

            if self.click_specifications():
                # 다이얼로그가 완전히 로드될 때까지 대기
                try:
                    # Model Number 요소가 나타날 때까지 최대 10초 대기
                    wait = WebDriverWait(self.driver, 10)
                    wait.until(EC.presence_of_element_located((By.XPATH, '//div[contains(text(), "Model Number")]')))
                    print("  [OK] 다이얼로그 로드 완료")
                except Exception as e:
                    print(f"  [WARNING] 다이얼로그 로딩 대기 타임아웃: {e}")

                time.sleep(2)
                # 다이얼로그 소스 가져오기
                dialog_source = self.driver.page_source
                dialog_tree = html.fromstring(dialog_source)

                # 3. Samsung_SKU_Name 추출
                samsung_sku_name = self.extract_samsung_sku_name(dialog_tree)
                print(f"  [✓] Samsung_SKU_Name: {samsung_sku_name}")

                # 4. Estimated_Annual_Electricity_Use 추출
                electricity_use = self.extract_electricity_use(dialog_tree)
                print(f"  [✓] Estimated_Annual_Electricity_Use: {electricity_use}")

                # 5. 다이얼로그 닫기
                self.close_specifications_dialog()

            # 6. See All Customer Reviews 클릭 및 데이터 수집
            star_ratings = None
            top_mentions = None
            detailed_reviews = None
            recommendation_intent = None

            if self.click_see_all_reviews():
                # 6-1. Star ratings 수집 (리뷰 페이지에서)
                star_ratings = self.extract_star_ratings_from_reviews_page()
                print(f"  [✓] Star_Ratings: {star_ratings}")

                # 6-2. Top mentions 수집 (리뷰 페이지에서)
                top_mentions = self.extract_top_mentions_from_reviews_page()
                print(f"  [✓] Top_Mentions: {top_mentions}")

                # 6-3. Detailed reviews 수집
                detailed_reviews = self.extract_reviews()
                print(f"  [✓] Detailed_Reviews: {len(detailed_reviews) if detailed_reviews else 0} chars")

                # 6-4. Recommendation intent 수집 (리뷰 페이지에서)
                page_source = self.driver.page_source
                tree = html.fromstring(page_source)
                recommendation_intent = self.extract_recommendation_intent(tree)
                print(f"  [✓] Recommendation_Intent: {recommendation_intent}")

            # DB 저장
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
                 Detailed_Review_Content, Recommendation_Intent, product_url)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
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
                product_url
            ))

            cursor.close()
            print(f"  [✓] DB 저장 완료")
            return True

        except Exception as e:
            print(f"  [ERROR] DB 저장 실패: {e}")
            import traceback
            traceback.print_exc()
            return False

    def run(self):
        """메인 실행"""
        try:
            print("="*80)
            print(f"Best Buy TV Detail Page Crawler (Batch ID: {self.batch_id})")
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
            for page_type, url in urls:
                if self.scrape_detail_page(page_type, url):
                    success_count += 1

                # 페이지 간 딜레이
                time.sleep(random.uniform(3, 5))

            print("\n" + "="*80)
            print(f"크롤링 완료! 성공: {success_count}/{len(urls)}개")
            print("="*80)

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
