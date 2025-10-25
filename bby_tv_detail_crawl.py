"""
Best Buy TV Detail Page Crawler
수집 테이블: bestbuy_tv_main_crawl, bby_tv_trend_crawl, bby_tv_promotion_crawl
저장 테이블: bby_tv_detail_crawled
"""
import time
import random
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
            print("🔧 Chrome 드라이버 설정 중...")
            self.driver = uc.Chrome()
            self.driver.maximize_window()
            print("✅ 드라이버 설정 완료")
            return True
        except Exception as e:
            print(f"❌ 드라이버 설정 실패: {e}")
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
                    time.sleep(3)  # 다이얼로그 로딩 대기
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
            # General 섹션에서 Model Number 찾기
            xpaths = [
                '//li[.//h4[text()="General"]]//div[.//div[text()="Model Number"]]//div[@class="grow basis-none pl-300"]',
                '//div[contains(text(), "Model Number")]/following-sibling::div[@class="grow basis-none pl-300"]'
            ]
            for xpath in xpaths:
                elem = tree.xpath(xpath)
                if elem:
                    return elem[0].text_content().strip()
            return None
        except Exception as e:
            print(f"  [ERROR] Samsung_SKU_Name 추출 실패: {e}")
            return None

    def extract_electricity_use(self, tree):
        """Estimated_Annual_Electricity_Use 추출"""
        try:
            # Power 섹션에서 Estimated Annual Electricity Use 찾기
            xpaths = [
                '//li[.//h4[text()="Power"]]//div[.//div[contains(text(), "Estimated Annual Electricity Use")]]//div[@class="grow basis-none pl-300"]',
                '//div[contains(text(), "Estimated Annual Electricity Use")]/following-sibling::div[@class="grow basis-none pl-300"]'
            ]
            for xpath in xpaths:
                elem = tree.xpath(xpath)
                if elem:
                    return elem[0].text_content().strip()
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

    def extract_star_ratings(self, tree):
        """Count_of_Star_Ratings 추출"""
        try:
            ratings = {}
            # 별점별 개수 추출
            for star in range(1, 6):
                xpath = f'//a[contains(@href, "rating={star}")]//span[@class="uq0khJy2NYfr1ydM text-left v-text-tech-black"]'
                elem = tree.xpath(xpath)
                if elem:
                    count = elem[0].text_content().strip()
                    ratings[f"{star}stars"] = count
                else:
                    ratings[f"{star}stars"] = "0"

            # 형식: "5stars:776, 4stars:156, 3stars:29, 2stars:14, 1star:30"
            rating_str = ", ".join([f"{k}:{v}" for k, v in ratings.items()])
            return rating_str if rating_str else None

        except Exception as e:
            print(f"  [ERROR] Star ratings 추출 실패: {e}")
            return None

    def extract_top_mentions(self, tree):
        """Top_Mentions 추출"""
        try:
            mentions = []
            mention_elements = tree.xpath('//li[contains(@class, "inline-block")]//a[contains(@class, "pPqRKazD1ugrkdAf")]')
            for elem in mention_elements[:10]:  # 최대 10개
                text = elem.text_content().strip()
                mentions.append(text)

            return ", ".join(mentions) if mentions else None

        except Exception as e:
            print(f"  [ERROR] Top mentions 추출 실패: {e}")
            return None

    def click_see_all_reviews(self):
        """See All Customer Reviews 버튼 클릭"""
        try:
            print("  [INFO] See All Customer Reviews 버튼 클릭...")
            xpaths = [
                '//button[contains(., "See All Customer Reviews")]',
                '//button[@class="relative border-xs border-solid rounded-lg justify-center items-center self-start flex flex-col cursor-pointer px-300 py-100 border-comp-outline-primary-emphasis bg-comp-surface-primary-emphasis mr-200 Op9coqeII1kYHR9Q"]'
            ]

            for xpath in xpaths:
                try:
                    button = self.driver.find_element(By.XPATH, xpath)
                    self.driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", button)
                    time.sleep(1)
                    button.click()
                    print("  [OK] See All Customer Reviews 클릭 성공")
                    time.sleep(3)
                    return True
                except:
                    continue

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
            xpaths = [
                '//div[contains(@class, "v-text-dark-gray") and contains(., "would recommend")]',
                '//div[.//svg[contains(@class, "mr-50")]]//span[@class="font-weight-bold"]/parent::*/text()'
            ]

            # 전체 텍스트 추출
            elem = tree.xpath('//div[contains(@class, "v-text-dark-gray") and contains(., "would recommend")]')
            if elem:
                text = elem[0].text_content().strip()
                # "93% would recommend to a friend" 형식으로 추출
                text = ' '.join(text.split())  # 여러 공백을 하나로
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

            # 페이지 다시 로드
            page_source = self.driver.page_source
            tree = html.fromstring(page_source)

            # 6. Similar products 추출
            similar_names, pros_list, cons_list = self.extract_similar_products(tree)
            print(f"  [✓] Similar products: {len([x for x in similar_names if x])}개")

            # 7. Star ratings 추출
            star_ratings = self.extract_star_ratings(tree)
            print(f"  [✓] Star_Ratings: {star_ratings}")

            # 8. Top mentions 추출
            top_mentions = self.extract_top_mentions(tree)
            print(f"  [✓] Top_Mentions: {top_mentions}")

            # 9. See All Customer Reviews 클릭 및 리뷰 수집
            detailed_reviews = None
            if self.click_see_all_reviews():
                detailed_reviews = self.extract_reviews()
                print(f"  [✓] Detailed_Reviews: {len(detailed_reviews) if detailed_reviews else 0} chars")

            # 10. Recommendation intent 추출
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
                similar_names=similar_names,
                pros_list=pros_list,
                cons_list=cons_list,
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
                   electricity_use, similar_names, pros_list, cons_list,
                   star_ratings, top_mentions, detailed_reviews,
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
                    Retailer_SKU_Name_similar TEXT,
                    Pros TEXT,
                    Cons TEXT,
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
                 Estimated_Annual_Electricity_Use, Retailer_SKU_Name_similar,
                 Pros, Cons, Count_of_Star_Ratings, Top_Mentions,
                 Detailed_Review_Content, Recommendation_Intent, product_url)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """

            # Similar products를 구분자로 연결
            similar_names_str = " | ".join([x for x in similar_names if x]) if any(similar_names) else None
            pros_str = " | ".join([x for x in pros_list if x]) if any(pros_list) else None
            cons_str = " | ".join([x for x in cons_list if x]) if any(cons_list) else None

            cursor.execute(insert_query, (
                self.batch_id,
                page_type,
                order,
                retailer_sku_name,
                samsung_sku_name,
                electricity_use,
                similar_names_str,
                pros_str,
                cons_str,
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
            print(f"❌ 크롤러 실행 오류: {e}")
            import traceback
            traceback.print_exc()

        finally:
            if self.driver:
                self.driver.quit()
                print("\n🔧 드라이버 종료")
            if self.db_conn:
                self.db_conn.close()
                print("🔧 DB 연결 종료")

def main():
    crawler = BestBuyDetailCrawler()
    crawler.run()

if __name__ == "__main__":
    main()
