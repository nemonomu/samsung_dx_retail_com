"""
Best Buy Trending Deals - TVs Crawler
https://www.bestbuy.com/ → Trending deals → TVs
수집 항목: rank, product_name, product_url
저장 테이블: bby_tv_Trend_crawl
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

class BestBuyTrendCrawler:
    def __init__(self):
        self.driver = None
        self.db_conn = None
        self.korea_tz = pytz.timezone('Asia/Seoul')
        self.batch_id = datetime.now(self.korea_tz).strftime('%Y%m%d_%H%M%S')

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

    def click_tvs_category(self):
        """Trending deals에서 TVs 카테고리 클릭"""
        try:
            # 홈페이지 접속
            print(f"🌐 Best Buy 홈페이지 접속...")
            self.driver.get("https://www.bestbuy.com/")
            time.sleep(random.uniform(3, 5))

            # 페이지 로드 대기
            wait = WebDriverWait(self.driver, 20)

            # TVs 버튼 찾기 및 클릭
            tvs_button_xpaths = [
                "//button[@data-testid='Trending-Deals-TVs']",
                "//button[contains(text(), 'TVs')]",
                "//button[@aria-controls='Trending-Deals-TVs']"
            ]

            clicked = False
            for xpath in tvs_button_xpaths:
                try:
                    tvs_button = wait.until(EC.presence_of_element_located((By.XPATH, xpath)))

                    # 버튼이 보일 때까지 스크롤
                    self.driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", tvs_button)
                    time.sleep(1)

                    # 클릭
                    tvs_button.click()
                    print("✅ TVs 카테고리 클릭 완료")
                    clicked = True
                    time.sleep(random.uniform(2, 3))
                    break
                except Exception as e:
                    continue

            if not clicked:
                print("[WARNING] TVs 버튼을 찾을 수 없습니다. 이미 선택되어 있을 수 있습니다.")

            return True

        except Exception as e:
            print(f"❌ TVs 카테고리 클릭 실패: {e}")
            return False

    def extract_trending_products(self):
        """Trending deals TVs 제품 정보 추출"""
        try:
            print("\n📊 제품 정보 추출 시작...")

            # 페이지 소스 가져오기
            page_source = self.driver.page_source
            tree = html.fromstring(page_source)

            products = []

            # 모든 제품 아이템 찾기 (li 요소)
            # 제품들은 ul > li 구조로 되어있음
            product_items = tree.xpath('//div[@id="Trending-Deals-TVs"]//ul[@class="c-carousel-list"]/li')

            print(f"✅ 총 {len(product_items)}개 제품 발견")

            for idx, item in enumerate(product_items, 1):
                try:
                    # 순위 추출
                    rank_xpath = './/div[@data-testid="trending-deals-number-test-id"]'
                    rank_elem = item.xpath(rank_xpath)
                    rank = rank_elem[0].text_content().strip() if rank_elem else str(idx)

                    # 제품명 추출
                    name_xpaths = [
                        './/span[contains(@class, "BxIuyHdYvE_KO21sTHqZ")]',
                        './/div[@data-testid="product-card-title"]//span',
                        './/a[@data-testid="product-card-title-link"]//span'
                    ]

                    product_name = None
                    for name_xpath in name_xpaths:
                        name_elem = item.xpath(name_xpath)
                        if name_elem:
                            product_name = name_elem[0].text_content().strip()
                            break

                    # URL 추출
                    url_xpaths = [
                        './/a[@data-testid="trending-deals-card-test-id"]/@href',
                        './/a[@data-testid="product-card-title-link"]/@href',
                        './/div[@class="content-wrapper"]//a/@href'
                    ]

                    product_url = None
                    for url_xpath in url_xpaths:
                        url_elem = item.xpath(url_xpath)
                        if url_elem:
                            product_url = url_elem[0]
                            # 상대 경로를 절대 경로로 변환
                            if product_url.startswith('/'):
                                product_url = f"https://www.bestbuy.com{product_url}"
                            break

                    if product_name and product_url:
                        product = {
                            'page_type': 'Trend',
                            'rank': int(rank),
                            'product_name': product_name,
                            'product_url': product_url
                        }
                        products.append(product)
                        print(f"  [{rank}] {product_name[:50]}...")

                except Exception as e:
                    print(f"  [WARNING] 제품 {idx} 추출 실패: {e}")
                    continue

            print(f"\n✅ 총 {len(products)}개 제품 추출 완료")
            return products

        except Exception as e:
            print(f"❌ 제품 정보 추출 실패: {e}")
            import traceback
            traceback.print_exc()
            return []

    def save_to_db(self, products):
        """DB에 저장"""
        if not products:
            print("[WARNING] 저장할 데이터가 없습니다.")
            return False

        try:
            cursor = self.db_conn.cursor()

            # 테이블 존재 확인 및 생성
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS bby_tv_Trend_crawl (
                    id SERIAL PRIMARY KEY,
                    page_type VARCHAR(50),
                    rank INTEGER,
                    product_name TEXT,
                    product_url TEXT,
                    crawl_datetime TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)

            # 데이터 삽입
            insert_query = """
                INSERT INTO bby_tv_Trend_crawl (batch_id, page_type, rank, product_name, product_url)
                VALUES (%s, %s, %s, %s, %s)
            """

            success_count = 0
            for product in products:
                try:
                    cursor.execute(insert_query, (
                        self.batch_id,
                        product['page_type'],
                        product['rank'],
                        product['product_name'],
                        product['product_url']
                    ))
                    success_count += 1
                except Exception as e:
                    print(f"[ERROR] 저장 실패 - Rank {product['rank']}: {e}")

            cursor.close()
            print(f"✅ DB 저장 완료: {success_count}/{len(products)}개")
            return True

        except Exception as e:
            print(f"❌ DB 저장 실패: {e}")
            import traceback
            traceback.print_exc()
            return False

    def run(self):
        """메인 실행"""
        try:
            print("="*80)
            print(f"Best Buy Trending Deals - TVs Crawler (Batch ID: {self.batch_id})")
            print("="*80)

            # DB 연결
            if not self.connect_db():
                return

            # Add batch_id column if not exists
            try:
                cursor = self.db_conn.cursor()
                cursor.execute("""
                    ALTER TABLE bby_tv_Trend_crawl
                    ADD COLUMN IF NOT EXISTS batch_id VARCHAR(50)
                """)
                self.db_conn.commit()
                cursor.close()
                print("[OK] Table schema updated (batch_id column added if needed)")
            except Exception as e:
                print(f"[WARNING] Could not add batch_id column: {e}")

            # 드라이버 설정
            if not self.setup_driver():
                return

            # TVs 카테고리 클릭
            if not self.click_tvs_category():
                return

            # 제품 정보 추출
            products = self.extract_trending_products()

            # DB 저장
            if products:
                self.save_to_db(products)

                # 결과 요약
                print("\n" + "="*80)
                print("크롤링 완료!")
                print(f"총 {len(products)}개 제품 수집")
                print("="*80)
            else:
                print("\n❌ 수집된 제품이 없습니다.")

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
    crawler = BestBuyTrendCrawler()
    crawler.run()

if __name__ == "__main__":
    main()
