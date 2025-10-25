"""
Best Buy TV Promotion Crawler
https://www.bestbuy.com/site/all-tv-home-theater-on-sale/tvs-on-sale/pcmcat1720647543741.c
수집 항목: page_type, rank, promotion_Type, Retailer_SKU_Name, product_url
저장 테이블: bby_tv_promotion_crawl
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

class BestBuyPromotionCrawler:
    def __init__(self):
        self.driver = None
        self.db_conn = None
        self.korea_tz = pytz.timezone('Asia/Seoul')
        self.batch_id = datetime.now(self.korea_tz).strftime('%Y%m%d_%H%M%S')
        self.url = "https://www.bestbuy.com/site/all-tv-home-theater-on-sale/tvs-on-sale/pcmcat1720647543741.c?id=pcmcat1720647543741"

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

    def navigate_to_page(self):
        """프로모션 페이지 접속"""
        try:
            print(f"🌐 Best Buy TV Promotion 페이지 접속...")
            self.driver.get(self.url)
            time.sleep(random.uniform(3, 5))

            # 페이지 로드 대기
            wait = WebDriverWait(self.driver, 20)
            print("✅ 페이지 접속 완료")
            return True

        except Exception as e:
            print(f"❌ 페이지 접속 실패: {e}")
            return False

    def extract_promotion_type(self, tree):
        """프로모션 타입 추출 (h2 + p 결합)"""
        try:
            # h2 텍스트 추출 (모든 텍스트 포함)
            h2_xpaths = [
                '//h2[contains(@class, "headline80")]',
                '//h2[@class="headline80 font-weight-bold font-condensed"]'
            ]

            h2_text = None
            for xpath in h2_xpaths:
                h2_elem = tree.xpath(xpath)
                if h2_elem:
                    h2_text = h2_elem[0].text_content().strip()
                    # 여러 공백을 하나로 합치기
                    h2_text = ' '.join(h2_text.split())
                    break

            # p 텍스트 추출
            p_xpaths = [
                '//p[contains(@class, "heading-4") and contains(@class, "font-weight-light")]',
                '//p[@class="heading-4 font-weight-light v-text-pure-white mb-200"]'
            ]

            p_text = None
            for xpath in p_xpaths:
                p_elem = tree.xpath(xpath)
                if p_elem:
                    p_text = p_elem[0].text_content().strip()
                    break

            # 결합
            if h2_text and p_text:
                promotion_type = f"{h2_text} {p_text}"
                print(f"✅ Promotion Type: {promotion_type}")
                return promotion_type
            elif h2_text:
                print(f"✅ Promotion Type: {h2_text} (p 텍스트 없음)")
                return h2_text
            else:
                print("[WARNING] Promotion Type을 찾을 수 없습니다.")
                return None

        except Exception as e:
            print(f"[ERROR] Promotion Type 추출 실패: {e}")
            return None

    def extract_products(self):
        """제품 정보 추출"""
        try:
            print("\n📊 제품 정보 추출 시작...")

            # 페이지 소스 가져오기
            page_source = self.driver.page_source
            tree = html.fromstring(page_source)

            # Promotion Type 추출
            promotion_type = self.extract_promotion_type(tree)

            products = []

            # 모든 제품 아이템 찾기 (li 요소, 최대 6개)
            product_items = tree.xpath('//ul[@class="c-carousel-list"]//li[@class="item c-carousel-item "]')[:6]

            print(f"✅ 총 {len(product_items)}개 제품 발견 (최대 6개)")

            for idx, item in enumerate(product_items, 1):
                try:
                    # rank는 1부터 시작 (data-order + 1)
                    rank = idx

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

                    # URL 추출 (data-testid="hero-experience-deal-card-test-id")
                    url_xpaths = [
                        './/a[@data-testid="hero-experience-deal-card-test-id"]/@href',
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
                            'page_type': 'Top deals',
                            'rank': rank,
                            'promotion_Type': promotion_type,
                            'Retailer_SKU_Name': product_name,
                            'product_url': product_url
                        }
                        products.append(product)
                        print(f"  [{rank}] {product_name[:50]}...")
                        print(f"      URL: {product_url[:80]}...")

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
                CREATE TABLE IF NOT EXISTS bby_tv_promotion_crawl (
                    id SERIAL PRIMARY KEY,
                    page_type VARCHAR(50),
                    rank INTEGER,
                    promotion_Type TEXT,
                    Retailer_SKU_Name TEXT,
                    product_url TEXT,
                    crawl_datetime TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)

            # 데이터 삽입
            insert_query = """
                INSERT INTO bby_tv_promotion_crawl
                (batch_id, page_type, rank, promotion_Type, Retailer_SKU_Name, product_url)
                VALUES (%s, %s, %s, %s, %s, %s)
            """

            success_count = 0
            for product in products:
                try:
                    cursor.execute(insert_query, (
                        self.batch_id,
                        product['page_type'],
                        product['rank'],
                        product['promotion_Type'],
                        product['Retailer_SKU_Name'],
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
            print(f"Best Buy TV Promotion Crawler (Batch ID: {self.batch_id})")
            print("="*80)

            # DB 연결
            if not self.connect_db():
                return

            # 드라이버 설정
            if not self.setup_driver():
                return

            # 페이지 접속
            if not self.navigate_to_page():
                return

            # 제품 정보 추출
            products = self.extract_products()

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
    crawler = BestBuyPromotionCrawler()
    crawler.run()

if __name__ == "__main__":
    main()
