"""
Best Buy TV Promotion Crawler (Modified)
https://www.bestbuy.com/site/all-tv-home-theater-on-sale/tvs-on-sale/pcmcat1720647543741.c

수정사항:
1. 프로모션 메인 페이지에서 4개 컬럼 추가 수집: final_sku_price, original_sku_price, offer, savings
2. offer는 숫자만 저장 (예: "+2 offers for you" -> "2")
3. savings 검증: original_sku_price - final_sku_price와 동일해야 함
4. 컬럼 순서 변경: page_type, retailer_sku_name, rank, final_sku_price, original_sku_price, offer, savings, promotion_type, product_url, crawl_datetime, calendar_week, batch_id
5. item -> retailer_sku_name으로 변경
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
            print("[INFO] Chrome 드라이버 설정 중...")
            self.driver = uc.Chrome()
            self.driver.maximize_window()
            print("[OK] 드라이버 설정 완료")
            return True
        except Exception as e:
            print(f"[ERROR] 드라이버 설정 실패: {e}")
            return False

    def navigate_to_page(self):
        """프로모션 페이지 접속"""
        try:
            print(f"[INFO] Best Buy TV Promotion 페이지 접속...")
            self.driver.get(self.url)
            time.sleep(random.uniform(3, 5))

            # 페이지 로드 대기
            wait = WebDriverWait(self.driver, 20)
            print("[OK] 페이지 접속 완료")
            return True

        except Exception as e:
            print(f"[ERROR] 페이지 접속 실패: {e}")
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
                print(f"[OK] Promotion Type: {promotion_type}")
                return promotion_type
            elif h2_text:
                print(f"[OK] Promotion Type: {h2_text} (p 텍스트 없음)")
                return h2_text
            else:
                print("[WARNING] Promotion Type을 찾을 수 없습니다.")
                return None

        except Exception as e:
            print(f"[ERROR] Promotion Type 추출 실패: {e}")
            return None

    def extract_price_from_text(self, text):
        """텍스트에서 가격 숫자 추출 (예: "$74.99" -> "74.99")"""
        if not text:
            return None
        # $, 콤마 제거하고 숫자와 점만 추출
        match = re.search(r'[\d,]+\.?\d*', text.replace('$', '').replace(',', ''))
        if match:
            return match.group(0)
        return None

    def validate_savings(self, final_price, original_price, savings):
        """Savings 검증: original - final = savings"""
        try:
            if not final_price or not original_price or not savings:
                return True  # 값이 없으면 검증 스킵

            # 숫자로 변환
            final = float(final_price.replace(',', ''))
            original = float(original_price.replace(',', ''))
            saving = float(savings.replace(',', ''))

            # 계산된 savings
            calculated = original - final

            # 소수점 2자리까지 비교
            if abs(calculated - saving) < 0.01:
                return True
            else:
                print(f"    [WARNING] Savings 불일치: original({original}) - final({final}) = {calculated}, but savings = {saving}")
                return False

        except Exception as e:
            print(f"    [ERROR] Savings 검증 실패: {e}")
            return False

    def extract_products(self):
        """제품 정보 추출"""
        try:
            print("\n[INFO] 제품 정보 추출 시작...")

            # 페이지 소스 가져오기
            page_source = self.driver.page_source
            tree = html.fromstring(page_source)

            # Promotion Type 추출
            promotion_type = self.extract_promotion_type(tree)

            products = []

            # 모든 제품 아이템 찾기 (li 요소, 최대 6개)
            product_items = tree.xpath('//ul[@class="c-carousel-list"]//li[@class="item c-carousel-item "]')[:6]

            print(f"[OK] 총 {len(product_items)}개 제품 발견 (최대 6개)")

            for idx, item in enumerate(product_items, 1):
                try:
                    # rank는 1부터 시작
                    rank = idx

                    # 제품명 추출 (retailer_sku_name)
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

                    # final_sku_price 추출
                    # XPath: //*[@id="top-deal-customer-price"]
                    final_price_xpaths = [
                        './/div[@id="top-deal-customer-price"]',
                        './/div[@data-testid="top-deal-customer-price"]'
                    ]
                    final_price = None
                    for xpath in final_price_xpaths:
                        elem = item.xpath(xpath)
                        if elem:
                            price_text = elem[0].text_content().strip()
                            final_price = self.extract_price_from_text(price_text)
                            break

                    # original_sku_price 추출
                    # XPath: //*[@id="top-deal-regular-price"]
                    original_price_xpaths = [
                        './/div[@id="top-deal-regular-price"]',
                        './/div[@data-testid="top-deal-regular-price"]'
                    ]
                    original_price = None
                    for xpath in original_price_xpaths:
                        elem = item.xpath(xpath)
                        if elem:
                            price_text = elem[0].text_content().strip()
                            original_price = self.extract_price_from_text(price_text)
                            break

                    # offer 추출 (숫자만)
                    # XPath: //*[@id="offer-link"]/div
                    offer_xpaths = [
                        './/button[@id="offer-link"]//div',
                        './/button[@data-testid="offer-link"]//div'
                    ]
                    offer = None
                    for xpath in offer_xpaths:
                        elem = item.xpath(xpath)
                        if elem:
                            offer_text = elem[0].text_content().strip()
                            # 숫자만 추출 (예: "+2 offers for you" -> "2")
                            match = re.search(r'(\d+)', offer_text)
                            if match:
                                offer = match.group(1)
                            break

                    # savings 추출
                    # XPath: //*[@id="top-deal-buck-total-savings"]
                    savings_xpaths = [
                        './/div[@id="top-deal-buck-total-savings"]',
                        './/div[@data-testid="top-deal-buck-total-savings"]'
                    ]
                    savings = None
                    for xpath in savings_xpaths:
                        elem = item.xpath(xpath)
                        if elem:
                            savings_text = elem[0].text_content().strip()
                            # 숫자만 추출 (예: "$55 off" -> "55")
                            savings = self.extract_price_from_text(savings_text)
                            break

                    # savings 검증
                    if final_price and original_price and savings:
                        is_valid = self.validate_savings(final_price, original_price, savings)
                        if not is_valid:
                            print(f"    [INFO] Savings 검증 실패했지만 계속 진행")

                    if product_name and product_url:
                        product = {
                            'page_type': 'Top deals',
                            'retailer_sku_name': product_name,
                            'rank': rank,
                            'final_sku_price': final_price,
                            'original_sku_price': original_price,
                            'offer': offer,
                            'savings': savings,
                            'promotion_type': promotion_type,
                            'product_url': product_url
                        }
                        products.append(product)
                        print(f"  [{rank}] {product_name[:50]}...")
                        print(f"      Price: ${final_price} (Was: ${original_price}, Save: ${savings}, Offers: {offer})")
                        print(f"      URL: {product_url[:80]}...")

                except Exception as e:
                    print(f"  [WARNING] 제품 {idx} 추출 실패: {e}")
                    import traceback
                    traceback.print_exc()
                    continue

            print(f"\n[OK] 총 {len(products)}개 제품 추출 완료")
            return products

        except Exception as e:
            print(f"[ERROR] 제품 정보 추출 실패: {e}")
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

            # 테이블 존재 확인 및 생성 (새로운 컬럼 순서로)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS bby_tv_promotion_crawl (
                    id SERIAL PRIMARY KEY,
                    page_type VARCHAR(50),
                    retailer_sku_name TEXT,
                    rank INTEGER,
                    final_sku_price VARCHAR(50),
                    original_sku_price VARCHAR(50),
                    offer VARCHAR(50),
                    savings VARCHAR(50),
                    promotion_type TEXT,
                    product_url TEXT,
                    crawl_datetime TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    calendar_week VARCHAR(10),
                    batch_id VARCHAR(50)
                )
            """)

            # Calculate calendar week
            calendar_week = f"w{datetime.now().isocalendar().week}"

            # 데이터 삽입
            insert_query = """
                INSERT INTO bby_tv_promotion_crawl
                (page_type, retailer_sku_name, rank, final_sku_price, original_sku_price, offer, savings,
                 promotion_type, product_url, calendar_week, batch_id)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """

            success_count = 0
            for product in products:
                try:
                    cursor.execute(insert_query, (
                        product['page_type'],
                        product['retailer_sku_name'],
                        product['rank'],
                        product['final_sku_price'],
                        product['original_sku_price'],
                        product['offer'],
                        product['savings'],
                        product['promotion_type'],
                        product['product_url'],
                        calendar_week,
                        self.batch_id
                    ))
                    success_count += 1
                except Exception as e:
                    print(f"[ERROR] 저장 실패 - Rank {product['rank']}: {e}")

            cursor.close()
            print(f"[OK] DB 저장 완료: {success_count}/{len(products)}개")
            return True

        except Exception as e:
            print(f"[ERROR] DB 저장 실패: {e}")
            import traceback
            traceback.print_exc()
            return False

    def run(self):
        """메인 실행"""
        try:
            print("="*80)
            print(f"Best Buy TV Promotion Crawler (Modified) (Batch ID: {self.batch_id})")
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
                print("\n[ERROR] 수집된 제품이 없습니다.")

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
    crawler = BestBuyPromotionCrawler()
    crawler.run()

if __name__ == "__main__":
    main()
