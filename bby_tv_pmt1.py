"""
Best Buy TV Promotion Crawler (Multi-Section Dynamic Version)
https://www.bestbuy.com/site/all-tv-home-theater-on-sale/tvs-on-sale/pcmcat1720647543741.c

핵심 기능:
1. 다중 섹션 처리: 3개 프로모션 섹션에서 총 18개 SKU 수집 (각 섹션당 6개)
2. 완전 동적 탐지: 키워드 독립적, 섹션 순서 변경 대응
3. HTML 태그 처리: <br> → 공백, <sup> → 소수점 변환
4. preceding 축 기반 매핑: 섹션과 carousel을 DOM 순서로 정확히 매핑

수집 데이터:
- page_type, retailer_sku_name, promotion_rank (섹션 내 1-6)
- final_sku_price, original_sku_price, offer, savings
- promotion_type (동적 추출), product_url
- crawl_strdatetime, calendar_week, batch_id

견고성:
- facet 섹션 자동 제외
- 개별 섹션 에러 시 계속 진행
- 빈 promotion_type 자동 필터링
- carousel 매핑 검증

버전: v2.0 (Dynamic Multi-Section)
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
from lxml import html, etree

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

    def extract_promotion_type_text(self, element):
        """
        promotion_type 텍스트 추출 (HTML 태그 처리)
        <br> → 공백으로 변환
        <sup>99</sup> → .99로 변환 (소수점)

        Args:
            element: lxml element
        Returns:
            str: 처리된 텍스트
        """
        try:
            # element의 HTML을 문자열로 변환
            html_string = etree.tostring(element, encoding='unicode', method='html')

            # <br> 태그를 공백으로 치환
            html_string = html_string.replace('<br>', ' ').replace('<br/>', ' ').replace('<br />', ' ')

            # <sup> 태그 처리: <sup>99</sup> → .99
            # <sup ...>숫자</sup> 패턴 찾기
            sup_pattern = r'<sup[^>]*>(\d+)</sup>'
            html_string = re.sub(sup_pattern, r'.\1', html_string)

            # HTML 태그 제거하고 텍스트만 추출
            clean_element = html.fromstring(html_string)
            text = clean_element.text_content().strip()

            # 여러 공백을 하나로 합치기
            text = ' '.join(text.split())

            return text

        except Exception as e:
            print(f"[WARNING] extract_promotion_type_text 오류: {e}")
            # 오류 시 기본 text_content() 사용
            return element.text_content().strip() if element is not None else ""

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

    def extract_promotion_sections(self, tree):
        """
        페이지에서 모든 프로모션 섹션 찾기 (동적 탐지 - 키워드 독립적)

        Returns:
            List of tuples: [(section_element, section_type, promotion_type), ...]
        """
        sections = []

        try:
            # 모든 section 찾기 (facet 제외)
            all_sections = tree.xpath('//section')
            print(f"[INFO] 총 {len(all_sections)}개 section 발견")

            # 각 섹션이 프로모션 섹션인지 확인 (carousel 매핑 여부로 판단)
            all_carousels = tree.xpath('//ul[@class="c-carousel-list"]')

            for section in all_sections:
                try:
                    # facet 섹션 제외 (필터 섹션)
                    section_class = section.get('class', '')
                    if 'facet' in section_class:
                        continue

                    # 이 섹션에 매핑된 carousel이 있는지 확인
                    has_carousel = False
                    for carousel in all_carousels:
                        preceding_sections = carousel.xpath('preceding::section')
                        if preceding_sections and preceding_sections[-1] == section:
                            has_carousel = True
                            break

                    if not has_carousel:
                        continue

                    # promotion_type 동적 추출
                    promotion_type = None

                    # 방법 1: hero-holiday-blue-gradient 섹션 (span 태그에서 추출)
                    if 'hero-holiday-blue-gradient' in section_class:
                        span_elem = section.xpath('.//span[contains(@class, "hero-fluid-headline-2")]')
                        if span_elem:
                            promotion_type = self.extract_promotion_type_text(span_elem[0])

                    # 방법 2: 일반 섹션 (h2 + p 또는 첫 2줄)
                    else:
                        # h2 태그 먼저 시도
                        h2_elem = section.xpath('.//h2')
                        p_elem = section.xpath('.//p[contains(@class, "heading") or contains(@class, "subhead")]')

                        if h2_elem and p_elem:
                            h2_text = h2_elem[0].text_content().strip()
                            p_text = p_elem[0].text_content().strip()
                            # 빈 문자열 체크
                            if h2_text or p_text:
                                promotion_type = f"{h2_text} {p_text}".strip()
                        elif h2_elem:
                            promotion_type = h2_elem[0].text_content().strip()
                        else:
                            # 텍스트 내용의 첫 2줄 사용
                            text_content = section.text_content().strip()
                            lines = [line.strip() for line in text_content.split('\n') if line.strip()]
                            if len(lines) >= 2:
                                promotion_type = f"{lines[0]} {lines[1]}"
                            elif lines:
                                promotion_type = lines[0]

                    # promotion_type 최종 검증 및 정리
                    if promotion_type:
                        # 공백 정리
                        promotion_type = ' '.join(promotion_type.split())
                        # 빈 문자열이 아닌지 재확인
                        if promotion_type:
                            sections.append((section, 'dynamic', promotion_type))
                            print(f"[OK] Section {len(sections)}: {promotion_type[:60]}...")

                except Exception as e:
                    print(f"[WARNING] 섹션 처리 중 오류 (건너뜀): {e}")
                    continue

            print(f"[OK] 총 {len(sections)}개 프로모션 섹션 발견")
            return sections

        except Exception as e:
            print(f"[ERROR] extract_promotion_sections 실패: {e}")
            import traceback
            traceback.print_exc()
            return []

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

            # 숫자로 변환 ($, 콤마 제거)
            final = float(final_price.replace(',', '').replace('$', ''))
            original = float(original_price.replace(',', '').replace('$', ''))
            saving = float(savings.replace(',', '').replace('$', ''))

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
        """제품 정보 추출 (3개 섹션, 최대 18개 SKU)"""
        try:
            print("\n[INFO] 제품 정보 추출 시작...")

            # 페이지 소스 가져오기
            page_source = self.driver.page_source
            tree = html.fromstring(page_source)

            # 모든 프로모션 섹션 찾기
            sections = self.extract_promotion_sections(tree)

            if not sections:
                print("[WARNING] 프로모션 섹션을 찾을 수 없습니다.")
                return []

            all_products = []

            # 각 섹션별로 처리
            for section_idx, (section_elem, section_type, promotion_type) in enumerate(sections, 1):
                try:
                    print(f"\n[INFO] Section {section_idx} 처리 중: {promotion_type[:60]}...")

                    # 이 섹션에 속하는 모든 carousel 찾기 (preceding 축 기반)
                    # 섹션 이후의 모든 c-carousel-list를 찾아서
                    # 각 carousel의 preceding::section[-1]이 현재 섹션인지 확인

                    all_carousels = tree.xpath('//ul[@class="c-carousel-list"]')
                    section_carousels = []

                    for carousel in all_carousels:
                        # 이 carousel 앞의 가장 가까운 section 찾기
                        preceding_sections = carousel.xpath('preceding::section')
                        if preceding_sections:
                            nearest_section = preceding_sections[-1]  # 가장 가까운 section
                            # 현재 섹션과 동일한지 확인 (메모리 주소 비교)
                            if nearest_section == section_elem:
                                section_carousels.append(carousel)

                    print(f"[OK] Section {section_idx}에 매핑된 carousel: {len(section_carousels)}개")

                    # 모든 carousel에서 li 아이템 수집 (최대 6개)
                    product_items = []
                    for carousel in section_carousels:
                        items = carousel.xpath('.//li[@class="item c-carousel-item "]')
                        product_items.extend(items)
                        if len(product_items) >= 6:
                            break

                    product_items = product_items[:6]  # 최대 6개로 제한
                    print(f"[OK] Section {section_idx}에서 총 {len(product_items)}개 제품 수집")

                    # 각 제품 처리 (promotion_rank는 섹션 내에서 1-6)
                    for idx, item in enumerate(product_items[:6], 1):
                        try:
                            # promotion_rank는 섹션 내에서 1부터 시작
                            promotion_rank = idx

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
                                './/div[@class="content-wrapper"]//a/@href',
                                './/a/@href'
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
                                    if final_price:
                                        final_price = f"${final_price}"
                                    break

                            # original_sku_price 추출
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
                                    if original_price:
                                        original_price = f"${original_price}"
                                    break

                            # offer 추출 (숫자만)
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
                            savings_xpaths = [
                                './/div[@id="top-deal-buck-total-savings"]',
                                './/div[@data-testid="top-deal-buck-total-savings"]'
                            ]
                            savings = None
                            for xpath in savings_xpaths:
                                elem = item.xpath(xpath)
                                if elem:
                                    savings_text = elem[0].text_content().strip()
                                    # 숫자만 추출하고 $ 붙이기 (예: "$55 off" -> "$55")
                                    savings = self.extract_price_from_text(savings_text)
                                    if savings:
                                        savings = f"${savings}"
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
                                    'promotion_rank': promotion_rank,
                                    'final_sku_price': final_price,
                                    'original_sku_price': original_price,
                                    'offer': offer,
                                    'savings': savings,
                                    'promotion_type': promotion_type,
                                    'product_url': product_url
                                }
                                all_products.append(product)
                                print(f"  [S{section_idx}-{promotion_rank}] {product_name[:50]}...")
                                print(f"      Price: {final_price} (Was: {original_price}, Save: {savings}, Offers: {offer})")
                                print(f"      URL: {product_url[:80]}...")

                        except Exception as e:
                            print(f"  [WARNING] Section {section_idx} 제품 {idx} 추출 실패: {e}")
                            import traceback
                            traceback.print_exc()
                            continue

                except Exception as e:
                    print(f"[WARNING] Section {section_idx} 처리 실패: {e}")
                    import traceback
                    traceback.print_exc()
                    continue

            print(f"\n[OK] 총 {len(all_products)}개 제품 추출 완료 ({len(sections)}개 섹션)")
            return all_products

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
                CREATE TABLE IF NOT EXISTS bby_tv_pmt1 (
                    id SERIAL PRIMARY KEY,
                    account_name VARCHAR(50),
                    page_type VARCHAR(50),
                    retailer_sku_name TEXT,
                    promotion_rank INTEGER,
                    final_sku_price VARCHAR(50),
                    original_sku_price VARCHAR(50),
                    offer VARCHAR(50),
                    savings VARCHAR(50),
                    promotion_type TEXT,
                    product_url TEXT,
                    crawl_strdatetime VARCHAR(20),
                    calendar_week VARCHAR(10),
                    batch_id VARCHAR(50)
                )
            """)

            # Calculate calendar week
            calendar_week = f"w{datetime.now().isocalendar().week}"

            # Calculate crawl_strdatetime (format: YYYYMMDDHHMISS + microseconds 4 digits)
            now = datetime.now()
            crawl_strdatetime = now.strftime('%Y%m%d%H%M%S') + now.strftime('%f')[:4]

            # 데이터 삽입
            insert_query = """
                INSERT INTO bby_tv_pmt1
                (account_name, page_type, retailer_sku_name, promotion_rank, final_sku_price, original_sku_price, offer, savings,
                 promotion_type, product_url, crawl_strdatetime, calendar_week, batch_id)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """

            success_count = 0
            for product in products:
                try:
                    cursor.execute(insert_query, (
                        'Bestbuy',
                        product['page_type'],
                        product['retailer_sku_name'],
                        product['promotion_rank'],
                        product['final_sku_price'],
                        product['original_sku_price'],
                        product['offer'],
                        product['savings'],
                        product['promotion_type'],
                        product['product_url'],
                        crawl_strdatetime,
                        calendar_week,
                        self.batch_id
                    ))
                    success_count += 1
                except Exception as e:
                    print(f"[ERROR] 저장 실패 - Promotion Rank {product['promotion_rank']}: {e}")

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
