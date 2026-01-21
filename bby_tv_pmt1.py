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
- offer, promotion_type (동적 추출), product_url
- crawl_datetime, calendar_week, batch_id

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
import os
import psycopg2
from datetime import datetime
import pytz
from DrissionPage import ChromiumPage, ChromiumOptions
from lxml import html, etree
from data_validator import DataValidator

# Import database configuration
from config import DB_CONFIG
from bby_config_loader import get_config

class BestBuyPromotionCrawler:
    def __init__(self):
        self.page = None
        self.db_conn = None
        self.korea_tz = pytz.timezone('Asia/Seoul')
        self.batch_id = datetime.now(self.korea_tz).strftime('%Y%m%d_%H%M%S')

        # Config loader 초기화
        self.config = get_config()
        self.file_name = 'bby_tv_pmt1'

        # URL from config
        self.url = self.config.get_url('promo_page', self.file_name)

        # Data validator 초기화
        session_start_time = os.environ.get('SESSION_START_TIME', datetime.now().strftime('%Y%m%d%H%M'))
        self.validator = DataValidator(session_start_time)

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

    def setup_browser(self):
        """Setup DrissionPage ChromiumPage - 최소 설정"""
        try:
            print("[INFO] Setting up DrissionPage browser...")
            self.page = ChromiumPage()
            print("[OK] DrissionPage browser setup complete")
            return True
        except Exception as e:
            print(f"[ERROR] Browser setup failed: {e}")
            import traceback
            traceback.print_exc()
            return False

    def navigate_to_page(self):
        """Navigate to promotion page with slow scroll for lazy loading"""
        try:
            print(f"[INFO] Accessing Best Buy TV Promotion page...")
            self.page.get(self.url)
            page_access_wait = self.config.get_float('timing', 'page_access_wait', self.file_name, 3)
            time.sleep(page_access_wait)

            # Slow scroll to trigger lazy loading
            print("[INFO] Starting slow scroll for lazy loading...")
            scroll_step = self.config.get_int('timing', 'scroll_step', self.file_name, 300)
            scroll_wait = self.config.get_float('timing', 'scroll_wait', self.file_name, 0.8)
            scroll_limit = self.config.get_int('constant', 'scroll_limit_px', self.file_name, 10000)
            current_position = 0
            last_height = self.page.run_js("return document.body.scrollHeight")

            while True:
                current_position += scroll_step
                self.page.run_js(f"window.scrollTo(0, {current_position})")
                time.sleep(scroll_wait)

                new_height = self.page.run_js("return document.body.scrollHeight")
                if current_position >= new_height:
                    break
                if current_position > scroll_limit:  # Safety limit
                    break

            # Scroll back to top
            self.page.run_js("window.scrollTo(0, 0)")
            scroll_top_wait = self.config.get_float('timing', 'scroll_top_wait', self.file_name, 2)
            time.sleep(scroll_top_wait)

            print("[OK] Page loaded successfully")
            return True

        except Exception as e:
            print(f"[ERROR] Page access failed: {e}")
            import traceback
            traceback.print_exc()
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
            print(f"[WARNING] extract_promotion_type_text error: {e}")
            # Fallback to default text_content() on error
            return element.text_content().strip() if element is not None else ""

    def extract_promotion_type(self, tree):
        """프로모션 타입 추출 (h2 + p 결합)"""
        try:
            # h2 텍스트 추출 (모든 텍스트 포함)
            h2_xpaths = self.config.get_xpath_list('h2_headline', self.file_name) or [
                '//h2[contains(@class, "headline80")]'
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
            p_xpaths = self.config.get_xpath_list('p_subtitle', self.file_name) or [
                '//p[contains(@class, "heading-4") and contains(@class, "font-weight-light")]'
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
                print(f"[OK] Promotion Type: {h2_text} (no p text)")
                return h2_text
            else:
                print("[WARNING] Promotion Type not found")
                return None

        except Exception as e:
            print(f"[ERROR] Promotion Type extraction failed: {e}")
            return None

    def extract_promotion_sections(self, tree):
        """
        페이지에서 모든 프로모션 섹션 찾기 (동적 탐지 - 키워드 독립적)

        Returns:
            List of tuples: [(section_element, section_type, promotion_type), ...]
        """
        sections = []

        # XPath from config
        all_sections_xpath = self.config.get('xpath', 'all_sections', self.file_name) or '//section'
        carousel_list_xpath = self.config.get('xpath', 'carousel_list', self.file_name) or '//ul[@class="c-carousel-list"]'
        promo_type_hero_xpath = self.config.get('xpath', 'promo_type_hero', self.file_name) or './/span[contains(@class, "hero-fluid-headline-2")]'
        section_title_xpath = self.config.get('xpath', 'section_title', self.file_name) or './/h2'
        section_p_xpath = self.config.get('xpath', 'section_p', self.file_name) or './/p[contains(@class, "heading") or contains(@class, "subhead")]'

        try:
            # Find all sections (exclude facet)
            all_sections = tree.xpath(all_sections_xpath)
            print(f"[INFO] Found {len(all_sections)} sections total")

            # 각 섹션이 프로모션 섹션인지 확인 (carousel 매핑 여부로 판단)
            all_carousels = tree.xpath(carousel_list_xpath)

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
                        span_elem = section.xpath(promo_type_hero_xpath)
                        if span_elem:
                            promotion_type = self.extract_promotion_type_text(span_elem[0])

                    # 방법 2: 일반 섹션 (h2 + p 또는 첫 2줄)
                    else:
                        # h2 태그 먼저 시도
                        h2_elem = section.xpath(section_title_xpath)
                        p_elem = section.xpath(section_p_xpath)

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
                    print(f"[WARNING] Section processing error (skipped): {e}")
                    continue

            print(f"[OK] Found {len(sections)} promotion sections")
            return sections

        except Exception as e:
            print(f"[ERROR] extract_promotion_sections failed: {e}")
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

    def extract_products(self):
        """Extract product information (3 sections, max 18 SKUs)"""
        try:
            print("\n[INFO] Starting product extraction...")

            # 페이지 소스 가져오기 (DrissionPage)
            page_source = self.page.html
            tree = html.fromstring(page_source)

            # Find all promotion sections
            sections = self.extract_promotion_sections(tree)

            if not sections:
                print("[WARNING] No promotion sections found")
                return []

            all_products = []

            # Config values
            carousel_list_xpath = self.config.get('xpath', 'carousel_list', self.file_name) or '//ul[@class="c-carousel-list"]'
            product_item_xpath = self.config.get('xpath', 'product_item', self.file_name) or './/li[@class="item c-carousel-item "]'
            section_max_products = self.config.get_int('constant', 'section_max_products', self.file_name, 6)
            page_type = self.config.get_constant('page_type_promo', self.file_name) or 'Top deals'
            name_xpaths = self.config.get_xpath_list('product_name', self.file_name) or [
                './/span[contains(@class, "BxIuyHdYvE_KO21sTHqZ")]'
            ]
            url_xpaths = self.config.get_xpath_list('product_url', self.file_name) or [
                './/a[@data-testid="hero-experience-deal-card-test-id"]/@href'
            ]
            offer_xpaths = self.config.get_xpath_list('offer', self.file_name) or [
                './/button[@id="offer-link"]//div'
            ]

            # Process each section
            for section_idx, (section_elem, section_type, promotion_type) in enumerate(sections, 1):
                try:
                    print(f"\n[INFO] Processing Section {section_idx}: {promotion_type[:60]}...")

                    # 이 섹션에 속하는 모든 carousel 찾기 (preceding 축 기반)
                    # 섹션 이후의 모든 c-carousel-list를 찾아서
                    # 각 carousel의 preceding::section[-1]이 현재 섹션인지 확인

                    all_carousels = tree.xpath(carousel_list_xpath)
                    section_carousels = []

                    for carousel in all_carousels:
                        # 이 carousel 앞의 가장 가까운 section 찾기
                        preceding_sections = carousel.xpath('preceding::section')
                        if preceding_sections:
                            nearest_section = preceding_sections[-1]  # 가장 가까운 section
                            # 현재 섹션과 동일한지 확인 (메모리 주소 비교)
                            if nearest_section == section_elem:
                                section_carousels.append(carousel)

                    print(f"[OK] Section {section_idx} mapped carousels: {len(section_carousels)}")

                    # 모든 carousel에서 li 아이템 수집
                    product_items = []
                    for carousel in section_carousels:
                        items = carousel.xpath(product_item_xpath)
                        product_items.extend(items)
                        if len(product_items) >= section_max_products:
                            break

                    product_items = product_items[:section_max_products]  # Limit to max
                    print(f"[OK] Section {section_idx} collected {len(product_items)} products")

                    # 각 제품 처리 (promotion_rank는 섹션 내에서 1-N)
                    for idx, item in enumerate(product_items[:section_max_products], 1):
                        try:
                            # promotion_rank는 섹션 내에서 1부터 시작
                            promotion_rank = idx

                            # 제품명 추출 (retailer_sku_name)
                            product_name = None
                            for name_xpath in name_xpaths:
                                name_elem = item.xpath(name_xpath)
                                if name_elem:
                                    product_name = name_elem[0].text_content().strip()
                                    break

                            # URL 추출
                            product_url = None
                            for url_xpath in url_xpaths:
                                url_elem = item.xpath(url_xpath)
                                if url_elem:
                                    product_url = url_elem[0]
                                    # 상대 경로를 절대 경로로 변환
                                    if product_url.startswith('/'):
                                        product_url = f"https://www.bestbuy.com{product_url}"
                                    break

                            # offer 추출 (숫자만)
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

                            if product_name and product_url:
                                # Validate data quality
                                self.validator.validate_item(product_name, product_url, 'bby_tv_pmt1')

                                product = {
                                    'page_type': page_type,
                                    'retailer_sku_name': product_name,
                                    'promotion_rank': promotion_rank,
                                    'offer': offer,
                                    'promotion_type': promotion_type,
                                    'product_url': product_url
                                }
                                all_products.append(product)
                                print(f"  [S{section_idx}-{promotion_rank}] {product_name[:50]}...")
                                print(f"      Offers: {offer}")
                                print(f"      URL: {product_url[:80]}...")

                        except Exception as e:
                            print(f"  [WARNING] Section {section_idx} product {idx} extraction failed: {e}")
                            import traceback
                            traceback.print_exc()
                            continue

                except Exception as e:
                    print(f"[WARNING] Section {section_idx} processing failed: {e}")
                    import traceback
                    traceback.print_exc()
                    continue

            print(f"\n[OK] Extracted {len(all_products)} products total ({len(sections)} sections)")
            return all_products

        except Exception as e:
            print(f"[ERROR] Product extraction failed: {e}")
            import traceback
            traceback.print_exc()
            return []

    def save_to_db(self, products):
        """Save to database"""
        if not products:
            print("[WARNING] No data to save")
            return False

        try:
            cursor = self.db_conn.cursor()

            # Calculate calendar week
            calendar_week = f"w{datetime.now().isocalendar().week}"

            # Calculate crawl_datetime (format: YYYY-MM-DD HH:MM:SS)
            now = datetime.now()
            crawl_datetime = now.strftime('%Y-%m-%d %H:%M:%S')

            # Config values
            table_name = self.config.get_table('pmt_data') or 'bby_tv_pmt1'
            account_name = self.config.get_constant('account_name', None, 'Bestbuy')

            # 데이터 삽입
            insert_query = f"""
                INSERT INTO {table_name}
                (account_name, page_type, retailer_sku_name, promotion_rank, offer,
                 promotion_type, product_url, crawl_datetime, calendar_week, batch_id)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """

            success_count = 0
            for product in products:
                try:
                    cursor.execute(insert_query, (
                        account_name,
                        product['page_type'],
                        product['retailer_sku_name'],
                        product['promotion_rank'],
                        product['offer'],
                        product['promotion_type'],
                        product['product_url'],
                        crawl_datetime,
                        calendar_week,
                        self.batch_id
                    ))
                    success_count += 1
                except Exception as e:
                    print(f"[ERROR] Save failed - Promotion Rank {product['promotion_rank']}: {e}")

            cursor.close()
            print(f"[OK] DB save complete: {success_count}/{len(products)} products")
            return True

        except Exception as e:
            print(f"[ERROR] DB save failed: {e}")
            import traceback
            traceback.print_exc()
            return False

    def run(self):
        """메인 실행"""
        try:
            print("="*80)
            print(f"Best Buy TV Promotion Crawler (DrissionPage) (Batch ID: {self.batch_id})")
            print("="*80)

            # DB 연결
            if not self.connect_db():
                return

            # 브라우저 설정 (DrissionPage)
            if not self.setup_browser():
                return

            # 페이지 접속
            if not self.navigate_to_page():
                return

            # 제품 정보 추출
            products = self.extract_products()

            # DB 저장
            if products:
                self.save_to_db(products)

                # Summary
                print("\n" + "="*80)
                print("Crawling complete!")
                print(f"Total products collected: {len(products)}")
                print("="*80)
            else:
                print("\n[ERROR] No products collected")

            # 데이터 검증 요약 출력
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
            print(f"[ERROR] Crawler execution error: {e}")
            import traceback
            traceback.print_exc()

        finally:
            if self.page:
                self.page.quit()
                print("\n[INFO] Browser closed")
            if self.db_conn:
                self.db_conn.close()
                print("[INFO] DB connection closed")

def main():
    crawler = BestBuyPromotionCrawler()
    crawler.run()

if __name__ == "__main__":
    main()
