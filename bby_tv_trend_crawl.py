"""
Best Buy Trending Deals - TVs & Projectors Crawler (DrissionPage)
https://www.bestbuy.com/ → Trending deals → TVs & Projectors
수집 항목: rank, product_name, product_url
저장 테이블: bby_tv_Trend_crawl
"""
import time
import random
import os
import psycopg2
from datetime import datetime
import pytz
from DrissionPage import ChromiumPage
from lxml import html

# Import database configuration
from config import DB_CONFIG
from bby_config_loader import get_config
from bby_utils import load_excluded_items, is_excluded_url

class BestBuyTrendCrawler:
    def __init__(self):
        self.page = None
        self.db_conn = None
        self.korea_tz = pytz.timezone('Asia/Seoul')
        self.batch_id = datetime.now(self.korea_tz).strftime('%Y%m%d_%H%M%S')

        # Config loader 초기화
        self.config = get_config()
        self.file_name = 'bby_tv_trend_crawl'

        # Load excluded items (is_product=false)
        self.excluded_items = load_excluded_items()

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
        """DrissionPage 브라우저 설정"""
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

    def click_tvs_category(self):
        """Trending deals에서 TVs & Projectors 카테고리 클릭"""
        try:
            # Config values
            homepage_url = self.config.get_url('homepage', self.file_name) or 'https://www.bestbuy.com/'
            homepage_load_range = self.config.get_timing_range('homepage_load_wait', self.file_name) or (5, 8)
            page_load_wait = self.config.get_float('timing', 'page_load_wait', self.file_name, 3)
            scroll_step = self.config.get_int('timing', 'scroll_step', self.file_name, 500)
            scroll_wait = self.config.get_float('timing', 'scroll_wait', self.file_name, 1)
            trending_section_xpath = self.config.get('xpath', 'trending_section', self.file_name) or '//div[@data-testid="story-block-trending_deals_story"]'
            trending_section_wait = self.config.get_float('timing', 'trending_section_wait', self.file_name, 2)
            button_view_wait = self.config.get_float('timing', 'button_view_wait', self.file_name, 2)
            category_click_range = self.config.get_timing_range('category_click_wait', self.file_name) or (3, 5)

            # 홈페이지 접속
            print(f"[INFO] Best Buy 홈페이지 접속...")
            self.page.get(homepage_url)
            time.sleep(random.uniform(*homepage_load_range))

            print("[INFO] 페이지 로딩 대기 중...")
            time.sleep(page_load_wait)

            # 페이지 소스 저장 (디버깅용)
            try:
                page_source = self.page.html
                with open('bby_homepage_debug.html', 'w', encoding='utf-8') as f:
                    f.write(page_source)
                print("[DEBUG] 홈페이지 소스 저장: bby_homepage_debug.html")
            except Exception as e:
                print(f"[WARNING] 페이지 소스 저장 실패: {e}")

            # Trending Deals 섹션으로 스크롤
            print("[INFO] Trending Deals 섹션 찾는 중...")

            # 페이지를 아래로 천천히 스크롤하여 Trending Deals 섹션 로드
            scroll_height = self.page.run_js("return document.body.scrollHeight")
            current_position = 0

            trending_found = False
            while current_position < scroll_height:
                current_position += scroll_step
                self.page.run_js(f"window.scrollTo(0, {current_position})")
                time.sleep(scroll_wait)

                # Trending Deals 섹션이 나타났는지 확인
                try:
                    trending_section = self.page.ele(f'xpath:{trending_section_xpath}', timeout=2)
                    if trending_section:
                        print("[OK] Trending Deals 섹션 발견")
                        trending_section.scroll.to_see()
                        time.sleep(trending_section_wait)
                        trending_found = True
                        break
                except:
                    pass

            if not trending_found:
                print("[WARNING] Trending Deals 섹션을 찾지 못했습니다. 계속 진행...")

            # TVs & Projectors 버튼 찾기 및 클릭 (fallback list from config)
            tvs_button_xpaths = self.config.get_xpath_list('tvs_button', self.file_name) or [
                '/html/body/div[6]/div/div/div/div/div[4]/div/div/div/div/div/div[2]/div[1]/button[1]',
                '//button[.//span[contains(text(), "TVs") and contains(text(), "Projectors")]]',
            ]
            tvs_button_selectors = [f'xpath:{xpath}' for xpath in tvs_button_xpaths]

            clicked = False
            for idx, selector in enumerate(tvs_button_selectors, 1):
                try:
                    print(f"[INFO] TVs & Projectors 버튼 찾기 시도 {idx}/{len(tvs_button_selectors)}...")
                    tvs_button = self.page.ele(selector, timeout=5)

                    if tvs_button:
                        # 버튼이 보일 때까지 스크롤
                        tvs_button.scroll.to_see()
                        time.sleep(button_view_wait)

                        # 클릭
                        tvs_button.click()
                        print("[OK] TVs & Projectors 카테고리 클릭 완료")
                        clicked = True
                        time.sleep(random.uniform(*category_click_range))
                        break
                except Exception as e:
                    print(f"[DEBUG] Selector {idx} 실패: {e}")
                    continue

            if not clicked:
                print("[ERROR] TVs & Projectors 버튼을 찾을 수 없습니다.")
                print("[INFO] bby_homepage_debug.html 파일을 확인하세요.")
                return False

            return True

        except Exception as e:
            print(f"[ERROR] TVs & Projectors 카테고리 클릭 실패: {e}")
            import traceback
            traceback.print_exc()
            return False

    def extract_trending_products(self):
        """Trending deals TVs & Projectors 제품 정보 추출"""
        try:
            print("\n[INFO] 제품 정보 추출 시작...")

            # Config values
            extract_wait = self.config.get_float('timing', 'extract_wait', self.file_name, 3)
            page_type = self.config.get_constant('page_type_trend', self.file_name) or 'Trend'
            product_items_xpaths = self.config.get_xpath_list('product_items', self.file_name) or [
                '//div[@data-testid="story-block-trending_deals_story"]//ul//li[@data-carousel-index]'
            ]
            rank_xpaths = self.config.get_xpath_list('rank', self.file_name) or [
                './/div[@data-testid="PriceBlockOffer-CarouselProductRank-TestID"]//span'
            ]
            name_xpaths = self.config.get_xpath_list('product_name', self.file_name) or [
                './/span[@data-testid="ProductCard-Title-TestID"]'
            ]
            url_xpaths = self.config.get_xpath_list('product_url', self.file_name) or [
                './/a[@data-testid="trending-deals-card-TestID"]/@href'
            ]

            # 추가 대기 시간
            time.sleep(extract_wait)

            # 페이지 소스 가져오기
            page_source = self.page.html
            tree = html.fromstring(page_source)

            # 디버깅용 HTML 저장
            try:
                with open('bby_trending_tvs_debug.html', 'w', encoding='utf-8') as f:
                    f.write(page_source)
                print("[DEBUG] Trending TVs 페이지 소스 저장: bby_trending_tvs_debug.html")
            except Exception as e:
                print(f"[WARNING] 페이지 소스 저장 실패: {e}")

            products = []

            # 모든 제품 아이템 찾기 (fallback list from config)
            product_items = []
            for xpath in product_items_xpaths:
                product_items = tree.xpath(xpath)
                if product_items:
                    print(f"[OK] XPath로 총 {len(product_items)}개 제품 발견")
                    break

            if not product_items:
                print("[ERROR] 제품 아이템을 찾을 수 없습니다.")
                print("[INFO] bby_trending_tvs_debug.html 파일을 확인하세요.")
                return []

            for idx, item in enumerate(product_items, 1):
                try:
                    # 순위 추출 (fallback list from config)
                    rank = None
                    for rank_xpath in rank_xpaths:
                        rank_elem = item.xpath(rank_xpath)
                        if rank_elem:
                            rank_text = rank_elem[0].text_content().strip()
                            # "#1" -> "1" 형식으로 변환
                            rank = rank_text.replace('#', '').strip()
                            break
                    if not rank:
                        rank = str(idx)

                    # 제품명 추출 (fallback list from config)
                    product_name = None
                    for name_xpath in name_xpaths:
                        if '@aria-label' in name_xpath:
                            name_elem = item.xpath(name_xpath)
                            if name_elem:
                                product_name = name_elem[0].strip()
                                if product_name and len(product_name) > 5:
                                    break
                        else:
                            name_elem = item.xpath(name_xpath)
                            if name_elem:
                                product_name = name_elem[0].text_content().strip()
                                if product_name and len(product_name) > 5:
                                    break

                    # URL 추출 (fallback list from config)
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
                        # Skip is_product=false items
                        if is_excluded_url(product_url, self.excluded_items):
                            print(f"  [SKIP {rank}] is_product=false - excluded")
                            continue

                        product = {
                            'page_type': page_type,
                            'rank': int(rank) if rank.isdigit() else idx,
                            'product_name': product_name,
                            'product_url': product_url
                        }
                        products.append(product)
                        print(f"  [{rank}] {product_name[:50]}...")
                    else:
                        print(f"  [WARNING] 제품 {idx} - 이름 또는 URL 누락 (Name: {product_name is not None}, URL: {product_url is not None})")

                except Exception as e:
                    print(f"  [WARNING] 제품 {idx} 추출 실패: {e}")
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

            # Config values
            table_name = self.config.get_table('trend_data') or 'bby_tv_Trend_crawl'
            account_name = self.config.get_constant('account_name', None, 'Bestbuy')

            # 테이블 존재 확인 및 생성
            cursor.execute(f"""
                CREATE TABLE IF NOT EXISTS {table_name} (
                    id SERIAL PRIMARY KEY,
                    account_name VARCHAR(50),
                    page_type VARCHAR(50),
                    rank INTEGER,
                    product_name TEXT,
                    product_url TEXT,
                    crawl_strdatetime VARCHAR(20),
                    batch_id VARCHAR(50),
                    calendar_week VARCHAR(10)
                )
            """)

            # Calculate calendar week
            calendar_week = f"w{datetime.now().isocalendar().week}"

            # Calculate crawl_strdatetime (format: YYYYMMDDHHMISS + microseconds 4 digits)
            now = datetime.now()
            crawl_strdatetime = now.strftime('%Y%m%d%H%M%S') + now.strftime('%f')[:4]

            # 데이터 삽입
            insert_query = f"""
                INSERT INTO {table_name} (account_name, batch_id, page_type, rank, product_name, product_url, crawl_strdatetime, calendar_week)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """

            success_count = 0
            for product in products:
                try:
                    cursor.execute(insert_query, (
                        account_name,
                        self.batch_id,
                        product['page_type'],
                        product['rank'],
                        product['product_name'],
                        product['product_url'],
                        crawl_strdatetime,
                        calendar_week
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
            print(f"Best Buy Trending Deals - TVs & Projectors Crawler (DrissionPage)")
            print(f"Batch ID: {self.batch_id}")
            print("="*80)

            # DB 연결
            if not self.connect_db():
                return

            # Add batch_id column if not exists
            try:
                cursor = self.db_conn.cursor()
                table_name = self.config.get_table('trend_data') or 'bby_tv_Trend_crawl'
                cursor.execute(f"""
                    ALTER TABLE {table_name}
                    ADD COLUMN IF NOT EXISTS batch_id VARCHAR(50)
                """)
                self.db_conn.commit()
                cursor.close()
                print("[OK] Table schema updated (batch_id column added if needed)")
            except Exception as e:
                print(f"[WARNING] Could not add batch_id column: {e}")

            # 브라우저 설정
            if not self.setup_browser():
                return

            # TVs & Projectors 카테고리 클릭
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
                print("\n[ERROR] 수집된 제품이 없습니다.")

        except Exception as e:
            print(f"[ERROR] 크롤러 실행 오류: {e}")
            import traceback
            traceback.print_exc()

        finally:
            if self.page:
                self.page.quit()
                print("\n[INFO] 브라우저 종료")
            if self.db_conn:
                self.db_conn.close()
                print("[INFO] DB 연결 종료")

def main():
    crawler = BestBuyTrendCrawler()
    crawler.run()

if __name__ == "__main__":
    main()
