"""
Amazon TV Crawler Config Setup Script
- amazon_tv_config 테이블 생성
- 하드코딩 값들을 DB로 이관
"""
import psycopg2
from config import DB_CONFIG


def create_table():
    """Create amazon_tv_config table (same structure as bby_tv_config)"""
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS amazon_tv_config (
                id SERIAL PRIMARY KEY,
                category VARCHAR(50) NOT NULL,
                config_key VARCHAR(100) NOT NULL,
                config_value TEXT NOT NULL,
                file_name VARCHAR(100),
                priority INTEGER DEFAULT 1,
                description VARCHAR(255),
                is_active BOOLEAN DEFAULT TRUE,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(category, config_key, file_name, priority)
            );
        """)

        # Create index for faster lookups
        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_amazon_tv_config_category_key
            ON amazon_tv_config(category, config_key);
        """)

        # Add UNIQUE constraint if not exists (for existing tables)
        cursor.execute("""
            DO $$
            BEGIN
                IF NOT EXISTS (
                    SELECT 1 FROM pg_constraint
                    WHERE conname = 'amazon_tv_config_category_config_key_file_name_priority_key'
                ) THEN
                    ALTER TABLE amazon_tv_config
                    ADD CONSTRAINT amazon_tv_config_category_config_key_file_name_priority_key
                    UNIQUE (category, config_key, file_name, priority);
                END IF;
            EXCEPTION WHEN duplicate_object THEN
                NULL;
            END $$;
        """)

        conn.commit()
        print("[OK] Created table: amazon_tv_config")

        cursor.close()
        conn.close()
        return True

    except Exception as e:
        print(f"[ERROR] Failed to create table: {e}")
        return False


def insert_configs():
    """Insert all config values"""
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()

        # 기존 XPath 레코드 삭제 (file_name 변경으로 인해)
        cursor.execute("DELETE FROM amazon_tv_config WHERE category = 'xpath' AND file_name = 'amazon_tv_dt1'")
        deleted = cursor.rowcount
        if deleted > 0:
            print(f"[INFO] Deleted {deleted} old xpath records with file_name='amazon_tv_dt1'")

        configs = [
            # =====================================================
            # CONSTANT - 수집 제한값
            # =====================================================
            ('constant', 'max_skus', '300', 'amazon_tv_dt1', 1, '상세페이지 최대 SKU 수집 개수'),
            ('constant', 'max_skus', '300', 'amazon_tv_dt2', 1, '상세페이지 최대 SKU 수집 개수'),
            ('constant', 'max_skus', '400', 'amazon_tv_main1', 1, '메인페이지 최대 SKU 수집 개수'),
            # dt1은 계정 전환 없음, dt2만 사용
            ('constant', 'account_switch_at', '100', 'amazon_tv_dt2', 1, 'dt2 계정 전환 기준 SKU 수'),
            ('constant', 'max_review_pages', '3', 'amazon_tv_dt1', 1, '리뷰 페이지 최대 수집 페이지 수'),
            ('constant', 'max_review_pages', '3', 'amazon_tv_dt2', 1, '리뷰 페이지 최대 수집 페이지 수'),
            ('constant', 'target_reviews', '20', 'amazon_tv_dt1', 1, '목표 리뷰 수집 개수'),
            ('constant', 'target_reviews', '20', 'amazon_tv_dt2', 1, '목표 리뷰 수집 개수'),

            # =====================================================
            # RETRY - 재시도 횟수
            # =====================================================
            ('retry', 'sorry_page_max', '5', 'amazon_tv_dt1', 1, 'Sorry 페이지 최대 재시도 횟수'),
            ('retry', 'sorry_page_max', '5', 'amazon_tv_dt2', 1, 'Sorry 페이지 최대 재시도 횟수'),
            ('retry', 'sorry_page_max', '20', 'amazon_tv_bsr1', 1, 'BSR Sorry 페이지 최대 재시도 횟수'),
            ('retry', 'sorry_page_max', '3', 'amazon_tv_main1', 1, '메인 Sorry 페이지 최대 재시도 횟수'),
            ('retry', 'throttling_max', '2', 'amazon_tv_bsr1', 1, '쓰로틀링 최대 재시도 횟수'),
            ('retry', 'page_verification', '2', 'amazon_tv_dt1', 1, '페이지 검증 최대 재시도 횟수'),
            ('retry', 'page_verification', '2', 'amazon_tv_dt2', 1, '페이지 검증 최대 재시도 횟수'),
            ('retry', 'field_extraction', '3', 'amazon_tv_dt1', 1, '필드 추출 최대 재시도 횟수'),
            ('retry', 'field_extraction', '3', 'amazon_tv_dt2', 1, '필드 추출 최대 재시도 횟수'),
            ('retry', 'browser_restart_after_fail', '5', 'amazon_tv_dt1', 1, '연속 실패 후 브라우저 재시작 기준'),
            ('retry', 'browser_restart_after_fail', '5', 'amazon_tv_dt2', 1, '연속 실패 후 브라우저 재시작 기준'),

            # =====================================================
            # TIMING - 대기 시간 (초)
            # =====================================================
            ('timing', 'throttling_wait_min', '15', 'amazon_tv_bsr1', 1, '쓰로틀링 대기 최소 시간(초)'),
            ('timing', 'throttling_wait_max', '20', 'amazon_tv_bsr1', 1, '쓰로틀링 대기 최대 시간(초)'),
            ('timing', 'sorry_page_wait_min', '5', 'amazon_tv_dt1', 1, 'Sorry 페이지 대기 최소 시간(초)'),
            ('timing', 'sorry_page_wait_max', '10', 'amazon_tv_dt1', 1, 'Sorry 페이지 대기 최대 시간(초)'),
            ('timing', 'sorry_page_wait_min', '5', 'amazon_tv_dt2', 1, 'Sorry 페이지 대기 최소 시간(초)'),
            ('timing', 'sorry_page_wait_max', '10', 'amazon_tv_dt2', 1, 'Sorry 페이지 대기 최대 시간(초)'),
            ('timing', 'page_load_wait', '10', 'amazon_tv_main1', 1, '페이지 로딩 대기 시간(초)'),
            ('timing', 'page_load_wait', '10', 'amazon_tv_dt1', 1, '페이지 로딩 대기 시간(초)'),
            ('timing', 'page_load_wait', '10', 'amazon_tv_dt2', 1, '페이지 로딩 대기 시간(초)'),
            ('timing', 'script_timeout', '21600', 'amazon_tv_crawl', 1, '전체 스크립트 타임아웃(초, 6시간)'),
            ('timing', 'between_products_min', '2', 'amazon_tv_dt1', 1, '제품 간 대기 최소 시간(초)'),
            ('timing', 'between_products_max', '5', 'amazon_tv_dt1', 1, '제품 간 대기 최대 시간(초)'),
            ('timing', 'between_products_min', '2', 'amazon_tv_dt2', 1, '제품 간 대기 최소 시간(초)'),
            ('timing', 'between_products_max', '5', 'amazon_tv_dt2', 1, '제품 간 대기 최대 시간(초)'),
            ('timing', 'review_page_wait', '5', 'amazon_tv_dt1', 1, '리뷰 페이지 대기 시간(초)'),
            ('timing', 'review_page_wait', '5', 'amazon_tv_dt2', 1, '리뷰 페이지 대기 시간(초)'),

            # =====================================================
            # BROWSER - 브라우저 설정
            # =====================================================
            ('browser', 'user_agent', 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36', 'amazon_tv_main1', 1, '메인 크롤러 User-Agent'),
            ('browser', 'user_agent', 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36', 'amazon_tv_dt1', 1, '상세 크롤러 User-Agent'),
            ('browser', 'user_agent', 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36', 'amazon_tv_dt2', 1, '상세 크롤러 User-Agent'),
            ('browser', 'user_agent', 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36', 'amazon_tv_bsr1', 1, 'BSR 크롤러 User-Agent'),

            # =====================================================
            # TEXT - 가격 상태 텍스트 패턴
            # =====================================================
            ('text', 'price_status_unavailable', 'Currently unavailable.', None, 1, '품절 상태 텍스트'),
            ('text', 'price_status_higher', 'Price higher than typical', None, 1, '평소보다 높은 가격 텍스트'),
            ('text', 'price_status_no_offers', 'No featured offers available', None, 1, '판매 제안 없음 텍스트'),
            ('text', 'price_status_see_cart', 'See price in cart', None, 1, '장바구니에서 가격 확인 텍스트'),
            ('text', 'price_status_add_cart', 'To see our price, add this item to your cart.', None, 1, '장바구니 추가 후 가격 확인 텍스트'),

            # =====================================================
            # TABLE - 테이블 이름 (공통)
            # =====================================================
            ('table', 'main_data', 'amazon_tv_main_crawled', None, 1, '메인 페이지 크롤링 데이터 테이블'),
            ('table', 'detail_data', 'amazon_tv_detail_crawled', None, 1, '상세 페이지 크롤링 데이터 테이블'),
            ('table', 'bsr_data', 'amazon_tv_bsr', None, 1, 'BSR 페이지 크롤링 데이터 테이블'),
            ('table', 'page_url', 'amazon_tv_main_page_url', None, 1, '메인 페이지 URL 테이블'),
            ('table', 'bsr_page_url', 'amazon_tv_bsr_page_url', None, 1, 'BSR 페이지 URL 테이블'),

            # =====================================================
            # URL - 기본 URL
            # =====================================================
            ('url', 'homepage', 'https://www.amazon.com', None, 1, '아마존 홈페이지 URL'),
            ('url', 'login_page', 'https://www.amazon.com/ap/signin', None, 1, '아마존 로그인 페이지 URL'),

            # =====================================================
            # ACCOUNT - 계정 설정
            # =====================================================
            # dt1: 단일 계정 사용 (계정 전환 없음)
            ('account', 'primary', 'unsandev0004', 'amazon_tv_dt1', 1, 'dt1 단일 계정'),
            # dt2: 계정 전환 (100개 SKU 후 ACCOUNT_1 → ACCOUNT_2)
            ('account', 'primary', 'unsandev0002', 'amazon_tv_dt2', 1, 'dt2 기본 계정 (처음 100개)'),
            ('account', 'secondary', 'unsandev0003', 'amazon_tv_dt2', 1, 'dt2 전환 계정 (100개 이후)'),

            # =====================================================
            # XPATH - 공통 XPath (dt1, dt2 공유)
            # =====================================================
            # 화면 크기 추출 (코드 fallback과 동일하게)
            ('xpath', 'screen_size_1', '//tr[contains(@class, "po-display.size")]//td[@class="a-span9"]//span[@class="a-size-base po-break-word"]', None, 1, '화면 크기 XPath 1 - po-display.size'),
            ('xpath', 'screen_size_2', '//table[@class="a-normal a-spacing-small"]//tr[contains(@class, "po-display.size")]//td[@class="a-span9"]//span[@class="a-size-base po-break-word"]', None, 2, '화면 크기 XPath 2 - table po-display'),
            ('xpath', 'screen_size_3', '//*[@id="poExpander"]/div[1]/div/table/tbody/tr[2]/td[2]/span', None, 3, '화면 크기 XPath 3 - poExpander'),
            ('xpath', 'screen_size_4', '//*[@id="productDetails_techSpec_section_1"]//tr[th[contains(text(), "Screen Size")]]/td', None, 4, '화면 크기 XPath 4 - techSpec'),
            ('xpath', 'screen_size_5', '//*[@id="productDetails_techSpec_section_1"]//tr[th[contains(text(), "Standing screen display size")]]/td', None, 5, '화면 크기 XPath 5 - Standing'),
            ('xpath', 'screen_size_6', '//*[@id="productDetails_detailBullets_sections1"]//tr[th[contains(text(), "Screen Size")]]/td', None, 6, '화면 크기 XPath 6 - detailBullets'),
            ('xpath', 'screen_size_7', '//table[@id="productDetails_techSpec_section_1"]//th[contains(., "Screen Size")]/following-sibling::td', None, 7, '화면 크기 XPath 7 - following-sibling'),
            ('xpath', 'screen_size_8', '//div[@id="detailBullets_feature_div"]//span[contains(text(), "Screen Size")]/../span[2]', None, 8, '화면 크기 XPath 8 - detailBullets div'),

            # 모델 연도 추출
            ('xpath', 'model_year_1', '//*[@id="productDetails_techSpec_section_1"]//tr[th[contains(text(), "Model Year")]]/td', None, 1, '모델 연도 XPath 1'),
            ('xpath', 'model_year_2', '//*[@id="productDetails_detailBullets_sections1"]//tr[th[contains(text(), "Model Year")]]/td', None, 2, '모델 연도 XPath 2'),
            ('xpath', 'model_year_3', '//*[@id="productDetails_techSpec_section_1"]//tr[th[contains(text(), "Item model number")]]/td', None, 3, '모델 연도 XPath 3'),
            ('xpath', 'model_year_4', '//*[@id="productDetails_detailBullets_sections1"]//tr[th[contains(text(), "Item model number")]]/td', None, 4, '모델 연도 XPath 4'),
            ('xpath', 'model_year_5', '//table[@id="productDetails_techSpec_section_1"]//th[contains(., "Model Year")]/following-sibling::td', None, 5, '모델 연도 XPath 5'),

            # 모델 번호 추출
            ('xpath', 'model_number_1', '//*[@id="productDetails_techSpec_section_1"]//tr[th[contains(text(), "Item model number")]]/td', None, 1, '모델 번호 XPath 1'),
            ('xpath', 'model_number_2', '//*[@id="productDetails_detailBullets_sections1"]//tr[th[contains(text(), "Item model number")]]/td', None, 2, '모델 번호 XPath 2'),
            ('xpath', 'model_number_3', '//table[@id="productDetails_techSpec_section_1"]//th[contains(., "Item model number")]/following-sibling::td', None, 3, '모델 번호 XPath 3'),

            # 리뷰 페이지 링크
            ('xpath', 'review_link_1', '//*[@id="reviews-medley-footer"]//a[contains(@href, "product-reviews")]/@href', None, 1, '리뷰 페이지 링크 XPath 1'),
            ('xpath', 'review_link_2', '//a[@data-hook="see-all-reviews-link-foot"]/@href', None, 2, '리뷰 페이지 링크 XPath 2'),
            ('xpath', 'review_link_3', '//div[@id="reviews-medley-footer"]//a[contains(@href, "product-reviews")]/@href', None, 3, '리뷰 페이지 링크 XPath 3'),
            ('xpath', 'review_link_4', '//*[@id="cr-pagination-footer-0"]/a/@href', None, 4, '리뷰 페이지 링크 XPath 4'),
            ('xpath', 'review_link_5', '//a[contains(@href, "/product-reviews/") and contains(text(), "reviews")]/@href', None, 5, '리뷰 페이지 링크 XPath 5'),

            # 리뷰 개수 (리뷰 페이지)
            ('xpath', 'review_count_1', '//*[@id="filter-info-section"]/div', None, 1, '리뷰 개수 XPath 1'),
            ('xpath', 'review_count_2', '//div[@data-hook="cr-filter-info-review-rating-count"]', None, 2, '리뷰 개수 XPath 2'),
            ('xpath', 'review_count_3', '//div[contains(@data-hook, "cr-filter-info")]', None, 3, '리뷰 개수 XPath 3'),
            ('xpath', 'review_count_4', '//*[contains(@id, "filter-info")]//span', None, 4, '리뷰 개수 XPath 4'),

            # 리뷰 본문
            ('xpath', 'review_body', '//span[@data-hook="review-body"]/span', None, 1, '리뷰 본문 XPath'),
            ('xpath', 'review_body_list', '//ul[@id="cm-cr-dp-review-list"]//li[@data-hook="review"]//span[@data-hook="review-body"]//span', None, 1, '리뷰 본문 목록 XPath'),

            # 배송 정보
            ('xpath', 'delivery_primary', '//*[@id="mir-layout-DELIVERY_BLOCK-slot-PRIMARY_DELIVERY_MESSAGE_LARGE"]/span', None, 1, '기본 배송 메시지 XPath'),
            ('xpath', 'delivery_secondary', '//*[@id="mir-layout-DELIVERY_BLOCK-slot-SECONDARY_DELIVERY_MESSAGE_LARGE"]/span', None, 2, '보조 배송 메시지 XPath'),
            ('xpath', 'delivery_holiday', '//*[@id="mir-layout-DELIVERY_BLOCK-slot-HOLIDAY_DELIVERY_MESSAGE"]/b/font', None, 3, '휴일 배송 메시지 XPath'),

            # 재고 상태
            ('xpath', 'availability', '//*[@id="availability"]/span', None, 1, '재고 상태 XPath'),

            # 할인 유형
            ('xpath', 'discount_type', '//*[@id="dealBadgeSupportingText"]', None, 1, '할인 유형 배지 XPath'),

            # 상품 상세 버튼
            ('xpath', 'item_details_button', '//span[contains(text(), "Item details")]/ancestor::a[contains(@class, "a-expander-header")]', None, 1, '상품 상세 펼치기 버튼 XPath'),

            # 가격 XPath
            ('xpath', 'price_1', '//*[@id="corePriceDisplay_desktop_feature_div"]/div[1]/span[1]', None, 1, '가격 XPath 1'),
            ('xpath', 'price_2', '//*[@id="corePriceDisplay_desktop_feature_div"]/div[1]/span[2]/span[2]', None, 2, '가격 XPath 2'),
            ('xpath', 'price_3', '//*[@id="corePrice_desktop"]/div/table/tbody/tr/td[2]/span[1]/span[2]', None, 3, '가격 XPath 3'),
            ('xpath', 'price_4', '//*[@id="corePrice_feature_div"]/div/div/span[1]/span[2]/span[2]', None, 4, '가격 XPath 4'),
            ('xpath', 'price_5', '//*[@id="corePriceDisplay_desktop_feature_div"]/div[1]/span[3]/span[2]/span[2]', None, 5, '가격 XPath 5'),

            # 원가 XPath
            ('xpath', 'original_price_1', '//*[@id="corePriceDisplay_desktop_feature_div"]//span[@class="aok-relative"]/span[contains(@class, "a-offscreen")]', None, 1, '원가 XPath 1'),
            ('xpath', 'original_price_2', '//*[@id="corePrice_desktop"]//span[@data-a-strike="true"]//span[@class="a-offscreen"]', None, 2, '원가 XPath 2'),

            # 별점
            ('xpath', 'star_rating_1', '//*[@id="acrPopover"]/span[1]/a/span', None, 1, '별점 XPath 1'),
            ('xpath', 'star_rating_2', '//*[@id="acrPopover"]//span[contains(@class, "a-icon-alt")]', None, 2, '별점 XPath 2'),
            ('xpath', 'star_rating_3', '//span[@data-hook="rating-out-of-text"]', None, 3, '별점 XPath 3'),
            ('xpath', 'star_rating_4', '//*[@id="averageCustomerReviews"]//span[contains(@class, "a-icon-alt")]', None, 4, '별점 XPath 4'),

            # 리뷰 개수 (제품 페이지)
            ('xpath', 'count_reviews_1', '//*[@id="acrCustomerReviewText"]', None, 1, '리뷰 개수 XPath 1'),
            ('xpath', 'count_reviews_2', '//span[@data-hook="total-review-count"]', None, 2, '리뷰 개수 XPath 2'),
            ('xpath', 'count_reviews_3', '//*[@id="averageCustomerReviews"]//a/span', None, 3, '리뷰 개수 XPath 3'),

            # 평점 개수
            ('xpath', 'total_ratings_1', '//*[@id="acrCustomerReviewText"]', None, 1, '평점 개수 XPath 1'),
            ('xpath', 'total_ratings_2', '//span[@id="acrCustomerReviewText"]', None, 2, '평점 개수 XPath 2'),
            ('xpath', 'total_ratings_3', '//div[@id="averageCustomerReviews"]//span[contains(text(), "ratings")]', None, 3, '평점 개수 XPath 3'),

            # 품절 확인
            ('xpath', 'unavailable_1', '//*[@id="availability"]/span', None, 1, '품절 확인 XPath 1'),
            ('xpath', 'unavailable_2', '//div[@id="availability"]//span[contains(text(), "unavailable")]', None, 2, '품절 확인 XPath 2'),
            ('xpath', 'unavailable_3', '//*[@id="outOfStock"]//span', None, 3, '품절 확인 XPath 3'),

            # 평소보다 높은 가격
            ('xpath', 'price_higher_1', '//*[@id="apex_desktop_qualifiedBuybox"]//*[contains(text(), "higher")]', None, 1, '높은 가격 경고 XPath 1'),
            ('xpath', 'price_higher_2', '//span[contains(text(), "Price higher than typical")]', None, 2, '높은 가격 경고 XPath 2'),

            # 판매 제안 없음
            ('xpath', 'no_offers_1', '//*[@id="apex_offerDisplay_desktop"]//span[contains(text(), "No featured offers")]', None, 1, '제안 없음 XPath 1'),
            ('xpath', 'no_offers_2', '//span[contains(text(), "No featured offers available")]', None, 2, '제안 없음 XPath 2'),
            ('xpath', 'no_offers_3', '//*[@id="availability"]//span[contains(text(), "No featured")]', None, 3, '제안 없음 XPath 3'),

            # 장바구니에서 가격 확인
            ('xpath', 'see_price_cart_1', '//a[contains(text(), "See price in cart")]', None, 1, '장바구니 가격 확인 XPath 1'),
            ('xpath', 'see_price_cart_2', '//span[@class="a-declarative"]//a[contains(text(), "See price in cart")]', None, 2, '장바구니 가격 확인 XPath 2'),

            # 장바구니 추가 후 가격 확인
            ('xpath', 'add_cart_price_1', '//span[contains(text(), "add this item to your cart")]', None, 1, '장바구니 추가 가격 XPath 1'),
            ('xpath', 'add_cart_price_2', '//span[contains(text(), "To see our price")]', None, 2, '장바구니 추가 가격 XPath 2'),

            # 리뷰 없음 (하단 리뷰 섹션)
            ('xpath', 'no_reviews_1', '//*[@id="cm-cr-dp-review-header"]/h3/span', None, 1, '리뷰 없음 XPath 1 - 리뷰 헤더'),
            ('xpath', 'no_reviews_2', '//span[@data-hook="top-customer-reviews-title"]', None, 2, '리뷰 없음 XPath 2 - 고객 리뷰 타이틀'),
            ('xpath', 'no_reviews_3', '//div[@id="cm-cr-dp-review-header"]//span[contains(text(), "No customer reviews")]', None, 3, '리뷰 없음 XPath 3 - 리뷰 헤더 div'),
            ('xpath', 'no_reviews_4', '//*[@id="cm_cr_dp_d_rating_histogram"]//span[contains(text(), "No customer reviews")]', None, 4, '리뷰 없음 XPath 4 - 히스토그램'),
            ('xpath', 'no_reviews_5', '//span[contains(text(), "Be the first to review")]', None, 5, '리뷰 없음 XPath 5 - 첫 리뷰 작성'),
            ('xpath', 'no_reviews_6', '//div[@id="averageCustomerReviews"]//span[contains(text(), "No customer")]', None, 6, '리뷰 없음 XPath 6 - 평균 리뷰'),

            # 0개 리뷰
            ('xpath', 'zero_reviews_1', '//*[@id="acrCustomerReviewText"]', None, 1, '0개 리뷰 확인 XPath 1'),
            ('xpath', 'zero_reviews_2', '//span[@id="acrCustomerReviewText"]', None, 2, '0개 리뷰 확인 XPath 2'),

            # AI 생성 리뷰 요약
            ('xpath', 'summarized_review_1', '//div[@data-testid="overall-summary"]//span[contains(@class, "__SAR2l0zNyyuZ")]', None, 1, 'AI 리뷰 요약 XPath 1'),
            ('xpath', 'summarized_review_2', '//*[@id="product-summary"]/p[1]/span', None, 2, 'AI 리뷰 요약 XPath 2'),
            ('xpath', 'summarized_review_3', '//div[@data-testid="overall-summary"]//span', None, 3, 'AI 리뷰 요약 XPath 3'),

            # 총 평점 개수 (count_of_star_ratings)
            ('xpath', 'count_of_star_ratings_1', '//*[@id="cm_cr_dp_d_rating_histogram"]/div[3]', None, 1, '총 평점 개수 XPath 1'),
            ('xpath', 'count_of_star_ratings_2', '//*[@id="acrCustomerReviewText"]', None, 2, '총 평점 개수 XPath 2'),
            ('xpath', 'count_of_star_ratings_3', '//span[@id="acrCustomerReviewText"]', None, 3, '총 평점 개수 XPath 3'),
            ('xpath', 'count_of_star_ratings_4', '//a[@id="acrCustomerReviewLink"]//span', None, 4, '총 평점 개수 XPath 4'),
        ]

        # Insert using INSERT ... ON CONFLICT to update existing values
        for config in configs:
            cursor.execute("""
                INSERT INTO amazon_tv_config (category, config_key, config_value, file_name, priority, description)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (category, config_key, file_name, priority)
                DO UPDATE SET config_value = EXCLUDED.config_value,
                              description = EXCLUDED.description,
                              updated_at = CURRENT_TIMESTAMP
            """, config)

        conn.commit()
        print(f"[OK] Inserted {len(configs)} config entries")

        cursor.close()
        conn.close()
        return True

    except Exception as e:
        print(f"[ERROR] Failed to insert configs: {e}")
        import traceback
        traceback.print_exc()
        return False


def show_stats():
    """Show config statistics"""
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()

        cursor.execute("""
            SELECT category, COUNT(*) as cnt
            FROM amazon_tv_config
            WHERE is_active = TRUE
            GROUP BY category
            ORDER BY category
        """)

        print("\n=== amazon_tv_config Statistics ===")
        total = 0
        for row in cursor.fetchall():
            print(f"  {row[0]}: {row[1]} entries")
            total += row[1]
        print(f"  -----------------")
        print(f"  Total: {total} entries")

        cursor.close()
        conn.close()

    except Exception as e:
        print(f"[ERROR] Failed to show stats: {e}")


if __name__ == '__main__':
    print("=" * 60)
    print("Amazon TV Config Setup")
    print("=" * 60)

    print("\n[STEP 1] Creating amazon_tv_config table...")
    if create_table():
        print("\n[STEP 2] Inserting config values...")
        if insert_configs():
            print("\n[STEP 3] Showing statistics...")
            show_stats()

    print("\n" + "=" * 60)
    print("Setup completed!")
    print("=" * 60)
