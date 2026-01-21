"""
Walmart TV Crawler Config Setup Script
- wmart_tv_config 테이블 생성
- 하드코딩 값들을 DB로 이관
"""
import psycopg2
from config import DB_CONFIG


def create_table():
    """Create wmart_tv_config table (same structure as amazon_tv_config)"""
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS wmart_tv_config (
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
            CREATE INDEX IF NOT EXISTS idx_wmart_tv_config_category_key
            ON wmart_tv_config(category, config_key);
        """)

        conn.commit()
        print("[OK] Created table: wmart_tv_config")

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

        configs = [
            # =====================================================
            # CONSTANT - 수집 제한값
            # =====================================================
            ('constant', 'max_skus', '300', 'wmart_tv_dt1', 1, '상세 크롤러 최대 SKU 수집 개수'),
            ('constant', 'max_skus', '9999', 'wmart_tv_main1', 1, 'main1 크롤러 최대 SKU 수집 개수'),
            ('constant', 'max_skus', '9999', 'wmart_tv_main2', 1, 'main2 크롤러 최대 SKU 수집 개수'),
            ('constant', 'max_skus', '100', 'wmart_tv_bsr', 1, 'BSR 크롤러 최대 SKU 수집 개수 (1-100)'),
            ('constant', 'account_name', 'Walmart', None, 1, '모든 Walmart 크롤러 계정명'),

            # 페이지 범위 설정
            ('constant', 'page_start', '1', 'wmart_tv_main1', 1, 'main1 시작 페이지 번호'),
            ('constant', 'page_end', '5', 'wmart_tv_main1', 1, 'main1 종료 페이지 번호'),
            ('constant', 'page_start', '6', 'wmart_tv_main2', 1, 'main2 시작 페이지 번호'),
            ('constant', 'page_end', '10', 'wmart_tv_main2', 1, 'main2 종료 페이지 번호'),

            # =====================================================
            # RETRY - 재시도 횟수
            # =====================================================
            ('retry', 'max_retries', '2', 'wmart_tv_main1', 1, '페이지 스크래핑 최대 재시도 횟수'),
            ('retry', 'max_retries', '2', 'wmart_tv_main2', 1, '페이지 스크래핑 최대 재시도 횟수'),
            ('retry', 'max_page_retries', '2', 'wmart_tv_main1', 1, '실패 페이지 최대 재시도 횟수'),
            ('retry', 'max_page_retries', '2', 'wmart_tv_main2', 1, '실패 페이지 최대 재시도 횟수'),

            # =====================================================
            # TIMING - 대기 시간 (초)
            # =====================================================
            # 홈페이지/브라우즈 타이밍
            ('timing', 'homepage_wait_min', '8', None, 1, '홈페이지 최소 대기 시간'),
            ('timing', 'homepage_wait_max', '12', None, 1, '홈페이지 최대 대기 시간'),
            ('timing', 'browse_wait_min', '10', None, 1, '브라우즈 페이지 최소 대기 시간'),
            ('timing', 'browse_wait_max', '15', None, 1, '브라우즈 페이지 최대 대기 시간'),

            # 페이지 로드 타이밍
            ('timing', 'page_load_wait_min', '8', 'wmart_tv_main1', 1, '페이지 로드 최소 대기 시간'),
            ('timing', 'page_load_wait_max', '12', 'wmart_tv_main1', 1, '페이지 로드 최대 대기 시간'),
            ('timing', 'page_load_wait_min', '8', 'wmart_tv_main2', 1, '페이지 로드 최소 대기 시간'),
            ('timing', 'page_load_wait_max', '12', 'wmart_tv_main2', 1, '페이지 로드 최대 대기 시간'),
            ('timing', 'page_load_wait_min', '8', 'wmart_tv_bsr', 1, '페이지 로드 최소 대기 시간'),
            ('timing', 'page_load_wait_max', '12', 'wmart_tv_bsr', 1, '페이지 로드 최대 대기 시간'),

            # 직접 URL 접근 대기 타이밍
            ('timing', 'direct_url_wait_min', '12', None, 1, '직접 URL 접근 최소 대기 시간'),
            ('timing', 'direct_url_wait_max', '18', None, 1, '직접 URL 접근 최대 대기 시간'),

            # 페이지 간 대기 타이밍
            ('timing', 'between_pages_min', '8', 'wmart_tv_main1', 1, '페이지 간 최소 대기 시간'),
            ('timing', 'between_pages_max', '12', 'wmart_tv_main1', 1, '페이지 간 최대 대기 시간'),
            ('timing', 'between_pages_min', '8', 'wmart_tv_main2', 1, '페이지 간 최소 대기 시간'),
            ('timing', 'between_pages_max', '12', 'wmart_tv_main2', 1, '페이지 간 최대 대기 시간'),
            ('timing', 'between_pages_min', '5', 'wmart_tv_bsr', 1, '페이지 간 최소 대기 시간'),
            ('timing', 'between_pages_max', '8', 'wmart_tv_bsr', 1, '페이지 간 최대 대기 시간'),

            # 재시도 대기 타이밍
            ('timing', 'retry_wait_min', '10', None, 1, '재시도 전 최소 대기 시간'),
            ('timing', 'retry_wait_max', '15', None, 1, '재시도 전 최대 대기 시간'),
            ('timing', 'robot_retry_base_wait', '30', None, 1, '로봇 감지 재시도 기본 대기 시간'),
            ('timing', 'robot_retry_increment', '15', None, 1, '로봇 감지 재시도당 증가 시간'),

            # 스크롤 타이밍
            ('timing', 'scroll_wait', '3', None, 1, '스크롤 후 대기 시간'),
            ('timing', 'scroll_wait_min', '1', None, 1, '랜덤 스크롤 최소 대기 시간'),
            ('timing', 'scroll_wait_max', '2', None, 1, '랜덤 스크롤 최대 대기 시간'),
            ('timing', 'product_load_wait_min', '5', None, 1, '상품 로드 최소 대기 시간'),
            ('timing', 'product_load_wait_max', '8', None, 1, '상품 로드 최대 대기 시간'),

            # CAPTCHA 타이밍
            ('timing', 'captcha_wait', '60', None, 1, '수동 CAPTCHA 처리 대기 시간'),
            ('timing', 'robot_recovery_wait', '30', None, 1, '로봇 감지 복구 대기 시간'),
            ('timing', 'captcha_after_wait_min', '3', None, 1, 'CAPTCHA 처리 후 최소 대기 시간'),
            ('timing', 'captcha_after_wait_max', '5', None, 1, 'CAPTCHA 처리 후 최대 대기 시간'),
            ('timing', 'scroll_between_wait_min', '1.5', None, 1, '스크롤 간 최소 대기 시간'),
            ('timing', 'scroll_between_wait_max', '2.5', None, 1, '스크롤 간 최대 대기 시간'),

            # dt1 전용 타이밍
            ('timing', 'detail_page_load_wait_min', '6', 'wmart_tv_dt1', 1, '상세 페이지 로드 최소 대기 시간'),
            ('timing', 'detail_page_load_wait_max', '10', 'wmart_tv_dt1', 1, '상세 페이지 로드 최대 대기 시간'),
            ('timing', 'scroll_step_wait_min', '3', 'wmart_tv_dt1', 1, '스크롤 단계 최소 대기 시간'),
            ('timing', 'scroll_step_wait_max', '4', 'wmart_tv_dt1', 1, '스크롤 단계 최대 대기 시간'),
            ('timing', 'api_retry_wait_min', '5', 'wmart_tv_dt1', 1, 'API 재시도 전 최소 대기 시간'),
            ('timing', 'api_retry_wait_max', '8', 'wmart_tv_dt1', 1, 'API 재시도 전 최대 대기 시간'),

            # bsr 전용 타이밍
            ('timing', 'actions_pause_min', '0.1', 'wmart_tv_bsr', 1, 'ActionChains pause 최소 시간'),
            ('timing', 'actions_pause_max', '0.3', 'wmart_tv_bsr', 1, 'ActionChains pause 최대 시간'),

            # =====================================================
            # TABLE - 테이블 이름 (공통)
            # =====================================================
            ('table', 'main_data_1', 'wmart_tv_main_1', None, 1, '메인 페이지 1-5 크롤링 데이터 테이블'),
            ('table', 'main_data_2', 'wmart_tv_main_2', None, 1, '메인 페이지 6-10 크롤링 데이터 테이블'),
            ('table', 'bsr_data', 'wmart_tv_bsr_crawl', None, 1, 'BSR 페이지 크롤링 데이터 테이블'),
            ('table', 'detail_data', 'Walmart_tv_detail_crawled', None, 1, '상세 페이지 크롤링 데이터 테이블'),
            ('table', 'main_page_url', 'wmart_tv_main_page_url', None, 1, '메인 페이지 URL 테이블'),
            ('table', 'bsr_page_url', 'wmart_tv_bsr_page_url', None, 1, 'BSR 페이지 URL 테이블'),
            ('table', 'xpath_selectors', 'xpath_selectors', None, 1, 'XPath 셀렉터 테이블'),
            ('table', 'item_mst', 'tv_item_mst', None, 1, 'TV 아이템 마스터 테이블'),

            # =====================================================
            # URL - 기본 URL
            # =====================================================
            ('url', 'homepage', 'https://www.walmart.com', None, 1, 'Walmart 홈페이지 URL'),
            ('url', 'browse_tvs', 'https://www.walmart.com/browse/electronics/tvs/3944_1060825', None, 1, 'TV 브라우즈 페이지 URL'),

            # =====================================================
            # BROWSER - 브라우저 설정
            # =====================================================
            ('browser', 'window_size', '1920,1080', None, 1, '브라우저 창 크기'),
            ('browser', 'page_load_timeout', '60', None, 1, '페이지 로드 타임아웃 (초)'),
            ('browser', 'webdriver_wait', '20', None, 1, 'WebDriverWait 타임아웃 (초)'),

            # =====================================================
            # TEXT - 로봇 감지 텍스트 패턴
            # =====================================================
            ('text', 'robot_check_1', 'Robot or human?', None, 1, '로봇 감지 텍스트 1'),
            ('text', 'robot_check_2', 'Enter the characters you see below', None, 1, '로봇 감지 텍스트 2'),
            ('text', 'captcha_keyword_1', 'press & hold', None, 1, 'CAPTCHA 키워드 1'),
            ('text', 'captcha_keyword_2', 'press and hold', None, 1, 'CAPTCHA 키워드 2'),
            ('text', 'captcha_keyword_3', 'captcha', None, 1, 'CAPTCHA 키워드 3'),
            ('text', 'captcha_keyword_4', 'human verification', None, 1, 'CAPTCHA 키워드 4'),

            # =====================================================
            # SCROLL - 스크롤 설정
            # =====================================================
            ('scroll', 'max_rounds', '2', None, 1, '상품 로드를 위한 최대 스크롤 횟수'),
            ('scroll', 'random_amount_min', '150', None, 1, '랜덤 스크롤 최소 이동량'),
            ('scroll', 'random_amount_max', '300', None, 1, '랜덤 스크롤 최대 이동량'),
            ('scroll', 'recovery_amount_min', '200', None, 1, '복구 스크롤 최소 이동량'),
            ('scroll', 'recovery_amount_max', '500', None, 1, '복구 스크롤 최대 이동량'),
        ]

        # Insert using INSERT ... ON CONFLICT to update existing values
        for config in configs:
            cursor.execute("""
                INSERT INTO wmart_tv_config (category, config_key, config_value, file_name, priority, description)
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
            FROM wmart_tv_config
            WHERE is_active = TRUE
            GROUP BY category
            ORDER BY category
        """)

        print("\n=== wmart_tv_config Statistics ===")
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
    print("Walmart TV Config Setup")
    print("=" * 60)

    print("\n[STEP 1] Creating wmart_tv_config table...")
    if create_table():
        print("\n[STEP 2] Inserting config values...")
        if insert_configs():
            print("\n[STEP 3] Showing statistics...")
            show_stats()

    print("\n" + "=" * 60)
    print("Setup completed!")
    print("=" * 60)
