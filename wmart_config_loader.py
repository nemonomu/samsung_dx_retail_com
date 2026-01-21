"""
Walmart TV Crawler Config Loader
- wmart_tv_config 테이블에서 설정값 로드
- 하드코딩 제거를 위한 중앙 설정 관리
"""
import psycopg2
from config import DB_CONFIG


class WmartConfigLoader:
    _instance = None
    _configs = None

    def __new__(cls):
        """싱글톤 패턴 - 한 번만 DB에서 로드"""
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._load_all()
        return cls._instance

    def _load_all(self):
        """DB에서 전체 설정 로드"""
        self._configs = {
            'url': {},
            'xpath': {},
            'timing': {},
            'retry': {},
            'constant': {},
            'table': {},
            'browser': {},
            'text': {},
            'scroll': {}
        }

        try:
            conn = psycopg2.connect(**DB_CONFIG)
            cursor = conn.cursor()
            cursor.execute("""
                SELECT category, config_key, config_value, file_name, priority
                FROM wmart_tv_config
                WHERE is_active = TRUE
                ORDER BY category, config_key, file_name, priority
            """)

            for row in cursor.fetchall():
                category, key, value, file_name, priority = row
                file_key = file_name or '_common'

                if category not in self._configs:
                    self._configs[category] = {}

                if file_key not in self._configs[category]:
                    self._configs[category][file_key] = {}

                # priority가 있는 경우 리스트로 저장 (fallback용)
                if key not in self._configs[category][file_key]:
                    self._configs[category][file_key][key] = []

                self._configs[category][file_key][key].append({
                    'value': value,
                    'priority': priority
                })

            cursor.close()
            conn.close()
            print(f"[CONFIG] Loaded {sum(len(v) for cat in self._configs.values() for v in cat.values())} wmart config entries")

        except Exception as e:
            print(f"[CONFIG ERROR] Failed to load wmart configs: {e}")
            raise

    def get(self, category, key, file_name=None, default=None):
        """
        단일 값 반환 (priority=1인 첫 번째 값)

        Args:
            category: url, xpath, timing, retry, constant, table, browser, text, scroll
            key: config_key
            file_name: 파일명 (None이면 _common에서 찾음)
            default: 값이 없을 때 반환할 기본값

        Returns:
            config_value (str) or default
        """
        try:
            file_key = file_name or '_common'

            # 파일별 설정 먼저 확인
            if file_key in self._configs.get(category, {}):
                if key in self._configs[category][file_key]:
                    entries = self._configs[category][file_key][key]
                    # priority가 가장 낮은 (우선순위 높은) 값 반환
                    sorted_entries = sorted(entries, key=lambda x: x['priority'])
                    return sorted_entries[0]['value']

            # 공통 설정에서 확인
            if '_common' in self._configs.get(category, {}):
                if key in self._configs[category]['_common']:
                    entries = self._configs[category]['_common'][key]
                    sorted_entries = sorted(entries, key=lambda x: x['priority'])
                    return sorted_entries[0]['value']

            return default

        except Exception as e:
            print(f"[CONFIG WARNING] get({category}, {key}, {file_name}) failed: {e}")
            return default

    def get_int(self, category, key, file_name=None, default=0):
        """정수형으로 반환"""
        value = self.get(category, key, file_name)
        if value is None:
            return default
        try:
            return int(value)
        except (ValueError, TypeError):
            return default

    def get_float(self, category, key, file_name=None, default=0.0):
        """실수형으로 반환"""
        value = self.get(category, key, file_name)
        if value is None:
            return default
        try:
            return float(value)
        except (ValueError, TypeError):
            return default if default is not None else 0.0

    def get_timing_range(self, key_prefix, file_name=None):
        """
        타이밍 min/max 쌍 반환 (random.uniform용)

        Args:
            key_prefix: 예) 'page_load_wait' -> page_load_wait_min, page_load_wait_max 찾음
            file_name: 파일명

        Returns:
            tuple (min_value, max_value) as floats
        """
        min_val = self.get_float('timing', f'{key_prefix}_min', file_name, default=None)
        max_val = self.get_float('timing', f'{key_prefix}_max', file_name, default=None)

        # None 체크 (0.0도 유효한 값이므로 is not None 사용)
        if min_val is not None and max_val is not None:
            return (min_val, max_val)
        return None

    def get_scroll_range(self, key_prefix):
        """
        스크롤 min/max 쌍 반환 (random.randint용)

        Args:
            key_prefix: 예) 'random_amount' -> random_amount_min, random_amount_max 찾음

        Returns:
            tuple (min_value, max_value) as ints
        """
        min_val = self.get_int('scroll', f'{key_prefix}_min', None, default=None)
        max_val = self.get_int('scroll', f'{key_prefix}_max', None, default=None)

        if min_val is not None and max_val is not None:
            return (min_val, max_val)
        return None

    def get_url(self, key, file_name=None):
        """URL 반환"""
        return self.get('url', key, file_name)

    def get_table(self, key):
        """테이블명 반환 (공통)"""
        return self.get('table', key, None)

    def get_retry(self, key, file_name=None, default=1):
        """재시도 횟수 반환"""
        return self.get_int('retry', key, file_name, default)

    def get_constant(self, key, file_name=None, default=None):
        """상수값 반환"""
        return self.get('constant', key, file_name, default)

    def get_constant_int(self, key, file_name=None, default=0):
        """상수값 정수로 반환"""
        return self.get_int('constant', key, file_name, default)

    def get_browser(self, key, file_name=None, default=None):
        """브라우저 설정 반환"""
        return self.get('browser', key, file_name, default)

    def get_browser_int(self, key, file_name=None, default=0):
        """브라우저 설정 정수로 반환"""
        return self.get_int('browser', key, file_name, default)

    def get_text(self, key, default=None):
        """텍스트 패턴 반환 (공통)"""
        return self.get('text', key, None, default)

    def get_robot_check_texts(self):
        """모든 로봇 감지 텍스트 반환"""
        texts = []
        for i in range(1, 10):  # robot_check_1 ~ robot_check_9
            text = self.get_text(f'robot_check_{i}')
            if text:
                texts.append(text)
        return texts

    def get_captcha_keywords(self):
        """모든 CAPTCHA 키워드 반환"""
        keywords = []
        for i in range(1, 10):  # captcha_keyword_1 ~ captcha_keyword_9
            keyword = self.get_text(f'captcha_keyword_{i}')
            if keyword:
                keywords.append(keyword)
        return keywords

    def get_page_range(self, file_name):
        """
        페이지 범위 반환 (page_start, page_end)

        Args:
            file_name: 'wmart_tv_main1' 또는 'wmart_tv_main2'

        Returns:
            tuple (start, end) as ints
        """
        start = self.get_constant_int('page_start', file_name, default=1)
        end = self.get_constant_int('page_end', file_name, default=5)
        return (start, end)

    def reload(self):
        """설정 다시 로드 (런타임 중 설정 변경 시)"""
        self._load_all()


# 편의를 위한 전역 인스턴스
_loader = None


def get_wmart_config():
    """전역 WmartConfigLoader 인스턴스 반환"""
    global _loader
    if _loader is None:
        _loader = WmartConfigLoader()
    return _loader


# 테스트
if __name__ == '__main__':
    config = get_wmart_config()

    print("\n=== URL ===")
    print(f"homepage: {config.get_url('homepage')}")
    print(f"browse_tvs: {config.get_url('browse_tvs')}")

    print("\n=== Table ===")
    print(f"main_data_1: {config.get_table('main_data_1')}")
    print(f"main_data_2: {config.get_table('main_data_2')}")
    print(f"bsr_data: {config.get_table('bsr_data')}")
    print(f"detail_data: {config.get_table('detail_data')}")

    print("\n=== Constant ===")
    print(f"max_skus (dt1): {config.get_constant_int('max_skus', 'wmart_tv_dt1')}")
    print(f"max_skus (main1): {config.get_constant_int('max_skus', 'wmart_tv_main1')}")
    print(f"max_skus (main2): {config.get_constant_int('max_skus', 'wmart_tv_main2')}")
    print(f"max_skus (bsr): {config.get_constant_int('max_skus', 'wmart_tv_bsr')}")
    print(f"account_name: {config.get_constant('account_name')}")

    print("\n=== Page Range ===")
    print(f"main1 page range: {config.get_page_range('wmart_tv_main1')}")
    print(f"main2 page range: {config.get_page_range('wmart_tv_main2')}")

    print("\n=== Retry ===")
    print(f"max_retries (main1): {config.get_retry('max_retries', 'wmart_tv_main1')}")
    print(f"max_page_retries (main1): {config.get_retry('max_page_retries', 'wmart_tv_main1')}")

    print("\n=== Timing ===")
    print(f"homepage_wait range: {config.get_timing_range('homepage_wait')}")
    print(f"browse_wait range: {config.get_timing_range('browse_wait')}")
    print(f"page_load_wait range (main1): {config.get_timing_range('page_load_wait', 'wmart_tv_main1')}")
    print(f"between_pages range (bsr): {config.get_timing_range('between_pages', 'wmart_tv_bsr')}")
    print(f"captcha_wait: {config.get_float('timing', 'captcha_wait')}")

    print("\n=== Browser ===")
    print(f"window_size: {config.get_browser('window_size')}")
    print(f"page_load_timeout: {config.get_browser_int('page_load_timeout')}")
    print(f"webdriver_wait: {config.get_browser_int('webdriver_wait')}")

    print("\n=== Text ===")
    print(f"robot_check texts: {config.get_robot_check_texts()}")
    print(f"captcha keywords: {config.get_captcha_keywords()}")

    print("\n=== Scroll ===")
    print(f"max_rounds: {config.get_int('scroll', 'max_rounds')}")
    print(f"random_amount range: {config.get_scroll_range('random_amount')}")
    print(f"recovery_amount range: {config.get_scroll_range('recovery_amount')}")
