"""
Amazon TV Crawler Config Loader
- amazon_tv_config 테이블에서 설정값 로드
- 하드코딩 제거를 위한 중앙 설정 관리
"""
import psycopg2
from config import DB_CONFIG


class AmazonConfigLoader:
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
            'account': {}
        }

        try:
            conn = psycopg2.connect(**DB_CONFIG)
            cursor = conn.cursor()
            cursor.execute("""
                SELECT category, config_key, config_value, file_name, priority
                FROM amazon_tv_config
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
            print(f"[CONFIG] Loaded {sum(len(v) for cat in self._configs.values() for v in cat.values())} amazon config entries")

        except Exception as e:
            print(f"[CONFIG WARNING] Failed to load amazon configs from DB: {e}")
            print("[CONFIG WARNING] Using default values - config changes in DB will not be applied")
            # DB 실패 시에도 크롤러가 기본값으로 동작하도록 빈 설정 유지

    def get(self, category, key, file_name=None, default=None):
        """
        단일 값 반환 (priority=1인 첫 번째 값)

        Args:
            category: url, xpath, timing, retry, constant, table, browser, text, account
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

    def get_xpath_list(self, key, file_name):
        """
        XPath 셀렉터 리스트 반환 (priority 순서대로, fallback용)

        Returns:
            list of xpath strings, sorted by priority
        """
        try:
            result = []
            file_key = file_name or '_common'

            if file_key in self._configs.get('xpath', {}):
                if key in self._configs['xpath'][file_key]:
                    entries = self._configs['xpath'][file_key][key]
                    sorted_entries = sorted(entries, key=lambda x: x['priority'])
                    result = [e['value'] for e in sorted_entries]

            return result if result else None

        except Exception as e:
            print(f"[CONFIG WARNING] get_xpath_list({key}, {file_name}) failed: {e}")
            return None

    def get_xpaths_by_prefix(self, prefix, file_name):
        """
        특정 prefix로 시작하는 모든 XPath 반환 (priority 순서대로)
        예: 'screen_size' -> screen_size_1, screen_size_2, ... 반환

        Returns:
            list of xpath strings, sorted by priority
        """
        try:
            matching_entries = []
            file_key = file_name or '_common'

            # 파일별 설정 확인
            if file_key in self._configs.get('xpath', {}):
                for key, entries in self._configs['xpath'][file_key].items():
                    if key.startswith(prefix):
                        for entry in entries:
                            matching_entries.append({
                                'key': key,
                                'value': entry['value'],
                                'priority': entry['priority']
                            })

            # _common 설정도 확인 (파일별 설정에서 못 찾은 경우)
            if not matching_entries and '_common' in self._configs.get('xpath', {}):
                for key, entries in self._configs['xpath']['_common'].items():
                    if key.startswith(prefix):
                        for entry in entries:
                            matching_entries.append({
                                'key': key,
                                'value': entry['value'],
                                'priority': entry['priority']
                            })

            if matching_entries:
                # priority 순으로 정렬
                sorted_entries = sorted(matching_entries, key=lambda x: x['priority'])
                return [e['value'] for e in sorted_entries]

            return None

        except Exception as e:
            print(f"[CONFIG WARNING] get_xpaths_by_prefix({prefix}, {file_name}) failed: {e}")
            return None

    def get_timing_range(self, key_prefix, file_name):
        """
        타이밍 min/max 쌍 반환 (random.uniform용)

        Args:
            key_prefix: 예) 'throttling_wait' -> throttling_wait_min, throttling_wait_max 찾음
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

    def get_text(self, key, default=None):
        """텍스트 패턴 반환 (공통)"""
        return self.get('text', key, None, default)

    def get_account(self, key, file_name=None, default=None):
        """계정 정보 반환"""
        return self.get('account', key, file_name, default)

    def get_price_status_texts(self):
        """모든 price_status 텍스트 반환"""
        texts = []
        for key in ['price_status_unavailable', 'price_status_higher', 'price_status_no_offers',
                    'price_status_see_cart', 'price_status_add_cart']:
            text = self.get_text(key)
            if text:
                texts.append(text)
        return texts

    def reload(self):
        """설정 다시 로드 (런타임 중 설정 변경 시)"""
        self._load_all()


# 편의를 위한 전역 인스턴스
_loader = None


def get_amazon_config():
    """전역 AmazonConfigLoader 인스턴스 반환"""
    global _loader
    if _loader is None:
        _loader = AmazonConfigLoader()
    return _loader


# 테스트
if __name__ == '__main__':
    config = get_amazon_config()

    print("\n=== URL ===")
    print(f"homepage: {config.get_url('homepage')}")
    print(f"login_page: {config.get_url('login_page')}")

    print("\n=== Table ===")
    print(f"main_data: {config.get_table('main_data')}")
    print(f"detail_data: {config.get_table('detail_data')}")
    print(f"bsr_data: {config.get_table('bsr_data')}")

    print("\n=== Constant ===")
    print(f"max_skus (dt1): {config.get_constant_int('max_skus', 'amazon_tv_dt1')}")
    print(f"max_skus (main1): {config.get_constant_int('max_skus', 'amazon_tv_main1')}")
    print(f"target_reviews: {config.get_constant_int('target_reviews', 'amazon_tv_dt1')}")

    print("\n=== Retry ===")
    print(f"sorry_page_max (dt1): {config.get_retry('sorry_page_max', 'amazon_tv_dt1')}")
    print(f"sorry_page_max (bsr1): {config.get_retry('sorry_page_max', 'amazon_tv_bsr1')}")

    print("\n=== Timing ===")
    print(f"throttling_wait range: {config.get_timing_range('throttling_wait', 'amazon_tv_bsr1')}")
    print(f"page_load_wait: {config.get_float('timing', 'page_load_wait', 'amazon_tv_dt1')}")

    print("\n=== Browser ===")
    print(f"user_agent (dt1): {config.get_browser('user_agent', 'amazon_tv_dt1')}")

    print("\n=== Text ===")
    print(f"price_status_unavailable: {config.get_text('price_status_unavailable')}")
    print(f"all price status texts: {config.get_price_status_texts()}")

    print("\n=== Account ===")
    print(f"primary (dt1): {config.get_account('primary', 'amazon_tv_dt1')}")
    print(f"secondary (dt1): {config.get_account('secondary', 'amazon_tv_dt1')}")

    print("\n=== XPath (fallback list) ===")
    print(f"screen_size xpaths: {config.get_xpaths_by_prefix('screen_size', 'amazon_tv_dt1')}")
    print(f"review_link xpaths: {config.get_xpaths_by_prefix('review_link', 'amazon_tv_dt1')}")
