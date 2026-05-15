"""
BestBuy Crawler Config Loader
- bby_tv_config 테이블에서 설정값 로드
- 하드코딩 제거를 위한 중앙 설정 관리
"""
import psycopg2
from config import DB_CONFIG


class BbyConfigLoader:
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
            'css': {},
            'timing': {},
            'retry': {},
            'constant': {},
            'table': {}
        }

        try:
            conn = psycopg2.connect(**DB_CONFIG)
            cursor = conn.cursor()
            cursor.execute("""
                SELECT category, config_key, config_value, file_name, priority
                FROM bby_tv_config
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
            print(f"[CONFIG] Loaded {sum(len(v) for cat in self._configs.values() for v in cat.values())} config entries")

        except Exception as e:
            print(f"[CONFIG WARNING] Failed to load configs; using code defaults: {e}")

    def get(self, category, key, file_name=None, default=None):
        """
        단일 값 반환 (priority=1인 첫 번째 값)

        Args:
            category: url, xpath, css, timing, retry, constant, table
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
            return default

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

    def get_selectors(self, key, file_name):
        """
        XPath + CSS 셀렉터 리스트 반환 (priority 순서대로)

        Returns:
            list of selector strings (xpath and css combined)
        """
        result = []

        # XPath 먼저
        xpath_list = self.get_xpath_list(key, file_name)
        if xpath_list:
            result.extend(xpath_list)

        # CSS 추가
        try:
            file_key = file_name or '_common'
            if file_key in self._configs.get('css', {}):
                if key in self._configs['css'][file_key]:
                    entries = self._configs['css'][file_key][key]
                    sorted_entries = sorted(entries, key=lambda x: x['priority'])
                    result.extend([e['value'] for e in sorted_entries])
        except Exception:
            pass

        return result if result else None

    def get_timing_range(self, key_prefix, file_name):
        """
        타이밍 min/max 쌍 반환 (random.uniform용)

        Args:
            key_prefix: 예) 'page_load_wait' -> page_load_wait_min, page_load_wait_max 찾음
            file_name: 파일명

        Returns:
            tuple (min_value, max_value) as floats
        """
        min_val = self.get_float('timing', f'{key_prefix}_min', file_name)
        max_val = self.get_float('timing', f'{key_prefix}_max', file_name)

        if min_val and max_val:
            return (min_val, max_val)
        return None

    def get_url(self, key, file_name=None):
        """URL 반환 (file_name 지정 가능)"""
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

    def reload(self):
        """설정 다시 로드 (런타임 중 설정 변경 시)"""
        self._load_all()


# 편의를 위한 전역 인스턴스
_loader = None

def get_config():
    """전역 ConfigLoader 인스턴스 반환"""
    global _loader
    if _loader is None:
        _loader = BbyConfigLoader()
    return _loader


# 테스트
if __name__ == '__main__':
    config = get_config()

    print("\n=== URL ===")
    print(f"homepage: {config.get_url('homepage')}")
    print(f"promo_page: {config.get_url('promo_page', 'bby_tv_pmt1')}")

    print("\n=== Table ===")
    print(f"main_data: {config.get_table('main_data')}")
    print(f"detail_data: {config.get_table('detail_data')}")

    print("\n=== Constant ===")
    print(f"account_name: {config.get_constant('account_name')}")
    print(f"max_products_main: {config.get_int('constant', 'max_products_main', 'bby_tv_main1')}")

    print("\n=== Timing ===")
    print(f"page_load_wait range: {config.get_timing_range('page_load_wait', 'bby_tv_main1')}")
    print(f"scroll_wait: {config.get_float('timing', 'scroll_wait', 'bby_tv_main1')}")

    print("\n=== Retry ===")
    print(f"similar_products: {config.get_retry('similar_products', 'bby_tv_dt1')}")

    print("\n=== XPath (fallback list) ===")
    print(f"top_mentions selectors: {config.get_xpath_list('top_mentions', 'bby_tv_dt1')}")
    print(f"product_title selectors: {config.get_selectors('product_title', 'bby_tv_main1')}")
    print(f"tvs_button selectors: {config.get_xpath_list('tvs_button', 'bby_tv_trend_crawl')}")
