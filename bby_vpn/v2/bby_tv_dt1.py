"""
Best Buy TV Detail Page Crawler (Modified v1)
collected table: bby_tv_main1, bby_tv_bsr1, bby_tv_pmt1, bby_tv_Trend_crawl
save table: bby_tv_crawl, tv_retail_com

수정사항:
1. estimated_annual_electricity_use: 숫자만 extraction (예: "286 kilowatt hours" -> "286")
2. screen_size 컬럼 추가
3. samsung_sku_name -> item으로 변경, item -> retailer_sku_name으로 변경
4. 소스 table에서 13items 컬럼 추가 collected:
   - 9items data 컬럼: final_sku_price, savings, original_sku_price, offer,
     pick_up_availability, shipping_availability, delivery_availability, sku_status, star_rating
   - 4items rank/type 컬럼: promotion_type, promotion_rank, bsr_rank, main_rank
   - first 번째 found된 URL의 data 우선 (중복 URL은 first 소스 data 사용)
   - 소스 table에 없는 컬럼은 NULL 처리

v1 추가 수정사항 (2025-11-15):
5. page 로딩 불확실성 해결:
   - page_load_strategy를 'none'에서 'eager'로 변경
   - page load 후 핵심 element(제품명) wait 로직 추가
6. dialog timeout 처리 items선:
   - click_specifications_with_retry() 메서드 추가 (retry 1회)
   - timeout 발생 시 2배 증가 (15sec -> 30sec)
   - failed 원인 추적 가능
7. 가격 정보 직접 collected으로 변경 (컨테이너 기반):
   - final_sku_price, original_sku_price, savings를 소스 table에서 가져오지 않고 detail page에서 직접 crawling
   - extract_final_sku_price(), extract_original_sku_price(), extract_savings() 메서드 추가
   - 2단계 extraction: 1) 가격 컨테이너 찾기 (order-2 또는 price-block) 2) 컨테이너 내부에서만 가격 extraction
   - data-testid 기반 XPath 사용 (price-block-customer-price, price-block-total-savings-text 등)
   - 다른 element와의 혼동 방지 (컨테이너 내부만 검색)
   - savings는 "Save $1,200" → "$1,200" 형식으로 정규식 파싱 (콤마 처리 포함)
8. star_rating 및 count_of_reviews 직접 collected으로 변경 (컨테이너 기반):
   - star_rating: 소스 table → 메인 page에서 직접 crawling (예: "4.7")
   - count_of_reviews: review page → 메인 page에서 직접 crawling (예: "(79 reviews)" → "79")
   - extract_star_rating(), extract_count_of_reviews_from_detail() 메서드 추가
   - 동일한 가격 컨테이너 사용 (order-2 또는 price-block)
   - 콤마 처리 포함 (예: "(1,234 reviews)" → "1234")
"""
import time
import random
import re
import os
import sys
import json
import csv
import psycopg2
from datetime import datetime, timedelta
import pytz
from DrissionPage import ChromiumPage, ChromiumOptions
from lxml import html

ROOT_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if ROOT_DIR not in sys.path:
    sys.path.insert(0, ROOT_DIR)

from data_validator import DataValidator
from bby_access_policy import detect_block_signal
from bby_crawl_controls import (
    BrowserSessionDiagnostics,
    ConservativeRateLimiter,
    JsonlAuditLog,
    NetworkDiagnostics,
    RowQualityAuditor,
)
from crawler.discovery.embedded_payload_mapper import EmbeddedPayloadMapper
from crawler.discovery.graphql_mapper import GraphQLMapper
from core.session_pool import cookies_from_drission_page, minimal_headers_from_packet
from diagnostics.endpoint_metrics import EndpointMetrics
from parsers.graphql_review_parser import collect_reviews as collect_graphql_reviews
from parsers.graphql_product_parser import parse_product_facts

# Import database configuration
from config import DB_CONFIG
from bby_config_loader import get_config
from core.db_readonly import connect_readonly


class Tee:
    """stdout을 콘솔과 파일 모두에 출력하는 클래스"""
    def __init__(self, file_path):
        self.file = open(file_path, 'w', encoding='utf-8')
        self.stdout = sys.stdout
        sys.stdout = self

    def write(self, data):
        self.stdout.write(data)
        self.file.write(data)
        self.file.flush()

    def flush(self):
        self.stdout.flush()
        self.file.flush()

    def close(self):
        sys.stdout = self.stdout
        self.file.close()


class BestBuyDetailCrawler:
    def __init__(self):
        self.page = None
        self.db_conn = None
        self.korea_tz = pytz.timezone('Asia/Seoul')
        self.batch_id = datetime.now(self.korea_tz).strftime('%Y%m%d_%H%M%S')
        self.order = 0
        self.total_collected = 0
        self.config = get_config()

        # Config loader 초기화
        self.config = get_config()
        self.file_name = 'bby_tv_dt1'
        self.csv_output_dir = os.environ.get(
            'BBY_OUTPUT_DIR',
            os.path.dirname(os.path.abspath(__file__))
        )
        os.makedirs(self.csv_output_dir, exist_ok=True)
        self.csv_output_path = os.path.join(self.csv_output_dir, 'bby_tv_vpn_test.csv')
        self.checkpoint_path = os.path.join(self.csv_output_dir, 'bby_tv_dt1_checkpoint.json')
        self.audit_log_path = os.path.join(self.csv_output_dir, 'bby_tv_dt1_audit.jsonl')
        self.discovery_dir = os.path.join(self.csv_output_dir, 'crawler', 'discovery')
        self.clear_output_on_start = os.environ.get('BBY_DT_CLEAR_OUTPUT', '0') == '1'
        if self.clear_output_on_start and os.path.exists(self.csv_output_path):
            os.remove(self.csv_output_path)

        self.max_skus = self.config.get_int('constant', 'max_products_detail', self.file_name, 300)
        self.core_only = os.environ.get('BBY_DT_CORE_ONLY', '1') == '1'
        self.browser_min_mode = os.environ.get('BBY_BROWSER_MIN_MODE', '1') == '1'
        self.review_extraction_enabled = os.environ.get('BBY_DT_SKIP_REVIEWS', '1') != '1'
        self.similar_extraction_enabled = os.environ.get('BBY_DT_SKIP_SIMILAR', '1') != '1'
        self.discovery_refresh_every = int(os.environ.get('BBY_DT_DISCOVERY_REFRESH_EVERY', '4'))
        self.proactive_restart_every = int(os.environ.get('BBY_DT_RESTART_EVERY', '8'))
        self.proactive_cooldown_every = int(os.environ.get('BBY_DT_COOLDOWN_EVERY', '8'))
        self.proactive_cooldown_min = int(os.environ.get('BBY_DT_COOLDOWN_MIN', '180'))
        self.proactive_cooldown_max = int(os.environ.get('BBY_DT_COOLDOWN_MAX', '360'))

        self.audit_log = JsonlAuditLog(self.audit_log_path)
        self.audit_log.write("run_init", {
            "batch_id": self.batch_id,
            "output_dir": self.csv_output_dir,
            "csv_output_path": self.csv_output_path,
            "core_only": self.core_only,
            "browser_min_mode": self.browser_min_mode,
            "review_extraction_enabled": self.review_extraction_enabled,
            "similar_extraction_enabled": self.similar_extraction_enabled,
            "discovery_refresh_every": self.discovery_refresh_every,
        })
        self.rate_limiter = ConservativeRateLimiter(self.audit_log)
        self.browser_diagnostics = BrowserSessionDiagnostics(self.audit_log)
        self.network_diagnostics = NetworkDiagnostics(self.audit_log)
        self.row_quality_auditor = RowQualityAuditor(self.audit_log)
        self.graphql_mapper = GraphQLMapper(self.discovery_dir)
        self.embedded_payload_mapper = EmbeddedPayloadMapper()
        self.endpoint_metrics = EndpointMetrics()

        # NULL detailed_review_content 로그 저장용
        self.null_review_logs = []

        # Mismatch tracking for tv_item_mst
        self.screen_size_mismatch_records = []
        self.electricity_use_mismatch_records = []
        self.sku_updated_records = []  # sku renewed in tv_item_mst

        # Data validator 초기화
        session_start_time = os.environ.get('SESSION_START_TIME', datetime.now().strftime('%Y%m%d%H%M'))
        self.validator = DataValidator(session_start_time)

        # 콘솔 로그 저장용
        self.tee = None

    def load_completed_product_urls(self):
        """Return product URLs already written to the detail CSV."""
        completed = set()
        if not os.path.exists(self.csv_output_path) or os.path.getsize(self.csv_output_path) == 0:
            return completed

        try:
            with open(self.csv_output_path, newline='', encoding='utf-8-sig') as csvfile:
                for row in csv.DictReader(csvfile):
                    product_url = row.get('product_url')
                    if product_url:
                        completed.add(product_url)
            print(f"[INFO] Resume mode: {len(completed)} detail URLs already saved")
        except Exception as e:
            print(f"[WARNING] Failed to read completed detail CSV: {e}")
        return completed

    def save_checkpoint(self, reason, url_index, url_data, success_count):
        """Persist the current crawl position so the next run can resume cleanly."""
        try:
            payload = {
                'timestamp': datetime.now(self.korea_tz).isoformat(),
                'reason': reason,
                'url_index': url_index,
                'product_url': url_data.get('product_url') if url_data else None,
                'page_type': url_data.get('page_type') if url_data else None,
                'success_count': success_count,
                'total_collected': self.total_collected,
                'csv_output_path': self.csv_output_path,
            }
            with open(self.checkpoint_path, 'w', encoding='utf-8') as f:
                json.dump(payload, f, ensure_ascii=False, indent=2)
            print(f"[INFO] Checkpoint saved: {self.checkpoint_path}")
        except Exception as e:
            print(f"[WARNING] Failed to save checkpoint: {e}")

    def extract_graphql_sku_id_from_page(self, product_url, page_source=None):
        """Extract Best Buy numeric skuId from PDP URL or rendered text."""
        candidates = [product_url or "", page_source or ""]
        try:
            body_text = self.page.run_js("return document.body ? document.body.innerText : ''") if self.page else ""
            candidates.append(body_text or "")
        except Exception:
            pass

        patterns = (
            r"/sku/(\d+)(?:/|$)",
            r"\bSKU\s*[:#]?\s*(\d{5,})\b",
            r'"skuId"\s*:\s*"(\d+)"',
            r'"skuId"\s*:\s*(\d+)',
        )
        for text in candidates:
            for pattern in patterns:
                match = re.search(pattern, text or "", re.IGNORECASE)
                if match:
                    return match.group(1)
        return None

    def record_graphql_sku_map(self, product_url, sku_id):
        """Persist URL/item -> Best Buy numeric skuId for API-first collectors."""
        if not product_url or not sku_id:
            return
        try:
            os.makedirs(self.discovery_dir, exist_ok=True)
            path = os.path.join(self.discovery_dir, "graphql_sku_map.json")
            try:
                with open(path, encoding="utf-8") as f:
                    sku_map = json.load(f)
            except Exception:
                sku_map = {}
            sku_map[product_url] = {
                "skuId": str(sku_id),
                "source": "pdp_rendered_text",
                "updated_at": datetime.utcnow().isoformat(timespec="seconds") + "Z",
            }
            with open(path, "w", encoding="utf-8") as f:
                json.dump(sku_map, f, ensure_ascii=False, indent=2)
            mirror_path = os.path.join(self.csv_output_dir, "graphql_sku_map.json")
            if os.path.abspath(mirror_path) != os.path.abspath(path):
                with open(mirror_path, "w", encoding="utf-8") as f:
                    json.dump(sku_map, f, ensure_ascii=False, indent=2)
            self.audit_log.write("graphql_sku_map", {"product_url": product_url, "skuId": str(sku_id)})
            print(f"  [INFO] GraphQL skuId mapped: {sku_id}")
        except Exception as e:
            print(f"  [WARNING] GraphQL skuId map save failed: {e}")

    def save_page_diagnostic(self, reason, product_url):
        """Save rendered HTML and browser/network summary for a failed PDP."""
        try:
            safe_reason = re.sub(r'[^A-Za-z0-9_.-]+', '_', reason or 'unknown')[:80]
            stamp = datetime.now(self.korea_tz).strftime('%Y%m%d_%H%M%S')
            diag_path = os.path.join(self.csv_output_dir, f'bby_dt_diag_{stamp}_{safe_reason}.html')
            page_html = self.page.html if self.page else ''
            page_title = self.page.title if self.page else ''
            current_url = self.page.url if self.page else ''
            with open(diag_path, 'w', encoding='utf-8') as f:
                f.write(f"<!-- reason={reason} requested_url={product_url} current_url={current_url} title={page_title} -->\n")
                f.write(page_html or '')
            self.audit_log.write("page_diagnostic", {
                "reason": reason,
                "requested_url": product_url,
                "current_url": current_url,
                "title": page_title,
                "html_path": diag_path,
            })
            print(f"  [DIAG] Page diagnostic saved: {diag_path}")
            return diag_path
        except Exception as e:
            print(f"  [WARNING] Failed to save page diagnostic: {e}")
            return None

    def connect_db(self):
        """DB connection"""
        try:
            self.db_conn = connect_readonly(DB_CONFIG)
            print("[OK] Database connected")
            return True
        except Exception as e:
            print("[INFO] DB unavailable - using CSV/default fallback")
            return False

    def get_item_mst_data(self, item):
        """Get screen_size and estimated_annual_electricity_use from tv_item_mst for given item"""
        try:
            if not self.db_conn or not item:
                return None
            cursor = self.db_conn.cursor()
            table_name = self.config.get_table('item_master') or 'tv_item_mst'
            cursor.execute(f"""
                SELECT screen_size, estimated_annual_electricity_use FROM {table_name} WHERE item = %s
            """, (item,))
            row = cursor.fetchone()
            cursor.close()
            if row:
                return {'screen_size': row[0], 'estimated_annual_electricity_use': row[1]}
            return None
        except Exception as e:
            print(f"  [WARNING] Failed to get item_mst data: {e}")
            return None

    def setup_browser(self):
        """Setup DrissionPage ChromiumPage - 이미지 비활성화로 속도 향상"""
        try:
            print("[INFO] Setting up DrissionPage browser...")
            co = ChromiumOptions()
            co.auto_port()
            co.no_imgs(True)
            self.page = ChromiumPage(co)
            print("[OK] DrissionPage browser setup complete")
            return True
        except Exception as e:
            print(f"[ERROR] Browser setup failed: {e}")
            import traceback
            traceback.print_exc()
            return False

    def _warmup_with_different_page(self):
        """차단 후 세션 워밍업 - TV가 아닌 다른 카테고리 상품 페이지 접속"""
        warmup_urls = [
            'https://www.bestbuy.com/site/apple-macbook-air-13-inch-laptop-m4-chip-16gb-memory-256gb/6604203.p',
            'https://www.bestbuy.com/site/sony-wh-1000xm5-wireless-noise-canceling-over-the-ear-headphones/6505727.p',
            'https://www.bestbuy.com/site/dyson-v15-detect-extra-cordless-vacuum/6539767.p',
            'https://www.bestbuy.com/site/nintendo-switch-oled-model-w-white-joy-con/6470923.p',
            'https://www.bestbuy.com/site/apple-ipad-10th-generation-with-wi-fi-64gb/4901809.p',
            'https://www.bestbuy.com/site/bose-quietcomfort-ultra-headphones/6554461.p',
        ]
        try:
            url = random.choice(warmup_urls)
            print(f"[INFO] Session warmup: {url[:60]}...")
            self.page.get(url)
            time.sleep(random.uniform(5, 10))
            # 페이지 내 스크롤 (사람처럼 행동)
            try:
                self.page.scroll.down(300)
                time.sleep(random.uniform(2, 4))
                self.page.scroll.down(300)
                time.sleep(random.uniform(2, 3))
            except Exception:
                pass
            print("[OK] Session warmup complete")
        except Exception as e:
            print(f"[WARNING] Session warmup failed: {e}")

    def refresh_discovery_page(self, success_count):
        """Insert a real discovery-page refresh between PDP batches."""
        if self.discovery_refresh_every <= 0 or success_count <= 0:
            return
        if success_count % self.discovery_refresh_every != 0:
            return

        discovery_urls = [
            'https://www.bestbuy.com/site/searchpage.jsp?st=tv&nrp=24',
            'https://www.bestbuy.com/site/tvs/all-flat-screen-tvs/abcat0101001.c',
            'https://www.bestbuy.com/site/promo/tv-deals',
        ]
        url = random.choice(discovery_urls)
        try:
            print(f"[INFO] Discovery refresh after {success_count} successful PDPs: {url[:80]}...")
            self.rate_limiter.wait(url, reason='discovery_refresh')
            self.page.get(url)
            time.sleep(random.uniform(4, 8))
            try:
                self.page.scroll.down(random.randint(300, 900))
                time.sleep(random.uniform(1, 3))
            except Exception:
                pass
            self.browser_diagnostics.snapshot(self.page, url, 'discovery_refresh')
            self.network_diagnostics.snapshot(self.page, url, 'discovery_refresh')
            print("[OK] Discovery refresh complete")
        except Exception as e:
            print(f"[WARNING] Discovery refresh failed: {e}")

    def extract_embedded_product_data(self, tree):
        """Extract lightweight product facts from JSON-LD/hydration payloads."""
        try:
            html_text = self.page.html if self.page else ''
            payloads = self.embedded_payload_mapper.extract(tree, html_text=html_text)
            data = self.embedded_payload_mapper.summarize_product_facts(payloads)
            self.audit_log.write("embedded_payload_summary", {
                "payload_counts": {key: len(value) for key, value in payloads.items()},
                "fact_keys": sorted(k for k, v in data.items() if v),
            })
            if data:
                print(f"  [INFO] Embedded data fallback available: {sorted(k for k, v in data.items() if v)}")
            return data
        except Exception as e:
            print(f"  [WARNING] Embedded data extraction failed: {e}")
            return {}

    def close_browser(self):
        """브라우저 안전 종료 + 해당 프로세스만 정리"""
        browser_pid = None
        try:
            if self.page:
                # quit 전에 브라우저 PID 확보
                try:
                    browser_pid = self.page.browser.process.pid
                except Exception:
                    pass
                self.page.quit()
                self.page = None
                print("[INFO] Browser closed")
        except Exception as e:
            print(f"[WARNING] Browser close error: {e}")
            self.page = None
        # quit 실패 시 해당 PID의 프로세스 트리만 강제 종료
        if browser_pid:
            try:
                import subprocess
                subprocess.run(['taskkill', '/f', '/t', '/pid', str(browser_pid)],
                             capture_output=True, timeout=10)
            except Exception:
                pass

    def restart_browser(self):
        """브라우저 종료 후 재시작"""
        print("[INFO] Restarting browser...")
        self.close_browser()
        time.sleep(3)
        return self.setup_browser()

    def proactive_session_refresh(self, success_count):
        """Take a conservative pause and browser refresh before bot checks accumulate."""
        if success_count <= 0:
            return True

        should_cooldown = (
            self.proactive_cooldown_every > 0
            and success_count % self.proactive_cooldown_every == 0
        )
        should_restart = (
            self.proactive_restart_every > 0
            and success_count % self.proactive_restart_every == 0
        )

        if should_cooldown:
            wait_time = random.randint(self.proactive_cooldown_min, self.proactive_cooldown_max)
            print(f"\n[INFO] Proactive cooldown after {success_count} detail items: {wait_time // 60}m {wait_time % 60}s")
            time.sleep(wait_time)

        if should_restart:
            print(f"[INFO] Proactive browser restart after {success_count} detail items")
            if not self.restart_browser():
                return False
            self._warmup_with_different_page()

        return True

    def check_db_connection(self):
        """DB 커넥션 상태 확인. VPN CSV 테스트에서는 DB가 없어도 계속 진행."""
        if not self.db_conn:
            return True
        try:
            cursor = self.db_conn.cursor()
            cursor.execute("SELECT 1")
            cursor.close()
            return True
        except Exception:
            print("[INFO] DB connection unavailable - continuing without DB")
            try:
                self.db_conn = connect_readonly(DB_CONFIG)
                print("[OK] DB reconnected")
                return True
            except Exception as e:
                self.db_conn = None
                return True

    def get_recent_urls(self):
        """최신 batch_id의 product URLs와 추가 data 가져오기"""
        try:
            # Helper function to extract item from URL for deduplication
            def _extract_item(url):
                """Extract item ID from URL for deduplication
                Example: /product/name/JJ8VPZW5KG/sku/123 -> JJ8VPZW5KG
                         /product/name/JJ8VPZW5KG -> JJ8VPZW5KG
                """
                try:
                    parts = url.rstrip('/').split('/')
                    if 'product' in parts:
                        product_index = parts.index('product')
                        if len(parts) > product_index + 2:
                            item = parts[product_index + 2]
                            if item and item.lower() != 'sku':
                                return item
                    return url  # Fallback to URL if item extraction fails
                except:
                    return url

            def _read_csv_rows(filename):
                path = os.path.join(self.csv_output_dir, filename)
                if not os.path.exists(path) or os.path.getsize(path) == 0:
                    return []
                with open(path, newline='', encoding='utf-8-sig') as csvfile:
                    return list(csv.DictReader(csvfile))

            csv_sources = {
                'main': _read_csv_rows('bby_tv_main1_vpn_test.csv'),
                'bsr': _read_csv_rows('bby_tv_bsr1_vpn_test.csv'),
                'promotion': _read_csv_rows('bby_tv_pmt1_vpn_test.csv'),
                'trend': _read_csv_rows('bby_tv_trend_crawl_vpn_test.csv'),
            }
            if any(csv_sources.values()):
                print("[INFO] VPN test mode: loading listing URLs from CSV files")
                url_data_map = {}

                for row in csv_sources['main']:
                    url = row.get('product_url')
                    if not url:
                        continue
                    item_key = _extract_item(url)
                    if item_key not in url_data_map:
                        url_data_map[item_key] = {
                            'page_type': row.get('page_type') or 'main',
                            'product_url': url,
                            'retailer_sku_name': row.get('retailer_sku_name'),
                            'final_sku_price': None,
                            'savings': None,
                            'original_sku_price': None,
                            'offer': row.get('offer'),
                            'pick_up_availability': row.get('pick_up_availability'),
                            'shipping_availability': row.get('shipping_availability'),
                            'delivery_availability': row.get('delivery_availability'),
                            'sku_status': row.get('sku_status'),
                            'star_rating': None,
                            'main_rank': row.get('main_rank'),
                            'bsr_rank': None,
                            'trend_rank': None,
                            'promotion_position': None,
                            'promotion_type': None
                        }

                for row in csv_sources['bsr']:
                    url = row.get('product_url')
                    if not url:
                        continue
                    item_key = _extract_item(url)
                    if item_key in url_data_map:
                        url_data_map[item_key]['bsr_rank'] = row.get('bsr_rank')
                    else:
                        url_data_map[item_key] = {
                            'page_type': row.get('page_type') or 'bsr',
                            'product_url': url,
                            'retailer_sku_name': row.get('retailer_sku_name'),
                            'final_sku_price': None,
                            'savings': None,
                            'original_sku_price': None,
                            'offer': row.get('offer'),
                            'pick_up_availability': row.get('pick_up_availability'),
                            'shipping_availability': row.get('shipping_availability'),
                            'delivery_availability': row.get('delivery_availability'),
                            'sku_status': row.get('sku_status'),
                            'star_rating': None,
                            'main_rank': None,
                            'bsr_rank': row.get('bsr_rank'),
                            'trend_rank': None,
                            'promotion_position': None,
                            'promotion_type': None
                        }

                for row in csv_sources['promotion']:
                    url = row.get('product_url')
                    if not url:
                        continue
                    item_key = _extract_item(url)
                    if item_key in url_data_map:
                        url_data_map[item_key]['promotion_position'] = row.get('promotion_rank')
                        url_data_map[item_key]['promotion_type'] = row.get('promotion_type')
                    else:
                        url_data_map[item_key] = {
                            'page_type': row.get('page_type') or 'promotion',
                            'product_url': url,
                            'retailer_sku_name': row.get('retailer_sku_name'),
                            'final_sku_price': None,
                            'savings': None,
                            'original_sku_price': None,
                            'offer': row.get('offer'),
                            'pick_up_availability': None,
                            'shipping_availability': None,
                            'delivery_availability': None,
                            'sku_status': None,
                            'star_rating': None,
                            'main_rank': None,
                            'bsr_rank': None,
                            'trend_rank': None,
                            'promotion_position': row.get('promotion_rank'),
                            'promotion_type': row.get('promotion_type')
                        }

                for row in csv_sources['trend']:
                    url = row.get('product_url')
                    if not url:
                        continue
                    item_key = _extract_item(url)
                    if item_key in url_data_map:
                        url_data_map[item_key]['trend_rank'] = row.get('rank')
                    else:
                        url_data_map[item_key] = {
                            'page_type': row.get('page_type') or 'Trend',
                            'product_url': url,
                            'retailer_sku_name': row.get('product_name'),
                            'final_sku_price': None,
                            'savings': None,
                            'original_sku_price': None,
                            'offer': None,
                            'pick_up_availability': None,
                            'shipping_availability': None,
                            'delivery_availability': None,
                            'sku_status': None,
                            'star_rating': None,
                            'main_rank': None,
                            'bsr_rank': None,
                            'trend_rank': row.get('rank'),
                            'promotion_position': None,
                            'promotion_type': None
                        }

                all_urls = list(url_data_map.values())
                before_openbox_filter = len(all_urls)
                all_urls = [u for u in all_urls if 'openbox' not in u['product_url'].lower()]
                openbox_filtered = before_openbox_filter - len(all_urls)
                if openbox_filtered > 0:
                    print(f"[INFO] Filtered out {openbox_filtered} Open Box products")
                print(f"[OK] Total unique items from listing CSV files: {len(all_urls)}")
                return all_urls

            cursor = self.db_conn.cursor()
            urls = []

            # Config에서 테이블명 가져오기
            main_table = self.config.get_table('main_data') or 'bby_tv_main1'
            bsr_table = self.config.get_table('bsr_data') or 'bby_tv_bsr1'
            pmt_table = self.config.get_table('pmt_data') or 'bby_tv_pmt1'
            trend_table = self.config.get_table('trend_data') or 'bby_tv_Trend_crawl'

            # bestbuy_tv_main_crawl에서 최신 batch_id 가져오기
            cursor.execute(f"""
                SELECT batch_id
                FROM {main_table}
                WHERE batch_id IS NOT NULL
                ORDER BY batch_id DESC
                LIMIT 1
            """)
            main_batch_result = cursor.fetchone()
            main_batch_id = main_batch_result[0] if main_batch_result else None

            # bby_tv_Trend_crawl에서 최신 batch_id 가져오기
            cursor.execute(f"""
                SELECT batch_id
                FROM {trend_table}
                WHERE batch_id IS NOT NULL
                ORDER BY batch_id DESC
                LIMIT 1
            """)
            trend_batch_result = cursor.fetchone()
            trend_batch_id = trend_batch_result[0] if trend_batch_result else None

            # bby_tv_promotion_crawl에서 최신 batch_id 가져오기
            cursor.execute(f"""
                SELECT batch_id
                FROM {pmt_table}
                WHERE batch_id IS NOT NULL
                ORDER BY batch_id DESC
                LIMIT 1
            """)
            promo_batch_result = cursor.fetchone()
            promo_batch_id = promo_batch_result[0] if promo_batch_result else None

            # bby_tv_bsr_crawl에서 최신 batch_id 가져오기
            cursor.execute(f"""
                SELECT batch_id
                FROM {bsr_table}
                WHERE batch_id IS NOT NULL
                ORDER BY batch_id DESC
                LIMIT 1
            """)
            bsr_batch_result = cursor.fetchone()
            bsr_batch_id = bsr_batch_result[0] if bsr_batch_result else None

            print(f"[INFO] Latest batch_id - Main: {main_batch_id}, BSR: {bsr_batch_id}, Promotion: {promo_batch_id}, Trend: {trend_batch_id}")

            # collected 순서: main → bsr → promotion → trend (우선순위 순서)
            # 각 table의 rank 순서대로 정렬
            # 중복 item은 rank 정보 병합 (crawling은 한 번만)

            # Dictionary to store merged URL data: {item: {page_type, ranks, data...}}
            # Key는 item ID (URL에서 추출) - 같은 item이면 URL이 달라도 중복 처리
            url_data_map = {}

            # 1. bestbuy_tv_main_crawl에서 해당 batch의 URLs와 data 가져오기
            if main_batch_id:
                cursor.execute(f"""
                    SELECT DISTINCT product_url, offer,
                           pick_up_availability, shipping_availability, delivery_availability,
                           sku_status, main_rank, retailer_sku_name
                    FROM {main_table}
                    WHERE batch_id = %s
                    AND product_url IS NOT NULL
                    ORDER BY main_rank
                """, (main_batch_id,))
                main_urls = cursor.fetchall()
                for row in main_urls:
                    url = row[0]
                    item_key = _extract_item(url)  # item ID로 중복 체크
                    if item_key not in url_data_map:
                        url_data_map[item_key] = {
                            'page_type': 'main',
                            'product_url': url,
                            'retailer_sku_name': row[7],
                            'final_sku_price': None,
                            'savings': None,
                            'original_sku_price': None,
                            'offer': row[1],
                            'pick_up_availability': row[2],
                            'shipping_availability': row[3],
                            'delivery_availability': row[4],
                            'sku_status': row[5],
                            'star_rating': None,
                            'main_rank': row[6],
                            'bsr_rank': None,
                            'trend_rank': None,
                            'promotion_position': None,
                            'promotion_type': None
                        }
                print(f"[OK] Main URLs (batch {main_batch_id}): {len(main_urls)} items")

            # 2. bby_tv_bsr_crawl에서 해당 batch의 URLs와 data 가져오기
            if bsr_batch_id:
                cursor.execute(f"""
                    SELECT DISTINCT product_url, offer,
                           pick_up_availability, shipping_availability, delivery_availability,
                           sku_status, bsr_rank, retailer_sku_name
                    FROM {bsr_table}
                    WHERE batch_id = %s
                    AND product_url IS NOT NULL
                    ORDER BY bsr_rank
                """, (bsr_batch_id,))
                bsr_urls = cursor.fetchall()
                for row in bsr_urls:
                    url = row[0]
                    item_key = _extract_item(url)  # item ID로 중복 체크
                    if item_key in url_data_map:
                        # Item already exists - just add bsr_rank
                        url_data_map[item_key]['bsr_rank'] = row[6]
                    else:
                        # New item from bsr
                        url_data_map[item_key] = {
                            'page_type': 'bsr',
                            'product_url': url,
                            'retailer_sku_name': row[7],
                            'final_sku_price': None,
                            'savings': None,
                            'original_sku_price': None,
                            'offer': row[1],
                            'pick_up_availability': row[2],
                            'shipping_availability': row[3],
                            'delivery_availability': row[4],
                            'sku_status': row[5],
                            'star_rating': None,
                            'main_rank': None,
                            'bsr_rank': row[6],
                            'trend_rank': None,
                            'promotion_position': None,
                            'promotion_type': None
                        }
                print(f"[OK] BSR URLs (batch {bsr_batch_id}): {len(bsr_urls)} items")

            # 3. bby_tv_promotion_crawl에서 해당 batch의 URLs와 data 가져오기
            if promo_batch_id:
                cursor.execute(f"""
                    SELECT DISTINCT product_url, offer, promotion_type, promotion_rank, retailer_sku_name
                    FROM {pmt_table}
                    WHERE batch_id = %s
                    AND product_url IS NOT NULL
                    ORDER BY promotion_rank
                """, (promo_batch_id,))
                promo_urls = cursor.fetchall()
                for row in promo_urls:
                    url = row[0]
                    item_key = _extract_item(url)  # item ID로 중복 체크
                    if item_key in url_data_map:
                        # Item already exists - just add promotion_position and promotion_type
                        url_data_map[item_key]['promotion_position'] = row[3]  # promotion_rank -> promotion_position
                        url_data_map[item_key]['promotion_type'] = row[2]
                    else:
                        # New item from promotion
                        url_data_map[item_key] = {
                            'page_type': 'promotion',
                            'product_url': url,
                            'retailer_sku_name': row[4],
                            'final_sku_price': None,
                            'savings': None,
                            'original_sku_price': None,
                            'offer': row[1],
                            'pick_up_availability': None,
                            'shipping_availability': None,
                            'delivery_availability': None,
                            'sku_status': None,
                            'star_rating': None,
                            'main_rank': None,
                            'bsr_rank': None,
                            'trend_rank': None,
                            'promotion_position': row[3],  # promotion_rank -> promotion_position
                            'promotion_type': row[2]
                        }
                print(f"[OK] Promotion URLs (batch {promo_batch_id}): {len(promo_urls)} items")

            # 4. bby_tv_Trend_crawl에서 해당 batch의 URLs와 data 가져오기
            if trend_batch_id:
                cursor.execute(f"""
                    SELECT DISTINCT product_url, rank, product_name
                    FROM {trend_table}
                    WHERE batch_id = %s
                    AND product_url IS NOT NULL
                    ORDER BY rank
                """, (trend_batch_id,))
                trend_urls = cursor.fetchall()
                for row in trend_urls:
                    url = row[0]
                    item_key = _extract_item(url)  # item ID로 중복 체크
                    if item_key in url_data_map:
                        # Item already exists - just add trend_rank
                        url_data_map[item_key]['trend_rank'] = row[1]
                    else:
                        # New item from trend
                        url_data_map[item_key] = {
                            'page_type': 'Trend',
                            'product_url': url,
                            'retailer_sku_name': row[2],  # product_name -> retailer_sku_name
                            'final_sku_price': None,
                            'savings': None,
                            'original_sku_price': None,
                            'offer': None,
                            'pick_up_availability': None,
                            'shipping_availability': None,
                            'delivery_availability': None,
                            'sku_status': None,
                            'star_rating': None,
                            'main_rank': None,
                            'bsr_rank': None,
                            'trend_rank': row[1],
                            'promotion_position': None,
                            'promotion_type': None
                        }
                print(f"[OK] Trend URLs (batch {trend_batch_id}): {len(trend_urls)} items")

            cursor.close()

            # Convert dictionary to list (maintains insertion order: main, bsr, promotion, trend)
            all_urls = list(url_data_map.values())

            # Filter out Open Box products (URL에 "openbox" 포함된 제품 제외)
            before_openbox_filter = len(all_urls)
            all_urls = [u for u in all_urls if 'openbox' not in u['product_url'].lower()]
            openbox_filtered = before_openbox_filter - len(all_urls)
            if openbox_filtered > 0:
                print(f"[INFO] Filtered out {openbox_filtered} Open Box products")

            # Count duplicates from source tables
            total_loaded = 0
            if main_batch_id:
                cursor = self.db_conn.cursor()
                cursor.execute(f"SELECT COUNT(*) FROM {main_table} WHERE batch_id = %s", (main_batch_id,))
                total_loaded += cursor.fetchone()[0]
                cursor.close()
            if bsr_batch_id:
                cursor = self.db_conn.cursor()
                cursor.execute(f"SELECT COUNT(*) FROM {bsr_table} WHERE batch_id = %s", (bsr_batch_id,))
                total_loaded += cursor.fetchone()[0]
                cursor.close()
            if promo_batch_id:
                cursor = self.db_conn.cursor()
                cursor.execute(f"SELECT COUNT(*) FROM {pmt_table} WHERE batch_id = %s", (promo_batch_id,))
                total_loaded += cursor.fetchone()[0]
                cursor.close()
            if trend_batch_id:
                cursor = self.db_conn.cursor()
                cursor.execute(f"SELECT COUNT(*) FROM {trend_table} WHERE batch_id = %s", (trend_batch_id,))
                total_loaded += cursor.fetchone()[0]
                cursor.close()

            duplicates_count = total_loaded - len(all_urls)
            if duplicates_count > 0:
                print(f"[INFO] Found {duplicates_count} duplicate items - rank information merged")

            print(f"[OK] Total unique items from main/bsr/promo/trend: {len(all_urls)}")

            print("[INFO] VPN test mode: skipping DB processed-url filter")
            print(f"[OK] URLs to process: {len(all_urls)}")

            if len(all_urls) == 0:
                print("[ERROR] No URLs found!")

            return all_urls

        except Exception as e:
            print(f"[ERROR] Failed to load URLs: {e}")
            import traceback
            traceback.print_exc()
            return []

    def extract_retailer_sku_name(self, tree):
        """Retailer_SKU_Name extraction"""
        try:
            xpaths = self.config.get_xpath_list('retailer_sku_name', self.file_name) or [
                '//h1[contains(@class, "h4")]',
                '//div[@class="sku-title"]//h1'
            ]
            for xpath in xpaths:
                elem = tree.xpath(xpath)
                if elem:
                    return elem[0].text_content().strip()
            return None
        except Exception as e:
            print(f"  [ERROR] Retailer_SKU_Name extraction failed: {e}")
            return None

    def click_specifications(self):
        """Specification button click (DrissionPage)"""
        try:
            print("  [INFO] Specification button click...")
            # CSS/XPath를 사용한 여러 attempt
            selectors = self.config.get_selectors('specs_button', self.file_name) or [
                'xpath://button[contains(@class, "specs-accordion")]',
                'xpath://button[.//h3[text()="Specifications"]]',
                'css:button.specs-accordion'
            ]

            for selector in selectors:
                try:
                    spec_button = self.page.ele(selector, timeout=3)
                    if spec_button:
                        spec_button.scroll.to_see()
                        time.sleep(2)
                        spec_button.click()
                        print("  [OK] Specification click successful")
                        time.sleep(7)  # dialog 로딩 wait 증가
                        return True
                except:
                    continue

            print("  [WARNING] Specification button not found.")
            return False

        except Exception as e:
            print(f"  [ERROR] Specification click failed: {e}")
            return False

    def click_specifications_with_retry(self):
        """
        Specifications dialog 열기 (retry 포함) - DrissionPage

        Returns:
            (success, error):
                (True, None): successful
                (False, 'dialog_timeout'): timeout
                (False, 'click_failed'): click failed
        """
        max_retries = 0 if self.browser_min_mode else 1
        retry_count = 0
        base_timeout = 10 if self.browser_min_mode else 15

        while retry_count <= max_retries:
            # Specifications button click
            if self.click_specifications():
                try:
                    wait_time = base_timeout * (2 ** retry_count)  # 15s -> 30s
                    # DrissionPage wait for element
                    model_num_elem = self.page.ele('xpath://div[contains(text(), "Model Number")]', timeout=wait_time)
                    if model_num_elem:
                        print(f"  [OK] dialog load complete (wait: {wait_time}sec)")
                        return True, None
                    else:
                        raise Exception("Model Number element not found")

                except Exception as e:
                    if retry_count < max_retries:
                        print(f"  [WARNING] dialog timeout, retry {retry_count + 1}/{max_retries}...")
                        retry_count += 1
                        self.close_specifications_dialog()

                        # Page refresh to recover from stuck state
                        print(f"  [INFO] Refreshing page to recover from timeout...")
                        try:
                            self.page.refresh()
                            time.sleep(3)  # Wait for page reload
                            print(f"  [OK] Page refreshed successfully")
                        except Exception as e2:
                            print(f"  [WARNING] Page refresh failed: {e2}")

                        time.sleep(2)
                        continue
                    else:
                        print(f"  [ERROR] dialog timeout (retry failed)")
                        return False, 'dialog_timeout'
            else:
                print(f"  [WARNING] Specifications button click failed")
                return False, 'click_failed'

        return False, 'dialog_timeout'

    def extract_item_from_url(self, url):
        """Extract item from BestBuy product URL

        Examples:
        https://www.bestbuy.com/product/roku-32-class.../J3PFCJQRY8/sku/6644457 -> J3PFCJQRY8
        https://www.bestbuy.com/product/tcl-75-class.../J36QYTQ595 -> J36QYTQ595

        Pattern: /product/[product-name]/[ITEM_ID]
        """
        try:
            # Split URL by "/"
            parts = url.rstrip('/').split('/')

            # Find "product" in the URL
            if 'product' in parts:
                product_index = parts.index('product')
                # Item ID is 2 positions after "product" (product_index + 2)
                if len(parts) > product_index + 2:
                    item = parts[product_index + 2]
                    if item:
                        print(f"  [✓] Item extracted from URL: {item}")
                        return item

            print(f"  [WARNING] Could not extract item from URL: {url}")
            return None

        except Exception as e:
            print(f"  [ERROR] Item extraction from URL failed: {e}")
            import traceback
            traceback.print_exc()
            return None

    def extract_electricity_use(self, tree):
        """Estimated_Annual_Electricity_Use extraction (숫자만)"""
        try:
            # dialog에서 Estimated Annual Electricity Use 찾기 (여러 패턴 attempt)
            xpaths = self.config.get_xpath_list('electricity_use', self.file_name) or [
                '//div[contains(@class, "dB7j8sHUbncyf79K")]//div[contains(text(), "Estimated Annual Electricity Use")]/following-sibling::div[@class="grow basis-none pl-300"]',
                '//li[.//h4[text()="Power"]]//div[.//div[contains(text(), "Estimated Annual Electricity Use")]]//div[@class="grow basis-none pl-300"]',
                '//div[contains(text(), "Estimated Annual Electricity Use")]/following-sibling::div[@class="grow basis-none pl-300"]',
                '//div[contains(text(), "Estimated Annual Electricity Use")]/..//div[@class="grow basis-none pl-300"]',
                '//div[contains(., "Estimated Annual Electricity Use")]//div[contains(@class, "pl-300")]'
            ]
            for xpath in xpaths:
                elem = tree.xpath(xpath)
                if elem:
                    electricity = elem[0].text_content().strip()
                    if electricity:
                        # 숫자만 extraction (예: "286 kilowatt hours" -> "286")
                        match = re.search(r'(\d+)', electricity)
                        if match:
                            return match.group(1)
                        return electricity  # 숫자를 찾지 못하면 원본 반환
            return None
        except Exception as e:
            print(f"  [ERROR] Estimated_Annual_Electricity_Use extraction failed: {e}")
            return None

    def extract_screen_size(self, tree):
        """Screen Size extraction"""
        try:
            # XPath 패턴
            xpaths = self.config.get_xpath_list('screen_size', self.file_name) or [
                '/html/body/div[5]/div[4]/div[2]/div/div[3]/div[1]/button[2]/div/div/div[2]',
                '//div[contains(text(), "Screen Size Class")]/following-sibling::div[@class="flex font-500 items-center"]',
                '//div[text()="Screen Size Class"]/..//div[contains(@class, "flex font-500")]'
            ]

            for xpath in xpaths:
                elem = tree.xpath(xpath)
                if elem:
                    screen_size_text = elem[0].text_content().strip()
                    # "65 inches" 형태로 반환 (svg 텍스트 제거)
                    # 정규식으로 "숫자 + inches" extraction
                    match = re.search(r'(\d+\s*inches)', screen_size_text)
                    if match:
                        return match.group(1)
                    # 만약 매칭 안되면 원본 반환
                    return screen_size_text
            return None
        except Exception as e:
            print(f"  [ERROR] Screen Size extraction failed: {e}")
            return None

    def extract_model_year(self, tree):
        """Extract model year from specifications - general dialog (DB에서 xpath 로드)"""
        try:
            xpaths = self.config.get_xpath_list('model_year', self.file_name)

            if not xpaths:
                return None

            for xpath in xpaths:
                elem = tree.xpath(xpath)
                if elem:
                    year_text = elem[0].text_content().strip()
                    # Validate it's a 4-digit year
                    if re.match(r'^\d{4}$', year_text):
                        return year_text

            return None
        except Exception as e:
            print(f"  [ERROR] Model Year extraction failed: {e}")
            return None

    def extract_sku(self, tree):
        """Extract SKU (Model Number) from Specifications dialog - General container

        HTML 구조:
        <div class="dB7j8sHUbncyf79K inline-flex w-full body-copy-lg">
          <div class="flex grow basis-none font-weight-medium gap-50 inline-align-middle items-center">Model Number</div>
          <div class="grow basis-none pl-300">32R3B5/32R3BX</div>
        </div>

        Returns:
            str: SKU (Model Number) or "no sku" if not found
        """
        try:
            # XPath 패턴 - Model Number 텍스트가 있는 div의 형제 div에서 값 추출
            xpaths = self.config.get_xpath_list('sku_model_number', self.file_name) or [
                '//div[contains(@class, "dB7j8sHUbncyf79K")][.//div[contains(text(), "Model Number")]]/div[contains(@class, "pl-300")]',
                '//div[contains(text(), "Model Number")]/following-sibling::div[contains(@class, "pl-300")]',
                '//div[contains(text(), "Model Number")]/../div[contains(@class, "pl-300")]',
                '//div[contains(text(), "Model Number")]/following-sibling::div'
            ]

            for xpath in xpaths:
                elem = tree.xpath(xpath)
                if elem:
                    sku = elem[0].text_content().strip()
                    if sku and len(sku) > 0:
                        print(f"  [✓] SKU (Model Number): {sku}")
                        return sku

            labels = tree.xpath('//*[normalize-space()="Model Number"]')
            for label in labels:
                candidates = label.xpath('./following::*[normalize-space()][1]')
                for candidate in candidates:
                    sku = candidate.text_content().strip()
                    if sku and sku != 'Model Number' and re.match(r'^[A-Za-z0-9][A-Za-z0-9._/-]{2,}$', sku):
                        print(f"  [?? SKU (Model Number fallback): {sku}")
                        return sku

            text = re.sub(r'\s+', ' ', tree.text_content())
            match = re.search(r'Model Number\s+([A-Za-z0-9][A-Za-z0-9._/-]{2,})', text)
            if match:
                sku = match.group(1).strip()
                print(f"  [?? SKU (Model Number text fallback): {sku}")
                return sku

            print(f"  [WARNING] SKU (Model Number) not found - using 'no sku'")
            return "no sku"

        except Exception as e:
            print(f"  [ERROR] SKU extraction failed: {e}")
            return "no sku"

    def extract_final_sku_price(self, tree):
        """Final SKU Price extraction (현재 판매 가격) - 컨테이너 기반"""
        try:
            # 0단계: "no longer available" 체크를 먼저 수행 (가격 컨테이너가 없을 수 있음)
            no_longer_available_xpaths = self.config.get_xpath_list('no_longer_available', self.file_name) or [
                '//div[@class="text-danger text-4 font-500 leading-4"]',
                '//div[contains(@class, "text-danger")][contains(text(), "no longer available")]',
                '//div[contains(text(), "This item is no longer available in new condition")]'
            ]

            for xpath in no_longer_available_xpaths:
                elem = tree.xpath(xpath)
                if elem:
                    text = elem[0].text_content().strip()
                    if "no longer available" in text.lower():
                        print(f"  [INFO] Item no longer available in new condition")
                        return "no longer available"

            # 1단계: 가격 컨테이너 찾기 (price-block 우선 - 유사상품 가격 혼입 방지)
            container_xpaths = self.config.get_xpath_list('price_block_container', self.file_name) or [
                '//div[@data-testid="price-block"]',
                '//div[contains(@class, "order-2")]'
            ]

            price_container = None
            for xpath in container_xpaths:
                containers = tree.xpath(xpath)
                if containers:
                    price_container = containers[0]
                    break

            if price_container is None:
                print(f"  [WARNING] price container not found")
                return None

            # 2단계: 컨테이너 내부에서만 가격 extraction
            price_xpaths = self.config.get_xpath_list('final_price_inner', self.file_name) or [
                './/div[@data-testid="price-block-customer-price"]//span',
                './/div[@data-lu-target="customer_price"]//span',
                './/span[@class="font-sans text-default text-style-body-md-400 font-500 text-7 leading-7"]'
            ]

            for xpath in price_xpaths:
                elem = price_container.xpath(xpath)
                if elem:
                    price = elem[0].text_content().strip()
                    # "$" 기호가 포함되어 있고 유효한 가격인지 확인
                    if price and '$' in price:
                        return price  # "$89.99" 형식 반환

            # Fallback 1: "See price in cart" 또는 "See details in checkout" 패턴 찾기
            see_price_xpaths = self.config.get_xpath_list('see_price_in_cart', self.file_name) or [
                './/div[@data-testid="price-restricted-price-tap-for-price"]//span',
                './/span[contains(text(), "See price in cart")]',
                './/span[contains(text(), "See details in checkout")]'
            ]

            for xpath in see_price_xpaths:
                elem = price_container.xpath(xpath)
                if elem:
                    text = elem[0].text_content().strip()
                    if "See price in cart" in text:
                        return "See price in cart"
                    if "See details in checkout" in text:
                        return "See details in checkout"

            return None
        except Exception as e:
            print(f"  [ERROR] Final_SKU_Price extraction failed: {e}")
            return None

    def extract_original_sku_price(self, tree):
        """Original SKU Price extraction (세일 전 원가) - 컨테이너 기반"""
        try:
            # 1단계: final_sku_price와 동일한 컨테이너 사용
            container_xpaths = self.config.get_xpath_list('price_block_container', self.file_name) or [
                '//div[@data-testid="price-block"]',
                '//div[contains(@class, "order-2")]'
            ]

            price_container = None
            for xpath in container_xpaths:
                containers = tree.xpath(xpath)
                if containers:
                    price_container = containers[0]
                    break

            if price_container is None:
                return None

            # 2단계: 컨테이너 내부에서만 원가 extraction
            price_xpaths = self.config.get_xpath_list('original_price_inner', self.file_name) or [
                './/div[@data-testid="price-block-regular-price-message-text"]//span[contains(@style, "line-through")]',
                './/div[@data-testid="price-block-regular-price-message-text"][contains(., "was")]',
            ]

            for xpath in price_xpaths:
                elem = price_container.xpath(xpath)
                if elem:
                    text = elem[0].text_content().strip()
                    match = re.search(r'\$[\d,]+(?:\.\d{2})?', text)
                    if match:
                        return match.group()

            return None  # 세일이 아니면 None
        except Exception as e:
            print(f"  [ERROR] Original_SKU_Price extraction failed: {e}")
            return None

    def extract_savings(self, tree):
        """Savings extraction (할인 금액) - data-testid로 직접 검색"""
        try:
            savings_xpaths = self.config.get_xpath_list('savings_inner', self.file_name) or [
                './/span[@data-testid="price-block-total-savings-text"]',
                './/div[@data-testid="price-block-total-savings"]//span',
            ]

            # savings 요소는 price-block 컨테이너 외부에 있을 수 있으므로 tree에서 직접 검색
            for xpath in savings_xpaths:
                elem = tree.xpath(xpath)
                if elem:
                    text = elem[0].text_content().strip()
                    match = re.search(r'\$[\d,]+(?:\.\d{2})?', text)
                    if match:
                        return match.group()

            return None
        except Exception as e:
            print(f"  [ERROR] Savings extraction failed: {e}")
            return None

    def extract_star_rating(self, tree):
        """Star Rating extraction (평점 점수) - 실제 평점 먼저 찾고, 없으면 Not yet reviewed"""
        try:
            # Priority 1: visually-hidden p tag (가장 정확)
            hidden_xpaths = self.config.get_xpath_list('star_rating_hidden', self.file_name) or [
                '//p[@class="visually-hidden"][contains(text(), "Rating")]',
                '//p[contains(@class, "visually-hidden")][contains(text(), "out of 5 stars")]'
            ]
            for xpath in hidden_xpaths:
                elem = tree.xpath(xpath)
                if elem:
                    text = elem[0].text_content().strip()
                    match = re.search(r'Rating\s+([\d.]+)\s+out of', text)
                    if match:
                        return match.group(1)

            # Priority 2: 컨테이너 기반 extraction (fallback)
            # star_rating은 price-block 밖에 있으므로 order-2 전체 사용
            container_xpaths = self.config.get_xpath_list('star_rating_container', self.file_name) or [
                '//div[contains(@class, "order-2")]',
                '//div[@data-testid="price-block"]'
            ]

            price_container = None
            for xpath in container_xpaths:
                containers = tree.xpath(xpath)
                if containers:
                    price_container = containers[0]
                    break

            if price_container is None:
                return None

            rating_xpaths = self.config.get_xpath_list('star_rating_inner', self.file_name) or [
                './/div/div[4]/a/div/span[1]',
                './/div/div[3]/a/div/span[1]',
                './/span[@class="font-weight-medium  font-weight-bold order-1"]',
                './/span[contains(@class, "font-weight-bold") and contains(@class, "order-1")]',
                './/span[@aria-hidden="true"][contains(@class, "order-1")]',
                './/span[contains(@class, "X1oPXJyKAwAqyfx_")]',
                './/span[contains(@class, "heading-2") and contains(@class, "font-weight-medium")]',
            ]

            for xpath in rating_xpaths:
                elem = price_container.xpath(xpath)
                if elem:
                    rating = elem[0].text_content().strip()

                    if "Not yet reviewed" in rating:
                        return "Not yet reviewed"

                    if rating and re.match(r'^\d+\.\d+$', rating):
                        return rating

            return None
        except Exception as e:
            print(f"  [ERROR] Star_Rating extraction failed: {e}")
            return None

    def extract_count_of_reviews_from_detail(self, tree):
        """Count of Reviews extraction (메인 detail page에서) - DB에서 xpath 로드

        Example: '(79 reviews)' -> '79', 'Not yet reviewed' -> 0
        Note: '(45 reviews from Skyworth USA)' 같은 외부 리뷰는 'EXTERNAL_REVIEWS' 반환

        중요: Sponsored/Related Item 섹션(spotlight-ad)의 리뷰 수가 아닌
              현재 제품의 리뷰 수만 추출해야 함
        """
        try:
            # Step 1: "Not yet reviewed" 우선 감지 (Sponsored 섹션 제외)
            # 리뷰가 없는 제품을 먼저 처리하여 다른 섹션의 값이 잘못 추출되는 것을 방지
            not_reviewed_xpaths = self.config.get_xpath_list('not_yet_reviewed', self.file_name)
            if not_reviewed_xpaths:
                for xpath in not_reviewed_xpaths:
                    elem = tree.xpath(xpath)
                    if elem:
                        text = elem[0].text_content().strip()
                        if "Not yet reviewed" in text:
                            return 0

            # Step 2: visually-hidden에서 리뷰 수 추출 (Sponsored 섹션 제외)
            hidden_xpaths = self.config.get_xpath_list('count_of_reviews_hidden', self.file_name)
            if hidden_xpaths:
                for xpath in hidden_xpaths:
                    elem = tree.xpath(xpath)
                    if elem:
                        text = elem[0].text_content().strip()
                        # 외부 리뷰 감지: "(45 reviews from Skyworth USA)" 패턴
                        if re.search(r'reviews?\s+from\s+', text, re.IGNORECASE):
                            print(f"  [INFO] External reviews detected (hidden): {text}")
                            return 'EXTERNAL_REVIEWS'
                        # "Rating X.X out of 5 stars with N reviews" 패턴
                        match = re.search(r'with\s+([\d,]+)\s+reviews', text)
                        if match:
                            # hidden에서 숫자 추출 성공 → return 전에 visible에서 외부 리뷰 여부 확인
                            visible_xpaths_check = self.config.get_xpath_list('count_of_reviews_visible', self.file_name)
                            if visible_xpaths_check:
                                for vxpath in visible_xpaths_check:
                                    velem = tree.xpath(vxpath)
                                    if velem:
                                        vtext = velem[0].text_content().strip()
                                        if re.search(r'reviews?\s+from\s+', vtext, re.IGNORECASE):
                                            print(f"  [INFO] External reviews detected (visible cross-check): {vtext}")
                                            return 'EXTERNAL_REVIEWS'
                            return match.group(1).replace(',', '')

            # Step 3: visible span에서 리뷰 수 추출 (Sponsored 섹션 제외)
            visible_xpaths = self.config.get_xpath_list('count_of_reviews_visible', self.file_name)
            if visible_xpaths:
                for xpath in visible_xpaths:
                    elem = tree.xpath(xpath)
                    if elem:
                        text = elem[0].text_content().strip()
                        # "Not yet reviewed" 재확인
                        if "Not yet reviewed" in text:
                            return 0
                        # 외부 리뷰 감지
                        if re.search(r'reviews?\s+from\s+', text, re.IGNORECASE):
                            print(f"  [INFO] External reviews detected (visible): {text}")
                            return 'EXTERNAL_REVIEWS'
                        # "(N reviews)" 패턴
                        match = re.search(r'\(([\d,]+)\s*reviews?[^)]*\)', text, re.IGNORECASE)
                        if match:
                            return match.group(1).replace(',', '')

            return None
        except Exception as e:
            print(f"  [ERROR] Count_of_Reviews extraction failed: {e}")
            return None

    def extract_count_of_reviews_from_page_js(self):
        """Extract review count from rendered DOM text/attributes without DB xpaths."""
        try:
            texts = self.page.run_js('''
                var values = [];
                document.querySelectorAll('a, button, span, p, div').forEach(function(el) {
                    var text = (el.textContent || '').trim();
                    var aria = (el.getAttribute('aria-label') || '').trim();
                    var title = (el.getAttribute('title') || '').trim();
                    [text, aria, title].forEach(function(v) {
                        if (v && /review|rating|stars/i.test(v)) values.push(v);
                    });
                });
                return values.slice(0, 300);
            ''') or []
            joined = ' '.join(texts)
            if re.search(r'reviews?\s+from\s+', joined, re.IGNORECASE):
                return 'EXTERNAL_REVIEWS'
            if 'Not yet reviewed' in joined:
                return 0
            for pattern in [
                r'with\s+([\d,]+)\s+reviews',
                r'\(([\d,]+)\s*reviews?\)',
                r'([\d,]+)\s+reviews'
            ]:
                match = re.search(pattern, joined, re.IGNORECASE)
                if match:
                    return match.group(1).replace(',', '')
            compact = re.sub(r'\s+', '', joined)
            match = re.search(r'\d(?:\.\d)?\(?([\d,]{2,})\)?', compact)
            if match:
                return match.group(1).replace(',', '')
            return None
        except Exception:
            return None

    def extract_review_count_from_text(self, text):
        """Extract review count from arbitrary rating/review text."""
        if not text:
            return None
        for pattern in [
            r'with\s+([\d,]+)\s+reviews',
            r'\(([\d,]+)\s*reviews?\)',
            r'([\d,]+)\s+reviews'
        ]:
            match = re.search(pattern, str(text), re.IGNORECASE)
            if match:
                return match.group(1).replace(',', '')
        return None

    def is_valid_review_text(self, text):
        """Return True only for actual customer review body text."""
        if not text:
            return False
        clean = re.sub(r'\s+', ' ', str(text)).strip()
        if len(clean) < 40:
            return False

        lower = clean.lower()
        boilerplate_phrases = [
            'this reviewer received promo considerations',
            'sweepstakes entry for writing a review',
            "we've verified that this content was written",
            'we’ve verified that this content was written',
            'verified that this content was written',
            'rating ',
            'out of 5 stars with',
            'would recommend to a friend',
        ]
        if any(phrase in lower for phrase in boilerplate_phrases):
            return False
        if re.search(r'^rating\s+[\d.]+\s+out of\s+5\s+stars', lower):
            return False
        return True

    def close_specifications_dialog(self):
        """Specification dialog close (DrissionPage)"""
        try:
            print("  [INFO] Specification dialog close...")
            selectors = self.config.get_xpath_list('dialog_close_btn', self.file_name) or [
                'xpath://button[@data-testid="brix-sheet-closeButton"]',
                'xpath://button[@aria-label="Close Sheet"]',
                'xpath://div[@class="relative"]//button'
            ]

            for selector in selectors:
                try:
                    close_button = self.page.ele(selector, timeout=3)
                    if close_button:
                        close_button.click()
                        print("  [OK] dialog close successful")
                        time.sleep(2)
                        return True
                except:
                    continue

            print("  [WARNING] dialog close button not found.")
            return False

        except Exception as e:
            print(f"  [ERROR] dialog close failed: {e}")
            return False

    def extract_similar_products(self, tree):
        """Compare similar products data extraction"""
        try:
            similar_names = []
            pros_list = []
            cons_list = []

            # Retailer_SKU_Name_similar extraction
            name_xpaths = self.config.get_xpath_list('similar_product_names', self.file_name) or [
                '//span[@class="clamp" and starts-with(@id, "compare-title-")]'
            ]
            for xpath in name_xpaths:
                name_elements = tree.xpath(xpath)
                if name_elements:
                    for elem in name_elements[:4]:  # 최대 4items
                        similar_names.append(elem.text_content().strip())
                    break

            # Pros extraction
            pros_xpaths = self.config.get_xpath_list('similar_product_pros', self.file_name) or [
                '//tr[@class="flex"]//td[.//svg[@aria-label="Advantage Icon"]]//span[@class="text-3 min-w-0 flex flex-wrap"]'
            ]
            for xpath in pros_xpaths:
                pros_elements = tree.xpath(xpath)
                if pros_elements:
                    for elem in pros_elements[:4]:  # 최대 4items
                        pros_list.append(elem.text_content().strip())
                    break

            # Cons extraction
            cons_xpaths = self.config.get_xpath_list('similar_product_cons', self.file_name) or [
                '//tr[@class="flex"]//td[.//svg[@aria-label="Disadvantage Icon"]]//span[@class="text-3 min-w-0 flex flex-wrap"]'
            ]
            for xpath in cons_xpaths:
                cons_elements = tree.xpath(xpath)
                if cons_elements:
                    for elem in cons_elements[:4]:  # 최대 4items
                        text = elem.text_content().strip()
                        if text and text != '—':
                            cons_list.append(text)
                        else:
                            cons_list.append(None)
                    break

            # 부족한 경우 None으로 fill
            while len(similar_names) < 4:
                similar_names.append(None)
            while len(pros_list) < 4:
                pros_list.append(None)
            while len(cons_list) < 4:
                cons_list.append(None)

            return similar_names[:4], pros_list[:4], cons_list[:4]

        except Exception as e:
            print(f"  [ERROR] Similar products extraction failed: {e}")
            return [None]*4, [None]*4, [None]*4

    def extract_star_rating_from_reviews_page(self, tree):
        """리뷰 페이지에서 star_rating (평점) 추출 - lxml tree
        예: <div class="overall-rating">4.5</div>
        """
        try:
            xpaths = self.config.get_xpath_list('star_rating_reviews_page', self.file_name) or [
                '//div[@class="overall-rating"]',
                '//*[@id="reviews-accordion"]/section/div[1]/div[1]/div/div/div[1]/div/div[1]',
                '//div[contains(@class, "overall-rating")]',
            ]

            for xpath in xpaths:
                try:
                    elem = tree.xpath(xpath)
                    if elem:
                        text = elem[0].text_content().strip()
                        # "4.5" 형태의 숫자만 추출
                        match = re.search(r'(\d+\.?\d*)', text)
                        if match:
                            rating = match.group(1)
                            print(f"  [OK] Star_Rating from reviews page: {rating}")
                            return rating
                except:
                    continue

            return None

        except Exception as e:
            print(f"  [ERROR] Star_Rating extraction from reviews page failed: {e}")
            return None

    def extract_star_ratings_from_reviews_page(self):
        """Count_of_Star_Ratings extraction (See All Customer Reviews page에서) - DrissionPage
        Returns: integer (total count) or None
        """
        try:
            time.sleep(3)  # page 로딩 wait
            total_count = 0
            # XPath 패턴 (5점부터 1점까지) - config에서 로드
            star_config_keys = ['star_ratings_5', 'star_ratings_4', 'star_ratings_3', 'star_ratings_2', 'star_ratings_1']
            default_xpaths = [
                'xpath://*[@id="reviews-accordion"]/section/div[1]/div[1]/div/div/div[2]/div/fieldset/div[1]/div/label/span[5]',  # 5점
                'xpath://*[@id="reviews-accordion"]/section/div[1]/div[1]/div/div/div[2]/div/fieldset/div[2]/div/label/span[5]',  # 4점
                'xpath://*[@id="reviews-accordion"]/section/div[1]/div[1]/div/div/div[2]/div/fieldset/div[3]/div/label/span[5]',  # 3점
                'xpath://*[@id="reviews-accordion"]/section/div[1]/div[1]/div/div/div[2]/div/fieldset/div[4]/div/label/span[5]',  # 2점
                'xpath://*[@id="reviews-accordion"]/section/div[1]/div[1]/div/div/div[2]/div/fieldset/div[5]/div/label/span[5]'   # 1점
            ]

            # 5점부터 1점까지 순서로 extraction
            for idx, config_key in enumerate(star_config_keys):
                selectors = self.config.get_xpath_list(config_key, self.file_name) or [default_xpaths[idx]]
                for selector in selectors:
                    try:
                        elem = self.page.ele(selector, timeout=2)
                        if elem:
                            count_text = elem.text.strip()
                            count = int(count_text) if count_text.isdigit() else 0
                            total_count += count
                            break
                    except Exception:
                        continue

            return total_count if total_count > 0 else None

        except Exception as e:
            print(f"  [ERROR] Star ratings extraction failed: {e}")
            return None

    def extract_count_of_reviews(self):
        """Count_of_Reviews extraction (See All Customer Reviews page에서) - DrissionPage"""
        try:
            # Selector 패턴
            selectors = self.config.get_xpath_list('count_reviews_page', self.file_name) or [
                'xpath://span[@class="c-reviews order-2"]',
                'xpath://div[contains(@id, "user-generated-content-ugc-stats")]//span[@class="c-reviews order-2"]',
                'xpath://span[contains(@class, "c-reviews")]'
            ]

            for selector in selectors:
                try:
                    elem = self.page.ele(selector, timeout=3)
                    if elem:
                        text = elem.text.strip()
                        # 숫자만 extraction (예: "(84 Reviews)" -> "84")
                        match = re.search(r'\((\d+)\s*Reviews?\)', text)
                        if match:
                            return match.group(1)
                except Exception:
                    continue

            return None

        except Exception as e:
            print(f"  [ERROR] Count of reviews extraction failed: {e}")
            return None

    def extract_top_mentions_from_reviews_page(self, tree):
        """Top_Mentions extraction (See All Customer Reviews page에서) - lxml tree
        Returns: 콤마로 구분된 모든 mentions (예: "Picture Quality, Setup, Size")
        우선순위: pros-container + cons-container (Pros/Cons 모두 수집) → 기존 XPath fallback
        """
        try:
            # 1순위: distillation-card에서 Pros + Cons mentioned 수집 (data-feature-name 속성 사용)
            try:
                mentions = []
                # Pros mentioned 먼저
                pros_elements = tree.xpath('//div[contains(@class, "pros-container")]//button[@data-feature-name]/@data-feature-name')
                for feature_name in pros_elements:
                    if feature_name:
                        mentions.append(feature_name.strip())
                # Cons mentioned 다음
                cons_elements = tree.xpath('//div[contains(@class, "cons-container")]//button[@data-feature-name]/@data-feature-name')
                for feature_name in cons_elements:
                    if feature_name:
                        mentions.append(feature_name.strip())
                if mentions:
                    return ', '.join(mentions)
            except Exception:
                pass

            # 2순위 (fallback): 기존 XPath 패턴
            xpaths = self.config.get_xpath_list('top_mentions', self.file_name) or [
                '/html/body/div[5]/div[8]/div[2]/aside/ul/li/a',
                '//ul[@class="list-unstyled"]/li/a[contains(@class, "v-text-tech-black")]',
                '//ul[@class="list-unstyled"]/li/a',
                '//div[contains(@class, "customer-review-pros-stats")]//span[@class="text-nowrap"]',
                '//div[contains(., "Highly rated by customers for")]//span[@class="text-nowrap"]'
            ]

            mentions = []
            for xpath in xpaths:
                try:
                    elements = tree.xpath(xpath)
                    if elements:
                        for elem in elements:
                            text = elem.text_content().strip()
                            if text:
                                clean_text = re.sub(r'\s*\([\d,]+\)\s*$', '', text)
                                clean_text = clean_text.replace('\xa0', ' ').strip()
                                if clean_text:
                                    mentions.append(clean_text)
                        break
                except Exception:
                    continue

            if mentions:
                return ', '.join(mentions)

            return None

        except Exception as e:
            print(f"  [ERROR] Top mentions extraction failed: {e}")
            return None

    def extract_bestbuy_sku_number(self):
        """BestBuy SKU 번호 추출 (예: 6614066)"""
        try:
            # 방법 1: SKU div에서 추출
            sku_selectors = self.config.get_xpath_list('bestbuy_sku', self.file_name) or [
                'xpath://div[contains(text(), "SKU:")]',
                'xpath://div[@class="pr-150 inline-block"][contains(text(), "SKU")]',
            ]

            for selector in sku_selectors:
                try:
                    elem = self.page.ele(selector, timeout=2)
                    if elem:
                        text = elem.text.strip()
                        # "SKU: 6614066" -> "6614066"
                        match = re.search(r'SKU[:\s]+(\d+)', text)
                        if match:
                            return match.group(1)
                except:
                    continue

            # 방법 2: data-testid 속성에서 추출
            testid_selectors = self.config.get_xpath_list('bestbuy_sku_testid', self.file_name) or [
                'xpath://div[contains(@data-testid, "mbo-entrypoint-")]'
            ]
            for testid_selector in testid_selectors:
                try:
                    elem = self.page.ele(testid_selector, timeout=2)
                    if elem:
                        testid = elem.attr('data-testid')
                        # "mbo-entrypoint-6614066" -> "6614066"
                        match = re.search(r'mbo-entrypoint-(\d+)', testid)
                        if match:
                            return match.group(1)
                except:
                    continue

            # 방법 3: 페이지 소스에서 직접 추출
            page_html = self.page.html
            match = re.search(r'SKU[:\s]+<!--\s*-->(\d+)', page_html)
            if match:
                return match.group(1)

            match = re.search(r'mbo-entrypoint-(\d+)', page_html)
            if match:
                return match.group(1)

            return None

        except Exception as e:
            print(f"  [ERROR] BestBuy SKU number extraction failed: {e}")
            return None

    def extract_product_slug_from_url(self, url):
        """URL에서 제품 slug 추출
        예: https://www.bestbuy.com/product/insignia-40-class.../J2FPJKSFFJ
        -> insignia-40-class-f40-series-led-full-hd-1080p-smart-fire-tv
        """
        try:
            # /product/ 뒤의 slug 추출
            match = re.search(r'/product/([^/]+)/', url)
            if match:
                return match.group(1)
            return None
        except:
            return None

    def navigate_to_reviews_page(self, product_url):
        """리뷰 페이지로 직접 이동 (버튼 클릭 대신) - SKU 검증 포함"""
        try:
            print("  [INFO] Navigating to reviews page directly...")

            # SKU 번호 추출 (상세 페이지에서)
            expected_sku = self.extract_bestbuy_sku_number()
            if not expected_sku:
                print("  [WARNING] Could not extract BestBuy SKU number")
                return False
            print(f"  [OK] BestBuy SKU number (expected): {expected_sku}")

            # 제품 slug 추출
            product_slug = self.extract_product_slug_from_url(product_url)
            if not product_slug:
                print("  [WARNING] Could not extract product slug from URL")
                return False
            print(f"  [OK] Product slug: {product_slug}")

            # 리뷰 URL 생성
            reviews_url = f"https://www.bestbuy.com/site/reviews/{product_slug}/{expected_sku}"
            print(f"  [INFO] Reviews URL: {reviews_url}")

            # 리뷰 페이지 접근
            self.page.get(reviews_url)
            time.sleep(3)

            # SKU 검증: 현재 URL에서 SKU 추출하여 비교
            current_url = self.page.url
            # /site/reviews/slug/123456 또는 리다이렉트된 /product/.../sku/123456 둘 다 대응
            actual_sku_match = re.search(r'(?:/reviews/[^/]+/|/sku/)(\d+)', current_url)
            if actual_sku_match:
                actual_sku = actual_sku_match.group(1)
                if actual_sku != expected_sku:
                    print(f"  [ERROR] SKU mismatch! Expected: {expected_sku}, Actual: {actual_sku}")
                    print(f"  [INFO] Email alert disabled for VPN test")
                    # send_review_url_error_alert(product_url, expected_sku, actual_sku)
                    return False
                else:
                    print(f"  [OK] SKU verified: {actual_sku}")
            else:
                print(f"  [WARNING] Could not extract SKU from review page URL for verification")
                print(f"  [INFO] Current URL: {current_url}")

            # 리다이렉트된 경우: 리뷰 탭 클릭하여 콘텐츠 로딩 트리거
            if '/site/reviews/' not in current_url:
                print(f"  [INFO] Redirected from /site/reviews/ - triggering review tab...")
                # 리뷰 탭/섹션 클릭으로 lazy-load 트리거
                self.page.run_js('''
                    var tabs = document.querySelectorAll('a, button, [role="tab"]');
                    for (var i = 0; i < tabs.length; i++) {
                        var text = tabs[i].textContent.trim().toLowerCase();
                        if ((text.includes('customer review') || text.includes('reviews'))
                            && !text.includes('write') && text.length < 50) {
                            tabs[i].scrollIntoView({behavior: "smooth", block: "center"});
                            tabs[i].click();
                            break;
                        }
                    }
                ''')
                time.sleep(3)
                # 리뷰 콘텐츠 렌더링 대기
                try:
                    self.page.ele('xpath://li[@class="review-item"]//p[@class="pre-white-space"]', timeout=10)
                    print(f"  [OK] Review content loaded after redirect")
                except:
                    print(f"  [WARNING] Review content not loaded after redirect")

            # 페이지 로드 확인
            page_html = self.page.html
            if "reviews" in page_html.lower() or "rating" in page_html.lower():
                print("  [OK] Reviews page loaded successfully")
                return True
            else:
                print("  [WARNING] Reviews page may not have loaded correctly")
                return True  # 일단 진행

        except Exception as e:
            print(f"  [ERROR] Navigate to reviews page failed: {e}")
            return False

    def capture_review_data_via_graphql(self):
        """제품 페이지에서 rating link 클릭 → GraphQL 응답 캡처
        top_mentions, recommendation_intent, summarized_review_content를 GraphQL에서 획득
        """
        captured_data = {
            'reviews': None,
            'pros_cons': None,
            'ai_summary': None,
            'rating_card': None,
        }

        try:
            self.page.listen.start('graphql')

            # rating/review link 클릭. VPN 렌더링에서는 rating link가 안 보이고
            # See all reviews 버튼만 보이는 경우가 있어 둘 다 시도한다.
            click_result = self.page.run_js('''
                function clickReviewCandidate() {
                    var direct = document.querySelector(
                        '.price-ratings a[href*="customerreviews"], '
                        + '.c-stars-reviews a, '
                        + 'a[href*="tabbed-customerreviews"], '
                        + 'a[href*="customerreviews"]'
                    );
                    if (direct) {
                        direct.scrollIntoView({behavior: "smooth", block: "center"});
                        direct.click();
                        return 'clicked: ' + direct.textContent.trim().substring(0, 80);
                    }
                    var nodes = document.querySelectorAll('a, button, [role="button"], [role="link"]');
                    for (var i = 0; i < nodes.length; i++) {
                        var text = (nodes[i].textContent || '').trim().toLowerCase();
                        var aria = (nodes[i].getAttribute('aria-label') || '').trim().toLowerCase();
                        var label = text + ' ' + aria;
                        if (label.includes('review') && !label.includes('write')) {
                            nodes[i].scrollIntoView({behavior: "smooth", block: "center"});
                            nodes[i].click();
                            return 'clicked: ' + (nodes[i].textContent || aria).trim().substring(0, 80);
                        }
                    }
                    return 'not found';
                }
                var result = clickReviewCandidate();
                if (result !== 'not found') return result;
                for (var pct of [0.55, 0.75, 0.9, 1.0]) {
                    window.scrollTo(0, document.body.scrollHeight * pct);
                    result = clickReviewCandidate();
                    if (result !== 'not found') return result;
                }
                return 'not found';
            ''')
            print(f"  [INFO] GraphQL capture - rating link: {click_result}")

            if click_result == 'not found':
                self.page.listen.stop()
                return captured_data

            time.sleep(3)

            target_ops = {
                'CustomerReviewList_Init': 'reviews',
                'Ai_Review_Summary_Init': 'ai_summary',
                'CustomerRatingCard_Init': 'rating_card',
            }

            for _ in range(30):
                packet = self.page.listen.wait(timeout=1)
                if not packet:
                    if captured_data['reviews']:
                        break
                    continue

                try:
                    req_body = None
                    for attr in ['body', 'postData', 'data']:
                        val = getattr(packet.request, attr, None)
                        if val:
                            req_body = val
                            break

                    if req_body:
                        if isinstance(req_body, str):
                            req_data = json.loads(req_body)
                        else:
                            req_data = req_body

                        op_name = None
                        if isinstance(req_data, dict):
                            op_name = req_data.get('operationName')
                        elif isinstance(req_data, list) and req_data:
                            op_name = req_data[0].get('operationName') if isinstance(req_data[0], dict) else None

                        if op_name and op_name in target_ops:
                            key = target_ops[op_name]
                            try:
                                resp_body = packet.response.body
                                if resp_body:
                                    captured_data[key] = resp_body
                                    try:
                                        endpoint_url = getattr(packet.request, 'url', None) or getattr(packet, 'url', None)
                                        status_code = getattr(packet.response, 'status', None) or getattr(packet.response, 'status_code', None)
                                        gql_errors = resp_body.get('errors') if isinstance(resp_body, dict) else None
                                        self.endpoint_metrics.record(endpoint_url, status_code=status_code, graphql_errors=gql_errors)
                                        headers = minimal_headers_from_packet(packet)
                                        cookies = cookies_from_drission_page(self.page)
                                        self.graphql_mapper.record(
                                            op_name,
                                            endpoint_url,
                                            req_data if isinstance(req_data, dict) else {'operations': req_data},
                                            headers,
                                            resp_body,
                                            cookies=cookies,
                                        )
                                    except Exception as map_error:
                                        print(f"  [WARNING] GraphQL map save failed: {map_error}")
                                    print(f"  [OK] GraphQL captured: {op_name}")
                            except:
                                pass
                except:
                    continue

            self.page.listen.stop()
            captured_count = sum(1 for v in captured_data.values() if v is not None)
            self.audit_log.write("endpoint_metrics", self.endpoint_metrics.summary())
            print(f"  [INFO] GraphQL capture done: {captured_count}/{len(target_ops)} operations captured")

        except Exception as e:
            print(f"  [ERROR] GraphQL capture failed: {e}")
            try:
                self.page.listen.stop()
            except:
                pass

        return captured_data

    def parse_graphql_reviews(self, captured_data):
        """GraphQL CustomerReviewList_Init 응답에서 리뷰 본문 파싱 (DOM 실패 시 fallback)"""
        reviews_data = captured_data.get('reviews')
        if not reviews_data:
            return None
        try:
            max_reviews = int(os.environ.get('BBY_MAX_GRAPHQL_REVIEWS', '20'))
            parsed = collect_graphql_reviews(reviews_data, max_reviews=max_reviews)
            if parsed.get('reviews'):
                print(f"  [OK] GraphQL reviews parsed via API parser: {parsed['count']} reviews")
                return parsed['reviews']

            def collect_review_texts(value, texts):
                if isinstance(value, dict):
                    for key in ['reviewText', 'text', 'comment', 'body', 'content', 'description']:
                        text = value.get(key)
                        if isinstance(text, str) and len(text.strip()) > 20:
                            texts.append(text.strip())
                    for child in value.values():
                        collect_review_texts(child, texts)
                elif isinstance(value, list):
                    for child in value:
                        collect_review_texts(child, texts)

            product = reviews_data.get('data', {}).get('productBySkuId', {})
            reviews_list = product.get('reviews', {})

            review_items = []
            if isinstance(reviews_list, dict):
                edges = reviews_list.get('edges', [])
                if edges:
                    review_items = [e.get('node', e) for e in edges]
                else:
                    # topReviews 등 다른 키 시도
                    for key in ['topReviews', 'items', 'nodes']:
                        items = reviews_list.get(key, [])
                        if items:
                            review_items = items
                            break
            elif isinstance(reviews_list, list):
                review_items = reviews_list

            if review_items:
                formatted = []
                for i, review in enumerate(review_items[:20], 1):
                    if isinstance(review, dict):
                        text = (review.get('reviewText') or review.get('text')
                                or review.get('comment') or review.get('body')
                                or review.get('content', '')).strip()
                        if self.is_valid_review_text(text):
                            formatted.append(f"review{i} - {text}")
                if formatted:
                    result = ' ||| '.join(formatted)
                    print(f"  [OK] GraphQL reviews: {len(formatted)} reviews, {len(result)} chars")
                    return result

            texts = []
            collect_review_texts(reviews_data, texts)
            unique_texts = []
            seen = set()
            for text in texts:
                key = re.sub(r'\s+', ' ', text).strip()
                if self.is_valid_review_text(key) and key not in seen:
                    seen.add(key)
                    unique_texts.append(key)
                if len(unique_texts) >= 20:
                    break
            if unique_texts:
                result = ' ||| '.join([f"review{i} - {text}" for i, text in enumerate(unique_texts, 1)])
                print(f"  [OK] GraphQL reviews fallback: {len(unique_texts)} reviews, {len(result)} chars")
                return result
            return None
        except Exception as e:
            print(f"  [ERROR] GraphQL review parsing failed: {e}")
            return None

    def parse_graphql_top_mentions(self, captured_data):
        """GraphQL pros_cons 응답에서 top_mentions 파싱"""
        pros_cons_data = captured_data.get('pros_cons')
        if not pros_cons_data:
            return None
        try:
            product = pros_cons_data.get('data', {}).get('productBySkuId', {})
            review_info = product.get('reviewInfo', {})
            mentions = []
            for key in ['proFeatures', 'pros', 'conFeatures', 'cons']:
                features = review_info.get(key, [])
                if isinstance(features, list):
                    for f in features:
                        name = f.get('name') or f.get('label') or f.get('text') or f.get('feature', '') if isinstance(f, dict) else str(f)
                        if name:
                            mentions.append(name)
            return ', '.join(mentions) if mentions else None
        except:
            return None

    def parse_graphql_recommendation(self, captured_data):
        """GraphQL rating_card 응답에서 recommendation_intent 파싱"""
        rating_data = captured_data.get('rating_card')
        if not rating_data:
            return None
        try:
            product = rating_data.get('data', {}).get('productBySkuId', {})
            review_info = product.get('reviewInfo', {})
            rec = (review_info.get('recommendedPercent')
                   or review_info.get('recommendedPercentage')
                   or review_info.get('recommendPercent'))
            if rec is not None:
                return f"{rec}% would recommend to a friend"
            return None
        except:
            return None

    def parse_graphql_ai_summary(self, captured_data):
        """GraphQL ai_summary 응답에서 summarized_review_content 파싱"""
        ai_data = captured_data.get('ai_summary')
        if not ai_data:
            return None
        try:
            product = ai_data.get('data', {}).get('productBySkuId', {})
            review_info = product.get('reviewInfo', {})
            for key in ['aiSummary', 'summary', 'reviewSummary']:
                val = review_info.get(key)
                if val:
                    if isinstance(val, str) and len(val.strip()) > 5:
                        return val.strip()
                    elif isinstance(val, dict):
                        text = val.get('text') or val.get('content') or val.get('summary') or val.get('body', '')
                        if text:
                            return str(text).strip()
            return None
        except:
            return None

    def parse_graphql_review_count(self, captured_data):
        """Extract review count from captured GraphQL payloads."""
        try:
            for payload in captured_data.values():
                if not payload:
                    continue
                text = json.dumps(payload, ensure_ascii=False)
                count = self.extract_review_count_from_text(text)
                if count is not None:
                    return count

                for key in [
                    'reviewCount', 'totalReviewCount', 'totalReviews',
                    'reviewsCount', 'customerReviewCount'
                ]:
                    match = re.search(rf'"{key}"\s*:\s*"?([\d,]+)"?', text, re.IGNORECASE)
                    if match:
                        return match.group(1).replace(',', '')
            return None
        except Exception:
            return None

    def extract_reviews_from_js_dom(self):
        """리뷰 페이지에서 JS querySelectorAll로 리뷰 20개 수집 (페이지네이션 포함)"""
        reviews = []
        collected = 0
        page_num = 1

        # 리뷰 텍스트를 추출할 셀렉터 목록 (우선순위 순)
        # JS single-quote 문자열로 안전하게 삽입 가능한 셀렉터만 사용
        review_selectors = [
            'p.pre-white-space',
            'li.review-item .ugc-review-body p',
            'li.review-item p',
            '.review-body p',
            '.ugc-review-body p',
        ]

        try:
            # 리뷰 콘텐츠 렌더링 대기 (최대 15초 폴링, 여러 셀렉터 시도)
            active_selector = None
            for wait in range(15):
                for selector in review_selectors:
                    safe_sel = json.dumps(selector)  # "selector" 형태로 escape
                    count = self.page.run_js(f'return document.querySelectorAll({safe_sel}).length')
                    if count and count > 0:
                        active_selector = selector
                        print(f"  [OK] Reviews rendered in DOM ({count} items, waited {wait}s, selector: {selector})")
                        break
                if active_selector:
                    break
                time.sleep(1)

            if not active_selector:
                # DOM 진단: 어떤 리뷰 관련 요소가 있는지 확인
                diag = self.page.run_js('''
                    var result = {};
                    result.url = window.location.href;
                    result.reviewItem = document.querySelectorAll('li.review-item').length;
                    result.reviewClass = document.querySelectorAll('[class*="review"]').length;
                    result.preWhiteSpace = document.querySelectorAll('p.pre-white-space').length;
                    result.ugcBody = document.querySelectorAll('.ugc-review-body').length;
                    result.allP = document.querySelectorAll('p').length;
                    result.reviewsAccordion = document.querySelectorAll('#reviews-accordion').length;
                    result.tabbedReviews = document.querySelectorAll('#tabbed-customerreviews').length;
                    // 긴 텍스트를 가진 <p> 요소 샘플 (리뷰 후보)
                    var longPs = [];
                    document.querySelectorAll('p').forEach(function(el) {
                        var text = el.textContent.trim();
                        if (text.length > 80) {
                            longPs.push({
                                text: text.substring(0, 100),
                                cls: el.className || '',
                                parentCls: el.parentElement ? (el.parentElement.className || '') : '',
                                parentTag: el.parentElement ? el.parentElement.tagName : ''
                            });
                        }
                    });
                    result.longParagraphs = longPs.slice(0, 5);
                    // [class*="review"] 요소 상세
                    var revEls = [];
                    document.querySelectorAll('[class*="review"]').forEach(function(el) {
                        revEls.push({
                            tag: el.tagName,
                            cls: el.className,
                            childCount: el.children.length,
                            text: el.textContent.trim().substring(0, 80)
                        });
                    });
                    result.reviewElements = revEls.slice(0, 5);
                    return result;
                ''')
                print(f"  [WARNING] Reviews not rendered in DOM after 15s wait")
                print(f"  [DIAG] DOM state: {json.dumps(diag, ensure_ascii=False, indent=2)[:2000]}")
                return None

            safe_active = json.dumps(active_selector)
            while collected < 20:
                page_reviews = self.page.run_js(f'''
                    var reviews = [];
                    document.querySelectorAll({safe_active}).forEach(function(el) {{
                        var text = el.textContent.trim();
                        if (text && text.length > 10) reviews.push(text);
                    }});
                    return reviews;
                ''')

                if page_reviews:
                    for text in page_reviews:
                        if collected >= 20:
                            break
                        collected += 1
                        reviews.append(f"review{collected} - {text}")
                        print(f"    [review {collected}/20] {text[:50]}...")

                if collected >= 20:
                    break

                has_next = self.page.run_js('''
                    var nextBtn = document.querySelector(
                        'li.page.next a, a[aria-label="Next"], '
                        + "button[aria-label='Next'], [data-testid*='next']"
                    );
                    if (nextBtn) {
                        nextBtn.scrollIntoView({behavior: "smooth", block: "center"});
                        nextBtn.click();
                        return true;
                    }
                    return false;
                ''')

                if not has_next:
                    break

                page_num += 1
                print(f"  [INFO] Navigating to next page... (Page {page_num})")
                time.sleep(4)

        except Exception as e:
            print(f"  [ERROR] JS DOM review extraction failed: {e}")

        return ' ||| '.join(reviews) if reviews else None

    def click_see_all_reviews(self, product_url=None):
        """See All Customer Reviews - JS 클릭 우선, DrissionPage 셀렉터, 직접 URL 순서 (DrissionPage)"""
        try:
            print("  [INFO] See All Customer Reviews button searching...")
            original_url = self.page.url

            # 1. 리뷰 섹션으로 스크롤 (lazy loading 트리거 - recovery script와 동일)
            self.page.run_js("window.scrollTo(0, document.body.scrollHeight * 0.7)")
            time.sleep(1)

            # 2. JS로 직접 버튼 검색 + 클릭
            clicked = False
            for scroll_pct in [0.7, 0.85, 1.0]:
                if scroll_pct > 0.7:
                    self.page.run_js(f"window.scrollTo(0, document.body.scrollHeight * {scroll_pct})")
                    time.sleep(1.5)

                js_result = self.page.run_js('''
                    var btns = document.querySelectorAll('button, a');
                    for (var i = 0; i < btns.length; i++) {
                        var text = btns[i].textContent.trim().toLowerCase();
                        if (text.includes('see all') && text.includes('review')) {
                            btns[i].scrollIntoView({behavior: "smooth", block: "center"});
                            btns[i].click();
                            return 'clicked: ' + btns[i].textContent.trim().substring(0, 60);
                        }
                    }
                    return 'not found';
                ''')

                if js_result != 'not found':
                    print(f"  [OK] See All Customer Reviews (JS): {js_result}")
                    clicked = True
                    break

            # 3. JS 실패 시 DrissionPage 셀렉터 시도
            if not clicked:
                selectors = self.config.get_xpath_list('see_all_reviews_btn', self.file_name) or [
                    'xpath://button[contains(., "See All Customer Reviews")]',
                    'xpath://button[contains(@class, "Op9coqeII1kYHR9Q")]',
                    'css:button.Op9coqeII1kYHR9Q'
                ]
                for selector in selectors:
                    try:
                        button = self.page.ele(selector, timeout=2)
                        if button:
                            print(f"  [OK] See All Customer Reviews button found (selector)")
                            button.scroll.to_see()
                            time.sleep(0.5)
                            button.click()
                            clicked = True
                            break
                    except:
                        continue

            # 4. 클릭 성공 시 페이지 이동 확인
            if clicked:
                time.sleep(5)
                current_url = self.page.url
                print(f"  [INFO] Reviews page URL: {current_url[:100]}")

                # 페이지가 이동했으면 성공
                if current_url != original_url:
                    return True

                # 페이지 미이동 - 제품 페이지에 머물러 있음 → direct URL fallback
                print(f"  [WARNING] Page did not navigate after button click. Trying direct URL...")
                if product_url and self.navigate_to_reviews_page(product_url):
                    return True
            else:
                # 5. 버튼 못 찾으면 직접 URL 접근 시도 (fallback)
                print("  [WARNING] See All Customer Reviews button not found. Trying direct URL...")
                if product_url and self.navigate_to_reviews_page(product_url):
                    return True

            print("  [ERROR] All review page navigation methods failed.")
            return False

        except Exception as e:
            print(f"  [ERROR] See All Customer Reviews click failed: {e}")
            return False

    def extract_reviews(self):
        """review 20items collected (page네이션 포함) - DrissionPage
        저장 형식: "review1 - 내용 ||| review2 - 내용 ||| ... ||| review20 - 내용"
        """
        try:
            reviews = []
            collected = 0
            page_num = 1

            while collected < 20:
                # page 소스 가져오기
                page_source = self.page.html
                tree = html.fromstring(page_source)

                # review extraction
                review_xpaths = self.config.get_xpath_list('review_items', self.file_name) or [
                    '//li[@class="review-item"]//div[@class="ugc-review-body"]//p[@class="pre-white-space"]'
                ]
                # fallback: 리다이렉트된 페이지에서 중간 구조가 다를 수 있으므로 간소화 xpath 추가
                review_xpaths.append('//li[@class="review-item"]//p[@class="pre-white-space"]')
                review_elements = []
                for xpath in review_xpaths:
                    review_elements = tree.xpath(xpath)
                    if review_elements:
                        break

                for elem in review_elements:
                    if collected >= 20:
                        break
                    review_text = ' '.join(elem.text_content().split())
                    if review_text:
                        collected += 1
                        formatted_review = f"review{collected} - {review_text}"
                        reviews.append(formatted_review)
                        print(f"    [review {collected}/20] {review_text[:50]}...")

                # 20items collected complete하면 closed
                if collected >= 20:
                    break

                # next page button 찾기
                try:
                    next_button = self.page.ele('xpath://li[contains(@class, "page next")]//a', timeout=3)
                    if next_button:
                        print(f"  [INFO] Navigating to next page... (Page {page_num + 1})")
                        next_button.scroll.to_see()
                        time.sleep(2)
                        next_button.click()
                        time.sleep(4)
                        page_num += 1
                    else:
                        print("  [INFO] next page button not found. collected closed.")
                        break
                except:
                    print("  [INFO] next page button not found. collected closed.")
                    break

            # review를 구분자로 connection (예: "review1 - 내용 ||| review2 - 내용 ||| ...")
            return ' ||| '.join(reviews) if reviews else None

        except Exception as e:
            print(f"  [ERROR] review collected failed: {e}")
            return None

    def extract_summarized_review_content(self, tree):
        """Summarized_Review_Content extraction (AI 요약 리뷰) - 상세 페이지에서
        예: "Customers frequently mention the superb picture quality..."
        """
        try:
            xpaths = self.config.get_xpath_list('summarized_review', self.file_name) or [
                # review-summary 컨테이너 기반 (우선순위 높음)
                '//div[contains(@class, "review-summary")]//p[contains(@class, "body-copy-lg")]',
                # "Customers are saying" 헤더 기준
                '//h4[contains(text(), "Customers are saying")]/following-sibling::p',
                # 클래스 기반
                '//p[@class="mt-200 body-copy-lg mb-none"]',
                # 부분 클래스 매칭
                '//p[contains(@class, "body-copy-lg") and contains(@class, "mb-none")]',
                # 제공된 절대 경로 (fallback)
                '/html/body/div[5]/div[8]/div[2]/div/div[1]/div/p',
            ]

            for xpath in xpaths:
                try:
                    elem = tree.xpath(xpath)
                    if elem:
                        text = elem[0].text_content().strip()
                        if text:
                            print(f"  [OK] Summarized_Review_Content found: {text[:60]}...")
                            return text
                except:
                    continue

            return None

        except Exception as e:
            print(f"  [ERROR] Summarized_Review_Content extraction failed: {e}")
            return None

    def extract_summarized_review_content_from_reviews_page(self, tree):
        """Summarized_Review_Content extraction (리뷰 페이지에서) - lxml tree
        상세 페이지에서 추출 못했을 때 fallback으로 사용
        """
        try:
            # 리뷰 페이지의 AI 요약 위치
            xpaths = self.config.get_xpath_list('summarized_review_reviews_page', self.file_name) or [
                '//*[@id="reviews-accordion"]/div[1]/div/p[1]',
                '//p[@class="mb-200 mt-none"]',
                '//div[@id="reviews-accordion"]//p[contains(@class, "mb-200")]',
            ]

            for xpath in xpaths:
                try:
                    elem = tree.xpath(xpath)
                    if elem:
                        text = elem[0].text_content().strip()
                        if text:
                            return text
                except:
                    continue

            return None

        except Exception as e:
            print(f"  [ERROR] Summarized_Review_Content (reviews page) extraction failed: {e}")
            return None

    def extract_recommendation_intent_from_reviews_page(self, tree):
        """Recommendation_Intent extraction (See All Customer Reviews page에서) - lxml tree"""
        try:
            # XPath 패턴
            xpaths = [
                # 제공된 HTML 기준
                '//div[contains(@class, "recommendation-card-no-donut")]//span[@class="recommendation-percent v-fw-medium"]',
                # 더 넓은 패턴
                '//span[contains(@class, "recommendation-percent")]'
            ]

            percent = None
            for xpath in xpaths:
                try:
                    elem = tree.xpath(xpath)
                    if elem:
                        percent = elem[0].text_content().strip()
                        if percent:
                            break
                except Exception:
                    continue

            if percent:
                # "100% would recommend to a friend" 형식으로 반환
                return f"{percent} would recommend to a friend"

            return None

        except Exception as e:
            print(f"  [ERROR] Recommendation intent extraction failed: {e}")
            return None

    def extract_compare_similar_products(self, current_url):
        """Compare similar products section data extraction (first page 로딩 items선) - DrissionPage"""
        if self.core_only or not self.similar_extraction_enabled:
            print("  [INFO] Similar Products skipped (core-only/default lightweight mode)")
            return None

        max_retries = 2

        for retry in range(max_retries):
            try:
                if retry > 0:
                    print(f"  [RETRY {retry}/{max_retries}] Compare similar products retry...")
                else:
                    print("  [INFO] Compare similar products section searching...")

                # page 상단으로 이동 후 30%까지 스크롤
                self.page.run_js("window.scrollTo(0, 0)")
                time.sleep(1)

                total_height = self.page.run_js("return document.body.scrollHeight")
                scroll_to = int(total_height * 0.3)
                self.page.run_js(f"window.scrollTo(0, {scroll_to})")

                # first page 여부 확인
                is_first_page = (self.order == 1)

                # timeout 설정: first page는 30sec, 나머지는 15sec
                timeout = 30 if is_first_page else 15

                if is_first_page:
                    print(f"  [INFO] first page detected - applying long wait time (max {timeout}sec)")

                # DrissionPage로 product-title element가 load될 때까지 명시적 wait
                try:
                    product_title_elem = self.page.ele(
                        'xpath://div[@class="product-title font-weight-normal pb-100 body-copy-lg min-h-600"]',
                        timeout=timeout
                    )
                    if product_title_elem:
                        print(f"  [OK] Compare similar products element load complete")
                        # element가 load된 후 안정화를 위한 추가 wait
                        additional_wait = 5 if is_first_page else 3
                        time.sleep(additional_wait)
                    else:
                        raise Exception("product-title element not found")

                except Exception as wait_error:
                    print(f"  [WARNING] element wait time exceeded: {wait_error}")
                    if retry < max_retries - 1:
                        # next retry를 위해 page refresh
                        print("  [INFO] page refresh and retry...")
                        self.page.refresh()
                        time.sleep(10)
                        continue
                    else:
                        # 마지막 attempt였다면 None 반환
                        return None

                # page 소스 가져오기
                page_source = self.page.html
                tree = html.fromstring(page_source)

                # 4items 제품 data save
                products = []

                # product-title div들 찾기
                product_divs = tree.xpath('//div[@class="product-title font-weight-normal pb-100 body-copy-lg min-h-600"]')

                if len(product_divs) < 4:
                    print(f"  [WARNING] insufficient products. (found items count: {len(product_divs)})")
                    if retry < max_retries - 1:
                        # retry
                        time.sleep(5)
                        continue
                    else:
                        return None

                # first 번째 제품 (현재 page)
                first_product = {
                    'product_url': current_url,
                    'product_name': None,
                    'pros': None,
                    'cons': None
                }

                # first 번째 제품명 extraction
                span_elem = product_divs[0].xpath('.//span[@class="clamp"]')
                if span_elem:
                    first_product['product_name'] = span_elem[0].text_content().strip()

                products.append(first_product)

                # 2-4번째 제품
                for i in range(1, 4):
                    if i < len(product_divs):
                        product = {
                            'product_url': None,
                            'product_name': None,
                            'pros': None,
                            'cons': None
                        }

                        # a 태그에서 URL과 제품명 extraction
                        a_elem = product_divs[i].xpath('.//a[@class="clamp"]')
                        if a_elem:
                            href = a_elem[0].get('href')
                            if href:
                                product['product_url'] = href
                            product['product_name'] = a_elem[0].text_content().strip()

                        products.append(product)

                # Pros extraction (tr[2]/td[1~4])
                for i in range(1, 5):
                    pros_xpath = f'/html/body/div[5]/div[6]/div/table/tbody/tr[2]/td[{i}]/span/span'
                    pros_elem = tree.xpath(pros_xpath)
                    if pros_elem and i-1 < len(products):
                        products[i-1]['pros'] = pros_elem[0].text_content().strip()

                # Cons extraction (tr[4]/td[1~4])
                for i in range(1, 5):
                    cons_xpath = f'/html/body/div[5]/div[6]/div/table/tbody/tr[4]/td[{i}]/span/span'
                    cons_elem = tree.xpath(cons_xpath)
                    if cons_elem and i-1 < len(products):
                        text = cons_elem[0].text_content().strip()
                        # '—' 같은 값은 None으로 처리
                        products[i-1]['cons'] = text if text and text != '—' else None

                print(f"  [OK] Compare similar products data extraction complete (4items)")
                return products

            except Exception as e:
                print(f"  [ERROR] Compare similar products extraction failed (attempt {retry + 1}/{max_retries}): {e}")
                if retry < max_retries - 1:
                    print("  [INFO] Retrying...")
                    time.sleep(5)
                    continue
                else:
                    import traceback
                    traceback.print_exc()
                    return None

        return None

    def get_item_by_product_name(self, product_name):
        """bby_tv_crawl에서 product_name으로 item 찾기"""
        try:
            if not product_name:
                return None

            cursor = self.db_conn.cursor()
            detail_table = self.config.get_table('detail_data') or 'bby_tv_crawl'

            # 가장 최근 data에서 retailer_sku_name과 product_name이 일치하는 것 찾기
            cursor.execute(f"""
                SELECT item
                FROM {detail_table}
                WHERE retailer_sku_name = %s
                AND item IS NOT NULL
                ORDER BY crawl_datetime DESC
                LIMIT 1
            """, (product_name,))

            result = cursor.fetchone()
            cursor.close()

            if result:
                return result[0]
            return None

        except Exception as e:
            print(f"  [ERROR] Item lookup failed ({product_name}): {e}")
            return None

    def scrape_detail_page(self, url_data):
        """detail page crawling (items선된 로딩 + dialog 처리) - DrissionPage"""
        try:
            self.order += 1
            page_type = url_data['page_type']
            product_url = url_data['product_url']

            print(f"\n{'='*80}")
            print(f"[{self.order}] [{page_type.upper()}] Accessing: {product_url[:80]}...")
            print(f"[INFO] Page type: {page_type} | Main rank: {url_data.get('main_rank', 'N/A')} | BSR rank: {url_data.get('bsr_rank', 'N/A')} | Trend rank: {url_data.get('trend_rank', 'N/A')}")

            # page 접속 (DrissionPage)
            print(f"  [INFO] Loading page...")
            self.rate_limiter.wait(product_url, reason='detail_page')
            self.page.get(product_url)
            self.browser_diagnostics.snapshot(self.page, product_url, 'detail_after_get')
            self.network_diagnostics.snapshot(self.page, product_url, 'detail_after_get')

            # ERR_HTTP2_PROTOCOL_ERROR 감지
            try:
                page_title = self.page.title or ''
                page_text = self.page.html[:2000] if self.page.html else ''
                block_reason = detect_block_signal(page_title, page_text)
                if block_reason:
                    print(f"  [BLOCKED] Block signal detected: {block_reason}")
                    self.rate_limiter.register_outcome(product_url, 'blocked')
                    return 'blocked'
            except Exception:
                pass

            # 핵심 element load wait (최대 20sec) - DrissionPage
            try:
                h1_elem = self.page.ele('xpath://h1[contains(@class, "h4") or contains(@class, "heading")]', timeout=20)
                if h1_elem:
                    print(f"  [OK] page load complete")
                else:
                    print(f"  [ERROR] page loading timeout - h1 element not found")
                    try:
                        block_reason = detect_block_signal(self.page.title or '', self.page.html[:5000] if self.page.html else '')
                        if block_reason:
                            print(f"  [BLOCKED] Block signal detected after timeout: {block_reason}")
                            self.rate_limiter.register_outcome(product_url, 'blocked')
                            self.save_page_diagnostic(f'blocked_{block_reason}', product_url)
                            return 'blocked'
                    except Exception:
                        pass
                    self.save_page_diagnostic('h1_timeout', product_url)
                    self.rate_limiter.register_outcome(product_url, 'failed')
                    return 'manual_pause'
            except Exception as e:
                print(f"  [ERROR] page loading timeout: {e}")
                try:
                    block_reason = detect_block_signal(self.page.title or '', self.page.html[:5000] if self.page.html else '')
                    if block_reason:
                        print(f"  [BLOCKED] Block signal detected after load exception: {block_reason}")
                        self.rate_limiter.register_outcome(product_url, 'blocked')
                        self.save_page_diagnostic(f'blocked_{block_reason}', product_url)
                        return 'blocked'
                except Exception:
                    pass
                self.save_page_diagnostic('h1_exception', product_url)
                self.rate_limiter.register_outcome(product_url, 'failed')
                return 'manual_pause'

            # DOM 우선 탐색 + 조건부 스크롤 (최적화)
            # 주요 요소들이 DOM에 이미 있으면 스크롤 생략
            print(f"  [INFO] Checking DOM for key elements...")
            page_source = self.page.html
            tree = html.fromstring(page_source)
            graphql_sku_id = self.extract_graphql_sku_id_from_page(product_url, page_source)
            self.record_graphql_sku_map(product_url, graphql_sku_id)
            embedded_data = self.extract_embedded_product_data(tree)

            # 핵심 요소 존재 여부 확인
            key_elements = {
                'price': tree.xpath('//div[@data-testid="price-block-customer-price"] | //span[contains(text(), "See price in cart")]'),
                'rating': tree.xpath('//p[contains(@class, "visually-hidden")][contains(text(), "Rating")]'),
                'title': tree.xpath('//h1[contains(@class, "heading")]//span')
            }

            elements_found = sum(1 for v in key_elements.values() if v)
            print(f"  [INFO] DOM check: {elements_found}/3 key elements found")

            # 핵심 요소가 2개 이상 있으면 스크롤 생략
            if elements_found >= 2:
                print(f"  [OK] Key elements found in DOM - skipping full scroll")
            elif self.browser_min_mode:
                print(f"  [INFO] Browser minimization mode - skipping full scroll")
            else:
                # 요소가 부족하면 스크롤 수행 (최적화된 버전)
                print(f"  [INFO] Starting optimized scroll for lazy loading...")
                scroll_positions = [1000, 3000, 5000, 8000]  # 주요 위치만 스크롤

                for pos in scroll_positions:
                    self.page.run_js(f"window.scrollTo(0, {pos})")
                    time.sleep(0.3)

                # Scroll back to top
                self.page.run_js("window.scrollTo(0, 0)")
                time.sleep(0.5)
                print(f"  [OK] Optimized scroll complete")

            # page 소스 가져오기
            page_source = self.page.html
            tree = html.fromstring(page_source)

            # 1. Retailer_SKU_Name - 소스 테이블에서 가져온 값 사용, 없으면 detail에서 추출
            retailer_sku_name = url_data.get('retailer_sku_name')
            if not retailer_sku_name and embedded_data.get('retailer_sku_name'):
                retailer_sku_name = embedded_data.get('retailer_sku_name')
            if not retailer_sku_name or len(retailer_sku_name) < 3:
                print(f"  [INFO] retailer_sku_name 없음 - detail 페이지에서 추출 시도")
                retailer_sku_name = self.extract_retailer_sku_name(tree)
            print(f"  [✓] Retailer_SKU_Name: {retailer_sku_name}")

            # 2. Screen Size extraction (메인 page에서)
            extracted_screen_size = self.extract_screen_size(tree)
            print(f"  [✓] Screen Size (extracted): {extracted_screen_size}")

            # 2-0. Model Year - dialog에서 추출 (아래에서 처리)
            model_year = None

            # 2-1. Price 정보 extraction (메인 page에서 직접 collected)
            # 가격 컨테이너 로딩 대기 (최대 10초) - DrissionPage
            try:
                price_elem = self.page.ele(
                    'xpath://div[@data-testid="price-block-customer-price"] | //div[@data-testid="price-restricted-price-tap-for-price"] | //div[contains(text(), "no longer available in new condition")]',
                    timeout=10
                )
                if price_elem:
                    print(f"  [OK] price container load complete")
                # 가격 컨테이너 로딩 후 tree 다시 가져오기
                page_source = self.page.html
                tree = html.fromstring(page_source)
            except Exception:
                print(f"  [WARNING] price container loading timeout - attempting extraction anyway")

            final_sku_price = self.extract_final_sku_price(tree)
            if final_sku_price is None and embedded_data.get('final_sku_price'):
                final_sku_price = embedded_data.get('final_sku_price')
                print(f"  [INFO] Final_SKU_Price from embedded data")
            print(f"  [✓] Final_SKU_Price: {final_sku_price}")

            original_sku_price = self.extract_original_sku_price(tree)
            print(f"  [✓] Original_SKU_Price: {original_sku_price}")

            savings = self.extract_savings(tree)
            print(f"  [✓] Savings: {savings}")

            # 2-2. Star Rating 및 Reviews 정보 extraction (메인 page에서 직접 collected)
            star_rating = self.extract_star_rating(tree)
            if star_rating is None and embedded_data.get('star_rating'):
                star_rating = embedded_data.get('star_rating')
                print(f"  [INFO] Star_Rating from embedded data")
            print(f"  [✓] Star_Rating: {star_rating}")

            count_of_reviews = self.extract_count_of_reviews_from_detail(tree)
            if count_of_reviews is None:
                count_of_reviews = self.extract_count_of_reviews_from_page_js()
            if count_of_reviews is None and embedded_data.get('count_of_reviews') is not None:
                count_of_reviews = embedded_data.get('count_of_reviews')
                print(f"  [INFO] Count_of_Reviews from embedded data")

            # 외부 리뷰 감지 시 (예: "reviews from Skyworth USA") 0으로 처리, star_rating도 변경
            is_external_reviews = (count_of_reviews == 'EXTERNAL_REVIEWS')
            if is_external_reviews:
                count_of_reviews = 0
                star_rating = "Not yet reviewed"  # 외부 리뷰는 BestBuy 리뷰가 아니므로
                print(f"  [✓] Count_of_Reviews: 0 (외부 리뷰 - BestBuy 자체 리뷰 아님)")
                print(f"  [✓] Star_Rating: Not yet reviewed (외부 리뷰)")
            else:
                print(f"  [✓] Count_of_Reviews: {count_of_reviews}")

            # 2-3. Summarized Review Content - 리뷰 페이지에서만 수집 (아래에서 처리)
            summarized_review_content = None

            # 3. Compare similar products extraction (실패 시 새로고침 후 재시도)
            similar_products = self.extract_compare_similar_products(product_url)
            if similar_products:
                print(f"  [✓] Similar Products: {len(similar_products)} items found")
            else:
                print(f"  [INFO] Similar Products 추출 실패, 페이지 새로고침 후 재시도...")
                if self.core_only or not self.similar_extraction_enabled:
                    print("  [INFO] Similar Products retry skipped")
                else:
                    self.page.refresh()
                if not (self.core_only or not self.similar_extraction_enabled):
                    time.sleep(5)
                    similar_products = self.extract_compare_similar_products(product_url)
                if similar_products:
                    print(f"  [✓] Similar Products (재시도 성공): {len(similar_products)} items found")
                else:
                    print(f"  [✓] Similar Products: None (섹션 없음 또는 로드 실패)")

            # 4. Item extraction from URL (simplified - no dialog needed)
            item = self.extract_item_from_url(product_url)

            # Electricity use and SKU - need to open dialog for these fields
            extracted_electricity_use = None
            sku = "no sku"
            success, error = self.click_specifications_with_retry()

            if success:
                time.sleep(3)
                # dialog 소스 가져오기 (DrissionPage)
                dialog_source = self.page.html
                dialog_tree = html.fromstring(dialog_source)

                # Extract Estimated_Annual_Electricity_Use (숫자만)
                extracted_electricity_use = self.extract_electricity_use(dialog_tree)
                print(f"  [✓] Estimated_Annual_Electricity_Use (extracted): {extracted_electricity_use}")

                # Extract SKU (Model Number) from dialog
                sku = self.extract_sku(dialog_tree)

                # Extract Model Year from dialog
                model_year = self.extract_model_year(dialog_tree)
                print(f"  [✓] Model Year: {model_year}")

                # dialog close
                self.close_specifications_dialog()
            else:
                print(f"  [WARNING] Could not extract electricity_use/SKU (dialog failed): {error}")

            # tv_item_mst fallback for screen_size and electricity_use
            item_mst_data = self.get_item_mst_data(item)
            mst_screen_size = item_mst_data.get('screen_size') if item_mst_data else None
            mst_electricity_use = item_mst_data.get('estimated_annual_electricity_use') if item_mst_data else None

            # Determine final screen_size with fallback and mismatch tracking
            if extracted_screen_size and mst_screen_size:
                if extracted_screen_size != mst_screen_size:
                    print(f"  [WARNING] screen_size mismatch: extracted='{extracted_screen_size}', tv_item_mst='{mst_screen_size}'")
                    self.screen_size_mismatch_records.append({
                        'item': item,
                        'url': product_url,
                        'extracted': extracted_screen_size,
                        'mst_value': mst_screen_size
                    })
                screen_size = extracted_screen_size
            elif extracted_screen_size:
                screen_size = extracted_screen_size
            elif mst_screen_size:
                screen_size = mst_screen_size
                print(f"  [INFO] Using screen_size from tv_item_mst: {mst_screen_size}")
            else:
                screen_size = None

            # Determine final electricity_use with fallback and mismatch tracking
            if extracted_electricity_use and mst_electricity_use:
                if extracted_electricity_use != mst_electricity_use:
                    print(f"  [WARNING] electricity_use mismatch: extracted='{extracted_electricity_use}', tv_item_mst='{mst_electricity_use}'")
                    self.electricity_use_mismatch_records.append({
                        'item': item,
                        'url': product_url,
                        'extracted': extracted_electricity_use,
                        'mst_value': mst_electricity_use
                    })
                electricity_use = extracted_electricity_use
            elif extracted_electricity_use:
                electricity_use = extracted_electricity_use
            elif mst_electricity_use:
                electricity_use = mst_electricity_use
                print(f"  [INFO] Using electricity_use from tv_item_mst: {mst_electricity_use}")
            else:
                electricity_use = None

            # 8. See All Customer Reviews click 및 data collected
            # count_of_star_ratings는 count_of_reviews와 동일 값 사용
            count_of_star_ratings = count_of_reviews
            print(f"  [✓] count_of_star_ratings: {count_of_star_ratings} (= count_of_reviews)")

            top_mentions = None
            detailed_reviews = None
            recommendation_intent = None
            try:
                review_count_for_decision = int(str(count_of_reviews).replace(',', '')) if count_of_reviews is not None else 0
            except Exception:
                review_count_for_decision = 0
            should_collect_reviews = (
                not self.core_only
                and self.review_extraction_enabled
                and not is_external_reviews
                and review_count_for_decision > 0
                and "not yet reviewed" not in str(star_rating or '').lower()
            )

            # 외부 리뷰인 경우 리뷰 페이지 접근 스킵
            if not should_collect_reviews:
                print(f"  [INFO] 외부 리뷰 - 리뷰 페이지 수집 스킵 (detailed_reviews, top_mentions 등 수집 안함)")
            else:
                # ── re_bby_tv_dt1_reviews.py와 완전 동일한 흐름 ──

                # 1) 페이지 새로 로드 (이전 인터랙션 상태 초기화)
                print(f"  [INFO] Reloading product page for review extraction...")
                self.rate_limiter.wait(product_url, reason='review_reload')
                self.page.get(product_url)
                self.browser_diagnostics.snapshot(self.page, product_url, 'review_reload_after_get')
                self.network_diagnostics.snapshot(self.page, product_url, 'review_reload_after_get')
                time.sleep(3)
                try:
                    self.page.ele('xpath://h1', timeout=10)
                except:
                    time.sleep(3)

                # 2) Rating link 클릭 → GraphQL 캡처
                gql_data = self.capture_review_data_via_graphql()
                gql_top_mentions = None
                gql_recommendation = self.parse_graphql_recommendation(gql_data)
                gql_ai_summary = self.parse_graphql_ai_summary(gql_data)
                gql_count = self.parse_graphql_review_count(gql_data)
                gql_reviews = self.parse_graphql_reviews(gql_data)

                if gql_count is not None and str(count_of_reviews or '0') in ('0', 'None', ''):
                    count_of_reviews = gql_count
                    count_of_star_ratings = gql_count
                    print(f"  [OK] Count_of_Reviews updated from GraphQL: {count_of_reviews}")

                if gql_reviews:
                    detailed_reviews = gql_reviews
                    print(f"  [INFO] Using GraphQL reviews; skipping review page DOM wait")

                # 3) "See All Customer Reviews" 클릭 (re 파일과 동일 - 단순 JS 클릭)
                if not detailed_reviews:
                    self.page.run_js("window.scrollTo(0, document.body.scrollHeight * 0.7)")
                    time.sleep(1)
                    see_all_result = self.page.run_js('''
                        var btns = document.querySelectorAll('button, a');
                        for (var i = 0; i < btns.length; i++) {
                            var text = btns[i].textContent.trim().toLowerCase();
                            if (text.includes('see all') && text.includes('review')) {
                                btns[i].scrollIntoView({behavior: "smooth", block: "center"});
                                btns[i].click();
                                return 'clicked: ' + btns[i].textContent.trim().substring(0, 60);
                            }
                        }
                        return 'not found';
                    ''')
                    print(f"  [INFO] See All Reviews: {see_all_result}")

                    if see_all_result == 'not found':
                        # 추가 스크롤 후 재시도 (re 파일과 동일)
                        for scroll_pct in [0.8, 0.9, 1.0]:
                            self.page.run_js(f"window.scrollTo(0, document.body.scrollHeight * {scroll_pct})")
                            time.sleep(1.5)
                            see_all_result = self.page.run_js('''
                                var btns = document.querySelectorAll('button, a');
                                for (var i = 0; i < btns.length; i++) {
                                    var text = btns[i].textContent.trim().toLowerCase();
                                    if (text.includes('see all') && text.includes('review')) {
                                        btns[i].scrollIntoView({behavior: "smooth", block: "center"});
                                        btns[i].click();
                                        return 'clicked: ' + btns[i].textContent.trim().substring(0, 60);
                                    }
                                }
                                return 'not found';
                            ''')
                            if see_all_result != 'not found':
                                print(f"  [INFO] See All Reviews (scroll {int(scroll_pct*100)}%): {see_all_result}")
                                break

                if not detailed_reviews and see_all_result != 'not found':
                    # 4) 리뷰 페이지 로딩 대기 (re 파일과 동일)
                    time.sleep(5)
                    print(f"  [INFO] Reviews page URL: {self.page.url[:100]}")

                    # 5) DOM에서 리뷰 20개 수집 (re 파일 extract_reviews_from_dom과 동일)
                    reviews = []
                    collected = 0
                    page_num = 1

                    review_dom_js = '''
                        var reviews = [];
                        var selectors = [
                            'p.pre-white-space',
                            'li.review-item p',
                            '.ugc-review-body p',
                            '[data-testid*="review"] p',
                            '[class*="review"] p'
                        ];
                        selectors.forEach(function(sel) {
                            document.querySelectorAll(sel).forEach(function(el) {
                                var text = el.textContent.trim();
                                if (text && text.length > 20 && !reviews.includes(text)) reviews.push(text);
                            });
                        });
                        return reviews;
                    '''
                    review_dom_count_js = review_dom_js.replace('return reviews;', 'return reviews.length;')

                    # 리뷰 DOM 렌더링 대기 (최대 5초)
                    for wait in range(5):
                        count = self.page.run_js(review_dom_count_js)
                        if count and count > 0:
                            print(f"  [OK] Reviews rendered in DOM ({count} items, waited {wait}s)")
                            break
                        time.sleep(1)
                    else:
                        print(f"  [INFO] Reviews not rendered in DOM after 5s")
                        # DOM 확인용 HTML 저장 (1회만)
                        try:
                            dump_path = os.path.join(os.path.dirname(__file__), 'review_page_dump.html')
                            with open(dump_path, 'w', encoding='utf-8') as f:
                                f.write(self.page.html)
                            print(f"  [DIAG] Page HTML saved: {dump_path}")
                        except:
                            pass

                    # 리뷰 수집 + 페이지네이션
                    while collected < 20:
                        page_reviews = self.page.run_js(review_dom_js)
                        if page_reviews:
                            page_count = self.extract_review_count_from_text(' '.join(page_reviews))
                            if page_count is not None and str(count_of_reviews or '0') in ('0', 'None', ''):
                                count_of_reviews = page_count
                                count_of_star_ratings = page_count
                                print(f"  [OK] Count_of_Reviews updated from review page text: {count_of_reviews}")
                            for text in page_reviews:
                                if collected >= 20:
                                    break
                                if not self.is_valid_review_text(text):
                                    continue
                                collected += 1
                                reviews.append(f"review{collected} - {text}")
                            print(f"  [INFO] Page {page_num}: {len(page_reviews)} reviews (total: {collected}/20)")
                        else:
                            break

                        if collected >= 20:
                            break

                        has_next = self.page.run_js('''
                            var nextBtn = document.querySelector('li.page.next a, a[aria-label="Next"]');
                            if (nextBtn) {
                                nextBtn.scrollIntoView({behavior: "smooth", block: "center"});
                                nextBtn.click();
                                return true;
                            }
                            return false;
                        ''')
                        if not has_next:
                            break
                        page_num += 1
                        time.sleep(4)

                    if reviews:
                        detailed_reviews = ' ||| '.join(reviews)
                        print(f"  [OK] Detailed_Reviews: {collected} reviews, {len(detailed_reviews)} chars")

                print(f"  [✓] Detailed_Reviews: {len(detailed_reviews) if detailed_reviews else 0} chars")

                # Top mentions are excluded from collection; keep CSV column empty for compatibility.
                top_mentions = None
                print(f"  [INFO] Top_Mentions skipped (excluded from collection target)")

                if gql_recommendation:
                    recommendation_intent = gql_recommendation
                print(f"  [✓] Recommendation_Intent: {recommendation_intent}")

                if gql_ai_summary:
                    summarized_review_content = gql_ai_summary
                print(f"  [✓] Summarized_Review_Content: {summarized_review_content[:50] if summarized_review_content else 'None'}...")

            # 9-4-1. detailed_reviews NULL 로그 기록 (count_of_reviews > 0인데 NULL인 경우)
            try:
                cor_int = int(str(count_of_reviews).replace(',', '')) if count_of_reviews else 0
            except:
                cor_int = 0

            if cor_int > 0 and not detailed_reviews:
                log_entry = {
                    'timestamp': datetime.now(self.korea_tz).strftime('%Y-%m-%d %H:%M:%S'),
                    'product_url': product_url,
                    'retailer_sku_name': retailer_sku_name[:80] if retailer_sku_name else 'N/A',
                    'count_of_reviews': count_of_reviews,
                    'star_rating': star_rating,
                    'reason': 'reviews_page_access_failed' if not top_mentions else 'extract_reviews_failed'
                }
                self.null_review_logs.append(log_entry)
                print(f"  [WARNING] detailed_reviews is NULL but count_of_reviews={count_of_reviews}")

            # 9-5. data 검증 (문제 감지 및 로깅)
            print(f"\n  [VALIDATION] Checking data quality...")
            self.validator.validate_item(item, product_url, 'bby_tv_dt1')
            self.validator.validate_screen_size(screen_size, product_url, 'bby_tv_dt1')
            self.validator.validate_price(final_sku_price, 'final_sku_price', product_url, 'bby_tv_dt1')
            if savings:  # savings는 없을 수도 있음
                self.validator.validate_price(savings, 'savings', product_url, 'bby_tv_dt1')
            if original_sku_price:  # original도 없을 수도 있음
                self.validator.validate_price(original_sku_price, 'original_sku_price', product_url, 'bby_tv_dt1')
            self.validator.validate_count(count_of_reviews, 'count_of_reviews', product_url, 'bby_tv_dt1')
            self.validator.validate_star_rating(star_rating, product_url, 'bby_tv_dt1')

            # 10. Detail CSV save
            self.save_to_db(
                page_type=page_type,
                order=self.order,
                retailer_sku_name=retailer_sku_name,
                item=item,
                electricity_use=electricity_use,
                screen_size=screen_size,
                count_of_reviews=count_of_reviews,
                count_of_star_ratings=count_of_star_ratings,
                top_mentions=top_mentions,
                detailed_reviews=detailed_reviews,
                summarized_review_content=summarized_review_content,
                recommendation_intent=recommendation_intent,
                product_url=product_url,
                # 가격 정보는 crawling한 값 사용 (CHANGED)
                final_sku_price=final_sku_price,
                savings=savings,
                original_sku_price=original_sku_price,
                # 소스 table의 추가 data (가격 제외)
                offer=url_data['offer'],
                pick_up_availability=url_data['pick_up_availability'],
                shipping_availability=url_data['shipping_availability'],
                delivery_availability=url_data['delivery_availability'],
                sku_status=url_data['sku_status'],
                star_rating_source=star_rating,  # 메인 page에서 crawling한 값 사용 (CHANGED)
                promotion_type=url_data['promotion_type'],
                promotion_position=url_data['promotion_position'],
                bsr_rank=url_data['bsr_rank'],
                main_rank=url_data['main_rank'],
                trend_rank=url_data.get('trend_rank'),
                model_year=model_year,  # ADDED: model_year parameter
                sku=sku,  # ADDED: sku parameter for tv_item_mst
                similar_products=similar_products  # Similar products for retailer_sku_name_similar
            )

            # Increment total collected after successful save
            self.total_collected += 1
            self.rate_limiter.register_outcome(product_url, 'success')

            return True

        except Exception as e:
            print(f"  [ERROR] detail page crawling failed: {e}")
            try:
                self.rate_limiter.register_outcome(url_data.get('product_url'), 'failed')
            except Exception:
                pass
            import traceback
            traceback.print_exc()
            return False

    def save_to_db(self, page_type, order, retailer_sku_name, item,
                   electricity_use, screen_size, count_of_reviews, count_of_star_ratings, top_mentions, detailed_reviews,
                   summarized_review_content, recommendation_intent, product_url,
                   final_sku_price, savings, original_sku_price, offer,
                   pick_up_availability, shipping_availability, delivery_availability,
                   sku_status, star_rating_source, promotion_type, promotion_position,
                   bsr_rank, main_rank, trend_rank, model_year, sku="no sku", similar_products=None):
        """VPN 테스트용 CSV 저장. DB에는 쓰지 않는다."""
        try:
            print(f"  [CSV] Saving to {self.csv_output_path}...")
            print(f"       Product: {retailer_sku_name[:60] if retailer_sku_name else 'N/A'}...")
            print(f"       Item (SKU): {item if item else 'N/A'}")

            account_name = self.config.get_constant('account_name', None, 'Bestbuy')
            calendar_week = f"w{datetime.now().isocalendar().week}"
            crawl_datetime = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

            if star_rating_source == "Not yet reviewed":
                count_of_star_ratings = 0
                count_of_reviews = 0

            count_of_reviews_int = None
            if count_of_reviews is not None:
                try:
                    count_of_reviews_int = int(str(count_of_reviews).replace(',', ''))
                except:
                    count_of_reviews_int = None

            if star_rating_source and "not yet reviewed" in str(star_rating_source).lower():
                if count_of_reviews_int != 0 and count_of_reviews_int is not None:
                    print(f"  [WARNING] Data inconsistency detected: star_rating='Not yet reviewed' but count_of_reviews={count_of_reviews_int}")
                    print(f"  [FIX] Setting count_of_reviews to 0")
                count_of_reviews_int = 0

            similar_names = None
            if similar_products:
                similar_names = ' ||| '.join([p.get('product_name', '') for p in similar_products if p.get('product_name')]) or None

            fieldnames = [
                'account_name', 'batch_id', 'page_type', 'order', 'retailer_sku_name',
                'item', 'sku', 'product_url', 'crawl_datetime', 'calendar_week',
                'star_rating', 'count_of_reviews', 'count_of_reviews_int',
                'count_of_star_ratings', 'screen_size', 'estimated_annual_electricity_use',
                'final_sku_price', 'original_sku_price', 'savings', 'offer',
                'pick_up_availability', 'shipping_availability', 'delivery_availability',
                'sku_status', 'top_mentions', 'detailed_review_content',
                'summarized_review_content', 'recommendation_intent', 'main_rank',
                'bsr_rank', 'trend_rank', 'promotion_position', 'promotion_type',
                'model_year', 'retailer_sku_name_similar'
            ]
            row = {
                'account_name': account_name,
                'batch_id': self.batch_id,
                'page_type': page_type,
                'order': order,
                'retailer_sku_name': retailer_sku_name,
                'item': item,
                'sku': sku,
                'product_url': product_url,
                'crawl_datetime': crawl_datetime,
                'calendar_week': calendar_week,
                'star_rating': star_rating_source,
                'count_of_reviews': count_of_reviews,
                'count_of_reviews_int': count_of_reviews_int,
                'count_of_star_ratings': count_of_star_ratings,
                'screen_size': screen_size,
                'estimated_annual_electricity_use': electricity_use,
                'final_sku_price': final_sku_price,
                'original_sku_price': original_sku_price,
                'savings': savings,
                'offer': offer,
                'pick_up_availability': pick_up_availability,
                'shipping_availability': shipping_availability,
                'delivery_availability': delivery_availability,
                'sku_status': sku_status,
                'top_mentions': top_mentions,
                'detailed_review_content': detailed_reviews,
                'summarized_review_content': summarized_review_content,
                'recommendation_intent': recommendation_intent,
                'main_rank': main_rank,
                'bsr_rank': bsr_rank,
                'trend_rank': trend_rank,
                'promotion_position': promotion_position,
                'promotion_type': promotion_type,
                'model_year': model_year,
                'retailer_sku_name_similar': similar_names
            }

            quality_issues = self.row_quality_auditor.audit_detail_row(row)
            if quality_issues:
                print(f"  [VALIDATION] Row quality audit issues: {len(quality_issues)}")
                for issue in quality_issues[:5]:
                    print(f"       - {issue['severity']} {issue['field']}: {issue['reason']}")

            file_exists = os.path.exists(self.csv_output_path)
            with open(self.csv_output_path, 'a', newline='', encoding='utf-8-sig') as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                if not file_exists or os.path.getsize(self.csv_output_path) == 0:
                    writer.writeheader()
                writer.writerow(row)

            print(f"  [CSV] ✓ Saved VPN test row")
            return True

        except Exception as e:
            print(f"  [ERROR] CSV save failed: {e}")
            import traceback
            traceback.print_exc()
            return False

    def cleanup_old_logs(self, log_dir, days=30):
        """30일 지난 로그 파일 삭제"""
        try:
            if not os.path.exists(log_dir):
                return
            cutoff_time = time.time() - (days * 24 * 60 * 60)
            deleted_count = 0
            for filename in os.listdir(log_dir):
                if filename.startswith('bby_tv_dt1_') and filename.endswith('.txt'):
                    file_path = os.path.join(log_dir, filename)
                    if os.path.getmtime(file_path) < cutoff_time:
                        os.remove(file_path)
                        deleted_count += 1
            if deleted_count > 0:
                print(f"[INFO] Deleted {deleted_count} old log files (older than {days} days)")
        except Exception as e:
            print(f"[WARNING] Failed to cleanup old logs: {e}")

    def run(self, start_from=0, stop_at=None):
        """메인 execution. start_from: 0-based index, stop_at: datetime 종료 시각"""
        # 콘솔 출력을 파일로도 저장
        log_dir = r"C:\samsung_dx_retail_com\log"
        if not os.path.exists(log_dir):
            os.makedirs(log_dir)
        self.cleanup_old_logs(log_dir)
        log_filename = f"bby_tv_dt1_{self.batch_id}.txt"
        log_path = os.path.join(log_dir, log_filename)
        self.tee = Tee(log_path)

        try:
            print("="*80)
            print(f"Best Buy TV Detail Page Crawler (DrissionPage) (Batch ID: {self.batch_id})")
            print("="*80)

            self.connect_db()

            # URLs 가져오기
            urls = self.get_recent_urls()
            if not urls:
                print("[ERROR] No URLs found")
                return

            # 브라우저 설정 (DrissionPage)
            completed_urls = self.load_completed_product_urls()
            if completed_urls:
                before_resume_filter = len(urls)
                urls = [u for u in urls if u.get('product_url') not in completed_urls]
                skipped = before_resume_filter - len(urls)
                print(f"[INFO] Resume mode: skipped {skipped} URLs already present in detail CSV")
                if not urls:
                    print("[INFO] Nothing left to crawl after resume filtering")
                    return

            if not self.setup_browser():
                return

            # 각 URL crawling
            success_count = 0
            if start_from > 0:
                urls = urls[start_from:]
                print(f"[INFO] Skipped first {start_from} URLs, remaining: {len(urls)}")

            # 자동 재시도 설정
            MAX_RETRIES = 5          # 최대 재시도 횟수
            INITIAL_WAIT = 1200      # 대기 시간 (20분)
            retry_count = 0          # 현재 재시도 횟수
            consecutive_fails = 0    # 연속 실패 횟수

            i = 0
            while i < len(urls):
                url_data = urls[i]
                idx = i + start_from + 1

                # 종료 시각 체크
                if stop_at and datetime.now() >= stop_at:
                    print(f"\n{'='*80}")
                    print(f"[INFO] Reached stop time: {stop_at.strftime('%Y-%m-%d %H:%M:%S')}")
                    print(f"[INFO] Collected so far: {success_count}")
                    break

                # Check if we've reached the maximum SKU limit
                if self.total_collected >= self.max_skus:
                    print(f"\n{'='*80}")
                    print(f"[INFO] Reached maximum SKU limit ({self.max_skus})")
                    print(f"[INFO] Stopping collection. Total collected: {self.total_collected}")
                    break

                result = self.scrape_detail_page(url_data)

                if result == 'blocked':
                    # ERR_HTTP2_PROTOCOL_ERROR 감지 - 차단 확실
                    consecutive_fails += 1
                    retry_count += 1
                    self.save_checkpoint('blocked', idx, url_data, success_count)
                    print(f"[ERROR] Block signal detected. Stopping to avoid repeated access.")
                    print(f"[INFO] Resume later with: python bby_tv_dt1.py")
                    break

                    self.check_db_connection()

                    # 다른 카테고리 상품 페이지 접속 (세션 워밍업)
                    self.restart_browser()
                    self._warmup_with_different_page()

                    # 같은 URL 다시 시도 (i 증가 안 함)
                    self.order -= 1  # order 카운터 복원 (scrape_detail_page에서 증가했으므로)
                    continue

                elif result == 'manual_pause':
                    self.save_checkpoint('h1_not_found_manual_pause', idx, url_data, success_count)
                    print("\n" + "="*80)
                    print("[PAUSE] h1 element not found. Collection is paused before the next item.")
                    print("[PAUSE] Check the browser/session and diagnostic HTML, then press Enter to retry this URL.")
                    print("="*80)
                    try:
                        input()
                    except EOFError:
                        print("[INFO] No interactive input available. Stopping collection.")
                        break

                    if not self.restart_browser():
                        print("[ERROR] Browser restart failed after manual pause. Stopping.")
                        break
                    self._warmup_with_different_page()
                    print("[INFO] Skipping failed URL after manual pause; it remains in checkpoint for later retry.")
                    i += 1
                    continue

                elif result is True:
                    success_count += 1
                    consecutive_fails = 0
                    retry_count = 0
                    self.refresh_discovery_page(success_count)
                    if not self.proactive_session_refresh(success_count):
                        print("[ERROR] Browser refresh failed. Stopping.")
                        break

                else:
                    # 일반 실패 (h1 not found 등) → skip하고 다음 URL로
                    consecutive_fails += 1
                    self.save_checkpoint('failed', idx, url_data, success_count)

                    if consecutive_fails >= 3:
                        # 연속 3회 실패 → 차단 판정
                        retry_count += 1
                        if retry_count > MAX_RETRIES:
                            print(f"\n{'='*80}")
                            print(f"[ERROR] Max retries ({MAX_RETRIES}) exceeded. Stopping.")
                            print(f"[INFO] Resume later with: python bby_tv_dt1.py {idx}")
                            break

                        if retry_count == 1:
                            print(f"\n{'='*80}")
                            print(f"[RETRY {retry_count}/{MAX_RETRIES}] {consecutive_fails} consecutive failures. Warmup and retry...")
                            print(f"{'='*80}")
                        else:
                            wait_time = min(INITIAL_WAIT * (retry_count - 1), 1200)
                            print(f"\n{'='*80}")
                            print(f"[RETRY {retry_count}/{MAX_RETRIES}] {consecutive_fails} consecutive failures. Waiting {wait_time // 60} minutes...")
                            print(f"{'='*80}")
                            time.sleep(wait_time)

                        self.check_db_connection()

                        # 다른 카테고리 상품 페이지 접속 (세션 워밍업)
                        self._warmup_with_different_page()

                        consecutive_fails = 0
                        # 실패한 항목 다시 시도 (i 증가 안 함)
                        self.order -= 1
                        continue
                    # 연속 3회 미만 → skip, 다음 URL로

                # page 간 딜레이
                time.sleep(random.uniform(5, 10))
                i += 1

            print("\n" + "="*80)
            print(f"crawling complete! successful: {success_count}/{len(urls)}items")
            print("="*80)

            # NULL detailed_review_content 로그 파일 저장
            if self.null_review_logs:
                log_date = datetime.now(self.korea_tz).strftime('%y%m%d')
                log_filename = f"null_detailed_review_content_{log_date}.txt"
                log_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), log_filename)

                with open(log_path, 'w', encoding='utf-8') as f:
                    f.write(f"NULL Detailed Review Content Log - {datetime.now(self.korea_tz).strftime('%Y-%m-%d %H:%M:%S')}\n")
                    f.write(f"Total: {len(self.null_review_logs)} products\n")
                    f.write("="*100 + "\n\n")

                    for i, log in enumerate(self.null_review_logs, 1):
                        f.write(f"[{i}] {log['timestamp']}\n")
                        f.write(f"    URL: {log['product_url']}\n")
                        f.write(f"    Product: {log['retailer_sku_name']}\n")
                        f.write(f"    Count of Reviews: {log['count_of_reviews']}\n")
                        f.write(f"    Star Rating: {log['star_rating']}\n")
                        f.write(f"    Reason: {log['reason']}\n")
                        f.write("-"*100 + "\n")

                print(f"\n[INFO] NULL detailed_review_content log saved: {log_path}")
                print(f"       Total products with NULL reviews: {len(self.null_review_logs)}")

            # data 검증 요약 출력
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

            print(f"\n[INFO] VPN test CSV saved at: {self.csv_output_path}")
            print(f"[INFO] Crawl audit JSONL saved at: {self.audit_log_path}")

        except Exception as e:
            print(f"[ERROR] crawler execution error: {e}")
            import traceback
            traceback.print_exc()

        finally:
            self.close_browser()
            if self.db_conn:
                self.db_conn.close()
                print("[INFO] DB connection closed")
            if self.tee:
                log_path = self.tee.file.name
                print(f"\n[INFO] Console log saved: {log_path}")
                self.tee.close()

def main():
    # Parse arguments:
    #   python bby_tv_dt1.py                       # 처음부터
    #   python bby_tv_dt1.py 39                    # 39번부터
    #   python bby_tv_dt1.py until 20260228060000  # 지정 시각까지 수집 후 종료
    #   python bby_tv_dt1.py 39 until 20260228060000  # 39번부터 + 시간 제한
    start_from = 0
    stop_at = None

    args = sys.argv[1:]
    i = 0
    while i < len(args):
        if args[i] == 'until' and i + 1 < len(args):
            try:
                stop_at = datetime.strptime(args[i + 1], '%Y%m%d%H%M%S')
                print(f"[INFO] Stop at: {stop_at.strftime('%Y-%m-%d %H:%M:%S')}")
            except ValueError:
                print(f"[WARNING] Invalid until format '{args[i + 1]}', use YYYYMMDDHHmmss")
            i += 2
        else:
            try:
                start_from = int(args[i]) - 1
                if start_from < 0:
                    start_from = 0
                print(f"[INFO] Start from: {start_from + 1} (skipping first {start_from})")
            except ValueError:
                print(f"[WARNING] Invalid argument '{args[i]}'")
            i += 1

    crawler = BestBuyDetailCrawler()
    crawler.run(start_from=start_from, stop_at=stop_at)

if __name__ == "__main__":
    main()
