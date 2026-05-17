"""
BestBuy TV Main 페이지 크롤러

================================================================================
실행 모드
================================================================================
- 개별 실행: test_mode=True (기본값)
- 통합 크롤러: test_mode 및 batch_id를 파라미터로 전달

================================================================================
주요 기능
================================================================================
- Main 페이지 제품 리스트를 GraphQL replay로 수집
- main_rank는 페이지 관계없이 1부터 순차 증가
- 테스트 모드: test_count 설정값만큼 수집
- 운영 모드: max_products 설정값만큼 수집

================================================================================
저장 파일
================================================================================
- data/main/parsed/bby_tv_main1_vpn_test.csv
"""

import sys
import os
import time
import random
import traceback
import re
import csv
import json
from datetime import datetime, timedelta
from pathlib import Path

from dotenv import load_dotenv
from zenrows import ZenRowsClient

# 공통 환경 설정 (작업 디렉토리, 한글 출력, 경로 설정)
RUNNING_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'running')
if RUNNING_DIR not in sys.path:
    sys.path.insert(0, RUNNING_DIR)
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
from common.setup import setup_environment
setup_environment(__file__)

from common.base_crawler import BaseCrawler
from config import DB_CONFIG
from db_readonly import connect_readonly
from data_paths import graphql_registry_dir, listing_parsed_dir

load_dotenv(Path(__file__).resolve().parent / ".env")


BESTBUY_BASE_URL = "https://www.bestbuy.com"
GRAPHQL_ENDPOINT = os.getenv("BESTBUY_GRAPHQL_ENDPOINT", "https://www.bestbuy.com/gateway/graphql")
SEARCH_TERM = os.getenv("BESTBUY_SEARCH_TERM", "tv")
SEARCH_SORT = os.getenv("BESTBUY_SEARCH_SORT", "")
ORGANIC_OFFSET = int(os.getenv("BESTBUY_MAIN_ORGANIC_OFFSET", "18"))
REQUEST_TIMEOUT = int(os.getenv("ZENROWS_TIMEOUT", "120"))
BESTBUY_LOCATION_ZIP = os.getenv("BESTBUY_LOCATION_ZIP", os.getenv("BESTBUY_ZIP_CODE", "55423"))
BESTBUY_STORE_ID = os.getenv("BESTBUY_STORE_ID", "7")
BESTBUY_X_FORWARDED_FOR = os.getenv("BESTBUY_X_FORWARDED_FOR", "").strip()
BESTBUY_USER_AGENT = os.getenv(
    "BESTBUY_USER_AGENT",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/148.0.0.0 Safari/537.36",
)
REQUIRE_FULFILLMENT = os.getenv("BESTBUY_MAIN_REQUIRE_FULFILLMENT", "1").lower() in {"1", "true", "yes", "y"}
RETRY_FULFILLMENT_WITH_COOKIES = os.getenv(
    "BESTBUY_MAIN_RETRY_FULFILLMENT_WITH_COOKIES", "1"
).lower() in {"1", "true", "yes", "y"}


class BestBuyTVMainCrawler(BaseCrawler):
    """
    BestBuy TV Main 페이지 크롤러
    """

    def extract_item_from_url(self, product_url):
        """URL에서 item (SKU ID) 추출 - dt.py와 동일한 로직"""
        if not product_url:
            return None

        try:
            # /sku/숫자, /openbox, ?쿼리 제거
            cleaned_url = re.sub(r'/sku/\d+(/openbox\?.*)?$', '', product_url)
            cleaned_url = cleaned_url.split('?')[0]
            parts = cleaned_url.split('/')
            if not parts:
                return None
            item = parts[-1]
            # .p 확장자 제거 (예: 6507560.p → 6507560)
            if item.endswith('.p'):
                item = item[:-2]
            return item if item else None
        except Exception:
            return None

    def is_product_excluded(self, item):
        """tv_item_mst에서 is_product=FALSE인지 확인"""
        if not item:
            return False

        try:
            cursor = self.db_conn.cursor()
            cursor.execute("""
                SELECT is_product FROM tv_item_mst
                WHERE item = %s AND account_name = %s
            """, (item, self.account_name))
            row = cursor.fetchone()
            cursor.close()

            # 조회 결과 없으면 제외 안함 (신규 item)
            if row is None:
                return False
            # is_product가 False이면 제외
            return row[0] is False
        except Exception:
            return False

    def __init__(self, test_mode=True, batch_id=None, time_offset_hours=0):
        """초기화. test_mode: 테스트(True)/운영 모드(False), batch_id: 통합 크롤러에서 전달"""
        super().__init__()
        self.test_mode = test_mode
        self.account_name = 'Bestbuy'
        self.page_type = 'main'
        self.batch_id = batch_id
        self.time_offset_hours = time_offset_hours
        self.calendar_week = None
        self.url_template = None

        self.test_count = 1  # 테스트 모드
        self.max_products = 300  # 운영 모드
        self.max_pages = 20  # 최대 페이지 수
        self.current_rank = 0
        self.saved_urls = set()  # 중복 URL 추적용
        self.csv_output_dir = str(listing_parsed_dir("main"))
        self.graphql_output_dir = str(graphql_registry_dir())
        self.csv_output_path = os.path.join(self.csv_output_dir, 'bby_tv_main1_vpn_test.csv')
        self.raw_target_output_path = os.path.join(self.csv_output_dir, 'bby_tv_main_raw_target_list.csv')
        self.main_data_dir = os.path.dirname(self.csv_output_dir)
        self.graphql_raw_dir = os.path.join(self.main_data_dir, "raw_graphql")
        self.benchmarks_dir = os.path.join(self.main_data_dir, "benchmarks")
        self.page_benchmarks_path = os.path.join(self.benchmarks_dir, "page_benchmarks.csv")
        self.page_summary_path = os.path.join(self.csv_output_dir, "main_page_summary.json")
        self.raw_search_summary_path = os.path.join(self.main_data_dir, "raw_search_summary.json")
        self.manifest_path = os.path.join(self.main_data_dir, "manifest.json")
        self.graphql_operation = None
        self.graphql_endpoint = GRAPHQL_ENDPOINT
        self.zenrows_client = None
        self.page_benchmarks = []
        self.raw_search = []
        self.run_started_at = None
        self.run_start_perf = None

        # 통계 변수
        self.stats = {
            'collected': 0,         # 수집 진행한 갯수
            'duplicates': 0,        # 중복 URL 제거 갯수
            'openbox_filtered': 0,  # Open Box 제외 갯수
            'non_product': 0,       # is_product=FALSE 제외 갯수
            'inserted': 0,          # INSERT 갯수
            'skipped_by_target': 0  # target 도달 후 미검사 갯수
        }

    def reset_output_files(self):
        """실제 실행을 시작할 때만 이전 CSV 산출물을 비운다."""
        for path in (self.csv_output_path, self.raw_target_output_path):
            if os.path.exists(path):
                os.remove(path)

    def connect_db(self):
        """V2 listing crawler는 DB에서 설정만 읽고 절대 저장하지 않는다."""
        try:
            self.db_conn = connect_readonly({**DB_CONFIG, 'database': 'postgres'})
            print("[SUCCESS] Read-only database connected")
            return True
        except Exception as e:
            print(f"[ERROR] Database connection failed: {e}")
            traceback.print_exc()
            return False

    def initialize(self):
        """초기화: main listing은 GraphQL direct만 사용한다."""
        # 1. DB 연결
        if not self.connect_db():
            print("[ERROR] Initialize failed: DB connection failed")
            return False

        self.url_template = self.load_page_urls(self.account_name, self.page_type, 'SEA', 'TV')
        if not self.url_template:
            print(f"[ERROR] Initialize failed: URL template load failed (account={self.account_name}, page_type={self.page_type})")
            return False

        print("[INFO] GraphQL direct mode enabled; skipping XPath/DOM initialization")
        print(f"[INFO] GraphQL referer URL template loaded from DB: {self.url_template}")

        api_key = os.environ.get("ZENROWS_API_KEY")
        if not api_key:
            print("[ERROR] Initialize failed: ZENROWS_API_KEY is missing in .env")
            return False
        self.zenrows_client = ZenRowsClient(api_key)

        self.graphql_operation = self.load_graphql_operation()
        if not self.graphql_operation:
            print("[ERROR] Initialize failed: GraphQL operation not found")
            return False
        self.graphql_endpoint = self.graphql_operation.get("endpoint_url") or GRAPHQL_ENDPOINT
        os.makedirs(self.graphql_raw_dir, exist_ok=True)
        os.makedirs(self.benchmarks_dir, exist_ok=True)
        print(f"[INFO] GraphQL endpoint={self.graphql_endpoint}")

        # batch_id 생성 (개별 실행 시 test_mode=True)
        if not self.batch_id:
            self.batch_id = self.generate_batch_id(self.account_name, test_mode=True, time_offset_hours=self.time_offset_hours)

        # calendar_week 생성 및 로그 정리
        self.calendar_week = self.generate_calendar_week(time_offset_hours=self.time_offset_hours)
        self.cleanup_old_logs()

        print(f"[INFO] Initialize completed: batch_id={self.batch_id}, calendar_week={self.calendar_week}")
        return True

    def load_graphql_operation(self):
        """step01 전용 GraphQL operation 로드."""
        candidates = [
            os.path.join(self.graphql_output_dir, "listing_graphql_operation.json"),
            os.path.join(os.path.dirname(os.path.abspath(__file__)), "listing_graphql_operation.json"),
        ]
        for path in candidates:
            if not os.path.exists(path):
                continue
            try:
                with open(path, encoding="utf-8") as f:
                    operation = json.load(f)
                request_payload = operation.get("request_payload")
                if operation.get("endpoint_url") and isinstance(request_payload, dict):
                    print(f"[INFO] Loaded GraphQL operation from {path}")
                    return operation
            except Exception as exc:
                print(f"[WARNING] Failed to load GraphQL operation {path}: {exc}")
        return None

    def build_referer_url(self, page_number):
        """DB url_template의 {page}를 치환해서 GraphQL referer로 사용한다."""
        if not self.url_template:
            return None
        if "{page}" in self.url_template:
            return self.url_template.replace("{page}", str(page_number))
        separator = "&" if "?" in self.url_template else "?"
        return f"{self.url_template}{separator}cp={page_number}"

    def prepare_graphql_payload(self, page_number, referer_url):
        """레퍼런스 구현처럼 operation variables만 page/search 기준으로 갱신한다."""
        template = self.graphql_operation.get("request_payload") or {}
        payload = json.loads(json.dumps(template))
        variables = payload.setdefault("variables", {})

        for key in ("input", "detailedSearchInput"):
            if isinstance(variables.get(key), dict):
                variables[key]["query"] = SEARCH_TERM
                variables[key]["queryType"] = "SEARCH"
                variables[key]["site"] = "WWW"

        variables["categoryId"] = SEARCH_TERM
        variables["isBrowse"] = False
        variables["revertProductListQueryChanges"] = False
        variables.setdefault("sort", {})
        variables["sort"]["sort"] = SEARCH_SORT

        variables.setdefault("pagination", {})
        variables["pagination"]["pageNumber"] = page_number
        variables["pagination"]["offset"] = ORGANIC_OFFSET
        variables.setdefault("paginationForDetailedProductSearch", {})
        variables["paginationForDetailedProductSearch"]["pageNumber"] = page_number
        variables["paginationForDetailedProductSearch"]["offset"] = ORGANIC_OFFSET

        self.update_location_context(variables)
        self.update_referers(variables, referer_url)
        return payload

    def update_location_context(self, variables):
        """Keep PLP fulfillment geo context consistent with the proxy/session.

        Captured operations may contain a stale local xForwardedFor value. When
        replaying through ZenRows, leaving that value in the GraphQL variables can
        make BestBuy's fulfillment resolver reject only fulfillmentOptions while
        the rest of the listing query still succeeds.
        """
        if not isinstance(variables, dict):
            return

        availability = self.nested_get(variables, ["filter", "availability"])
        if isinstance(availability, dict):
            availability["zipCode"] = BESTBUY_LOCATION_ZIP

        variables["destinationZipCode"] = BESTBUY_LOCATION_ZIP
        variables["preferredStoreZipCode"] = BESTBUY_LOCATION_ZIP
        variables["hasPreferredStoreZipCode"] = True

        fulfillment_input = variables.setdefault("fulfillmentInput", {})
        if isinstance(fulfillment_input, dict):
            shipping = fulfillment_input.setdefault("shipping", {})
            if isinstance(shipping, dict):
                shipping["destinationZipCode"] = BESTBUY_LOCATION_ZIP
            delivery = fulfillment_input.setdefault("delivery", {})
            if isinstance(delivery, dict):
                delivery["destinationZipCode"] = BESTBUY_LOCATION_ZIP
            pickup = fulfillment_input.setdefault("inStorePickup", {})
            if isinstance(pickup, dict):
                pickup["storeId"] = BESTBUY_STORE_ID
                pickup["searchNearby"] = True
            button_state = fulfillment_input.setdefault("buttonState", {})
            if isinstance(button_state, dict):
                button_state["destinationZipCode"] = BESTBUY_LOCATION_ZIP
                button_state["storeId"] = BESTBUY_STORE_ID

        media_input = variables.get("searchWithBestMediaInput")
        if isinstance(media_input, dict):
            if BESTBUY_X_FORWARDED_FOR:
                media_input["xForwardedFor"] = BESTBUY_X_FORWARDED_FOR
            else:
                media_input.pop("xForwardedFor", None)

    def update_referers(self, value, referer_url):
        if not referer_url:
            return
        if isinstance(value, dict):
            for key in list(value.keys()):
                if str(key).lower() in {"referer", "referrer"}:
                    value[key] = referer_url
                else:
                    self.update_referers(value[key], referer_url)
        elif isinstance(value, list):
            for child in value:
                self.update_referers(child, referer_url)

    def zenrows_params(self):
        params = {"custom_headers": "true"}
        if os.getenv("BESTBUY_GRAPHQL_PREMIUM_PROXY", "1").lower() in {"1", "true", "yes"}:
            params["premium_proxy"] = "true"
            params["proxy_country"] = "us"
        if os.getenv("BESTBUY_GRAPHQL_JS_RENDER", "1").lower() in {"1", "true", "yes"}:
            params["js_render"] = "true"
        if os.getenv("BESTBUY_GRAPHQL_MODE_AUTO", "0").lower() in {"1", "true", "yes"}:
            params["mode"] = "auto"
            params["proxy_country"] = "us"
        return params

    def post_graphql(self, payload, page_number, referer_url, use_operation_cookies=False):
        headers = {
            "accept": "application/json, text/plain, */*",
            "content-type": "application/json",
            "origin": BESTBUY_BASE_URL,
            "referer": referer_url,
            "user-agent": BESTBUY_USER_AGENT,
        }
        cookie_header = self.operation_cookie_header(force=use_operation_cookies)
        if cookie_header:
            headers["cookie"] = cookie_header
        started = time.perf_counter()
        started_at = datetime.now().isoformat(timespec="seconds")
        cookie_mode = " with operation cookies" if cookie_header else ""
        print(f"[INFO] Page {page_number}: POST GraphQL via ZenRows{cookie_mode} timeout={REQUEST_TIMEOUT}s referer={referer_url}")
        response = self.zenrows_client.post(
            self.graphql_endpoint,
            params=self.zenrows_params(),
            headers=headers,
            data=json.dumps(payload),
            timeout=REQUEST_TIMEOUT,
        )
        elapsed = round(time.perf_counter() - started, 3)
        print(
            f"[INFO] Page {page_number}: ZenRows status={response.status_code} "
            f"elapsed={elapsed}s cost={response.headers.get('x-request-cost', '')}"
        )
        finished_at = datetime.now().isoformat(timespec="seconds")
        meta = self.save_graphql_artifacts(page_number, payload, response, started_at, finished_at, elapsed, referer_url)
        return response, meta

    def operation_cookie_header(self, force=False):
        use_cookies = force or os.getenv("BESTBUY_GRAPHQL_USE_OPERATION_COOKIES", "0").lower() in {"1", "true", "yes"}
        if not use_cookies:
            return ""
        cookies = self.graphql_operation.get("cookies") if isinstance(self.graphql_operation, dict) else {}
        if not isinstance(cookies, dict):
            return ""
        return "; ".join(f"{key}={value}" for key, value in cookies.items() if value not in (None, ""))

    def save_graphql_artifacts(self, page_number, payload, response, started_at, finished_at, elapsed, referer_url):
        stem = f"page_{page_number:03d}"
        request_path = os.path.join(self.graphql_raw_dir, f"{stem}_request.json")
        response_path = os.path.join(self.graphql_raw_dir, f"{stem}_response.txt")
        headers_path = os.path.join(self.graphql_raw_dir, f"{stem}_headers.json")
        meta_path = os.path.join(self.graphql_raw_dir, f"{stem}_meta.json")
        json_path = os.path.join(self.graphql_raw_dir, f"{stem}_response.json")
        parse_error = ""
        parsed = None
        try:
            with open(request_path, "w", encoding="utf-8") as f:
                json.dump(payload, f, ensure_ascii=False, indent=2)
            with open(response_path, "w", encoding="utf-8", errors="replace") as f:
                f.write(response.text or "")
            with open(headers_path, "w", encoding="utf-8") as f:
                json.dump(dict(response.headers), f, ensure_ascii=False, indent=2)
            try:
                parsed = response.json()
                with open(json_path, "w", encoding="utf-8") as f:
                    json.dump(parsed, f, ensure_ascii=False, indent=2)
            except ValueError as exc:
                parse_error = str(exc)
        except Exception as exc:
            parse_error = parse_error or str(exc)
            print(f"[WARNING] Page {page_number}: failed to save GraphQL artifacts: {exc}")

        meta = {
            "page": page_number,
            "url": referer_url,
            "started_at": started_at,
            "finished_at": finished_at,
            "elapsed_seconds": elapsed,
            "status_code": response.status_code,
            "x_request_cost": response.headers.get("x-request-cost", ""),
            "bytes": len(response.text or ""),
            "parse_error": parse_error,
            "request_path": request_path,
            "response_path": response_path,
            "response_json_path": json_path if parsed is not None else "",
            "headers_path": headers_path,
        }
        try:
            with open(meta_path, "w", encoding="utf-8") as f:
                json.dump(meta, f, ensure_ascii=False, indent=2)
        except Exception as exc:
            print(f"[WARNING] Page {page_number}: failed to save GraphQL meta: {exc}")
        return meta

    def nested_get(self, value, path, default=None):
        current = value
        for key in path:
            if not isinstance(current, dict):
                return default
            current = current.get(key)
        return default if current is None else current

    def normalize_url(self, value):
        if not value:
            return None
        text = str(value)
        if text.startswith("/"):
            return BESTBUY_BASE_URL + text
        return text

    def legacy_product_url(self, sku):
        """Downstream dt 단계가 읽기 쉬운 기존 BestBuy PDP URL 형태."""
        if not sku:
            return None
        return f"{BESTBUY_BASE_URL}/site/-/{sku}.p?skuId={sku}"

    def parse_graphql_product(self, product, page_number, extra=None):
        if not isinstance(product, dict) or not product.get("skuId"):
            return None
        sku = str(product.get("skuId"))
        price = product.get("price") if isinstance(product.get("price"), dict) else {}
        fulfillment = product.get("fulfillmentOptions") if isinstance(product.get("fulfillmentOptions"), dict) else {}
        is_sponsored = bool(extra and (
            extra.get("is_sponsored")
            or str(extra.get("container_type") or "").startswith("sponsored")
        ))
        source_product_url = (
            self.nested_get(product, ["url", "skuSpecificUrl"])
            or self.nested_get(product, ["url", "pdp"])
            or self.nested_get(product, ["url", "relativePdp"])
            or product.get("pdpUrl")
        )
        source_product_url = self.normalize_url(source_product_url)
        product_name = self.nested_get(product, ["name", "short"]) or self.nested_get(product, ["name", "title"])
        price_fields = self.listing_price_fields(price)
        fulfillment_fields = self.listing_fulfillment_fields(fulfillment)
        row = {
            "account_name": self.account_name,
            "page_type": self.page_type,
            "retailer_sku_name": product_name,
            "product_name": product_name,
            "offer": None,
            "pick_up_availability": fulfillment_fields.get("pick_up_availability"),
            "fastest_delivery": fulfillment_fields.get("fastest_delivery"),
            "delivery_availability": fulfillment_fields.get("delivery_availability"),
            "sku_status": "Sponsored" if is_sponsored else None,
            "main_rank": 0,
            "page_number": page_number,
            "product_url": self.legacy_product_url(sku),
            "source_product_url": source_product_url,
            "numeric_sku": sku,
            "sku_id": sku,
            "bsin": product.get("bsin"),
            "calendar_week": self.calendar_week,
            "crawl_datetime": (datetime.now() + timedelta(hours=self.time_offset_hours)).strftime('%Y-%m-%d %H:%M:%S'),
            "batch_id": self.batch_id,
            "final_sku_price": price_fields.get("final_sku_price"),
            "original_sku_price": price_fields.get("original_sku_price"),
            "savings": price_fields.get("savings"),
            "offer": self.offer_summary(product),
        }
        if extra:
            row.update(extra)
        return row

    def listing_fulfillment_fields(self, fulfillment):
        if not isinstance(fulfillment, dict) or not fulfillment:
            return {
                "pick_up_availability": None,
                "fastest_delivery": None,
                "delivery_availability": None,
            }
        return {
            "pick_up_availability": self.pickup_display(fulfillment),
            "fastest_delivery": self.shipping_display(fulfillment),
            "delivery_availability": self.delivery_display(fulfillment),
        }

    def pickup_display(self, fulfillment):
        availability = (
            self.nested_get(fulfillment, ["ispuDetails", "ispuAvailability"])
            or self.nested_get(fulfillment, ["ispuDetails", "nearbyLocation", "availability"])
            or {}
        )
        if not isinstance(availability, dict):
            return None
        pickup_eligible = availability.get("pickupEligible")
        if pickup_eligible is False:
            return "Pick up unavailable"
        if pickup_eligible is not True:
            return None

        min_hours = availability.get("minPickupInHours")
        try:
            if min_hours is not None and float(min_hours) <= 1:
                return "Pick up in 1 hour"
        except Exception:
            pass
        pickup_date = self.bestbuy_display_date(availability.get("fulfillDate") or availability.get("maxDate"))
        return f"Pick up {pickup_date}" if pickup_date else "Pick up available"

    def shipping_display(self, fulfillment):
        availability = self.nested_get(fulfillment, ["shippingDetails", "shippingAvailability"], {})
        if not isinstance(availability, dict):
            return None
        if availability.get("shippingEligible") is False:
            return "Shipping unavailable"

        los = availability.get("customerLOSGroup") if isinstance(availability.get("customerLOSGroup"), dict) else {}
        for key in ("name", "displayName"):
            value = los.get(key)
            if value:
                return str(value)
        ship_date = self.bestbuy_display_date(los.get("maxLineItemMaxDate") or los.get("minLineItemMaxDate"))
        if ship_date:
            return f"Get it by {ship_date}"
        if availability.get("shippingEligible") is True:
            return "Shipping available"
        return None

    def delivery_display(self, fulfillment):
        availability = self.nested_get(fulfillment, ["deliveryDetails", "deliveryAvailability"], {})
        if not isinstance(availability, dict):
            return None
        delivery_eligible = availability.get("deliveryEligible")
        deliverable = availability.get("deliverable")
        if delivery_eligible is False or deliverable is False:
            return "Delivery unavailable"

        slots = availability.get("deliverySlots") if isinstance(availability.get("deliverySlots"), list) else []
        slot_date = None
        for slot in slots:
            if isinstance(slot, dict) and slot.get("date"):
                slot_date = slot.get("date")
                break
        delivery_date = self.bestbuy_display_date(slot_date)
        if delivery_date:
            return f"Delivery as soon as {delivery_date}"
        if delivery_eligible is True or deliverable is True:
            return "Delivery available"
        return None

    def bestbuy_display_date(self, value):
        if value in (None, ""):
            return None
        text = str(value).strip()
        try:
            parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
            return parsed.strftime("%a, %b ") + str(parsed.day)
        except Exception:
            return text

    def numeric_price_value(self, value):
        if value in (None, ""):
            return None
        text = re.sub(r"[^0-9.\-]", "", str(value))
        if not text:
            return None
        try:
            return float(text)
        except Exception:
            return None

    def listing_price_fields(self, price):
        restricted_marker = self.restricted_price_marker(
            price.get("restrictedPriceDisplayMessage") or price.get("priceWithCart")
        )
        if restricted_marker:
            return {
                "final_sku_price": restricted_marker,
                "original_sku_price": None,
                "savings": None,
            }

        final_price = (
            price.get("displayableCustomerPrice")
            or price.get("customerPrice")
            or price.get("currentPrice")
        )
        original_price = price.get("displayableRegularPrice") or price.get("regularPrice")
        savings = price.get("totalSavings")

        final_value = self.numeric_price_value(final_price)
        original_value = self.numeric_price_value(original_price)
        savings_value = self.numeric_price_value(savings)

        if final_value is None:
            return {"final_sku_price": None, "original_sku_price": None, "savings": None}
        if original_value is None or original_value <= final_value:
            return {"final_sku_price": final_price, "original_sku_price": None, "savings": None}
        if savings_value is None or savings_value <= 0:
            return {"final_sku_price": final_price, "original_sku_price": None, "savings": None}
        if abs((original_value - final_value) - savings_value) > 1.0:
            return {"final_sku_price": final_price, "original_sku_price": None, "savings": None}
        return {
            "final_sku_price": final_price,
            "original_sku_price": original_price,
            "savings": savings,
        }

    def restricted_price_marker(self, value):
        text = json.dumps(value, ensure_ascii=False) if isinstance(value, (dict, list)) else str(value or "")
        lowered = text.lower()
        if "see price in cart" in lowered:
            return "See price in cart"
        if "see details in checkout" in lowered:
            return "See details in checkout"
        return None

    def offer_summary(self, product):
        """Return the visible '+ N offers' count only when the payload exposes it."""
        for text in self.iter_strings(product):
            match = re.search(r"\+\s*(\d+)\s+offers?\b", text, re.IGNORECASE)
            if match:
                return match.group(1)
        return None

    def iter_strings(self, value):
        if isinstance(value, str):
            yield value
        elif isinstance(value, dict):
            for child in value.values():
                yield from self.iter_strings(child)
        elif isinstance(value, list):
            for child in value:
                yield from self.iter_strings(child)

    def parse_page_rows(self, page_number, response_json):
        data = response_json.get("data", {}) if isinstance(response_json, dict) else {}
        rows = []
        visual_rank = 0

        def add_product(product, extra=None):
            nonlocal visual_rank
            visual_rank += 1
            metadata = {"visual_rank": visual_rank}
            if extra:
                metadata.update(extra)
            row = self.parse_graphql_product(product, page_number, metadata)
            if not row:
                visual_rank -= 1
                return
            rows.append(row)

        documents = self.nested_get(data, ["detailedProductSearch", "documents"], [])
        if isinstance(documents, list):
            for organic_rank, document in enumerate(documents, 1):
                product = document.get("product") if isinstance(document, dict) else None
                add_product(product, {
                    "container_type": "organic_product",
                    "organic_rank": organic_rank,
                    "global_organic_rank": (page_number - 1) * ORGANIC_OFFSET + organic_rank,
                    "is_sponsored": False,
                    "placement": "detailedProductSearch.documents",
                })

        placements = self.nested_get(data, ["search", "withBestMedia", "placements"], [])
        if isinstance(placements, list):
            for placement_index, placement in enumerate(placements):
                if not isinstance(placement, dict):
                    continue
                placement_name = placement.get("name", "")
                sponsored = self.nested_get(placement, ["documentsGridView", "sponsoredDocuments"], [])
                if isinstance(sponsored, list):
                    for sponsored_rank, document in enumerate(sponsored, 1):
                        product = document.get("product") if isinstance(document, dict) else None
                        add_product(product, {
                            "container_type": "sponsored_ingrid",
                            "is_sponsored": True,
                            "placement": placement_name or "SEARCH_SPONSORED_INGRID",
                            "placement_name": placement_name,
                            "placement_index": placement_index,
                            "sponsored_rank": sponsored_rank,
                            "source_doc_index": sponsored_rank,
                        })
                carousel = placement.get("documents", [])
                if isinstance(carousel, list):
                    for sponsored_rank, document in enumerate(carousel, 1):
                        product = document.get("product") if isinstance(document, dict) else None
                        add_product(product, {
                            "container_type": "sponsored_carousel",
                            "is_sponsored": True,
                            "placement": placement_name or "SEARCH_SPONSORED_CAROUSEL_DEFAULT",
                            "placement_name": placement_name,
                            "placement_index": placement_index,
                            "sponsored_rank": sponsored_rank,
                            "source_doc_index": sponsored_rank,
                        })
        for row in rows:
            row["global_visual_rank"] = (page_number - 1) * 1000 + int(row.get("visual_rank") or 0)
        return rows

    def page_summary(self, page_number, rows, meta, response_json, source="network"):
        errors = response_json.get("errors", []) if isinstance(response_json, dict) else []
        organic = [row for row in rows if row.get("container_type") == "organic_product"]
        ingrid = [row for row in rows if row.get("container_type") == "sponsored_ingrid"]
        carousel = [row for row in rows if row.get("container_type") == "sponsored_carousel"]
        return {
            "page": page_number,
            "source": source,
            "started_at": meta.get("started_at", ""),
            "finished_at": meta.get("finished_at", ""),
            "elapsed_seconds": meta.get("elapsed_seconds", ""),
            "status_code": meta.get("status_code", ""),
            "x_request_cost": meta.get("x_request_cost", ""),
            "bytes": meta.get("bytes", ""),
            "error_count": len(errors),
            "organic_count": len(organic),
            "sponsored_ingrid_count": len(ingrid),
            "sponsored_carousel_count": len(carousel),
            "total_occurrence_count": len(rows),
            "unique_sku_count": len({row.get("numeric_sku") for row in rows if row.get("numeric_sku")}),
            "organic_price_missing": sum(1 for row in organic if row.get("final_sku_price") in ("", None)),
            "sponsored_price_missing": sum(1 for row in ingrid + carousel if row.get("final_sku_price") in ("", None)),
            "fulfillment_error_count": self.fulfillment_error_count(response_json),
            "pickup_count": sum(1 for row in rows if row.get("pick_up_availability")),
            "fastest_delivery_count": sum(1 for row in rows if row.get("fastest_delivery")),
            "delivery_count": sum(1 for row in rows if row.get("delivery_availability")),
            "response_path": meta.get("response_json_path") or meta.get("response_path", ""),
        }

    def fulfillment_error_count(self, response_json):
        errors = response_json.get("errors", []) if isinstance(response_json, dict) else []
        count = 0
        for error in errors:
            path = error.get("path") if isinstance(error, dict) else None
            if isinstance(path, list) and "fulfillmentOptions" in path:
                count += 1
        return count

    def fulfillment_value_count(self, rows):
        return sum(
            1
            for row in rows
            if row.get("pick_up_availability") or row.get("fastest_delivery") or row.get("delivery_availability")
        )

    def should_refuse_fulfillment_page(self, page_number, products, response_json):
        if not REQUIRE_FULFILLMENT or not products:
            return False
        error_count = self.fulfillment_error_count(response_json)
        if error_count:
            print(
                f"[ERROR] Page {page_number}: fulfillmentOptions GraphQL errors={error_count}; "
                "refusing incomplete listing save"
            )
            return True
        value_count = self.fulfillment_value_count(products)
        if value_count == 0:
            print(
                f"[ERROR] Page {page_number}: no pickup/shipping/delivery values parsed; "
                "refusing incomplete listing save"
            )
            return True
        return False

    def crawl_page(self, page_number):
        """GraphQL replay로 main listing 제품을 수집한다."""
        try:
            referer_url = self.build_referer_url(page_number)
            payload = self.prepare_graphql_payload(page_number, referer_url)
            response, meta = self.post_graphql(payload, page_number, referer_url)
            if response.status_code != 200:
                print(f"[ERROR] Page {page_number}: ZenRows GraphQL HTTP {response.status_code}: {(response.text or '')[:300]}")
                self.page_benchmarks.append(self.page_summary(page_number, [], meta, {}, source="network"))
                self.raw_search.append({"page": page_number, "url": referer_url, "meta": meta, "summary": self.page_benchmarks[-1]})
                return []
            response_json = response.json()
            products = self.parse_page_rows(page_number, response_json)
            if (
                products
                and self.fulfillment_error_count(response_json)
                and RETRY_FULFILLMENT_WITH_COOKIES
                and not self.operation_cookie_header()
                and self.operation_cookie_header(force=True)
            ):
                print(
                    f"[WARNING] Page {page_number}: fulfillmentOptions failed; "
                    "retrying once with captured operation cookies"
                )
                retry_response, retry_meta = self.post_graphql(
                    payload,
                    page_number,
                    referer_url,
                    use_operation_cookies=True,
                )
                if retry_response.status_code == 200:
                    retry_json = retry_response.json()
                    retry_products = self.parse_page_rows(page_number, retry_json)
                    if self.fulfillment_error_count(retry_json) < self.fulfillment_error_count(response_json):
                        response_json = retry_json
                        products = retry_products
                        meta = retry_meta

            summary = self.page_summary(page_number, products, meta, response_json, source="network")
            self.page_benchmarks.append(summary)
            self.raw_search.append({"page": page_number, "url": referer_url, "meta": meta, "summary": summary})
            if self.should_refuse_fulfillment_page(page_number, products, response_json):
                return []
            if products:
                print(f"[INFO] Page {page_number}: GraphQL rows parsed: {len(products)}")
            else:
                print(f"[WARNING] Page {page_number}: GraphQL response parsed 0 rows")
            return products

        except Exception as e:
            print(f"[ERROR] Page {page_number} failed: {e}")
            traceback.print_exc()
            return []

    def save_products(self, products):
        """V2 저장: DB INSERT 없이 dt1 입력용 CSV에만 저장한다."""
        if not products:
            return 0

        # 수집 갯수 통계
        self.stats['collected'] += len(products)

        # 중복 제거, is_product 체크 및 rank 할당
        unique_products = []
        page_valid_items = 0
        for idx, product in enumerate(products):
            retailer_sku_name = product.get('retailer_sku_name') or ''

            # URL에서 item 추출 (dt.py와 동일한 로직)
            product_url = product.get('product_url')
            item = self.extract_item_from_url(product_url)

            # openbox URL 제외
            if product_url and 'openbox' in product_url.lower():
                print(f"[SKIP] Open Box 상품 제외: {product_url}")
                self.stats['openbox_filtered'] += 1
                continue

            # is_product=FALSE 체크 (비제품 제외)
            if self.is_product_excluded(item):
                print(f"[SKIP] 비제품(is_product=FALSE): {retailer_sku_name[:40] if retailer_sku_name else 'N/A'}...")
                self.stats['non_product'] += 1
                continue

            if item:
                page_valid_items += 1

            # 중복 item 필터링
            if item and item in self.saved_urls:
                print(f"[SKIP] 중복 item={item}: {retailer_sku_name[:40] if retailer_sku_name else 'N/A'}... url={product_url}")
                self.stats['duplicates'] += 1
                continue

            if item:
                self.saved_urls.add(item)

            # rank 할당 (중복 제거된 제품에만 순차적으로)
            self.current_rank += 1
            target = self.test_count if self.test_mode else self.max_products
            if self.current_rank > target:
                self.stats['skipped_by_target'] += len(products) - idx
                break
            product['main_rank'] = self.current_rank
            unique_products.append(product)

        if not unique_products:
            return 0

        fieldnames = [
            'account_name', 'batch_id', 'page_type', 'main_rank', 'retailer_sku_name',
            'offer', 'pick_up_availability', 'shipping_availability',
            'delivery_availability', 'sku_status', 'product_url', 'numeric_sku',
            'sku_id', 'bsin', 'source_product_url',
            'final_sku_price', 'original_sku_price', 'savings',
            'crawl_datetime', 'calendar_week', 'page_number', 'fastest_delivery'
        ]

        try:
            file_exists = os.path.exists(self.csv_output_path)
            with open(self.csv_output_path, 'a', newline='', encoding='utf-8-sig') as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                if not file_exists or os.path.getsize(self.csv_output_path) == 0:
                    writer.writeheader()
                for product in unique_products:
                    row = {field: product.get(field) for field in fieldnames}
                    row['shipping_availability'] = product.get('fastest_delivery')
                    writer.writerow(row)

            total_saved = len(unique_products)
            self.stats['inserted'] += total_saved
            print(f"[CSV] Saved {total_saved} rows to {self.csv_output_path}")
            return total_saved

        except Exception as e:
            print(f"[ERROR] Failed to save products to CSV: {e}")
            traceback.print_exc()
            return 0

    def save_raw_target_products(self, products):
        """GraphQL에서 받은 target list를 필터링 전 원본 형태로 별도 CSV 저장."""
        if not products:
            return 0

        fieldnames = [
            'account_name', 'batch_id', 'page_type', 'page_number',
            'visual_rank', 'global_visual_rank', 'organic_rank', 'global_organic_rank',
            'container_type', 'is_sponsored', 'placement', 'placement_name',
            'placement_index', 'sponsored_rank', 'source_doc_index',
            'sku_id', 'numeric_sku', 'bsin',
            'retailer_sku_name', 'product_name', 'offer',
            'product_url', 'source_product_url',
            'sku_status', 'final_sku_price', 'original_sku_price', 'savings',
            'pick_up_availability', 'shipping_availability',
            'delivery_availability', 'fastest_delivery',
            'crawl_datetime', 'calendar_week'
        ]

        try:
            file_exists = os.path.exists(self.raw_target_output_path)
            with open(self.raw_target_output_path, 'a', newline='', encoding='utf-8-sig') as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames, extrasaction="ignore")
                if not file_exists or os.path.getsize(self.raw_target_output_path) == 0:
                    writer.writeheader()
                for product in products:
                    row = {field: product.get(field) for field in fieldnames}
                    row['shipping_availability'] = product.get('fastest_delivery')
                    writer.writerow(row)
            print(f"[RAW CSV] Saved {len(products)} rows to {self.raw_target_output_path}")
            return len(products)
        except Exception as e:
            print(f"[ERROR] Failed to save raw target CSV: {e}")
            traceback.print_exc()
            return 0

    def write_rows_csv(self, path, rows):
        if not rows:
            return
        keys = []
        seen = set()
        for row in rows:
            for key in row:
                if key not in seen:
                    seen.add(key)
                    keys.append(key)
        with open(path, "w", newline="", encoding="utf-8-sig") as f:
            writer = csv.DictWriter(f, fieldnames=keys, extrasaction="ignore")
            writer.writeheader()
            writer.writerows(rows)

    def write_benchmark_outputs(self, total_products):
        run_elapsed = round(time.perf_counter() - self.run_start_perf, 3) if self.run_start_perf else None
        self.write_rows_csv(self.page_benchmarks_path, self.page_benchmarks)
        with open(self.page_summary_path, "w", encoding="utf-8") as f:
            json.dump(self.page_benchmarks, f, ensure_ascii=False, indent=2)
        with open(self.raw_search_summary_path, "w", encoding="utf-8") as f:
            json.dump(self.raw_search, f, ensure_ascii=False, indent=2)

        total_cost = 0.0
        for summary in self.page_benchmarks:
            try:
                total_cost += float(summary.get("x_request_cost") or 0)
            except (TypeError, ValueError):
                pass

        manifest = {
            "run_type": "step01_main_list",
            "run_root": self.main_data_dir,
            "run_started_at": self.run_started_at,
            "run_finished_at": datetime.now().isoformat(timespec="seconds"),
            "elapsed_seconds": run_elapsed,
            "search_term": SEARCH_TERM,
            "search_sort": SEARCH_SORT,
            "organic_offset": ORGANIC_OFFSET,
            "graphql_endpoint": self.graphql_endpoint,
            "actual_post_calls": len(self.page_benchmarks),
            "total_x_request_cost": round(total_cost, 7),
            "main_saved_rows": total_products,
            "main_occurrences": sum(int(row.get("total_occurrence_count") or 0) for row in self.page_benchmarks),
            "unique_skus": sum(int(row.get("unique_sku_count") or 0) for row in self.page_benchmarks),
            "organic_occurrences": sum(int(row.get("organic_count") or 0) for row in self.page_benchmarks),
            "sponsored_ingrid_occurrences": sum(int(row.get("sponsored_ingrid_count") or 0) for row in self.page_benchmarks),
            "sponsored_carousel_occurrences": sum(int(row.get("sponsored_carousel_count") or 0) for row in self.page_benchmarks),
            "outputs": {
                "main_csv": self.csv_output_path,
                "raw_target_csv": self.raw_target_output_path,
                "page_benchmarks": self.page_benchmarks_path,
                "main_page_summary": self.page_summary_path,
                "raw_search_summary": self.raw_search_summary_path,
            },
        }
        with open(self.manifest_path, "w", encoding="utf-8") as f:
            json.dump(manifest, f, ensure_ascii=False, indent=2)
        print(f"[BENCHMARK] page_benchmarks={self.page_benchmarks_path}")
        print(f"[BENCHMARK] main_page_summary={self.page_summary_path}")
        print(f"[BENCHMARK] manifest={self.manifest_path}")

    def run(self):
        """실행: initialize() → 페이지별 crawl_page() → save_products() → 리소스 정리"""
        try:
            if not self.initialize():
                print("[ERROR] Initialization failed")
                return False

            self.reset_output_files()
            self.run_started_at = datetime.now().isoformat(timespec="seconds")
            self.run_start_perf = time.perf_counter()
            total_products = 0
            target_products = self.test_count if self.test_mode else self.max_products
            self.current_rank = 0
            page_num = 1

            while total_products < target_products and page_num <= self.max_pages:
                products = self.crawl_page(page_num)

                if not products:
                    if page_num > 1:
                        break
                    print(f"[ERROR] No products found at page {page_num}")
                else:
                    self.save_raw_target_products(products)

                    null_url_count = sum(1 for p in products if not p.get('product_url'))
                    if null_url_count >= 3:
                        print(f"[WARNING] Page {page_num}: product_url NULL {null_url_count}/{len(products)}")

                    saved_count = self.save_products(products)
                    total_products += saved_count

                    if total_products >= target_products:
                        break

                time.sleep(random.uniform(5, 8))
                page_num += 1

            if page_num > self.max_pages:
                print(f"[INFO] Max pages ({self.max_pages}) reached")

            print(f"[DONE] Page: {page_num}, Saved: {total_products}, batch_id: {self.batch_id}")
            self.write_benchmark_outputs(total_products)
            return True

        except Exception as e:
            print(f"[ERROR] Crawler failed: {e}")
            traceback.print_exc()
            return False

        finally:
            # 통계 출력
            print(f"\n{'='*50}")
            print(f"[통계] 수집: {self.stats['collected']}, 중복제거: {self.stats['duplicates']}, OpenBox: {self.stats['openbox_filtered']}, 비제품: {self.stats['non_product']}, INSERT: {self.stats['inserted']}")
            if self.stats.get('skipped_by_target', 0) > 0:
                print(f"  미검사: {self.stats['skipped_by_target']} (target 도달 후 나머지)")
            print(f"{'='*50}")

            if self.db_conn:
                self.db_conn.close()


import argparse

def main():
    """개별 실행 진입점 (테스트 모드)"""
    parser = argparse.ArgumentParser(description='BestBuy TV Main Crawler')
    parser.add_argument('--time_offset', type=int, default=0, help='시간 오프셋 (기본값: 0)')
    args = parser.parse_args()

    test_mode = os.environ.get('BBY_TEST_MODE', '0') == '1'
    crawler = BestBuyTVMainCrawler(test_mode=test_mode, time_offset_hours=args.time_offset)
    crawler.run()

if __name__ == '__main__':
    main()


