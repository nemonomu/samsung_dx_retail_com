"""
BestBuy TV CSV-only v2 orchestrator.

DB is used only for crawler configuration such as XPath and page URLs.
Listing and detail results are written to CSV files, not DB tables.
"""

import argparse
import csv
import importlib.util
import os
import random
import shutil
import sys
import tempfile
import time
import traceback
from datetime import datetime, timedelta

import pytz
from DrissionPage import ChromiumOptions, ChromiumPage

CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
REPO_DIR = os.path.dirname(CURRENT_DIR)
if CURRENT_DIR in sys.path:
    sys.path.remove(CURRENT_DIR)
sys.path.insert(0, CURRENT_DIR)

from common.setup import setup_environment
setup_environment(__file__)
if CURRENT_DIR in sys.path:
    sys.path.remove(CURRENT_DIR)
sys.path.insert(0, CURRENT_DIR)
if REPO_DIR not in sys.path:
    sys.path.insert(1, REPO_DIR)

from common.base_crawler import BaseCrawler


def load_local_class(module_name, class_name):
    module_path = os.path.join(CURRENT_DIR, f"{module_name}.py")
    if not os.path.exists(module_path):
        raise FileNotFoundError(f"Required local module not found: {module_path}")
    spec = importlib.util.spec_from_file_location(f"bby_local_{module_name}", module_path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return getattr(module, class_name)


BestBuyTVBSRCrawler = load_local_class("bby_tv_bsr", "BestBuyTVBSRCrawler")
BestBuyTVDetailCrawler = load_local_class("bby_tv_dt", "BestBuyTVDetailCrawler")
BestBuyTVMainCrawler = load_local_class("bby_tv_main", "BestBuyTVMainCrawler")
BestBuyTVPromotionCrawler = load_local_class("bby_tv_pmt", "BestBuyTVPromotionCrawler")
BestBuyTVTrendCrawler = load_local_class("bby_tv_trend", "BestBuyTVTrendCrawler")


RESUME_STAGES = ["main", "bsr", "pmt", "trend", "detail"]
DEFAULT_OUTPUT_DIR = r"C:\samsung_dx_retail_com\bby_vpn"

LISTING_FIELDS = [
    "account_name", "batch_id", "page_type", "main_rank", "bsr_rank", "trend_rank",
    "promotion_position", "promotion_type", "retailer_sku_name", "offer",
    "pick_up_availability", "fastest_delivery", "delivery_availability", "sku_status",
    "product_url", "calendar_week", "crawl_datetime",
]

DETAIL_FIELDS = [
    "account_name", "batch_id", "page_type", "order", "retailer_sku_name", "item",
    "sku", "product_url", "crawl_datetime", "calendar_week", "star_rating",
    "count_of_reviews", "count_of_star_ratings", "screen_size",
    "estimated_annual_electricity_use", "final_sku_price", "original_sku_price",
    "savings", "offer", "pick_up_availability", "fastest_delivery",
    "delivery_availability", "sku_status", "recommendation_intent",
    "detailed_review_content", "retailer_sku_name_similar", "main_rank", "bsr_rank",
    "trend_rank", "promotion_position", "promotion_type", "model_year",
]


class CsvProductStore:
    def __init__(self, listing_csv):
        self.listing_csv = listing_csv
        self.rows = []
        self.item_index = {}
        os.makedirs(os.path.dirname(self.listing_csv), exist_ok=True)
        self.load()

    @staticmethod
    def extract_item_from_url(product_url):
        if not product_url:
            return None
        cleaned = product_url.split("?")[0]
        cleaned = cleaned.replace("/openbox", "")
        if "/sku/" in cleaned:
            cleaned = cleaned.split("/sku/")[0].rstrip("/")
        item = cleaned.rstrip("/").split("/")[-1]
        return item[:-2] if item.endswith(".p") else item

    def load(self):
        if not os.path.exists(self.listing_csv):
            return
        with open(self.listing_csv, "r", newline="", encoding="utf-8-sig") as f:
            reader = csv.DictReader(f)
            for row in reader:
                self.rows.append(row)
                item = self.extract_item_from_url(row.get("product_url"))
                if item and item not in self.item_index:
                    self.item_index[item] = len(self.rows) - 1

    def item_to_url(self):
        return {
            item: self.rows[idx].get("product_url")
            for item, idx in self.item_index.items()
            if self.rows[idx].get("product_url")
        }

    def upsert(self, product):
        item = self.extract_item_from_url(product.get("product_url"))
        if not item:
            return False

        row = {field: "" for field in LISTING_FIELDS}
        for field in LISTING_FIELDS:
            value = product.get(field)
            if value is not None:
                row[field] = value

        if item in self.item_index:
            existing = self.rows[self.item_index[item]]
            for field, value in row.items():
                if field in ("page_type", "product_url"):
                    continue
                if value not in (None, ""):
                    existing[field] = value
        else:
            self.item_index[item] = len(self.rows)
            self.rows.append(row)
        self.flush()
        return True

    def flush(self):
        with open(self.listing_csv, "w", newline="", encoding="utf-8-sig") as f:
            writer = csv.DictWriter(f, fieldnames=LISTING_FIELDS)
            writer.writeheader()
            writer.writerows(self.rows)

    def product_list(self):
        products = []
        for row in self.rows:
            if not row.get("product_url"):
                continue
            item = self.extract_item_from_url(row.get("product_url"))
            product = dict(row)
            product["item"] = item
            products.append(product)
        return products


class CsvListingMixin:
    def attach_csv_store(self, csv_store):
        self.csv_store = csv_store

    def build_db_url_cache(self):
        cache = self.csv_store.item_to_url() if getattr(self, "csv_store", None) else {}
        print(f"[INFO] CSV URL cache loaded: {len(cache)} items")
        return cache

    def is_product_excluded(self, item):
        return False

    def _skip_product(self, product):
        product_url = product.get("product_url")
        if product_url and "openbox" in product_url.lower():
            self.stats["openbox_filtered"] = self.stats.get("openbox_filtered", 0) + 1
            print(f"[SKIP] Open Box: {product_url}")
            return True
        return False


class ReviewSkipLogFilter:
    def __init__(self, wrapped):
        self.wrapped = wrapped
        self.buffer = ""

    def write(self, text):
        self.buffer += text
        while "\n" in self.buffer:
            line, self.buffer = self.buffer.split("\n", 1)
            if not self._should_suppress(line):
                self.wrapped.write(line + "\n")

    def flush(self):
        if self.buffer:
            if not self._should_suppress(self.buffer):
                self.wrapped.write(self.buffer)
            self.buffer = ""
        self.wrapped.flush()

    def _should_suppress(self, line):
        return "reviews_button" in line and ("DB" in line or "XPath" in line or "7" in line)


class BestBuyTVMainCsvCrawler(CsvListingMixin, BestBuyTVMainCrawler):
    def save_products(self, products):
        if not products:
            return 0
        self.stats["collected"] += len(products)
        saved = 0
        for idx, product in enumerate(products):
            item = self.extract_item_from_url(product.get("product_url"))
            if self._skip_product(product):
                continue
            if item and item in self.saved_urls:
                self.stats["duplicates"] += 1
                continue
            if item:
                self.saved_urls.add(item)
            self.current_rank += 1
            target = self.test_count if self.test_mode else self.max_products
            if self.current_rank > target:
                self.stats["skipped_by_target"] += len(products) - idx
                break
            product["main_rank"] = self.current_rank
            if self.csv_store.upsert(product):
                saved += 1
        self.stats["inserted"] += saved
        return saved


class BestBuyTVBSRCsvCrawler(CsvListingMixin, BestBuyTVBSRCrawler):
    def save_products(self, products):
        if not products:
            return {"insert": 0, "update": 0}
        self.stats["collected"] += len(products)
        saved = 0
        updated = 0
        for idx, product in enumerate(products):
            item = self.extract_item_from_url(product.get("product_url"))
            if self._skip_product(product):
                continue
            if item in self.crawled_urls:
                self.stats["duplicates"] += 1
                continue
            self.crawled_urls.add(item)
            self.current_rank += 1
            target = self.test_count if self.test_mode else self.max_products
            if self.current_rank > target:
                self.stats["skipped_by_target"] += len(products) - idx
                break
            product["bsr_rank"] = self.current_rank
            existed = item in self.csv_store.item_index
            if self.csv_store.upsert(product):
                updated += 1 if existed else 0
                saved += 0 if existed else 1
        self.stats["updated"] += updated
        self.stats["inserted"] += saved
        return {"insert": saved, "update": updated}


class BestBuyTVPromotionCsvCrawler(CsvListingMixin, BestBuyTVPromotionCrawler):
    def save_products(self, products):
        if not products:
            return {"insert": 0, "update": 0}
        inserted = 0
        updated = 0
        for product in products:
            item = self.extract_item_from_url(product.get("product_url"))
            ptype = product.get("promotion_type") or "Unknown"
            self.stats_by_type.setdefault(ptype, {"collected": 0, "updated": 0, "inserted": 0, "skipped": 0})
            self.stats_by_type[ptype]["collected"] += 1
            if self._skip_product(product):
                continue
            existed = item in self.csv_store.item_index
            if self.csv_store.upsert(product):
                if existed:
                    updated += 1
                    self.stats_by_type[ptype]["updated"] += 1
                else:
                    inserted += 1
                    self.stats_by_type[ptype]["inserted"] += 1
        return {"insert": inserted, "update": updated}


class BestBuyTVTrendCsvCrawler(CsvListingMixin, BestBuyTVTrendCrawler):
    def save_products(self, products):
        if not products:
            return {"insert": 0, "update": 0}
        self.stats["collected"] += len(products)
        inserted = 0
        updated = 0
        for product in products:
            item = self.extract_item_from_url(product.get("product_url"))
            if self._skip_product(product):
                continue
            if item in self.crawled_urls:
                self.stats["duplicates"] += 1
                continue
            self.crawled_urls.add(item)
            existed = item in self.csv_store.item_index
            if self.csv_store.upsert(product):
                updated += 1 if existed else 0
                inserted += 0 if existed else 1
        self.stats["updated"] += updated
        self.stats["inserted"] += inserted
        return {"insert": inserted, "update": updated}


class BestBuyTVDetailCsvCrawler(BestBuyTVDetailCrawler):
    def __init__(
        self, batch_id, csv_store, detail_csv, time_offset_hours=0, chunk_size=None,
        chunk_min=5, chunk_max=10,
        cooldown_min=60, cooldown_max=180,
        block_cooldown_min=900, block_cooldown_max=1800,
        skip_reviews=True, skip_similar=True,
        deadline=None, start_order=None, end_order=None,
        target_count=None, stable_mode=False, profile_dir=None,
    ):
        super().__init__(batch_id=batch_id, test_mode=False, time_offset_hours=time_offset_hours)
        self.csv_store = csv_store
        self.detail_csv = detail_csv
        self.chunk_size = chunk_size
        self.chunk_min = chunk_min
        self.chunk_max = chunk_max
        self.cooldown_min = cooldown_min
        self.cooldown_max = cooldown_max
        self.block_cooldown_min = block_cooldown_min
        self.block_cooldown_max = block_cooldown_max
        self.skip_reviews = skip_reviews
        self.skip_similar = skip_similar
        self.deadline = deadline
        self.start_order = start_order
        self.end_order = end_order
        self.target_count = target_count
        self.stable_mode = stable_mode
        self.profile_dir = profile_dir
        self.pending_csv = os.path.splitext(self.detail_csv)[0] + "_pending.csv"
        self.profile_dirs = []
        self.risk_chunks_remaining = 0
        self.items_until_cooldown = self._next_chunk_size()
        self.standalone = False
        os.makedirs(os.path.dirname(self.detail_csv), exist_ok=True)

    def _next_chunk_size(self):
        if self.chunk_size:
            return self.chunk_size
        if self.risk_chunks_remaining > 0:
            self.risk_chunks_remaining -= 1
            return random.randint(2, 5)
        low = max(int(self.chunk_min or 5), 1)
        high = max(int(self.chunk_max or low), low)
        return random.randint(low, high)

    def setup_drission_driver(self):
        profile_root = os.path.join(os.path.dirname(self.detail_csv), "chrome_profiles")
        os.makedirs(profile_root, exist_ok=True)
        if self.stable_mode:
            user_data_path = self.profile_dir or os.path.join(profile_root, "stable_default")
            os.makedirs(user_data_path, exist_ok=True)
        else:
            user_data_path = tempfile.mkdtemp(prefix="bby_dp_profile_", dir=profile_root)
            self.profile_dirs.append(user_data_path)
        cache_path = os.path.join(user_data_path, "cache")
        os.makedirs(cache_path, exist_ok=True)

        opts = ChromiumOptions()
        opts.set_user_data_path(user_data_path)
        opts.set_cache_path(cache_path)
        opts.set_argument("--disable-blink-features=AutomationControlled")
        opts.set_argument("--disable-features=IsolateOrigins,site-per-process")
        opts.set_argument("--no-first-run")
        opts.set_argument("--no-default-browser-check")
        opts.set_argument("--disable-dev-shm-usage")
        opts.set_argument("--lang=en-US,en;q=0.9")
        opts.set_argument("--window-size=1366,768")
        opts.set_argument("--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36")
        if not self.stable_mode:
            opts.set_argument("--disable-application-cache")
            opts.set_argument("--disk-cache-size", "1")
        self.page = ChromiumPage(opts)
        self.page.set.headers({
            "Accept-Language": "en-US,en;q=0.9",
            "Upgrade-Insecure-Requests": "1",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
        })
        mode = "stable profile" if self.stable_mode else "fresh profile"
        print(f"[SUCCESS] DrissionPage setup complete with {mode}: {user_data_path}")

    def _cleanup_profile_dirs(self):
        for path in list(self.profile_dirs):
            try:
                if path and os.path.isdir(path):
                    shutil.rmtree(path, ignore_errors=True)
                    print(f"[CLEANUP] Removed Chrome profile: {path}")
            except Exception as e:
                print(f"[WARNING] Chrome profile cleanup failed: {path}: {e}")
        self.profile_dirs.clear()

    def load_product_list(self):
        products = self.csv_store.product_list()
        if self.start_order or self.end_order:
            start_idx = max((self.start_order or 1) - 1, 0)
            end_idx = self.end_order if self.end_order else len(products)
            products = products[start_idx:end_idx]
            print(f"[INFO] Detail order filter applied: start_order={self.start_order or 1}, end_order={self.end_order or 'end'}")
        if self.target_count:
            products = products[:max(self.target_count, 0)]
            print(f"[INFO] Detail target count applied: {len(products)} products")
        print(f"[INFO] Loaded {len(products)} products from CSV listing")
        return products

    def get_item_mst_specs(self, item):
        return None, None

    def upsert_item_mst(self, product):
        return None

    def _write_detail_row(self, product):
        exists = os.path.exists(self.detail_csv)
        row = {field: "" for field in DETAIL_FIELDS}
        row["account_name"] = self.account_name
        row["batch_id"] = self.batch_id
        for field in DETAIL_FIELDS:
            value = product.get(field)
            if value is not None:
                row[field] = value
        with open(self.detail_csv, "a", newline="", encoding="utf-8-sig") as f:
            writer = csv.DictWriter(f, fieldnames=DETAIL_FIELDS)
            if not exists:
                writer.writeheader()
            writer.writerow(row)

    def save_to_retail_com(self, product):
        if not product:
            return False
        self._write_detail_row(product)
        print(f"[CSV] Saved detail row: {product.get('item') or product.get('product_url')}")
        return True

    def _write_pending_row(self, product, actual_order, reason, pass_name):
        exists = os.path.exists(self.pending_csv)
        fields = LISTING_FIELDS + ["item", "order", "reason", "pass_name", "pending_datetime"]
        row = {field: "" for field in fields}
        for field in LISTING_FIELDS:
            value = product.get(field)
            if value is not None:
                row[field] = value
        row["item"] = product.get("item") or self.extract_item_from_url(product.get("product_url"))
        row["order"] = actual_order
        row["reason"] = reason
        row["pass_name"] = pass_name
        row["pending_datetime"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        with open(self.pending_csv, "a", newline="", encoding="utf-8-sig") as f:
            writer = csv.DictWriter(f, fieldnames=fields)
            if not exists:
                writer.writeheader()
            writer.writerow(row)
        print(f"[PENDING] {pass_name}: order={actual_order}, reason={reason}, url={product.get('product_url')}")

    def _warm_up_session(self):
        if not self.stable_mode or not self.page:
            return
        try:
            print("[INFO] Stable mode warm-up: BestBuy home")
            self.page.get("https://www.bestbuy.com/")
            time.sleep(random.uniform(4, 7))
            print("[INFO] Stable mode warm-up: TV search page")
            self.page.get("https://www.bestbuy.com/site/searchpage.jsp?st=tv")
            time.sleep(random.uniform(5, 9))
        except Exception as e:
            print(f"[WARNING] Stable warm-up failed; continuing: {e}")

    def crawl_detail(self, product):
        removed = {}
        original_stdout = None
        if self.skip_reviews:
            print("[INFO] v2 skip_reviews=True - review button click and detailed review extraction skipped")
            for key in (
                "reviews_button", "detailed_review_content",
                "reviewpage_recommendation_intent",
            ):
                if key in self.xpaths:
                    removed[key] = self.xpaths.pop(key)
            original_stdout = sys.stdout
            sys.stdout = ReviewSkipLogFilter(original_stdout)
        if self.skip_similar:
            for key in ("similar_products_container", "similar_product_name"):
                if key in self.xpaths:
                    removed[key] = self.xpaths.pop(key)
        try:
            return super().crawl_detail(product)
        finally:
            if original_stdout:
                sys.stdout.flush()
                sys.stdout = original_stdout
            self.xpaths.update(removed)

    def _looks_incomplete(self, data):
        return not any(data.get(k) for k in ("sku", "screen_size", "final_sku_price", "star_rating"))

    def _restart_after_wait(self, reason, min_seconds, max_seconds):
        wait_seconds = random.randint(min_seconds, max_seconds)
        print(f"[COOLDOWN] {reason}: waiting {wait_seconds // 60}m {wait_seconds % 60}s")
        if self.page:
            try:
                self.page.quit()
            except Exception:
                pass
            self.page = None
        self._cleanup_profile_dirs()
        time.sleep(wait_seconds)
        self.setup_drission_driver()

    def _chunk_cooldown(self, reason):
        self._restart_after_wait(reason, self.cooldown_min, self.cooldown_max)

    def _block_cooldown(self, reason):
        self.risk_chunks_remaining = max(self.risk_chunks_remaining, 2)
        self.items_until_cooldown = self._next_chunk_size()
        self._restart_after_wait(reason, self.block_cooldown_min, self.block_cooldown_max)

    def _process_detail_queue(self, product_list, pass_name, retry_incomplete):
        total_saved = 0
        pending = []
        total_orders = self.end_order or ((self.start_order or 1) + len(product_list) - 1)

        for i, product in enumerate(product_list, 1):
            actual_order = product.get("_actual_order") or ((self.start_order or 1) + i - 1)
            if self.deadline and datetime.now() >= self.deadline:
                print(f"[TIME LIMIT] Detail deadline reached during {pass_name}. Stopping.")
                break

            sku_name = product.get("retailer_sku_name") or "N/A"
            print(f"\n{'=' * 70}")
            print(f"[{actual_order}/{total_orders}] {pass_name}: {sku_name[:60]}")
            print(f"{'=' * 70}")

            combined_data = self.crawl_detail(product)
            if combined_data and self._looks_incomplete(combined_data) and retry_incomplete:
                print("[WARNING] Detail data looks incomplete. Long block cooldown and retrying once.")
                self._block_cooldown("incomplete detail / possible block")
                combined_data = self.crawl_detail(product)

            if not combined_data or self._looks_incomplete(combined_data):
                reason = "no_data" if not combined_data else "incomplete"
                self._write_pending_row(product, actual_order, reason, pass_name)
                item = dict(product)
                item["_actual_order"] = actual_order
                pending.append(item)
                continue

            combined_data["order"] = actual_order
            if self.save_to_retail_com(combined_data):
                total_saved += 1

            self.items_until_cooldown -= 1
            if i < len(product_list) and self.items_until_cooldown <= 0:
                completed_chunk = self._next_chunk_size()
                self.items_until_cooldown = completed_chunk
                self._chunk_cooldown(f"random chunk boundary after order {actual_order}")
            else:
                time.sleep(random.uniform(5, 8))

        return total_saved, pending

    def run(self):
        try:
            if not self.initialize():
                print("[ERROR] Detail CSV initialization failed")
                return False

            product_list = self.load_product_list()
            if not product_list:
                print("[ERROR] No products found in listing CSV")
                return False

            self._warm_up_session()
            total_saved, pending = self._process_detail_queue(
                product_list,
                pass_name="pass1",
                retry_incomplete=not self.stable_mode,
            )

            if self.stable_mode and pending:
                if self.deadline and datetime.now() >= self.deadline:
                    print("[TIME LIMIT] Deadline reached before stable pending retry pass.")
                else:
                    print(f"[INFO] Stable mode pending retry pass queued: {len(pending)} products")
                    self._block_cooldown("stable mode pending retry pass")
                    saved_retry, still_pending = self._process_detail_queue(
                        pending,
                        pass_name="pending_retry",
                        retry_incomplete=False,
                    )
                    total_saved += saved_retry
                    print(f"[INFO] Stable mode pending retry unresolved: {len(still_pending)}")

            print(f"[DONE] Processed: {len(product_list)}, CSV saved: {total_saved}, batch_id: {self.batch_id}")
            return total_saved > 0
        except Exception as e:
            print(f"[ERROR] Detail CSV crawler failed: {e}")
            traceback.print_exc()
            return False
        finally:
            if self.page:
                self.page.quit()
            if self.db_conn:
                self.db_conn.close()
            self._cleanup_profile_dirs()


class BestBuyTVCsvOrchestrator:
    def __init__(
        self, resume_from=None, batch_id=None, time_offset_hours=0,
        output_dir=DEFAULT_OUTPUT_DIR, chunk_size=None, chunk_min=5, chunk_max=10,
        cooldown_min=60, cooldown_max=180,
        block_cooldown_min=900, block_cooldown_max=1800,
        skip_reviews=True, skip_similar=True,
        deadline=None, start_order=None, end_order=None,
        target_count=None, stable_mode=False, profile_dir=None,
    ):
        self.account_name = "Bestbuy"
        self.resume_from = resume_from
        self.time_offset_hours = time_offset_hours
        self.base_crawler = BaseCrawler()
        self.batch_id = batch_id or self.base_crawler.generate_batch_id(
            self.account_name, time_offset_hours=self.time_offset_hours
        )
        self.output_dir = output_dir
        self.chunk_size = chunk_size
        self.chunk_min = chunk_min
        self.chunk_max = chunk_max
        self.cooldown_min = cooldown_min
        self.cooldown_max = cooldown_max
        self.block_cooldown_min = block_cooldown_min
        self.block_cooldown_max = block_cooldown_max
        self.skip_reviews = skip_reviews
        self.skip_similar = skip_similar
        self.deadline = deadline
        self.start_order = start_order
        self.end_order = end_order
        self.target_count = target_count
        self.stable_mode = stable_mode
        self.korea_tz = pytz.timezone("Asia/Seoul")
        self.listing_csv = os.path.join(self.output_dir, f"bby_tv_v2_listing_{self.batch_id}.csv")
        self.detail_csv = os.path.join(self.output_dir, f"bby_tv_v2_detail_{self.batch_id}.csv")
        self.profile_dir = profile_dir or os.path.join(self.output_dir, "chrome_profiles", "stable_default")
        self.csv_store = CsvProductStore(self.listing_csv)

    def _should_run(self, stage):
        if not self.resume_from:
            return True
        return RESUME_STAGES.index(stage) >= RESUME_STAGES.index(self.resume_from)

    def _run_listing_stage(self, stage, crawler_cls):
        if not self._should_run(stage):
            return "skipped"
        crawler = crawler_cls(test_mode=False, batch_id=self.batch_id, time_offset_hours=self.time_offset_hours)
        crawler.attach_csv_store(self.csv_store)
        return crawler.run()

    def run(self):
        start_time = datetime.now(self.korea_tz)
        print("\n" + "=" * 70)
        print("BestBuy TV CSV-only v2 orchestrator")
        print(f"batch_id: {self.batch_id}")
        print(f"listing_csv: {self.listing_csv}")
        print(f"detail_csv: {self.detail_csv}")
        print(f"start_kst: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
        print("=" * 70)

        results = {}
        stages = [
            ("main", BestBuyTVMainCsvCrawler),
            ("bsr", BestBuyTVBSRCsvCrawler),
            ("pmt", BestBuyTVPromotionCsvCrawler),
            ("trend", BestBuyTVTrendCsvCrawler),
        ]
        for stage, crawler_cls in stages:
            if self.deadline and datetime.now() >= self.deadline:
                print("[TIME LIMIT] Deadline reached before listing stage. Stopping.")
                results[stage] = "time_limit"
                continue
            print(f"\n[STAGE] {stage}")
            try:
                results[stage] = self._run_listing_stage(stage, crawler_cls)
            except Exception as e:
                print(f"[ERROR] {stage}: {e}")
                traceback.print_exc()
                results[stage] = False

        if self._should_run("detail"):
            if self.deadline and datetime.now() >= self.deadline:
                print("[TIME LIMIT] Deadline reached before detail stage. Skipping detail.")
                results["detail"] = "time_limit"
                return any(result is True for result in results.values())
            print("\n[STAGE] detail")
            detail = BestBuyTVDetailCsvCrawler(
                batch_id=self.batch_id,
                csv_store=self.csv_store,
                detail_csv=self.detail_csv,
                time_offset_hours=self.time_offset_hours,
                chunk_size=self.chunk_size,
                chunk_min=self.chunk_min,
                chunk_max=self.chunk_max,
                cooldown_min=self.cooldown_min,
                cooldown_max=self.cooldown_max,
                block_cooldown_min=self.block_cooldown_min,
                block_cooldown_max=self.block_cooldown_max,
                skip_reviews=self.skip_reviews,
                skip_similar=self.skip_similar,
                deadline=self.deadline,
                start_order=self.start_order,
                end_order=self.end_order,
                target_count=self.target_count,
                stable_mode=self.stable_mode,
                profile_dir=self.profile_dir,
            )
            results["detail"] = detail.run()
        else:
            results["detail"] = "skipped"

        elapsed = datetime.now(self.korea_tz) - start_time
        print("\n" + "=" * 70)
        print(f"[DONE] elapsed: {elapsed}")
        for stage, result in results.items():
            print(f"  {stage}: {result}")
        print(f"  listing_csv: {self.listing_csv}")
        print(f"  detail_csv: {self.detail_csv}")
        print("=" * 70)
        return any(result is True for result in results.values())


def parse_duration(value):
    if value is None:
        return None
    text = str(value).strip().lower()
    if text.endswith("hours"):
        return int(float(text[:-5].strip()) * 3600)
    if text.endswith("hour"):
        return int(float(text[:-4].strip()) * 3600)
    if text.endswith("h"):
        return int(float(text[:-1].strip()) * 3600)
    return int(float(text) * 3600)


def listing_csv_candidates(output_dir):
    prefix = "bby_tv_v2_listing_"
    suffix = ".csv"
    if not os.path.isdir(output_dir):
        return []
    candidates = []
    for name in os.listdir(output_dir):
        if not (name.startswith(prefix) and name.endswith(suffix)):
            continue
        path = os.path.join(output_dir, name)
        batch_id = name[len(prefix):-len(suffix)]
        try:
            mtime = os.path.getmtime(path)
            with open(path, "r", newline="", encoding="utf-8-sig") as f:
                row_count = sum(1 for _ in csv.DictReader(f))
        except Exception:
            mtime = 0
            row_count = 0
        candidates.append({
            "path": path,
            "name": name,
            "batch_id": batch_id,
            "mtime": mtime,
            "row_count": row_count,
        })
    candidates.sort(key=lambda x: x["mtime"], reverse=True)
    return candidates


def choose_listing_batch_id(output_dir, mode):
    candidates = listing_csv_candidates(output_dir)
    if not candidates:
        raise FileNotFoundError(f"No listing CSV found in {output_dir}")

    if mode == "latest":
        selected = candidates[0]
        print(f"[INFO] Latest listing selected: {selected['name']} ({selected['row_count']} rows)")
        return selected["batch_id"]

    print("\nAvailable listing CSV files:")
    for idx, item in enumerate(candidates[:30], 1):
        modified = datetime.fromtimestamp(item["mtime"]).strftime("%Y-%m-%d %H:%M:%S")
        print(f"  {idx}. {item['name']} | rows={item['row_count']} | modified={modified}")

    while True:
        choice = input("Select listing number: ").strip()
        try:
            choice_idx = int(choice)
            if 1 <= choice_idx <= min(len(candidates), 30):
                selected = candidates[choice_idx - 1]
                print(f"[INFO] Listing selected: {selected['name']} ({selected['row_count']} rows)")
                return selected["batch_id"]
        except Exception:
            pass
        print("Invalid selection. Enter a number from the list.")


def main():
    parser = argparse.ArgumentParser(description="BestBuy TV CSV-only v2 orchestrator")
    parser.add_argument("--resume-from", choices=RESUME_STAGES)
    parser.add_argument("--batch-id")
    parser.add_argument("--time_offset", type=int, default=0)
    parser.add_argument("--output-dir", default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--chunk-size", type=int, help="fixed cooldown interval; overrides --chunk-min/--chunk-max")
    parser.add_argument("--chunk-min", type=int, default=5, help="minimum random detail chunk size")
    parser.add_argument("--chunk-max", type=int, default=10, help="maximum random detail chunk size")
    parser.add_argument("--cooldown-min", type=int, default=60, help="normal chunk cooldown minimum seconds")
    parser.add_argument("--cooldown-max", type=int, default=180, help="normal chunk cooldown maximum seconds")
    parser.add_argument("--block-cooldown-min", type=int, default=900, help="possible block cooldown minimum seconds")
    parser.add_argument("--block-cooldown-max", type=int, default=1800, help="possible block cooldown maximum seconds")
    parser.add_argument("--with-reviews", action="store_true", help="review page actions are skipped by default")
    parser.add_argument("--with-similar", action="store_true", help="similar product actions are skipped by default")
    parser.add_argument("--start-order", type=int, help="detail stage starts from this 1-based listing order")
    parser.add_argument("--end-order", type=int, help="detail stage ends at this 1-based listing order")
    parser.add_argument("--target-count", type=int, help="limit detail stage to this many products after order filtering")
    parser.add_argument("--stable-mode", action="store_true", help="use a persistent profile, warm-up, and deferred retry for detail")
    parser.add_argument("--profile-dir", help="persistent Chrome profile directory for --stable-mode")
    parser.add_argument("--latest-listing", action="store_true", help="use the most recently modified listing CSV")
    parser.add_argument("--select-listing", action="store_true", help="select a listing CSV from a numbered menu")
    parser.add_argument("max_runtime", nargs="*", help='optional duration such as "6 hours" or "6h"')
    args = parser.parse_args()

    if args.latest_listing and args.select_listing:
        parser.error("--latest-listing and --select-listing cannot be used together")

    batch_id = args.batch_id
    if args.latest_listing or args.select_listing:
        mode = "latest" if args.latest_listing else "select"
        batch_id = choose_listing_batch_id(args.output_dir, mode)

    max_runtime_text = " ".join(args.max_runtime) if args.max_runtime else None
    max_runtime_seconds = parse_duration(max_runtime_text)
    deadline = None
    if max_runtime_seconds:
        deadline = datetime.now() + timedelta(seconds=max_runtime_seconds)
        print(f"[INFO] Max runtime: {max_runtime_seconds / 3600:.1f} hours, deadline={deadline}")

    crawler = BestBuyTVCsvOrchestrator(
        resume_from=args.resume_from,
        batch_id=batch_id,
        time_offset_hours=args.time_offset,
        output_dir=args.output_dir,
        chunk_size=args.chunk_size,
        chunk_min=args.chunk_min,
        chunk_max=args.chunk_max,
        cooldown_min=args.cooldown_min,
        cooldown_max=args.cooldown_max,
        block_cooldown_min=args.block_cooldown_min,
        block_cooldown_max=args.block_cooldown_max,
        skip_reviews=not args.with_reviews,
        skip_similar=not args.with_similar,
        deadline=deadline,
        start_order=args.start_order,
        end_order=args.end_order,
        target_count=args.target_count,
        stable_mode=args.stable_mode,
        profile_dir=args.profile_dir,
    )
    success = crawler.run()
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
