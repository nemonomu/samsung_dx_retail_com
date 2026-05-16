"""
BestBuy TV CSV-only v2 orchestrator.

DB is used only for crawler configuration such as XPath and page URLs.
Listing and detail results are written to CSV files, not DB tables.
"""

import argparse
import csv
import os
import random
import sys
import time
import traceback
from datetime import datetime, timedelta

import pytz

CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
REPO_DIR = os.path.dirname(CURRENT_DIR)
if CURRENT_DIR in sys.path:
    sys.path.remove(CURRENT_DIR)
sys.path.insert(0, CURRENT_DIR)

from common.setup import setup_environment
setup_environment(__file__)
if REPO_DIR not in sys.path:
    sys.path.insert(1, REPO_DIR)

from common.base_crawler import BaseCrawler
from bby_tv_bsr import BestBuyTVBSRCrawler
from bby_tv_dt import BestBuyTVDetailCrawler
from bby_tv_main import BestBuyTVMainCrawler
from bby_tv_pmt import BestBuyTVPromotionCrawler
from bby_tv_trend import BestBuyTVTrendCrawler


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
        self, batch_id, csv_store, detail_csv, time_offset_hours=0, chunk_size=20,
        cooldown_min=300, cooldown_max=900, skip_reviews=True, skip_similar=True,
        deadline=None,
    ):
        super().__init__(batch_id=batch_id, test_mode=False, time_offset_hours=time_offset_hours)
        self.csv_store = csv_store
        self.detail_csv = detail_csv
        self.chunk_size = chunk_size
        self.cooldown_min = cooldown_min
        self.cooldown_max = cooldown_max
        self.skip_reviews = skip_reviews
        self.skip_similar = skip_similar
        self.deadline = deadline
        self.standalone = False
        os.makedirs(os.path.dirname(self.detail_csv), exist_ok=True)

    def load_product_list(self):
        products = self.csv_store.product_list()
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

    def crawl_detail(self, product):
        removed = {}
        if self.skip_reviews:
            for key in (
                "reviews_button", "detailed_review_content",
                "reviewpage_recommendation_intent",
            ):
                if key in self.xpaths:
                    removed[key] = self.xpaths.pop(key)
        if self.skip_similar:
            for key in ("similar_products_container", "similar_product_name"):
                if key in self.xpaths:
                    removed[key] = self.xpaths.pop(key)
        try:
            return super().crawl_detail(product)
        finally:
            self.xpaths.update(removed)

    def _looks_incomplete(self, data):
        return not any(data.get(k) for k in ("item", "sku", "screen_size", "final_sku_price"))

    def _cooldown(self, reason):
        wait_seconds = random.randint(self.cooldown_min, self.cooldown_max)
        print(f"[COOLDOWN] {reason}: waiting {wait_seconds // 60}m {wait_seconds % 60}s")
        if self.page:
            try:
                self.page.quit()
            except Exception:
                pass
            self.page = None
        time.sleep(wait_seconds)
        self.setup_drission_driver()

    def run(self):
        try:
            if not self.initialize():
                print("[ERROR] Detail CSV initialization failed")
                return False

            product_list = self.load_product_list()
            if not product_list:
                print("[ERROR] No products found in listing CSV")
                return False

            total_saved = 0
            for i, product in enumerate(product_list, 1):
                if self.deadline and datetime.now() >= self.deadline:
                    print("[TIME LIMIT] Detail deadline reached. Stopping detail stage.")
                    break

                sku_name = product.get("retailer_sku_name") or "N/A"
                print(f"\n{'=' * 70}")
                print(f"[{i}/{len(product_list)}] {sku_name[:60]}")
                print(f"{'=' * 70}")

                combined_data = self.crawl_detail(product)
                if combined_data and self._looks_incomplete(combined_data):
                    print("[WARNING] Detail data looks incomplete. Cooling down and retrying once.")
                    self._cooldown("incomplete detail")
                    combined_data = self.crawl_detail(product)

                if combined_data:
                    combined_data["order"] = i
                if combined_data and self.save_to_retail_com(combined_data):
                    total_saved += 1

                if i < len(product_list) and self.chunk_size and i % self.chunk_size == 0:
                    self._cooldown(f"chunk boundary after {i} products")
                else:
                    time.sleep(random.uniform(5, 8))

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


class BestBuyTVCsvOrchestrator:
    def __init__(
        self, resume_from=None, batch_id=None, time_offset_hours=0,
        output_dir=DEFAULT_OUTPUT_DIR, chunk_size=20, cooldown_min=300,
        cooldown_max=900, skip_reviews=True, skip_similar=True,
        deadline=None,
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
        self.cooldown_min = cooldown_min
        self.cooldown_max = cooldown_max
        self.skip_reviews = skip_reviews
        self.skip_similar = skip_similar
        self.deadline = deadline
        self.korea_tz = pytz.timezone("Asia/Seoul")
        self.listing_csv = os.path.join(self.output_dir, f"bby_tv_v2_listing_{self.batch_id}.csv")
        self.detail_csv = os.path.join(self.output_dir, f"bby_tv_v2_detail_{self.batch_id}.csv")
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
                cooldown_min=self.cooldown_min,
                cooldown_max=self.cooldown_max,
                skip_reviews=self.skip_reviews,
                skip_similar=self.skip_similar,
                deadline=self.deadline,
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


def main():
    parser = argparse.ArgumentParser(description="BestBuy TV CSV-only v2 orchestrator")
    parser.add_argument("--resume-from", choices=RESUME_STAGES)
    parser.add_argument("--batch-id")
    parser.add_argument("--time_offset", type=int, default=0)
    parser.add_argument("--output-dir", default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--chunk-size", type=int, default=20)
    parser.add_argument("--cooldown-min", type=int, default=300)
    parser.add_argument("--cooldown-max", type=int, default=900)
    parser.add_argument("--with-reviews", action="store_true", help="review page actions are skipped by default")
    parser.add_argument("--with-similar", action="store_true", help="similar product actions are skipped by default")
    parser.add_argument("max_runtime", nargs="*", help='optional duration such as "6 hours" or "6h"')
    args = parser.parse_args()

    max_runtime_text = " ".join(args.max_runtime) if args.max_runtime else None
    max_runtime_seconds = parse_duration(max_runtime_text)
    deadline = None
    if max_runtime_seconds:
        deadline = datetime.now() + timedelta(seconds=max_runtime_seconds)
        print(f"[INFO] Max runtime: {max_runtime_seconds / 3600:.1f} hours, deadline={deadline}")

    crawler = BestBuyTVCsvOrchestrator(
        resume_from=args.resume_from,
        batch_id=args.batch_id,
        time_offset_hours=args.time_offset,
        output_dir=args.output_dir,
        chunk_size=args.chunk_size,
        cooldown_min=args.cooldown_min,
        cooldown_max=args.cooldown_max,
        skip_reviews=not args.with_reviews,
        skip_similar=not args.with_similar,
        deadline=deadline,
    )
    success = crawler.run()
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
