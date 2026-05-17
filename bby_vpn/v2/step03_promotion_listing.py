"""Best Buy TV promotion listing crawler for the v2 GraphQL test flow.

This file intentionally follows the existing production crawler in
``running/bby_tv_pmt.py`` for page source, selectors, and field semantics.
The v2 test variant only writes CSV and never writes listing rows to DB.
"""

import csv
import os
import random
import re
import sys
import time
import traceback
from datetime import datetime, timedelta

from DrissionPage import ChromiumOptions, ChromiumPage
from lxml import html

RUNNING_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "running")
if RUNNING_DIR not in sys.path:
    sys.path.insert(0, RUNNING_DIR)
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from common.setup import setup_environment

setup_environment(__file__)

from listing_sku import extract_numeric_sku
from listing_graphql import ListingGraphQLSkuCollector
from common.base_crawler import BaseCrawler
from config import DB_CONFIG
from db_readonly import connect_readonly
from data_paths import graphql_registry_dir, listing_parsed_dir


class BestBuyTVPromotionCrawler(BaseCrawler):
    """Promotion listing crawler based on the existing running implementation."""

    def __init__(self, test_mode=True, batch_id=None, time_offset_hours=0):
        super().__init__()
        self.test_mode = test_mode
        self.account_name = "Bestbuy"
        self.page_type = "promotion"
        self.batch_id = batch_id
        self.time_offset_hours = time_offset_hours
        self.calendar_week = None
        self.url_template = None
        self.current_rank = 0
        self.test_count = 1
        self.max_sections = 100
        self.csv_output_dir = str(listing_parsed_dir("promotion"))
        self.graphql_output_dir = str(graphql_registry_dir())
        self.csv_output_path = os.path.join(self.csv_output_dir, "bby_tv_pmt1_vpn_test.csv")
        self.page = None
        self.stats = {
            "collected": 0,
            "openbox_filtered": 0,
            "non_product": 0,
        }

        if os.path.exists(self.csv_output_path):
            os.remove(self.csv_output_path)

    def connect_db(self):
        """Read selector/url config only; v2 listing test does not write to DB."""
        try:
            self.db_conn = connect_readonly({**DB_CONFIG, "database": "postgres"})
            print("[SUCCESS] Read-only database connected")
            return True
        except Exception as exc:
            print(f"[ERROR] Database connection failed: {exc}")
            traceback.print_exc()
            return False

    def setup_drission_driver(self):
        try:
            co = ChromiumOptions()
            co.auto_port()
            co.no_imgs(True)
            self.page = ChromiumPage(co)
            print("[SUCCESS] DrissionPage setup complete")
        except Exception as exc:
            print(f"[ERROR] DrissionPage setup failed: {exc}")
            traceback.print_exc()
            raise

    def initialize(self):
        if not self.connect_db():
            return False
        if not self.load_xpaths(self.account_name, self.page_type, "SEA", "TV"):
            return False
        self.url_template = self.load_page_urls(self.account_name, self.page_type, "SEA", "TV")
        if not self.url_template:
            return False
        try:
            self.setup_drission_driver()
        except Exception:
            return False
        if not self.batch_id:
            self.batch_id = self.generate_batch_id(
                self.account_name,
                test_mode=True,
                time_offset_hours=self.time_offset_hours,
            )
        self.calendar_week = self.generate_calendar_week(time_offset_hours=self.time_offset_hours)
        self.cleanup_old_logs()
        return True

    def extract_item_from_url(self, product_url):
        if not product_url:
            return None
        try:
            cleaned_url = re.sub(r"/sku/\d+(/openbox\?.*)?", "", product_url)
            cleaned_url = cleaned_url.split("?")[0].rstrip("/")
            item = cleaned_url.split("/")[-1]
            return item[:-2] if item.endswith(".p") else item
        except Exception:
            return None

    def is_product_excluded(self, item):
        if not item or not self.db_conn:
            return False
        try:
            cursor = self.db_conn.cursor()
            cursor.execute(
                """
                SELECT is_product FROM tv_item_mst
                WHERE item = %s AND account_name = %s
                """,
                (item, self.account_name),
            )
            row = cursor.fetchone()
            cursor.close()
            return row is not None and row[0] is False
        except Exception:
            return False

    def get_page_html_safely(self, context, max_attempts=3):
        last_error = None
        for attempt in range(1, max_attempts + 1):
            try:
                return self.page.html
            except Exception as exc:
                last_error = exc
                print(f"[WARNING] HTML read failed during {context} ({attempt}/{max_attempts}): {exc}")
                try:
                    js_html = self.page.run_js("return document.documentElement.outerHTML;", timeout=8)
                    if js_html:
                        print("[INFO] Recovered HTML via JS outerHTML")
                        return js_html
                except Exception as js_exc:
                    print(f"[WARNING] JS outerHTML fallback failed: {js_exc}")
                if attempt < max_attempts:
                    try:
                        self.page.refresh()
                    except Exception as refresh_exc:
                        print(f"[WARNING] Refresh after HTML timeout failed: {refresh_exc}")
                    time.sleep(random.uniform(5, 8))
        raise last_error

    def extract_promotion_type(self, section):
        parts = []
        for field in ("promotion_type_h2", "promotion_type_h3", "promotion_type_p", "promotion_type_sub"):
            xpath = self.xpaths.get(field, {}).get("xpath")
            if not xpath:
                continue
            try:
                elems = section.xpath(xpath)
                if elems:
                    text = " ".join(elems[0].text_content().split()).strip()
                    if text:
                        parts.append(text)
            except Exception:
                continue
        return " ".join(parts).strip() or None

    def crawl_page(self):
        sku_collector = ListingGraphQLSkuCollector(self.page, output_dir=self.graphql_output_dir)
        products = []
        try:
            section_container_xpath = self.xpaths.get("section_container", {}).get("xpath")
            base_container_xpath = self.xpaths.get("base_container", {}).get("xpath")
            if not section_container_xpath or not base_container_xpath:
                print("[ERROR] section_container or base_container XPath not found")
                return []

            print(f"[INFO] Accessing promotion page: {self.url_template}")
            sku_collector.start()
            self.page.get(self.url_template)
            time.sleep(random.uniform(8, 12))
            sku_collector.drain(5)

            sections = []
            for attempt in range(1, 4):
                page_html = self.get_page_html_safely(f"promotion attempt {attempt}")
                tree = html.fromstring(page_html)
                sections = tree.xpath(section_container_xpath)
                print(f"[INFO] Attempt {attempt}: Found {len(sections)} sections")
                if sections:
                    break
                if attempt < 3:
                    time.sleep(random.uniform(5, 8))
                    sku_collector.drain(2)

            if not sections:
                print("[ERROR] No promotion sections found")
                return []

            section_limit = self.test_count if self.test_mode else self.max_sections
            for sec_idx, section in enumerate(sections[:section_limit], 1):
                promotion_type = self.extract_promotion_type(section)
                items = section.xpath(base_container_xpath)
                print(f"[INFO] Section {sec_idx}: type={promotion_type}, items={len(items)}")

                for pos, item in enumerate(items, 1):
                    try:
                        retailer_sku_name = self.safe_extract(item, "retailer_sku_name") or ""
                        product_url_raw = self.safe_extract(item, "product_url")
                        product_url = (
                            f"https://www.bestbuy.com{product_url_raw}"
                            if product_url_raw and product_url_raw.startswith("/")
                            else product_url_raw
                        )
                        if not retailer_sku_name or not product_url:
                            continue
                        if "openbox" in product_url.lower():
                            self.stats["openbox_filtered"] += 1
                            continue
                        item_id = self.extract_item_from_url(product_url)
                        if self.is_product_excluded(item_id):
                            self.stats["non_product"] += 1
                            continue

                        offer_raw = self.safe_extract(item, "offer")
                        offer_match = re.search(r"\d+", offer_raw or "")
                        offer = offer_match.group() if offer_match else offer_raw
                        numeric_sku = extract_numeric_sku(item, product_url)

                        products.append(
                            {
                                "account_name": self.account_name,
                                "page_type": self.page_type,
                                "retailer_sku_name": retailer_sku_name,
                                "promotion_position": pos,
                                "offer": offer,
                                "promotion_type": promotion_type,
                                "product_url": product_url,
                                "numeric_sku": numeric_sku,
                                "calendar_week": self.calendar_week,
                                "crawl_datetime": (
                                    datetime.now() + timedelta(hours=self.time_offset_hours)
                                ).strftime("%Y-%m-%d %H:%M:%S"),
                                "batch_id": self.batch_id,
                            }
                        )
                        print(f"  [S{sec_idx}-{pos}] {retailer_sku_name[:60]}...")
                    except Exception as exc:
                        print(f"[WARNING] Promotion item extraction failed: {exc}")
                        continue

            self.stats["collected"] = len(products)
            sku_collector.apply(products)
            print(f"[OK] Promotion products extracted: {len(products)}")
            return products
        except Exception as exc:
            print(f"[ERROR] Promotion crawl failed: {exc}")
            traceback.print_exc()
            if products:
                sku_collector.apply(products)
            return []
        finally:
            sku_collector.stop()

    def save_to_db(self, products):
        if not products:
            print("[WARNING] No data to save")
            return False
        fieldnames = [
            "account_name",
            "batch_id",
            "page_type",
            "retailer_sku_name",
            "promotion_position",
            "offer",
            "promotion_type",
            "product_url",
            "numeric_sku",
            "crawl_datetime",
            "calendar_week",
        ]
        try:
            with open(self.csv_output_path, "w", newline="", encoding="utf-8-sig") as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                writer.writeheader()
                for product in products:
                    writer.writerow({field: product.get(field) for field in fieldnames})
            print(f"[OK] CSV saved: {len(products)} rows -> {self.csv_output_path}")
            return True
        except Exception as exc:
            print(f"[ERROR] CSV save failed: {exc}")
            traceback.print_exc()
            return False

    def run(self):
        try:
            print("=" * 80)
            print(f"BestBuy TV Promotion Listing Crawler (Batch ID: {self.batch_id})")
            print("=" * 80)
            if not self.initialize():
                return
            products = self.crawl_page()
            if products:
                self.save_to_db(products)
            else:
                print("[ERROR] No promotion products collected")
        finally:
            if self.page:
                self.page.quit()
                print("[INFO] Browser closed")
            if self.db_conn:
                self.db_conn.close()
                print("[INFO] DB connection closed")


BestBuyPromotionCrawler = BestBuyTVPromotionCrawler


def main():
    BestBuyTVPromotionCrawler().run()


if __name__ == "__main__":
    main()


