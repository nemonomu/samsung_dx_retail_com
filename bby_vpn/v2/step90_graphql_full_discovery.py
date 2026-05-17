"""Discover Best Buy PDP GraphQL operations for API-first detail collection.

This is a short, bounded discovery runner. It opens a small representative set
of PDP URLs, triggers the expensive UI surfaces once, and persists every
GraphQL operation it observes into data/graphql/registry.
"""

from __future__ import annotations

import csv
import json
import os
import random
import sys
import time
from datetime import datetime
from pathlib import Path

from DrissionPage import ChromiumOptions, ChromiumPage

BASE_DIR = Path(__file__).resolve().parent
BBY_VPN_DIR = BASE_DIR.parent
PROJECT_DIR = BBY_VPN_DIR.parent


def add_import_path(path):
    if path and path.exists() and str(path) not in sys.path:
        sys.path.insert(0, str(path))


for path in (BASE_DIR, BBY_VPN_DIR, PROJECT_DIR):
    add_import_path(path)

for ancestor in (BASE_DIR, *BASE_DIR.parents):
    for path in (
        ancestor,
        ancestor / "running",
        ancestor / "bby_vpn",
        ancestor / "bby_vpn" / "running",
    ):
        if (path / "common" / "setup.py").exists():
            add_import_path(path)

from listing_sku import extract_numeric_sku_from_text
from session_pool import cookies_from_drission_page, minimal_headers_from_packet
from graphql_mapper import GraphQLMapper
from data_paths import detail_parsed_dir, graphql_registry_dir, listing_csv_path


LISTING_FILES = (
    listing_csv_path("main"),
    listing_csv_path("bsr"),
    listing_csv_path("promotion"),
    listing_csv_path("trend"),
    detail_parsed_dir() / "bby_tv_vpn_test.csv",
)


def extract_item_from_url(product_url):
    if not product_url:
        return None
    cleaned = str(product_url).split("?", 1)[0].rstrip("/")
    if "/sku/" in cleaned:
        cleaned = cleaned.split("/sku/", 1)[0]
    item = cleaned.rsplit("/", 1)[-1]
    return item[:-2] if item.endswith(".p") else item


def read_candidate_rows():
    rows = []
    for path in LISTING_FILES:
        if not path.exists() or path.stat().st_size == 0:
            continue
        with path.open(newline="", encoding="utf-8-sig") as csvfile:
            for row in csv.DictReader(csvfile):
                product_url = row.get("product_url")
                if not product_url or "openbox" in product_url.lower():
                    continue
                numeric_sku = row.get("numeric_sku") or row.get("sku") or extract_numeric_sku_from_text(product_url)
                rows.append({
                    "product_url": product_url,
                    "item": row.get("item") or extract_item_from_url(product_url),
                    "numeric_sku": numeric_sku,
                    "name": row.get("retailer_sku_name") or row.get("product_name") or "",
                    "reviews": row.get("count_of_reviews") or "",
                    "source": path.name,
                })
    return rows


def choose_representative_rows(rows, limit):
    seen = set()
    chosen = []

    def score(row):
        score_value = 0
        name = row.get("name", "").lower()
        reviews = row.get("reviews", "")
        if row.get("numeric_sku"):
            score_value += 10
        try:
            if int(str(reviews).replace(",", "")) > 0:
                score_value += 5
        except Exception:
            pass
        for brand in ("samsung", "lg", "insignia", "tcl", "roku", "hisense", "sony"):
            if brand in name:
                score_value += 1
                break
        return score_value

    for row in sorted(rows, key=score, reverse=True):
        key = row.get("numeric_sku") or row.get("item") or row.get("product_url")
        if not key or key in seen:
            continue
        seen.add(key)
        chosen.append(row)
        if len(chosen) >= limit:
            break
    return chosen


class FullDiscovery:
    def __init__(self):
        self.output_dir = Path(os.environ.get("BBY_GRAPHQL_REGISTRY_DIR", graphql_registry_dir()))
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.discovery_dir = self.output_dir
        self.discovery_dir.mkdir(parents=True, exist_ok=True)
        self.mapper = GraphQLMapper(str(self.discovery_dir))
        self.page = None
        self.seen = set()
        self.events = []

    def setup_browser(self):
        co = ChromiumOptions()
        co.auto_port()
        co.no_imgs(True)
        self.page = ChromiumPage(co)

    def close(self):
        if self.page:
            try:
                self.page.quit()
            except Exception:
                try:
                    self.page.close()
                except Exception:
                    pass

    def record_packet(self, packet, product_url):
        try:
            req_body = None
            for attr in ("body", "postData", "data"):
                value = getattr(packet.request, attr, None)
                if value:
                    req_body = value
                    break
            if not req_body:
                return False
            req_data = json.loads(req_body) if isinstance(req_body, str) else req_body
            if not isinstance(req_data, dict):
                return False
            op_name = req_data.get("operationName")
            if not op_name:
                return False
            endpoint_url = getattr(packet.request, "url", None) or getattr(packet, "url", None)
            resp_body = getattr(packet.response, "body", None)
            if not endpoint_url or not resp_body:
                return False

            variables = req_data.get("variables") if isinstance(req_data.get("variables"), dict) else {}
            signature = (op_name, json.dumps(sorted((variables or {}).keys()), ensure_ascii=False))
            if signature in self.seen:
                return False

            headers = minimal_headers_from_packet(packet)
            headers.setdefault("Referer", product_url)
            cookies = cookies_from_drission_page(self.page)
            path = self.mapper.record(op_name, endpoint_url, req_data, headers, resp_body, cookies=cookies)
            self.seen.add(signature)
            self.events.append({
                "operationName": op_name,
                "endpoint_url": endpoint_url,
                "variables": sorted((variables or {}).keys()),
                "path": path,
            })
            print(f"  [DISCOVERY] captured {op_name} vars={sorted((variables or {}).keys())}")
            return True
        except Exception as exc:
            print(f"  [WARNING] packet record failed: {exc}")
            return False

    def drain_packets(self, product_url, seconds, quiet=False):
        captured = 0
        deadline = time.time() + seconds
        while time.time() < deadline:
            try:
                packet = self.page.listen.wait(timeout=0.5)
            except Exception:
                break
            if not packet:
                continue
            if self.record_packet(packet, product_url):
                captured += 1
        if captured or not quiet:
            print(f"  [DISCOVERY] drain {seconds:.1f}s captured={captured}")
        return captured

    def js(self, script):
        try:
            return self.page.run_js(script)
        except Exception as exc:
            print(f"  [WARNING] JS action failed: {exc}")
            return None

    def discover_url(self, row, index, total):
        product_url = row["product_url"]
        print("\n" + "=" * 80)
        print(f"[{index}/{total}] Discovery PDP: {product_url}")
        print(f"  item={row.get('item')} numeric_sku={row.get('numeric_sku')} name={row.get('name')[:80]}")
        print("=" * 80)

        self.page.listen.start("graphql")
        try:
            self.page.get(product_url)
            self.drain_packets(product_url, float(os.environ.get("BBY_DISCOVERY_LOAD_SECONDS", "10")))

            for pct in (0.35, 0.65, 0.9, 1.0):
                self.js(f"window.scrollTo(0, document.body.scrollHeight * {pct});")
                time.sleep(random.uniform(1.0, 1.8))
                self.drain_packets(product_url, 3, quiet=True)

            self.trigger_reviews(product_url)
            self.trigger_specs(product_url)
            self.trigger_similar(product_url)
        finally:
            try:
                self.page.listen.stop()
            except Exception:
                pass

    def trigger_reviews(self, product_url):
        print("  [ACTION] reviews/rating")
        result = self.js("""
            const nodes = Array.from(document.querySelectorAll('a, button, [role="button"], [role="link"]'));
            const node = nodes.find(n => {
                const label = ((n.textContent || '') + ' ' + (n.getAttribute('aria-label') || '')).toLowerCase();
                return label.includes('review') && !label.includes('write');
            });
            if (node) {
                node.scrollIntoView({block: 'center'});
                node.click();
                return (node.textContent || node.getAttribute('aria-label') || '').trim().slice(0, 80);
            }
            return 'not found';
        """)
        print(f"  [ACTION] reviews result={result}")
        time.sleep(2)
        self.drain_packets(product_url, float(os.environ.get("BBY_DISCOVERY_ACTION_SECONDS", "8")))

    def trigger_specs(self, product_url):
        print("  [ACTION] specifications")
        result = self.js("""
            const nodes = Array.from(document.querySelectorAll('button, a, [role="button"]'));
            const node = nodes.find(n => {
                const label = ((n.textContent || '') + ' ' + (n.getAttribute('aria-label') || '')).toLowerCase();
                return label.includes('specification') || label.includes('details');
            });
            if (node) {
                node.scrollIntoView({block: 'center'});
                node.click();
                return (node.textContent || node.getAttribute('aria-label') || '').trim().slice(0, 80);
            }
            return 'not found';
        """)
        print(f"  [ACTION] specs result={result}")
        time.sleep(3)
        self.drain_packets(product_url, float(os.environ.get("BBY_DISCOVERY_ACTION_SECONDS", "8")))
        self.js("document.dispatchEvent(new KeyboardEvent('keydown', {key:'Escape'}));")

    def trigger_similar(self, product_url):
        print("  [ACTION] similar/compare scroll")
        for text in ("similar", "compare", "recommend"):
            self.js(f"""
                const targetText = {json.dumps(text)};
                const nodes = Array.from(document.querySelectorAll('h2,h3,section,div,button,a'));
                const node = nodes.find(n => (n.textContent || '').toLowerCase().includes(targetText));
                if (node) node.scrollIntoView({{block: 'center'}});
                return node ? targetText : 'not found';
            """)
            time.sleep(1.5)
            self.drain_packets(product_url, 3, quiet=True)

    def write_summary(self):
        path = self.output_dir / f"graphql_full_discovery_summary_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        with path.open("w", encoding="utf-8") as f:
            json.dump({
                "operation_count": len(self.events),
                "operations": self.events,
            }, f, ensure_ascii=False, indent=2, default=str)
        print(f"[OK] Discovery summary saved: {path}")


def main():
    limit = int(os.environ.get("BBY_DISCOVERY_MAX_PRODUCTS", "12"))
    rows = read_candidate_rows()
    rows = choose_representative_rows(rows, limit)
    if not rows:
        print("[ERROR] No candidate rows found. Run listing first or provide listing CSVs.")
        return 1

    runner = FullDiscovery()
    try:
        runner.setup_browser()
        for idx, row in enumerate(rows, 1):
            runner.discover_url(row, idx, len(rows))
            time.sleep(random.uniform(3, 8))
        runner.write_summary()
    finally:
        runner.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())


