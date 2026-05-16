"""Small CLI to verify API-first GraphQL collection from captured mappings."""

import argparse
import csv
import json
import os
import sys

from collectors.graphql_collector import (
    GraphQLCollector,
    load_graphql_registry,
    load_sku_map,
    resolve_sku_id_from_product_page,
)


def read_urls(args):
    urls = []
    if args.url:
        urls.extend(args.url)
    if args.csv:
        with open(args.csv, newline="", encoding="utf-8-sig") as csvfile:
            for row in csv.DictReader(csvfile):
                url = row.get("product_url")
                if url:
                    urls.append(url)
    return list(dict.fromkeys(urls))[: args.limit]


def main():
    parser = argparse.ArgumentParser(description="Test Best Buy GraphQL API collector")
    parser.add_argument("--registry-dir", default=os.path.dirname(os.path.abspath(__file__)))
    parser.add_argument("--url", action="append", help="Product URL. Can be repeated.")
    parser.add_argument("--csv", help="CSV with product_url column.")
    parser.add_argument("--limit", type=int, default=3)
    parser.add_argument("--out", default="graphql_collect_test_output.jsonl")
    args = parser.parse_args()

    registry = load_graphql_registry(args.registry_dir)
    if not registry:
        print(f"[ERROR] graphql_registry.json not found under {args.registry_dir}")
        return 2

    urls = read_urls(args)
    if not urls:
        print("[ERROR] Provide --url or --csv with product_url column")
        return 2

    collector = GraphQLCollector(timeout=30, concurrency=2)
    out_path = os.path.abspath(args.out)

    with open(out_path, "w", encoding="utf-8") as outfile:
        for idx, url in enumerate(urls, 1):
            print(f"[{idx}/{len(urls)}] GraphQL collect: {url[:100]}")
            result = collector.collect_review_bundle_sync(url, registry, sku_map=sku_map)
            if result.get("errors"):
                sku_id = resolve_sku_id_from_product_page(url, registry)
                print(f"  [ERROR] {result.get('errors')} resolved_skuId={sku_id}")
                outfile.write(json.dumps(result, ensure_ascii=False, default=str) + "\n")
                continue
            parsed = result.get("parsed") or {}
            print(
                "  skuId={skuId} rating={rating} reviews={reviews} collected_reviews={collected}".format(
                    skuId=parsed.get("skuId") or result.get("skuId"),
                    rating=parsed.get("star_rating"),
                    reviews=parsed.get("count_of_reviews"),
                    collected=parsed.get("review_count_collected"),
                )
            )
            outfile.write(json.dumps(result, ensure_ascii=False, default=str) + "\n")

    print(f"[OK] Saved: {out_path}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
    sku_map = load_sku_map(args.registry_dir)
    print(f"[INFO] Loaded sku map entries: {len(sku_map)}")
