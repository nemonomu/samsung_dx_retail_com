# Best Buy V2 Runbook

## Entry Points

Run the full ordered pipeline:

```bat
cd /d C:\samsung_dx_retail_com\bby_vpn\v2
python run_pipeline.py
```

List steps:

```bat
python run_pipeline.py --list-steps
```

Run selected steps:

```bat
python run_pipeline.py --only 01_main,05_filter --skip-detail
python run_pipeline.py --start-at 05_filter
```

## Step Files

- `step00_fetch_listing_seed_html.py`: ZenRows seed HTML fetch/diagnostics.
- `step01_main_listing.py`: main search listing.
- `step02_bsr_listing.py`: best-seller listing.
- `step03_promotion_listing.py`: promotion listing.
- `step04_trend_listing.py`: trend listing.
- `step05_listing_detail_flow.py`: shared listing filter and API-detail flow helpers.
- `step06_detail_crawler.py`: detail/API replay crawler.
- `step90_graphql_full_discovery.py`: bounded GraphQL discovery.
- `step91_graphql_collect_test.py`: small GraphQL replay QA runner.

## Data Layout

Runtime data lives under `data/`:

```text
data/main/parsed/
data/bsr/parsed/
data/promotion/parsed/
data/trend/parsed/
data/detail/raw/
data/detail/parsed/
data/graphql/registry/
```

GraphQL replay files:

```text
data/graphql/registry/graphql_registry.json
data/graphql/registry/graphql_sku_map.json
data/graphql/registry/graphql_cookies.json
data/graphql/registry/graphql_operation_*.json
```

Detail output:

```text
data/detail/parsed/bby_tv_vpn_test.csv
data/detail/raw/bby_tv_dt1_audit.jsonl
data/detail/raw/bby_tv_dt1_checkpoint.json
```

## Notes

- `archive/` contains old docs, legacy batch files, old module folders, and previous runtime outputs.
- DB writes are still controlled inside the crawler implementation. Listing steps write CSV/API artifacts first.
- `BBY_DATA_DIR` can override the whole `data/` root.
