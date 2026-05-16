# V2 GraphQL/API-First Collector Plan

## Current Structure Analysis

`bby_tv_crawl.py` remains the orchestrator. It runs listing collectors first and
then runs `bby_tv_dt1.py` for detail collection. V2 keeps that operational shape
so existing DB/config/CSV flows still work, but changes detail collection defaults
to browser-minimized mode.

Existing retained capabilities:

- session warmup
- discovery refresh
- conservative rate limiter
- retry/backoff
- diagnostics
- GraphQL interception
- embedded JSON fallback
- quality audit
- checkpoint
- browser restart

## Bottleneck Functions

The main bottleneck is `scrape_detail_page()` in `bby_tv_dt1.py`.

High-cost behavior:

- repeated `page.get(product_url)`
- h1 wait against fully rendered PDP
- lazy-load scrolling
- similar-products section extraction
- specs dialog interaction
- review reload and review-page navigation
- DOM pagination for reviews

The second high-cost function is `capture_review_data_via_graphql()`. It is
useful because it already discovers GraphQL operations, but before V2 it did not
persist operation metadata as a reusable API registry.

## DOM Extraction That Can Be Reduced

Can be reduced first:

- review details
- top mentions
- recommendation intent
- AI review summary
- review count
- star rating
- current price when embedded/API payload contains it

Still browser fallback candidates:

- model number/specs when no API mapping exists
- screen size if neither embedded payload nor title regex is reliable
- schema drift verification
- missing critical fields

## API/GraphQL Replacement Targets

Initial known operation names:

- `CustomerReviewList_Init`
- `Reviews_Pros_Cons_Init`
- `Ai_Review_Summary_Init`
- `CustomerRatingCard_Init`

V2 now records discovered operations under:

```text
v2/crawler/discovery/graphql_map/
```

and maintains:

```text
v2/crawler/discovery/graphql_registry.json
```

## V2 Changes Implemented

- Copied current `bby_tv_crawl.py` and related scripts into `v2/`.
- Added GraphQL operation mapper and registry.
- Added async `httpx` GraphQL collector scaffold with retry/backoff, timeout,
  semaphore, cookie/header reuse, and request logging.
- Added embedded payload mapper for JSON-LD, `__NEXT_DATA__`, Apollo cache,
  `window.__INITIAL_STATE__`, and `script[type="application/json"]`.
- Added GraphQL review parser with pagination cursor collection,
  deduplication, normalization, max-review limit, and invalid text filtering.
- Added product parser scaffold for API payloads.
- Added schema drift and endpoint metrics diagnostics.
- Added browser-minimized defaults to `v2/bby_tv_crawl.py`.
- Extended `capture_review_data_via_graphql()` to persist operation metadata.

## Next Step

After one V2 discovery run, inspect saved `graphql_operation_*.json` files. Once
the required variables/cookies are confirmed, route review/recommendation/top
mentions through `collectors/graphql_collector.py` before attempting any browser
review navigation.
