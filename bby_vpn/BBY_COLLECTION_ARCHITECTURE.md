# Best Buy TV Collection Architecture

## Operating Model

The target architecture should not be a single long-running script. Treat the
crawler as a small data platform:

1. Scheduler discovers work and creates typed tasks.
2. Distributed queue assigns tasks to workers.
3. Redis coordinates host/account/region rate budgets.
4. API-first collectors attempt low-cost structured collection.
5. Browser fallback handles dynamic rendering gaps and quality verification.
6. Quality gate validates each snapshot before storage.
7. Diff pipeline records product changes and appends price history.

The current DrissionPage scripts can remain as compatibility workers while the
new Playwright workers are introduced task-by-task.

## Queue And Scheduler

Use explicit task kinds instead of script order:

- `listing`: collect product URLs and listing ranks.
- `api_product`: structured product facts.
- `api_price`: price and offer facts.
- `inventory`: ZIP/region availability.
- `pdp`: browser fallback for PDP extraction.
- `review`: review details where allowed and needed.
- `render_compare`: headed/headless and mobile/desktop sampling.
- `quality_check`: post-collection validation.

Each task should include `sku`, `url`, `region`, `render_mode`, `attempts`,
`not_before`, and `parent_task_id`. The implementation scaffold is in
`bby_playwright_pipeline.py`.

## API-First, Browser Fallback

Prefer API collection when the endpoint is observed from normal rendered page
loads or documented. Do not replay aggressively when an API response is denied,
incomplete, stale, or schema-incompatible.

Browser fallback should trigger when:

- API fields are missing required values.
- API price conflicts with rendered PDP price.
- region inventory is ambiguous.
- schema/hash changed since the last successful parse.
- a sample is selected for render comparison.

This keeps browsers for the cases where rendering genuinely adds value.

## Retry And Backoff

Retry policy should classify failures:

- `timeout` / `navigation_error`: retry with exponential backoff and jitter.
- `5xx`: retry with longer backoff.
- `schema_mismatch`: one browser fallback, then quarantine.
- `blocked` / `captcha` / `forbidden`: terminal for that worker/session; checkpoint and stop.
- `quality_reject`: no blind retry; send to review/quarantine.

Retries must update `not_before` in the queue rather than sleeping inside a
worker for a long time.

## Redis Coordination

Redis should coordinate:

- per-host requests per minute/hour;
- per-region inventory checks;
- per-account/session work budgets;
- task leases and requeue after worker crash;
- cache entries for product, price, inventory, and render diagnostics.

The important rule is that every worker asks Redis for a slot before touching a
host. Local sleeps are not enough once workers are distributed.

## Cache Strategy

Use separate TTLs:

- product metadata: long TTL, for example 12-48 hours.
- price: short TTL, for example 15-60 minutes.
- inventory by ZIP: short TTL, for example 10-30 minutes.
- negative/missing results: very short TTL, for example 5 minutes.
- schema diagnostics: long TTL until parser version changes.

Serve stale cache only to avoid duplicate work, then revalidate asynchronously.

## Region And ZIP Inventory

Represent region as first-class task metadata, not a global browser state.
Store inventory facts keyed by `(retailer, sku, zip_code, store_id)`.

Do not mix ZIP-specific availability into the global product snapshot. A product
can have one global identity but many regional inventory states.

## Diff And Price History

Store immutable snapshots and derive diffs:

- `product_snapshot`: full current observation.
- `product_diff`: changed fields from previous comparable snapshot.
- `price_history`: append-only price observations by SKU and ZIP.
- `inventory_history`: append-only availability by SKU, ZIP, store.

Price history should never be overwritten. Current price can be a materialized
view over the latest accepted history row.

## Render Comparison

Run render comparison on a sample, not every SKU:

- desktop headless
- desktop headed
- mobile headless
- mobile headed

Compare extracted title, price, availability, review count, and critical DOM
presence. If fields disagree, prefer browser fallback output only after quality
gate acceptance and log the disagreement.

## Anti-Fragile Design

The scraper should degrade by scope, not fail the whole run:

- listing failure does not erase prior URL inventory;
- API failure schedules browser fallback;
- browser failure quarantines one SKU;
- block signal stops that worker/session;
- quality failure rejects one row and keeps evidence;
- parser schema change creates diagnostics and reduces task priority.

This is the main difference between a resilient collection system and a single
script with retries.
