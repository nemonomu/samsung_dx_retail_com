# Working Memory

This repository is used with an RDP runtime for BestBuy collection. Before changing crawler code, review this checklist and keep the full pipeline in scope.

## BestBuy Crawler Principles

- Treat page-visible values as the source of truth. Do not store internal API counts or hidden structures unless they are validated against visible page behavior.
- Do not use local direct BestBuy network tests as proof. The local IP may be blocked. If runtime verification is needed, ask for RDP results or raw files.
- Avoid one-symptom fixes. Trace the affected field through listing, target selection, detail enrichment, final CSV, product list CSV, and DB load.
- Keep open-box products out of the collection path. They are not targets and must not influence price, offer, availability, review, or spec fields.
- For `offer`, do not use raw `offers.offers.length`. The visible `+N offers for you` count must come from page text or a validated equivalent such as `hotOffer` plus gift offer count.
- For TV promotion collection, exclude `Featured deals` by default. It is not part of the promotion target set; expected promotion rows are the other TV promo placements.
- For price fields, prefer validated new-product numeric prices over policy text when both exist. If `See price in cart` uses hidden numeric data, verify with RDP/browser samples before treating it as final truth.
- If `final_sku_price == original_sku_price`, assume original price is not visibly distinct and leave `original_sku_price` and `savings` blank.
- `Unavailable` is not a useful `fastest_delivery` value; leave it blank.
- Preserve customer-required DB schemas and field meanings. Do not assume DB table order, column names, or types are flexible without checking `step13`, `step14`, and the live table schema.
- After code changes, run syntax checks and inspect output field distributions before declaring success.
- A command that finishes is not proof that it did the intended work. For recrawl/refresh commands, verify the semantic success condition in the log before telling the user it worked.
- For "refresh at execution time" requests, cached successful raw files must be bypassed explicitly. Check for early-return paths such as `detail_success()`, `review_success()`, existing raw folders, retry-only filters, and rebuild-only modes.
- For any paid/network collection step, the log must prove whether a new call happened. Expected indicators include nonzero `detail_cost_usd_this_run`/`review_cost_usd_this_run`, fresh `started_at`/`finished_at`, and explicit `force_refresh` or equivalent flags. If cost is `0.0`, assume cache reuse until proven otherwise.
- When creating a shortcut runner such as `tvsku`, test the runner's actual control flow against the target code, not just its command syntax. The runner must set all environment variables needed to satisfy the user's intent.
- If a targeted refresh rewrites DB output, confirm both dimensions separately: which SKUs were freshly fetched, and whether the full batch was reloaded into DB.
- Treat the RDP command shell environment as persistent and potentially dirty. Before explaining or changing behavior, check whether old `set` values can override code defaults.
- Always check this `memory.md` before answering for this repo. RDP cmd env values can also contain trailing spaces; `BESTBUY_RUN_DATE=20260519 ` produced `bestbuy\data\hhp\20260519 \...` and step01 mkdir failures. Category runners must clear or intentionally set `BESTBUY_RUN_DATE`, code must `.strip()` run-date env values before building paths, and path diagnostics must check for invisible trailing spaces in env-derived segments.
- Treat `.env` as another persistent environment source. Clearing a cmd variable with `set NAME=` can allow `.env` defaults to be loaded again via `load_env()`. Use explicit override values such as `0` when a blank value has semantic meaning.
- For batch files and one-line rerun commands, explicitly set or clear every environment variable that controls scope, target size, output table, batch id, run date, retry/rebuild/force behavior, and workers. Do not rely on a variable being unset.
- When a code change changes the meaning of an env var, update the runners too. A correct Python default is not enough if `.bat` or the user's current cmd session still exports the old value.
- When `BESTBUY_FORCE_STEP_ENV=1`, orchestrator step defaults override parent cmd and batch variables. Check `bestbuy_orchestrator.py` step env values before trusting a runner variable.
- For every rerun diagnosis, inspect the manifest/log values actually used at runtime, especially `target_size`, `batch_id`, `run_root`, `table`, `force_refresh`, `retry_only`, and row counts. Do not infer them from code alone.
- BestBuy delivery and pickup fields are location sensitive. Current PDP raw samples show `destinationZipCode`/`postalCode` such as `55423`; do not claim ZIP `10010` collection unless the raw request/response proves that ZIP was actually applied.
- Shipping fields must preserve page-visible payment text. If the page shows `Get it tomorrow • FREE` or `Delivery as soon as ... • FREE`, do not truncate it to only the date phrase.
- Current production listing flow does not fetch rendered PLP HTML, so listing-only fulfillment text is unavailable during full runs. Use detail-derived fulfillment fallback; when `fastest_delivery` starts with `Get` and does not already contain `FREE`/`•`, append ` • FREE` per the user's BestBuy display rule.
- External/syndicated-only reviews are not BestBuy-owned reviews. Store `star_rating=Not yet reviewed`, `count_of_reviews=0`, `count_of_star_ratings=0`, and leave `recommendation_intent` blank/NULL only when the page-visible text is like `(11 reviews from Samsung US)`, or when `syndicatedReviewSummary` exists and BestBuy's own `reviewCount` is 0. Do not write literal `none` to DB for this field, and do not wipe normal BestBuy ratings just because syndicated summaries also exist.
- Numeric zero is a real value, not blank. Do not normalize with `value or ""` in parsing helpers because it can hide `0` ratings/review counts and skip required review normalization.
- `retailer_sku_name_similar` must come from page/raw sources such as PDP `Compare similar products` or validated variation payloads. Do not synthesize similar names from brand/series matching across collected rows.
- For BestBuy TV, removing the final target size cap does not mean `main_rank` can exceed 300. Keep listing main ranks capped at 300, and include BSR/promotion/trending extras as backfill rows with blank `main_rank`.
- BestBuy backfill page types must reflect their source. If both `main_rank` and `bsr_rank` are blank, `page_type` must be `promotion` or `trend`, not `main`/`bsr`. `item` must never be blank; fall back from target `bsin` or PDP URL BSIN when detail payload lacks it.

## Change Workflow

1. Identify the affected field and every step that reads or writes it.
2. Inspect available RDP raw files, final CSV, product list CSV, DB result CSV, and logs before editing.
3. Make the smallest complete change that covers all affected pipeline steps.
4. Rebuild the correct downstream artifacts in one runner when possible.
5. Validate counts and suspicious distributions: empty values, repeated constants, policy text, price equality, and DB inserted rows.
6. For refresh/retry runners, verify that cache-bypass behavior is active when the user asked for current execution-time data.
7. Before giving an RDP command, include env var clearing/setting for any variable that could persist from prior commands and change the result.
8. Commit and push only the intended files. Ignore unrelated untracked RDP/raw/result files.
