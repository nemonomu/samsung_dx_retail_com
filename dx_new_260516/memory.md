# Working Memory

This repository is used with an RDP runtime for BestBuy collection. Before changing crawler code, review this checklist and keep the full pipeline in scope.

## BestBuy Crawler Principles

- Treat page-visible values as the source of truth. Do not store internal API counts or hidden structures unless they are validated against visible page behavior.
- Do not use local direct BestBuy network tests as proof. The local IP may be blocked. If runtime verification is needed, ask for RDP results or raw files.
- Avoid one-symptom fixes. Trace the affected field through listing, target selection, detail enrichment, final CSV, product list CSV, and DB load.
- Keep open-box products out of the collection path. They are not targets and must not influence price, offer, availability, review, or spec fields.
- For `offer`, do not use raw `offers.offers.length`. The visible `+N offers for you` count must come from page text or a validated equivalent such as `hotOffer` plus gift offer count.
- For price fields, prefer validated new-product numeric prices over policy text when both exist. If `See price in cart` uses hidden numeric data, verify with RDP/browser samples before treating it as final truth.
- If `final_sku_price == original_sku_price`, assume original price is not visibly distinct and leave `original_sku_price` and `savings` blank.
- `Unavailable` is not a useful `fastest_delivery` value; leave it blank.
- Preserve customer-required DB schemas and field meanings. Do not assume DB table order, column names, or types are flexible without checking `step13`, `step14`, and the live table schema.
- After code changes, run syntax checks and inspect output field distributions before declaring success.

## Change Workflow

1. Identify the affected field and every step that reads or writes it.
2. Inspect available RDP raw files, final CSV, product list CSV, DB result CSV, and logs before editing.
3. Make the smallest complete change that covers all affected pipeline steps.
4. Rebuild the correct downstream artifacts in one runner when possible.
5. Validate counts and suspicious distributions: empty values, repeated constants, policy text, price equality, and DB inserted rows.
6. Commit and push only the intended files. Ignore unrelated untracked RDP/raw/result files.

