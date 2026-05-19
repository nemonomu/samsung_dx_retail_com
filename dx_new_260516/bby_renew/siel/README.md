# SIEL India Retail Crawlers

This folder is reserved for India-market crawlers owned separately from the
existing US-oriented `amazon`, `bestbuy`, and `lowes` pipelines.

Initial target retailers:

```text
siel/amazon/     Amazon India crawler work
siel/flipkart/   Flipkart India crawler work
```

Initial product lines:

```text
TV   television
REF  refrigerator
LDY  washing machine / laundry
HHP  mobile phone
```

Keep India-specific code, configs, raw artifacts, and run outputs under this
folder so the existing retailer pipelines remain unchanged.

## Current Port

The first SIEL port keeps the proven India runtime logic but removes live DB
dependencies for smoke tests:

```text
selectors: siel/references/dx_siel_xpath_selectors_202605181653.csv
run output: siel/{amazon|flipkart}/logs/*.jsonl
csv output: final_output.csv and product_list.csv
db insert: disabled
```

Smoke-test commands:

```powershell
python -m siel.amazon.siel_amazon_orchestrator --product tv --dry-run
python -m siel.flipkart.siel_flipkart_orchestrator --product tv --dry-run
```

Export an existing JSONL run to CSV:

```powershell
python -m siel.csv_export path\to\run.jsonl --output-dir siel\output_test
```
