import csv
import json
import os
import time
from datetime import datetime
from pathlib import Path

from requests import RequestException
from zenrows import ZenRowsClient

from .step00_config import DEFAULT_BESTBUY_RUN_ROOT, KRW_PER_USD, rel_path
from .step00_fulfillment_graphql import (
    FULFILLMENT_ENDPOINT,
    fulfillment_url,
    fulfillment_variables,
    parse_fulfillment_response,
    request_cost,
    zenrows_params,
)
from .step08_detail_enrichment import FINAL_OUTPUT_CSV as STEP08_FINAL_OUTPUT_CSV
from .step08_detail_enrichment import TARGET_CSV, compact_text, write_csv


AVAILABILITY_FIELDS = ["pick_up_availability", "fastest_delivery", "delivery_availability"]
FINAL_OUTPUT_CSV = Path(
    os.getenv("BESTBUY_AVAILABILITY_BACKFILL_FINAL_CSV", os.getenv("BESTBUY_FINAL_OUTPUT_CSV", STEP08_FINAL_OUTPUT_CSV))
)
DETAIL_ROWS_CSV = Path(
    os.getenv("BESTBUY_AVAILABILITY_BACKFILL_DETAIL_ROWS_CSV", DEFAULT_BESTBUY_RUN_ROOT / "detail" / "parsed" / "detail_enriched_rows.csv")
)
BACKFILL_ROOT = Path(os.getenv("BESTBUY_AVAILABILITY_BACKFILL_ROOT", DEFAULT_BESTBUY_RUN_ROOT / "availability_backfill"))
BACKFILL_BATCH_ID = os.getenv("BESTBUY_AVAILABILITY_BACKFILL_BATCH_ID", "b_20260525_040458").strip()
REQUESTED_CHUNK_SIZE = int(os.getenv("BESTBUY_AVAILABILITY_BACKFILL_CHUNK_SIZE", "1"))
ALLOW_MULTI_SKU_FULFILLMENT = os.getenv("BESTBUY_AVAILABILITY_BACKFILL_ALLOW_MULTI_SKU", "0").lower() in {
    "1",
    "true",
    "yes",
    "y",
}
CHUNK_SIZE = REQUESTED_CHUNK_SIZE if ALLOW_MULTI_SKU_FULFILLMENT else 1
REQUEST_TIMEOUT = int(os.getenv("BESTBUY_AVAILABILITY_BACKFILL_TIMEOUT", os.getenv("ZENROWS_TIMEOUT", "120")))
DRY_RUN = os.getenv("BESTBUY_AVAILABILITY_BACKFILL_DRY_RUN", "0").lower() in {"1", "true", "yes", "y"}


def now():
    return datetime.now().isoformat(timespec="seconds")


def read_csv(path):
    path = Path(path)
    if not path.exists():
        return []
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        return list(csv.DictReader(f))


def csv_fields(path, rows):
    path = Path(path)
    header = []
    if path.exists():
        with path.open("r", encoding="utf-8-sig", newline="") as f:
            reader = csv.reader(f)
            try:
                header = next(reader)
            except StopIteration:
                pass
    keys = list(header)
    seen = set(header)
    for row in rows:
        for key in row:
            if key not in seen:
                seen.add(key)
                keys.append(key)
    return keys


def norm_key(value):
    return compact_text(value).lower()


def canonical_url(value):
    text = compact_text(value)
    if not text:
        return ""
    if "?" in text:
        text = text.split("?", 1)[0]
    if "/sku/" in text:
        text = text.split("/sku/", 1)[0]
    return text.rstrip("/").lower()


def item_from_product_url(value):
    text = compact_text(value)
    if not text or "/sku/" not in text:
        return ""
    before_sku = text.split("/sku/", 1)[0].rstrip("/")
    item = before_sku.rsplit("/", 1)[-1].strip()
    return item if item and item.lower() not in {"product", "site"} else ""


def ensure_item_from_url(row):
    if not compact_text(row.get("item")):
        item = item_from_product_url(row.get("product_url"))
        if item:
            row["item"] = item


def all_availability_blank(row):
    return all(not compact_text(row.get(field)) for field in AVAILABILITY_FIELDS)


def backfill_candidate(row, batch_id):
    if compact_text(row.get("batch_id")) != batch_id:
        return False
    return all_availability_blank(row)


def add_lookup(mapping, key, sku):
    key = norm_key(key)
    sku = compact_text(sku)
    if key and sku and key not in mapping:
        mapping[key] = sku


def build_sku_lookup(targets):
    lookup = {}
    for target in targets:
        sku = target.get("sku_id") or target.get("sku")
        add_lookup(lookup, target.get("sku_id"), sku)
        add_lookup(lookup, target.get("sku"), sku)
        add_lookup(lookup, target.get("item"), sku)
        add_lookup(lookup, target.get("bsin"), sku)
        add_lookup(lookup, item_from_product_url(target.get("product_url")), sku)
        add_lookup(lookup, target.get("product_url"), sku)
        add_lookup(lookup, canonical_url(target.get("product_url")), sku)
    return lookup


def sku_for_row(row, lookup):
    for key in (
        row.get("sku_id"),
        row.get("sku"),
        row.get("item"),
        row.get("bsin"),
        item_from_product_url(row.get("product_url")),
        row.get("product_url"),
        canonical_url(row.get("product_url")),
    ):
        direct = norm_key(key)
        if direct and direct in lookup:
            return lookup[direct]
        if direct.isdigit():
            return direct
    return ""


def unique_ordered(values):
    seen = set()
    result = []
    for value in values:
        value = compact_text(value)
        if value and value not in seen:
            seen.add(value)
            result.append(value)
    return result


def fulfillment_headers():
    return {
        "accept": "application/json, text/plain, */*",
        "referer": "https://www.bestbuy.com/",
        "x-client-id": "pdp-web",
        "x-requested-for-operation-name": "AIV_FulfillmentBatchCall",
        "user-agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
        ),
    }


def fetch_chunk(client, chunk, chunk_dir):
    chunk_dir.mkdir(parents=True, exist_ok=True)
    variables = fulfillment_variables(chunk, context="PLP")
    target_url = fulfillment_url(chunk, context="PLP")
    (chunk_dir / "request.json").write_text(
        json.dumps(
            {
                "endpoint": FULFILLMENT_ENDPOINT,
                "url": target_url,
                "sku_count": len(chunk),
                "skus": chunk,
                "variables": variables,
            },
            indent=2,
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    started = time.perf_counter()
    response = client.get(
        target_url,
        params=zenrows_params(),
        headers=fulfillment_headers(),
        timeout=REQUEST_TIMEOUT,
    )
    elapsed = round(time.perf_counter() - started, 3)
    (chunk_dir / "response.txt").write_text(response.text, encoding="utf-8", errors="replace")
    (chunk_dir / "headers.json").write_text(
        json.dumps(dict(response.headers), indent=2, ensure_ascii=False),
        encoding="utf-8",
    )
    response_json = {}
    try:
        response_json = response.json()
    except ValueError:
        pass
    if response_json:
        (chunk_dir / "response.json").write_text(
            json.dumps(response_json, indent=2, ensure_ascii=False),
            encoding="utf-8",
        )
    values = parse_fulfillment_response(response_json)
    errors = response_json.get("errors") if isinstance(response_json, dict) else None
    return {
        "status_code": response.status_code,
        "elapsed_seconds": elapsed,
        "x_request_cost": request_cost(response.headers),
        "values": values,
        "error": json.dumps(errors, ensure_ascii=False)[:500] if errors else "",
    }


def fetch_availability(skus, raw_dir):
    api_key = os.getenv("ZENROWS_API_KEY")
    if not api_key:
        raise RuntimeError("Set ZENROWS_API_KEY in .env")
    client = ZenRowsClient(api_key)
    values_by_sku = {}
    calls = []
    chunks = [skus[index : index + CHUNK_SIZE] for index in range(0, len(skus), CHUNK_SIZE)]
    for index, chunk in enumerate(chunks, 1):
        chunk_dir = raw_dir / f"chunk_{index:03d}"
        status = "ERR"
        cost = 0.0
        elapsed = 0.0
        error = ""
        returned = {}
        started_at = now()
        try:
            result = fetch_chunk(client, chunk, chunk_dir)
            status = result["status_code"]
            cost = result["x_request_cost"]
            elapsed = result["elapsed_seconds"]
            error = result["error"]
            returned = result["values"]
            for sku, values in returned.items():
                values_by_sku.setdefault(sku, {}).update(values)
        except RequestException as exc:
            error = str(exc)
        calls.append(
            {
                "chunk": index,
                "sku_count": len(chunk),
                "returned_sku_count": len(returned),
                "status_code": status,
                "elapsed_seconds": elapsed,
                "x_request_cost": cost,
                "started_at": started_at,
                "finished_at": now(),
                "error": error,
                "request_path": rel_path(chunk_dir / "request.json"),
                "response_path": rel_path(chunk_dir / "response.json"),
            }
        )
        value_count = sum(1 for values in returned.values() for field in AVAILABILITY_FIELDS if values.get(field))
        print(
            f"[availability_backfill:chunk] {index}/{len(chunks)} skus={len(chunk)} "
            f"status={status} returned={len(returned)} values={value_count} cost={cost}",
            flush=True,
        )
    return values_by_sku, calls


def apply_values(rows, row_to_sku, values_by_sku):
    updated = 0
    changed_fields = 0
    for index, row in enumerate(rows):
        sku = row_to_sku.get(index)
        if not sku:
            continue
        values = values_by_sku.get(sku) or {}
        row_changed = False
        for field in AVAILABILITY_FIELDS:
            value = values.get(field)
            if value and not compact_text(row.get(field)):
                row[field] = value
                row_changed = True
                changed_fields += 1
        if row_changed:
            updated += 1
    return updated, changed_fields


def main():
    started_at = now()
    final_rows = read_csv(FINAL_OUTPUT_CSV)
    detail_rows = read_csv(DETAIL_ROWS_CSV)
    targets = read_csv(TARGET_CSV)
    if not final_rows:
        raise RuntimeError(f"final_output.csv not found or empty: {FINAL_OUTPUT_CSV}")
    if not targets:
        raise RuntimeError(f"target CSV not found or empty: {TARGET_CSV}")
    for row in final_rows:
        ensure_item_from_url(row)
    for row in detail_rows:
        ensure_item_from_url(row)

    lookup = build_sku_lookup(targets)
    batch_indexes = [index for index, row in enumerate(final_rows) if compact_text(row.get("batch_id")) == BACKFILL_BATCH_ID]
    candidate_indexes = [index for index in batch_indexes if all_availability_blank(final_rows[index])]
    row_to_sku = {}
    missing_sku = []
    for index in candidate_indexes:
        sku = sku_for_row(final_rows[index], lookup)
        if sku:
            row_to_sku[index] = sku
        else:
            missing_sku.append(index)
    skus = unique_ordered(row_to_sku.values())
    run_dir = BACKFILL_ROOT / datetime.now().strftime("%Y%m%d_%H%M%S")
    raw_dir = run_dir / "raw"
    run_dir.mkdir(parents=True, exist_ok=True)

    estimated_calls = (len(skus) + CHUNK_SIZE - 1) // CHUNK_SIZE if skus else 0
    print(
        f"[availability_backfill:plan] batch={BACKFILL_BATCH_ID} final_rows={len(final_rows)} "
        f"batch_rows={len(batch_indexes)} blank_rows={len(candidate_indexes)} mapped_rows={len(row_to_sku)} skus={len(skus)} "
        f"chunk_size={CHUNK_SIZE} requested_chunk_size={REQUESTED_CHUNK_SIZE} "
        f"multi_sku={str(ALLOW_MULTI_SKU_FULFILLMENT).lower()} calls={estimated_calls} dry_run={str(DRY_RUN).lower()}",
        flush=True,
    )
    if missing_sku:
        print(f"[availability_backfill:missing_sku] rows={len(missing_sku)}", flush=True)

    values_by_sku = {}
    calls = []
    if skus and not DRY_RUN:
        values_by_sku, calls = fetch_availability(skus, raw_dir)

    detail_row_to_sku = {}
    for index, row in enumerate(detail_rows):
        if compact_text(row.get("batch_id")) != BACKFILL_BATCH_ID:
            continue
        if not all_availability_blank(row):
            continue
        sku = sku_for_row(row, lookup)
        if sku:
            detail_row_to_sku[index] = sku

    final_updated, final_changed_fields = apply_values(final_rows, row_to_sku, values_by_sku)
    detail_updated, detail_changed_fields = apply_values(detail_rows, detail_row_to_sku, values_by_sku)

    if not DRY_RUN:
        write_csv(FINAL_OUTPUT_CSV, final_rows, csv_fields(FINAL_OUTPUT_CSV, final_rows))
        if detail_rows:
            write_csv(DETAIL_ROWS_CSV, detail_rows, csv_fields(DETAIL_ROWS_CSV, detail_rows))

    call_cost = round(sum(float(call.get("x_request_cost") or 0) for call in calls), 7)
    manifest = {
        "run_type": "step08_availability_backfill",
        "started_at": started_at,
        "finished_at": now(),
        "batch_id": BACKFILL_BATCH_ID,
        "dry_run": DRY_RUN,
        "target_csv": rel_path(TARGET_CSV),
        "final_output_csv": rel_path(FINAL_OUTPUT_CSV),
        "detail_rows_csv": rel_path(DETAIL_ROWS_CSV),
        "batch_final_rows": len(batch_indexes),
        "blank_final_rows": len(candidate_indexes),
        "mapped_final_rows": len(row_to_sku),
        "missing_sku_rows": len(missing_sku),
        "sku_count": len(skus),
        "chunk_size": CHUNK_SIZE,
        "requested_chunk_size": REQUESTED_CHUNK_SIZE,
        "multi_sku_fulfillment_enabled": ALLOW_MULTI_SKU_FULFILLMENT,
        "call_count": len(calls) if calls else estimated_calls,
        "returned_sku_count": len(values_by_sku),
        "final_rows_updated": final_updated,
        "final_fields_updated": final_changed_fields,
        "detail_rows_updated": detail_updated,
        "detail_fields_updated": detail_changed_fields,
        "x_request_cost": call_cost,
        "estimated_krw_1550": round(call_cost * KRW_PER_USD, 2),
        "calls": calls,
    }
    (run_dir / "manifest.json").write_text(json.dumps(manifest, indent=2, ensure_ascii=False), encoding="utf-8")
    print(
        f"[availability_backfill:output] final_updated={final_updated} detail_updated={detail_updated} "
        f"returned_skus={len(values_by_sku)} cost_usd={call_cost} raw={rel_path(run_dir)}",
        flush=True,
    )


if __name__ == "__main__":
    main()
