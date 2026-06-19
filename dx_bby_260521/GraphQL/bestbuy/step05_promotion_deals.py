import csv
import json
import os
import time
from datetime import datetime
from pathlib import Path

from zenrows import ZenRowsClient

from .step00_apollo import iter_apollo_push_payloads
from .step00_config import (
    DEFAULT_BESTBUY_RUN_ROOT,
    PROMOTION_LABELS,
    bestbuy_category,
    has_target_url,
    load_initial_urls,
    rel_path,
)
from .step00_parse_search import (
    delivery_availability_text,
    fastest_delivery_text,
    first_nested,
    listing_offer_count,
    money_text,
    pickup_availability_text,
    price_value,
    savings_money_text,
)

RUN_DATE = os.getenv("BESTBUY_RUN_DATE", datetime.now().strftime("%Y%m%d"))
RUN_ROOT = Path(os.getenv("BESTBUY_PROMOTION_RUN_ROOT", DEFAULT_BESTBUY_RUN_ROOT / "promotion"))
REQUEST_TIMEOUT = int(os.getenv("ZENROWS_TIMEOUT", "180"))
PROMOTION_MAX_ATTEMPTS = max(1, int(os.getenv("BESTBUY_PROMOTION_MAX_ATTEMPTS", "5")))
PROMOTION_EXPECTED_MIN_ROWS = max(0, int(os.getenv("BESTBUY_PROMOTION_EXPECTED_MIN_ROWS", "18")))
PROMOTION_RETRY_SLEEP_SECONDS = float(os.getenv("BESTBUY_PROMOTION_RETRY_SLEEP_SECONDS", "2"))
PROMOTION_RETRY_STATUS_CODES = {
    int(value)
    for value in os.getenv(
        "BESTBUY_PROMOTION_RETRY_STATUS_CODES", "408,409,422,425,429,500,502,503,504"
    )
    .replace(",", " ")
    .split()
    if value.strip().isdigit()
}
ENDPOINT = os.getenv("BESTBUY_GRAPHQL_ENDPOINT", "https://www.bestbuy.com/gateway/graphql")
PLACEMENT = os.getenv("BESTBUY_PROMOTION_PLACEMENT", "all")
REFERER = os.getenv("BESTBUY_PROMOTION_REFERER", load_initial_urls().get("promotion_tv_home_theater", ""))
QUERY_TEMPLATE_HTML = Path(
    os.getenv("BESTBUY_PROMOTION_QUERY_TEMPLATE_HTML", "references/bestbuy_promotion_page_sample.html")
)
EXCLUDED_PROMOTION_TYPES = {
    value.strip().lower()
    for value in os.getenv("BESTBUY_PROMOTION_EXCLUDE_TYPES", "Featured deals").split("|")
    if value.strip()
}


def now():
    return datetime.now().isoformat(timespec="seconds")


def find_started_operation_for_placement(html_text, placement):
    for payload in iter_apollo_push_payloads(html_text):
        for event in payload.get("events", []):
            if event.get("type") != "started":
                continue
            options = event.get("options") or {}
            variables = options.get("variables") or {}
            if variables.get("placement") == placement:
                query = options.get("query") or ""
                operation_name = query.split("{", 1)[0].replace("query", "", 1).strip().split("(", 1)[0]
                return {
                    "operationName": operation_name,
                    "variables": variables,
                    "query": query,
                }
    raise RuntimeError(f"Could not find operation for placement={placement}")


def promotion_type_for_placement(placement):
    return PROMOTION_LABELS.get(placement, placement)


def promotion_placement_excluded(placement):
    return promotion_type_for_placement(placement).strip().lower() in EXCLUDED_PROMOTION_TYPES


def extract_rows_from_response(response_json, placement):
    if promotion_placement_excluded(placement):
        return []
    promotion_type = promotion_type_for_placement(placement)
    rows = []
    deals = (((response_json.get("data") or {}).get("customer") or {}).get("deals") or {})
    for position, item in enumerate(deals.get("items") or [], 1):
        product = item.get("product") or item.get("featuredProduct") or {}
        sku_id = product.get("skuId")
        if not sku_id:
            continue
        name = product.get("name") or {}
        if isinstance(name, dict):
            name = name.get("short") or name.get("title") or ""
        url = product.get("url") or {}
        relative_url = url.get("relativePdp") if isinstance(url, dict) else ""
        price = product.get("price") if isinstance(product.get("price"), dict) else {}
        shipping = first_nested(product, ["fulfillmentOptions", "shippingDetails", "shippingAvailability"], {})
        delivery = first_nested(product, ["fulfillmentOptions", "deliveryDetails", "deliveryAvailability"], {})
        pickup = first_nested(product, ["fulfillmentOptions", "ispuDetails", "ispuAvailability"], {})
        customer_price = price_value(price, "displayableCustomerPrice", "customerPrice")
        regular_price = price_value(price, "displayableRegularPrice", "regularPrice")
        total_savings = price_value(price, "totalSavings")
        offer_count = listing_offer_count(product)
        rows.append(
            {
                "promotion_type": promotion_type,
                "promotion_placement": placement,
                "promotion_position": position,
                "sku_id": sku_id,
                "retailer_sku_name": name,
                "product_url": f"https://www.bestbuy.com{relative_url}" if relative_url else "",
                "customer_price": customer_price,
                "regular_price": regular_price,
                "total_savings": total_savings,
                "final_sku_price": money_text(customer_price),
                "original_sku_price": money_text(regular_price),
                "savings": savings_money_text(customer_price, regular_price, total_savings),
                "offer": offer_count,
                "offer_count": offer_count,
                "pick_up_availability": pickup_availability_text(pickup),
                "fastest_delivery": fastest_delivery_text(shipping, delivery),
                "delivery_availability": delivery_availability_text(delivery),
            }
        )
    return rows


def safe_part(value):
    return "".join(ch if ch.isalnum() or ch in "-_" else "_" for ch in str(value or "").strip()).strip("_") or "na"


def placement_folder(placement, status=None):
    raw_root = RUN_ROOT / "raw"
    placement_part = safe_part(placement)
    if status:
        folder = raw_root / f"{placement_part}_{status}"
        folder.mkdir(parents=True, exist_ok=True)
        return folder
    for suffix in ("success", "fail"):
        folder = raw_root / f"{placement_part}_{suffix}"
        if folder.exists():
            return folder
    return raw_root


def placement_artifact_paths(placement, status=None):
    folder = placement_folder(placement, status)
    placement_part = safe_part(placement)
    return {
        "folder": folder,
        "request": folder / f"{placement_part}_request.json",
        "response": folder / f"{placement_part}_response.txt",
        "headers": folder / f"{placement_part}_headers.json",
        "json": folder / f"{placement_part}_response.json",
    }


def attempt_status(status, attempt):
    return f"{status}_attempt_{attempt:02d}"


def cost_float(value):
    try:
        return float(value or 0)
    except (TypeError, ValueError):
        return 0.0


def retryable_summary(summary):
    try:
        status_code = int(summary.get("status_code") or 0)
    except (TypeError, ValueError):
        status_code = 0
    if status_code in PROMOTION_RETRY_STATUS_CODES:
        return True
    return status_code == 200 and int(summary.get("row_count") or 0) == 0


def sleep_before_retry(attempt):
    if attempt < PROMOTION_MAX_ATTEMPTS and PROMOTION_RETRY_SLEEP_SECONDS > 0:
        time.sleep(PROMOTION_RETRY_SLEEP_SECONDS)


def run_one(client, html_text, placement, attempt=1):
    payload = find_started_operation_for_placement(html_text, placement)

    start = time.perf_counter()
    response = client.post(
        ENDPOINT,
        params={
            "custom_headers": "true",
            "premium_proxy": "true",
            "proxy_country": "us",
            "js_render": "true",
        },
        headers={
            "accept": "application/json, text/plain, */*",
            "content-type": "application/json",
            "origin": "https://www.bestbuy.com",
            "referer": REFERER,
        },
        data=json.dumps(payload),
        timeout=REQUEST_TIMEOUT,
    )
    elapsed = round(time.perf_counter() - start, 3)
    text = response.text
    status = "success" if response.status_code == 200 else "fail"
    paths = placement_artifact_paths(placement, attempt_status(status, attempt))
    paths["request"].write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")
    paths["response"].write_text(text, encoding="utf-8", errors="replace")
    paths["headers"].write_text(json.dumps(dict(response.headers), indent=2, ensure_ascii=False), encoding="utf-8")

    response_json = {}
    parse_error = ""
    try:
        response_json = response.json()
        paths["json"].write_text(
            json.dumps(response_json, indent=2, ensure_ascii=False), encoding="utf-8"
        )
    except ValueError as exc:
        parse_error = str(exc)

    rows = extract_rows_from_response(response_json, placement)
    return {
        "summary": {
            "started_at": now(),
            "placement": placement,
            "promotion_type": promotion_type_for_placement(placement),
            "attempt": attempt,
            "status_code": response.status_code,
            "elapsed_seconds": elapsed,
            "x_request_cost": response.headers.get("x-request-cost", ""),
            "bytes": len(text or ""),
            "parse_error": parse_error,
            "row_count": len(rows),
            "artifact_folder": rel_path(paths["folder"]),
            "response_json_path": rel_path(paths["json"]) if response_json else "",
        },
        "rows": rows,
    }


def run_batch(client, html_text, placements, attempt=1):
    payloads = [find_started_operation_for_placement(html_text, placement) for placement in placements]

    start = time.perf_counter()
    response = client.post(
        ENDPOINT,
        params={
            "custom_headers": "true",
            "premium_proxy": "true",
            "proxy_country": "us",
            "js_render": "true",
        },
        headers={
            "accept": "application/json, text/plain, */*",
            "content-type": "application/json",
            "origin": "https://www.bestbuy.com",
            "referer": REFERER,
        },
        data=json.dumps(payloads),
        timeout=REQUEST_TIMEOUT,
    )
    elapsed = round(time.perf_counter() - start, 3)
    text = response.text
    status = "success" if response.status_code == 200 else "fail"
    paths = placement_artifact_paths("all_batch", attempt_status(status, attempt))
    paths["request"].write_text(json.dumps(payloads, indent=2, ensure_ascii=False), encoding="utf-8")
    paths["response"].write_text(text, encoding="utf-8", errors="replace")
    paths["headers"].write_text(json.dumps(dict(response.headers), indent=2, ensure_ascii=False), encoding="utf-8")

    parse_error = ""
    response_json = None
    try:
        response_json = response.json()
        paths["json"].write_text(
            json.dumps(response_json, indent=2, ensure_ascii=False), encoding="utf-8"
        )
    except ValueError as exc:
        parse_error = str(exc)

    response_items = response_json if isinstance(response_json, list) else []
    all_rows = []
    summaries = []
    for index, placement in enumerate(placements):
        item_json = response_items[index] if index < len(response_items) and isinstance(response_items[index], dict) else {}
        rows = extract_rows_from_response(item_json, placement)
        all_rows.extend(rows)
        summaries.append(
            {
                "started_at": now(),
                "placement": placement,
                "promotion_type": promotion_type_for_placement(placement),
                "attempt": attempt,
                "status_code": response.status_code,
                "elapsed_seconds": elapsed,
                "x_request_cost": response.headers.get("x-request-cost", ""),
                "bytes": len(text or ""),
                "parse_error": parse_error,
                "row_count": len(rows),
                "artifact_folder": rel_path(paths["folder"]),
                "response_json_path": rel_path(paths["json"]) if response_json is not None else "",
                "batch_index": index,
            }
        )

    return {"summaries": summaries, "rows": all_rows}


def batch_attempt_summary(result, attempt):
    summaries = result.get("summaries") or []
    first = summaries[0] if summaries else {}
    return {
        "mode": "batch",
        "attempt": attempt,
        "placements": [summary.get("placement") for summary in summaries],
        "status_code": first.get("status_code", ""),
        "x_request_cost": first.get("x_request_cost", ""),
        "bytes": first.get("bytes", ""),
        "row_count": len(result.get("rows") or []),
        "artifact_folder": first.get("artifact_folder", ""),
        "retryable": any(retryable_summary(summary) for summary in summaries),
    }


def single_attempt_summary(result, attempt):
    summary = result.get("summary") or {}
    return {
        "mode": "single",
        "attempt": attempt,
        "placement": summary.get("placement", ""),
        "status_code": summary.get("status_code", ""),
        "x_request_cost": summary.get("x_request_cost", ""),
        "bytes": summary.get("bytes", ""),
        "row_count": len(result.get("rows") or []),
        "artifact_folder": summary.get("artifact_folder", ""),
        "retryable": retryable_summary(summary),
    }


def dedupe_promotion_rows(rows):
    seen = set()
    deduped = []
    for row in rows:
        key = (row.get("promotion_placement") or "", row.get("sku_id") or "")
        if key in seen:
            continue
        seen.add(key)
        deduped.append(row)
    return deduped


def run_batch_with_retries(client, html_text, placements):
    attempts = []
    last_result = {"summaries": [], "rows": []}
    for attempt in range(1, PROMOTION_MAX_ATTEMPTS + 1):
        last_result = run_batch(client, html_text, placements, attempt=attempt)
        attempt_info = batch_attempt_summary(last_result, attempt)
        attempts.append(attempt_info)
        if last_result.get("rows"):
            break
        if not attempt_info["retryable"]:
            break
        sleep_before_retry(attempt)
    return {
        "summaries": last_result.get("summaries") or [],
        "rows": last_result.get("rows") or [],
        "attempts": attempts,
        "call_count": len(attempts),
        "total_x_request_cost": sum(cost_float(attempt.get("x_request_cost")) for attempt in attempts),
    }


def run_one_with_retries(client, html_text, placement):
    attempts = []
    last_result = {"summary": {}, "rows": []}
    for attempt in range(1, PROMOTION_MAX_ATTEMPTS + 1):
        last_result = run_one(client, html_text, placement, attempt=attempt)
        attempt_info = single_attempt_summary(last_result, attempt)
        attempts.append(attempt_info)
        if last_result.get("rows"):
            break
        if not attempt_info["retryable"]:
            break
        sleep_before_retry(attempt)
    return {
        "summary": last_result.get("summary") or {},
        "rows": last_result.get("rows") or [],
        "attempts": attempts,
        "call_count": len(attempts),
        "total_x_request_cost": sum(cost_float(attempt.get("x_request_cost")) for attempt in attempts),
    }


def write_rows(path, rows):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=[
                "promotion_type",
                "promotion_placement",
                "promotion_position",
                "sku_id",
                "retailer_sku_name",
                "product_url",
                "customer_price",
                "regular_price",
                "total_savings",
                "final_sku_price",
                "original_sku_price",
                "savings",
                "offer",
                "offer_count",
                "pick_up_availability",
                "fastest_delivery",
                "delivery_availability",
            ],
        )
        writer.writeheader()
        writer.writerows(rows)


def main():
    category = bestbuy_category()
    if category == "HHP" or not has_target_url("promotion"):
        summary = {
            "started_at": now(),
            "skipped": True,
            "reason": "HHP promotion page is not collected" if category == "HHP" else "no promotion URL for category",
            "placements": [],
            "call_count": 0,
            "row_count": 0,
            "total_x_request_cost": 0,
        }
        RUN_ROOT.mkdir(parents=True, exist_ok=True)
        write_rows(RUN_ROOT / "parsed" / "all_promotion_products.csv", [])
        (RUN_ROOT / "summary.json").write_text(json.dumps(summary, indent=2, ensure_ascii=False), encoding="utf-8")
        print(json.dumps(summary, indent=2, ensure_ascii=False))
        return
    requested_placements = list(PROMOTION_LABELS) if PLACEMENT.lower() == "all" else [PLACEMENT]
    excluded_placements = [placement for placement in requested_placements if promotion_placement_excluded(placement)]
    excluded_set = set(excluded_placements)
    placements = [placement for placement in requested_placements if placement not in excluded_set]
    if not placements:
        summary = {
            "started_at": now(),
            "placements": [],
            "excluded_placements": excluded_placements,
            "call_count": 0,
            "row_count": 0,
            "total_x_request_cost": 0,
            "summaries": [],
            "csv": rel_path(RUN_ROOT / "parsed" / "all_promotion_products.csv"),
        }
        RUN_ROOT.mkdir(parents=True, exist_ok=True)
        write_rows(RUN_ROOT / "parsed" / "all_promotion_products.csv", [])
        (RUN_ROOT / "summary.json").write_text(json.dumps(summary, indent=2, ensure_ascii=False), encoding="utf-8")
        print(json.dumps(summary, indent=2, ensure_ascii=False))
        return

    api_key = os.getenv("ZENROWS_API_KEY")
    if not api_key:
        raise RuntimeError("Set ZENROWS_API_KEY in .env")
    html_text = QUERY_TEMPLATE_HTML.read_text(encoding="utf-8", errors="ignore")
    client = ZenRowsClient(api_key)

    fallback_to_single = False
    attempts = []
    total_x_request_cost = 0.0
    if PLACEMENT.lower() == "all":
        result = run_batch_with_retries(client, html_text, placements)
        all_rows = result["rows"]
        attempts.extend(result["attempts"])
        total_x_request_cost += result["total_x_request_cost"]
        latest_by_placement = {summary.get("placement"): summary for summary in result["summaries"]}
        collected_placements = {row.get("promotion_placement") for row in all_rows if row.get("promotion_placement")}
        if PROMOTION_EXPECTED_MIN_ROWS and len(all_rows) < PROMOTION_EXPECTED_MIN_ROWS:
            missing_placements = placements
        else:
            missing_placements = [placement for placement in placements if placement not in collected_placements]
        if missing_placements:
            fallback_to_single = True
            for placement in missing_placements:
                single_result = run_one_with_retries(client, html_text, placement)
                all_rows.extend(single_result["rows"])
                all_rows = dedupe_promotion_rows(all_rows)
                attempts.extend(single_result["attempts"])
                total_x_request_cost += single_result["total_x_request_cost"]
                latest_by_placement[placement] = single_result["summary"]
        summaries = [latest_by_placement.get(placement, {}) for placement in placements]
        call_count = len(attempts)
    else:
        result = run_one_with_retries(client, html_text, placements[0])
        all_rows = result["rows"]
        summaries = [result["summary"]]
        attempts.extend(result["attempts"])
        total_x_request_cost += result["total_x_request_cost"]
        call_count = len(attempts)

    slug = "all" if PLACEMENT.lower() == "all" else PLACEMENT
    out_csv = RUN_ROOT / "parsed" / f"{slug}_promotion_products.csv"
    write_rows(out_csv, all_rows)
    summary = {
        "started_at": now(),
        "placements": placements,
        "excluded_placements": excluded_placements,
        "call_count": call_count,
        "row_count": len(all_rows),
        "total_x_request_cost": round(total_x_request_cost, 7),
        "max_attempts": PROMOTION_MAX_ATTEMPTS,
        "expected_min_rows": PROMOTION_EXPECTED_MIN_ROWS if PLACEMENT.lower() == "all" else 0,
        "fallback_to_single": fallback_to_single,
        "attempts": attempts,
        "summaries": summaries,
        "csv": rel_path(out_csv),
    }
    (RUN_ROOT / "summary.json").write_text(json.dumps(summary, indent=2, ensure_ascii=False), encoding="utf-8")
    print(json.dumps(summary, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()
