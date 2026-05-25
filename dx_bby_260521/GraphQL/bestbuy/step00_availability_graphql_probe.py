import json
import os
import re
import time
from datetime import datetime
from pathlib import Path

from requests import RequestException
from zenrows import ZenRowsClient

from .step00_config import DEFAULT_BESTBUY_RUN_ROOT, apply_bestbuy_location, bestbuy_store_id, bestbuy_zip_code
from .step08_detail_enrichment import (
    as_list,
    best_fulfillment_availability,
    best_shipping_availability,
    delivery_text,
    fallback_review20_payload,
    fastest_delivery_text,
    graphql_params,
    pickup_text,
    request_cost,
    target_url,
)


REQUEST_TIMEOUT = int(os.getenv("ZENROWS_TIMEOUT", "240"))
PROBE_ROOT = Path(os.getenv("BESTBUY_AVAILABILITY_PROBE_ROOT", DEFAULT_BESTBUY_RUN_ROOT / "availability_probe"))
PROBE_SKUS = [
    value.strip()
    for value in re.split(r"[\s,;]+", os.getenv("BESTBUY_AVAILABILITY_PROBE_SKUS", os.getenv("BESTBUY_DETAIL_SKUS", "6623791")))
    if value.strip()
]


PRODUCT_SCHEMA_WITH_FULFILLMENT_QUERY = (
    "query ProductSchemaAvailabilityProbe($skuId:String!$salesChannel:String!$fulfillmentInput:ProductFulfillmentInput!)"
    "{productBySkuId(skuId:$skuId){skuId bsin name{short}url{pdp}"
    "price(input:{salesChannel:$salesChannel}){customerPrice}"
    "fulfillmentOptions(input:$fulfillmentInput){buttonStates{buttonState displayText secondaryDisplayText}"
    "shippingDetails{shippingAvailability{shippingEligible defaultCustomerLosGroupId promiseByStreetDate "
    "customerLOSGroup{customerLosGroupId minLineItemMaxDate maxLineItemMaxDate name displayDateType price}}}"
    "deliveryDetails{deliveryAvailability{deliveryEligible deliverable deliverySlots{date}}}"
    "ispuDetails{ispuAvailability{pickupEligible instoreInventoryAvailable quantity minPickupInHours maxDate fulfillDate promiseByStreetDate}}}}}"
)


FULFILLMENT_DYNAMIC_QUERY = (
    "query FulfillmentOptionHook_FulfillmentDynamicQuery($skuId:String!$fulfillmentInput:ProductFulfillmentInput!"
    "$productPriceInput:ProductItemPriceInput!$openBoxCondition:Int){"
    "productBySkuId(skuId:$skuId openBoxCondition:$openBoxCondition){skuId bsin name{short}url{pdp}"
    "fulfillmentOptions(input:$fulfillmentInput){buttonStates{buttonState displayText secondaryDisplayText}"
    "shippingDetails{shippingAvailability{shippingEligible defaultCustomerLosGroupId promiseByStreetDate "
    "customerLOSGroup{customerLosGroupId minLineItemMaxDate maxLineItemMaxDate name displayDateType price}}}"
    "deliveryDetails{deliveryAvailability{deliveryEligible deliverable deliverySlots{date} installationSlots{date}}}"
    "ispuDetails{ispuAvailability{pickupEligible instoreInventoryAvailable quantity minPickupInHours maxDate fulfillDate promiseByStreetDate}}}}}"
)


def now():
    return datetime.now().isoformat(timespec="seconds")


def fulfillment_input(option_marker=None):
    zip_code = bestbuy_zip_code()
    store_id = bestbuy_store_id()
    variables = {
        "shipping": {
            "destinationZipCode": zip_code,
            "effectivePlanPaidMembership": "NULL",
        },
        "delivery": {
            "destinationZipCode": zip_code,
            "deliveryDateOption": "EARLIEST_AVAILABLE_DATE",
            "effectivePlanPaidMembership": "NULL",
        },
        "inStorePickup": {
            "storeId": store_id,
            "searchNearby": True,
            "showNearbyLocations": False,
        },
        "profileCode": None,
        "buttonState": {
            "fulfillmentOption": option_marker,
            "context": "PDP",
            "destinationZipCode": zip_code,
            "storeId": store_id,
            "effectivePlanPaidMembership": "NULL",
        },
    }
    return apply_bestbuy_location(variables)


def product_price_input():
    variables = {
        "customerAttributes": "",
        "salesChannel": "LargeView",
        "customerId": None,
        "planPaidMemberType": "NULL",
        "ct": "",
        "isStoreAgent": False,
        "locationId": "",
        "usePriceWithCart": True,
        "useCabo": True,
        "useSuco": True,
    }
    return variables


def product_schema_fulfillment_payload(sku):
    return {
        "operationName": "ProductSchemaAvailabilityProbe",
        "variables": {
            "skuId": str(sku),
            "salesChannel": "LargeView",
            "fulfillmentInput": fulfillment_input("PICKUP"),
        },
        "query": PRODUCT_SCHEMA_WITH_FULFILLMENT_QUERY,
    }


def fulfillment_dynamic_payload(sku, option_marker=None):
    return {
        "operationName": "FulfillmentOptionHook_FulfillmentDynamicQuery",
        "variables": {
            "skuId": str(sku),
            "fulfillmentInput": fulfillment_input(option_marker),
            "productPriceInput": product_price_input(),
        },
        "query": FULFILLMENT_DYNAMIC_QUERY,
    }


def response_item(response_json, index):
    if isinstance(response_json, list) and index < len(response_json):
        item = response_json[index]
        return item if isinstance(item, dict) else {}
    if index == 0 and isinstance(response_json, dict):
        return response_json
    return {}


def product_from_response(item):
    data = item.get("data") if isinstance(item, dict) else {}
    product = data.get("productBySkuId") if isinstance(data, dict) else {}
    return product if isinstance(product, dict) else {}


def availability_values(product):
    products = [product] if isinstance(product, dict) else []
    pickup = best_fulfillment_availability(
        products,
        "ispuDetails",
        "ispuAvailability",
        ("maxDate", "fulfillDate", "promiseByStreetDate"),
    )
    shipping = best_shipping_availability(products)
    delivery = best_fulfillment_availability(
        products,
        "deliveryDetails",
        "deliveryAvailability",
        ("deliverySlots",),
    )
    return {
        "pick_up_availability": pickup_text(pickup),
        "fastest_delivery": fastest_delivery_text(shipping),
        "delivery_availability": delivery_text(delivery),
    }


def error_summary(item):
    errors = item.get("errors") if isinstance(item, dict) else []
    output = []
    for error in as_list(errors):
        if not isinstance(error, dict):
            continue
        output.append(
            {
                "message": error.get("message", ""),
                "path": ".".join(str(part) for part in as_list(error.get("path"))),
                "code": (error.get("extensions") or {}).get("code", ""),
            }
        )
    return output


def probe_sku(client, sku):
    pdp_url = f"https://www.bestbuy.com/site/-/{sku}.p?skuId={sku}&intl=nosplash"
    request_payload = [
        fallback_review20_payload(sku),
        product_schema_fulfillment_payload(sku),
        fulfillment_dynamic_payload(sku, None),
        fulfillment_dynamic_payload(sku, "PICKUP"),
        fulfillment_dynamic_payload(sku, "SHIPPING"),
        fulfillment_dynamic_payload(sku, "DELIVERY"),
    ]
    labels = [
        "detail_control",
        "product_schema_fulfillment",
        "fulfillment_dynamic_default",
        "fulfillment_dynamic_pickup",
        "fulfillment_dynamic_shipping",
        "fulfillment_dynamic_delivery",
    ]
    started_at = now()
    start = time.perf_counter()
    response = client.post(
        "https://www.bestbuy.com/gateway/graphql",
        params=graphql_params(),
        headers={
            "accept": "application/json, text/plain, */*",
            "content-type": "application/json",
            "origin": "https://www.bestbuy.com",
            "referer": pdp_url,
            "x-client-id": "pdp-web",
        },
        data=json.dumps(request_payload),
        timeout=REQUEST_TIMEOUT,
    )
    text = response.text
    try:
        response_json = response.json()
    except ValueError:
        response_json = {}
    elapsed = round(time.perf_counter() - start, 3)

    rows = []
    for index, label in enumerate(labels):
        item = response_item(response_json, index)
        product = product_from_response(item)
        values = availability_values(product)
        rows.append(
            {
                "index": index,
                "label": label,
                "operation": request_payload[index].get("operationName", ""),
                "has_product": bool(product),
                "has_fulfillment_options": isinstance(product.get("fulfillmentOptions"), dict),
                "value_count": sum(1 for value in values.values() if value),
                "values": values,
                "errors": error_summary(item),
            }
        )

    run_dir = PROBE_ROOT / datetime.now().strftime("%Y%m%d_%H%M%S") / str(sku)
    run_dir.mkdir(parents=True, exist_ok=True)
    (run_dir / "request.json").write_text(json.dumps(request_payload, indent=2, ensure_ascii=False), encoding="utf-8")
    (run_dir / "response.txt").write_text(text, encoding="utf-8", errors="replace")
    (run_dir / "response.json").write_text(json.dumps(response_json, indent=2, ensure_ascii=False), encoding="utf-8")
    (run_dir / "summary.json").write_text(
        json.dumps(
            {
                "sku_id": str(sku),
                "url": pdp_url,
                "endpoint": "https://www.bestbuy.com/gateway/graphql",
                "http_call_count": 1,
                "status_code": response.status_code,
                "elapsed_seconds": elapsed,
                "x_request_cost": request_cost(response.headers),
                "started_at": started_at,
                "finished_at": now(),
                "rows": rows,
            },
            indent=2,
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    return run_dir, response.status_code, rows


def main():
    api_key = os.getenv("ZENROWS_API_KEY")
    if not api_key:
        raise RuntimeError("Set ZENROWS_API_KEY in .env")
    if not PROBE_SKUS:
        raise RuntimeError("Set BESTBUY_AVAILABILITY_PROBE_SKUS or BESTBUY_DETAIL_SKUS")
    client = ZenRowsClient(api_key)
    for sku in PROBE_SKUS:
        try:
            run_dir, status_code, rows = probe_sku(client, sku)
        except RequestException as exc:
            print(f"[availability_probe:error] sku={sku} error={exc}", flush=True)
            continue
        print(f"[availability_probe:call] sku={sku} endpoint=gateway_graphql http_calls=1 status={status_code}", flush=True)
        for row in rows:
            values = row["values"]
            errors = row["errors"]
            error_text = ";".join(
                f"{error.get('path')}:{error.get('code')}" for error in errors if error.get("path") or error.get("code")
            )
            print(
                "[availability_probe:op] "
                f"{row['index']} {row['label']} product={row['has_product']} "
                f"fulfillment={row['has_fulfillment_options']} values={row['value_count']} "
                f"pickup={values.get('pick_up_availability', '')!r} "
                f"fastest={values.get('fastest_delivery', '')!r} "
                f"delivery={values.get('delivery_availability', '')!r} "
                f"errors={error_text}",
                flush=True,
            )
        print(f"[availability_probe:raw] {run_dir}", flush=True)


if __name__ == "__main__":
    main()
