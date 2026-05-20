import csv
import json
import os
from datetime import datetime
from pathlib import Path

from .step00_config import DEFAULT_BESTBUY_RUN_ROOT, bestbuy_category, db_config, rel_path


TARGET_SCHEMA = "public"
CATEGORY = bestbuy_category()
RUN_ROOT = Path(DEFAULT_BESTBUY_RUN_ROOT)
DETAIL_ROWS_CSV = RUN_ROOT / "detail" / "parsed" / "detail_enriched_rows.csv"
FINAL_OUTPUT_CSV = RUN_ROOT / "output" / "final_output.csv"
MANIFEST_PATH = RUN_ROOT / "output" / "item_mst_manifest.json"


def now():
    return datetime.now().isoformat(timespec="seconds")


def quote_ident(value):
    return '"' + str(value).replace('"', '""') + '"'


def item_mst_table():
    category_key = CATEGORY.upper()
    if category_key == "HHP":
        return (
            os.getenv(f"BESTBUY_ITEM_MST_TABLE_{category_key}")
            or "hhp_item_mst"
        )
    if category_key != "TV":
        return ""
    return (
        os.getenv(f"BESTBUY_ITEM_MST_TABLE_{category_key}")
        or "tv_item_mst"
    )


def read_csv(path):
    path = Path(path)
    if not path.exists():
        return []
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        return list(csv.DictReader(f))


def clean(value):
    value = str(value or "").strip()
    return value or None


def table_columns(cur, table_name):
    cur.execute(
        """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = %s AND table_name = %s
        """,
        (TARGET_SCHEMA, table_name),
    )
    return {row[0] for row in cur.fetchall()}


def source_rows():
    rows = read_csv(DETAIL_ROWS_CSV)
    if rows:
        return rows, DETAIL_ROWS_CSV
    return read_csv(FINAL_OUTPUT_CSV), FINAL_OUTPUT_CSV


def record_score(record):
    if CATEGORY.upper() == "HHP":
        fields = ("sku", "product_url", "hhp_carrier", "hhp_color", "hhp_storage")
    else:
        fields = ("sku", "product_url", "screen_size", "estimated_annual_electricity_use")
    return sum(1 for field in fields if record.get(field))


def item_records(rows):
    by_item = {}
    for row in rows:
        item = clean(row.get("item"))
        if not item:
            continue
        record = {
            "item": item,
            "account_name": clean(row.get("account_name")) or "Bestbuy",
            "product_url": clean(row.get("product_url")),
            "sku": clean(row.get("sku")),
            "screen_size": clean(row.get("screen_size")),
            "estimated_annual_electricity_use": clean(row.get("estimated_annual_electricity_use")),
            "hhp_carrier": clean(row.get("hhp_carrier")),
            "hhp_color": clean(row.get("hhp_color")),
            "hhp_storage": clean(row.get("hhp_storage")),
            "is_product": True,
            "is_checked": True,
        }
        current = by_item.get(item)
        if current is None or record_score(record) > record_score(current):
            by_item[item] = record
    return list(by_item.values())


def insert_record(cur, table_name, columns, record, timestamp):
    values = {
        "item": record["item"],
        "account_name": record["account_name"],
        "product_url": record["product_url"],
        "sku": record["sku"],
        "screen_size": record["screen_size"],
        "estimated_annual_electricity_use": record["estimated_annual_electricity_use"],
        "hhp_carrier": record["hhp_carrier"],
        "hhp_color": record["hhp_color"],
        "hhp_storage": record["hhp_storage"],
        "is_product": record["is_product"],
        "is_checked": record["is_checked"],
        "created_at": timestamp,
        "updated_at": timestamp,
    }
    insert_columns = [name for name in values if name in columns]
    sql = (
        f"INSERT INTO {quote_ident(TARGET_SCHEMA)}.{quote_ident(table_name)} "
        f"({', '.join(quote_ident(name) for name in insert_columns)}) "
        f"VALUES ({', '.join(['%s'] * len(insert_columns))})"
    )
    cur.execute(sql, [values[name] for name in insert_columns])


def existing_blank(value):
    return value is None or str(value).strip() == ""


def key_where(columns):
    if "account_name" in columns:
        return f"{quote_ident('item')} = %s AND {quote_ident('account_name')} = %s", [
            "item",
            "account_name",
        ]
    return f"{quote_ident('item')} = %s", ["item"]


def update_record(cur, table_name, columns, existing, record, timestamp):
    updates = []
    params = []
    always_update = {"product_url"} if CATEGORY.upper() == "HHP" else set()
    for field in (
        "account_name",
        "product_url",
        "sku",
        "screen_size",
        "estimated_annual_electricity_use",
        "hhp_carrier",
        "hhp_color",
        "hhp_storage",
    ):
        if field not in columns:
            continue
        if record.get(field) and (field in always_update or existing_blank(existing.get(field))):
            updates.append(f"{quote_ident(field)} = %s")
            params.append(record[field])
    for field in ("is_product", "is_checked"):
        if field not in columns:
            continue
        if existing.get(field) is None:
            updates.append(f"{quote_ident(field)} = %s")
            params.append(record[field])
    if not updates:
        return False
    if "updated_at" in columns:
        updates.append(f"{quote_ident('updated_at')} = %s")
        params.append(timestamp)
    where_sql, key_fields = key_where(columns)
    params.extend(record[field] for field in key_fields)
    sql = (
        f"UPDATE {quote_ident(TARGET_SCHEMA)}.{quote_ident(table_name)} "
        f"SET {', '.join(updates)} WHERE {where_sql}"
    )
    cur.execute(sql, params)
    return cur.rowcount > 0


def upsert_records(cur, table_name, records):
    columns = table_columns(cur, table_name)
    if not columns:
        raise RuntimeError(f"DB table not found or has no columns: {TARGET_SCHEMA}.{table_name}")
    required = {"item"}
    missing = sorted(required - columns)
    if missing:
        raise RuntimeError(f"{TARGET_SCHEMA}.{table_name} missing required columns: {missing}")

    inserted = 0
    updated = 0
    skipped = 0
    timestamp = datetime.now()
    select_columns = [
        name
        for name in (
            "item",
            "account_name",
            "product_url",
            "sku",
            "screen_size",
            "estimated_annual_electricity_use",
            "hhp_carrier",
            "hhp_color",
            "hhp_storage",
            "is_product",
            "is_checked",
        )
        if name in columns
    ]
    where_sql, key_fields = key_where(columns)
    select_sql = (
        f"SELECT {', '.join(quote_ident(name) for name in select_columns)} "
        f"FROM {quote_ident(TARGET_SCHEMA)}.{quote_ident(table_name)} "
        f"WHERE {where_sql}"
    )

    for record in records:
        cur.execute(select_sql, [record[field] for field in key_fields])
        row = cur.fetchone()
        if row is None:
            insert_record(cur, table_name, columns, record, timestamp)
            inserted += 1
            continue
        existing = dict(zip(select_columns, row))
        if update_record(cur, table_name, columns, existing, record, timestamp):
            updated += 1
        else:
            skipped += 1
    return {
        "inserted": inserted,
        "updated_blank_fields": updated,
        "skipped_existing": skipped,
        "columns_available": sorted(columns),
    }


def main():
    import psycopg2

    started_at = now()
    table_name = item_mst_table()
    rows, source_csv = source_rows()
    records = item_records(rows)
    manifest = {
        "run_type": "step15_item_mst_load",
        "started_at": started_at,
        "finished_at": "",
        "category": CATEGORY,
        "run_root": rel_path(RUN_ROOT),
        "source_csv": rel_path(source_csv),
        "source_rows": len(rows),
        "unique_items": len(records),
        "table": f"{TARGET_SCHEMA}.{table_name}" if table_name else "",
        "skipped": "",
    }

    if CATEGORY.upper() not in {"TV", "HHP"}:
        manifest.update({"skipped": "item_mst_load is only configured for TV/HHP"})
        manifest["finished_at"] = now()
        MANIFEST_PATH.write_text(json.dumps(manifest, indent=2, ensure_ascii=False), encoding="utf-8")
        print(json.dumps(manifest, indent=2, ensure_ascii=False))
        return
    if not rows:
        raise RuntimeError(f"Missing item mst source CSV: {DETAIL_ROWS_CSV} or {FINAL_OUTPUT_CSV}")

    config = db_config()
    if not config:
        raise RuntimeError("DB_CONFIG is missing")

    conn = psycopg2.connect(
        host=config.get("host"),
        port=int(config.get("port") or 5432),
        user=config.get("user"),
        password=config.get("password"),
        dbname=config.get("database"),
        connect_timeout=10,
    )
    with conn:
        with conn.cursor() as cur:
            result = upsert_records(cur, table_name, records)
    conn.close()

    manifest.update(result)
    manifest["finished_at"] = now()
    MANIFEST_PATH.write_text(json.dumps(manifest, indent=2, ensure_ascii=False), encoding="utf-8")
    print(json.dumps(manifest, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()
