import csv
import json
import os
import re
import smtplib
from datetime import datetime
from email.message import EmailMessage
from pathlib import Path

from .step00_config import DEFAULT_BESTBUY_RUN_ROOT, KRW_PER_USD, bestbuy_category, rel_path


CATEGORY = bestbuy_category()
RUN_ROOT = Path(os.getenv("BESTBUY_RUN_ROOT", DEFAULT_BESTBUY_RUN_ROOT))
OUTPUT_ROOT = Path(os.getenv("BESTBUY_OUTPUT_ROOT", RUN_ROOT / "output"))
FINAL_OUTPUT_CSV = Path(os.getenv("BESTBUY_FINAL_OUTPUT_CSV", OUTPUT_ROOT / "final_output.csv"))
MANIFEST_PATH = OUTPUT_ROOT / "email_notify_manifest.json"

CRITICAL_COLUMN_ALIASES = {
    "retailer_sku_name": ["retailer_sku_name"],
    "sku": ["sku", "sku_id", "item"],
    "final_sku_price": ["final_sku_price"],
}


def now():
    return datetime.now().isoformat(timespec="seconds")


def truthy(value, default=False):
    text = str(value if value is not None else ("1" if default else "0")).strip().lower()
    return text in {"1", "true", "yes", "y"}


def read_json(path):
    path = Path(path)
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except ValueError:
        return {}


def read_csv_rows(path):
    path = Path(path)
    if not path.exists():
        return []
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        return list(csv.DictReader(f))


def blank(value):
    if value is None:
        return True
    text = str(value).strip()
    return text == "" or text.lower() in {"null", "[null]", "none", "nan"}


def as_float(value):
    if value in ("", None):
        return 0.0
    try:
        return float(str(value).replace(",", "").strip())
    except ValueError:
        return 0.0


def as_int(value):
    if value in ("", None):
        return 0
    try:
        return int(float(str(value).replace(",", "").strip()))
    except ValueError:
        return 0


def money_krw(value):
    return f"{int(round(float(value or 0))):,}원"


def numeric_rank(value):
    match = re.search(r"\d+", str(value or ""))
    return int(match.group(0)) if match else 0


def unique_csv_count(path, key):
    seen = set()
    for row in read_csv_rows(path):
        value = str(row.get(key) or "").strip()
        if value:
            seen.add(value)
    return len(seen)


def latest_manifest_cost(root):
    manifests = sorted(Path(root).glob("*/manifest.json"), key=lambda item: item.stat().st_mtime)
    return sum(as_float(read_json(path).get("x_request_cost")) for path in manifests)


def manifest_costs(run_root):
    paths_and_keys = [
        (run_root / "main" / "manifest.json", ["total_x_request_cost"]),
        (run_root / "bsr" / "manifest.json", ["total_x_request_cost"]),
        (run_root / "main" / "manifest_main_targets.json", ["sponsored_cost_usd"]),
        (run_root / "promotion" / "summary.json", ["total_x_request_cost"]),
        (run_root / "detail" / "manifest_detail_enrichment.json", ["total_cost_usd_this_run"]),
    ]
    total = 0.0
    sources = []
    for path, keys in paths_and_keys:
        data = read_json(path)
        if not data:
            continue
        for key in keys:
            cost = as_float(data.get(key))
            if cost:
                total += cost
                sources.append({"path": rel_path(path), "key": key, "cost_usd": cost})
                break

    for path in sorted((run_root / "trending").glob("summary*.json")):
        data = read_json(path)
        key = "total_x_request_cost" if data.get("total_x_request_cost") not in ("", None) else "x_request_cost"
        cost = as_float(data.get(key))
        if cost:
            total += cost
            sources.append({"path": rel_path(path), "key": key, "cost_usd": cost})

    availability_cost = latest_manifest_cost(run_root / "availability_backfill")
    if availability_cost:
        total += availability_cost
        sources.append(
            {
                "path": rel_path(run_root / "availability_backfill"),
                "key": "x_request_cost",
                "cost_usd": availability_cost,
            }
        )
    return round(total, 7), sources


def db_manifest(run_root):
    return read_json(run_root / "output" / "db_load_manifest.json")


def final_targets_manifest(run_root):
    return read_json(run_root / "output" / "bestbuy_final_targets.manifest.json")


def insert_columns(db_data, rows):
    columns = list((db_data.get("final_output") or {}).get("columns") or [])
    if columns:
        return [column for column in columns if column != "id"]
    if rows:
        return [column for column in rows[0].keys() if column != "id"]
    return []


def row_url(row):
    return str(row.get("product_url") or row.get("url") or "").strip()


def first_present_column(rows, columns, candidates):
    available = set(columns)
    if rows:
        available.update(rows[0].keys())
    for candidate in candidates:
        if candidate in available:
            return candidate
    return ""


def all_null_column_issues(rows, columns):
    if not rows or not columns:
        return []
    all_null = [
        column
        for column in columns
        if column != "id" and all(blank(row.get(column)) for row in rows)
    ]
    if not all_null:
        return []
    return [f"전체 null 컬럼: {', '.join(all_null)}"]


def critical_null_issues(rows, columns):
    issues = []
    for logical_name, candidates in CRITICAL_COLUMN_ALIASES.items():
        column = first_present_column(rows, columns, candidates)
        if not column:
            issues.append(f"{logical_name} 컬럼이 final_output/db insert 대상에 없음")
            continue
        bad_rows = [row for row in rows if blank(row.get(column))]
        if not bad_rows:
            continue
        examples = []
        for row in bad_rows[:3]:
            url = row_url(row)
            item = str(row.get("item") or row.get("sku_id") or "").strip()
            if url:
                examples.append(f"{item} {url}".strip())
            elif item:
                examples.append(item)
        suffix = f", product_url: {' | '.join(examples)}" if examples else ""
        issues.append(f"{logical_name} {len(bad_rows)} rows null{suffix}")
    return issues


def db_count_issue(db_data, row_count):
    final = db_data.get("final_output") or {}
    expected = as_int(final.get("candidate_rows") or final.get("csv_rows") or row_count)
    inserted = as_int(final.get("inserted"))
    if inserted == 0 and "inserted" not in final:
        inserted = as_int(final.get("updated") or expected)
    if expected and inserted < expected:
        return f"DB insert rows 미달: {inserted}/{expected} success"
    return ""


def max_rank(rows, column):
    return max((numeric_rank(row.get(column)) for row in rows), default=0)


def listing_count_issues(category, run_root, rows, target_manifest):
    issues = []
    main_rank = max_rank(rows, "main_rank")
    if main_rank and main_rank < 300:
        issues.append(f"main_rank {main_rank}/300")
    bsr_rank = max_rank(rows, "bsr_rank")
    if bsr_rank and bsr_rank < 100:
        issues.append(f"bsr_rank {bsr_rank}/100")

    category = category.upper()
    if category in {"TV", "HHP"}:
        trend_count = as_int(target_manifest.get("trending_unique_count"))
        if not trend_count:
            trend_count = unique_csv_count(run_root / "trending" / "parsed" / "trending_products.csv", "sku_id")
        if trend_count < 10:
            issues.append(f"trend listing sku {trend_count}/10")

    if category == "TV":
        promotion_count = as_int(target_manifest.get("promotion_unique_count"))
        if not promotion_count:
            promotion_count = unique_csv_count(
                run_root / "promotion" / "parsed" / "all_promotion_products.csv",
                "sku_id",
            )
        if promotion_count < 18:
            issues.append(f"promotion listing sku {promotion_count}/18")
    return issues


def failure_detail_from_log(path, max_lines=8):
    path = Path(path or "")
    if not path.exists():
        return ""
    lines = [line.strip() for line in path.read_text(encoding="utf-8", errors="ignore").splitlines() if line.strip()]
    interesting = [
        line
        for line in lines
        if any(token in line for token in ("[fail]", "RuntimeError:", "Error:", "Exception:", "status=", "cost="))
    ]
    selected = (interesting or lines)[-max_lines:]
    return " / ".join(selected)


def step_failure_issues(status, failed_step, failed_step_name, log_path):
    if str(status or "").strip().lower() not in {"failed", "fail", "error"}:
        return []
    label = " ".join(part for part in [f"step{failed_step}" if failed_step else "", failed_step_name] if part)
    label = label or "step failure"
    detail = failure_detail_from_log(log_path)
    return [f"{label} failed" + (f": {detail}" if detail else "")]


def build_subject(category, issues):
    product = str(category or CATEGORY).strip().upper()
    if issues:
        return f"[SEA] [Warning] BBY {product} crawled"
    return f"[SEA] BBY {product} crawled"


def build_body(collected_count, cost_krw, issues):
    lines = [
        f"총 수집 {collected_count} sku",
        f"총 호출 비용 {money_krw(cost_krw)}(환율 {KRW_PER_USD:,}원 기준)",
    ]
    if issues:
        lines.append("특이사항")
        lines.extend(f"- {issue}" for issue in issues)
    else:
        lines.append("특이사항 없음")
    return "\n".join(lines)


def build_notification(category, run_root, status="success", failed_step="", failed_step_name="", log_path=""):
    rows = read_csv_rows(FINAL_OUTPUT_CSV if run_root == RUN_ROOT else run_root / "output" / "final_output.csv")
    db_data = db_manifest(run_root)
    target_manifest = final_targets_manifest(run_root)
    columns = insert_columns(db_data, rows)
    final_db = db_data.get("final_output") or {}
    collected_count = as_int(final_db.get("inserted"))
    if collected_count == 0 and "inserted" not in final_db:
        collected_count = as_int(final_db.get("updated") or len(rows))
    if not collected_count:
        collected_count = len(rows)

    issues = []
    issues.extend(step_failure_issues(status, failed_step, failed_step_name, log_path))
    if not rows:
        issues.append("final_output.csv rows 0 또는 파일 없음")
    issues.extend(all_null_column_issues(rows, columns))
    issues.extend(critical_null_issues(rows, columns))
    count_issue = db_count_issue(db_data, len(rows))
    if count_issue:
        issues.append(count_issue)
    issues.extend(listing_count_issues(category, run_root, rows, target_manifest))

    cost_usd, cost_sources = manifest_costs(run_root)
    cost_krw = round(cost_usd * KRW_PER_USD)
    subject = build_subject(category, issues)
    body = build_body(collected_count, cost_krw, issues)
    return {
        "subject": subject,
        "body": body,
        "issues": issues,
        "metrics": {
            "collected_count": collected_count,
            "cost_usd": cost_usd,
            "cost_krw": cost_krw,
            "krw_per_usd": KRW_PER_USD,
            "final_output_rows": len(rows),
            "db_insert_columns": columns,
            "cost_sources": cost_sources,
        },
    }


def first_env(names):
    for name in names:
        value = os.getenv(name, "").strip()
        if value:
            return value
    return ""


def email_config():
    config = {
        "smtp_server": first_env(["BESTBUY_SMTP_SERVER", "SMTP_SERVER"]),
        "smtp_port": first_env(["BESTBUY_SMTP_PORT", "SMTP_PORT"]) or 587,
        "sender_email": first_env(["BESTBUY_EMAIL_FROM", "SMTP_EMAIL", "SMTP_SENDER_EMAIL"]),
        "sender_password": first_env(["BESTBUY_EMAIL_PASSWORD", "SMTP_PASSWORD", "SMTP_SENDER_PASSWORD"]),
        "receiver_email": first_env(["BESTBUY_EMAIL_TO", "ALERT_EMAIL", "SMTP_RECEIVER_EMAIL"]),
        "source": "env",
    }
    try:
        config["smtp_port"] = int(config["smtp_port"])
    except (TypeError, ValueError):
        config["smtp_port"] = 587
    return config


def recipient_list(value):
    return [item.strip() for item in re.split(r"[;,]", str(value or "")) if item.strip()]


def send_email(subject, body, config):
    missing = [
        name
        for name in ("smtp_server", "smtp_port", "sender_email", "sender_password", "receiver_email")
        if not config.get(name)
    ]
    if missing:
        return False, f"email config missing: {', '.join(missing)}"
    message = EmailMessage()
    message["Subject"] = subject
    message["From"] = config["sender_email"]
    message["To"] = ", ".join(recipient_list(config["receiver_email"]))
    message.set_content(body, charset="utf-8")
    with smtplib.SMTP(config["smtp_server"], config["smtp_port"], timeout=30) as server:
        server.starttls()
        server.login(config["sender_email"], config["sender_password"])
        server.send_message(message)
    return True, ""


def write_manifest(payload):
    MANIFEST_PATH.parent.mkdir(parents=True, exist_ok=True)
    MANIFEST_PATH.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")


def main():
    started_at = now()
    category = CATEGORY
    run_root = RUN_ROOT
    status = os.getenv("BESTBUY_NOTIFY_STATUS", "success").strip().lower() or "success"
    notification = build_notification(
        category,
        run_root,
        status=status,
        failed_step=os.getenv("BESTBUY_NOTIFY_FAILED_STEP", ""),
        failed_step_name=os.getenv("BESTBUY_NOTIFY_FAILED_STEP_NAME", ""),
        log_path=os.getenv("BESTBUY_NOTIFY_LOG", ""),
    )
    config = email_config()
    enabled = truthy(os.getenv("BESTBUY_EMAIL_NOTIFY", "1"), default=True)
    dry_run = truthy(os.getenv("BESTBUY_EMAIL_DRY_RUN", "0"))
    sent = False
    skipped_reason = ""
    error = ""

    if not enabled:
        skipped_reason = "BESTBUY_EMAIL_NOTIFY=0"
    elif dry_run:
        skipped_reason = "BESTBUY_EMAIL_DRY_RUN=1"
    else:
        try:
            sent, skipped_reason = send_email(notification["subject"], notification["body"], config)
        except Exception as exc:
            error = f"{type(exc).__name__}: {exc}"

    manifest = {
        "run_type": "step16_email_notify",
        "started_at": started_at,
        "finished_at": now(),
        "category": category,
        "run_root": rel_path(run_root),
        "status": status,
        "subject": notification["subject"],
        "body": notification["body"],
        "issues": notification["issues"],
        "metrics": notification["metrics"],
        "email": {
            "enabled": enabled,
            "dry_run": dry_run,
            "sent": sent,
            "skipped_reason": skipped_reason,
            "error": error,
            "from": config.get("sender_email", ""),
            "to": config.get("receiver_email", ""),
            "config_source": config.get("source", ""),
        },
    }
    write_manifest(manifest)
    print(json.dumps(manifest, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()
