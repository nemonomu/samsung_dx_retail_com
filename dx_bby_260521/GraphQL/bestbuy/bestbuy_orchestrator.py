import argparse
import csv
import json
import os
import subprocess
import sys
from dataclasses import dataclass, field
from pathlib import Path

from .step00_config import DEFAULT_BESTBUY_RUN_ROOT, bestbuy_dated_run_root, has_target_url


PYTHON = sys.executable
TARGET_SIZE = 300
CATEGORY_SEARCH_TERMS = {
    "HHP": "cellphone",
    "REF": "refrigerator",
    "LDY": "washing machine",
}


@dataclass(frozen=True)
class Step:
    number: int
    name: str
    module: str
    env: dict[str, str] = field(default_factory=dict)
    resume_env: dict[str, str] = field(default_factory=dict)
    implemented: bool = True

    @property
    def key(self):
        return f"{self.number:02d}"


STEPS = [
    Step(
        1,
        "main_list",
        "bestbuy.step01_main_list",
        {
            "BESTBUY_MAIN_PAGES": "16",
            "BESTBUY_MAIN_RUN_ID": "main",
            "BESTBUY_MAIN_ORGANIC_OFFSET": "18",
            "BESTBUY_GRAPHQL_PREMIUM_PROXY": "1",
            "BESTBUY_GRAPHQL_JS_RENDER": "1",
            "BESTBUY_SANITIZE_PRODUCT_LIST_QUERY": "0",
            "BESTBUY_STRIP_PRODUCT_LIST_FULFILLMENT": "0",
            "ZENROWS_TIMEOUT": "180",
        },
    ),
    Step(
        2,
        "main_targets",
        "bestbuy.step02_main_targets",
        {
            "BESTBUY_MAIN_TARGET_RUN_ID": "main",
            "BESTBUY_FETCH_SPONSORED_ENRICHMENT": "0",
            "BESTBUY_GRAPHQL_PREMIUM_PROXY": "1",
            "BESTBUY_GRAPHQL_JS_RENDER": "1",
            "ZENROWS_TIMEOUT": "180",
        },
    ),
    Step(
        3,
        "bsr_list",
        "bestbuy.step03_bsr_list",
        {
            "BESTBUY_MAIN_PAGES": "2",
            "BESTBUY_MAIN_RUN_ID": "bsr",
            "BESTBUY_MAIN_ORGANIC_OFFSET": "72",
            "BESTBUY_SEARCH_SORT": "Best-Selling",
            "BESTBUY_GRAPHQL_PREMIUM_PROXY": "1",
            "BESTBUY_GRAPHQL_JS_RENDER": "1",
            "BESTBUY_SANITIZE_PRODUCT_LIST_QUERY": "0",
            "BESTBUY_STRIP_PRODUCT_LIST_FULFILLMENT": "0",
            "ZENROWS_TIMEOUT": "180",
        },
    ),
    Step(4, "bsr_rank", "bestbuy.step04_bsr_rank"),
    Step(
        5,
        "promotion_deals",
        "bestbuy.step05_promotion_deals",
        {"BESTBUY_PROMOTION_PLACEMENT": "all", "ZENROWS_TIMEOUT": "180"},
    ),
    Step(6, "trending_deals", "bestbuy.step06_trending_deals"),
    Step(
        7,
        "final_targets",
        "bestbuy.step07_final_targets",
        {
            "BESTBUY_FINAL_MAIN_RUN_ID": "main",
            "BESTBUY_FINAL_BSR_RUN_ID": "bsr",
            "BESTBUY_FINAL_TARGET_SIZE": "300",
            "BESTBUY_MAIN_RANK_LIMIT": "300",
            "BESTBUY_BSR_RANK_LIMIT": "100",
            "BESTBUY_FINAL_ROW_LIMIT": "0",
        },
    ),
    Step(
        8,
        "detail_html",
        "bestbuy.step08_detail_enrichment",
        {
            "BESTBUY_DETAIL_STAGE": "detail",
            "BESTBUY_DETAIL_FETCH_COMPARE": "1",
            "BESTBUY_DETAIL_JSON_RESPONSE": "1",
            "BESTBUY_DETAIL_SCROLL": "1",
            "BESTBUY_DETAIL_SCROLL_NETWORK_IDLE": "1",
            "BESTBUY_DETAIL_COMPARE_CAPTURE_HOOK": "1",
            "BESTBUY_DETAIL_COMPARE_SCROLL_SCAN": "1",
            "BESTBUY_DETAIL_COMPARE_DOM_OBSERVER": "1",
            "BESTBUY_DETAIL_COMPARE_FORCE_FETCH": "1",
            "BESTBUY_DETAIL_COMPARE_FORCE_FETCH_WAIT": "2500",
            "BESTBUY_DETAIL_REQUIRE_SIMILAR": "1",
            "BESTBUY_DETAIL_RETRY_ON_MISSING_SIMILAR": "0",
            "BESTBUY_DETAIL_RETRY_ONLY": "0",
            "BESTBUY_DETAIL_REBUILD_ONLY": "0",
            "BESTBUY_DETAIL_RETRY_MISSING_SIMILAR": "0",
            "BESTBUY_DETAIL_LIMIT": "0",
            "BESTBUY_DETAIL_SKUS": "",
            "BESTBUY_DETAIL_FETCH_GET_IT_FAST": "0",
            "BESTBUY_DETAIL_FETCH_FULFILLMENT_DYNAMIC": "0",
            "BESTBUY_DETAIL_MAX_ATTEMPTS": "1",
            "BESTBUY_DETAIL_SKU_BATCH_SIZE": "5",
            "BESTBUY_DETAIL_USE_DB_SELECTORS": "0",
            "BESTBUY_DETAIL_WORKERS": "3",
            "ZENROWS_TIMEOUT": "240",
        },
        {
            "BESTBUY_DETAIL_STAGE": "detail",
            "BESTBUY_DETAIL_FETCH_COMPARE": "1",
            "BESTBUY_DETAIL_JSON_RESPONSE": "1",
            "BESTBUY_DETAIL_SCROLL": "1",
            "BESTBUY_DETAIL_SCROLL_NETWORK_IDLE": "1",
            "BESTBUY_DETAIL_COMPARE_CAPTURE_HOOK": "1",
            "BESTBUY_DETAIL_COMPARE_SCROLL_SCAN": "1",
            "BESTBUY_DETAIL_COMPARE_DOM_OBSERVER": "1",
            "BESTBUY_DETAIL_COMPARE_FORCE_FETCH": "1",
            "BESTBUY_DETAIL_COMPARE_FORCE_FETCH_WAIT": "2500",
            "BESTBUY_DETAIL_REQUIRE_SIMILAR": "1",
            "BESTBUY_DETAIL_RETRY_ON_MISSING_SIMILAR": "0",
            "BESTBUY_DETAIL_RETRY_ONLY": "0",
            "BESTBUY_DETAIL_REBUILD_ONLY": "0",
            "BESTBUY_DETAIL_RETRY_MISSING_SIMILAR": "0",
            "BESTBUY_DETAIL_LIMIT": "0",
            "BESTBUY_DETAIL_SKUS": "",
            "BESTBUY_DETAIL_FETCH_GET_IT_FAST": "0",
            "BESTBUY_DETAIL_FETCH_FULFILLMENT_DYNAMIC": "0",
            "BESTBUY_DETAIL_MAX_ATTEMPTS": "1",
            "BESTBUY_DETAIL_SKU_BATCH_SIZE": "5",
            "BESTBUY_DETAIL_USE_DB_SELECTORS": "0",
            "BESTBUY_DETAIL_WORKERS": "3",
            "BESTBUY_DETAIL_RETRY_ONLY": "1",
            "ZENROWS_TIMEOUT": "240",
        },
    ),
    Step(
        9,
        "review20",
        "bestbuy.step09_review20",
        {
            "BESTBUY_DETAIL_WORKERS": "3",
            "BESTBUY_DETAIL_MAX_ATTEMPTS": "1",
            "BESTBUY_DETAIL_LIMIT": "0",
            "BESTBUY_DETAIL_SKUS": "",
            "BESTBUY_DETAIL_REBUILD_ONLY": "0",
            "BESTBUY_DETAIL_RETRY_MISSING_SIMILAR": "0",
            "BESTBUY_DETAIL_FETCH_GET_IT_FAST": "0",
            "BESTBUY_DETAIL_FETCH_FULFILLMENT_DYNAMIC": "0",
            "ZENROWS_TIMEOUT": "240",
        },
        {
            "BESTBUY_DETAIL_RETRY_ONLY": "1",
            "BESTBUY_DETAIL_WORKERS": "3",
            "BESTBUY_DETAIL_MAX_ATTEMPTS": "1",
            "BESTBUY_DETAIL_LIMIT": "0",
            "BESTBUY_DETAIL_SKUS": "",
            "BESTBUY_DETAIL_REBUILD_ONLY": "0",
            "BESTBUY_DETAIL_RETRY_MISSING_SIMILAR": "0",
            "BESTBUY_DETAIL_FETCH_GET_IT_FAST": "0",
            "BESTBUY_DETAIL_FETCH_FULFILLMENT_DYNAMIC": "0",
            "ZENROWS_TIMEOUT": "240",
        },
    ),
    Step(
        10,
        "availability_backfill",
        "bestbuy.step08_availability_backfill",
        {
            "BESTBUY_AVAILABILITY_BACKFILL_CHUNK_SIZE": "1",
            "BESTBUY_AVAILABILITY_BACKFILL_ALLOW_MULTI_SKU": "0",
            "BESTBUY_AVAILABILITY_BACKFILL_CANDIDATE_MODE": "all_rows",
            "BESTBUY_AVAILABILITY_BACKFILL_OVERWRITE": "1",
            "BESTBUY_AVAILABILITY_BACKFILL_CLEAR_EXISTING_FIELDS": "1",
            "BESTBUY_AVAILABILITY_BACKFILL_SKIP": "0",
            "BESTBUY_AVAILABILITY_BACKFILL_LIMIT": "0",
            "BESTBUY_AVAILABILITY_BACKFILL_TIMEOUT": "180",
            "ZENROWS_TIMEOUT": "180",
        },
    ),
    Step(11, "status_check", "bestbuy.step10_status_check"),
    Step(12, "s3_sync", "bestbuy.step11_s3_sync"),
    Step(13, "local_cleanup", "bestbuy.step12_local_cleanup"),
    Step(14, "db_prepare", "bestbuy.step13_db_prepare"),
    Step(15, "db_load", "bestbuy.step14_db_load"),
]


def run_root(env=None):
    merged = os.environ.copy()
    if env:
        merged.update(env)
    return Path(merged.get("BESTBUY_RUN_ROOT", DEFAULT_BESTBUY_RUN_ROOT))


def apply_run_path_env(env):
    root = Path(env.get("BESTBUY_RUN_ROOT", DEFAULT_BESTBUY_RUN_ROOT))
    output_root = root / "output"
    detail_root = root / "detail"
    derived_paths = {
        "BESTBUY_OUTPUT_ROOT": output_root,
        "BESTBUY_DETAIL_RUN_ROOT": detail_root,
        "BESTBUY_DETAIL_TARGET_CSV": output_root / "bestbuy_final_targets.csv",
        "BESTBUY_FINAL_OUTPUT_CSV": output_root / "final_output.csv",
        "BESTBUY_PRODUCT_LIST_OUTPUT": output_root / "bestbuy_product_list.csv",
        "BESTBUY_AVAILABILITY_BACKFILL_FINAL_CSV": output_root / "final_output.csv",
        "BESTBUY_AVAILABILITY_BACKFILL_DETAIL_ROWS_CSV": detail_root / "parsed" / "detail_enriched_rows.csv",
        "BESTBUY_AVAILABILITY_BACKFILL_ROOT": root / "availability_backfill",
    }
    force_paths = env.get("BESTBUY_FORCE_RUN_PATH_ENV", "1").lower() in {"1", "true", "yes", "y"}
    for key, path in derived_paths.items():
        if force_paths:
            env[key] = str(path)
        else:
            env.setdefault(key, str(path))


def path_exists(path):
    return Path(path).exists()


def read_json(path):
    path = Path(path)
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except ValueError:
        return {}


def csv_count(path):
    path = Path(path)
    if not path.exists():
        return 0
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.reader(f)
        next(reader, None)
        return sum(1 for _ in reader)


def csv_unique_count(path, key):
    path = Path(path)
    if not path.exists():
        return 0
    seen = set()
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        for row in csv.DictReader(f):
            value = str(row.get(key) or "").strip()
            if value:
                seen.add(value)
    return len(seen)


def csv_stage_count(path, stage):
    path = Path(path)
    if not path.exists():
        return 0
    count = 0
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        for row in csv.DictReader(f):
            if str(row.get("stage") or "").strip() == stage:
                count += 1
    return count


def expected_pages(step):
    value = os.getenv("BESTBUY_MAIN_PAGES") or step.env.get("BESTBUY_MAIN_PAGES", "0")
    return int(value or 0)


def main_list_complete(step):
    root = run_root(step.env) / step.env.get("BESTBUY_MAIN_RUN_ID", "main")
    manifest = read_json(root / "manifest.json")
    expected = expected_pages(step)
    if not manifest or int(manifest.get("actual_post_calls") or 0) < expected:
        return False, f"calls {manifest.get('actual_post_calls', 0)}/{expected}"
    csv_path = root / "parsed" / "main_occurrences.csv"
    if csv_count(csv_path) <= 0:
        return False, "missing main_occurrences.csv"
    return True, f"calls {manifest.get('actual_post_calls')}/{expected}"


def main_targets_complete(step):
    root = run_root(step.env) / step.env.get("BESTBUY_MAIN_TARGET_RUN_ID", "main")
    manifest = read_json(root / "manifest_main_targets.json")
    csv_path = root / "parsed" / "main_target_occurrences.csv"
    if csv_count(csv_path) <= 0:
        return False, "missing main_target_occurrences.csv"
    if not manifest:
        return False, "missing manifest_main_targets.json"
    return True, f"unique {manifest.get('target_unique_sku_count', csv_unique_count(csv_path, 'sku_id'))}"


def bsr_rank_complete():
    root = run_root() / "bsr"
    csv_path = root / "parsed" / "bsr_rank_map.csv"
    count = csv_count(csv_path)
    if count < 100:
        return False, f"bsr rows {count}/100"
    return True, f"bsr rows {count}"


def promotion_complete():
    if os.getenv("BESTBUY_CATEGORY", "").strip().upper() == "HHP":
        return True, "HHP promotion page is not collected"
    if not has_target_url("promotion"):
        return True, "no promotion URL for category"
    path = run_root() / "promotion" / "parsed" / "all_promotion_products.csv"
    count = csv_unique_count(path, "sku_id")
    if count <= 0:
        return False, "missing promotion products"
    return True, f"unique {count}"


def trending_complete():
    if not has_target_url("trend"):
        return True, "no trend URL for category"
    path = run_root() / "trending" / "parsed" / "trending_products.csv"
    count = csv_unique_count(path, "sku_id")
    if count <= 0:
        return False, "missing trending products"
    return True, f"unique {count}"


def final_targets_complete():
    root = run_root() / "output"
    manifest = read_json(root / "bestbuy_final_targets.manifest.json")
    csv_path = root / "bestbuy_final_targets.csv"
    count = csv_unique_count(csv_path, "sku_id")
    if count < TARGET_SIZE:
        return False, f"target unique {count}/{TARGET_SIZE}"
    if manifest.get("needs_more_main_candidates") is True:
        return False, "needs more main candidates"
    return True, f"target unique {count}"


def detail_html_complete():
    root = run_root()
    target_csv = root / "output" / "bestbuy_final_targets.csv"
    target_count = csv_unique_count(target_csv, "sku_id")
    detail_meta = list((root / "detail" / "raw" / "detail_html").rglob("*_meta.json"))
    detail_success = sum(1 for path in detail_meta if read_json(path).get("success") is True)
    if target_count <= 0:
        return False, "missing final targets"
    if detail_success < target_count:
        return False, f"detail {detail_success}/{target_count}"
    return True, f"detail {detail_success}/{target_count}"


def review20_complete():
    root = run_root()
    target_csv = root / "output" / "bestbuy_final_targets.csv"
    target_count = csv_unique_count(target_csv, "sku_id")
    output_count = csv_count(root / "output" / "final_output.csv")
    review_failures = csv_stage_count(root / "detail" / "parsed" / "detail_failures.csv", "review20")
    if target_count <= 0:
        return False, "missing final targets"
    if output_count < target_count or review_failures:
        return False, f"output {output_count}/{target_count}, review_failures {review_failures}"
    return True, f"output {output_count}/{target_count}, review_failures {review_failures}"


def step_complete(step):
    if step.name in {"main_list", "bsr_list"}:
        return main_list_complete(step)
    if step.name == "main_targets":
        return main_targets_complete(step)
    if step.name == "bsr_rank":
        return bsr_rank_complete()
    if step.name == "promotion_deals":
        return promotion_complete()
    if step.name == "trending_deals":
        return trending_complete()
    if step.name == "final_targets":
        return final_targets_complete()
    if step.name == "detail_html":
        return detail_html_complete()
    if step.name == "review20":
        return review20_complete()
    if step.name == "availability_backfill":
        return False, "always refresh availability backfill"
    if step.name == "status_check":
        return False, "always refresh status"
    if step.name == "s3_sync":
        return False, "always sync to S3 when selected"
    if step.name == "local_cleanup":
        return False, "always evaluate local retention when selected"
    if step.name == "db_prepare":
        return False, "always ensure DB tables when selected"
    if step.name == "db_load":
        return False, "always load final outputs to DB when selected"
    return False, "no completion rule"


def step_by_key(value):
    for step in STEPS:
        if value in {step.key, step.name, str(step.number)}:
            return step
    raise SystemExit(f"Unknown step: {value}")


def run_step(step, dry_run=False, resume=False):
    if not step.implemented:
        print(f"[skip] step {step.key} {step.name}: not implemented yet")
        return
    if step.name == "promotion_deals":
        if os.getenv("BESTBUY_CATEGORY", "").strip().upper() == "HHP":
            print(f"[skip] step {step.key} {step.name}: HHP promotion page is not collected")
            return
        if not has_target_url("promotion"):
            print(f"[skip] step {step.key} {step.name}: no promotion URL for category")
            return
    if step.name == "trending_deals" and not has_target_url("trend"):
        print(f"[skip] step {step.key} {step.name}: no trend URL for category")
        return

    env = os.environ.copy()
    force_step_env = os.getenv("BESTBUY_FORCE_STEP_ENV", "1").lower() in {"1", "true", "yes", "y"}
    if force_step_env:
        env.update(step.env)
    else:
        for key, value in step.env.items():
            env.setdefault(key, value)
    if resume:
        if force_step_env:
            env.update(step.resume_env)
        else:
            for key, value in step.resume_env.items():
                env.setdefault(key, value)
    category_overrides = {}
    category_key = env.get("BESTBUY_CATEGORY", "").strip().upper()
    if step.name in {"main_list", "bsr_list"} and category_key in CATEGORY_SEARCH_TERMS:
        category_overrides["BESTBUY_SEARCH_TERM"] = CATEGORY_SEARCH_TERMS[category_key]
        env.update(category_overrides)
    apply_run_path_env(env)
    command = [PYTHON, "-m", step.module]
    print(f"[run] step {step.key} {step.name}: {' '.join(command)}")
    effective_env = {key: env.get(key, value) for key, value in step.env.items()}
    if resume:
        effective_env.update({key: env.get(key, value) for key, value in step.resume_env.items()})
    effective_env.update(category_overrides)
    if effective_env:
        print("[env] " + " ".join(f"{key}={value}" for key, value in effective_env.items()))
    if dry_run:
        return
    try:
        subprocess.run(command, check=True, env=env)
    except subprocess.CalledProcessError as exc:
        print(f"[fail] step {step.key} {step.name}: exit_code={exc.returncode}")
        raise


def parse_args():
    parser = argparse.ArgumentParser(description="Best Buy crawler orchestrator")
    parser.add_argument(
        "steps",
        nargs="*",
        help="Step numbers or names to run. Omit to list steps.",
    )
    parser.add_argument(
        "--from-step",
        dest="from_step",
        help="Run from this step through the last implemented step.",
    )
    parser.add_argument("--all", action="store_true", help="Run all implemented steps.")
    parser.add_argument(
        "--resume",
        action="store_true",
        help="Run only incomplete steps for today's operational folder, plus dependent downstream steps.",
    )
    parser.add_argument("--dry-run", action="store_true", help="Print commands without running.")
    parser.add_argument(
        "--category",
        default=os.getenv("BESTBUY_CATEGORY", "TV"),
        help="Category key from dx_target_page_url, e.g. TV, HHP, REF, LDY.",
    )
    return parser.parse_args()


def selected_steps(args):
    if args.resume:
        return resume_steps()
    if args.all:
        return [step for step in STEPS if step.implemented]
    if args.from_step:
        start = step_by_key(args.from_step).number
        return [step for step in STEPS if step.number >= start and step.implemented]
    if args.steps:
        return [step_by_key(value) for value in args.steps]
    return []


def resume_steps():
    selected = []
    dirty_main = False
    dirty_bsr = False
    dirty_join_sources = False

    for step in STEPS:
        if not step.implemented:
            continue
        complete, reason = step_complete(step)
        force = False
        if step.name == "main_targets" and dirty_main:
            force = True
            reason = "main_list rerun"
        elif step.name == "bsr_rank" and dirty_bsr:
            force = True
            reason = "bsr_list rerun"
        elif step.name == "final_targets" and (dirty_main or dirty_bsr or dirty_join_sources):
            force = True
            reason = "upstream source changed"
        elif step.name == "detail_html" and any(item.name == "final_targets" for item in selected):
            force = True
            reason = "final targets refreshed"
        elif step.name == "review20" and any(item.name in {"final_targets", "detail_html"} for item in selected):
            force = True
            reason = "detail source refreshed"
        elif step.name == "availability_backfill" and any(
            item.name in {"final_targets", "detail_html", "review20"} for item in selected
        ):
            force = True
            reason = "detail output refreshed"

        if complete and not force and step.name != "status_check":
            print(f"[ok] step {step.key} {step.name}: {reason}")
            continue

        print(f"[todo] step {step.key} {step.name}: {reason}")
        selected.append(step)
        if step.name == "main_list":
            dirty_main = True
        elif step.name == "bsr_list":
            dirty_bsr = True
        elif step.name in {"promotion_deals", "trending_deals"}:
            dirty_join_sources = True

    return selected


def print_steps():
    print("Best Buy pipeline steps:")
    for step in STEPS:
        status = "ready" if step.implemented else "planned"
        print(f"  {step.key} {step.name:<18} {status:<7} {step.module}")


def main():
    args = parse_args()
    os.environ["BESTBUY_CATEGORY"] = str(args.category).strip().upper()
    os.environ.setdefault("BESTBUY_RUN_ROOT", str(bestbuy_dated_run_root(category=os.environ["BESTBUY_CATEGORY"])))
    steps = selected_steps(args)
    if not steps:
        print_steps()
        return
    for step in steps:
        run_step(step, dry_run=args.dry_run, resume=args.resume)


if __name__ == "__main__":
    main()
