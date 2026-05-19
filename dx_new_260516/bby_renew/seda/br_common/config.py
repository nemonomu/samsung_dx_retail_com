import ast
import csv
import json
import os
import sys
from datetime import datetime
from pathlib import Path
from urllib.parse import urlencode


PROJECT_ROOT = Path(__file__).resolve().parent.parent


def load_env(path=None):
    env_path = Path(path or (PROJECT_ROOT / ".env"))
    if not env_path.exists():
        return
    lines = env_path.read_text(encoding="utf-8", errors="ignore").splitlines()
    i = 0
    while i < len(lines):
        line = lines[i].strip()
        i += 1
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip()
        if value == "{":
            collected = ["{"]
            depth = 1
            while i < len(lines) and depth > 0:
                part = lines[i]
                i += 1
                collected.append(part)
                depth += part.count("{") - part.count("}")
            value = "\n".join(collected)
        else:
            value = value.strip().strip('"').strip("'")
        os.environ.setdefault(key, value)


load_env()


def read_multiline_env_object(name):
    raw = os.getenv(name)
    if raw and raw.strip() not in {"{", ""}:
        return raw
    env_path = PROJECT_ROOT / ".env"
    if not env_path.exists():
        return raw or ""
    lines = env_path.read_text(encoding="utf-8", errors="ignore").splitlines()
    collecting = False
    collected = []
    depth = 0
    for line in lines:
        stripped = line.strip()
        if not collecting and stripped.startswith(f"{name}") and "=" in stripped:
            value = line.split("=", 1)[1].strip()
            collecting = True
            collected.append(value)
            depth += value.count("{") - value.count("}")
            if depth <= 0 and value:
                break
            continue
        if collecting:
            collected.append(line)
            depth += line.count("{") - line.count("}")
            if depth <= 0:
                break
    return "\n".join(collected).strip()


def db_config():
    raw = read_multiline_env_object("DB_CONFIG")
    if not raw:
        return {}
    for parser in (json.loads, ast.literal_eval):
        try:
            value = parser(raw)
            return value if isinstance(value, dict) else {}
        except Exception:
            continue
    return {}


def run_date(env_prefix):
    return os.getenv(f"{env_prefix}_RUN_DATE", datetime.now().strftime("%Y%m%d"))


def product_type(env_prefix, default_product_type):
    return os.getenv(f"{env_prefix}_PRODUCT_TYPE", default_product_type).strip().lower() or default_product_type.lower()


def dated_run_root(package_dir, env_prefix, default_product_type, run_date_value=None, product_type_value=None):
    product = product_type_value or product_type(env_prefix, default_product_type)
    date_value = run_date_value or run_date(env_prefix)
    return Path(package_dir) / "data" / product.lower() / date_value


def load_initial_urls(path, env_prefix, default_urls, product_type_value=None):
    urls = dict(default_urls)
    if not Path(path).exists():
        return urls
    wanted_product_type = str(product_type_value or "").strip().upper()
    with Path(path).open("r", encoding="utf-8-sig", newline="") as f:
        for row in csv.DictReader(f):
            enabled = str(row.get("enabled", "true")).strip().lower()
            if enabled in {"0", "false", "no", "n"}:
                continue
            row_product_type = str(
                row.get("product_line") or row.get("category_key") or row.get("category") or ""
            ).strip().upper()
            if wanted_product_type and row_product_type and row_product_type != wanted_product_type:
                continue
            key = str(row.get("key") or row.get("page_type") or row.get("url_type") or "").strip().lower()
            url = str(row.get("url") or row.get("url_template") or "").strip()
            if key and url:
                urls[target_url_key(key)] = url
    return urls


def target_url_key(page_type):
    value = str(page_type or "").strip().lower()
    aliases = {
        "main": "main",
        "main_search": "main",
        "search": "main",
        "bsr": "bsr",
        "best_selling": "bsr",
        "best-selling": "bsr",
        "promotion": "promotion",
        "promo": "promotion",
        "trend": "trending",
        "trending": "trending",
    }
    return aliases.get(value, value)


def url_for_page(url_template, page):
    value = str(url_template or "")
    if "{page}" in value:
        return value.replace("{page}", str(page))
    if page and int(page) > 1:
        joiner = "&" if "?" in value else "?"
        return f"{value}{joiner}{urlencode({'page': int(page)})}"
    return value


def absolute_url(base_url, path):
    if not path:
        return ""
    value = str(path)
    if value.startswith("http"):
        return value
    return f"{base_url}{value}"


def rel_path(path):
    if path in ("", None):
        return ""
    value = Path(path)
    try:
        return os.path.relpath(value.resolve(), PROJECT_ROOT.resolve())
    except (OSError, ValueError):
        return str(path)
