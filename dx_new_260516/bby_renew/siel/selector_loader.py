from __future__ import annotations

import csv
import os
from functools import lru_cache
from pathlib import Path


PACKAGE_DIR = Path(__file__).resolve().parent
DEFAULT_SELECTOR_CSV = PACKAGE_DIR / "references" / "dx_siel_xpath_selectors_202605181653.csv"


def _truthy(value) -> bool:
    return str(value or "").strip().lower() in {"1", "true", "yes", "y"}


def selector_csv_path() -> Path:
    return Path(os.getenv("SIEL_SELECTOR_CSV", str(DEFAULT_SELECTOR_CSV)))


@lru_cache(maxsize=8)
def _rows(path_text: str) -> tuple[dict, ...]:
    path = Path(path_text)
    if not path.exists():
        return ()
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        return tuple(dict(row) for row in csv.DictReader(f))


def load_selectors(site_account: str, page_type: str, domain: str) -> dict:
    site = str(site_account or "").strip().lower()
    page = str(page_type or "").strip().lower()
    product = str(domain or "").strip().lower()
    selectors = {}
    for row in _rows(str(selector_csv_path())):
        if str(row.get("site_account") or "").strip().lower() != site:
            continue
        if str(row.get("page_type") or "").strip().lower() != page:
            continue
        if str(row.get("domain") or "").strip().lower() != product:
            continue
        if not _truthy(row.get("is_active")):
            continue
        field = str(row.get("data_field") or "").strip()
        xpath = str(row.get("xpath_primary") or "").strip()
        fallback = str(row.get("fallback_xpath") or "").strip()
        if field and xpath:
            selectors[field] = {"xpath": xpath, "fallback": fallback or None}
    return selectors
