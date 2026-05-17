"""Central filesystem layout for V2 runtime data."""

from __future__ import annotations

import os
from pathlib import Path


BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = Path(os.environ.get("BBY_DATA_DIR", BASE_DIR / "data"))

LISTING_FILES = {
    "main": ("main", "bby_tv_main1_vpn_test.csv"),
    "bsr": ("bsr", "bby_tv_bsr1_vpn_test.csv"),
    "promotion": ("promotion", "bby_tv_pmt1_vpn_test.csv"),
    "trend": ("trend", "bby_tv_trend_crawl_vpn_test.csv"),
}


def data_dir(*parts: str) -> Path:
    return DATA_DIR.joinpath(*parts)


def ensure_dir(path: Path) -> Path:
    path.mkdir(parents=True, exist_ok=True)
    return path


def listing_parsed_dir(source: str) -> Path:
    return ensure_dir(data_dir(source, "parsed"))


def listing_csv_path(source: str) -> Path:
    folder, filename = LISTING_FILES[source]
    return listing_parsed_dir(folder) / filename


def detail_raw_dir() -> Path:
    return ensure_dir(data_dir("detail", "raw"))


def detail_parsed_dir() -> Path:
    return ensure_dir(data_dir("detail", "parsed"))


def graphql_registry_dir() -> Path:
    return ensure_dir(data_dir("graphql", "registry"))


def ensure_data_layout() -> None:
    for key in LISTING_FILES:
        listing_csv_path(key)
    detail_raw_dir()
    detail_parsed_dir()
    graphql_registry_dir()
