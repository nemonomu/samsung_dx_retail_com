"""Ordered pipeline steps for the Best Buy V2 crawler.

This module keeps the execution order separate from the crawler implementations.
The crawler files still own site-specific extraction; this layer only decides
what runs, in what order, and with which shared batch context.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
import os
from pathlib import Path
from typing import Callable

import step05_listing_detail_flow as listing_flow


BASE_DIR = Path(__file__).resolve().parents[1]


@dataclass
class PipelineContext:
    batch_id: str
    pages: int
    skip_detail: bool = False
    keep_listing_files: bool = False


@dataclass(frozen=True)
class PipelineStep:
    key: str
    label: str
    runner: Callable[[PipelineContext], int]


def default_context(pages: int | None = None, batch_id: str | None = None, skip_detail: bool = False) -> PipelineContext:
    return PipelineContext(
        batch_id=batch_id or datetime.now().strftime("%Y%m%d_%H%M%S"),
        pages=pages if pages is not None else int(os.environ.get("BBY_LISTING_TEST_PAGES", "3")),
        skip_detail=skip_detail,
        keep_listing_files=os.environ.get("BBY_KEEP_LISTING_FILES", "0").strip().lower() in {"1", "true", "yes"},
    )


def step_clean_listing_outputs(context: PipelineContext) -> int:
    if context.keep_listing_files:
        print("[STEP 00] keep existing listing CSV files")
        return 0
    for path in listing_flow.LISTING_FILES.values():
        if path.exists():
            path.unlink()
            print(f"[STEP 00] removed {path.name}")
    return 0


def step_main_listing(context: PipelineContext) -> int:
    listing_flow.run_listing(listing_flow.BestBuyTVMainCrawler, "main listing", context.batch_id, context.pages)
    return 0


def step_bsr_listing(context: PipelineContext) -> int:
    listing_flow.run_listing(listing_flow.BestBuyTVBSRCrawler, "bsr listing", context.batch_id, context.pages)
    return 0


def step_promotion_listing(context: PipelineContext) -> int:
    listing_flow.run_single_listing(listing_flow.BestBuyTVPromotionCrawler, "promotion listing", context.batch_id)
    return 0


def step_trend_listing(context: PipelineContext) -> int:
    listing_flow.run_single_listing(listing_flow.BestBuyTVTrendCrawler, "trend listing", context.batch_id)
    return 0


def step_filter_listing_outputs(context: PipelineContext) -> int:
    stats = listing_flow.filter_listing_csvs()
    return 0 if stats["kept"] > 0 else 1


def step_api_detail(context: PipelineContext) -> int:
    if context.skip_detail:
        print("[STEP 06] detail step skipped")
        return 0
    rows = listing_flow.filtered_listing_rows()
    if not rows:
        print("[ERROR] No listing rows left after filtering. Detail crawl skipped.")
        return 1
    listing_flow.run_api_only_detail(rows)
    return 0


PIPELINE_STEPS = (
    PipelineStep("00_clean", "Clean previous listing outputs", step_clean_listing_outputs),
    PipelineStep("01_main", "Collect main search listing", step_main_listing),
    PipelineStep("02_bsr", "Collect best-seller listing", step_bsr_listing),
    PipelineStep("03_promotion", "Collect promotion listing", step_promotion_listing),
    PipelineStep("04_trend", "Collect trend listing", step_trend_listing),
    PipelineStep("05_filter", "Filter open-box and duplicate listing rows", step_filter_listing_outputs),
    PipelineStep("06_detail", "Collect API-only detail rows", step_api_detail),
)


STEP_BY_KEY = {step.key: step for step in PIPELINE_STEPS}

