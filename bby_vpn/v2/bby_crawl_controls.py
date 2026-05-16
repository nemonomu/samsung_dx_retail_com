"""Responsible crawl controls for dynamic retail pages.

The helpers in this module are intentionally conservative. They do not bypass
access controls; they slow the crawler down, record page/network diagnostics,
and produce row-level validation evidence for later review.
"""

from __future__ import annotations

import json
import os
import random
import re
import time
from collections import Counter, deque
from datetime import datetime
from decimal import Decimal, InvalidOperation
from urllib.parse import urlparse


class JsonlAuditLog:
    def __init__(self, path: str):
        self.path = path
        os.makedirs(os.path.dirname(path), exist_ok=True)

    def write(self, event_type: str, payload: dict) -> None:
        event = {
            "ts": datetime.utcnow().isoformat(timespec="seconds") + "Z",
            "event_type": event_type,
            **payload,
        }
        with open(self.path, "a", encoding="utf-8") as f:
            f.write(json.dumps(event, ensure_ascii=False, default=str) + "\n")


class ConservativeRateLimiter:
    """Per-host rate limiter with jitter and simple backoff.

    Defaults are deliberately slower than a typical browser session. The values
    can be tuned via environment variables without changing crawler code.
    """

    def __init__(
        self,
        audit_log: JsonlAuditLog,
        min_delay: float | None = None,
        max_delay: float | None = None,
        max_per_minute: int | None = None,
        max_per_hour: int | None = None,
    ):
        self.audit_log = audit_log
        self.min_delay = float(os.environ.get("BBY_RATE_MIN_DELAY", min_delay or 12))
        self.max_delay = float(os.environ.get("BBY_RATE_MAX_DELAY", max_delay or 28))
        self.max_per_minute = int(os.environ.get("BBY_RATE_MAX_PER_MINUTE", max_per_minute or 4))
        self.max_per_hour = int(os.environ.get("BBY_RATE_MAX_PER_HOUR", max_per_hour or 120))
        self.history = {}
        self.backoff_until = {}

    def _host(self, url: str) -> str:
        return urlparse(url or "").netloc.lower() or "unknown"

    def wait(self, url: str, reason: str = "navigation") -> float:
        host = self._host(url)
        now = time.monotonic()
        bucket = self.history.setdefault(host, deque())

        while bucket and now - bucket[0] > 3600:
            bucket.popleft()

        wait_for = 0.0
        minute_count = sum(1 for ts in bucket if now - ts <= 60)
        if minute_count >= self.max_per_minute:
            oldest_in_minute = next(ts for ts in bucket if now - ts <= 60)
            wait_for = max(wait_for, 60 - (now - oldest_in_minute))

        if len(bucket) >= self.max_per_hour:
            wait_for = max(wait_for, 3600 - (now - bucket[0]))

        if host in self.backoff_until:
            wait_for = max(wait_for, self.backoff_until[host] - now)

        if bucket:
            elapsed = now - bucket[-1]
            wait_for = max(wait_for, random.uniform(self.min_delay, self.max_delay) - elapsed)

        wait_for = max(0.0, wait_for)
        if wait_for:
            self.audit_log.write("rate_limit_sleep", {
                "host": host,
                "reason": reason,
                "sleep_seconds": round(wait_for, 2),
                "minute_count": minute_count,
                "hour_count": len(bucket),
            })
            time.sleep(wait_for)

        bucket.append(time.monotonic())
        return wait_for

    def register_outcome(self, url: str, outcome: str) -> None:
        host = self._host(url)
        if outcome == "blocked":
            delay = random.uniform(15 * 60, 30 * 60)
        elif outcome == "failed":
            delay = random.uniform(2 * 60, 5 * 60)
        else:
            return
        self.backoff_until[host] = time.monotonic() + delay
        self.audit_log.write("rate_limit_backoff", {
            "host": host,
            "outcome": outcome,
            "backoff_seconds": round(delay, 2),
        })


class BrowserSessionDiagnostics:
    """Collect rendered-page health signals after navigation."""

    def __init__(self, audit_log: JsonlAuditLog):
        self.audit_log = audit_log

    def snapshot(self, page, url: str, label: str) -> dict:
        try:
            metrics = page.run_js("""
                return {
                    url: window.location.href,
                    title: document.title,
                    readyState: document.readyState,
                    bodyTextLength: document.body ? document.body.innerText.length : 0,
                    h1Count: document.querySelectorAll('h1').length,
                    priceBlockCount: document.querySelectorAll('[data-testid*="price-block"]').length,
                    reviewNodeCount: document.querySelectorAll('[class*="review"], [id*="review"]').length,
                    imageCount: document.images.length,
                    loadedImageCount: Array.from(document.images).filter(function(img) {
                        return img.complete && img.naturalWidth > 0;
                    }).length,
                    viewport: {width: window.innerWidth, height: window.innerHeight},
                    scrollHeight: document.documentElement.scrollHeight || document.body.scrollHeight
                };
            """) or {}
        except Exception as exc:
            metrics = {"error": str(exc)}

        self.audit_log.write("browser_snapshot", {
            "label": label,
            "requested_url": url,
            "metrics": metrics,
        })
        return metrics


class NetworkDiagnostics:
    """Summarize browser Performance API entries without replaying requests."""

    API_HINTS = ("graphql", "price", "pricing", "fulfillment", "availability", "review", "ugc")

    def __init__(self, audit_log: JsonlAuditLog):
        self.audit_log = audit_log

    def snapshot(self, page, url: str, label: str) -> dict:
        try:
            entries = page.run_js("""
                return performance.getEntriesByType('resource').map(function(e) {
                    return {
                        name: e.name,
                        initiatorType: e.initiatorType,
                        duration: Math.round(e.duration),
                        transferSize: e.transferSize || 0,
                        decodedBodySize: e.decodedBodySize || 0
                    };
                }).slice(-250);
            """) or []
        except Exception as exc:
            summary = {"error": str(exc)}
            self.audit_log.write("network_summary", {
                "label": label,
                "requested_url": url,
                "summary": summary,
            })
            return summary

        by_type = Counter(entry.get("initiatorType") or "unknown" for entry in entries)
        api_entries = [
            entry for entry in entries
            if any(hint in (entry.get("name") or "").lower() for hint in self.API_HINTS)
        ]
        slow_entries = sorted(entries, key=lambda e: e.get("duration") or 0, reverse=True)[:10]
        summary = {
            "resource_count": len(entries),
            "by_type": dict(by_type),
            "api_count": len(api_entries),
            "api_hosts": sorted({
                urlparse(entry.get("name") or "").netloc
                for entry in api_entries
                if entry.get("name")
            })[:20],
            "slow_entries": [
                {
                    "host": urlparse(entry.get("name") or "").netloc,
                    "path": urlparse(entry.get("name") or "").path[:120],
                    "type": entry.get("initiatorType"),
                    "duration": entry.get("duration"),
                    "transferSize": entry.get("transferSize"),
                }
                for entry in slow_entries
            ],
        }
        self.audit_log.write("network_summary", {
            "label": label,
            "requested_url": url,
            "summary": summary,
        })
        return summary


class RowQualityAuditor:
    """Row-level validation report that complements DataValidator."""

    PRICE_RE = re.compile(r"\$?\s*([\d,]+(?:\.\d{1,2})?)")

    def __init__(self, audit_log: JsonlAuditLog):
        self.audit_log = audit_log

    def _price_decimal(self, value):
        if value in (None, ""):
            return None
        text = str(value)
        if "see price" in text.lower() or "no longer available" in text.lower():
            return None
        match = self.PRICE_RE.search(text)
        if not match:
            return None
        try:
            return Decimal(match.group(1).replace(",", ""))
        except InvalidOperation:
            return None

    def audit_detail_row(self, row: dict) -> list[dict]:
        issues = []

        for field in ("product_url", "retailer_sku_name", "item"):
            if not row.get(field):
                issues.append({"severity": "error", "field": field, "reason": "missing_required"})

        if row.get("product_url") and "bestbuy.com" not in row["product_url"]:
            issues.append({"severity": "error", "field": "product_url", "reason": "unexpected_host"})

        final_price = self._price_decimal(row.get("final_sku_price"))
        original_price = self._price_decimal(row.get("original_sku_price"))
        savings = self._price_decimal(row.get("savings"))

        if original_price is not None and final_price is not None and original_price < final_price:
            issues.append({"severity": "warning", "field": "original_sku_price", "reason": "less_than_final_price"})

        if savings is not None and final_price is not None and original_price is not None:
            expected = original_price - final_price
            if expected >= 0 and abs(expected - savings) > Decimal("2.00"):
                issues.append({"severity": "warning", "field": "savings", "reason": "does_not_match_price_delta"})

        rating = row.get("star_rating")
        if rating not in (None, "", "Not yet reviewed"):
            try:
                rating_float = float(str(rating))
                if rating_float < 0 or rating_float > 5:
                    issues.append({"severity": "error", "field": "star_rating", "reason": "outside_0_to_5"})
            except ValueError:
                issues.append({"severity": "warning", "field": "star_rating", "reason": "not_numeric"})

        count = row.get("count_of_reviews_int")
        if count is not None:
            try:
                if int(count) < 0:
                    issues.append({"severity": "error", "field": "count_of_reviews_int", "reason": "negative"})
            except Exception:
                issues.append({"severity": "warning", "field": "count_of_reviews_int", "reason": "not_integer"})

        if row.get("star_rating") == "Not yet reviewed" and row.get("count_of_reviews_int") not in (0, None):
            issues.append({"severity": "warning", "field": "count_of_reviews_int", "reason": "not_reviewed_but_nonzero"})

        self.audit_log.write("row_quality", {
            "product_url": row.get("product_url"),
            "item": row.get("item"),
            "issue_count": len(issues),
            "issues": issues,
        })
        return issues
