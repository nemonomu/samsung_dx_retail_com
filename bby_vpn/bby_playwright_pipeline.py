"""Reference architecture for a Playwright-based retail collection pipeline.

This file is a conservative scaffold, not a bypass tool. It defines the moving
parts needed to operate dynamic retail collection responsibly:
- Playwright browser contexts for desktop/mobile/headed/headless comparisons;
- distributed queue contracts;
- Redis-backed rate coordination;
- retry/backoff decisions;
- region/ZIP scoped inventory tasks;
- snapshot diffing and price-history records.

Site-specific selectors, credentials, and request replay code belong outside
this module.
"""

from __future__ import annotations

import hashlib
import json
import random
import time
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from decimal import Decimal
from enum import Enum
from typing import Any, Iterable
from urllib.parse import urlparse


class RenderMode(str, Enum):
    DESKTOP_HEADLESS = "desktop_headless"
    DESKTOP_HEADED = "desktop_headed"
    MOBILE_HEADLESS = "mobile_headless"
    MOBILE_HEADED = "mobile_headed"


class TaskKind(str, Enum):
    LISTING = "listing"
    PDP = "pdp"
    API_PRODUCT = "api_product"
    API_PRICE = "api_price"
    INVENTORY = "inventory"
    REVIEW = "review"
    RENDER_COMPARE = "render_compare"
    QUALITY_CHECK = "quality_check"


@dataclass(frozen=True)
class Region:
    name: str
    zip_code: str
    state: str | None = None
    store_id: str | None = None


@dataclass
class CrawlTask:
    task_id: str
    kind: TaskKind
    url: str
    sku: str | None = None
    region: Region | None = None
    render_mode: RenderMode = RenderMode.DESKTOP_HEADLESS
    priority: int = 100
    attempts: int = 0
    not_before_epoch: float = 0
    parent_task_id: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass
class RetryDecision:
    retry: bool
    delay_seconds: float
    reason: str
    terminal: bool = False


@dataclass
class CachePolicy:
    ttl_seconds: int
    stale_while_revalidate_seconds: int = 0
    negative_ttl_seconds: int = 300


@dataclass
class CacheEntry:
    key: str
    value: dict[str, Any] | None
    created_epoch: float
    expires_epoch: float
    stale_until_epoch: float
    source: str

    def is_fresh(self, now_epoch: float | None = None) -> bool:
        now_epoch = now_epoch or time.time()
        return now_epoch < self.expires_epoch

    def is_servable_stale(self, now_epoch: float | None = None) -> bool:
        now_epoch = now_epoch or time.time()
        return now_epoch < self.stale_until_epoch


@dataclass
class ProductSnapshot:
    retailer: str
    sku: str
    product_url: str
    region_zip: str | None
    captured_at: str
    title: str | None = None
    final_price: str | None = None
    original_price: str | None = None
    savings: str | None = None
    availability: str | None = None
    pickup_availability: str | None = None
    shipping_availability: str | None = None
    delivery_availability: str | None = None
    star_rating: str | None = None
    review_count: int | None = None
    model_year: str | None = None
    raw_hash: str | None = None
    collection_source: str | None = None

    def comparable(self) -> dict[str, Any]:
        ignored = {"captured_at", "raw_hash"}
        return {k: v for k, v in asdict(self).items() if k not in ignored}


@dataclass
class ProductDiff:
    sku: str
    region_zip: str | None
    changed_at: str
    changes: dict[str, dict[str, Any]]


@dataclass
class PriceHistoryRecord:
    retailer: str
    sku: str
    region_zip: str | None
    observed_at: str
    final_price: str | None
    original_price: str | None
    savings: str | None
    availability: str | None
    source_snapshot_hash: str | None


class RetryPolicy:
    def __init__(self, max_attempts: int = 4, base_delay: float = 90, max_delay: float = 1800):
        self.max_attempts = max_attempts
        self.base_delay = base_delay
        self.max_delay = max_delay

    def decide(self, task: CrawlTask, error_kind: str) -> RetryDecision:
        if error_kind in {"blocked", "captcha", "forbidden"}:
            return RetryDecision(False, 0, error_kind, terminal=True)
        if task.attempts >= self.max_attempts:
            return RetryDecision(False, 0, "max_attempts", terminal=True)

        multiplier = 4 if error_kind in {"timeout", "navigation_error"} else 2
        delay = min(self.max_delay, self.base_delay * (multiplier ** task.attempts))
        jitter = random.uniform(0.75, 1.35)
        return RetryDecision(True, delay * jitter, error_kind)


class CollectionScheduler:
    """Expands product URLs/SKUs into API-first tasks with browser fallback."""

    def __init__(self, regions: list[Region], render_compare_sample_rate: float = 0.05):
        self.regions = regions
        self.render_compare_sample_rate = render_compare_sample_rate

    def schedule_product(self, sku: str, product_url: str, parent_task_id: str | None = None) -> list[CrawlTask]:
        base = stable_task_id("api-product", sku, product_url)
        tasks = [
            CrawlTask(
                task_id=base,
                kind=TaskKind.API_PRODUCT,
                sku=sku,
                url=product_url,
                priority=20,
                parent_task_id=parent_task_id,
            ),
            CrawlTask(
                task_id=stable_task_id("api-price", sku, product_url),
                kind=TaskKind.API_PRICE,
                sku=sku,
                url=product_url,
                priority=25,
                parent_task_id=base,
            ),
        ]

        for region in self.regions:
            tasks.append(CrawlTask(
                task_id=stable_task_id("inventory", sku, region.zip_code, product_url),
                kind=TaskKind.INVENTORY,
                sku=sku,
                url=product_url,
                region=region,
                priority=40,
                parent_task_id=base,
            ))

        if random.random() < self.render_compare_sample_rate:
            for mode in RenderMode:
                tasks.append(CrawlTask(
                    task_id=stable_task_id("render-compare", sku, mode.value, product_url),
                    kind=TaskKind.RENDER_COMPARE,
                    sku=sku,
                    url=product_url,
                    render_mode=mode,
                    priority=80,
                    parent_task_id=base,
                ))

        return tasks

    def browser_fallback(self, failed_task: CrawlTask, reason: str) -> CrawlTask:
        return CrawlTask(
            task_id=stable_task_id("browser-fallback", failed_task.sku or "", failed_task.url, reason),
            kind=TaskKind.PDP,
            sku=failed_task.sku,
            url=failed_task.url,
            region=failed_task.region,
            render_mode=RenderMode.DESKTOP_HEADLESS,
            priority=max(10, failed_task.priority + 30),
            attempts=0,
            parent_task_id=failed_task.task_id,
            metadata={"fallback_reason": reason},
        )


class ApiFirstCollector:
    """Contract for API-first collection.

    Implementations should consume APIs observed during normal rendered page
    loads or documented first-party APIs. If an API response is incomplete,
    stale, denied, or schema-incompatible, schedule browser fallback instead of
    forcing repeated API calls.
    """

    def collect(self, task: CrawlTask) -> ProductSnapshot | RetryDecision:
        raise NotImplementedError


class BrowserFallbackCollector:
    """Contract for Playwright fallback extraction and render comparison."""

    def collect(self, task: CrawlTask) -> ProductSnapshot | RetryDecision:
        raise NotImplementedError


class CacheStore:
    """Simple cache interface. Redis, filesystem, or DB implementations can use it."""

    def get(self, key: str) -> CacheEntry | None:
        raise NotImplementedError

    def set(self, key: str, value: dict[str, Any] | None, policy: CachePolicy, source: str) -> CacheEntry:
        raise NotImplementedError


class InMemoryCacheStore(CacheStore):
    def __init__(self):
        self.entries: dict[str, CacheEntry] = {}

    def get(self, key: str) -> CacheEntry | None:
        return self.entries.get(key)

    def set(self, key: str, value: dict[str, Any] | None, policy: CachePolicy, source: str) -> CacheEntry:
        now = time.time()
        ttl = policy.ttl_seconds if value is not None else policy.negative_ttl_seconds
        entry = CacheEntry(
            key=key,
            value=value,
            created_epoch=now,
            expires_epoch=now + ttl,
            stale_until_epoch=now + ttl + policy.stale_while_revalidate_seconds,
            source=source,
        )
        self.entries[key] = entry
        return entry


class CacheKeyBuilder:
    def product(self, retailer: str, sku: str) -> str:
        return f"{retailer}:product:{sku}"

    def price(self, retailer: str, sku: str, region: Region | None = None) -> str:
        region_key = region.zip_code if region else "default"
        return f"{retailer}:price:{sku}:{region_key}"

    def inventory(self, retailer: str, sku: str, region: Region) -> str:
        return f"{retailer}:inventory:{sku}:{region.zip_code}"


class QualityGate:
    """Decides whether a snapshot is usable, needs browser verification, or fails."""

    REQUIRED_FIELDS = ("retailer", "sku", "product_url")

    def evaluate(self, snapshot: ProductSnapshot) -> tuple[str, list[str]]:
        issues = []
        for field_name in self.REQUIRED_FIELDS:
            if not getattr(snapshot, field_name):
                issues.append(f"missing:{field_name}")

        if snapshot.final_price and snapshot.original_price:
            final_price = normalize_decimal(snapshot.final_price)
            original_price = normalize_decimal(snapshot.original_price)
            if final_price is not None and original_price is not None and original_price < final_price:
                issues.append("price:original_less_than_final")

        if snapshot.star_rating not in (None, "", "Not yet reviewed"):
            try:
                rating = float(str(snapshot.star_rating))
                if rating < 0 or rating > 5:
                    issues.append("rating:outside_range")
            except ValueError:
                issues.append("rating:not_numeric")

        if any(issue.startswith("missing:") for issue in issues):
            return "reject", issues
        if issues:
            return "verify_with_browser", issues
        return "accept", issues


class RedisRateCoordinator:
    """Cross-worker token coordination using Redis sorted sets.

    Requires redis-py at runtime. Import is lazy so this module can be compiled
    in environments that do not have Redis installed.
    """

    def __init__(self, redis_url: str, namespace: str = "bby:crawl"):
        import redis

        self.client = redis.Redis.from_url(redis_url, decode_responses=True)
        self.namespace = namespace

    def wait_for_slot(self, host: str, actor_key: str, max_per_minute: int, max_per_hour: int) -> float:
        key = f"{self.namespace}:rate:{host}:{actor_key}"
        now = time.time()
        pipe = self.client.pipeline()
        pipe.zremrangebyscore(key, 0, now - 3600)
        pipe.zcount(key, now - 60, now)
        pipe.zcount(key, now - 3600, now)
        _, minute_count, hour_count = pipe.execute()

        if minute_count >= max_per_minute or hour_count >= max_per_hour:
            oldest = self.client.zrange(key, 0, 0, withscores=True)
            delay = 60 if minute_count >= max_per_minute else 3600
            if oldest:
                delay = max(1, delay - (now - oldest[0][1]))
            time.sleep(delay)
            return delay

        self.client.zadd(key, {str(now): now})
        self.client.expire(key, 7200)
        return 0


class RedisTaskQueue:
    """Priority queue contract for distributed crawling workers."""

    def __init__(self, redis_url: str, namespace: str = "bby:crawl"):
        import redis

        self.client = redis.Redis.from_url(redis_url, decode_responses=True)
        self.namespace = namespace

    @property
    def ready_key(self) -> str:
        return f"{self.namespace}:tasks:ready"

    @property
    def payload_key(self) -> str:
        return f"{self.namespace}:tasks:payload"

    def enqueue(self, task: CrawlTask) -> None:
        payload = json.dumps(asdict(task), ensure_ascii=False, default=str)
        score = max(task.not_before_epoch, time.time()) + task.priority / 100000
        pipe = self.client.pipeline()
        pipe.hset(self.payload_key, task.task_id, payload)
        pipe.zadd(self.ready_key, {task.task_id: score})
        pipe.execute()

    def dequeue_due(self) -> CrawlTask | None:
        now = time.time()
        task_ids = self.client.zrangebyscore(self.ready_key, 0, now, start=0, num=1)
        if not task_ids:
            return None
        task_id = task_ids[0]
        if self.client.zrem(self.ready_key, task_id) != 1:
            return None
        payload = self.client.hget(self.payload_key, task_id)
        if not payload:
            return None
        data = json.loads(payload)
        data["kind"] = TaskKind(data["kind"])
        data["render_mode"] = RenderMode(data["render_mode"])
        if data.get("region"):
            data["region"] = Region(**data["region"])
        return CrawlTask(**data)


class SnapshotDiffer:
    def diff(self, previous: ProductSnapshot | None, current: ProductSnapshot) -> ProductDiff | None:
        if previous is None:
            return ProductDiff(current.sku, current.region_zip, current.captured_at, {
                field: {"old": None, "new": value}
                for field, value in current.comparable().items()
                if value is not None
            })

        changes = {}
        before = previous.comparable()
        after = current.comparable()
        for field, new_value in after.items():
            old_value = before.get(field)
            if old_value != new_value:
                changes[field] = {"old": old_value, "new": new_value}

        if not changes:
            return None
        return ProductDiff(current.sku, current.region_zip, current.captured_at, changes)

    def price_history_record(self, snapshot: ProductSnapshot) -> PriceHistoryRecord:
        return PriceHistoryRecord(
            retailer=snapshot.retailer,
            sku=snapshot.sku,
            region_zip=snapshot.region_zip,
            observed_at=snapshot.captured_at,
            final_price=snapshot.final_price,
            original_price=snapshot.original_price,
            savings=snapshot.savings,
            availability=snapshot.availability,
            source_snapshot_hash=snapshot.raw_hash,
        )


class RenderComparator:
    """Compare headless/headed and mobile/desktop extraction outputs."""

    def compare(self, snapshots: Iterable[ProductSnapshot]) -> dict[str, Any]:
        grouped = list(snapshots)
        fields = ["title", "final_price", "availability", "star_rating", "review_count"]
        result = {"snapshot_count": len(grouped), "field_disagreements": {}}
        for field_name in fields:
            values = {getattr(item, field_name) for item in grouped}
            if len(values) > 1:
                result["field_disagreements"][field_name] = sorted(str(v) for v in values)
        return result


class PlaywrightCollector:
    """Playwright collection shell.

    The implementation intentionally keeps site-specific extraction callbacks
    external. That lets the same browser/session/rate/retry infrastructure run
    listing, PDP, inventory, and render-comparison tasks.
    """

    def __init__(self, rate_coordinator=None, storage_state_dir: str = ".playwright_state"):
        self.rate_coordinator = rate_coordinator
        self.storage_state_dir = storage_state_dir

    def context_options(self, mode: RenderMode, region: Region | None = None) -> dict[str, Any]:
        is_mobile = mode in {RenderMode.MOBILE_HEADLESS, RenderMode.MOBILE_HEADED}
        options = {
            "locale": "en-US",
            "timezone_id": "America/New_York",
            "viewport": {"width": 390, "height": 844} if is_mobile else {"width": 1440, "height": 900},
            "is_mobile": is_mobile,
            "has_touch": is_mobile,
        }
        if region:
            options["extra_http_headers"] = {"X-Crawl-Region-Zip": region.zip_code}
        return options

    def storage_state_path(self, retailer: str, mode: RenderMode, region: Region | None) -> str:
        region_key = region.zip_code if region else "default"
        return f"{self.storage_state_dir}/{retailer}_{mode.value}_{region_key}.json"

    def host_key(self, url: str) -> str:
        return urlparse(url).netloc.lower()

    def snapshot_hash(self, payload: dict[str, Any]) -> str:
        body = json.dumps(payload, sort_keys=True, ensure_ascii=False, default=str)
        return hashlib.sha256(body.encode("utf-8")).hexdigest()


def now_utc() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def stable_task_id(*parts: str) -> str:
    body = "|".join(str(part) for part in parts)
    return hashlib.sha256(body.encode("utf-8")).hexdigest()[:24]


def normalize_decimal(value: str | None) -> Decimal | None:
    normalized = normalize_price(value)
    if normalized is None:
        return None
    try:
        return Decimal(normalized)
    except Exception:
        return None


def normalize_price(value: str | None) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    if "see price" in text.lower() or "no longer available" in text.lower():
        return text
    cleaned = text.replace("$", "").replace(",", "")
    try:
        return str(Decimal(cleaned).quantize(Decimal("0.01")))
    except Exception:
        return text
