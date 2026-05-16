"""BestBuy-style server-side bot defense prototype for TV PDP traffic.

This module is defensive. It does not help a crawler bypass controls. It models
how a retailer could protect high-value product data by scoring request streams
and returning allow/throttle/challenge/block decisions with explainable evidence.

The policy is tuned around concrete retail scraping patterns:
- listing pages are used to harvest TV PDP URLs;
- PDP pages are visited in a linear SKU walk;
- each PDP fans out to price, fulfillment, review, and recommendation APIs;
- blocked actors often retry the same SKU shortly after 403/429/challenge.
"""

from collections import defaultdict, deque
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from urllib.parse import parse_qs, urlparse
import ipaddress
import re


PDP_RE = re.compile(r"/site/.+/(?P<sku>\d+)\.p(?:$|[?#])", re.IGNORECASE)
SEARCH_RE = re.compile(r"/site/searchpage\.jsp", re.IGNORECASE)
CATEGORY_RE = re.compile(r"/site/.+/(pcmcat|abcat|cat)\w+", re.IGNORECASE)
ZIP_QUERY_KEYS = ("zip", "zipcode", "postalCode", "locationZip")

SENSITIVE_API_HINTS = {
    "price": ("price", "pricing", "offers"),
    "fulfillment": ("availability", "fulfillment", "pickup", "shipping", "delivery"),
    "reviews": ("reviews", "ugc", "rating"),
    "recommendation": ("recommendation", "similar", "compare"),
    "graphql": ("graphql",),
}

KNOWN_DATACENTER_ASN_HINTS = (
    "amazon",
    "aws",
    "google cloud",
    "microsoft",
    "azure",
    "digitalocean",
    "ovh",
    "hetzner",
    "linode",
)


@dataclass
class RequestEvent:
    """Normalized request data expected from CDN, edge worker, or app server logs."""

    ip: str
    method: str
    path: str
    user_agent: str = ""
    session_id: str = ""
    device_id: str = ""
    account_id: str = ""
    referer: str = ""
    accept_language: str = ""
    country: str = ""
    asn_org: str = ""
    status_code: int = 200
    response_ms: int = 0
    js_challenge_passed: bool = True
    viewport_width: int = 0
    viewport_height: int = 0
    timezone: str = ""
    webdriver_flag: bool = False
    cdp_detected: bool = False
    image_load_ratio: float = 1.0
    prior_path: str = ""
    now: datetime = field(default_factory=lambda: datetime.now(timezone.utc))

    @property
    def actor(self):
        return self.device_id or self.session_id or self.account_id or self.ip

    @property
    def parsed_path(self):
        return urlparse(self.path or "").path

    @property
    def sku(self):
        match = PDP_RE.search(self.path or "")
        if match:
            return match.group("sku")
        query = parse_qs(urlparse(self.path or "").query)
        for key in ("sku", "skuId", "itemId"):
            if key in query and query[key]:
                return query[key][0]
        return None

    @property
    def zip_code(self):
        query = parse_qs(urlparse(self.path or "").query)
        for key in ZIP_QUERY_KEYS:
            if key in query and query[key]:
                value = query[key][0]
                if re.match(r"^\d{5}$", value):
                    return value
        return None

    @property
    def page_kind(self):
        path = self.path or ""
        if self.sku and PDP_RE.search(path):
            return "pdp"
        if SEARCH_RE.search(path):
            return "search"
        if CATEGORY_RE.search(path):
            return "category"
        if self.sensitive_api_kind:
            return "api"
        return "other"

    @property
    def is_pdp(self):
        return self.page_kind == "pdp"

    @property
    def sensitive_api_kind(self):
        path = (self.path or "").lower()
        for api_kind, hints in SENSITIVE_API_HINTS.items():
            if any(hint in path for hint in hints):
                return api_kind
        return None


@dataclass
class Evidence:
    code: str
    points: int
    detail: str


@dataclass
class Decision:
    action: str
    score: int
    actor: str
    evidence: list[Evidence]
    ttl_seconds: int


class SlidingWindowCounter:
    def __init__(self):
        self.events = defaultdict(deque)

    def add(self, key, now, value=None):
        self.events[key].append((now, value))

    def values(self, key, now, window_seconds):
        cutoff = now - timedelta(seconds=window_seconds)
        bucket = self.events[key]
        while bucket and bucket[0][0] < cutoff:
            bucket.popleft()
        return [value for _, value in bucket]

    def count(self, key, now, window_seconds):
        return len(self.values(key, now, window_seconds))


class RetailBotDefense:
    """Explainable risk-scoring policy for high-value retail product traffic."""

    def __init__(self):
        self.counter = SlidingWindowCounter()
        self.last_block_by_actor = {}
        self.last_seen_sku_by_actor = {}

    def score(self, event: RequestEvent) -> Decision:
        evidence = []
        actor = event.actor

        self._record(event)

        self._score_volume(event, evidence)
        self._score_tv_pdp_harvest(event, evidence)
        self._score_api_fanout(event, evidence)
        self._score_journey(event, evidence)
        self._score_identity(event, evidence)
        self._score_browser_integrity(event, evidence)
        self._score_known_crawler_flow(event, evidence)
        self._score_region_inventory_probe(event, evidence)
        self._score_distributed_queue_pattern(event, evidence)
        self._score_block_response(event, evidence)

        score = min(100, sum(item.points for item in evidence))
        action, ttl = self._decision(score, evidence)

        if action in ("challenge", "block") or event.status_code in (403, 429):
            self.last_block_by_actor[actor] = event.now

        return Decision(
            action=action,
            score=score,
            actor=actor,
            evidence=evidence,
            ttl_seconds=ttl,
        )

    def _record(self, event):
        actor = event.actor
        self.counter.add(("actor", actor), event.now, event.path)
        self.counter.add(("ip", event.ip), event.now, event.path)
        self.counter.add(("kind", actor, event.page_kind), event.now, event.path)
        if event.account_id:
            self.counter.add(("account", event.account_id), event.now, event.path)
            self.counter.add(("account_actor", event.account_id), event.now, actor)

        if event.sku:
            self.counter.add(("sku", actor), event.now, event.sku)
            self.counter.add(("sku_path", actor, event.sku), event.now, event.path)
            if event.account_id:
                self.counter.add(("account_sku", event.account_id), event.now, event.sku)

        api_kind = event.sensitive_api_kind
        if api_kind:
            self.counter.add(("api", actor), event.now, api_kind)
            if event.sku:
                self.counter.add(("sku_api", actor, event.sku), event.now, api_kind)

        if event.zip_code:
            self.counter.add(("zip", actor), event.now, event.zip_code)
            if event.sku:
                self.counter.add(("sku_zip", actor, event.sku), event.now, event.zip_code)

    def _score_volume(self, event, evidence):
        actor = event.actor
        req_1m = self.counter.count(("actor", actor), event.now, 60)
        req_10m = self.counter.count(("actor", actor), event.now, 600)
        ip_10m = self.counter.count(("ip", event.ip), event.now, 600)

        if req_1m > 90:
            evidence.append(Evidence(
                "burst_rate_actor_1m",
                30,
                f"{req_1m} requests from one actor in 60s; normal PDP browsing rarely needs this burst.",
            ))
        elif req_1m > 45:
            evidence.append(Evidence(
                "elevated_rate_actor_1m",
                15,
                f"{req_1m} requests from one actor in 60s.",
            ))

        if req_10m > 300:
            evidence.append(Evidence(
                "sustained_actor_volume_10m",
                25,
                f"{req_10m} requests in 10m indicates automation or page-resource abuse.",
            ))

        if ip_10m > 1000:
            evidence.append(Evidence(
                "shared_ip_pressure_10m",
                20,
                f"{ip_10m} requests from one IP in 10m; protect origin before customer latency rises.",
            ))

        if event.account_id:
            account_10m = self.counter.count(("account", event.account_id), event.now, 600)
            if account_10m > 250:
                evidence.append(Evidence(
                    "account_level_distributed_volume",
                    25,
                    f"{account_10m} requests in 10m under one account/session group across devices.",
                ))

    def _score_tv_pdp_harvest(self, event, evidence):
        actor = event.actor
        pdp_10m = self.counter.count(("kind", actor, "pdp"), event.now, 600)
        skus_10m = self.counter.values(("sku", actor), event.now, 600)
        unique_skus = len(set(skus_10m))
        skus_1h = self.counter.values(("sku", actor), event.now, 3600)
        unique_skus_1h = len(set(skus_1h))

        if pdp_10m >= 40 and unique_skus >= 30:
            evidence.append(Evidence(
                "pdp_catalog_walk_10m",
                35,
                f"{pdp_10m} PDP views and {unique_skus} unique SKUs in 10m; typical of listing-to-detail crawling.",
            ))
        elif pdp_10m >= 20 and unique_skus >= 15:
            evidence.append(Evidence(
                "pdp_catalog_walk_watch",
                20,
                f"{pdp_10m} PDP views and {unique_skus} unique SKUs in 10m.",
            ))

        if unique_skus >= 12:
            unique_ratio = unique_skus / max(1, len(skus_10m))
            if unique_ratio >= 0.85:
                evidence.append(Evidence(
                    "mostly_never_repeat_skus",
                    15,
                    f"{unique_ratio:.0%} of recent PDP SKU visits are unique; shopping users revisit/compare more often.",
                ))

        if unique_skus_1h >= 25 or len(skus_1h) >= 50:
            evidence.append(Evidence(
                "hourly_tv_sku_harvest",
                30,
                f"{unique_skus_1h} unique TV SKUs and {len(skus_1h)} SKU-linked requests in 1h; catches slower crawlers that evade 10m windows.",
            ))

    def _score_api_fanout(self, event, evidence):
        actor = event.actor
        api_10m = self.counter.values(("api", actor), event.now, 600)
        total_api = len(api_10m)
        api_kinds = set(api_10m)

        if total_api >= 150:
            evidence.append(Evidence(
                "sensitive_api_pressure_10m",
                30,
                f"{total_api} sensitive API calls in 10m across {sorted(api_kinds)}.",
            ))
        elif total_api >= 75:
            evidence.append(Evidence(
                "sensitive_api_pressure_watch",
                15,
                f"{total_api} sensitive API calls in 10m.",
            ))

        if event.sku:
            sku_api = self.counter.values(("sku_api", actor, event.sku), event.now, 120)
            sku_api_kinds = set(sku_api)
            if len(sku_api_kinds) >= 3:
                evidence.append(Evidence(
                    "pdp_api_fanout_single_sku",
                    25,
                    f"SKU {event.sku} touched {sorted(sku_api_kinds)} APIs within 2m; common scraper extraction fan-out.",
                ))

    def _score_journey(self, event, evidence):
        actor = event.actor
        searches_10m = self.counter.count(("kind", actor, "search"), event.now, 600)
        categories_10m = self.counter.count(("kind", actor, "category"), event.now, 600)
        pdp_10m = self.counter.count(("kind", actor, "pdp"), event.now, 600)

        if event.is_pdp and not event.referer:
            evidence.append(Evidence(
                "direct_pdp_without_referer",
                10,
                "PDP reached without referer; acceptable alone, suspicious with high SKU velocity.",
            ))

        if pdp_10m >= 15 and searches_10m == 0 and categories_10m == 0:
            evidence.append(Evidence(
                "no_discovery_before_pdp_walk",
                25,
                f"{pdp_10m} PDPs in 10m with no search/category discovery in same actor journey.",
            ))

        if event.response_ms and event.response_ms < 250 and event.is_pdp:
            evidence.append(Evidence(
                "implausibly_short_pdp_processing",
                10,
                f"PDP response cycle recorded at {event.response_ms}ms; useful as weak signal only.",
            ))

    def _score_identity(self, event, evidence):
        if not event.user_agent:
            evidence.append(Evidence("missing_user_agent", 20, "User-Agent is absent."))

        ua = (event.user_agent or "").lower()
        if event.user_agent and "mozilla" not in ua and "bestbuy" not in ua:
            evidence.append(Evidence(
                "non_browser_user_agent",
                20,
                f"User-Agent does not look like a browser: {event.user_agent[:80]}",
            ))

        if not event.js_challenge_passed:
            evidence.append(Evidence(
                "js_challenge_failed",
                40,
                "Actor failed client-side integrity or bot challenge.",
            ))

        asn_org = (event.asn_org or "").lower()
        if any(hint in asn_org for hint in KNOWN_DATACENTER_ASN_HINTS):
            evidence.append(Evidence(
                "datacenter_asn",
                20,
                f"Traffic comes from hosting/cloud ASN: {event.asn_org}",
            ))

        if self._is_private_ip(event.ip):
            return

        if event.country and event.accept_language:
            lang = event.accept_language.lower()
            if event.country.upper() == "US" and not ("en-us" in lang or "en" in lang):
                evidence.append(Evidence(
                    "locale_country_mismatch",
                    8,
                    f"US traffic with accept-language={event.accept_language}. Weak signal.",
                ))

    def _score_browser_integrity(self, event, evidence):
        """Client telemetry that should be collected by first-party JS."""
        if event.webdriver_flag:
            evidence.append(Evidence(
                "webdriver_flag_present",
                50,
                "Client telemetry reported webdriver automation flag.",
            ))

        if event.cdp_detected:
            evidence.append(Evidence(
                "cdp_automation_surface",
                35,
                "Client telemetry observed Chrome DevTools Protocol automation surface.",
            ))

        if event.viewport_width == 1366 and event.viewport_height == 768:
            evidence.append(Evidence(
                "fixed_1366x768_viewport",
                12,
                "Actor repeatedly uses 1366x768 viewport; weak alone, useful with PDP walk.",
            ))

        if event.image_load_ratio < 0.2 and event.page_kind in ("pdp", "search", "category"):
            evidence.append(Evidence(
                "low_image_load_ratio",
                20,
                f"Only {event.image_load_ratio:.0%} of expected product images loaded.",
            ))

        ua = event.user_agent or ""
        if "Chrome/121.0.0.0" in ua:
            evidence.append(Evidence(
                "stale_fixed_chrome_121_ua",
                12,
                "Fixed Chrome/121 UA is stale for current consumer traffic and matches known crawler profile.",
            ))

    def _score_known_crawler_flow(self, event, evidence):
        """Detect flow patterns observed in bby_tv_crawl_v2.py."""
        if event.is_pdp and "searchpage.jsp?st=tv" in (event.referer or ""):
            pdp_10m = self.counter.count(("kind", event.actor, "pdp"), event.now, 600)
            if pdp_10m >= 10:
                evidence.append(Evidence(
                    "repeated_synthetic_search_referer",
                    20,
                    f"{pdp_10m} PDP visits in 10m all appear to come from generic TV search referer.",
                ))

        if event.prior_path == "about:blank" and event.is_pdp:
            evidence.append(Evidence(
                "about_blank_then_same_pdp_retry",
                35,
                "Actor navigated about:blank immediately before PDP retry; matches automated recovery flow.",
            ))

        skus_10m = self.counter.values(("sku", event.actor), event.now, 600)
        if len(skus_10m) >= 20:
            numeric_skus = []
            for sku in skus_10m[-20:]:
                try:
                    numeric_skus.append(int(sku))
                except Exception:
                    pass
            if len(numeric_skus) >= 12:
                direction_changes = 0
                prev_direction = 0
                for left, right in zip(numeric_skus, numeric_skus[1:]):
                    direction = 1 if right > left else -1 if right < left else 0
                    if prev_direction and direction and direction != prev_direction:
                        direction_changes += 1
                    if direction:
                        prev_direction = direction
                if direction_changes <= 2:
                    evidence.append(Evidence(
                        "monotonic_sku_sequence",
                        20,
                        "Recent SKU visits are mostly monotonic; indicates CSV/list traversal rather than shopping.",
                    ))

    def _score_region_inventory_probe(self, event, evidence):
        actor = event.actor
        zips_10m = self.counter.values(("zip", actor), event.now, 600)
        unique_zips = len(set(zips_10m))
        if unique_zips >= 8:
            evidence.append(Evidence(
                "multi_zip_inventory_probe",
                30,
                f"{unique_zips} ZIP codes queried by one actor in 10m; normal users rarely probe many pickup regions.",
            ))

        if event.sku:
            sku_zips = self.counter.values(("sku_zip", actor, event.sku), event.now, 600)
            unique_sku_zips = len(set(sku_zips))
            if unique_sku_zips >= 5:
                evidence.append(Evidence(
                    "same_sku_many_zip_probe",
                    25,
                    f"SKU {event.sku} checked against {unique_sku_zips} ZIP codes in 10m.",
                ))

    def _score_distributed_queue_pattern(self, event, evidence):
        if not event.account_id:
            return

        actors_1h = self.counter.values(("account_actor", event.account_id), event.now, 3600)
        unique_actors = len(set(actors_1h))
        skus_1h = self.counter.values(("account_sku", event.account_id), event.now, 3600)
        unique_skus = len(set(skus_1h))
        if unique_actors >= 6 and unique_skus >= 40:
            evidence.append(Evidence(
                "distributed_catalog_queue",
                35,
                f"Account group used {unique_actors} actors to touch {unique_skus} unique SKUs in 1h.",
            ))

    def _score_block_response(self, event, evidence):
        actor = event.actor
        last_block = self.last_block_by_actor.get(actor)
        if not last_block:
            return

        elapsed = event.now - last_block
        if elapsed <= timedelta(minutes=5):
            evidence.append(Evidence(
                "immediate_retry_after_block",
                45,
                f"Actor retried {int(elapsed.total_seconds())}s after prior block/challenge.",
            ))
        elif elapsed <= timedelta(minutes=15):
            evidence.append(Evidence(
                "retry_after_recent_block",
                30,
                f"Actor retried {int(elapsed.total_seconds() / 60)}m after prior block/challenge.",
            ))

    @staticmethod
    def _decision(score, evidence):
        codes = {item.code for item in evidence}
        if "js_challenge_failed" in codes and score >= 70:
            return "block", 3600
        if "immediate_retry_after_block" in codes:
            return "block", 1800
        if score >= 90:
            return "block", 1800
        if score >= 65:
            return "challenge", 900
        if score >= 40:
            return "throttle", 300
        return "allow", 0

    @staticmethod
    def _is_private_ip(ip):
        try:
            return ipaddress.ip_address(ip).is_private
        except ValueError:
            return False


def edge_adapter(headers, path, ip, status_code=200, response_ms=0):
    """Adapter example for CDN/edge logs."""
    return RequestEvent(
        ip=ip,
        method=headers.get("method", "GET"),
        path=path,
        user_agent=headers.get("user-agent", ""),
        session_id=headers.get("x-session-id", ""),
        device_id=headers.get("x-device-id", ""),
        account_id=headers.get("x-account-id", ""),
        referer=headers.get("referer", ""),
        accept_language=headers.get("accept-language", ""),
        country=headers.get("x-country", ""),
        asn_org=headers.get("x-asn-org", ""),
        status_code=status_code,
        response_ms=response_ms,
        js_challenge_passed=headers.get("x-js-ok", "1") == "1",
        viewport_width=int(headers.get("x-viewport-width", "0") or 0),
        viewport_height=int(headers.get("x-viewport-height", "0") or 0),
        timezone=headers.get("x-timezone", ""),
        webdriver_flag=headers.get("x-webdriver", "0") == "1",
        cdp_detected=headers.get("x-cdp-detected", "0") == "1",
        image_load_ratio=float(headers.get("x-image-load-ratio", "1") or 1),
        prior_path=headers.get("x-prior-path", ""),
    )


def explain(decision):
    """Return a compact human-readable explanation for SOC or product teams."""
    lines = [
        f"action={decision.action}",
        f"score={decision.score}",
        f"actor={decision.actor}",
        f"ttl_seconds={decision.ttl_seconds}",
    ]
    for item in decision.evidence:
        lines.append(f"- {item.code} (+{item.points}): {item.detail}")
    return "\n".join(lines)


def simulate_tv_scraper_case():
    """Concrete interview demo: listing-harvested TV PDP walk with API fan-out."""
    defense = RetailBotDefense()
    base = datetime(2026, 5, 16, 1, 0, tzinfo=timezone.utc)
    headers = {
        "user-agent": "Mozilla/5.0 Chrome/124",
        "x-device-id": "device-123",
        "referer": "",
        "accept-language": "en-US,en;q=0.9",
        "x-country": "US",
        "x-asn-org": "Residential ISP",
        "x-js-ok": "1",
    }

    decision = None
    for i in range(36):
        sku = 6500000 + i
        now = base + timedelta(seconds=i * 12)
        pdp = edge_adapter(headers, f"/site/samsung-tv/{sku}.p", "73.1.2.3")
        pdp.now = now
        decision = defense.score(pdp)
        for api in ("price", "fulfillment", "reviews"):
            api_event = edge_adapter(headers, f"/api/{api}?sku={sku}", "73.1.2.3", response_ms=120)
            api_event.now = now + timedelta(seconds=2)
            decision = defense.score(api_event)

    return explain(decision)
