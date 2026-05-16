"""Synthetic red-team simulator for BestBuy-style bot defense.

This does not crawl BestBuy and does not bypass controls. It creates local
request events that represent likely scraper evolution paths, then feeds them
into bby_bot_defense.RetailBotDefense to prove which signals remain detectable.
"""

from datetime import datetime, timedelta, timezone

from bby_bot_defense import RequestEvent, RetailBotDefense, explain


def event(now, path, actor="device-redteam", referer="", ua=None, **kwargs):
    return RequestEvent(
        ip=kwargs.pop("ip", "73.1.2.3"),
        method="GET",
        path=path,
        user_agent=ua or "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/141.0.0.0 Safari/537.36",
        device_id=actor,
        session_id=f"session-{actor}",
        referer=referer,
        accept_language=kwargs.pop("accept_language", "en-US,en;q=0.9"),
        country=kwargs.pop("country", "US"),
        asn_org=kwargs.pop("asn_org", "Residential ISP"),
        now=now,
        **kwargs,
    )


def scenario_existing_v2():
    """Observed v2 pattern: generic search referer, PDP walk, API fan-out."""
    defense = RetailBotDefense()
    base = datetime(2026, 5, 16, 1, 0, tzinfo=timezone.utc)
    decision = None

    for i in range(36):
        sku = 6500000 + i
        now = base + timedelta(seconds=i * 12)
        decision = defense.score(event(
            now,
            f"/site/samsung-tv/{sku}.p",
            referer="https://www.bestbuy.com/site/searchpage.jsp?st=tv",
            ua="Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/121.0.0.0 Safari/537.36",
            viewport_width=1366,
            viewport_height=768,
            cdp_detected=True,
        ))
        for api in ("price", "fulfillment", "reviews"):
            decision = defense.score(event(
                now + timedelta(seconds=2),
                f"/api/{api}?sku={sku}",
                referer=f"https://www.bestbuy.com/site/samsung-tv/{sku}.p",
            ))

    return decision


def scenario_adversary_looks_more_human():
    """Attacker varies superficial signals but still needs many TV PDP facts."""
    defense = RetailBotDefense()
    base = datetime(2026, 5, 16, 2, 0, tzinfo=timezone.utc)
    decision = None

    for i in range(28):
        sku = 6600000 + i
        now = base + timedelta(seconds=i * 35)
        if i % 7 == 0:
            defense.score(event(now, "/site/searchpage.jsp?st=tv"))
        referer = "https://www.bestbuy.com/site/searchpage.jsp?st=tv" if i % 3 else ""
        decision = defense.score(event(
            now + timedelta(seconds=5),
            f"/site/lg-tv/{sku}.p",
            referer=referer,
            viewport_width=1536 if i % 2 else 1440,
            viewport_height=864 if i % 2 else 900,
            image_load_ratio=0.95,
        ))
        for api in ("price", "fulfillment"):
            decision = defense.score(event(
                now + timedelta(seconds=12),
                f"/api/{api}?sku={sku}",
                referer=f"https://www.bestbuy.com/site/lg-tv/{sku}.p",
            ))

    return decision


def scenario_block_retry():
    """Actor receives a block/challenge and retries the same PDP after a short pause."""
    defense = RetailBotDefense()
    base = datetime(2026, 5, 16, 3, 0, tzinfo=timezone.utc)
    sku = 6700001

    first = event(base, f"/site/sony-tv/{sku}.p", status_code=429)
    defense.score(first)
    retry = event(
        base + timedelta(minutes=3),
        f"/site/sony-tv/{sku}.p",
        prior_path="about:blank",
    )
    return defense.score(retry)


def scenario_distributed_low_and_slow_queue():
    """Multiple devices stay individually slow but share one account-level work queue."""
    defense = RetailBotDefense()
    base = datetime(2026, 5, 16, 4, 0, tzinfo=timezone.utc)
    decision = None

    for i in range(72):
        actor = f"device-worker-{i % 8}"
        sku = 6800000 + i
        now = base + timedelta(seconds=i * 45)
        decision = defense.score(event(
            now,
            f"/site/tv/{sku}.p",
            actor=actor,
            account_id="crawl-account-cluster-1",
            session_id=f"session-{actor}",
            ip=f"73.1.2.{10 + (i % 8)}",
            referer="https://www.bestbuy.com/site/searchpage.jsp?st=tv",
            viewport_width=1440 + (i % 3) * 40,
            viewport_height=900,
        ))

    return decision


def scenario_region_zip_inventory_probe():
    """One SKU is checked across many ZIP regions for pickup/ship availability."""
    defense = RetailBotDefense()
    base = datetime(2026, 5, 16, 5, 0, tzinfo=timezone.utc)
    sku = 6900001
    zips = ["10001", "60601", "30301", "75201", "94105", "98101", "85001", "02108"]
    decision = None

    for i, zip_code in enumerate(zips):
        now = base + timedelta(seconds=i * 30)
        defense.score(event(now, f"/site/samsung-tv/{sku}.p"))
        decision = defense.score(event(
            now + timedelta(seconds=5),
            f"/api/fulfillment?sku={sku}&zip={zip_code}",
            referer=f"https://www.bestbuy.com/site/samsung-tv/{sku}.p",
        ))

    return decision


def scenario_mobile_desktop_render_diff():
    """Same actor compares mobile and desktop render paths for the same SKU set."""
    defense = RetailBotDefense()
    base = datetime(2026, 5, 16, 6, 0, tzinfo=timezone.utc)
    decision = None
    mobile_ua = "Mozilla/5.0 (iPhone; CPU iPhone OS 18_4 like Mac OS X) AppleWebKit/605.1.15 Mobile/15E148"
    desktop_ua = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/141.0.0.0 Safari/537.36"

    for i in range(18):
        sku = 7000000 + i
        now = base + timedelta(seconds=i * 25)
        decision = defense.score(event(
            now,
            f"/site/tv/{sku}.p",
            ua=mobile_ua if i % 2 else desktop_ua,
            viewport_width=390 if i % 2 else 1440,
            viewport_height=844 if i % 2 else 900,
        ))
        decision = defense.score(event(
            now + timedelta(seconds=4),
            f"/api/price?sku={sku}",
            referer=f"https://www.bestbuy.com/site/tv/{sku}.p",
        ))

    return decision


def run_all():
    scenarios = [
        ("existing_v2", scenario_existing_v2),
        ("adversary_looks_more_human", scenario_adversary_looks_more_human),
        ("block_retry", scenario_block_retry),
        ("distributed_low_and_slow_queue", scenario_distributed_low_and_slow_queue),
        ("region_zip_inventory_probe", scenario_region_zip_inventory_probe),
        ("mobile_desktop_render_diff", scenario_mobile_desktop_render_diff),
    ]
    for name, fn in scenarios:
        print("=" * 80)
        print(name)
        print("=" * 80)
        print(explain(fn()))
        print()


if __name__ == "__main__":
    run_all()
