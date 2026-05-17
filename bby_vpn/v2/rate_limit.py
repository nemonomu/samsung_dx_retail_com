"""Async conservative rate coordination for API collectors."""

import asyncio
import time
from collections import defaultdict, deque
from urllib.parse import urlparse


class AsyncHostRateLimiter:
    def __init__(self, min_delay=2.0, max_per_minute=20):
        self.min_delay = min_delay
        self.max_per_minute = max_per_minute
        self.history = defaultdict(deque)
        self.locks = defaultdict(asyncio.Lock)

    async def wait(self, url):
        host = urlparse(url or "").netloc or "unknown"
        async with self.locks[host]:
            now = time.monotonic()
            bucket = self.history[host]
            while bucket and now - bucket[0] > 60:
                bucket.popleft()

            wait_for = 0.0
            if bucket:
                wait_for = max(wait_for, self.min_delay - (now - bucket[-1]))
            if len(bucket) >= self.max_per_minute:
                wait_for = max(wait_for, 60 - (now - bucket[0]))
            if wait_for > 0:
                await asyncio.sleep(wait_for)
            bucket.append(time.monotonic())

