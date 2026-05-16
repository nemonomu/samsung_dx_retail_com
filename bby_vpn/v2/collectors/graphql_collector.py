"""API-first GraphQL collector using httpx when available."""

import asyncio
import json
import time

from core.rate_limit import AsyncHostRateLimiter
from core.retry import ExponentialBackoff


class GraphQLCollector:
    def __init__(self, audit_log=None, timeout=20, concurrency=3, rate_limiter=None, retry_policy=None):
        self.audit_log = audit_log
        self.timeout = timeout
        self.semaphore = asyncio.Semaphore(concurrency)
        self.rate_limiter = rate_limiter or AsyncHostRateLimiter()
        self.retry_policy = retry_policy or ExponentialBackoff()

    async def execute(self, endpoint_url, payload, headers=None, cookies=None):
        try:
            import httpx
        except ImportError as exc:
            raise RuntimeError("httpx is required for GraphQLCollector") from exc

        headers = headers or {}
        cookies = cookies or {}
        async with self.semaphore:
            async with httpx.AsyncClient(timeout=self.timeout, headers=headers, cookies=cookies) as client:
                attempt = 1
                while True:
                    await self.rate_limiter.wait(endpoint_url)
                    started = time.time()
                    try:
                        response = await client.post(endpoint_url, json=payload)
                        elapsed_ms = int((time.time() - started) * 1000)
                        self._log("graphql_request", {
                            "endpoint_url": endpoint_url,
                            "operationName": payload.get("operationName") if isinstance(payload, dict) else None,
                            "status_code": response.status_code,
                            "elapsed_ms": elapsed_ms,
                            "attempt": attempt,
                        })
                        if response.status_code == 200:
                            body = response.json()
                            if isinstance(body, dict) and body.get("errors"):
                                self._log("graphql_errors", {"errors": body.get("errors"), "operationName": payload.get("operationName")})
                            return body

                        decision = self.retry_policy.decide(attempt, status_code=response.status_code, error_kind="http_status")
                    except Exception as exc:
                        self._log("graphql_exception", {"error": str(exc), "attempt": attempt})
                        decision = self.retry_policy.decide(attempt, error_kind="exception")

                    if not decision.retry:
                        return {"errors": [{"message": decision.reason, "terminal": decision.terminal}]}
                    await asyncio.sleep(decision.delay_seconds)
                    attempt += 1

    def execute_sync(self, endpoint_url, payload, headers=None, cookies=None):
        return asyncio.run(self.execute(endpoint_url, payload, headers=headers, cookies=cookies))

    def _log(self, event_type, payload):
        if self.audit_log:
            self.audit_log.write(event_type, payload)
        else:
            print(json.dumps({"event_type": event_type, **payload}, ensure_ascii=False))

