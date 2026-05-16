"""Network-aware endpoint metrics."""

from collections import Counter, defaultdict


class EndpointMetrics:
    def __init__(self):
        self.status_counts = Counter()
        self.graphql_errors = Counter()
        self.slow = []
        self.by_endpoint = defaultdict(Counter)

    def record(self, endpoint, status_code=None, elapsed_ms=None, graphql_errors=None):
        if status_code is not None:
            self.status_counts[str(status_code)] += 1
            self.by_endpoint[endpoint][str(status_code)] += 1
        if graphql_errors:
            self.graphql_errors[endpoint] += len(graphql_errors)
        if elapsed_ms and elapsed_ms >= 3000:
            self.slow.append({"endpoint": endpoint, "elapsed_ms": elapsed_ms})

    def summary(self):
        blocked = sum(self.status_counts[str(code)] for code in (401, 403, 429))
        total = sum(self.status_counts.values())
        return {
            "status_distribution": dict(self.status_counts),
            "graphql_error_rate": (sum(self.graphql_errors.values()) / total) if total else 0,
            "blocked_endpoint_count": blocked,
            "slow_endpoint_report": sorted(self.slow, key=lambda item: item["elapsed_ms"], reverse=True)[:20],
            "by_endpoint": {k: dict(v) for k, v in self.by_endpoint.items()},
        }
