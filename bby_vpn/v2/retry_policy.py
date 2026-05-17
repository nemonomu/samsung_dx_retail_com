"""Retry policy for API-first collection."""

import random
from dataclasses import dataclass


@dataclass
class RetryDecision:
    retry: bool
    delay_seconds: float
    reason: str
    terminal: bool = False


class ExponentialBackoff:
    def __init__(self, max_attempts=4, base_delay=2.0, max_delay=120.0):
        self.max_attempts = max_attempts
        self.base_delay = base_delay
        self.max_delay = max_delay

    def decide(self, attempt, status_code=None, error_kind="error"):
        if status_code in (401, 403) or error_kind in ("blocked", "captcha", "forbidden"):
            return RetryDecision(False, 0, error_kind, terminal=True)
        if attempt >= self.max_attempts:
            return RetryDecision(False, 0, "max_attempts", terminal=True)

        multiplier = 4 if status_code == 429 else 2
        delay = min(self.max_delay, self.base_delay * (multiplier ** max(0, attempt - 1)))
        delay *= random.uniform(0.75, 1.35)
        return RetryDecision(True, delay, error_kind)

