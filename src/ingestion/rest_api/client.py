"""REST API extraction: pagination, auth placeholder, rate limiting, retries,
response validation (Section 2C). Used for the promotions feed and
marketplace product feed. This module contains real, runnable pagination and
backoff logic against a local mock HTTP layer (`MockApiTransport`) so the
control flow — not just the concept — is demonstrated and unit-testable.
"""
from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Any, Callable, Iterator, Optional

from src.common.exceptions import ApiRateLimitError, NonRetryableError
from src.common.logging_utils import get_logger
from src.common.retry import RetryPolicy, with_retry

logger = get_logger(__name__)


@dataclass
class ApiPage:
    records: list[dict[str, Any]]
    next_cursor: Optional[str]


class MockApiTransport:
    """Local stand-in for an authenticated REST call.

    `pages` is pre-baked test/demo data; `fail_first_n_calls` simulates rate
    limiting (HTTP 429) to exercise the retry/backoff path.
    """

    def __init__(self, pages: list[ApiPage], fail_first_n_calls: int = 0):
        self._pages = pages
        self._fail_first_n_calls = fail_first_n_calls
        self._calls = 0

    def get_page(self, cursor: Optional[str]) -> ApiPage:
        self._calls += 1
        if self._calls <= self._fail_first_n_calls:
            raise ApiRateLimitError("Simulated HTTP 429 rate limit")
        index = 0 if cursor is None else int(cursor)
        if index >= len(self._pages):
            return ApiPage(records=[], next_cursor=None)
        return self._pages[index]


class RestApiClient:
    """Pulls all pages for a feed, respecting rate limits and retrying
    transient failures. Non-retryable response validation errors abort
    immediately (Section 21 — API rate limit vs. permanent failure)."""

    def __init__(self, transport: MockApiTransport, rate_limit_per_minute: int = 60):
        self.transport = transport
        self.min_interval_seconds = 60.0 / max(rate_limit_per_minute, 1)
        self._last_call_ts: Optional[float] = None

    def _respect_rate_limit(self, sleep_fn: Callable[[float], None]) -> None:
        if self._last_call_ts is not None:
            elapsed = time.monotonic() - self._last_call_ts
            wait = self.min_interval_seconds - elapsed
            if wait > 0:
                sleep_fn(wait)
        self._last_call_ts = time.monotonic()

    @with_retry(RetryPolicy(max_attempts=5, initial_delay_seconds=1, max_delay_seconds=8))
    def _fetch_page(self, cursor: Optional[str]) -> ApiPage:
        return self.transport.get_page(cursor)

    def fetch_all(self, validate_record: Optional[Callable[[dict], None]] = None,
                   sleep_fn: Callable[[float], None] = time.sleep) -> Iterator[dict[str, Any]]:
        cursor: Optional[str] = None
        page_count = 0
        while True:
            self._respect_rate_limit(sleep_fn)
            page = self._fetch_page(cursor)
            page_count += 1
            logger.info("api_page_fetched", extra={"records_read": len(page.records), "task": f"page_{page_count}"})
            for record in page.records:
                if validate_record:
                    try:
                        validate_record(record)
                    except NonRetryableError:
                        logger.error("api_record_validation_failed", extra={"error_category": "SCHEMA"})
                        raise
                yield record
            if page.next_cursor is None:
                break
            cursor = page.next_cursor
