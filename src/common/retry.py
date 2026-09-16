"""Reusable retry framework (Section 22).

Supports max attempts, exponential backoff, jitter, and an explicit
retryable/non-retryable exception split so business-rule and data-quality
failures are never silently retried into a false sense of health.
"""
from __future__ import annotations

import functools
import random
import time
from dataclasses import dataclass
from typing import Callable, TypeVar

from src.common.exceptions import NonRetryableError, RetryableError
from src.common.logging_utils import get_logger

logger = get_logger(__name__)

T = TypeVar("T")


@dataclass
class RetryPolicy:
    max_attempts: int = 3
    initial_delay_seconds: float = 30
    max_delay_seconds: float = 300
    exponential_backoff: bool = True
    jitter: bool = True

    @classmethod
    def from_config(cls, config: dict) -> "RetryPolicy":
        return cls(
            max_attempts=int(config.get("max_attempts", 3)),
            initial_delay_seconds=float(config.get("initial_delay_seconds", 30)),
            max_delay_seconds=float(config.get("max_delay_seconds", 300)),
            exponential_backoff=bool(config.get("exponential_backoff", True)),
            jitter=bool(config.get("jitter", True)),
        )

    def delay_for_attempt(self, attempt: int) -> float:
        """attempt is 1-indexed (first retry = attempt 1)."""
        if self.exponential_backoff:
            delay = self.initial_delay_seconds * (2 ** (attempt - 1))
        else:
            delay = self.initial_delay_seconds
        delay = min(delay, self.max_delay_seconds)
        if self.jitter:
            delay = random.uniform(0, delay)
        return delay


def with_retry(
    policy: RetryPolicy | None = None,
    sleep_fn: Callable[[float], None] = time.sleep,
) -> Callable[[Callable[..., T]], Callable[..., T]]:
    """Decorator applying `policy` to a callable.

    Only exceptions that are (or subclass) `RetryableError` are retried.
    A `NonRetryableError` — including DataQualityError, SchemaValidationError,
    and ReconciliationError — propagates immediately on first occurrence.
    """
    policy = policy or RetryPolicy()

    def decorator(fn: Callable[..., T]) -> Callable[..., T]:
        @functools.wraps(fn)
        def wrapper(*args, **kwargs) -> T:
            last_exc: Exception | None = None
            for attempt in range(1, policy.max_attempts + 1):
                try:
                    return fn(*args, **kwargs)
                except NonRetryableError:
                    raise
                except RetryableError as exc:
                    last_exc = exc
                    if attempt >= policy.max_attempts:
                        logger.error(
                            "Retry attempts exhausted",
                            extra={"attempts": attempt, "function": fn.__name__},
                        )
                        raise
                    delay = policy.delay_for_attempt(attempt)
                    logger.warning(
                        "Retryable failure, backing off",
                        extra={
                            "attempt": attempt,
                            "max_attempts": policy.max_attempts,
                            "delay_seconds": round(delay, 2),
                            "function": fn.__name__,
                            "error": str(exc),
                        },
                    )
                    sleep_fn(delay)
            # Unreachable, but keeps type checkers satisfied.
            assert last_exc is not None
            raise last_exc

        return wrapper

    return decorator
