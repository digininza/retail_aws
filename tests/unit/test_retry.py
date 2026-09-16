import pytest

from src.common.exceptions import DataQualityError, SourceUnavailableError
from src.common.retry import RetryPolicy, with_retry


def test_retries_retryable_error_then_succeeds():
    calls = {"count": 0}

    @with_retry(RetryPolicy(max_attempts=3, initial_delay_seconds=0.01, jitter=False))
    def flaky():
        calls["count"] += 1
        if calls["count"] < 3:
            raise SourceUnavailableError("transient")
        return "ok"

    assert flaky() == "ok"
    assert calls["count"] == 3


def test_exhausts_retries_and_raises():
    @with_retry(RetryPolicy(max_attempts=2, initial_delay_seconds=0.01, jitter=False))
    def always_fails():
        raise SourceUnavailableError("still down")

    with pytest.raises(SourceUnavailableError):
        always_fails()


def test_non_retryable_error_propagates_immediately():
    calls = {"count": 0}

    @with_retry(RetryPolicy(max_attempts=5, initial_delay_seconds=0.01, jitter=False))
    def business_rule_failure():
        calls["count"] += 1
        raise DataQualityError("critical DQ failure")

    with pytest.raises(DataQualityError):
        business_rule_failure()
    assert calls["count"] == 1  # must not retry a non-retryable/business-rule error


def test_delay_respects_exponential_backoff_bounds():
    policy = RetryPolicy(max_attempts=5, initial_delay_seconds=10, max_delay_seconds=60,
                          exponential_backoff=True, jitter=False)
    assert policy.delay_for_attempt(1) == 10
    assert policy.delay_for_attempt(2) == 20
    assert policy.delay_for_attempt(3) == 40
    assert policy.delay_for_attempt(10) == 60  # capped at max_delay_seconds
