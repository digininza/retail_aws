"""Exception hierarchy shared across ingestion, processing, DQ, and reconciliation.

Retryable vs. non-retryable classification is used directly by
`src.common.retry` to decide whether a failure should be retried
(Section 22 — do not retry business-rule/data-quality failures indefinitely).
"""


class RetailPlatformError(Exception):
    """Base class for all platform-raised exceptions."""


class RetryableError(RetailPlatformError):
    """Transient failure (network blip, throttling, source timeout).

    Safe to retry with backoff. Examples: connection reset, HTTP 429/503,
    S3 slow-down, temporary lock contention.
    """


class NonRetryableError(RetailPlatformError):
    """Permanent failure. Retrying will not help — stop and alert."""


class SourceUnavailableError(RetryableError):
    """Source system connection failed or timed out."""


class ApiRateLimitError(RetryableError):
    """Source REST API returned a rate-limit response (HTTP 429)."""


class SchemaValidationError(NonRetryableError):
    """A BREAKING schema change was detected (Section 24)."""


class DataQualityError(NonRetryableError):
    """One or more CRITICAL data-quality rules failed (Section 11)."""


class ReconciliationError(NonRetryableError):
    """Source/target reconciliation variance exceeded tolerance (Section 12)."""


class ConfigurationError(NonRetryableError):
    """Metadata/config could not be resolved (missing pipeline, bad YAML, etc.)."""


class WatermarkConflictError(NonRetryableError):
    """Concurrent run detected against the same pipeline's control-table row."""


class IdempotencyViolationError(NonRetryableError):
    """A write would have produced a non-idempotent duplicate effect."""
