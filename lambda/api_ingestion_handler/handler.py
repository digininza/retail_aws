"""Lambda: lightweight API ingestion handler (Section 17).

Distinct from `glue/jobs/ingest_api.py`: that Glue Python Shell job performs
the full paginated pull of an entire feed (promotions, marketplace) on a
schedule. This Lambda handles the lightweight, single-call cases — a
partner webhook delivering one promotion update, or a scheduled EventBridge
rule calling a small reference-data endpoint (e.g. FX rates) — where
spinning up a Glue job would be disproportionate. Section 17: "Do not use
Lambda for TB-scale transformations" — this handler never processes more
than one API response per invocation.

Timeout: 15s, matched to the external API's documented p99 latency plus one
retry; anything slower should be a Glue Python Shell job instead.
"""
from __future__ import annotations

import json
from typing import Any

from src.common.exceptions import ApiRateLimitError, RetryableError
from src.common.logging_utils import get_logger
from src.common.retry import RetryPolicy, with_retry

logger = get_logger(__name__)


class _SingleCallApiClient:
    """Stand-in for a single lightweight authenticated API call (e.g. an FX
    reference-data lookup). Real credentials are resolved via
    SecretsManagerAdapter, never hardcoded (Section 19)."""

    def __init__(self, payload: dict[str, Any], simulate_failure: bool = False):
        self._payload = payload
        self._simulate_failure = simulate_failure

    def call(self) -> dict[str, Any]:
        if self._simulate_failure:
            raise ApiRateLimitError("Simulated transient API failure")
        return self._payload


@with_retry(RetryPolicy(max_attempts=3, initial_delay_seconds=1, max_delay_seconds=4))
def _call_with_retry(client: _SingleCallApiClient) -> dict[str, Any]:
    return client.call()


def lambda_handler(event: dict[str, Any], context: Any) -> dict[str, Any]:
    request_payload = event.get("payload", {})
    client = _SingleCallApiClient(payload=request_payload, simulate_failure=event.get("simulate_failure", False))

    try:
        result = _call_with_retry(client)
    except RetryableError as exc:
        logger.error("api_call_failed_after_retries", extra={"error_category": "API_UNAVAILABLE"})
        return {"statusCode": 502, "body": json.dumps({"error": str(exc)})}

    logger.info("api_call_succeeded", extra={"task": "single_api_ingest"})
    return {"statusCode": 200, "body": json.dumps(result)}
