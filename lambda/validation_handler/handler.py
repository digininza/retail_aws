"""Lambda: validation handler / streaming event consumer (Section 17, Section 16).

Consumes a batch of records from the POS/e-commerce Kinesis stream (Lambda's
native Kinesis event-source mapping), validates each event against the
shared schema (`src.ingestion.streaming.event_schema`), and writes valid
records to S3 raw/, invalid ones to quarantine. This is the "file/record
metadata validation" responsibility from Section 17's Lambda examples,
applied to streaming records rather than file drops (file-level validation
lives in `file_arrival_handler`). Kept intentionally simple — one
validate-and-land pass per record — consistent with Section 17's rule
against using Lambda for heavy transformation; enrichment/joins happen later
in Glue.

Timeout: 60s, batch size tuned so a full batch comfortably fits in that
window even under S3 write latency; a stuck batch fails and Kinesis retries
per the stream's retry/DLQ configuration (Section 16, Section 21).
"""
from __future__ import annotations

import json
from typing import Any

from src.common.logging_utils import get_logger
from src.ingestion.streaming.event_schema import validate_ecommerce_event, validate_pos_event

logger = get_logger(__name__)


def _decode_kinesis_record(record: dict[str, Any]) -> dict[str, Any]:
    import base64

    payload = base64.b64decode(record["kinesis"]["data"])
    return json.loads(payload)


def lambda_handler(event: dict[str, Any], context: Any) -> dict[str, Any]:
    accepted, rejected = [], []

    for record in event.get("Records", []):
        try:
            data = _decode_kinesis_record(record)
        except (json.JSONDecodeError, KeyError) as exc:
            logger.error("kinesis_record_decode_failed", extra={"error_category": "MALFORMED"})
            rejected.append({"reason": str(exc)})
            continue

        stream_name = record["eventSourceARN"].split("/")[-1]
        is_pos = "pos" in stream_name
        result = validate_pos_event(data) if is_pos else validate_ecommerce_event(data)

        if result.valid:
            logger.info("event_accepted", extra={"source": stream_name, "task": "land_to_raw"})
            # In AWS: write `data` to s3://.../raw/{pos_events|ecommerce_events}/
            accepted.append(data)
        else:
            logger.warning("event_rejected", extra={"source": stream_name, "error_category": "SCHEMA",
                                                      "records_rejected": 1})
            # In AWS: write to quarantine/DLQ with reason attached (Section 16)
            rejected.append({"event": data, "reason": result.reason})

    return {"statusCode": 200, "body": json.dumps({"accepted": len(accepted), "rejected": len(rejected)})}
