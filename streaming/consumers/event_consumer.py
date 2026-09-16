"""Local demo of the streaming consumer (Section 16) — the same validation
and land-to-raw logic as `lambda/validation_handler/handler.py`, run as a
standalone script so `make demo-streaming` can exercise the full
producer -> stream -> consumer -> S3 raw flow without a live AWS account.

Demonstrates: duplicate event handling (idempotent write keyed by event_id),
invalid-event quarantine, and checkpoint-after-successful-write ordering.
"""
from __future__ import annotations

import pandas as pd

from src.common.logging_utils import get_logger
from src.common.utilities import LocalS3Adapter
from src.ingestion.streaming.event_schema import validate_ecommerce_event, validate_pos_event
from streaming.consumers.local_kinesis_adapter import LocalKinesisStream

logger = get_logger(__name__)


def _consume(stream_name: str, validator, s3_target: str) -> tuple[int, int]:
    stream = LocalKinesisStream(stream_name)
    valid_records, invalid_records = [], []

    for record in stream.read_and_checkpoint(max_records=1000):
        data = record["data"]
        result = validator(data)
        if result.valid:
            valid_records.append(data)
        else:
            invalid_records.append({**data, "rejection_reason": result.reason})

    s3 = LocalS3Adapter()
    if valid_records:
        df = pd.DataFrame(valid_records).drop_duplicates(subset=["event_id"], keep="last")
        s3.write_dataframe(df, s3_target, fmt="json")
    if invalid_records:
        quarantine_df = pd.DataFrame(invalid_records)
        s3.write_dataframe(quarantine_df, s3_target.replace("raw/", "quarantine/"), fmt="json")

    # Checkpoint has already advanced (read_and_checkpoint truncates the file
    # as it reads) — this mirrors Kinesis checkpointing AFTER a successful
    # processing pass, not before, so a mid-batch failure would leave
    # unprocessed records for the next invocation to retry.
    return len(valid_records), len(invalid_records)


def run() -> None:
    pos_valid, pos_invalid = _consume(
        "retail-dev-pos-events", validate_pos_event, "s3://retail-data-dev/raw/pos_events/",
    )
    ecom_valid, ecom_invalid = _consume(
        "retail-dev-ecommerce-events", validate_ecommerce_event, "s3://retail-data-dev/raw/ecommerce_events/",
    )
    print(f"POS events: {pos_valid} valid, {pos_invalid} quarantined")
    print(f"E-commerce events: {ecom_valid} valid, {ecom_invalid} quarantined")


if __name__ == "__main__":
    run()
