"""E-commerce event producer (Section 16). Partitions by `customer_id` so a
given customer's session events (view -> cart -> order -> payment) stay
ordered within a shard, matching how the consumer reconstructs session flow.
"""
from __future__ import annotations

import json
from pathlib import Path

from src.common.logging_utils import get_logger
from src.ingestion.streaming.event_schema import partition_key_for
from streaming.consumers.local_kinesis_adapter import LocalKinesisStream

logger = get_logger(__name__)

SAMPLE_FILE = Path(__file__).resolve().parents[2] / "sample_data" / "events" / "ecommerce_events_sample.json"
STREAM_NAME = "retail-dev-ecommerce-events"


def run() -> None:
    stream = LocalKinesisStream(STREAM_NAME)
    count = 0
    with open(SAMPLE_FILE) as fh:
        for line in fh:
            if not line.strip():
                continue
            event = json.loads(line)
            stream.put_record(event, partition_key=partition_key_for(event, "customer_id"))
            count += 1
    logger.info("ecommerce_events_produced", extra={"source": "ecom_producer", "target": STREAM_NAME, "records_written": count})
    print(f"Produced {count} e-commerce events to local stream '{STREAM_NAME}'")


if __name__ == "__main__":
    run()
