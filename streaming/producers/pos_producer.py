"""POS event producer (Section 16). Reads sample POS transactions and puts
them onto the local Kinesis stand-in, partitioned by `store_id` so a given
store's events land in the same shard/order in a real deployment — this
matters because downstream store-level reconciliation assumes intra-store
ordering (Section 16: "ordering considerations").
"""
from __future__ import annotations

import json
from pathlib import Path

from src.common.logging_utils import get_logger
from src.ingestion.streaming.event_schema import partition_key_for
from streaming.consumers.local_kinesis_adapter import LocalKinesisStream

logger = get_logger(__name__)

SAMPLE_FILE = Path(__file__).resolve().parents[2] / "sample_data" / "events" / "pos_events_sample.json"
STREAM_NAME = "retail-dev-pos-events"


def run() -> None:
    stream = LocalKinesisStream(STREAM_NAME)
    count = 0
    with open(SAMPLE_FILE) as fh:
        for line in fh:
            if not line.strip():
                continue
            event = json.loads(line)
            stream.put_record(event, partition_key=partition_key_for(event, "store_id"))
            count += 1
    logger.info("pos_events_produced", extra={"source": "pos_producer", "target": STREAM_NAME, "records_written": count})
    print(f"Produced {count} POS events to local stream '{STREAM_NAME}'")


if __name__ == "__main__":
    run()
