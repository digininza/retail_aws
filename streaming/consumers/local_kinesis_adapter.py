"""Local file-backed stand-in for a Kinesis Data Stream (Section 16).

Producers append JSON-lines records to a per-stream file; the consumer
tails and removes them, giving an at-least-once, ordered-within-partition-key
simulation without needing AWS credentials. This is intentionally simple —
its only job is to let `pos_producer.py` / `ecom_producer.py` and
`event_consumer.py` demonstrate the real control flow (partition key
selection, checkpointing, DLQ-on-invalid) that a real Kinesis integration
would also need.
"""
from __future__ import annotations

import json
import os
import time
from pathlib import Path
from typing import Any, Iterator

STREAM_ROOT = Path(os.getenv("LOCAL_KINESIS_ROOT", ".local_s3/kinesis"))


class LocalKinesisStream:
    def __init__(self, stream_name: str):
        self.stream_name = stream_name
        self.path = STREAM_ROOT / f"{stream_name}.jsonl"
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.path.touch(exist_ok=True)

    def put_record(self, data: dict[str, Any], partition_key: str) -> None:
        record = {"partition_key": partition_key, "data": data, "put_at": time.time()}
        with open(self.path, "a") as fh:
            fh.write(json.dumps(record) + "\n")

    def read_and_checkpoint(self, max_records: int = 100) -> Iterator[dict[str, Any]]:
        """Reads up to `max_records`, then truncates the file to only the
        remainder — simulating a Kinesis checkpoint advance after successful
        processing (Section 16). If processing fails before this is called,
        the records are re-read on the next invocation (at-least-once)."""
        if not self.path.exists():
            return
        lines = self.path.read_text().splitlines()
        to_process, remainder = lines[:max_records], lines[max_records:]
        for line in to_process:
            if line.strip():
                yield json.loads(line)
        self.path.write_text("\n".join(remainder) + ("\n" if remainder else ""))
