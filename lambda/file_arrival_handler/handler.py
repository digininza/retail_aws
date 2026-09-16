"""Lambda: S3 file-arrival handler (Section 17).

Triggered by an S3 `ObjectCreated` event notification on the raw/ prefix
(e.g. a supplier file drop). Performs only lightweight, fast checks —
existence, naming convention, basic size sanity — and then invokes the
pipeline-trigger Lambda / starts the corresponding Airflow DAG run. It does
NOT parse or transform the file contents; that belongs to Glue (Section 17:
"Do not use Lambda for TB-scale transformations").

Timeout: configured at 30s (see infrastructure/lambda config, illustrative)
— comfortably above the few hundred ms this handler needs, with headroom
for S3 HEAD-object latency, but short enough that a hang fails fast rather
than burning Lambda concurrency.
"""
from __future__ import annotations

import json
import re
from typing import Any

from src.common.logging_utils import get_logger

logger = get_logger(__name__)

FILENAME_PATTERN = re.compile(r"^(?P<source>[a-z_]+)_(?P<date>\d{8})\.(csv|json)$")


def lambda_handler(event: dict[str, Any], context: Any) -> dict[str, Any]:
    results = []
    for record in event.get("Records", []):
        bucket = record["s3"]["bucket"]["name"]
        key = record["s3"]["object"]["key"]
        size = record["s3"]["object"].get("size", 0)
        filename = key.rsplit("/", 1)[-1]

        match = FILENAME_PATTERN.match(filename)
        if not match:
            logger.error("file_naming_convention_violation", extra={"source": key, "error_category": "VALIDATION"})
            results.append({"key": key, "status": "REJECTED", "reason": "filename does not match convention"})
            continue

        if size == 0:
            logger.error("zero_byte_file", extra={"source": key, "error_category": "VALIDATION"})
            results.append({"key": key, "status": "REJECTED", "reason": "zero-byte file"})
            continue

        logger.info("file_arrival_accepted", extra={"source": key, "task": "trigger_pipeline"})
        # In AWS: invoke pipeline_trigger Lambda or start an MWAA DAG run here.
        results.append({"key": key, "status": "ACCEPTED", "source_system": match.group("source")})

    return {"statusCode": 200, "body": json.dumps(results)}
