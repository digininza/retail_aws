"""Quarantine writer (Section 11). Bad records are written to
`s3://.../quarantine/` with run/rule metadata attached so they can be
triaged and, once fixed upstream, replayed through the pipeline.
"""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

import pandas as pd

from src.common.logging_utils import get_logger
from src.common.utilities import LocalS3Adapter

logger = get_logger(__name__)


def quarantine_records(
    records: pd.DataFrame,
    *,
    run_id: str,
    pipeline_name: str,
    rule_name: str,
    error_reason: str,
    source_file: str,
    s3_adapter: LocalS3Adapter,
    quarantine_zone_uri: str,
) -> str:
    """Writes `records` plus quarantine metadata columns to the quarantine zone.
    Returns the path written to."""
    if records.empty:
        return ""
    enriched = records.copy()
    enriched["run_id"] = run_id
    enriched["pipeline"] = pipeline_name
    enriched["rule_name"] = rule_name
    enriched["error_reason"] = error_reason
    enriched["source_file"] = source_file
    enriched["ingestion_timestamp"] = datetime.now(timezone.utc).isoformat()

    target_uri = f"{quarantine_zone_uri}{pipeline_name}/{run_id}/"
    path = s3_adapter.write_dataframe(enriched, target_uri, fmt="json")
    logger.warning(
        "records_quarantined",
        extra={
            "pipeline": pipeline_name,
            "run_id": run_id,
            "records_rejected": len(records),
            "error_category": rule_name,
            "target": target_uri,
        },
    )
    return path
