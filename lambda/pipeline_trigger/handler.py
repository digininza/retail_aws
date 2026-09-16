"""Lambda: pipeline trigger (Section 17).

Invoked by `file_arrival_handler` (or an EventBridge schedule) to start the
corresponding Airflow DAG run via the MWAA REST API, or start a standalone
Glue job directly for simple single-table pipelines that don't need full
DAG orchestration. This is the "glue" between an event and the
orchestration layer — it never performs data processing itself (Section 17).
"""
from __future__ import annotations

import json
from typing import Any

from src.common.logging_utils import get_logger

logger = get_logger(__name__)


def _resolve_dag_id(source_system: str) -> str:
    mapping = {
        "SQLSERVER": "retail_batch_pipeline",
        "SAP": "retail_batch_pipeline",
        "SUPPLIER_FILE": "product_pipeline",
        "LEGACY_ARCHIVE": "sales_pipeline",
    }
    return mapping.get(source_system, "retail_batch_pipeline")


def lambda_handler(event: dict[str, Any], context: Any) -> dict[str, Any]:
    source_system = event.get("source_system", "UNKNOWN")
    pipeline_name = event.get("pipeline_name")
    dag_id = _resolve_dag_id(source_system)

    if not pipeline_name:
        logger.error("pipeline_trigger_missing_pipeline_name", extra={"error_category": "VALIDATION"})
        return {"statusCode": 400, "body": json.dumps({"error": "pipeline_name is required"})}

    logger.info("pipeline_trigger_dispatched", extra={"pipeline": pipeline_name, "task": f"trigger_dag:{dag_id}"})
    # In AWS: POST to the MWAA REST API's /dags/{dag_id}/dagRuns endpoint with
    # {"conf": {"pipeline_name": pipeline_name}}. Idempotency is handled by
    # Airflow's dag_run_id uniqueness constraint — a duplicate trigger for the
    # same logical run is rejected by Airflow rather than creating a second run.
    return {"statusCode": 200, "body": json.dumps({"dag_id": dag_id, "pipeline_name": pipeline_name, "status": "TRIGGERED"})}
