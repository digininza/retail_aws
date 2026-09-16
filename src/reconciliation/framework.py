"""Reconciliation framework entry point (Section 12).

Runs the checks defined in a named reconciliation profile
(config/base/reconciliation.yaml) and produces a run-level result written to
the audit/reconciliation table. Reconciliation is distinct from data
quality: DQ asks "is the data valid?"; reconciliation asks "did we move/
process the expected data correctly?"
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

import pandas as pd

from src.common.exceptions import ReconciliationError
from src.common.logging_utils import get_logger
from src.reconciliation.aggregate_check import check_aggregate
from src.reconciliation.control_totals import check_control_total, check_key_reconciliation
from src.reconciliation.record_count import check_record_count

logger = get_logger(__name__)


@dataclass
class ReconciliationRunResult:
    run_id: str
    pipeline_name: str
    source_count: int
    target_count: int
    source_total: float
    target_total: float
    variance: float
    status: str
    validation_timestamp: str
    check_details: list[dict[str, Any]] = field(default_factory=list)

    @property
    def passed(self) -> bool:
        return self.status == "PASSED"


def run_reconciliation(
    source_df: pd.DataFrame,
    target_df: pd.DataFrame,
    profile: dict,
    *,
    run_id: str,
    pipeline_name: str,
) -> ReconciliationRunResult:
    check_details: list[dict[str, Any]] = []
    all_passed = True

    for check in profile.get("checks", []):
        check_type = check["type"]
        if check_type == "RECORD_COUNT" or check_type == "PARTITION_DATE":
            result = check_record_count(len(source_df), len(target_df), check.get("tolerance_pct", 0.0))
        elif check_type == "AGGREGATE":
            result = check_aggregate(source_df, target_df, check["column"], check.get("agg", "SUM"),
                                      check.get("tolerance_pct", 0.01))
        elif check_type == "CONTROL_TOTAL":
            result = check_control_total(source_df, target_df, check["column"], check.get("tolerance_pct", 0.01))
        elif check_type == "KEY_RECONCILIATION":
            result = check_key_reconciliation(source_df, target_df, check["key"])
        else:
            continue
        check_details.append(vars(result) if hasattr(result, "__dict__") else result.__dict__)
        all_passed = all_passed and result.passed

    source_total = float(pd.to_numeric(source_df.iloc[:, -1], errors="coerce").sum()) if not source_df.empty else 0.0
    target_total = float(pd.to_numeric(target_df.iloc[:, -1], errors="coerce").sum()) if not target_df.empty else 0.0

    result = ReconciliationRunResult(
        run_id=run_id,
        pipeline_name=pipeline_name,
        source_count=len(source_df),
        target_count=len(target_df),
        source_total=source_total,
        target_total=target_total,
        variance=target_total - source_total,
        status="PASSED" if all_passed else "FAILED",
        validation_timestamp=datetime.now(timezone.utc).isoformat(),
        check_details=check_details,
    )
    logger.info(
        "reconciliation_result",
        extra={"pipeline": pipeline_name, "run_id": run_id, "status": result.status,
               "records_read": result.source_count, "records_written": result.target_count},
    )
    return result


def enforce(result: ReconciliationRunResult) -> None:
    if not result.passed:
        raise ReconciliationError(
            f"Reconciliation FAILED for pipeline '{result.pipeline_name}' (run {result.run_id}): "
            f"source_count={result.source_count} target_count={result.target_count}"
        )
