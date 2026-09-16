"""Integration test: silver processing -> Redshift staging commit ->
control-table watermark advance, exercising the full guard lifecycle
end-to-end against local adapters (Section 7, Section 29)."""
from __future__ import annotations

import pandas as pd

from src.common.audit import AuditLogStore, ControlTableStore, new_run_id, utc_now_iso
from src.common.config_loader import ConfigLoader
from src.common.idempotency import RunStage, WatermarkCommitGuard
from src.common.utilities import LocalS3Adapter
from src.reconciliation import framework as recon_framework


def test_successful_run_advances_watermark_and_writes_audit():
    cfg = ConfigLoader(environment="dev")
    control, audit, s3 = ControlTableStore(), AuditLogStore(), LocalS3Adapter()

    pipeline_name = "sqlserver_orders_incremental"
    run_id = new_run_id()
    control.start_run(pipeline_name, "SQLSERVER", "orders", "INCREMENTAL", "modified_date", run_id)

    guard = WatermarkCommitGuard(pipeline_name, previous_watermark=None)
    df = pd.DataFrame({"order_id": ["O1", "O2"], "amount": [10, 20]})

    guard.advance(RunStage.EXTRACTED)
    s3.write_dataframe(df, cfg.s3_path("gold") + "redshift_staging/stg_orders/", fmt="parquet")
    guard.advance(RunStage.WRITTEN)
    guard.advance(RunStage.DQ_PASSED)

    recon_result = recon_framework.run_reconciliation(
        df, df, {"checks": [{"type": "RECORD_COUNT", "tolerance_pct": 0}]}, run_id=run_id, pipeline_name=pipeline_name,
    )
    assert recon_result.passed
    guard.advance(RunStage.RECONCILED)
    guard.advance(RunStage.COMMITTED)

    new_watermark = "2026-08-10T00:00:00"
    watermark_to_persist = guard.resolved_watermark(new_watermark)
    control.complete_run(pipeline_name, run_id, "SUCCESS", len(df), len(df), watermark_to_persist)
    audit.write(run_id, pipeline_name, "dev", utc_now_iso(), utc_now_iso(), "SUCCESS",
                len(df), len(df), 0, "orders", "redshift.stg_orders", None, watermark_to_persist)

    stored = control.get(pipeline_name)
    assert stored.last_successful_watermark == new_watermark
    assert stored.last_run_status == "SUCCESS"

    history = audit.history(pipeline_name)
    assert len(history) == 1
    assert history[0]["status"] == "SUCCESS"


def test_failed_run_never_advances_watermark():
    control = ControlTableStore()
    pipeline_name = "sqlserver_orders_incremental"

    run_id_1 = new_run_id()
    control.start_run(pipeline_name, "SQLSERVER", "orders", "INCREMENTAL", "modified_date", run_id_1)
    control.complete_run(pipeline_name, run_id_1, "SUCCESS", 10, 10, "2026-08-01T00:00:00")

    # Second run fails before reaching COMMITTED.
    run_id_2 = new_run_id()
    control.start_run(pipeline_name, "SQLSERVER", "orders", "INCREMENTAL", "modified_date", run_id_2)
    guard = WatermarkCommitGuard(pipeline_name, previous_watermark="2026-08-01T00:00:00")
    guard.advance(RunStage.EXTRACTED)
    guard.advance(RunStage.FAILED)

    watermark_to_persist = guard.resolved_watermark("2026-08-15T00:00:00")
    control.complete_run(pipeline_name, run_id_2, "FAILED", 5, 0, watermark_to_persist, "DQ failure")

    stored = control.get(pipeline_name)
    assert stored.last_successful_watermark == "2026-08-01T00:00:00"  # unchanged from run 1
    assert stored.last_run_status == "FAILED"
