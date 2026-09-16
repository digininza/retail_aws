"""Scenario 1 demo (Section 41, DATA_FLOW.md):

    SQL Server orders -> incremental extraction -> S3 raw -> schema validation
    -> Glue Bronze/Silver -> DQ -> reconciliation -> Redshift staging (local
    S3 stand-in) -> MERGE -> audit -> watermark update

Run with: `make demo-incremental` or `python -m src.ingestion.sqlserver.run_demo`
"""
from __future__ import annotations

from src.common.audit import AuditLogStore, ControlTableStore, new_run_id, utc_now_iso
from src.common.config_loader import ConfigLoader
from src.common.exceptions import DataQualityError, ReconciliationError
from src.common.idempotency import RunStage, WatermarkCommitGuard
from src.common.logging_utils import get_logger
from src.common.utilities import LocalS3Adapter
from src.data_quality import framework as dq_framework
from src.ingestion.sqlserver.extractor import extract_incremental
from src.reconciliation import framework as recon_framework

logger = get_logger(__name__)

PIPELINE_NAME = "sqlserver_orders_incremental"


def run() -> None:
    cfg = ConfigLoader()
    pipeline = cfg.get_pipeline(PIPELINE_NAME)
    control = ControlTableStore()
    audit = AuditLogStore()
    s3 = LocalS3Adapter()

    run_id = new_run_id()
    start_time = utc_now_iso()

    control.start_run(
        pipeline_name=PIPELINE_NAME, source_system=pipeline.source_system,
        source_table=pipeline.source_table, load_type=pipeline.load_type,
        watermark_column=pipeline.watermark_column, run_id=run_id,
    )
    existing = control.get(PIPELINE_NAME)
    previous_watermark = existing.last_successful_watermark if existing else None
    guard = WatermarkCommitGuard(pipeline_name=PIPELINE_NAME, previous_watermark=previous_watermark)

    logger.info("pipeline_start", extra={"pipeline": PIPELINE_NAME, "run_id": run_id,
                                          "task": "extract", "source": pipeline.source_table})

    status = "SUCCESS"
    error_message = None
    records_read = records_written = 0
    new_watermark = previous_watermark

    try:
        # 1. Extract (watermark read -> extract changes)
        df = extract_incremental(pipeline.source_table, pipeline.watermark_column, previous_watermark)
        records_read = len(df)
        guard.advance(RunStage.EXTRACTED)

        if not df.empty and pipeline.watermark_column in df.columns:
            new_watermark = str(df[pipeline.watermark_column].max())

        # 2. Land to S3 raw (immutable)
        raw_uri = cfg.s3_path("raw") + "orders/"
        s3.write_dataframe(df, raw_uri, fmt="parquet")

        # 3. Bronze->Silver (standard cleansing stands in for glue/jobs/bronze_to_silver.py)
        silver_df = df.dropna(subset=["order_id"]).drop_duplicates(subset=["order_id"], keep="last")
        silver_uri = cfg.s3_path("silver") + "orders/"
        s3.write_dataframe(silver_df, silver_uri, fmt="parquet")
        guard.advance(RunStage.WRITTEN)
        records_written = len(silver_df)

        # 4. Data quality (CRITICAL failures block promotion)
        profile = cfg.dq_profile(pipeline.dq_profile)
        dq_result = dq_framework.run_data_quality(silver_df, profile, pipeline.dq_profile)
        dq_framework.enforce(dq_result)
        guard.advance(RunStage.DQ_PASSED)

        # 5. Reconciliation (source vs. target counts/sums)
        recon_profile = cfg.reconciliation_profile(pipeline.reconciliation_profile)
        recon_result = recon_framework.run_reconciliation(
            df, silver_df, recon_profile, run_id=run_id, pipeline_name=PIPELINE_NAME,
        )
        recon_framework.enforce(recon_result)
        guard.advance(RunStage.RECONCILED)

        # 6. Redshift staging commit (local S3 stand-in for `sql/staging/stg_orders.sql` load)
        staging_uri = cfg.s3_path("gold") + "redshift_staging/stg_orders/"
        s3.write_dataframe(silver_df, staging_uri, fmt="parquet")
        guard.advance(RunStage.COMMITTED)

        logger.info("pipeline_success", extra={"pipeline": PIPELINE_NAME, "run_id": run_id,
                                                 "records_read": records_read, "records_written": records_written})

    except (DataQualityError, ReconciliationError) as exc:
        status = "FAILED"
        error_message = str(exc)
        logger.error("pipeline_failed", extra={"pipeline": PIPELINE_NAME, "run_id": run_id,
                                                 "error_category": type(exc).__name__, "status": "FAILED"})
    finally:
        # CRITICAL RULE (Section 7): watermark only advances if guard reached COMMITTED.
        watermark_to_persist = guard.resolved_watermark(new_watermark or "")
        control.complete_run(
            pipeline_name=PIPELINE_NAME, run_id=run_id, status=status,
            records_read=records_read, records_written=records_written,
            new_watermark=watermark_to_persist or None, error_message=error_message,
        )
        audit.write(
            run_id=run_id, pipeline_name=PIPELINE_NAME, environment=cfg.environment,
            start_time=start_time, end_time=utc_now_iso(), status=status,
            records_read=records_read, records_written=records_written, records_rejected=0,
            source=pipeline.source_table, target="redshift.stg_orders",
            watermark_before=previous_watermark, watermark_after=watermark_to_persist,
            error_message=error_message,
        )
        print(f"Run {run_id} finished with status={status}. "
              f"records_read={records_read} records_written={records_written} "
              f"watermark: {previous_watermark} -> {watermark_to_persist}")


if __name__ == "__main__":
    run()
