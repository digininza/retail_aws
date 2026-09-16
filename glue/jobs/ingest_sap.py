"""AWS Glue job: SAP ERP incremental extraction -> S3 raw (Section 14).

Follows the identical control-flow pattern as `ingest_sqlserver.py` (extract
-> land raw -> DQ -> reconciliation -> commit -> watermark advance); the only
difference is the source connector (`src.ingestion.sap.extractor`) and a
higher default retry count (5 vs. 3) reflecting SAP's higher rate of
transient RFC/BAPI connectivity errors observed in Section 2A. See that file
for full commentary on Job Bookmarks vs. the control table.

Job arguments: --JOB_NAME --environment --pipeline_name --connection_name
"""
from __future__ import annotations

import sys

try:
    from awsglue.context import GlueContext
    from awsglue.job import Job
    from awsglue.utils import getResolvedOptions
    from pyspark.context import SparkContext
except ImportError:  # pragma: no cover
    GlueContext = Job = getResolvedOptions = SparkContext = None  # type: ignore

from src.common.audit import AuditLogStore, ControlTableStore, new_run_id, utc_now_iso
from src.common.config_loader import ConfigLoader
from src.common.exceptions import DataQualityError, ReconciliationError
from src.common.idempotency import RunStage, WatermarkCommitGuard
from src.common.logging_utils import get_logger
from src.common.utilities import LocalS3Adapter
from src.data_quality import framework as dq_framework
from src.ingestion.sap.extractor import extract_incremental
from src.reconciliation import framework as recon_framework

logger = get_logger(__name__)


def main() -> None:
    args = getResolvedOptions(sys.argv, ["JOB_NAME", "environment", "pipeline_name", "connection_name"]) if getResolvedOptions else {
        "JOB_NAME": "ingest_sap_local", "environment": "dev",
        "pipeline_name": "sap_product_master_incremental", "connection_name": "sap-rfc-dev",
    }
    if SparkContext is not None:
        job = Job(GlueContext(SparkContext()))
        job.init(args["JOB_NAME"], args)

    cfg = ConfigLoader(environment=args["environment"])
    pipeline = cfg.get_pipeline(args["pipeline_name"])
    control, audit, s3 = ControlTableStore(), AuditLogStore(), LocalS3Adapter()

    run_id, start_time = new_run_id(), utc_now_iso()
    control.start_run(pipeline.pipeline_name, pipeline.source_system, pipeline.source_table,
                       pipeline.load_type, pipeline.watermark_column, run_id)
    existing = control.get(pipeline.pipeline_name)
    previous_watermark = existing.last_successful_watermark if existing else None
    guard = WatermarkCommitGuard(pipeline.pipeline_name, previous_watermark)

    status, error_message, records_read, records_written = "SUCCESS", None, 0, 0
    new_watermark = previous_watermark

    try:
        df = extract_incremental(pipeline.source_table, pipeline.watermark_column, previous_watermark)
        records_read = len(df)
        guard.advance(RunStage.EXTRACTED)
        if not df.empty and pipeline.watermark_column in df.columns:
            new_watermark = str(df[pipeline.watermark_column].max())

        s3.write_dataframe(df, cfg.s3_path("raw") + f"{pipeline.source_table}/", fmt="parquet")
        guard.advance(RunStage.WRITTEN)
        records_written = len(df)

        dq_result = dq_framework.run_data_quality(df, cfg.dq_profile(pipeline.dq_profile), pipeline.dq_profile)
        dq_framework.enforce(dq_result)
        guard.advance(RunStage.DQ_PASSED)

        recon_result = recon_framework.run_reconciliation(
            df, df, cfg.reconciliation_profile(pipeline.reconciliation_profile),
            run_id=run_id, pipeline_name=pipeline.pipeline_name,
        )
        recon_framework.enforce(recon_result)
        guard.advance(RunStage.RECONCILED)
        guard.advance(RunStage.COMMITTED)

    except (DataQualityError, ReconciliationError) as exc:
        status, error_message = "FAILED", str(exc)
        logger.error("glue_job_failed", extra={"pipeline": pipeline.pipeline_name, "run_id": run_id})
    finally:
        watermark_to_persist = guard.resolved_watermark(new_watermark or "")
        control.complete_run(pipeline.pipeline_name, run_id, status, records_read, records_written,
                              watermark_to_persist or None, error_message)
        audit.write(run_id, pipeline.pipeline_name, cfg.environment, start_time, utc_now_iso(), status,
                    records_read, records_written, 0, pipeline.source_table, "s3.raw",
                    previous_watermark, watermark_to_persist, error_message)

    if SparkContext is not None:
        job.commit()
    if status == "FAILED":
        raise SystemExit(1)


if __name__ == "__main__":
    main()
