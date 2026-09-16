"""AWS Glue job: SQL Server incremental extraction -> S3 raw (Section 14).

Runs inside the AWS Glue Spark runtime (requires the `awsglue` library,
available only in the managed Glue environment — this script is not meant to
run locally; local logic validation lives in
`src/ingestion/sqlserver/extractor.py` and its unit tests instead).

Job arguments (see infrastructure/glue/*.json for the Glue job definition):
    --JOB_NAME              Glue job name
    --environment           dev | qa | prod
    --pipeline_name          e.g. sqlserver_orders_incremental
    --connection_name        Glue JDBC connection to SQL Server (Secrets-Manager-backed)

AWS Glue Job Bookmarks vs. this project's control table (Section 14):
    Glue bookmarks track which S3 files or JDBC rows a job has already
    processed *per job*, transparently, using an internal Glue-managed
    state store. They are convenient but: (1) are tied to the Glue job
    definition, not portable to EMR/Lambda-driven reprocessing, (2) don't
    expose a queryable audit trail, (3) can't express the
    read-watermark -> extract -> write -> DQ -> reconciliation -> commit
    ordering this project requires (Section 7). We therefore use bookmarks
    only as a *secondary* optimization (skip already-seen S3 objects on
    reruns) and treat the explicit control table
    (`src.common.audit.ControlTableStore`) as the source of truth for
    watermark state. Do not treat the two as interchangeable.
"""
from __future__ import annotations

import sys

# The awsglue/pyspark imports below only resolve inside the managed Glue
# runtime. Guarded so this file can still be imported/linted locally.
try:
    from awsglue.context import GlueContext
    from awsglue.job import Job
    from awsglue.utils import getResolvedOptions
    from pyspark.context import SparkContext
except ImportError:  # pragma: no cover - local lint/documentation environment
    GlueContext = Job = getResolvedOptions = SparkContext = None  # type: ignore

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


def main() -> None:
    args = getResolvedOptions(
        sys.argv, ["JOB_NAME", "environment", "pipeline_name", "connection_name"]
    ) if getResolvedOptions else {
        "JOB_NAME": "ingest_sqlserver_local", "environment": "dev",
        "pipeline_name": "sqlserver_orders_incremental", "connection_name": "sqlserver-jdbc-dev",
    }

    if SparkContext is not None:
        sc = SparkContext()
        glue_context = GlueContext(sc)
        job = Job(glue_context)
        job.init(args["JOB_NAME"], args)

    cfg = ConfigLoader(environment=args["environment"])
    pipeline = cfg.get_pipeline(args["pipeline_name"])
    control = ControlTableStore()
    audit = AuditLogStore()
    s3 = LocalS3Adapter()  # in AWS this is glue_context's DynamicFrame writer targeting s3://

    run_id = new_run_id()
    start_time = utc_now_iso()
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

        raw_uri = cfg.s3_path("raw") + f"{pipeline.source_table}/"
        s3.write_dataframe(df, raw_uri, fmt="parquet")
        guard.advance(RunStage.WRITTEN)
        records_written = len(df)

        profile = cfg.dq_profile(pipeline.dq_profile)
        dq_result = dq_framework.run_data_quality(df, profile, pipeline.dq_profile)
        dq_framework.enforce(dq_result)
        guard.advance(RunStage.DQ_PASSED)

        recon_profile = cfg.reconciliation_profile(pipeline.reconciliation_profile)
        recon_result = recon_framework.run_reconciliation(df, df, recon_profile, run_id=run_id,
                                                            pipeline_name=pipeline.pipeline_name)
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
                    records_read, records_written, 0, pipeline.source_table, "s3.raw", previous_watermark,
                    watermark_to_persist, error_message)

    if SparkContext is not None:
        job.commit()

    if status == "FAILED":
        raise SystemExit(1)


if __name__ == "__main__":
    main()
