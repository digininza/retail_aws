"""AWS Glue Python Shell job: REST API extraction (promotions/marketplace
feeds) -> S3 raw (Section 14, Section 2C).

Unlike the JDBC-sourced jobs, this typically runs as a Glue **Python Shell**
job (not Spark) since API pagination is a lightweight, single-threaded
concern — spinning up a full Spark cluster for a paginated REST pull would
be wasteful (see ADR-002 for the general "right tool for the workload"
principle applied to Glue vs. EMR; the same reasoning applies to Spark vs.
Python Shell *within* Glue).

Job arguments: --JOB_NAME --environment --pipeline_name
"""
from __future__ import annotations

import sys

try:
    from awsglue.utils import getResolvedOptions
except ImportError:  # pragma: no cover
    getResolvedOptions = None  # type: ignore

import pandas as pd

from src.common.audit import AuditLogStore, ControlTableStore, new_run_id, utc_now_iso
from src.common.config_loader import ConfigLoader
from src.common.exceptions import ApiRateLimitError, DataQualityError, NonRetryableError, ReconciliationError
from src.common.idempotency import RunStage, WatermarkCommitGuard
from src.common.logging_utils import get_logger
from src.common.utilities import LocalS3Adapter
from src.data_quality import framework as dq_framework
from src.ingestion.rest_api.client import ApiPage, MockApiTransport, RestApiClient
from src.reconciliation import framework as recon_framework

logger = get_logger(__name__)


def main() -> None:
    args = getResolvedOptions(sys.argv, ["JOB_NAME", "environment", "pipeline_name"]) if getResolvedOptions else {
        "JOB_NAME": "ingest_api_local", "environment": "dev", "pipeline_name": "api_promotions_feed",
    }

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
        # Demo transport: 2 pages, first call simulates a rate limit to exercise retry/backoff.
        transport = MockApiTransport(
            pages=[
                ApiPage(records=[{"promo_code": "PROMO-10", "discount_pct": 10, "updated_at": "2026-08-01T00:00:00"}], next_cursor="1"),
                ApiPage(records=[{"promo_code": "PROMO-11", "discount_pct": 15, "updated_at": "2026-08-03T00:00:00"}], next_cursor=None),
            ],
            fail_first_n_calls=1,
        )
        client = RestApiClient(transport, rate_limit_per_minute=pipeline.raw.get("rate_limit_per_minute", 60))
        records = list(client.fetch_all(sleep_fn=lambda s: None))
        df = pd.DataFrame(records)
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

    except (DataQualityError, ReconciliationError, ApiRateLimitError, NonRetryableError) as exc:
        status, error_message = "FAILED", str(exc)
        logger.error("glue_job_failed", extra={"pipeline": pipeline.pipeline_name, "run_id": run_id})
    finally:
        watermark_to_persist = guard.resolved_watermark(new_watermark or "")
        control.complete_run(pipeline.pipeline_name, run_id, status, records_read, records_written,
                              watermark_to_persist or None, error_message)
        audit.write(run_id, pipeline.pipeline_name, cfg.environment, start_time, utc_now_iso(), status,
                    records_read, records_written, 0, pipeline.source_table, "s3.raw",
                    previous_watermark, watermark_to_persist, error_message)

    if status == "FAILED":
        raise SystemExit(1)


if __name__ == "__main__":
    main()
