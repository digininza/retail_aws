"""AWS Glue job: Silver -> Gold (Section 14, Section 5).

Gold holds business-ready, conformed, often aggregated data — the layer
Redshift loads read from. This job builds the dimensional artifacts that are
simple enough for Glue (dim_store, dim_promotion, dim_date); dim_customer's
SCD2 logic and the sales fact join are intentionally kept in
`src/dimensional_model/` and invoked from here so the *logic* is engine-
agnostic and unit-testable outside the Glue runtime.

Job arguments: --JOB_NAME --environment
"""
from __future__ import annotations

import sys

try:
    from awsglue.utils import getResolvedOptions
except ImportError:  # pragma: no cover
    getResolvedOptions = None  # type: ignore

from src.common.config_loader import ConfigLoader
from src.common.logging_utils import get_logger
from src.common.utilities import LocalS3Adapter
from src.dimensional_model.dimensions import build_dim_date, build_dim_promotion, build_dim_store

logger = get_logger(__name__)


def main() -> None:
    args = getResolvedOptions(sys.argv, ["JOB_NAME", "environment"]) if getResolvedOptions else {
        "JOB_NAME": "silver_to_gold_local", "environment": "dev",
    }
    cfg = ConfigLoader(environment=args["environment"])
    s3 = LocalS3Adapter()

    stores = s3.read_dataframe(cfg.s3_path("silver") + "stores/", fmt="parquet")
    promotions = s3.read_dataframe(cfg.s3_path("silver") + "promotions/", fmt="parquet")

    if not stores.empty:
        dim_store = build_dim_store(stores)
        s3.write_dataframe(dim_store, cfg.s3_path("gold") + "dim_store/", fmt="parquet")
        logger.info("gold_dim_written", extra={"target": "dim_store", "records_written": len(dim_store)})

    if not promotions.empty:
        dim_promotion = build_dim_promotion(promotions)
        s3.write_dataframe(dim_promotion, cfg.s3_path("gold") + "dim_promotion/", fmt="parquet")
        logger.info("gold_dim_written", extra={"target": "dim_promotion", "records_written": len(dim_promotion)})

    dim_date = build_dim_date("2025-01-01", "2027-12-31")
    s3.write_dataframe(dim_date, cfg.s3_path("gold") + "dim_date/", fmt="parquet")
    logger.info("gold_dim_written", extra={"target": "dim_date", "records_written": len(dim_date)})


if __name__ == "__main__":
    main()
