"""Integration test: S3 bronze -> silver processing, including quarantine of
DQ failures, using local adapters (Section 29)."""
from __future__ import annotations

import pandas as pd

from src.common.config_loader import ConfigLoader
from src.common.utilities import LocalS3Adapter
from src.data_quality import framework as dq_framework
from src.data_quality.quarantine import quarantine_records


def test_bronze_to_silver_dedup_and_quarantine_flow():
    cfg = ConfigLoader(environment="dev")
    s3 = LocalS3Adapter()

    bronze_df = pd.DataFrame([
        {"order_id": "O1", "customer_id": "C1", "order_date": "2026-08-01", "amount": 100, "status": "PLACED"},
        {"order_id": "O1", "customer_id": "C1", "order_date": "2026-08-01", "amount": 100, "status": "PLACED"},  # dup
        {"order_id": "O2", "customer_id": None, "order_date": "2026-08-02", "amount": 50, "status": "PLACED"},  # bad
    ])
    s3.write_dataframe(bronze_df, cfg.s3_path("bronze") + "orders/", fmt="parquet")

    read_back = s3.read_dataframe(cfg.s3_path("bronze") + "orders/", fmt="parquet")
    deduped = read_back.drop_duplicates(subset=["order_id"], keep="last")
    assert len(deduped) == 2  # O1 dup collapsed, O2 kept (bad customer_id caught by DQ next)

    profile = cfg.dq_profile("orders_standard")
    result = dq_framework.run_data_quality(deduped, profile, "orders_standard")
    assert result.blocks_promotion  # O2's null customer_id is CRITICAL

    bad_rows = deduped[deduped["customer_id"].isna()]
    quarantine_records(
        bad_rows, run_id="test-run-1", pipeline_name="sqlserver_orders_incremental",
        rule_name="NOT_NULL:customer_id", error_reason="null customer_id",
        source_file="bronze/orders/", s3_adapter=s3, quarantine_zone_uri=cfg.s3_path("quarantine"),
    )
    quarantine_uri = cfg.s3_path("quarantine") + "sqlserver_orders_incremental/test-run-1/"
    assert s3.exists(quarantine_uri)

    silver_df = deduped.dropna(subset=["customer_id"])
    assert len(silver_df) == 1
