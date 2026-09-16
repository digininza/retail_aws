"""Integration test: SQL Server source -> S3 raw, using local adapters in
place of live AWS/SQL Server (Section 29)."""
from __future__ import annotations

import pandas as pd

from src.common.config_loader import ConfigLoader
from src.common.utilities import LocalS3Adapter
from src.ingestion.sqlserver.extractor import extract_incremental


def test_full_extract_lands_in_local_s3_raw():
    cfg = ConfigLoader(environment="dev")
    pipeline = cfg.get_pipeline("sqlserver_orders_incremental")

    df = extract_incremental(pipeline.source_table, pipeline.watermark_column, since=None)
    assert len(df) > 0

    s3 = LocalS3Adapter()
    raw_uri = cfg.s3_path("raw") + "orders/"
    s3.write_dataframe(df, raw_uri, fmt="parquet")

    assert s3.exists(raw_uri)
    round_tripped = s3.read_dataframe(raw_uri, fmt="parquet")
    assert len(round_tripped) == len(df)


def test_incremental_extract_only_returns_rows_after_watermark():
    df_full = extract_incremental("orders", "modified_date", since=None)
    midpoint = sorted(pd.to_datetime(df_full["modified_date"]))[len(df_full) // 2]

    df_incremental = extract_incremental("orders", "modified_date", since=str(midpoint))
    assert len(df_incremental) < len(df_full)
    assert all(ts > midpoint for ts in pd.to_datetime(df_incremental["modified_date"]))
