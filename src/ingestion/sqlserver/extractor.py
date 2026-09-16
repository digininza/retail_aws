"""SQL Server watermark-based incremental extraction (Section 7, LLD-1).

In a real deployment this issues `SELECT ... WHERE {watermark_column} > ?`
against SQL Server via a Glue JDBC connection. Locally, `MockSqlServerSource`
reads from `sample_data/` CSVs and applies the same watermark filter, so the
extraction *logic* — not just the plumbing — is exercised by tests and demos.
"""
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Optional

import pandas as pd

from src.common.exceptions import SourceUnavailableError
from src.common.logging_utils import get_logger
from src.common.retry import RetryPolicy, with_retry

logger = get_logger(__name__)

SAMPLE_DATA_ROOT = Path(__file__).resolve().parents[3] / "sample_data"


class MockSqlServerSource:
    """Local stand-in for a SQL Server connection resolved via Secrets Manager."""

    def __init__(self, table: str):
        self.table = table

    @with_retry(RetryPolicy(max_attempts=3, initial_delay_seconds=1, max_delay_seconds=5))
    def read_full(self) -> pd.DataFrame:
        path = SAMPLE_DATA_ROOT / _table_dir(self.table) / f"{self.table}.csv"
        if not path.exists():
            raise SourceUnavailableError(f"Sample source file not found: {path}")
        return pd.read_csv(path)

    def read_incremental(self, watermark_column: str, since: Optional[str]) -> pd.DataFrame:
        df = self.read_full()
        if since is None or watermark_column not in df.columns:
            return df
        df[watermark_column] = pd.to_datetime(df[watermark_column])
        cutoff = pd.to_datetime(since)
        return df[df[watermark_column] > cutoff].copy()


def _table_dir(table: str) -> str:
    """Sample data is organized by business domain, not literal table name."""
    mapping = {
        "orders": "orders",
        "order_items": "orders",
        "customers": "customers",
        "customers_cdc": "customers",
        "stores": "products",
        "promotions": "products",
    }
    return mapping.get(table, table)


def extract_incremental(table: str, watermark_column: str, since: Optional[str]) -> pd.DataFrame:
    """Top-level extraction entry point used by glue/jobs/ingest_sqlserver.py."""
    logger.info("extract_start", extra={"source": table, "task": "extract"})
    source = MockSqlServerSource(table)
    df = source.read_incremental(watermark_column, since)
    logger.info("extract_complete", extra={"source": table, "records_read": len(df)})
    return df
