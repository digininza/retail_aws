"""SAP ERP extraction (product master, vendor master, purchase orders,
inventory, store master). SAP characteristically exposes changes via
`last_changed_on` style fields on IDoc/BAPI extracts; the extraction pattern
mirrors SQL Server's watermark approach but is kept in its own module because
SAP field-naming and connection conventions (RFC/BAPI vs JDBC) are distinct
in a real integration (Section 2A).
"""
from __future__ import annotations

from pathlib import Path
from typing import Optional

import pandas as pd

from src.common.exceptions import SourceUnavailableError
from src.common.logging_utils import get_logger
from src.common.retry import RetryPolicy, with_retry

logger = get_logger(__name__)

SAMPLE_DATA_ROOT = Path(__file__).resolve().parents[3] / "sample_data"

_TABLE_TO_DIR = {
    "product_master": "products",
    "vendor_master": "suppliers",
    "purchase_orders": "suppliers",
    "inventory": "inventory",
    "store_master": "products",
}


class MockSapSource:
    """Local stand-in for an SAP RFC/BAPI or IDoc-based extract."""

    def __init__(self, table: str):
        self.table = table

    @with_retry(RetryPolicy(max_attempts=5, initial_delay_seconds=2, max_delay_seconds=10))
    def read_full(self) -> pd.DataFrame:
        directory = _TABLE_TO_DIR.get(self.table, self.table)
        path = SAMPLE_DATA_ROOT / directory / f"{self.table}.csv"
        if not path.exists():
            raise SourceUnavailableError(f"SAP sample extract not found: {path}")
        return pd.read_csv(path)

    def read_incremental(self, watermark_column: str, since: Optional[str]) -> pd.DataFrame:
        df = self.read_full()
        if since is None or watermark_column not in df.columns:
            return df
        df[watermark_column] = pd.to_datetime(df[watermark_column])
        return df[df[watermark_column] > pd.to_datetime(since)].copy()


def extract_incremental(table: str, watermark_column: str, since: Optional[str]) -> pd.DataFrame:
    logger.info("sap_extract_start", extra={"source": table, "task": "extract"})
    df = MockSapSource(table).read_incremental(watermark_column, since)
    logger.info("sap_extract_complete", extra={"source": table, "records_read": len(df)})
    return df
