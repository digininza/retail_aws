"""Supplier file ingestion: catalog, price list, inventory CSV/JSON drops
(Section 2F). Files are treated as FULL loads keyed by `file_date` — supplier
partners do not provide change feeds, so each drop supersedes the prior one
for that supplier/date partition (see config/base/sources.yaml:
supplier_catalog_files).
"""
from __future__ import annotations

from pathlib import Path

import pandas as pd

from src.common.exceptions import SchemaValidationError
from src.common.logging_utils import get_logger

logger = get_logger(__name__)

SAMPLE_DATA_ROOT = Path(__file__).resolve().parents[3] / "sample_data"

REQUIRED_COLUMNS = {"supplier_id", "supplier_sku", "list_price", "file_date"}


def load_supplier_catalog(file_name: str = "supplier_catalog.csv") -> pd.DataFrame:
    path = SAMPLE_DATA_ROOT / "suppliers" / file_name
    df = pd.read_csv(path)
    missing = REQUIRED_COLUMNS - set(df.columns)
    if missing:
        raise SchemaValidationError(f"Supplier catalog file missing required columns: {missing}")
    logger.info("supplier_file_loaded", extra={"source": file_name, "records_read": len(df)})
    return df
