"""CDC batch parsing and validation (Section 8).

Expects columns: business key(s), attribute columns, `operation` (I/U/D),
`change_timestamp`. Validates the operation code and required fields before
handing the batch to `src.cdc.processor`.
"""
from __future__ import annotations

import pandas as pd

from src.common.exceptions import SchemaValidationError

VALID_OPERATIONS = {"I", "U", "D"}


def parse_cdc_batch(df: pd.DataFrame, business_key: str, timestamp_column: str = "change_timestamp") -> pd.DataFrame:
    if "operation" not in df.columns:
        raise SchemaValidationError("CDC batch missing required 'operation' column")
    if timestamp_column not in df.columns:
        raise SchemaValidationError(f"CDC batch missing required '{timestamp_column}' column")
    if business_key not in df.columns:
        raise SchemaValidationError(f"CDC batch missing business key column '{business_key}'")

    invalid_ops = set(df["operation"].unique()) - VALID_OPERATIONS
    if invalid_ops:
        raise SchemaValidationError(f"CDC batch contains invalid operation codes: {invalid_ops}")

    parsed = df.copy()
    parsed[timestamp_column] = pd.to_datetime(parsed[timestamp_column])
    return parsed
