"""SCD Type 2 implementation for dim_customer (Section 10).

Columns: customer_sk, customer_id, customer_name, email, segment, city,
effective_start_date, effective_end_date, is_current, record_hash.

Logic:
    NEW CUSTOMER            -> insert current record
    EXISTING + NO CHANGE    -> do nothing
    EXISTING + CHANGE       -> expire current record, insert new current record

Why a surrogate key: `customer_sk` uniquely identifies one *version* of a
customer row, so fact tables can join to "the customer as they were on
order date" rather than only ever the latest version — this is the entire
point of SCD2. `customer_id` (business key) stays constant across versions.

Concurrency: this reference implementation assumes single-writer batch runs
per pipeline (enforced by the control table's one-row-per-pipeline model in
src.common.audit). A concurrent-run guard is provided by
WatermarkCommitGuard; true multi-writer concurrency would additionally need
a row-level lock or optimistic-concurrency version column on dim_customer.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import date, timedelta
from typing import Optional

import pandas as pd

from src.common.idempotency import record_hash

TRACKED_ATTRIBUTES = ["customer_name", "email", "segment", "city"]
BUSINESS_KEY = "customer_id"

DIM_CUSTOMER_COLUMNS = [
    "customer_sk", "customer_id", "customer_name", "email", "segment", "city",
    "effective_start_date", "effective_end_date", "is_current", "record_hash",
]


def empty_dim_customer() -> pd.DataFrame:
    return pd.DataFrame(columns=DIM_CUSTOMER_COLUMNS)


def _next_surrogate_key(dim: pd.DataFrame) -> int:
    if dim.empty:
        return 1
    return int(dim["customer_sk"].max()) + 1


@dataclass
class Scd2ApplyResult:
    dim: pd.DataFrame
    inserted_new: int
    inserted_changed: int
    expired: int
    unchanged: int


def apply_scd2(dim_customer: pd.DataFrame, incoming: pd.DataFrame, as_of_date: Optional[date] = None) -> Scd2ApplyResult:
    """Applies incoming (deduplicated, one row per customer_id) records to
    dim_customer. `incoming` should already be the output of CDC dedup
    (src.cdc.processor.deduplicate_cdc_batch) so late-arriving/duplicate
    source events never reach this function directly — see ADR-006.
    """
    as_of_date = as_of_date or date.today()
    dim = dim_customer.copy()
    if dim.empty:
        dim = empty_dim_customer()

    new_rows = []
    inserted_new = inserted_changed = expired = unchanged = 0

    for _, row in incoming.iterrows():
        customer_id = row[BUSINESS_KEY]
        incoming_hash = record_hash(row.to_dict(), TRACKED_ATTRIBUTES)

        current_mask = (dim["customer_id"] == customer_id) & (dim["is_current"] == True)  # noqa: E712
        current_rows = dim[current_mask]

        if current_rows.empty:
            # NEW CUSTOMER -> insert current record
            new_rows.append({
                "customer_sk": _next_surrogate_key(dim) + len(new_rows),
                "customer_id": customer_id,
                "customer_name": row.get("customer_name") or row.get("name"),
                "email": row.get("email"),
                "segment": row.get("segment"),
                "city": row.get("city"),
                "effective_start_date": as_of_date.isoformat(),
                "effective_end_date": None,
                "is_current": True,
                "record_hash": incoming_hash,
            })
            inserted_new += 1
            continue

        current_hash = current_rows.iloc[0]["record_hash"]
        if current_hash == incoming_hash:
            # EXISTING + NO CHANGE -> do nothing (idempotent rerun-safe)
            unchanged += 1
            continue

        # EXISTING + CHANGE -> expire current, insert new current
        dim.loc[current_mask, "is_current"] = False
        dim.loc[current_mask, "effective_end_date"] = (as_of_date - timedelta(days=1)).isoformat()
        expired += 1

        new_rows.append({
            "customer_sk": _next_surrogate_key(dim) + len(new_rows),
            "customer_id": customer_id,
            "customer_name": row.get("customer_name") or row.get("name"),
            "email": row.get("email"),
            "segment": row.get("segment"),
            "city": row.get("city"),
            "effective_start_date": as_of_date.isoformat(),
            "effective_end_date": None,
            "is_current": True,
            "record_hash": incoming_hash,
        })
        inserted_changed += 1

    if new_rows:
        dim = pd.concat([dim, pd.DataFrame(new_rows)], ignore_index=True)

    return Scd2ApplyResult(
        dim=dim, inserted_new=inserted_new, inserted_changed=inserted_changed,
        expired=expired, unchanged=unchanged,
    )
