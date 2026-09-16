"""CDC + SCD2 demo: applies sample_data/customers/customers_cdc.csv to
dim_customer, demonstrating dedup, ordered D/U/I apply, and SCD2 history
(Section 8, Section 10). Run with `make demo-cdc-scd2`.
"""
from __future__ import annotations

from pathlib import Path

import pandas as pd

from src.cdc.merge import merge_upsert
from src.cdc.parser import parse_cdc_batch
from src.cdc.processor import deduplicate_cdc_batch
from src.common.logging_utils import get_logger
from src.dimensional_model.scd_type2 import apply_scd2, empty_dim_customer

logger = get_logger(__name__)

SAMPLE_DATA_ROOT = Path(__file__).resolve().parents[2] / "sample_data"


def run() -> None:
    cdc_batch = pd.read_csv(SAMPLE_DATA_ROOT / "customers" / "customers_cdc.csv")
    parsed = parse_cdc_batch(cdc_batch, business_key="customer_id")

    deduped = deduplicate_cdc_batch(parsed, business_key="customer_id")
    print(f"Parsed {len(cdc_batch)} raw CDC events -> "
          f"{deduped.duplicate_events_dropped} duplicate/superseded events dropped")
    print(f"  inserts={len(deduped.inserts)} updates={len(deduped.updates)} deletes={len(deduped.deletes)}")

    # Demonstrate merge/upsert against a plain "current state" projection first.
    current_state = pd.read_csv(SAMPLE_DATA_ROOT / "customers" / "customers.csv")
    merge_result = merge_upsert(current_state, deduped.inserts, deduped.updates, deduped.deletes, "customer_id")
    print(f"Merge/upsert result: inserted={merge_result.inserted} updated={merge_result.updated} "
          f"deleted={merge_result.deleted} unchanged={merge_result.unchanged}")

    # Now demonstrate SCD2 history against dim_customer, applying inserts+updates
    # (a CDC delete against an SCD2 dimension is typically a soft business-effective
    # end-date rather than a physical delete — see docs/lld/03_scd2_lld.md).
    dim_customer = empty_dim_customer()
    seed_result = apply_scd2(dim_customer, current_state.rename(columns={"name": "customer_name"}))
    dim_customer = seed_result.dim
    print(f"Seeded dim_customer with {len(dim_customer)} current rows")

    changes = pd.concat([deduped.inserts, deduped.updates], ignore_index=True)
    scd2_result = apply_scd2(dim_customer, changes)
    dim_customer = scd2_result.dim

    print(f"SCD2 apply: new={scd2_result.inserted_new} changed={scd2_result.inserted_changed} "
          f"expired={scd2_result.expired} unchanged={scd2_result.unchanged}")
    print(dim_customer.sort_values(["customer_id", "effective_start_date"]).to_string(index=False))


if __name__ == "__main__":
    run()
