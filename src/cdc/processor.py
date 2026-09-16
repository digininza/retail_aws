"""CDC deduplication and deterministic ordering (Section 8).

For a CDC batch:
  1. identify business key
  2. deduplicate by key + latest change_timestamp
  3. apply deletes
  4. apply updates
  5. apply inserts
  6. write audit information
  7. commit only after validation

This module implements steps 1-2 (dedup) and the D/U/I split (steps 3-5's
partitioning); `src.cdc.merge` performs the actual apply against a target
DataFrame, and callers are responsible for steps 6-7 (audit + commit gating),
consistent with `src.common.idempotency.WatermarkCommitGuard`.
"""
from __future__ import annotations

from dataclasses import dataclass

import pandas as pd


@dataclass
class DedupedCdcBatch:
    inserts: pd.DataFrame
    updates: pd.DataFrame
    deletes: pd.DataFrame
    duplicate_events_dropped: int


def deduplicate_cdc_batch(df: pd.DataFrame, business_key: str,
                           timestamp_column: str = "change_timestamp") -> DedupedCdcBatch:
    """Handles duplicate CDC events and out-of-order events deterministically:
    for each business key, only the row with the latest change_timestamp
    survives. Ties are broken by original row order (last one wins) so the
    result is stable and replay-safe.
    """
    original_count = len(df)
    sorted_df = df.sort_values(timestamp_column, kind="stable")
    deduped = sorted_df.drop_duplicates(subset=[business_key], keep="last")
    duplicates_dropped = original_count - len(deduped)

    inserts = deduped[deduped["operation"] == "I"].copy()
    updates = deduped[deduped["operation"] == "U"].copy()
    deletes = deduped[deduped["operation"] == "D"].copy()

    return DedupedCdcBatch(
        inserts=inserts, updates=updates, deletes=deletes,
        duplicate_events_dropped=duplicates_dropped,
    )
