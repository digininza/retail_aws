"""Reusable merge/upsert logic (Section 9).

    incoming -> deduplicate -> validate -> MERGE target
                                              |
                                    matched / unmatched
                                       |          |
                                    update      insert

Rerun guarantee: `merge_upsert(target, same_incoming)` twice in a row yields
the same `target` both times (Section 9's idempotency requirement) because
matching is keyed purely on the business key plus attribute values, never on
"was this the same physical run".
"""
from __future__ import annotations

from dataclasses import dataclass

import pandas as pd

_CDC_METADATA_COLUMNS = ("operation", "change_timestamp")


def _strip_cdc_metadata(df: pd.DataFrame) -> pd.DataFrame:
    """CDC batches carry `operation`/`change_timestamp` alongside business
    attributes; the merge target only ever stores business attributes, so
    these are dropped before a row is written into `target`."""
    return df.drop(columns=[c for c in _CDC_METADATA_COLUMNS if c in df.columns])


@dataclass
class MergeResult:
    target: pd.DataFrame
    inserted: int
    updated: int
    deleted: int
    unchanged: int


def merge_upsert(
    target: pd.DataFrame,
    inserts: pd.DataFrame,
    updates: pd.DataFrame,
    deletes: pd.DataFrame,
    business_key: str,
) -> MergeResult:
    result = target.copy()
    unchanged = 0

    # Deletes first: CDC delete removes the row outright (Section 8 step 3).
    if not deletes.empty:
        delete_keys = set(deletes[business_key])
        before = len(result)
        result = result[~result[business_key].isin(delete_keys)]
        deleted_count = before - len(result)
    else:
        deleted_count = 0

    # Updates: matched rows are replaced; a row not present in target is
    # treated as insert-on-update (defensive — source should not send an
    # update for a key we've never seen, but rerun-safety requires handling it).
    updated_count = 0
    if not updates.empty:
        clean_updates = _strip_cdc_metadata(updates)
        compare_cols = [c for c in clean_updates.columns if c in result.columns]
        existing_keys = set(result[business_key]) if not result.empty else set()
        for _, row in clean_updates.iterrows():
            key = row[business_key]
            if key in existing_keys:
                mask = result[business_key] == key
                if compare_cols and result.loc[mask, compare_cols].iloc[0].equals(row[compare_cols]):
                    unchanged += 1
                    continue
                result = result[~mask]
            result = pd.concat([result, row.to_frame().T], ignore_index=True)
            updated_count += 1

    # Inserts: insert-if-not-found; if the key already exists (rerun of the
    # same batch), this is a no-op overwrite rather than a duplicate row —
    # that is what makes reruns idempotent.
    inserted_count = 0
    if not inserts.empty:
        clean_inserts = _strip_cdc_metadata(inserts)
        existing_keys = set(result[business_key]) if not result.empty else set()
        for _, row in clean_inserts.iterrows():
            key = row[business_key]
            if key in existing_keys:
                mask = result[business_key] == key
                result = result[~mask]
            result = pd.concat([result, row.to_frame().T], ignore_index=True)
            inserted_count += 1

    result = result.reset_index(drop=True)
    return MergeResult(
        target=result, inserted=inserted_count, updated=updated_count,
        deleted=deleted_count, unchanged=unchanged,
    )
