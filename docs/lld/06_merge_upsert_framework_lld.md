# LLD 06 — Merge/Upsert Framework

## Objective

Provide one reusable, deterministic merge/upsert implementation used by
every pipeline that needs insert-if-absent/update-if-present/delete-if-marked
semantics — so idempotency guarantees (Section 9) live in one tested place,
not reimplemented per pipeline.

## Assumptions

- A deterministic business key is available and never changes meaning
  (Section 9: "avoid using a timestamp alone as an idempotency key").
- CDC metadata columns (`operation`, `change_timestamp`) accompany
  insert/update rows but are not part of the target schema.

## Input / Output

**Input:** `target` DataFrame, `inserts`, `updates`, `deletes` DataFrames
(already deduplicated — see LLD-02), business key column name.

**Output:** `MergeResult(target, inserted, updated, deleted, unchanged)`.

## Components

`src/cdc/merge.py::merge_upsert`. SQL equivalent:
`sql/facts/merge_fact_sales.sql` (Redshift `MERGE` statement keyed on
`(order_id, order_item_id)`).

## Sequence flow

```
incoming (inserts, updates, deletes)
  -> deduplicate (LLD-02, upstream of this component)
  -> validate (schema check, upstream)
  -> MERGE target:
       deletes: remove matching keys from target
       updates: matched -> replace row (no-op if attributes identical); unmatched -> insert
       inserts: matched (rerun case) -> overwrite; unmatched -> insert
  -> return counts
```

## Pseudocode

```
result = target.copy()
result = result[~result[key].isin(deletes[key])]                 # deletes first

for row in strip_cdc_metadata(updates):
    if row[key] in result[key]:
        if unchanged(result, row): continue                       # no-op, idempotent
        result = replace(result, row)
    else:
        result = append(result, row)                               # defensive insert-on-update

for row in strip_cdc_metadata(inserts):
    result = replace_or_append(result, row)                         # idempotent: rerun overwrites, not duplicates
```

## Why this makes reruns idempotent

The critical property (Section 9): `merge_upsert(target, same_batch)` run
twice produces the same `target` both times. This holds because every
write path is keyed on business key membership, never on "have I already
processed this specific run" — a rerun of the exact same insert batch finds
the key already present and overwrites in place rather than appending a
second row.

## Exception handling & retry

`merge_upsert` itself doesn't raise — it's a pure data transformation. A
downstream commit failure (writing the merged target back to S3/Redshift)
is retryable; retrying is safe specifically *because* the merge logic is
idempotent — a partial write followed by a full retry re-derives the same
correct final state rather than double-applying deltas.

## Logging & audit

`MergeResult` counts (`inserted, updated, deleted, unchanged`) feed the
run's audit row and are a useful signal on their own: an unexpectedly high
`inserted` count on what should be a small incremental batch can indicate
an upstream watermark regression (accidentally re-extracting a much larger
window than intended).

## Security

No special handling — inherits the sensitivity classification of whatever
data is being merged (same as its source).

## Performance

The reference Python implementation iterates row-by-row within
`updates`/`inserts` for correctness/clarity (see `src/cdc/merge.py`); this
is appropriate for CDC batch sizes (thousands of changed rows per run,
Section 2B), not designed for TB-scale merges — those use the Redshift-native
`MERGE` statement (`merge_fact_sales.sql`), which is fully set-based.

## Edge cases

| Case | Handling |
|---|---|
| Insert for a key that already exists (rerun) | Overwritten, not duplicated |
| Update for a key that doesn't exist yet | Treated as insert (defensive) |
| Update with identical attribute values to current state | Counted as `unchanged`, not `updated` — avoids inflating change metrics on a no-op rerun |
| Delete for a key not present in target | No-op (nothing to remove); not an error |
| Empty inserts/updates/deletes | No-op for that category; `merge_upsert` still returns a valid `MergeResult` |

## Test cases

`tests/unit/test_merge.py` — insert, update, delete, rerun idempotency
(no duplicate on repeated insert batch), and unchanged-attribute detection.
