# LLD 02 — CDC Processing

## Objective

Deterministically apply a batch of insert/update/delete (I/U/D) change
events to a target, handling duplicate and out-of-order events, without
losing or double-applying any change.

## Assumptions

- Each CDC event carries the business key, the changed attribute values,
  an `operation` code (`I`/`U`/`D`), and a `change_timestamp`.
- `change_timestamp` reflects source-side commit order closely enough that
  "latest timestamp wins" is a correct conflict-resolution rule; true
  source-side LSN/sequence-number ordering is a stronger guarantee not
  modeled here.
- A delete event's payload may carry only the business key (attribute
  values are not required to process a delete).

## Input / Output

**Input:** A raw CDC batch (DataFrame with `operation`, `change_timestamp`,
business key, attributes) — see `sample_data/customers/customers_cdc.csv`.

**Output:** Three partitioned DataFrames — `inserts`, `updates`, `deletes`
— each deduplicated to one row per business key, ready for
`src.cdc.merge.merge_upsert`.

## Components

| Component | File |
|---|---|
| Parse + validate | `src/cdc/parser.py` |
| Dedup + split | `src/cdc/processor.py` |
| Merge into target | `src/cdc/merge.py` |

## Sequence flow

```
raw CDC batch
  -> parse_cdc_batch(): validate operation codes, required columns
  -> deduplicate_cdc_batch(): sort by change_timestamp, keep latest per key
  -> split into inserts / updates / deletes
  -> merge_upsert(target, inserts, updates, deletes): delete -> update -> insert
  -> write audit row
  -> commit only after validation
```

## Table/schema: CDC batch

```
customer_id, name, email, segment, city, operation, change_timestamp
```

## Pseudocode

```
parsed = parse_cdc_batch(raw, business_key)          # raises SchemaValidationError on bad op codes
deduped = deduplicate_cdc_batch(parsed, business_key) # latest timestamp wins per key
result = merge_upsert(target, deduped.inserts, deduped.updates, deduped.deletes, business_key)
# result.inserted / updated / deleted / unchanged counts feed the audit row
```

## Actual code

`src/cdc/processor.py::deduplicate_cdc_batch` — sorts with a **stable**
sort on `change_timestamp`, then `drop_duplicates(keep="last")`. Stability
matters: for two events with an identical timestamp (a real possibility
from some CDC sources), the *last one in the original batch* wins,
deterministically, rather than an unspecified tiebreak.

## Exception handling & retry

- Invalid `operation` codes or missing required columns raise
  `SchemaValidationError` (`NonRetryableError`) — the whole batch is
  rejected, not partially processed, because a malformed batch usually
  indicates an upstream extraction bug that retrying won't fix.
- Downstream write failures (target unavailable) are retryable.

## Logging & audit

Dedup summary (`duplicate_events_dropped`, per-operation counts) is logged
and pushed to Airflow XCom for UI visibility (`customer_pipeline.py`).

## Security

CDC batches may carry PII (name, email) — same S3/KMS/IAM controls as any
other customer data; no special handling beyond standard encryption at
rest/in transit.

## Performance

Dedup is a single sort + `drop_duplicates` over the batch — O(n log n),
negligible at batch sizes this pattern targets (thousands to low millions
of change events per run, not continuous high-throughput CDC streaming,
which would use a different architecture such as DMS + Kinesis).

## Edge cases

| Case | Handling |
|---|---|
| Duplicate identical event repeated 3x | Collapses to 1 (Section 8 requirement) |
| Out-of-order events (earlier timestamp arrives after later one) | Correctly resolved by sorting before dedup, not by arrival order |
| Delete followed by insert for the same key in one batch | Both survive dedup (different `operation` values aren't deduped against each other) — `merge_upsert` applies delete first, then insert, so the net effect is "insert" |
| Update for a key never seen before | Treated as insert-on-update (defensive; documented in `src.cdc.merge`) |
| Null business key | Rejected by `parse_cdc_batch` schema validation |

## Test cases

`tests/unit/test_cdc_dedup.py` — 6 cases including exact duplicates,
out-of-order events, invalid operation codes, and missing required columns.
Negative fixture: `sample_data/customers/customers_cdc_invalid_sample.csv`.
