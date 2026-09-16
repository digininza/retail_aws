# LLD 04 — Reconciliation Framework

## Objective

Independently verify that the volume/value of data that *should* have moved
between two stages actually did — catching silent data loss or duplication
that per-record DQ validation would not notice (Section 12).

## Assumptions

- A reconciliation profile is defined per pipeline in
  `config/base/reconciliation.yaml`, referencing a source and target
  identifier (e.g. `sqlserver.orders` vs. `redshift.stg_orders`).
- Tolerances are non-zero only where legitimate skew is expected (e.g.
  inventory snapshot timing); financial/keyed data uses `tolerance_pct: 0`.

## Input / Output

**Input:** Source DataFrame, target DataFrame, a reconciliation profile
(list of checks).

**Output:** `ReconciliationRunResult` (status PASSED/FAILED,
counts/totals/variance per check) written to
`audit.reconciliation_log`.

## Components

| Check type | File |
|---|---|
| `RECORD_COUNT` | `src/reconciliation/record_count.py` |
| `AGGREGATE` | `src/reconciliation/aggregate_check.py` |
| `CONTROL_TOTAL`, `KEY_RECONCILIATION` | `src/reconciliation/control_totals.py` |
| Orchestration | `src/reconciliation/framework.py` |

## Table/schema: audit.reconciliation_log

See `sql/audit_tables/ddl_audit_tables.sql`:
`run_id, pipeline_name, check_type, source_count, target_count,
source_total, target_total, variance, variance_pct, status,
validation_timestamp`.

## Sequence flow

```
for each check in profile.checks:
    dispatch to record_count / aggregate_check / control_totals based on check.type
    collect CheckResult (passed: bool)
all_passed = AND of every check's passed
write ReconciliationRunResult(status = PASSED if all_passed else FAILED)
if not all_passed: raise ReconciliationError  # caller decides whether this blocks promotion
```

## Pseudocode (see `src/reconciliation/framework.py::run_reconciliation`)

```
results = []
for check in profile["checks"]:
    if check.type == RECORD_COUNT: results.append(check_record_count(...))
    elif check.type == AGGREGATE: results.append(check_aggregate(...))
    elif check.type == CONTROL_TOTAL: results.append(check_control_total(...))
    elif check.type == KEY_RECONCILIATION: results.append(check_key_reconciliation(...))
all_passed = all(r.passed for r in results)
return ReconciliationRunResult(status="PASSED" if all_passed else "FAILED", check_details=results)
```

## Exception handling & retry

Reconciliation failures are `NonRetryableError` (`ReconciliationError`) —
retrying a reconciliation check against the same static source/target
snapshot would produce the identical result; the correct response is to
alert and investigate, not retry. If the underlying *cause* is a transient
issue (e.g. the extract hadn't finished writing when reconciliation ran),
that is a pipeline sequencing bug to fix, not something reconciliation
itself should paper over by retrying.

## Logging & audit

Every check's result is logged; `ReconciliationRunResult` is written to
`audit.reconciliation_log` regardless of pass/fail (Section 36 — append-only
history, not just failure logging).

## Security

No special handling beyond standard S3/Redshift access controls;
reconciliation reads data, never writes to source or target systems.

## Performance

Checks are computed via `pandas`/Spark aggregations (`sum`, `count`,
`nunique`) over already-in-memory or already-loaded DataFrames — no
additional source-system round trip. For very large tables, `KEY_RECONCILIATION`
(materializing full key sets for set difference) is the most expensive
check; it's used selectively (customer dimension only in the reference
config), not on every pipeline.

## Edge cases

| Case | Handling |
|---|---|
| Source count = 0, target count = 0 | Passes (0% variance, degenerate case handled explicitly in `check_record_count`) |
| Source count = 0, target count > 0 | Fails (100% variance — data appeared from nowhere, always suspicious) |
| Floating-point rounding in SUM comparison | `tolerance_pct` absorbs expected rounding; exact-tolerance profiles (financial data) will correctly fail on real rounding bugs |
| Reconciliation profile references a check type not implemented | Silently skipped in the loop (documented as a gap — a stricter implementation would raise `ConfigurationError` for an unknown check type) |

## Test cases

`tests/reconciliation/test_reconciliation_framework.py` — 7 cases covering
each check type plus full-run pass/fail with `enforce()` raising
`ReconciliationError`.
