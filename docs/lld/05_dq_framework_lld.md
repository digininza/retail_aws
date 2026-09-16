# LLD 05 — Data Quality Framework

## Objective

Evaluate a configurable set of per-column and per-table rules against a
batch, classify failures by severity, and gate promotion accordingly
(Section 11).

## Assumptions

- Rules are declared in YAML (`config/base/dq_rules.yaml`), not hardcoded
  per pipeline — onboarding a new table's DQ rules is a config change.
- Severity determines *effect*, not just *logging verbosity*: CRITICAL has
  a materially different pipeline outcome than WARNING, not just a
  different log level.

## Input / Output

**Input:** A DataFrame + a named DQ profile (rules keyed by column, plus an
optional `__table__` key for table-level rules like `RECORD_COUNT`/`FRESHNESS`).

**Output:** `DQRunResult` (list of `RuleResult`, `blocks_promotion` flag).

## Components

| Component | File |
|---|---|
| Rule/severity model | `src/data_quality/rules.py` |
| Per-rule-type evaluators | `src/data_quality/validators.py` |
| Orchestration + enforcement | `src/data_quality/framework.py` |
| Quarantine writer | `src/data_quality/quarantine.py` |

## Sequence flow

```
profile = config.dq_profile(profile_name)
rules = build_rules(profile)               # one Rule per (column, rule_config) pair
for rule in rules:
    result = evaluate_rule(df, rule)       # dispatches by RuleType to a handler function
    log if not result.passed
return DQRunResult(rule_results)
enforce(result)  # raises DataQualityError if any CRITICAL rule failed
```

## Pseudocode: rule dispatch

```
HANDLERS = {NOT_NULL: _not_null, UNIQUE: _unique, RANGE: _range, REGEX: _regex,
            REFERENTIAL_INTEGRITY: _referential_integrity, RECORD_COUNT: _record_count,
            FRESHNESS: _freshness, DATA_TYPE: _data_type, ...}

def evaluate_rule(df, rule):
    handler = HANDLERS[rule.rule_type]
    return handler(df, rule)
```

Each handler returns a `RuleResult(column, rule_type, severity, passed,
failed_record_count, detail)` — uniform shape regardless of rule type, so
`DQRunResult.critical_failures` / `.error_failures` can filter across rule
types without special-casing.

## Actual code

See `src/data_quality/validators.py` for each handler
(`_not_null, _unique, _range, _regex, _referential_integrity, _record_count,
_freshness, _data_type, _business_rule, _schema_match`).

## Exception handling & retry

- `enforce()` raises `DataQualityError` (`NonRetryableError`) on any
  CRITICAL failure — never retried, since re-evaluating the same rule
  against the same data yields the same result. The *fix* is either
  correcting upstream data or updating the rule, followed by a fresh run.
- A rule referencing a column not present in the DataFrame does not raise a
  Python exception — it returns `passed=False` with `detail="column
  missing"`, which surfaces through the normal severity-gating path rather
  than crashing the pipeline with a `KeyError`.

## Logging & audit

Every failing rule logs at `ERROR` (CRITICAL) or `WARNING` (everything
else) with `error_category = "{rule_type}:{severity}"` and
`records_rejected` — see `src/data_quality/framework.py::run_data_quality`.
Quarantined records carry `run_id, pipeline, rule_name, error_reason,
source_file, ingestion_timestamp` (`quarantine.py`).

## Security

Quarantined data retains the same sensitivity as the source (e.g. customer
PII) — the quarantine zone has identical encryption/access controls to
bronze/silver, not relaxed controls, despite being "reject" data.

## Performance

Each rule is a vectorized pandas/Spark operation (`isna().sum()`,
`duplicated()`, numeric comparison) — O(n) per rule, no row-by-row Python
loop. For very wide rule sets, rules run sequentially per column; this is
not a bottleneck at the batch sizes this reference implementation targets.

## Edge cases

| Case | Handling |
|---|---|
| Empty DataFrame | Most rules pass trivially (nothing to violate); `RECORD_COUNT` with `min > 0` correctly fails |
| Rule references non-existent column | Fails with `"column missing"` rather than raising |
| Multiple CRITICAL rules fail simultaneously | All are collected in `critical_failures`; `enforce()` reports all failed rule names in one error message, not just the first |
| `REGEX` rule against a numeric column | Coerced via `.astype(str)` before matching |

## Test cases

`tests/data_quality/test_dq_framework.py` — clean batch passes, CRITICAL
blocks + raises, ERROR doesn't block, duplicate key is CRITICAL, WARNING
never blocks, and an explicit "null key" negative test per Section 29.
`tests/unit/test_schema_evolution.py` covers the schema-match/evolution
side separately.
