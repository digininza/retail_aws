# Airflow Orchestration

DAGs in this directory target a managed Airflow environment (Amazon MWAA).
They import `airflow.providers.amazon.aws.*` operators that are not
installed in this repo's local dev environment — unlike the demo scripts
under `src/`, these DAGs are not meant to run locally; they are reviewed for
structure/logic, and validated in a real MWAA Dev environment as part of
CI/CD deployment (see `cicd/buildspec.yml`).

## DAGs

| DAG | Purpose | Schedule |
|---|---|---|
| `retail_batch_pipeline` | Master reference DAG: extract → land → validate → Glue → EMR → DQ → reconciliation → Redshift → commit | Daily 03:00 UTC |
| `customer_pipeline` | CDC extraction → dedup → DQ → SCD2 merge into `dim_customer` | Daily 04:00 UTC |
| `product_pipeline` | Parallel SAP master-data extraction (product/vendor/PO) → Gold | Daily 02:30 UTC |
| `sales_pipeline` | The explicit Glue → EMR → Reconciliation → Redshift sequence (Section 13); backfill-capable via `dag_run.conf` | Daily 05:00 UTC |
| `reconciliation_pipeline` | Detective-control sweep across all active pipelines' reconciliation profiles | Daily 07:00 UTC |

## Why Airflow never runs Spark itself

Every Glue/EMR task in these DAGs triggers a managed job and polls for
completion (`GlueJobOperator`, `EmrAddStepsOperator` + `EmrStepSensor`).
No DAG contains a `PythonOperator` that itself loads a Spark DataFrame or
processes bulk data — that would make the Airflow worker (a small, shared
resource) a processing bottleneck and single point of failure for
unrelated DAGs. See ADR-003 and `ARCHITECTURE.md`'s Glue/EMR/Airflow table.

## Retry, SLA, and failure handling

- `retries` + `retry_delay` (+ exponential backoff on the heavier DAGs) are
  set per DAG in `default_args`, distinct from the retry policy inside
  Glue/EMR jobs themselves (`src.common.retry`) — a task can retry at the
  Airflow level even if the underlying job has already exhausted its own
  internal retries, and vice versa.
- `sla` flags a run that is taking too long without failing it outright —
  paired with a CloudWatch/SNS alert in a real deployment.
- `on_failure_callback` (see `retail_batch_pipeline.py::_on_failure_callback`)
  publishes to SNS on every task failure with full run context.
- Every task is designed to be safe to rerun (Section 13: "Airflow retry:
  tasks must be safe to rerun") because the underlying framework
  (`src.common.idempotency`, `src.cdc.merge`, SCD2 hash comparison) is
  idempotent by construction — Airflow's retry mechanism does not need any
  special-casing to be safe.

## Backfill

`sales_pipeline` demonstrates the pattern: pass `start_date`/`end_date` via
`dag_run.conf` (or trigger via the MWAA CLI/API) to reprocess a historical
window; the DAG structure and tasks are identical to a normal daily run —
backfill is a data-scoping parameter, not a separate code path.

## Plugins

`plugins/dq_check_operator.py` — `DataQualityCheckOperator`, a reusable
custom operator wrapping `src.data_quality.framework`, used for the
`dq_precheck`/`dq_postcheck` tasks so DQ enforcement is not duplicated
inline across DAGs.
