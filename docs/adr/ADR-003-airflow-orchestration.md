# ADR-003: Airflow for Orchestration

## Context

Pipelines span multiple engines (Glue, EMR) and multiple sequenced stages
(extract, validate, transform, DQ, reconciliation, load) with dependencies,
retry needs, and scheduling requirements (Section 13). Something needs to
own "what runs when, in what order, with what retry policy" independent of
any single engine's own scheduling capability.

## Decision

Use Apache Airflow (Amazon MWAA) as the orchestration layer. Airflow
triggers and monitors Glue jobs and EMR steps; it never executes Spark
transformations itself.

## Alternatives considered

- **Glue Workflows** — rejected as the sole orchestrator: works for
  Glue-only sequences but doesn't naturally extend to EMR step monitoring,
  cross-system dependencies (e.g. "wait for an S3 file from a Lambda"), or
  the reconciliation/control-table Python logic this project needs woven
  into the DAG.
- **Step Functions** — a reasonable alternative for this project's needs;
  not chosen because Airflow's DAG-as-Python model, mature retry/backoff
  configuration, and wide operator ecosystem for AWS services (Glue, EMR,
  SNS, Redshift Data API) fit an already-Python-centric codebase more
  naturally, and Airflow is the more commonly expected orchestration tool
  in Senior Data Engineer interview contexts this project targets.
- **Cron + custom scripts** — rejected: no dependency graph, no built-in
  retry/backoff, no UI/observability, no backfill support.

## Rationale

- One place to see "is today's pipeline healthy," across every engine
  involved.
- Declarative retry/backoff/SLA configuration per task, independent of
  each engine's own (if any) retry behavior.
- `dag_run.conf` parameterization makes backfill a config difference, not
  a separate code path (Section 13, LLD-07).
- A large operator ecosystem for AWS services means orchestration logic
  stays declarative rather than hand-rolled boto3 polling loops scattered
  across the codebase.

## Trade-offs

- Airflow is another system to operate/secure (MWAA environment, its own
  execution role) — adds operational surface versus a simpler
  single-engine scheduling approach.
- Discipline is required to keep heavy computation out of `PythonOperator`
  callables — nothing technically stops a developer from loading a large
  DataFrame inside a DAG file, so this project treats it as a documented
  rule (Section 13) enforced by code review, not by a hard technical
  barrier.
- DAG-as-Python means DAG correctness bugs are only caught at parse/run
  time in the MWAA environment, not locally with the same fidelity as this
  project's Python-native demo scripts (see `airflow/README.md`'s note on
  DAGs not running locally).
