# Architecture Decision Records — Index

Full ADRs live in [docs/adr/](docs/adr/). Each follows Context / Decision /
Alternatives / Rationale / Trade-offs.

| ADR | Title | Summary |
|---|---|---|
| [ADR-001](docs/adr/ADR-001-s3-as-data-lake.md) | S3 as data lake | S3 raw/bronze/silver/gold/quarantine/archive zones as the durable system of record |
| [ADR-002](docs/adr/ADR-002-glue-vs-emr.md) | Glue vs EMR | Glue for managed ingestion/standard ETL/catalog; EMR for heavy/complex Spark |
| [ADR-003](docs/adr/ADR-003-airflow-orchestration.md) | Airflow orchestration | Airflow orchestrates; it never executes Spark transformations itself |
| [ADR-004](docs/adr/ADR-004-watermark-control-table.md) | Watermark/control-table strategy | Relational control table; watermark commits only after successful downstream write |
| [ADR-005](docs/adr/ADR-005-cdc-strategy.md) | CDC strategy | Business-key + latest-change-timestamp dedup, ordered D/U/I apply |
| [ADR-006](docs/adr/ADR-006-scd-type2.md) | SCD Type 2 | Hash-based change detection, surrogate key, effective-dated history |
| [ADR-007](docs/adr/ADR-007-kinesis-streaming.md) | Kinesis for streaming | Kinesis Data Streams for POS/e-commerce; at-least-once, idempotent consumers |
| [ADR-008](docs/adr/ADR-008-redshift-warehouse.md) | Redshift analytical warehouse | Star-schema dimensional model as the BI boundary |
| [ADR-009](docs/adr/ADR-009-environment-configuration.md) | Environment configuration | Base + per-env YAML overlay; secrets never in YAML |
| [ADR-010](docs/adr/ADR-010-cicd-approach.md) | CI/CD approach | CodeBuild/CodePipeline; Dev auto-deploy, QA gated by smoke test, Prod gated by manual approval |

## How to read an ADR

Each ADR answers: what problem forced a decision, what was decided, what else
was considered, why the chosen option won, and what it costs us. ADRs are not
updated retroactively to look right in hindsight — if a decision is revisited,
a new ADR supersedes the old one and both are kept for history.
