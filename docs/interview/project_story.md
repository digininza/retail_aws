# Interview Project Story

## 30-second version

I built a reference AWS data platform for a fictional omnichannel retailer,
modernizing fragmented SAP/SQL Server/API/POS/e-commerce sources into a
governed S3 data lake and a Redshift dimensional warehouse — with
production-style patterns baked in: incremental and CDC ingestion, SCD Type
2, idempotent merge/upsert, embedded data quality and reconciliation gates,
and full CI/CD across Dev/QA/Prod.

## 60-second version

The architecture separates concerns deliberately: AWS Glue handles managed
ingestion and standard ETL; Amazon EMR handles heavy, complex Spark
workloads like historical backfills; Apache Airflow orchestrates
dependencies and retries but never runs Spark itself; Kinesis and Lambda
capture near-real-time POS and e-commerce events; S3 is the durable lake
(raw → bronze → silver → gold, plus quarantine and archive zones); and
Redshift holds the conformed dimensional model BI consumes. Every pipeline
is metadata-driven — onboarding a new source table is a YAML change, not new
code — and every promotion to Gold or Redshift is gated by data quality and
reconciliation checks that run automatically.

## 2-minute version

RetailCo's legacy estate — SAP, SQL Server, REST APIs, POS terminals,
e-commerce events, supplier files — had no shared identity model, no CDC,
no systematic data quality, and pipelines that weren't safely restartable.
The business problem was slow, manual reconciliation and no single trusted
view of a customer, product, or store.

I designed the platform around a watermark-based control table that only
advances *after* extract, write, DQ, and reconciliation have all succeeded
— never before — so a mid-pipeline failure always leaves the system in a
safe, resumable state. CDC events are deduplicated by business key and
latest timestamp before being applied in delete-then-update-then-insert
order, feeding an SCD Type 2 customer dimension that preserves full
attribute history using hash-based change detection. Every merge/upsert is
idempotent by construction — rerunning the same batch twice produces the
same final state both times, which is what makes Airflow's automatic
retries actually safe rather than just hopeful.

Data quality and reconciliation are two distinct, both-mandatory gates: DQ
asks "is each record valid," reconciliation asks "did the expected volume
of data actually move correctly." A CRITICAL DQ failure or a reconciliation
mismatch blocks promotion to Gold/Redshift and quarantines the offending
batch with full audit metadata attached.

The outcome: a centralized, governed data lake; metadata-driven pipelines
that don't require new code per table; embedded quality gates instead of
downstream firefighting; restartable, idempotent processing; a trusted
dimensional warehouse; near-real-time event capture; and CI/CD-governed
promotion across environments with least-privilege IAM throughout.

## Senior-level version

The architecture decisions I'd highlight in a senior discussion:

- **Glue vs. EMR is a workload-shape decision, not a preference** — Glue
  for managed, moderate-volume, per-table ETL; EMR for heavy joins/
  aggregations where I need explicit control over partitioning, broadcast
  joins, and skew handling. Using EMR for everything wastes cost on small
  jobs; using Glue for everything under-powers the heavy backfills.
- **The watermark commit is a state machine, not a convention** — I built
  a `WatermarkCommitGuard` that makes it structurally impossible to
  advance the watermark without passing through every required stage.
  This isn't a "remember to do X" rule that a future engineer can forget;
  it's enforced by the code path itself.
- **Idempotency is the reliability foundation, not an afterthought** —
  every merge/upsert and SCD2 apply is designed so a rerun of the same
  batch is a no-op on top of itself. That's what makes retries — Airflow's,
  Glue's own retry, anything — actually safe rather than a hope.
- **Reliability trade-offs I made explicitly, not accidentally**: I do not
  claim exactly-once stream processing (I implement at-least-once with
  idempotent consumers, which is honest about what Kinesis + Lambda
  actually guarantee). I do not apply SCD2 to every dimension — only where
  historical attribution is analytically meaningful, because the surrogate-
  key/effective-date complexity has a real cost.
- **Security and environment isolation are structural, not just
  policy** — separate AWS accounts, separate IAM roles per service per
  environment, secrets resolved via Secrets Manager ARN references never
  hardcoded, and Prod deploys gated by a human approval stage that Dev/QA
  don't require.
- **Cost and operational discipline show up in concrete choices**: EMR
  clusters are ephemeral, not standing; S3 lifecycle policies move raw data
  to cheaper storage classes on a schedule; Redshift WLM queues separate
  ETL load from BI query concurrency.

## Service one-liners

| Service | One-line explanation |
|---|---|
| S3 | Central data lake. |
| Glue | Managed ingestion and standard ETL. |
| EMR | Large-scale Spark processing. |
| Airflow | Workflow orchestration. |
| Kinesis | Real-time event ingestion. |
| Lambda | Lightweight event-driven processing. |
| Redshift | Analytical warehouse. |
| IAM | Identity and least-privilege access. |
| KMS | Encryption/key management. |
| CloudWatch | Operational monitoring. |
| SNS | Notifications. |
| CodePipeline/CodeBuild | CI/CD automation. |
