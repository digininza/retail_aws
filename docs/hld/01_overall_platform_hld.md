# HLD 01 — Overall Platform

## 1. Purpose

Describes the end-to-end architecture of the RetailCo AWS data platform at a
level suitable for a design review: what each layer does, what moves
between layers, and why each AWS service was chosen for its role. See
[ARCHITECTURE.md](../../ARCHITECTURE.md) for the full Mermaid diagram set.

## 2. Layers

| Layer | Technology | Responsibility |
|---|---|---|
| Source | SAP, SQL Server, REST APIs, POS, e-commerce, supplier files | Systems of origin — never modified by this platform |
| Ingestion | AWS Glue, AWS Lambda, Amazon Kinesis | Get data into the lake, in as close to raw form as practical |
| Data Lake | Amazon S3 | Durable, zoned storage: raw/bronze/silver/gold/quarantine/archive |
| Processing | AWS Glue, Amazon EMR | Standard ETL (Glue) vs. heavy/complex Spark (EMR) — see ADR-002 |
| Curated / Warehouse | Amazon Redshift | Dimensional model for BI and analytics |
| Orchestration | Apache Airflow (MWAA) | Scheduling, dependency management, retries — never the transform engine |
| Cross-cutting | IAM, KMS, Secrets Manager, CloudWatch, SNS, CodePipeline/CodeBuild | Security, observability, alerting, deployment |

## 3. Why this shape

The platform is organized around a single principle: **each AWS service
does the one thing it is best suited for, and nothing else pretends to be
that service.** This shows up concretely as:

- Glue never runs a TB-scale historical join (that's EMR's job).
- EMR never handles lightweight, per-table cleansing (that's Glue's job —
  spinning up a cluster for a 10K-row incremental load wastes both time and
  money).
- Airflow never executes a Spark transformation inline (it triggers and
  monitors Glue/EMR; see ADR-003).
- Lambda never processes bulk/TB-scale data (Section 17) — it is reserved
  for sub-second, event-driven glue logic.
- S3 is the only place data is durably stored pre-Redshift; nothing writes
  directly to Redshift from a source system.

## 4. Data movement, end to end

```
Source systems -> Ingestion (Glue/Lambda/Kinesis) -> S3 raw
  -> [DQ, dedup] -> S3 bronze -> [conform, validate] -> S3 silver
  -> [aggregate, join, business logic — Glue or EMR depending on weight] -> S3 gold
  -> Redshift staging -> MERGE -> Redshift curated (dims + facts) -> marts -> BI
```

Every arrow in this chain is gated: a DQ CRITICAL failure or a reconciliation
mismatch stops the batch at that stage rather than letting bad data flow
downstream (Section 11, Section 12).

## 5. Environments

Dev, QA, and Prod are fully separate AWS accounts/buckets/clusters (ADR-009)
— there is no shared "staging" bucket that multiple environments write to.
Promotion between environments is code + config promotion via CI/CD
(Section 26), never data copying.

## 6. Non-goals

- This platform does not attempt exactly-once stream processing (Section 16)
  — it is at-least-once with idempotent downstream writes.
- This platform does not replace the source systems' own transactional
  guarantees; it is a downstream analytical copy.
- BI tooling itself is out of scope — Redshift marts are the contract
  boundary with BI/analytics.

## 7. Related documents

- [02_batch_ingestion_hld.md](02_batch_ingestion_hld.md) — SQL Server
  incremental (Glue) and historical sales backfill (EMR)
- [03_streaming_hld.md](03_streaming_hld.md) — POS/e-commerce real-time events
- [04_data_quality_hld.md](04_data_quality_hld.md) — DQ + reconciliation
- [05_cicd_security_hld.md](05_cicd_security_hld.md) — CI/CD and security
- [../adr/](../adr/) — full Architecture Decision Records
