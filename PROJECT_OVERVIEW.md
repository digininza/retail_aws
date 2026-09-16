# Project Overview

## 1. Company: RetailCo

RetailCo is a fictional mid-size omnichannel retailer (~350 stores, national
e-commerce site and mobile app, a distribution network of 6 regional warehouses,
and roughly 1,200 active suppliers). It sells apparel, home goods, and electronics.

## 2. Business Problem

RetailCo's data estate grew organically over 15 years:

- **SAP ERP** holds product, vendor, purchase-order and inventory data, extracted via
  nightly batch jobs into flat files that analysts stitch together manually.
- **SQL Server** holds customer, order, and store data for the e-commerce and
  store-ops applications; there is no CDC — downstream systems either poll fully
  or rely on brittle, hand-written trigger tables.
- **REST APIs** from promotions and marketplace partners are pulled ad hoc by
  scripts with no retry/backoff discipline, causing silent data gaps during
  partner outages.
- **POS terminals** emit sales transactions that reach the warehouse only via a
  next-day batch file, so store performance is always a day stale.
- **E-commerce events** (cart, order, payment) are logged but not centrally
  captured for analytics beyond what the checkout database retains.
- **Supplier files** (CSV/JSON catalogs, price lists, inventory feeds) land in an
  FTP folder and are loaded inconsistently, with no schema or quality gate.

The result: fragmented, stale, and unreliable data; slow, manual reconciliation
between finance and merchandising; no single trusted view of a customer, product,
or store; and a reporting stack that cannot support same-day decisions.

## 3. Objective

Modernize RetailCo's data platform on AWS to deliver:

1. A governed, durable **data lake** (S3) as the system of record for raw and
   curated data.
2. **Incremental and CDC-based ingestion** from SAP, SQL Server, REST APIs and
   supplier files — eliminating full reloads.
3. **Near-real-time capture** of POS and e-commerce events via Kinesis.
4. A **metadata-driven pipeline framework** (Glue + EMR) so onboarding a new
   source table is a configuration change, not new code.
5. **SCD Type 2** dimensional history, **merge/upsert** semantics, and
   **idempotent, restartable** pipelines.
6. Embedded **data quality** and **reconciliation** at every promotion boundary.
7. A conformed **dimensional model in Redshift** for BI and analytics.
8. **Security, observability, and CI/CD** appropriate for a regulated retail
   environment (PCI-adjacent payment data, PII in customer records).

## 4. Scope

In scope: ingestion framework design and reference implementation, S3 lake
layout, Glue/EMR processing responsibilities, Airflow orchestration, Redshift
dimensional model, DQ/reconciliation frameworks, security model, CI/CD design,
and full HLD/LLD/ADR documentation.

Out of scope: a live AWS deployment, real SAP/SQL Server connectivity, and a BI
tool implementation (Redshift marts are the boundary; BI consumption is
described but not built).

## 5. Before vs After

### Before
- Fragmented source systems with no shared identity or timing model.
- Nightly, all-or-nothing batch loads; no incremental extraction.
- No CDC — updates and deletes are invisible or require full reloads to surface.
- No systematic data quality checks; bad data reaches reports and is caught by
  end users, not the pipeline.
- No reconciliation discipline; finance and merchandising reconcile manually
  in spreadsheets.
- Pipelines are not restartable — a mid-run failure often requires a manual
  full reprocess.
- Weak lineage and auditability — nobody can answer "what changed and when"
  without reading application logs.
- Store and e-commerce sales visibility lags by a day or more.
- Environment configuration and credentials are managed ad hoc, increasing the
  risk of a Dev script touching Prod data.
- Deployments are manual, undocumented, and person-dependent.

### After
- A centralized AWS data lake (S3, raw → bronze → silver → gold) as the
  governed system of record.
- Metadata-driven, watermark-based incremental ingestion; CDC-aware processing
  for SQL Server-originated entities.
- POS and e-commerce events captured near-real-time via Kinesis.
- Reusable data-quality and reconciliation frameworks gate every promotion to
  Gold and Redshift.
- Idempotent, restartable pipelines: a rerun after failure is safe by
  construction (see ADR-004, ADR-005).
- Full run-level audit trail (`run_id`, record counts, watermarks, status) for
  every pipeline.
- A conformed Redshift dimensional model (SCD2 dimensions + facts) supports
  consistent, trusted analytics.
- Explicit IAM roles, KMS encryption, and Secrets Manager-based credential
  handling replace ad hoc access.
- CI/CD (CodeBuild/CodePipeline) governs promotion across Dev → QA → Prod with
  automated validation and a manual approval gate.
- Operational visibility via CloudWatch metrics/alarms and SNS notifications
  replaces "someone notices the report is wrong."

*Outcomes above are described qualitatively and directionally. No specific
percentage improvement is claimed unless explicitly marked as illustrative —
see [DECISIONS.md](DECISIONS.md) and Section 31 realism rules.*

## 6. Stakeholders (illustrative)

| Role | Interest |
|---|---|
| VP Merchandising | Trusted, timely sales/inventory data |
| Finance | Auditable, reconciled transaction totals |
| Store Operations | Near-real-time store performance |
| Data Platform Team | Reusable, low-maintenance pipeline framework |
| Security/Compliance | Least-privilege access, PII protection, auditability |
| BI/Analytics | A stable, documented dimensional model |

## 7. Success Criteria

See Section 44 (Final Quality Check) in the original requirements and the
[Definition of Done](../retail_aws/README.md) checklist — the platform is
considered complete when every framework (incremental, CDC, SCD2, merge,
idempotency, DQ, reconciliation, retry, audit) is implemented and documented,
Glue/EMR/Airflow responsibilities are unambiguous, and the repository can be
explained end-to-end in an interview setting.
