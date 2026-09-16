# ADR-008: Redshift as the Analytical Warehouse

## Context

BI/analytics needs a stable, performant, SQL-queryable, conformed
dimensional model (facts + dimensions) as the trusted contract boundary
between the data platform and consumers — distinct from the data lake's
raw/bronze/silver/gold zones, which are pipeline-internal, not a BI-facing
interface.

## Decision

Use Amazon Redshift as the curated analytical warehouse: a star-schema
dimensional model (`dim_customer` [SCD2], `dim_product`, `dim_store`,
`dim_date`, `dim_promotion`, `fact_sales`, `fact_inventory`) plus derived
marts (Section 18).

## Alternatives considered

- **Query the S3 Gold layer directly via Athena/Spectrum, skip a
  warehouse** — rejected as the *only* analytical layer: works for ad hoc
  exploration, but lacks the workload-managed concurrency, consistent
  sub-second BI-dashboard performance, and mature MERGE/upsert semantics
  this platform's SCD2/fact-load patterns rely on. (Spectrum is still used
  conceptually as a complementary path for querying Gold data that hasn't
  yet been loaded into Redshift — not modeled further in this reference
  repo.)
- **Snowflake** — a strong alternative with excellent separation of storage
  and compute; not chosen here specifically because this project is scoped
  as an AWS-native reference architecture (the brief calls for Glue, EMR,
  Redshift specifically), not a cross-cloud comparison.
- **Keep everything in S3/Glue Catalog, use Athena as the sole query
  engine** — rejected: Athena's per-query pricing and lack of native
  MERGE support make it a weaker fit for a frequently-updated dimensional
  model than for occasional ad hoc analysis.

## Rationale

Redshift's `MERGE` statement support directly implements this platform's
merge/upsert and SCD2 patterns in native SQL (`sql/facts/merge_fact_sales.sql`,
`sql/dimensions/scd2_merge_dim_customer.sql`); its WLM (workload
management) queues let ETL load and BI query concurrency be isolated so a
heavy load doesn't starve dashboard users (`infrastructure/redshift/redshift-cluster-config.json`).

## Trade-offs

- Another piece of always-on(ish) infrastructure with its own cost profile
  — mitigated by RA3 node types (storage/compute separation) and
  environment-appropriate sizing (single-node Dev/QA, multi-node Prod).
- Star-schema modeling requires upfront dimensional design discipline
  (surrogate keys, conformed dimensions) versus dumping flat tables — a
  deliberate cost for long-term query simplicity and consistency.
- Redshift is not the right tool for the heavy Spark-side transformation
  work itself (EMR's job, per ADR-002) — data must already be
  conformed/aggregated appropriately by the time it reaches Redshift
  staging.
