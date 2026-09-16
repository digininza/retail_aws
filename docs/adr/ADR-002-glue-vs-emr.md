# ADR-002: Glue vs. EMR — Workload Split

## Context

The platform has two distinct processing shapes: (a) frequent, moderate-volume,
per-table extraction and standard cleansing (dozens of source tables, daily),
and (b) infrequent but heavy historical joins/aggregations/backfills
(potentially billions of rows, complex multi-way joins). A single engine
choice for both is either wasteful (a cluster for small jobs) or
under-powered (a managed job model for heavy Spark tuning).

## Decision

Use **AWS Glue** for standard ETL, ingestion, and Data Catalog integration.
Use **Amazon EMR** for heavy/complex Spark workloads: large historical
joins, TB-scale aggregations, and backfills (Section 15). Never blur the
two — a workload gets one engine, chosen by its actual shape, not by
convenience or habit.

## Alternatives considered

- **EMR for everything** — rejected: massively over-provisions for
  per-table incremental loads; a standing or per-job cluster for a
  10K-row extract wastes cost and adds operational overhead (job
  submission, cluster health) that Glue's managed model avoids.
- **Glue for everything, including heavy historical processing** —
  rejected: Glue's managed job model abstracts away exactly the tuning
  knobs (explicit partition counts, broadcast join control, cluster
  instance mix) that large historical joins need to avoid shuffle
  bottlenecks and skew.
- **A single Spark-on-Kubernetes (EKS) platform for all workloads** —
  rejected as unnecessary operational complexity for this project's scale;
  would be reconsidered at a materially larger organization-wide Spark
  footprint.

## Rationale

Routing decision, concretely (see `config/base/pipelines.yaml:
engine_routing`):

| Signal | Engine |
|---|---|
| Single-table, JDBC/API-sourced, standard cleansing | Glue |
| Needs Glue Data Catalog / crawler integration | Glue |
| Multi-way join across large datasets | EMR |
| Explicit control over partitioning/broadcast/skew needed | EMR |
| Occasional/backfill cadence, benefits from ephemeral clusters | EMR |
| Frequent (daily+) cadence, benefits from managed job scheduling | Glue |

## Trade-offs

- Two engines means two operational surfaces to monitor, secure
  (separate IAM roles), and reason about — versus a simpler one-engine
  story.
- A workload that starts small (Glue-appropriate) and grows large over
  time requires a deliberate migration decision to EMR — there's no
  automatic escalation.
- Developers need to understand both APIs (Glue's DynamicFrame-flavored
  patterns and EMR's plain PySpark) rather than one.
