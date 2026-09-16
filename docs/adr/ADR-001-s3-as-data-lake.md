# ADR-001: S3 as the Data Lake

## Context

RetailCo needs a single, durable store for both raw and increasingly-refined
copies of data from six heterogeneous source systems, at a scale and
retention period that a purely relational staging area would make expensive
and inflexible.

## Decision

Use Amazon S3 as the data lake, organized into `raw/`, `bronze/`, `silver/`,
`gold/`, `quarantine/`, and `archive/` zones, with Parquet as the standard
format for processed data (Section 5).

## Alternatives considered

- **HDFS on a standing EMR/on-prem cluster** — rejected: requires
  always-on compute to keep storage available, versus S3's storage/compute
  decoupling.
- **Land directly into Redshift staging tables** — rejected: no cheap,
  durable, replayable copy of raw source data; reprocessing after a bug fix
  would require re-extracting from source systems rather than replaying
  already-captured raw files.
- **A relational "raw" database (e.g. RDS) as the landing zone** — rejected:
  poor fit for large volumes of semi-structured/varied-schema source data,
  and materially more expensive per TB than S3.

## Rationale

- S3 decouples storage growth from compute cost — Glue/EMR/Redshift can
  scale independently of how much raw history is retained.
- Immutable `raw/` preserves the ability to reprocess from source truth if
  a downstream bug is discovered, without re-extracting from often-fragile
  source systems (Section 5).
- Zone separation (bronze/silver/gold) gives every consumer a clear
  contract about data maturity, rather than one undifferentiated bucket.
- Native lifecycle policies handle cost-effective retention/archival
  without custom tooling (Section 34).

## Trade-offs

- S3 is not a query engine — analytical access requires Glue/Athena/Redshift
  Spectrum on top, an added layer versus a database that's directly
  queryable.
- Zone-to-zone promotion logic (what makes data "silver" vs. "gold") must
  be enforced by pipeline code/DQ gates — S3 itself has no concept of data
  quality or schema.
- Parquet requires a write-time schema decision; schema evolution across
  files in the same prefix needs explicit handling (Section 24), unlike a
  schema-on-read approach with raw JSON/CSV everywhere.
