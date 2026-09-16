# ADR-005: CDC Strategy

## Context

SQL Server's `customers` table (and similar entities) needs updates and
deletes to be visible downstream — a plain watermark scan only sees
inserts/updates on rows that still exist with an updated `modified_date`;
it never sees a deleted row disappear as a distinguishable event (Section 8).

## Decision

Model CDC as an explicit event stream (`operation` = I/U/D +
`change_timestamp` + business key + attributes), deduplicated by
business-key + latest-timestamp before ordered apply: deletes, then
updates, then inserts (`src.cdc.processor`, `src.cdc.merge`).

## Alternatives considered

- **AWS DMS (Database Migration Service) ongoing replication** — the
  natural production choice for capturing true SQL Server transaction-log
  CDC at scale; not implemented in this reference repo because it requires
  live AWS infrastructure and SQL Server connectivity this project
  explicitly avoids depending on (Section 1: "executable locally where
  practical"). The *processing* logic downstream of DMS (dedup, ordered
  apply, merge) is exactly what `src.cdc` implements and would be
  unchanged if DMS were the actual event source.
- **Full-table diff each run (compare full extract to previous full
  extract)** — rejected: expensive at scale, and cannot distinguish "row
  updated twice" from "row updated once," losing intra-window history that
  a real CDC feed preserves.
- **Trigger-based shadow tables in SQL Server** — rejected: couples this
  platform's needs to source-system schema changes (adding triggers to a
  production OLTP database), which SQL Server application owners are
  reasonably reluctant to accept.

## Rationale

Explicit I/U/D events let the platform apply the exact same operation the
source system applied, in a safe deterministic order, rather than inferring
intent from before/after snapshots. Deduplication by business key + latest
timestamp (not by arrival order) correctly resolves duplicate and
out-of-order delivery, which any real CDC transport (Kafka Connect, DMS,
Debezium) can produce under retry/redelivery.

## Trade-offs

- Requires a genuine CDC-capable source feed (DMS, Debezium, or
  vendor-native change tracking) in production — not available for every
  source system equally; SAP's IDoc/BAPI change tracking, for example, has
  different characteristics than SQL Server's, requiring a system-specific
  adapter even though the downstream processing logic is shared.
- Ordering guarantees depend on `change_timestamp` accuracy — a source
  system whose clock or commit-order tracking is unreliable would need a
  stronger ordering signal (e.g. an LSN) than this reference implementation
  models (see LLD-02's documented assumption).
- More moving parts than a plain watermark pipeline for tables that don't
  need delete visibility — hence CDC is used selectively (customers only,
  in the reference config), not for every table by default.
