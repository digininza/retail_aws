# ADR-007: Kinesis for Streaming Ingestion

## Context

POS and e-commerce systems produce events (sales transactions, cart
activity, order lifecycle) that the business wants visible with much lower
latency than the daily batch cadence serves — without the platform claiming
guarantees (exactly-once, sub-second global ordering) it doesn't actually
implement (Section 16, Section 38).

## Decision

Use Amazon Kinesis Data Streams to ingest POS and e-commerce events,
consumed by Lambda (or an equivalent streaming consumer), landing validated
records in S3 raw/ for downstream micro-batch processing.

## Alternatives considered

- **Amazon MSK (Kafka)** — a reasonable alternative, generally favored when
  multiple independent consumer groups need the same stream with long
  retention and replay, or when there's an existing organizational Kafka
  investment. Not chosen here: this platform has a small, well-defined set
  of consumers (land-to-S3, near-real-time mart refresh) that Kinesis
  serves well with materially less operational overhead than running/tuning
  a Kafka cluster.
- **Direct API Gateway + Lambda (no stream)** — rejected: no buffering
  against consumer slowdowns, no replay capability, no natural shard-based
  partitioning for ordering.
- **Firehose direct-to-S3 (skip Lambda validation)** — rejected as the
  sole path: Firehose is excellent for pure landing, but this platform
  needs per-record schema validation and quarantine routing *before* data
  reaches raw/, which a Lambda-based consumer provides and pure Firehose
  delivery does not.

## Rationale

Kinesis's shard-based partition-key model maps naturally onto this
platform's ordering needs (store-level for POS, customer/session-level for
e-commerce — Section 16), and its native Lambda event-source-mapping
integration keeps the consumer path simple: no separate consumer-group
infrastructure to run.

## Trade-offs

- At-least-once delivery, not exactly-once — every downstream consumer
  (this project's `event_consumer.py`/Lambda validation handler) must be
  idempotent (dedup by `event_id`) rather than relying on the stream to
  prevent duplicates (Section 38 — explicitly not claimed).
- Shard-level ordering only — no global ordering guarantee across shards;
  the partition-key choice (store_id/customer_id) is a deliberate trade
  of "order within an entity" for "no single global bottleneck shard."
- Kinesis capacity (shard count) must be sized/monitored — under-provisioned
  shards cause `IteratorAge` growth (consumer falling behind), a metric
  this platform explicitly monitors (Section 16, `infrastructure/monitoring/`).
