# HLD 03 — Real-Time POS/E-commerce Events (HLD-3)

## Flow

```
POS / E-commerce -> Kinesis -> Lambda/consumer -> S3 -> processing -> Redshift/analytics
```

## Event schema

POS (`pos_sales_events`): `event_id, store_id, sku, quantity, amount,
event_timestamp, cashier_id, payment_method` — see
`streaming/schemas/pos_event.schema.json`.

E-commerce (`ecommerce_events`): `event_id, event_type, event_timestamp,
customer_id, order_id, sku, session_id` — see
`streaming/schemas/ecommerce_event.schema.json`. `event_type` is one of
`ORDER_CREATED, ORDER_UPDATED, PAYMENT_COMPLETED, CART_ACTIVITY,
PRODUCT_VIEWED`.

## Partition key

POS events partition by `store_id`; e-commerce events partition by
`customer_id` (`src/ingestion/streaming/event_schema.py::partition_key_for`).
This keeps a given store's transaction sequence — or a given customer's
session sequence (view → cart → order → payment) — ordered *within* a
shard, which matters for any downstream logic that assumes intra-entity
ordering (e.g. "payment should never precede order creation for the same
order_id"). It does **not** guarantee global ordering across shards, and
nothing in this platform assumes it does.

## Duplicates

Kinesis + Lambda's event-source mapping is **at-least-once**, not
exactly-once (Section 16, Section 38: "do not claim exactly-once processing
without implementation"). Duplicate delivery is handled downstream, not
prevented upstream: the consumer dedups by `event_id` before writing to S3
raw (`streaming/consumers/event_consumer.py`, `lambda/validation_handler/handler.py`),
so a redelivered record is a no-op, not a double-counted sale.

## Retries

A validation failure (missing field, invalid `event_type`, negative
quantity) does not retry — it is a permanent, not transient, failure for
that specific record, and is routed to quarantine/DLQ immediately
(`src.ingestion.streaming.event_schema.validate_pos_event`). A downstream
S3 write failure (throttling, transient network) *does* retry, governed by
the Lambda's own retry configuration and the stream's retry/DLQ settings.

## Late events

An event whose business timestamp falls meaningfully before the current
processing watermark is flagged (not dropped) via
`is_late_event()` — it is still written to S3 raw and processed normally,
but tagged so downstream aggregates that care about "as of" semantics (e.g.
the near-real-time sales mart) can choose whether to include it in an
already-closed window.

## Monitoring / alerting

- CloudWatch: Lambda error rate, Kinesis `IteratorAge` (consumer falling
  behind), invalid-record count.
- SNS: paged on sustained `IteratorAge` growth or elevated Lambda error
  rate — see `infrastructure/monitoring/cloudwatch-alarms.json`.

## Reference implementation

- `streaming/producers/pos_producer.py`, `streaming/producers/ecom_producer.py`
- `streaming/consumers/local_kinesis_adapter.py`, `streaming/consumers/event_consumer.py`
- `lambda/validation_handler/handler.py` (the AWS-hosted equivalent of the
  local consumer)
- `config/base/sources.yaml: pos_sales_events_stream, ecommerce_events_stream`
- `sql/marts/mart_sales_realtime.sql`
- Demo: `make demo-streaming`
