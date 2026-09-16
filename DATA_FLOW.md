# Data Flow — End-to-End Demonstration Scenarios

Three demonstrable end-to-end flows (Section 41), each runnable locally against
sample data using the local adapters in `src/common`.

## Scenario 1 — SQL Server Incremental Orders (Glue path)

```
SQL Server (orders)
  -> src/ingestion/sqlserver/extractor.py (watermark read)
  -> S3 raw/orders/  (glue/jobs/ingest_sqlserver.py)
  -> schema validation (config/base/schemas.yaml)
  -> Glue Bronze->Silver transform (glue/jobs/bronze_to_silver.py)
  -> Data Quality (src/data_quality/framework.py, dq_profile=orders_standard)
  -> Reconciliation (src/reconciliation/framework.py, reconciliation_profile=orders_daily)
  -> Redshift staging (sql/staging/stg_orders.sql)
  -> MERGE into fact_sales (sql/facts/merge_fact_sales.sql)
  -> audit row written (src/common/audit.py)
  -> watermark advanced only now (src/common/idempotency.py)
```

Demo entry point: `make demo-incremental` → `src/ingestion/sqlserver/run_demo.py`.
See HLD-1 (`docs/hld/02_batch_ingestion_hld.md`) and LLD-1
(`docs/lld/01_sqlserver_incremental_ingestion_lld.md`).

## Scenario 2 — Historical Sales Modernization (EMR path)

```
Legacy historical sales extract (sample_data/orders/historical_sales_sample.csv)
  -> S3 raw/historical_sales/
  -> Airflow DAG airflow/dags/sales_pipeline.py: validate_input
  -> EMR submit: emr/jobs/large_sales_transformation.py
  -> S3 silver/sales/ -> S3 gold/sales/ (customer_360.py, inventory_aggregation.py join in)
  -> DQ (CRITICAL rules gate promotion)
  -> Reconciliation (control totals vs. source extract manifest)
  -> Redshift load (sql/facts/load_fact_sales_historical.sql)
```

Demo entry point: `make demo-emr-backfill` → `emr/jobs/historical_backfill.py` run
in local PySpark mode. See HLD-2 (`docs/hld/02_batch_ingestion_hld.md` §EMR) and
ADR-002.

## Scenario 3 — POS / E-commerce Real-Time Events (Kinesis path)

```
POS terminal / e-commerce app (streaming/producers/pos_producer.py, ecom_producer.py)
  -> Kinesis Data Stream (local: streaming/consumers/local_kinesis_adapter.py)
  -> Lambda consumer (lambda/api_ingestion_handler / streaming consumer logic)
  -> S3 raw/events/ (partitioned by event_date/hour)
  -> Glue micro-batch processing (bronze -> silver)
  -> Redshift near-real-time mart (sql/marts/mart_sales_realtime.sql)
```

Demo entry point: `make demo-streaming` → `streaming/producers/pos_producer.py`
feeding `streaming/consumers/event_consumer.py`. See HLD-3
(`docs/hld/03_streaming_hld.md`).

## Cross-Scenario Invariants

- Every scenario writes an audit row (`run_id`, counts, watermarks, status) —
  see Section 36 / `src/common/audit.py`.
- Every scenario blocks promotion on a CRITICAL data-quality failure — see
  `docs/hld/04_data_quality_hld.md`.
- Every scenario is idempotent: rerunning with the same input and `run_id`
  produces the same final state — see ADR-004, ADR-005, `src/common/idempotency.py`.
- Watermarks / stream checkpoints are only advanced after a fully successful
  downstream commit (Section 7, Section 23).
