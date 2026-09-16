# HLD 02 — Batch Ingestion (HLD-1: SQL Server Incremental, HLD-2: Historical Sales/EMR)

## HLD-1: SQL Server Incremental Retail Orders

### Flow

```
SQL Server (orders) -> Glue -> S3 Raw -> Glue validation -> Glue transformation
  -> S3 Silver -> Redshift -> BI
```

### Why Glue

This is a textbook Glue workload: moderate volume, JDBC-sourced, a single
table per job, standard cleansing/validation logic, and it needs Glue Data
Catalog integration for downstream Spectrum/Athena queryability. There is no
heavy join or TB-scale aggregation here that would justify EMR's operational
overhead (ADR-002).

### Control flow (Section 7)

```
read control.pipeline_control.last_successful_watermark
  -> extract WHERE modified_date > watermark
  -> write S3 raw/orders/
  -> bronze_to_silver: dedup by order_id, schema validation
  -> DQ (orders_standard profile) — CRITICAL failures block promotion
  -> reconciliation (orders_daily profile) — count/sum vs. source
  -> commit S3 gold/redshift_staging/stg_orders/
  -> MERGE into curated.fact_sales
  -> write audit.pipeline_run_log row
  -> ONLY NOW: update control.pipeline_control.last_successful_watermark
```

If any stage from DQ onward fails, the watermark is **not** updated —
`src.common.idempotency.WatermarkCommitGuard` enforces this in code, not
just by convention. The next run re-reads the last *successful* watermark
and re-extracts the same window, safely, because the merge into
`fact_sales` is keyed on `(order_id, order_item_id)`, not on run timestamp
(Section 9).

### Idempotency

Rerunning the same watermark window twice produces the same `fact_sales`
state both times: `sql/facts/merge_fact_sales.sql` is a `MATCHED ->
UPDATE / NOT MATCHED -> INSERT` MERGE on the composite business key, so a
duplicate extract overwrites rather than duplicates.

### Reference implementation

- `src/ingestion/sqlserver/extractor.py`, `src/ingestion/sqlserver/run_demo.py`
- `glue/jobs/ingest_sqlserver.py`, `glue/jobs/bronze_to_silver.py`
- `config/base/sources.yaml: sqlserver_orders_incremental`
- LLD: [../lld/01_sqlserver_incremental_ingestion_lld.md](../lld/01_sqlserver_incremental_ingestion_lld.md)

---

## HLD-2: Large Historical Sales Modernization

### Flow

```
Legacy historical data -> S3 Raw -> EMR Spark -> S3 Silver -> S3 Gold -> Redshift
```

Airflow:

```
start -> validate input -> EMR submit -> monitor -> DQ -> reconciliation -> Redshift
```

### Why EMR

Historical backfill reprocesses multiple years of order/order-item data —
potentially billions of rows — with wide joins against dimension data and
region/month-level aggregation. This needs explicit control over shuffle
partitioning, broadcast-join decisions, and cluster sizing that Glue's
managed job model does not expose to the same degree. It also runs
*occasionally* (a backfill, not a daily job), which is exactly the shape
that benefits from an ephemeral, purpose-sized EMR cluster rather than a
standing resource (Section 34 cost optimization). See ADR-002.

### Why Airflow is separate from EMR

Airflow's job here is entirely about orchestration: confirm the input data
exists for the requested backfill window (`validate_input`), launch the EMR
cluster/step (`emr_submit`), poll for completion (`emr_monitor` via
`EmrStepSensor`), and only then run reconciliation and load Redshift. At no
point does an Airflow worker load or transform data itself — that would
make a shared, comparatively small Airflow environment a bottleneck and
single point of failure for a workload EMR is specifically sized to handle
(ADR-003).

### Reference implementation

- `emr/jobs/historical_backfill.py`, `emr/jobs/large_sales_transformation.py`
- `airflow/dags/sales_pipeline.py` (the explicit Glue -> EMR -> Reconciliation
  -> Redshift sequence required by Section 13)
- `config/base/sources.yaml: historical_sales_backfill`
- Demo: `make demo-emr-backfill`

### Backfill semantics

Backfill is not a separate code path — `sales_pipeline`'s tasks are
identical whether triggered on the daily schedule or via
`dag_run.conf={"start_date": ..., "end_date": ...}` for a historical window.
See `airflow/README.md`.
