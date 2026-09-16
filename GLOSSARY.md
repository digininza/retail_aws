# Glossary

| Term | Definition |
|---|---|
| **Watermark** | The highest source timestamp/key successfully processed and committed downstream; the starting point for the next incremental extract. |
| **Control table** | Relational metadata table tracking per-pipeline run state: watermark, run status, record counts, error message. Separate from data, never overwritten by a failed run. |
| **CDC (Change Data Capture)** | A stream of I/U/D operations representing row-level source changes, as opposed to a plain "what changed since watermark" snapshot diff. |
| **SCD Type 2** | Slowly Changing Dimension pattern that preserves history by expiring the current row and inserting a new one on a tracked-attribute change. |
| **Idempotency** | Property that reprocessing the same input (identified by a stable idempotency key) produces the same final state, never duplicate effects. |
| **Merge/Upsert** | Insert-if-absent, update-if-present logic keyed on a deterministic business key. |
| **Bronze/Silver/Gold** | Data lake maturity zones: Bronze = raw-typed/deduplicated, Silver = cleansed/conformed, Gold = business-ready/aggregated. |
| **Quarantine** | Zone holding records that failed data-quality validation, with rule/run metadata attached, for investigation and reprocessing. |
| **Reconciliation** | Verification that the expected volume/value of data moved correctly between stages (counts, sums, control totals) — distinct from data *validity*. |
| **Data Quality (DQ)** | Rule-based validation of record-level correctness (nulls, uniqueness, ranges, referential integrity, schema). |
| **Run ID** | Unique identifier for one pipeline execution, used to correlate control-table, audit, DQ, and reconciliation records. |
| **Business key** | The natural identifier for an entity in the source system (e.g., `customer_id`), independent of any warehouse-generated surrogate key. |
| **Surrogate key** | Warehouse-generated key (e.g., `customer_sk`) uniquely identifying a dimension row/version, decoupled from the business key so SCD2 history can exist. |
| **Late-arriving record** | A record whose business event occurred before the current watermark but which arrives after it was advanced; handled via reprocessing windows / CDC replay. |
| **Backfill** | Reprocessing of historical data, typically via EMR, outside the normal incremental cadence. |
| **Job Bookmark** | AWS Glue's built-in incremental-read tracking mechanism; distinct from and not a substitute for this project's custom control-table watermark (see LLD on Glue). |
| **Breaking schema change** | A schema change (type change, removed required column, incompatible rename) that must halt the affected pipeline rather than degrade silently. |
| **Exactly-once** | A delivery guarantee not claimed anywhere in this project; streaming consumers are documented as at-least-once with idempotent downstream writes. |
