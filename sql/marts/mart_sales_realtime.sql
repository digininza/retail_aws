-- Near-real-time sales mart fed by the Kinesis -> Lambda -> S3 raw ->
-- Glue micro-batch path (Section 16, Scenario 3). Refreshed on a short
-- schedule (e.g. every 5-15 minutes via Airflow) rather than continuously —
-- this project does not claim true streaming-SQL/materialized-continuously
-- semantics, only frequent micro-batch refresh (Section 16: no exactly-once
-- claim; this mart can show a duplicate POS event until the next refresh's
-- dedup pass removes it).
CREATE TABLE IF NOT EXISTS marts.mart_sales_realtime (
    store_id       VARCHAR(50),
    event_hour        TIMESTAMP,
    units_sold           INTEGER,
    total_sales             DECIMAL(12,2),
    last_refreshed_at         TIMESTAMP DEFAULT GETDATE()
);

-- Refresh statement (run by airflow/dags/sales_pipeline.py on a schedule):
DELETE FROM marts.mart_sales_realtime WHERE event_hour >= DATEADD(hour, -24, CURRENT_TIMESTAMP);

INSERT INTO marts.mart_sales_realtime (store_id, event_hour, units_sold, total_sales)
SELECT
    store_id,
    DATE_TRUNC('hour', event_timestamp) AS event_hour,
    SUM(quantity)  AS units_sold,
    SUM(amount)       AS total_sales
FROM staging.stg_pos_events_realtime
WHERE event_timestamp >= DATEADD(hour, -24, CURRENT_TIMESTAMP)
GROUP BY store_id, DATE_TRUNC('hour', event_timestamp);
