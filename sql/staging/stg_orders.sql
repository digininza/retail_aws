-- Staging load for orders (Section 18). Staging is TRUNCATE + COPY per run —
-- staging tables are never the target of a MERGE; they exist only to give
-- Redshift a set-based source for the MERGE into curated.fact_sales
-- (Section 23: transaction/commit strategy — staging is the "temporary
-- location", curated.* is only written to after DQ + reconciliation pass).

CREATE SCHEMA IF NOT EXISTS staging;

CREATE TABLE IF NOT EXISTS staging.stg_orders (
    order_id        VARCHAR(50)  NOT NULL,
    customer_id      VARCHAR(50),
    order_date        TIMESTAMP,
    amount              DECIMAL(12,2),
    status               VARCHAR(20),
    store_id              VARCHAR(50),
    promotion_id           VARCHAR(50),
    channel                  VARCHAR(20),
    run_id                     VARCHAR(64) NOT NULL
);

TRUNCATE TABLE staging.stg_orders;

COPY staging.stg_orders (order_id, customer_id, order_date, amount, status, store_id, promotion_id, channel, run_id)
FROM 's3://retail-data-{environment}/gold/redshift_staging/stg_orders/'
IAM_ROLE 'arn:aws:iam::{account_id}:role/retail-{environment}-redshift-load-role'
FORMAT AS PARQUET;

-- Sanity check before MERGE: staging row count should be > 0 for an
-- INCREMENTAL pipeline that reported records_written > 0 in the control
-- table for this run_id — enforced by the reconciliation framework, not by
-- this script, but documented here as the expected invariant.
