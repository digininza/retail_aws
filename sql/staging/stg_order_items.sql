CREATE TABLE IF NOT EXISTS staging.stg_order_items (
    order_item_id   VARCHAR(50) NOT NULL,
    order_id          VARCHAR(50) NOT NULL,
    sku                  VARCHAR(50),
    quantity               INTEGER,
    unit_price               DECIMAL(12,2),
    discount_pct               DECIMAL(5,2),
    run_id                        VARCHAR(64) NOT NULL
);

TRUNCATE TABLE staging.stg_order_items;

COPY staging.stg_order_items (order_item_id, order_id, sku, quantity, unit_price, discount_pct, run_id)
FROM 's3://retail-data-{environment}/gold/redshift_staging/stg_order_items/'
IAM_ROLE 'arn:aws:iam::{account_id}:role/retail-{environment}-redshift-load-role'
FORMAT AS PARQUET;
