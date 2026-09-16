-- MERGE/upsert into fact_sales (Section 9, Section 18). Keyed on the
-- deterministic composite business key (order_id, order_item_id) — never on
-- a load timestamp — so a rerun of the same run_id's staging batch is a
-- no-op overwrite, not a duplicate insert (Section 9's idempotency rule).

BEGIN TRANSACTION;

MERGE INTO curated.fact_sales AS target
USING (
    SELECT
        si.order_item_id,
        si.order_id,
        CAST(TO_CHAR(so.order_date, 'YYYYMMDD') AS INTEGER) AS date_sk,
        dc.customer_sk,
        ds.store_sk,
        si.sku,
        si.quantity,
        si.unit_price,
        si.quantity * si.unit_price * (1 - COALESCE(si.discount_pct, 0) / 100.0) AS line_amount,
        si.run_id
    FROM staging.stg_order_items si
    JOIN staging.stg_orders so ON so.order_id = si.order_id
    LEFT JOIN curated.dim_customer dc ON dc.customer_id = so.customer_id AND dc.is_current = TRUE
    LEFT JOIN curated.dim_store ds ON ds.store_id = so.store_id
) AS source
ON target.order_id = source.order_id AND target.order_item_id = source.order_item_id
WHEN MATCHED THEN UPDATE SET
    date_sk = source.date_sk,
    customer_sk = source.customer_sk,
    store_sk = source.store_sk,
    sku = source.sku,
    quantity = source.quantity,
    unit_price = source.unit_price,
    line_amount = source.line_amount,
    run_id = source.run_id
WHEN NOT MATCHED THEN INSERT (
    order_id, order_item_id, date_sk, customer_sk, store_sk, sku, quantity, unit_price, line_amount, run_id
) VALUES (
    source.order_id, source.order_item_id, source.date_sk, source.customer_sk, source.store_sk,
    source.sku, source.quantity, source.unit_price, source.line_amount, source.run_id
);

COMMIT;
