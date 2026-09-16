-- Analytical mart: sales by store/product/date (Section 18).
CREATE MATERIALIZED VIEW marts.mart_sales_by_store_product_date AS
SELECT
    dd.full_date,
    ds.store_id,
    ds.store_name,
    ds.region,
    dp.product_id,
    dp.product_name,
    dp.category,
    SUM(fs.quantity)      AS units_sold,
    SUM(fs.line_amount)   AS total_sales,
    COUNT(DISTINCT fs.order_id) AS order_count
FROM curated.fact_sales fs
JOIN curated.dim_date dd    ON dd.date_sk = fs.date_sk
JOIN curated.dim_store ds   ON ds.store_sk = fs.store_sk
JOIN curated.dim_product dp ON dp.product_id = fs.sku
GROUP BY dd.full_date, ds.store_id, ds.store_name, ds.region, dp.product_id, dp.product_name, dp.category;
