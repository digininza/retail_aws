-- Inventory turnover: units sold in a trailing window divided by average
-- inventory on hand over the same window, by store/product (Section 18).
CREATE MATERIALIZED VIEW marts.mart_inventory_turnover AS
WITH sales_30d AS (
    SELECT ds.store_id, fs.sku, SUM(fs.quantity) AS units_sold_30d
    FROM curated.fact_sales fs
    JOIN curated.dim_date dd ON dd.date_sk = fs.date_sk
    JOIN curated.dim_store ds ON ds.store_sk = fs.store_sk
    WHERE dd.full_date >= DATEADD(day, -30, CURRENT_DATE)
    GROUP BY ds.store_id, fs.sku
),
avg_inventory AS (
    SELECT store_id, sku, AVG(quantity_on_hand) AS avg_qty_on_hand
    FROM (
        SELECT ds.store_id, dp.product_id AS sku, fi.quantity_on_hand
        FROM curated.fact_inventory fi
        JOIN curated.dim_store ds ON ds.store_sk = fi.store_sk
        JOIN curated.dim_product dp ON dp.product_sk = fi.product_sk
        WHERE fi.snapshot_date >= DATEADD(day, -30, CURRENT_DATE)
    ) recent_inventory
    GROUP BY store_id, sku
)
SELECT
    s.store_id,
    s.sku,
    s.units_sold_30d,
    i.avg_qty_on_hand,
    CASE WHEN i.avg_qty_on_hand > 0 THEN ROUND(s.units_sold_30d::DECIMAL / i.avg_qty_on_hand, 2) END AS turnover_ratio_30d
FROM sales_30d s
JOIN avg_inventory i ON i.store_id = s.store_id AND i.sku = s.sku;
