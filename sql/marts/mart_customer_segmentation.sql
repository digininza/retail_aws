-- Customer segmentation mart: RFM-style spend/recency banding, joined to the
-- SCD2 current customer profile (Section 18).
CREATE MATERIALIZED VIEW marts.mart_customer_segmentation AS
WITH customer_activity AS (
    SELECT
        fs.customer_sk,
        SUM(fs.line_amount)         AS lifetime_spend,
        COUNT(DISTINCT fs.order_id) AS order_count,
        MAX(dd.full_date)              AS last_order_date,
        DATEDIFF(day, MAX(dd.full_date), CURRENT_DATE) AS days_since_last_order
    FROM curated.fact_sales fs
    JOIN curated.dim_date dd ON dd.date_sk = fs.date_sk
    GROUP BY fs.customer_sk
)
SELECT
    dc.customer_id,
    dc.customer_name,
    dc.segment AS source_segment,
    ca.lifetime_spend,
    ca.order_count,
    ca.last_order_date,
    ca.days_since_last_order,
    CASE
        WHEN ca.lifetime_spend >= 1000 AND ca.days_since_last_order <= 30 THEN 'HIGH_VALUE_ACTIVE'
        WHEN ca.lifetime_spend >= 1000 AND ca.days_since_last_order > 90 THEN 'HIGH_VALUE_AT_RISK'
        WHEN ca.lifetime_spend < 100 THEN 'LOW_VALUE'
        ELSE 'STANDARD'
    END AS computed_segment
FROM curated.dim_customer dc
JOIN customer_activity ca ON ca.customer_sk = dc.customer_sk
WHERE dc.is_current = TRUE;
