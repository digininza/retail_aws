-- SCD Type 2 merge for dim_customer (Section 10, Section 18).
-- Mirrors src/dimensional_model/scd_type2.py::apply_scd2 exactly — same
-- three cases (new / no-change / changed) — so the Python reference
-- implementation and this SQL implementation cannot silently drift apart in
-- an interview explanation. Assumes `staging.stg_customers_cdc` has already
-- been through CDC dedup (Section 8) before this MERGE runs.

BEGIN TRANSACTION;

-- 1. Expire current rows whose tracked attributes changed.
UPDATE curated.dim_customer d
SET is_current = FALSE,
    effective_end_date = DATEADD(day, -1, CURRENT_DATE)
FROM staging.stg_customers_cdc s
WHERE d.customer_id = s.customer_id
  AND d.is_current = TRUE
  AND d.record_hash <> s.record_hash;

-- 2. Insert new current rows for: (a) brand-new customers, (b) customers
--    whose current row was just expired in step 1.
INSERT INTO curated.dim_customer (
    customer_id, customer_name, email, segment, city,
    effective_start_date, effective_end_date, is_current, record_hash
)
SELECT
    s.customer_id, s.customer_name, s.email, s.segment, s.city,
    CURRENT_DATE, NULL, TRUE, s.record_hash
FROM staging.stg_customers_cdc s
LEFT JOIN curated.dim_customer d
  ON d.customer_id = s.customer_id AND d.is_current = TRUE
WHERE d.customer_id IS NULL         -- brand-new customer, OR
   OR d.record_hash <> s.record_hash; -- attribute change (row was just expired above)

-- Rows where d.record_hash = s.record_hash (no change) contribute nothing —
-- this is the "EXISTING + NO CHANGE -> do nothing" case from Section 10,
-- and is what makes a rerun of the same staging batch idempotent: the WHERE
-- clause above naturally excludes unchanged customers on every run.

COMMIT;
