-- dim_customer: the platform's one SCD Type 2 dimension (Section 10, ADR-006).
CREATE SCHEMA IF NOT EXISTS curated;

CREATE TABLE IF NOT EXISTS curated.dim_customer (
    customer_sk           BIGINT IDENTITY(1,1) PRIMARY KEY,
    customer_id             VARCHAR(50)  NOT NULL,
    customer_name             VARCHAR(200),
    email                        VARCHAR(200),
    segment                        VARCHAR(20),
    city                              VARCHAR(100),
    effective_start_date               DATE NOT NULL,
    effective_end_date                   DATE,
    is_current                             BOOLEAN NOT NULL DEFAULT TRUE,
    record_hash                              VARCHAR(64) NOT NULL
)
DISTSTYLE KEY
DISTKEY (customer_id)
SORTKEY (customer_id, effective_start_date);

-- Only one current row per business key — enforced by application logic
-- (src/dimensional_model/scd_type2.py) since Redshift does not support
-- partial/filtered unique constraints; documented here as the invariant a
-- reconciliation check should periodically verify:
--   SELECT customer_id, COUNT(*) FROM curated.dim_customer
--   WHERE is_current GROUP BY customer_id HAVING COUNT(*) > 1;
-- (expected: zero rows)
