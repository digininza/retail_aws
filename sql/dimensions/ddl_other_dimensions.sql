-- Type-1 (overwrite) dimensions: dim_product, dim_store, dim_promotion,
-- dim_date. See src/dimensional_model/dimensions.py for the equivalent
-- pandas builders and ADR-006 for why only dim_customer gets SCD2 history.

CREATE TABLE IF NOT EXISTS curated.dim_product (
    product_sk    BIGINT IDENTITY(1,1) PRIMARY KEY,
    product_id      VARCHAR(50) NOT NULL,
    product_name      VARCHAR(200),
    category             VARCHAR(50)
)
DISTSTYLE ALL   -- small, frequently-joined dimension: replicate to every node
SORTKEY (product_id);

CREATE TABLE IF NOT EXISTS curated.dim_store (
    store_sk     BIGINT IDENTITY(1,1) PRIMARY KEY,
    store_id       VARCHAR(50) NOT NULL,
    store_name       VARCHAR(200),
    region              VARCHAR(20)
)
DISTSTYLE ALL
SORTKEY (store_id);

CREATE TABLE IF NOT EXISTS curated.dim_promotion (
    promotion_sk   BIGINT IDENTITY(1,1) PRIMARY KEY,
    promotion_id     VARCHAR(50) NOT NULL,
    discount_pct        DECIMAL(5,2)
)
DISTSTYLE ALL
SORTKEY (promotion_id);

CREATE TABLE IF NOT EXISTS curated.dim_date (
    date_sk      INTEGER PRIMARY KEY,        -- YYYYMMDD
    full_date      DATE NOT NULL,
    year_number       SMALLINT,
    quarter_number      SMALLINT,
    month_number          SMALLINT,
    day_number               SMALLINT,
    day_of_week                 VARCHAR(10),
    is_weekend                     BOOLEAN
)
DISTSTYLE ALL
SORTKEY (date_sk);
