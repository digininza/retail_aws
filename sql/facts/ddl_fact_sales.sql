CREATE TABLE IF NOT EXISTS curated.fact_sales (
    order_id        VARCHAR(50)  NOT NULL,
    order_item_id      VARCHAR(50)  NOT NULL,
    date_sk               INTEGER      NOT NULL REFERENCES curated.dim_date(date_sk),
    customer_sk              BIGINT       REFERENCES curated.dim_customer(customer_sk),
    store_sk                    BIGINT       REFERENCES curated.dim_store(store_sk),
    sku                            VARCHAR(50),
    quantity                          INTEGER,
    unit_price                          DECIMAL(12,2),
    line_amount                            DECIMAL(12,2),
    run_id                                    VARCHAR(64) NOT NULL,
    PRIMARY KEY (order_id, order_item_id)
)
DISTSTYLE KEY
DISTKEY (customer_sk)          -- co-locates with dim_customer for the most common join
SORTKEY (date_sk, store_sk);   -- most analytical queries filter/aggregate by date and store

CREATE TABLE IF NOT EXISTS curated.fact_inventory (
    snapshot_date    DATE NOT NULL,
    store_sk            BIGINT REFERENCES curated.dim_store(store_sk),
    product_sk              BIGINT REFERENCES curated.dim_product(product_sk),
    quantity_on_hand           INTEGER,
    run_id                        VARCHAR(64) NOT NULL,
    PRIMARY KEY (snapshot_date, store_sk, product_sk)
)
DISTSTYLE KEY
DISTKEY (store_sk)
SORTKEY (snapshot_date, store_sk);
