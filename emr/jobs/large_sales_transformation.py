"""EMR Spark job: large historical sales transformation into fact_sales
(Section 15, Section 27 HLD-2).

Builds the Gold-layer `fact_sales` table from Silver-layer orders/order_items
at historical scale, joining against dimension surrogate keys. This is the
EMR counterpart to `src.dimensional_model.fact_sales.build_fact_sales`
(which is engine-agnostic pandas logic used by the local demo); this module
is the Spark-native equivalent used when volume requires a cluster.

Demonstrates:
    - broadcast join for dim_store / dim_date (small, static dimensions)
    - sort-merge join left as default for the large orders x order_items join
      (both sides large — broadcasting would OOM the executor)
    - partitionBy on write for downstream partition pruning
"""
from __future__ import annotations

from pyspark.sql import DataFrame, functions as F


def build_fact_sales_spark(
    orders: DataFrame,
    order_items: DataFrame,
    dim_customer_current: DataFrame,
    dim_store: DataFrame,
    dim_date: DataFrame,
) -> DataFrame:
    # Large x large: leave as a standard shuffle (sort-merge) join — do NOT
    # broadcast either side.
    fact = order_items.join(orders, on="order_id", how="inner")

    # Small x large: broadcast the dimension side explicitly rather than
    # relying on Spark's auto-broadcast threshold, which can misfire on
    # inaccurate source statistics.
    fact = fact.join(F.broadcast(dim_customer_current.select("customer_sk", "customer_id")),
                      on="customer_id", how="left")
    fact = fact.join(F.broadcast(dim_store.select("store_sk", "store_id")), on="store_id", how="left")

    fact = fact.withColumn("date_sk", F.date_format("order_date", "yyyyMMdd").cast("int"))
    fact = fact.join(F.broadcast(dim_date.select("date_sk")), on="date_sk", how="left")

    fact = fact.withColumn(
        "line_amount",
        F.col("quantity") * F.col("unit_price") * (1 - F.coalesce(F.col("discount_pct"), F.lit(0)) / 100),
    )

    return fact.select("order_id", "order_item_id", "date_sk", "customer_sk", "store_sk", "sku",
                        "quantity", "unit_price", "line_amount")


def write_fact_sales(fact_df: DataFrame, output_path: str) -> None:
    # partitionBy(date-derived column) lets Redshift Spectrum / downstream
    # Spark readers prune partitions on typical "sales in date range" queries.
    (
        fact_df.withColumn("order_year", (F.col("date_sk") / 10000).cast("int"))
        .repartition("order_year")
        .write.mode("overwrite")
        .partitionBy("order_year")
        .parquet(output_path)
    )
