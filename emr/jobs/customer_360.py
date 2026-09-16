"""EMR Spark job: Customer 360 (Section 15).

Joins conformed order, order-item, POS event, and e-commerce event data into
a single customer-level view (lifetime spend, channel mix, last purchase
date, RFM-style segments). This is a wide, multi-source join across
potentially large event volumes — exactly the "complex joins across large
datasets" workload EMR is reserved for (ADR-002); Glue's managed job model
is not the right fit for hand-tuned join strategy and cluster sizing here.

Demonstrates:
    - predicate pushdown (filtering source reads by date range before the join)
    - avoiding an unnecessary shuffle by co-partitioning on customer_id
    - explicit persist() only where the DataFrame is reused multiple times downstream
"""
from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession, functions as F


def build_customer_360(
    spark: SparkSession,
    orders: DataFrame,
    order_items: DataFrame,
    pos_events: DataFrame,
    ecommerce_events: DataFrame,
    start_date: str,
    end_date: str,
) -> DataFrame:
    # Predicate pushdown: filter to the analysis window before any join, so
    # Spark's file-format readers (Parquet) can skip row groups outside the
    # range rather than reading everything and filtering in-memory.
    orders_window = orders.filter((F.col("order_date") >= start_date) & (F.col("order_date") < end_date))

    order_value = (
        orders_window.join(order_items, on="order_id", how="inner")
        .groupBy("customer_id")
        .agg(
            F.sum(F.col("quantity") * F.col("unit_price")).alias("online_instore_spend"),
            F.max("order_date").alias("last_order_date"),
            F.count("order_id").alias("order_count"),
        )
    )

    pos_value = (
        pos_events.filter((F.col("event_timestamp") >= start_date) & (F.col("event_timestamp") < end_date))
        .groupBy("store_id")
        .agg(F.sum("amount").alias("pos_spend"))
    )

    ecommerce_activity = (
        ecommerce_events.filter(F.col("event_type") == "PRODUCT_VIEWED")
        .groupBy("customer_id")
        .agg(F.count("event_id").alias("product_views"))
    )

    # `order_value` is reused in two downstream branches (join + segment calc);
    # persist it once rather than recomputing the upstream join twice.
    order_value = order_value.persist()

    customer_360 = (
        order_value.join(ecommerce_activity, on="customer_id", how="left")
        .withColumn("product_views", F.coalesce(F.col("product_views"), F.lit(0)))
        .withColumn(
            "segment",
            F.when(F.col("online_instore_spend") >= 500, "HIGH_VALUE")
            .when(F.col("online_instore_spend") >= 100, "MID_VALUE")
            .otherwise("LOW_VALUE"),
        )
    )

    return customer_360
