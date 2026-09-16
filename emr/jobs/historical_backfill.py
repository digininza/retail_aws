"""EMR Spark job: historical sales backfill (Section 15, Scenario 2 in DATA_FLOW.md).

Why EMR and not Glue: this workload reprocesses multiple years of historical
order/order-item data in one pass, requiring full control over shuffle
partitioning, join strategy, and cluster sizing that Glue's managed job
model abstracts away. See ADR-002.

Run locally (stands in for a real EMR step) with:
    python emr/jobs/historical_backfill.py --local

Demonstrates (Section 15):
    - explicit partitioning by order_year/order_month (partition pruning on read)
    - column pruning (select only needed columns before the join)
    - broadcast join for the small dim_store lookup
    - repartition before a wide aggregation to avoid a single-task bottleneck
    - AQE enabled to coalesce post-shuffle partitions automatically
    - avoiding small-file explosion by coalescing before write
"""
from __future__ import annotations

import argparse
from pathlib import Path

from pyspark.sql import SparkSession, functions as F

REPO_ROOT = Path(__file__).resolve().parents[2]


def build_spark(local: bool) -> SparkSession:
    builder = SparkSession.builder.appName("retail-historical-sales-backfill")
    if local:
        builder = builder.master("local[*]")
    return (
        builder
        # AQE: let Spark dynamically coalesce shuffle partitions instead of a
        # hand-tuned static number, avoiding both small-file explosion and
        # long-tail stragglers on skewed partitions.
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.sql.shuffle.partitions", "8")  # small for local demo; sized for cluster in real EMR
        .getOrCreate()
    )


def run(local: bool) -> None:
    spark = build_spark(local)

    # Column pruning: only select the columns the join/aggregation actually needs.
    historical_sales = (
        spark.read.option("header", "true").option("inferSchema", "true")
        .csv(str(REPO_ROOT / "sample_data" / "orders" / "historical_sales_sample.csv"))
        .select("order_id", "order_item_id", "customer_id", "store_id", "sku",
                "quantity", "unit_price", "amount", "order_date", "order_year", "order_month")
    )

    # Negative-quantity / null-key rows are exactly what DQ should catch before
    # Gold promotion (Section 11) — filtered here only to keep the aggregation
    # demonstration numerically sane; the real pipeline path is
    # DQ framework -> quarantine, not a silent filter.
    clean_sales = historical_sales.filter((F.col("customer_id").isNotNull()) & (F.col("quantity") > 0))

    stores = spark.read.option("header", "true").csv(str(REPO_ROOT / "sample_data" / "products" / "stores.csv"))

    # Broadcast join: `stores` is small (dozens of rows) relative to historical
    # sales (potentially billions in production), so broadcasting avoids a
    # shuffle of the large side entirely.
    joined = clean_sales.join(F.broadcast(stores), on="store_id", how="left")

    # Repartition by the natural aggregation key before a wide groupBy so work
    # is spread evenly across executors rather than funneling through Spark's
    # default hash partitioning, which can concentrate skewed keys.
    aggregated = (
        joined.repartition("region", "order_year", "order_month")
        .groupBy("region", "order_year", "order_month")
        .agg(
            F.sum("amount").alias("total_sales"),
            F.countDistinct("order_id").alias("order_count"),
            F.countDistinct("customer_id").alias("distinct_customers"),
        )
    )

    output_path = REPO_ROOT / ".local_s3" / "gold" / "historical_sales_by_region_month"
    # Coalesce before write to avoid the small-file explosion that a
    # high-shuffle-partition-count write would otherwise produce.
    (
        aggregated.coalesce(2)
        .write.mode("overwrite")
        .partitionBy("order_year", "order_month")  # partition pruning benefits downstream readers
        .parquet(str(output_path))
    )

    print(f"Historical backfill complete. Wrote aggregated output to {output_path}")
    aggregated.show(truncate=False)

    spark.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--local", action="store_true", help="Run with a local Spark master instead of an EMR cluster")
    args = parser.parse_args()
    run(local=args.local)
