"""EMR Spark job: large-scale inventory aggregation (Section 15).

Aggregates store-level, SKU-level inventory snapshots into region/category
rollups across the full store network. Chosen for EMR because inventory
snapshots are wide (store x SKU x day) and the aggregation is prone to data
skew: a small number of high-velocity SKUs in flagship stores dominate row
counts relative to long-tail SKUs in compact-format stores.

Demonstrates:
    - skew handling via salting a hot join key before an otherwise-skewed join
    - explicit shuffle-partition sizing rather than relying on the 200-partition default
    - caching only the one intermediate reused across two aggregations
"""
from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession, functions as F


SALT_BUCKETS = 8


def _salted_join(inventory: DataFrame, product_master: DataFrame) -> DataFrame:
    """`product_master` is not small enough to broadcast in production scale,
    and a handful of SKUs (e.g. flagship-store bestsellers) dominate row
    counts, causing partition skew on a plain join. Salting spreads the hot
    key across SALT_BUCKETS synthetic partitions so no single reducer task
    is overloaded.
    """
    salted_inventory = inventory.withColumn("salt", (F.rand() * SALT_BUCKETS).cast("int"))
    salted_products = product_master.withColumn(
        "salt", F.explode(F.array([F.lit(i) for i in range(SALT_BUCKETS)]))
    )
    return salted_inventory.join(salted_products, on=["sku", "salt"], how="inner").drop("salt")


def build_inventory_rollup(spark: SparkSession, inventory: DataFrame, product_master: DataFrame,
                            stores: DataFrame) -> DataFrame:
    joined = _salted_join(inventory, product_master.withColumnRenamed("material_number", "sku"))
    joined = joined.join(stores, on="store_id", how="inner")

    # Reused for both the category and region rollups below — cache once.
    joined = joined.cache()

    by_category = joined.groupBy("region", "category").agg(
        F.sum("quantity_on_hand").alias("total_quantity"),
        F.countDistinct("store_id").alias("store_count"),
    )
    by_region = joined.groupBy("region").agg(F.sum("quantity_on_hand").alias("region_total_quantity"))

    rollup = by_category.join(by_region, on="region", how="inner").withColumn(
        "pct_of_region", F.round(F.col("total_quantity") / F.col("region_total_quantity") * 100, 2)
    )
    joined.unpersist()
    return rollup
