"""fact_sales construction: joins conformed order/order_item data against
dimension surrogate keys (Section 18). Runs after DQ + reconciliation have
passed for the underlying orders/order_items batches.
"""
from __future__ import annotations

import pandas as pd


def build_fact_sales(
    orders: pd.DataFrame,
    order_items: pd.DataFrame,
    dim_customer_current: pd.DataFrame,
    dim_store: pd.DataFrame,
    dim_date: pd.DataFrame,
) -> pd.DataFrame:
    """Grain: one row per order_item. Surrogate keys are resolved via lookups
    against the *current* dimension snapshot at fact-build time; for SCD2
    dim_customer, a historical/point-in-time join would instead filter on
    effective_start_date <= order_date < effective_end_date — omitted here
    for clarity, documented in docs/lld/03_scd2_lld.md.
    """
    fact = order_items.merge(orders, on="order_id", how="inner", suffixes=("_item", "_order"))

    fact = fact.merge(
        dim_customer_current[["customer_sk", "customer_id"]], on="customer_id", how="left",
    )
    if "store_id" in fact.columns and not dim_store.empty:
        fact = fact.merge(dim_store[["store_sk", "store_id"]], on="store_id", how="left")
    else:
        fact["store_sk"] = None

    fact["order_date"] = pd.to_datetime(fact["order_date"])
    fact["date_sk"] = fact["order_date"].dt.strftime("%Y%m%d").astype(int)

    fact["line_amount"] = fact["quantity"] * fact["unit_price"] * (1 - fact.get("discount_pct", 0).fillna(0) / 100)

    columns = ["order_id", "order_item_id", "date_sk", "customer_sk", "store_sk", "sku",
               "quantity", "unit_price", "line_amount"]
    return fact[[c for c in columns if c in fact.columns]]
