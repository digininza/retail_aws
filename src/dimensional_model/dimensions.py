"""Type-1 (overwrite) dimension builders for dim_product, dim_store,
dim_promotion, dim_date. Only dim_customer uses SCD Type 2 (see
scd_type2.py) — the others change rarely enough, or their history isn't
analytically meaningful enough, to justify SCD2 overhead. This asymmetry is
intentional; see docs/adr/ADR-006-scd-type2.md for the rationale on which
dimensions get history tracking.
"""
from __future__ import annotations

import pandas as pd


def build_dim_product(product_master: pd.DataFrame) -> pd.DataFrame:
    dim = product_master.rename(columns={"material_number": "product_id"}).copy()
    dim["product_sk"] = range(1, len(dim) + 1)
    return dim[["product_sk", "product_id", "product_name", "category"]]


def build_dim_store(stores: pd.DataFrame) -> pd.DataFrame:
    dim = stores.copy()
    dim["store_sk"] = range(1, len(dim) + 1)
    cols = ["store_sk", "store_id", "store_name", "region"]
    return dim[[c for c in cols if c in dim.columns]]


def build_dim_promotion(promotions: pd.DataFrame) -> pd.DataFrame:
    dim = promotions.copy()
    dim["promotion_sk"] = range(1, len(dim) + 1)
    cols = ["promotion_sk", "promotion_id", "discount_pct"]
    return dim[[c for c in cols if c in dim.columns]]


def build_dim_date(start: str, end: str) -> pd.DataFrame:
    """Standard calendar dimension — generated, not sourced, since date is a
    closed, fully-known domain."""
    dates = pd.date_range(start=start, end=end, freq="D")
    return pd.DataFrame({
        "date_sk": dates.strftime("%Y%m%d").astype(int),
        "full_date": dates.date,
        "year": dates.year,
        "quarter": dates.quarter,
        "month": dates.month,
        "day": dates.day,
        "day_of_week": dates.day_name(),
        "is_weekend": dates.dayofweek >= 5,
    })
