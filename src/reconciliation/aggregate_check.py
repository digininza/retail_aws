"""Aggregate reconciliation: e.g. source SUM(amount) == target SUM(amount)
(Section 12)."""
from __future__ import annotations

from dataclasses import dataclass

import pandas as pd


@dataclass
class AggregateCheckResult:
    check_type: str = "AGGREGATE"
    column: str = ""
    agg: str = "SUM"
    source_value: float = 0.0
    target_value: float = 0.0
    variance_pct: float = 0.0
    tolerance_pct: float = 0.0
    passed: bool = True


_AGGS = {"SUM": lambda s: s.sum(), "AVG": lambda s: s.mean(), "COUNT": lambda s: s.count()}


def check_aggregate(source_df: pd.DataFrame, target_df: pd.DataFrame, column: str,
                     agg: str = "SUM", tolerance_pct: float = 0.01) -> AggregateCheckResult:
    fn = _AGGS[agg]
    source_value = float(fn(pd.to_numeric(source_df[column], errors="coerce"))) if column in source_df.columns else 0.0
    target_value = float(fn(pd.to_numeric(target_df[column], errors="coerce"))) if column in target_df.columns else 0.0
    variance_pct = abs(target_value - source_value) / abs(source_value) * 100 if source_value else (
        0.0 if target_value == 0 else 100.0
    )
    return AggregateCheckResult(
        column=column, agg=agg, source_value=source_value, target_value=target_value,
        variance_pct=variance_pct, tolerance_pct=tolerance_pct, passed=variance_pct <= tolerance_pct,
    )
