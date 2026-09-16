"""Control-total and key-set reconciliation (Section 12):
- CONTROL_TOTAL: source transaction amount vs. target transaction amount
  (an alias of aggregate_check with SUM, kept distinct for readability in
  finance-facing reconciliation reports).
- KEY_RECONCILIATION: source distinct keys vs. target distinct keys, useful
  when counts could coincidentally match but the actual key sets differ
  (e.g. one dropped + one duplicated row).
"""
from __future__ import annotations

from dataclasses import dataclass, field

import pandas as pd


@dataclass
class ControlTotalResult:
    check_type: str = "CONTROL_TOTAL"
    column: str = ""
    source_total: float = 0.0
    target_total: float = 0.0
    variance_pct: float = 0.0
    tolerance_pct: float = 0.0
    passed: bool = True


def check_control_total(source_df: pd.DataFrame, target_df: pd.DataFrame, column: str,
                         tolerance_pct: float = 0.01) -> ControlTotalResult:
    source_total = float(pd.to_numeric(source_df.get(column, pd.Series(dtype=float)), errors="coerce").sum())
    target_total = float(pd.to_numeric(target_df.get(column, pd.Series(dtype=float)), errors="coerce").sum())
    variance_pct = abs(target_total - source_total) / abs(source_total) * 100 if source_total else (
        0.0 if target_total == 0 else 100.0
    )
    return ControlTotalResult(
        column=column, source_total=source_total, target_total=target_total,
        variance_pct=variance_pct, tolerance_pct=tolerance_pct, passed=variance_pct <= tolerance_pct,
    )


@dataclass
class KeyReconciliationResult:
    check_type: str = "KEY_RECONCILIATION"
    key: str = ""
    missing_in_target: set = field(default_factory=set)
    unexpected_in_target: set = field(default_factory=set)
    passed: bool = True


def check_key_reconciliation(source_df: pd.DataFrame, target_df: pd.DataFrame, key: str) -> KeyReconciliationResult:
    source_keys = set(source_df[key].dropna()) if key in source_df.columns else set()
    target_keys = set(target_df[key].dropna()) if key in target_df.columns else set()
    missing = source_keys - target_keys
    unexpected = target_keys - source_keys
    return KeyReconciliationResult(
        key=key, missing_in_target=missing, unexpected_in_target=unexpected,
        passed=(not missing and not unexpected),
    )
