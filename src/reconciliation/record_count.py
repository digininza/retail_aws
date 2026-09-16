"""Record-count reconciliation: source count == target count (Section 12)."""
from __future__ import annotations

from dataclasses import dataclass


@dataclass
class CountCheckResult:
    check_type: str = "RECORD_COUNT"
    source_count: int = 0
    target_count: int = 0
    variance: int = 0
    variance_pct: float = 0.0
    tolerance_pct: float = 0.0
    passed: bool = True


def check_record_count(source_count: int, target_count: int, tolerance_pct: float = 0.0) -> CountCheckResult:
    variance = target_count - source_count
    variance_pct = abs(variance) / source_count * 100 if source_count else (0.0 if target_count == 0 else 100.0)
    return CountCheckResult(
        source_count=source_count,
        target_count=target_count,
        variance=variance,
        variance_pct=variance_pct,
        tolerance_pct=tolerance_pct,
        passed=variance_pct <= tolerance_pct,
    )
