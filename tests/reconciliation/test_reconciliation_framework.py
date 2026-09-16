import pandas as pd
import pytest

from src.common.exceptions import ReconciliationError
from src.reconciliation import framework as recon_framework
from src.reconciliation.record_count import check_record_count
from src.reconciliation.aggregate_check import check_aggregate
from src.reconciliation.control_totals import check_control_total, check_key_reconciliation


def test_record_count_passes_within_tolerance():
    result = check_record_count(source_count=100, target_count=100, tolerance_pct=0)
    assert result.passed


def test_record_count_fails_outside_tolerance():
    result = check_record_count(source_count=100, target_count=95, tolerance_pct=0)
    assert not result.passed
    assert result.variance == -5


def test_aggregate_sum_within_tolerance_passes():
    source = pd.DataFrame({"amount": [10, 20, 30]})
    target = pd.DataFrame({"amount": [10, 20, 30.001]})
    result = check_aggregate(source, target, "amount", "SUM", tolerance_pct=0.01)
    assert result.passed


def test_control_total_detects_missing_value():
    source = pd.DataFrame({"po_total": [100, 200, 300]})
    target = pd.DataFrame({"po_total": [100, 200]})  # one PO missing downstream
    result = check_control_total(source, target, "po_total", tolerance_pct=0.01)
    assert not result.passed


def test_key_reconciliation_detects_missing_and_unexpected_keys():
    source = pd.DataFrame({"customer_id": ["C1", "C2", "C3"]})
    target = pd.DataFrame({"customer_id": ["C1", "C2", "C4"]})
    result = check_key_reconciliation(source, target, "customer_id")
    assert result.missing_in_target == {"C3"}
    assert result.unexpected_in_target == {"C4"}
    assert not result.passed


def test_full_run_reconciliation_passes_for_matching_data():
    profile = {"checks": [{"type": "RECORD_COUNT", "tolerance_pct": 0}]}
    df = pd.DataFrame({"order_id": ["O1", "O2"], "amount": [10, 20]})
    result = recon_framework.run_reconciliation(df, df.copy(), profile, run_id="r1", pipeline_name="orders")
    assert result.passed


def test_full_run_reconciliation_fails_and_enforce_raises():
    profile = {"checks": [{"type": "RECORD_COUNT", "tolerance_pct": 0}]}
    source = pd.DataFrame({"order_id": ["O1", "O2", "O3"], "amount": [10, 20, 30]})
    target = source.iloc[:2]  # dropped a row downstream
    result = recon_framework.run_reconciliation(source, target, profile, run_id="r2", pipeline_name="orders")
    assert not result.passed
    with pytest.raises(ReconciliationError):
        recon_framework.enforce(result)
