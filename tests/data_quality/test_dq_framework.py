import pandas as pd
import pytest

from src.common.exceptions import DataQualityError
from src.data_quality import framework as dq_framework

ORDERS_PROFILE = {
    "table": "orders",
    "rules": {
        "order_id": [{"type": "NOT_NULL", "severity": "CRITICAL"}, {"type": "UNIQUE", "severity": "CRITICAL"}],
        "amount": [{"type": "NOT_NULL", "severity": "CRITICAL"}, {"type": "RANGE", "min": 0, "max": 1000, "severity": "ERROR"}],
        "status": [{"type": "REGEX", "pattern": "^(PLACED|SHIPPED)$", "severity": "WARNING"}],
    },
}


def test_clean_batch_passes_all_rules():
    df = pd.DataFrame([
        {"order_id": "O1", "amount": 100, "status": "PLACED"},
        {"order_id": "O2", "amount": 200, "status": "SHIPPED"},
    ])
    result = dq_framework.run_data_quality(df, ORDERS_PROFILE, "orders_standard")
    assert result.passed
    assert not result.blocks_promotion


def test_critical_failure_blocks_promotion():
    df = pd.DataFrame([{"order_id": None, "amount": 100, "status": "PLACED"}])
    result = dq_framework.run_data_quality(df, ORDERS_PROFILE, "orders_standard")
    assert result.blocks_promotion
    with pytest.raises(DataQualityError):
        dq_framework.enforce(result)


def test_error_severity_does_not_block_promotion():
    df = pd.DataFrame([{"order_id": "O1", "amount": -50, "status": "PLACED"}])  # amount out of range: ERROR only
    result = dq_framework.run_data_quality(df, ORDERS_PROFILE, "orders_standard")
    assert not result.blocks_promotion
    assert len(result.error_failures) == 1
    dq_framework.enforce(result)  # must not raise


def test_duplicate_order_id_is_critical():
    df = pd.DataFrame([
        {"order_id": "O1", "amount": 100, "status": "PLACED"},
        {"order_id": "O1", "amount": 150, "status": "SHIPPED"},
    ])
    result = dq_framework.run_data_quality(df, ORDERS_PROFILE, "orders_standard")
    assert result.blocks_promotion


def test_warning_severity_never_blocks():
    df = pd.DataFrame([{"order_id": "O1", "amount": 100, "status": "UNKNOWN_STATUS"}])
    result = dq_framework.run_data_quality(df, ORDERS_PROFILE, "orders_standard")
    assert not result.blocks_promotion
    assert not result.passed  # overall not "passed" since a rule failed, but doesn't block


def test_null_key_negative_case():
    """Negative test (Section 29): a null business key must be caught, never
    silently promoted."""
    df = pd.DataFrame([{"order_id": None, "amount": None, "status": "PLACED"}])
    result = dq_framework.run_data_quality(df, ORDERS_PROFILE, "orders_standard")
    assert len(result.critical_failures) >= 2  # order_id NOT_NULL, amount NOT_NULL
