import pandas as pd
import pytest

from src.cdc.parser import parse_cdc_batch
from src.cdc.processor import deduplicate_cdc_batch
from src.common.exceptions import SchemaValidationError


def test_dedup_keeps_latest_change_per_key():
    df = pd.DataFrame([
        {"customer_id": "C1", "name": "A", "operation": "U", "change_timestamp": "2026-08-01T09:00:00"},
        {"customer_id": "C1", "name": "B", "operation": "U", "change_timestamp": "2026-08-01T10:00:00"},
    ])
    parsed = parse_cdc_batch(df, business_key="customer_id")
    result = deduplicate_cdc_batch(parsed, business_key="customer_id")
    assert len(result.updates) == 1
    assert result.updates.iloc[0]["name"] == "B"  # latest timestamp wins
    assert result.duplicate_events_dropped == 1


def test_dedup_handles_out_of_order_events():
    df = pd.DataFrame([
        {"customer_id": "C1", "name": "LATEST", "operation": "U", "change_timestamp": "2026-08-01T12:00:00"},
        {"customer_id": "C1", "name": "STALE", "operation": "U", "change_timestamp": "2026-08-01T08:00:00"},
    ])
    parsed = parse_cdc_batch(df, business_key="customer_id")
    result = deduplicate_cdc_batch(parsed, business_key="customer_id")
    assert result.updates.iloc[0]["name"] == "LATEST"


def test_exact_duplicate_events_collapse_to_one():
    row = {"customer_id": "C1", "name": "A", "operation": "I", "change_timestamp": "2026-08-01T09:00:00"}
    df = pd.DataFrame([row, row, row])
    parsed = parse_cdc_batch(df, business_key="customer_id")
    result = deduplicate_cdc_batch(parsed, business_key="customer_id")
    assert len(result.inserts) == 1
    assert result.duplicate_events_dropped == 2


def test_operation_split_into_insert_update_delete():
    df = pd.DataFrame([
        {"customer_id": "C1", "operation": "I", "change_timestamp": "2026-08-01T09:00:00"},
        {"customer_id": "C2", "operation": "U", "change_timestamp": "2026-08-01T09:00:00"},
        {"customer_id": "C3", "operation": "D", "change_timestamp": "2026-08-01T09:00:00"},
    ])
    parsed = parse_cdc_batch(df, business_key="customer_id")
    result = deduplicate_cdc_batch(parsed, business_key="customer_id")
    assert len(result.inserts) == 1 and len(result.updates) == 1 and len(result.deletes) == 1


def test_invalid_operation_code_rejected():
    df = pd.DataFrame([{"customer_id": "C1", "operation": "X", "change_timestamp": "2026-08-01T09:00:00"}])
    with pytest.raises(SchemaValidationError):
        parse_cdc_batch(df, business_key="customer_id")


def test_missing_required_column_rejected():
    df = pd.DataFrame([{"customer_id": "C1", "operation": "I"}])  # missing change_timestamp
    with pytest.raises(SchemaValidationError):
        parse_cdc_batch(df, business_key="customer_id")
