import pytest

from src.common.idempotency import RunStage, WatermarkCommitGuard, build_idempotency_key, record_hash


def test_watermark_only_advances_on_committed_stage():
    guard = WatermarkCommitGuard(pipeline_name="p1", previous_watermark="2026-01-01")
    assert guard.resolved_watermark("2026-02-01") == "2026-01-01"

    guard.advance(RunStage.EXTRACTED)
    guard.advance(RunStage.WRITTEN)
    guard.advance(RunStage.DQ_PASSED)
    assert guard.resolved_watermark("2026-02-01") == "2026-01-01"  # still not committed

    guard.advance(RunStage.RECONCILED)
    guard.advance(RunStage.COMMITTED)
    assert guard.resolved_watermark("2026-02-01") == "2026-02-01"


def test_watermark_frozen_on_failure():
    guard = WatermarkCommitGuard(pipeline_name="p1", previous_watermark="2026-01-01")
    guard.advance(RunStage.EXTRACTED)
    guard.advance(RunStage.FAILED)
    assert guard.resolved_watermark("2026-02-01") == "2026-01-01"


def test_illegal_stage_transition_raises():
    guard = WatermarkCommitGuard(pipeline_name="p1", previous_watermark=None)
    with pytest.raises(ValueError):
        guard.advance(RunStage.COMMITTED)  # cannot skip straight from STARTED to COMMITTED


def test_idempotency_key_stable_across_replays():
    key1 = build_idempotency_key("SQLSERVER", "ORD-1", "2026-08-01", "batch-42")
    key2 = build_idempotency_key("SQLSERVER", "ORD-1", "2026-08-01", "batch-42")
    assert key1 == key2


def test_idempotency_key_differs_for_different_batch():
    key1 = build_idempotency_key("SQLSERVER", "ORD-1", "2026-08-01", "batch-42")
    key2 = build_idempotency_key("SQLSERVER", "ORD-1", "2026-08-01", "batch-43")
    assert key1 != key2


def test_record_hash_ignores_untracked_attributes():
    h1 = record_hash({"name": "Asha", "city": "Delhi", "extra": "x"}, ["name", "city"])
    h2 = record_hash({"name": "Asha", "city": "Delhi", "extra": "y"}, ["name", "city"])
    assert h1 == h2


def test_record_hash_changes_with_tracked_attribute():
    h1 = record_hash({"name": "Asha", "city": "Delhi"}, ["name", "city"])
    h2 = record_hash({"name": "Asha", "city": "Gurgaon"}, ["name", "city"])
    assert h1 != h2
