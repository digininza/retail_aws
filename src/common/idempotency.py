"""Idempotency helpers and the watermark commit lifecycle (Sections 7, 9, 23).

CRITICAL RULE (Section 7): the watermark must never advance before the
downstream write, DQ, and reconciliation stages have all succeeded. This
module encodes that ordering as a small state machine so pipeline code
cannot accidentally commit a watermark early.

Idempotency keys (Section 9) are always composite — never a bare timestamp —
so a rerun of the exact same batch is provably a no-op rather than a
duplicate-producing side effect.
"""
from __future__ import annotations

import hashlib
from dataclasses import dataclass
from enum import Enum
from typing import Any, Iterable, Optional


def build_idempotency_key(
    source_system: str,
    source_record_id: str,
    business_date: str,
    batch_id: str,
    event_id: Optional[str] = None,
) -> str:
    """Composite idempotency key per Section 9.

    Deliberately excludes any raw timestamp-only component: replaying the
    same batch_id for the same business_date and record must yield the same
    key, regardless of when the replay happens.
    """
    parts = [source_system, source_record_id, business_date, batch_id]
    if event_id:
        parts.append(event_id)
    raw = "|".join(parts)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def record_hash(values: dict[str, Any], tracked_attributes: Iterable[str]) -> str:
    """Deterministic hash over tracked attributes, used by SCD2 change detection
    and by merge/upsert dedup to detect no-op updates."""
    parts = [f"{attr}={values.get(attr)}" for attr in sorted(tracked_attributes)]
    return hashlib.sha256("|".join(parts).encode("utf-8")).hexdigest()


class RunStage(str, Enum):
    STARTED = "STARTED"
    EXTRACTED = "EXTRACTED"
    WRITTEN = "WRITTEN"
    DQ_PASSED = "DQ_PASSED"
    RECONCILED = "RECONCILED"
    COMMITTED = "COMMITTED"
    FAILED = "FAILED"


_ALLOWED_TRANSITIONS: dict[RunStage, set[RunStage]] = {
    RunStage.STARTED: {RunStage.EXTRACTED, RunStage.FAILED},
    RunStage.EXTRACTED: {RunStage.WRITTEN, RunStage.FAILED},
    RunStage.WRITTEN: {RunStage.DQ_PASSED, RunStage.FAILED},
    RunStage.DQ_PASSED: {RunStage.RECONCILED, RunStage.FAILED},
    RunStage.RECONCILED: {RunStage.COMMITTED, RunStage.FAILED},
    RunStage.COMMITTED: set(),
    RunStage.FAILED: set(),
}


@dataclass
class WatermarkCommitGuard:
    """Enforces: read watermark -> extract -> write -> DQ -> reconciliation ->
    commit -> update watermark. Only `stage == COMMITTED` permits
    `may_advance_watermark()` to return True; any failure freezes the
    watermark at its last successfully committed value (Section 7, 23).
    """

    pipeline_name: str
    previous_watermark: Optional[str]
    stage: RunStage = RunStage.STARTED

    def advance(self, next_stage: RunStage) -> None:
        allowed = _ALLOWED_TRANSITIONS[self.stage]
        if next_stage not in allowed:
            raise ValueError(
                f"Illegal stage transition for pipeline '{self.pipeline_name}': "
                f"{self.stage} -> {next_stage}"
            )
        self.stage = next_stage

    def may_advance_watermark(self) -> bool:
        return self.stage == RunStage.COMMITTED

    def resolved_watermark(self, new_watermark: str) -> str:
        """Return the watermark to persist to the control table.

        Only returns `new_watermark` once COMMITTED; otherwise returns the
        previous committed watermark, preserving Section 7's critical rule.
        """
        if self.may_advance_watermark():
            return new_watermark
        return self.previous_watermark or ""
