"""Shared pytest fixtures: isolates every test run's local S3 / control DB /
Kinesis state to a per-test tmp directory so tests never share state with a
developer's own `make demo-*` runs or with each other.
"""
from __future__ import annotations

import os
from pathlib import Path

import pytest


@pytest.fixture(autouse=True)
def isolated_local_state(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    monkeypatch.setenv("AWS_USE_LOCAL_ADAPTERS", "true")
    monkeypatch.setenv("LOCAL_S3_ROOT", str(tmp_path / "local_s3"))
    monkeypatch.setenv("CONTROL_DB_PATH", str(tmp_path / "control.sqlite3"))
    monkeypatch.setenv("LOCAL_KINESIS_ROOT", str(tmp_path / "kinesis"))
    monkeypatch.setenv("ENVIRONMENT", "dev")
    return tmp_path


@pytest.fixture
def sample_data_root() -> Path:
    return Path(__file__).resolve().parents[1] / "sample_data"
