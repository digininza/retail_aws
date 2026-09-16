"""Control table and audit log (Sections 7, 36).

Reference implementation backs both by SQLite (`CONTROL_DB_PATH`), standing
in for an RDS/DynamoDB control schema in a real AWS deployment — see
docs/lld/01_sqlserver_incremental_ingestion_lld.md for the alternative-design
discussion (DynamoDB vs. RDS vs. Redshift control schema).

The control table and the audit log are deliberately separate concerns:
- `ControlTableStore` holds the *current* state needed to run the next
  incremental extract (one row per pipeline, upserted every run).
- `AuditLogStore` holds the *history* of every run, append-only, for
  observability and troubleshooting (Section 36).

Both live outside the data lake itself (raw/bronze/silver/gold) precisely so
that reprocessing or deleting lake data never destroys operational history.
"""
from __future__ import annotations

import os
import sqlite3
import uuid
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterator, Optional

DEFAULT_DB_PATH = ".local_s3/control.sqlite3"


def new_run_id() -> str:
    return str(uuid.uuid4())


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _db_path() -> str:
    return os.getenv("CONTROL_DB_PATH", DEFAULT_DB_PATH)


@contextmanager
def _connection() -> Iterator[sqlite3.Connection]:
    path = _db_path()
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    try:
        yield conn
        conn.commit()
    finally:
        conn.close()


def init_db() -> None:
    with _connection() as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS control_table (
                pipeline_name TEXT PRIMARY KEY,
                source_system TEXT,
                source_table TEXT,
                load_type TEXT,
                watermark_column TEXT,
                last_successful_watermark TEXT,
                current_run_id TEXT,
                last_run_status TEXT,
                last_run_start TEXT,
                last_run_end TEXT,
                records_read INTEGER,
                records_written INTEGER,
                error_message TEXT
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS audit_log (
                run_id TEXT PRIMARY KEY,
                pipeline_name TEXT,
                environment TEXT,
                start_time TEXT,
                end_time TEXT,
                status TEXT,
                records_read INTEGER,
                records_written INTEGER,
                records_rejected INTEGER,
                source TEXT,
                target TEXT,
                watermark_before TEXT,
                watermark_after TEXT,
                error_message TEXT
            )
            """
        )


@dataclass
class ControlTableRow:
    pipeline_name: str
    source_system: str
    source_table: str
    load_type: str
    watermark_column: Optional[str]
    last_successful_watermark: Optional[str]
    current_run_id: Optional[str]
    last_run_status: Optional[str]
    last_run_start: Optional[str]
    last_run_end: Optional[str]
    records_read: Optional[int]
    records_written: Optional[int]
    error_message: Optional[str]


class ControlTableStore:
    """One row per pipeline. `last_successful_watermark` is the only field a
    downstream extract should read to plan its next run — see Section 7.
    """

    def __init__(self) -> None:
        init_db()

    def get(self, pipeline_name: str) -> Optional[ControlTableRow]:
        with _connection() as conn:
            row = conn.execute(
                "SELECT * FROM control_table WHERE pipeline_name = ?", (pipeline_name,)
            ).fetchone()
            return ControlTableRow(**dict(row)) if row else None

    def start_run(self, pipeline_name: str, source_system: str, source_table: str,
                   load_type: str, watermark_column: Optional[str], run_id: str) -> None:
        """Register a run as in-flight WITHOUT touching last_successful_watermark."""
        existing = self.get(pipeline_name)
        with _connection() as conn:
            conn.execute(
                """
                INSERT INTO control_table (
                    pipeline_name, source_system, source_table, load_type, watermark_column,
                    last_successful_watermark, current_run_id, last_run_status,
                    last_run_start, last_run_end, records_read, records_written, error_message
                ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)
                ON CONFLICT(pipeline_name) DO UPDATE SET
                    current_run_id=excluded.current_run_id,
                    last_run_status='RUNNING',
                    last_run_start=excluded.last_run_start,
                    last_run_end=NULL,
                    error_message=NULL
                """,
                (
                    pipeline_name, source_system, source_table, load_type, watermark_column,
                    existing.last_successful_watermark if existing else None,
                    run_id, "RUNNING", utc_now_iso(), None, None, None, None,
                ),
            )

    def complete_run(self, pipeline_name: str, run_id: str, status: str,
                      records_read: int, records_written: int,
                      new_watermark: Optional[str], error_message: Optional[str] = None) -> None:
        """Finalize a run. `new_watermark` is only persisted when status == SUCCESS
        (Section 7's critical rule is enforced by the caller passing None/previous
        value on failure — see src.common.idempotency.WatermarkCommitGuard)."""
        existing = self.get(pipeline_name)
        watermark_to_store = (
            new_watermark if status == "SUCCESS" else (existing.last_successful_watermark if existing else None)
        )
        with _connection() as conn:
            conn.execute(
                """
                UPDATE control_table SET
                    last_successful_watermark = ?,
                    last_run_status = ?,
                    last_run_end = ?,
                    records_read = ?,
                    records_written = ?,
                    error_message = ?
                WHERE pipeline_name = ? AND current_run_id = ?
                """,
                (watermark_to_store, status, utc_now_iso(), records_read, records_written,
                 error_message, pipeline_name, run_id),
            )


class AuditLogStore:
    """Append-only run history (Section 36)."""

    def __init__(self) -> None:
        init_db()

    def write(self, run_id: str, pipeline_name: str, environment: str, start_time: str,
               end_time: Optional[str], status: str, records_read: int, records_written: int,
               records_rejected: int, source: str, target: str,
               watermark_before: Optional[str], watermark_after: Optional[str],
               error_message: Optional[str] = None) -> None:
        with _connection() as conn:
            conn.execute(
                """
                INSERT INTO audit_log (
                    run_id, pipeline_name, environment, start_time, end_time, status,
                    records_read, records_written, records_rejected, source, target,
                    watermark_before, watermark_after, error_message
                ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                ON CONFLICT(run_id) DO UPDATE SET
                    end_time=excluded.end_time, status=excluded.status,
                    records_read=excluded.records_read, records_written=excluded.records_written,
                    records_rejected=excluded.records_rejected,
                    watermark_after=excluded.watermark_after, error_message=excluded.error_message
                """,
                (run_id, pipeline_name, environment, start_time, end_time, status,
                 records_read, records_written, records_rejected, source, target,
                 watermark_before, watermark_after, error_message),
            )

    def history(self, pipeline_name: str, limit: int = 20) -> list[dict]:
        with _connection() as conn:
            rows = conn.execute(
                "SELECT * FROM audit_log WHERE pipeline_name = ? ORDER BY start_time DESC LIMIT ?",
                (pipeline_name, limit),
            ).fetchall()
            return [dict(r) for r in rows]
