"""Shared helpers for Glue job scripts (Section 14): consistent argument
resolution and a thin wrapper documenting the Job Bookmark vs. control-table
distinction at a single call site, so every job references the same
explanation rather than restating it inline.
"""
from __future__ import annotations

from typing import Any

try:
    from awsglue.utils import getResolvedOptions
except ImportError:  # pragma: no cover - local environment
    getResolvedOptions = None  # type: ignore


def resolve_job_args(argv: list[str], required: list[str], local_defaults: dict[str, Any]) -> dict[str, Any]:
    """Resolves Glue job arguments in AWS; falls back to `local_defaults`
    when run outside the Glue runtime (e.g. under `python -m` for linting
    or local dry-run)."""
    if getResolvedOptions is not None:
        return getResolvedOptions(argv, required)
    return local_defaults


NOTE_BOOKMARKS_VS_CONTROL_TABLE = """
Glue Job Bookmarks are an internal, per-job, Glue-managed checkpoint of which
S3 objects or JDBC rows have already been processed. They are useful as a
belt-and-braces optimization but are NOT this project's watermark mechanism:
they are opaque (no queryable audit trail), scoped to the Glue job (not
portable if the same table is later processed by EMR or Lambda), and cannot
express our required stage ordering (extract -> write -> DQ -> reconciliation
-> commit -> watermark advance, Section 7). The control table
(`src.common.audit.ControlTableStore`) is always the source of truth.
"""
