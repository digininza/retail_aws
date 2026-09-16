"""Structured logging (Section 20).

Every log line carries: timestamp, environment, pipeline, task, run_id,
source, target, status, record counts, error category — as a single JSON
object so it can be shipped to CloudWatch Logs and queried with Logs Insights
in a real deployment. Locally it prints to stdout.
"""
from __future__ import annotations

import json
import logging
import sys
from datetime import datetime, timezone
from typing import Any, Optional


class StructuredFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        payload: dict[str, Any] = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "level": record.levelname,
            "message": record.getMessage(),
        }
        for field in (
            "environment",
            "pipeline",
            "task",
            "run_id",
            "source",
            "target",
            "status",
            "records_read",
            "records_written",
            "records_rejected",
            "error_category",
        ):
            value = getattr(record, field, None)
            if value is not None:
                payload[field] = value
        if record.exc_info:
            payload["exception"] = self.formatException(record.exc_info)
        return json.dumps(payload, default=str)


def get_logger(name: str, level: str = "INFO") -> logging.Logger:
    logger = logging.getLogger(name)
    if not logger.handlers:
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(StructuredFormatter())
        logger.addHandler(handler)
    logger.setLevel(getattr(logging, level.upper(), logging.INFO))
    logger.propagate = False
    return logger


def log_with_context(
    logger: logging.Logger,
    level: str,
    message: str,
    *,
    environment: Optional[str] = None,
    pipeline: Optional[str] = None,
    task: Optional[str] = None,
    run_id: Optional[str] = None,
    source: Optional[str] = None,
    target: Optional[str] = None,
    status: Optional[str] = None,
    records_read: Optional[int] = None,
    records_written: Optional[int] = None,
    records_rejected: Optional[int] = None,
    error_category: Optional[str] = None,
) -> None:
    extra = {
        "environment": environment,
        "pipeline": pipeline,
        "task": task,
        "run_id": run_id,
        "source": source,
        "target": target,
        "status": status,
        "records_read": records_read,
        "records_written": records_written,
        "records_rejected": records_rejected,
        "error_category": error_category,
    }
    extra = {k: v for k, v in extra.items() if v is not None}
    getattr(logger, level.lower())(message, extra=extra)
