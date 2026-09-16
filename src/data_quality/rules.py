"""Data-quality rule types and severities (Section 11)."""
from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Any, Optional


class Severity(str, Enum):
    CRITICAL = "CRITICAL"
    ERROR = "ERROR"
    WARNING = "WARNING"
    INFO = "INFO"


class RuleType(str, Enum):
    NOT_NULL = "NOT_NULL"
    UNIQUE = "UNIQUE"
    DUPLICATE = "DUPLICATE"
    DATA_TYPE = "DATA_TYPE"
    RANGE = "RANGE"
    REFERENTIAL_INTEGRITY = "REFERENTIAL_INTEGRITY"
    REGEX = "REGEX"
    RECORD_COUNT = "RECORD_COUNT"
    FRESHNESS = "FRESHNESS"
    BUSINESS_RULE = "BUSINESS_RULE"
    SCHEMA_MATCH = "SCHEMA_MATCH"


@dataclass
class RuleResult:
    column: str
    rule_type: RuleType
    severity: Severity
    passed: bool
    failed_record_count: int = 0
    detail: Optional[str] = None


@dataclass
class Rule:
    column: str
    rule_type: RuleType
    severity: Severity
    params: dict[str, Any]

    @classmethod
    def from_config(cls, column: str, config: dict[str, Any]) -> "Rule":
        rule_type = RuleType(config["type"])
        severity = Severity(config.get("severity", "WARNING"))
        params = {k: v for k, v in config.items() if k not in {"type", "severity"}}
        return cls(column=column, rule_type=rule_type, severity=severity, params=params)
