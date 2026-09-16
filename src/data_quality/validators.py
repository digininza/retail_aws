"""Per-rule-type evaluators and schema-evolution classification (Section 11, 24)."""
from __future__ import annotations

import re
from datetime import datetime, timezone
from typing import Any, Optional

import pandas as pd

from src.data_quality.rules import Rule, RuleResult, RuleType, Severity


def evaluate_rule(df: pd.DataFrame, rule: Rule, reference_data: Optional[dict[str, pd.Series]] = None) -> RuleResult:
    handler = _HANDLERS.get(rule.rule_type)
    if handler is None:
        return RuleResult(rule.column, rule.rule_type, rule.severity, passed=True, detail="unhandled rule type")
    return handler(df, rule, reference_data or {})


def _not_null(df: pd.DataFrame, rule: Rule, _ref: dict) -> RuleResult:
    if rule.column not in df.columns:
        return RuleResult(rule.column, rule.rule_type, rule.severity, False, len(df), "column missing")
    failed = df[rule.column].isna().sum()
    return RuleResult(rule.column, rule.rule_type, rule.severity, failed == 0, int(failed))


def _unique(df: pd.DataFrame, rule: Rule, _ref: dict) -> RuleResult:
    if rule.column not in df.columns:
        return RuleResult(rule.column, rule.rule_type, rule.severity, False, len(df), "column missing")
    dup_count = int(df[rule.column].duplicated(keep=False).sum())
    return RuleResult(rule.column, rule.rule_type, rule.severity, dup_count == 0, dup_count)


def _duplicate(df: pd.DataFrame, rule: Rule, _ref: dict) -> RuleResult:
    return _unique(df, rule, _ref)


def _range(df: pd.DataFrame, rule: Rule, _ref: dict) -> RuleResult:
    if rule.column not in df.columns:
        return RuleResult(rule.column, rule.rule_type, rule.severity, False, len(df), "column missing")
    lo = rule.params.get("min")
    hi = rule.params.get("max")
    series = pd.to_numeric(df[rule.column], errors="coerce")
    mask = pd.Series(False, index=df.index)
    if lo is not None:
        mask |= series < lo
    if hi is not None:
        mask |= series > hi
    failed = int(mask.sum())
    return RuleResult(rule.column, rule.rule_type, rule.severity, failed == 0, failed)


def _regex(df: pd.DataFrame, rule: Rule, _ref: dict) -> RuleResult:
    if rule.column not in df.columns:
        return RuleResult(rule.column, rule.rule_type, rule.severity, False, len(df), "column missing")
    pattern = re.compile(rule.params["pattern"])
    failed = int((~df[rule.column].astype(str).apply(lambda v: bool(pattern.match(v)))).sum())
    return RuleResult(rule.column, rule.rule_type, rule.severity, failed == 0, failed)


def _referential_integrity(df: pd.DataFrame, rule: Rule, ref: dict) -> RuleResult:
    ref_series = ref.get(rule.params.get("ref_table", ""))
    if ref_series is None or rule.column not in df.columns:
        return RuleResult(rule.column, rule.rule_type, rule.severity, True, 0, "no reference data supplied - skipped")
    failed = int((~df[rule.column].isin(ref_series)).sum())
    return RuleResult(rule.column, rule.rule_type, rule.severity, failed == 0, failed)


def _record_count(df: pd.DataFrame, rule: Rule, _ref: dict) -> RuleResult:
    minimum = rule.params.get("min", 0)
    passed = len(df) >= minimum
    return RuleResult(rule.column, rule.rule_type, rule.severity, passed, 0 if passed else len(df))


def _freshness(df: pd.DataFrame, rule: Rule, _ref: dict) -> RuleResult:
    if rule.column not in df.columns or df.empty:
        return RuleResult(rule.column, rule.rule_type, rule.severity, True, 0, "no data to check")
    max_age_hours = rule.params.get("max_age_hours", 24)
    latest = pd.to_datetime(df[rule.column]).max()
    if latest.tzinfo is None:
        latest = latest.tz_localize(timezone.utc)
    age_hours = (datetime.now(timezone.utc) - latest).total_seconds() / 3600
    passed = age_hours <= max_age_hours
    return RuleResult(rule.column, rule.rule_type, rule.severity, passed, 0 if passed else len(df),
                       f"latest record is {age_hours:.1f}h old")


def _data_type(df: pd.DataFrame, rule: Rule, _ref: dict) -> RuleResult:
    if rule.column not in df.columns:
        return RuleResult(rule.column, rule.rule_type, rule.severity, False, len(df), "column missing")
    expected = rule.params.get("expected")
    try:
        if expected == "timestamp":
            pd.to_datetime(df[rule.column])
        elif expected in ("decimal", "float", "integer"):
            pd.to_numeric(df[rule.column])
        return RuleResult(rule.column, rule.rule_type, rule.severity, True, 0)
    except (ValueError, TypeError):
        return RuleResult(rule.column, rule.rule_type, rule.severity, False, len(df), f"cast to {expected} failed")


def _business_rule(df: pd.DataFrame, rule: Rule, _ref: dict) -> RuleResult:
    # Placeholder for custom callables registered per-pipeline; passes by default.
    return RuleResult(rule.column, rule.rule_type, rule.severity, True, 0, "no business rule callable registered")


def _schema_match(df: pd.DataFrame, rule: Rule, _ref: dict) -> RuleResult:
    # Actual schema comparison is performed by classify_schema_change(); this
    # rule type is a hook so it can participate in the same severity gating.
    return RuleResult(rule.column, rule.rule_type, rule.severity, True, 0)


_HANDLERS = {
    RuleType.NOT_NULL: _not_null,
    RuleType.UNIQUE: _unique,
    RuleType.DUPLICATE: _duplicate,
    RuleType.RANGE: _range,
    RuleType.REGEX: _regex,
    RuleType.REFERENTIAL_INTEGRITY: _referential_integrity,
    RuleType.RECORD_COUNT: _record_count,
    RuleType.FRESHNESS: _freshness,
    RuleType.DATA_TYPE: _data_type,
    RuleType.BUSINESS_RULE: _business_rule,
    RuleType.SCHEMA_MATCH: _schema_match,
}


# --- Schema evolution classification (Section 24) -------------------------

SchemaChangeClass = str  # "COMPATIBLE" | "WARNING" | "BREAKING"


def classify_schema_change(registered_schema: dict[str, Any], incoming_columns: set[str],
                            incoming_types: dict[str, str]) -> dict[str, SchemaChangeClass]:
    """Compares an incoming batch's columns/types against the registered
    schema (config/base/schemas.yaml) and classifies each difference."""
    required = set(registered_schema.get("required", []))
    optional = set(registered_schema.get("optional", []))
    known = required | optional
    registered_types = registered_schema.get("types", {})

    findings: dict[str, SchemaChangeClass] = {}

    for missing_required in required - incoming_columns:
        findings[missing_required] = "BREAKING"  # missing_required_column

    for extra_column in incoming_columns - known:
        findings[extra_column] = "WARNING"  # unexpected_extra_column

    for col in incoming_columns & known:
        expected_type = registered_types.get(col)
        actual_type = incoming_types.get(col)
        if expected_type and actual_type and expected_type != actual_type:
            findings[col] = _classify_type_change(expected_type, actual_type)

    return findings


_TYPE_WIDENING = {
    ("integer", "decimal"): "WARNING",
    ("integer", "string"): "WARNING",
    ("decimal", "string"): "WARNING",
}


def _classify_type_change(expected: str, actual: str) -> SchemaChangeClass:
    if (expected, actual) in _TYPE_WIDENING:
        return "WARNING"
    return "BREAKING"  # narrowing or incompatible change


def has_breaking_change(findings: dict[str, SchemaChangeClass]) -> bool:
    return any(v == "BREAKING" for v in findings.values())
