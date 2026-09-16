"""Data-quality framework entry point (Section 11).

Evaluates a named DQ profile (from config/base/dq_rules.yaml) against a
DataFrame and returns a DQRunResult describing: overall pass/fail, whether
promotion should be blocked (any CRITICAL rule failed), and per-rule detail
for audit/quarantine.

Severity semantics:
  CRITICAL -> block promotion entirely, quarantine the whole batch
  ERROR    -> quarantine only the failing rows, promote the rest
  WARNING  -> log only, promote everything
  INFO     -> log only, promote everything
"""
from __future__ import annotations

from dataclasses import dataclass, field

import pandas as pd

from src.common.exceptions import DataQualityError
from src.common.logging_utils import get_logger
from src.data_quality.rules import Rule, RuleResult, Severity
from src.data_quality.validators import evaluate_rule

logger = get_logger(__name__)


@dataclass
class DQRunResult:
    profile_name: str
    rule_results: list[RuleResult] = field(default_factory=list)

    @property
    def critical_failures(self) -> list[RuleResult]:
        return [r for r in self.rule_results if not r.passed and r.severity == Severity.CRITICAL]

    @property
    def error_failures(self) -> list[RuleResult]:
        return [r for r in self.rule_results if not r.passed and r.severity == Severity.ERROR]

    @property
    def blocks_promotion(self) -> bool:
        return len(self.critical_failures) > 0

    @property
    def passed(self) -> bool:
        return all(r.passed for r in self.rule_results)


def _build_rules(profile: dict) -> list[Rule]:
    rules: list[Rule] = []
    for column, rule_configs in profile.get("rules", {}).items():
        for config in rule_configs:
            rules.append(Rule.from_config(column, config))
    return rules


def run_data_quality(df: pd.DataFrame, profile: dict, profile_name: str,
                      reference_data: dict[str, pd.Series] | None = None) -> DQRunResult:
    rules = _build_rules(profile)
    result = DQRunResult(profile_name=profile_name)
    for rule in rules:
        target_df = df  # __table__ rules still evaluate against the full frame
        rule_result = evaluate_rule(target_df, rule, reference_data)
        result.rule_results.append(rule_result)
        if not rule_result.passed:
            logger.log(
                40 if rule_result.severity == Severity.CRITICAL else 30,
                "dq_rule_failed",
                extra={
                    "task": "dq_check",
                    "error_category": f"{rule.rule_type.value}:{rule.severity.value}",
                    "records_rejected": rule_result.failed_record_count,
                },
            )
    return result


def enforce(result: DQRunResult) -> None:
    """Raise if the run result should stop the pipeline (CRITICAL failure)."""
    if result.blocks_promotion:
        failed_rules = ", ".join(f"{r.column}:{r.rule_type.value}" for r in result.critical_failures)
        raise DataQualityError(f"CRITICAL DQ failure in profile '{result.profile_name}': {failed_rules}")
