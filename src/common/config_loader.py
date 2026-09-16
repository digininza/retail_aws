"""Metadata-driven configuration loader (Section 6, Section 25).

Merges `config/base/*.yaml` with `config/<environment>/environment.yaml`.
Business/pipeline metadata (sources, pipelines, dq_rules, reconciliation,
schemas) lives in base and is environment-agnostic; only environment-specific
values (bucket names, role ARNs, cluster sizing, feature flags) live in the
per-environment overlay. Secrets are never read from YAML — see
`src.common.secrets`-style resolution via `SecretsManagerAdapter` below.

This module is the single source of truth pipelines use to answer: what is
my source, target, load type, watermark column, primary key, processing
engine, retry policy, DQ profile, and reconciliation profile?
"""
from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Optional

import yaml

from src.common.exceptions import ConfigurationError

REPO_ROOT = Path(__file__).resolve().parents[2]
CONFIG_ROOT = REPO_ROOT / "config"

VALID_ENVIRONMENTS = {"dev", "qa", "prod"}


def _load_yaml(path: Path) -> dict[str, Any]:
    if not path.exists():
        raise ConfigurationError(f"Missing configuration file: {path}")
    with open(path, "r", encoding="utf-8") as fh:
        return yaml.safe_load(fh) or {}


def _deep_merge(base: dict[str, Any], override: dict[str, Any]) -> dict[str, Any]:
    """Recursively merge `override` on top of `base`, without mutating either."""
    merged = dict(base)
    for key, value in override.items():
        if key in merged and isinstance(merged[key], dict) and isinstance(value, dict):
            merged[key] = _deep_merge(merged[key], value)
        else:
            merged[key] = value
    return merged


@dataclass
class PipelineMetadata:
    pipeline_name: str
    source_system: str
    source_table: str
    target_path: str
    load_type: str
    watermark_column: Optional[str]
    primary_key: Any
    active: bool
    processing_engine: str
    retry_count: int
    dq_profile: Optional[str] = None
    reconciliation_profile: Optional[str] = None
    raw: dict[str, Any] = field(default_factory=dict)

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "PipelineMetadata":
        return cls(
            pipeline_name=data["pipeline_name"],
            source_system=data["source_system"],
            source_table=data["source_table"],
            target_path=data["target_path"],
            load_type=data["load_type"],
            watermark_column=data.get("watermark_column"),
            primary_key=data.get("primary_key"),
            active=bool(data.get("active", True)),
            processing_engine=data.get("processing_engine", "GLUE"),
            retry_count=int(data.get("retry_count", 3)),
            dq_profile=data.get("dq_profile"),
            reconciliation_profile=data.get("reconciliation_profile"),
            raw=data,
        )


class ConfigLoader:
    """Loads and merges metadata-driven configuration for a given environment.

    Usage:
        cfg = ConfigLoader(environment="dev")
        pipeline = cfg.get_pipeline("sqlserver_orders_incremental")
        env = cfg.environment_config()
        dq_profile = cfg.dq_profile(pipeline.dq_profile)
    """

    def __init__(self, environment: Optional[str] = None, config_root: Optional[Path] = None):
        self.environment = environment or os.getenv("ENVIRONMENT", "dev")
        if self.environment not in VALID_ENVIRONMENTS:
            raise ConfigurationError(
                f"Unknown environment '{self.environment}'. Expected one of {VALID_ENVIRONMENTS}."
            )
        self.config_root = config_root or CONFIG_ROOT
        self._base_cache: dict[str, Any] | None = None
        self._env_cache: dict[str, Any] | None = None

    def _base(self) -> dict[str, Any]:
        if self._base_cache is None:
            base_dir = self.config_root / "base"
            merged: dict[str, Any] = {}
            for filename in ("sources.yaml", "pipelines.yaml", "dq_rules.yaml", "reconciliation.yaml", "schemas.yaml"):
                merged = _deep_merge(merged, _load_yaml(base_dir / filename))
            self._base_cache = merged
        return self._base_cache

    def environment_config(self) -> dict[str, Any]:
        if self._env_cache is None:
            self._env_cache = _load_yaml(self.config_root / self.environment / "environment.yaml")
        return self._env_cache

    def list_pipelines(self, active_only: bool = True) -> list[PipelineMetadata]:
        pipelines = [PipelineMetadata.from_dict(p) for p in self._base().get("pipelines", [])]
        return [p for p in pipelines if p.active or not active_only]

    def get_pipeline(self, pipeline_name: str) -> PipelineMetadata:
        for p in self.list_pipelines(active_only=False):
            if p.pipeline_name == pipeline_name:
                return p
        raise ConfigurationError(f"Pipeline '{pipeline_name}' not found in sources.yaml")

    def dq_profile(self, profile_name: str) -> dict[str, Any]:
        profiles = self._base().get("profiles", {})
        # dq_rules.yaml and reconciliation.yaml both use a top-level `profiles` key;
        # dq_rules profiles carry a `table` key which disambiguates them.
        if profile_name not in profiles:
            raise ConfigurationError(f"DQ profile '{profile_name}' not found in dq_rules.yaml")
        return profiles[profile_name]

    def reconciliation_profile(self, profile_name: str) -> dict[str, Any]:
        profiles = self._base().get("profiles", {})
        if profile_name not in profiles:
            raise ConfigurationError(f"Reconciliation profile '{profile_name}' not found in reconciliation.yaml")
        return profiles[profile_name]

    def schema(self, schema_name: str) -> dict[str, Any]:
        schemas = self._base().get("schemas", {})
        if schema_name not in schemas:
            raise ConfigurationError(f"Schema '{schema_name}' not found in schemas.yaml")
        return schemas[schema_name]

    def retry_defaults(self) -> dict[str, Any]:
        return self._base().get("defaults", {}).get("retry", {})

    def s3_path(self, zone: str) -> str:
        """Resolve a full s3://bucket/prefix URI for a lake zone (raw/bronze/silver/gold/...)."""
        env = self.environment_config()
        bucket = env["s3"]["bucket"]
        prefix_key = f"{zone}_prefix"
        if prefix_key not in env["s3"]:
            raise ConfigurationError(f"Unknown S3 zone '{zone}'")
        return f"s3://{bucket}/{env['s3'][prefix_key]}"
