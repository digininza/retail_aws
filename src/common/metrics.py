"""Lightweight metrics emission, standing in for CloudWatch custom metrics
(Section 20). In AWS this would call `boto3 cloudwatch.put_metric_data`;
locally it logs a structured metric event so the same call sites work in
both environments (see AWS_USE_LOCAL_ADAPTERS in src.common.config_loader).
"""
from __future__ import annotations

import os
from dataclasses import dataclass, field
from typing import Optional

from src.common.logging_utils import get_logger

logger = get_logger(__name__)


@dataclass
class PipelineMetrics:
    pipeline_name: str
    run_id: str
    environment: str
    records_read: int = 0
    records_written: int = 0
    records_rejected: int = 0
    duration_seconds: float = 0.0
    dq_failures: int = 0
    reconciliation_status: Optional[str] = None
    extra: dict = field(default_factory=dict)

    def emit(self) -> None:
        use_local = os.getenv("AWS_USE_LOCAL_ADAPTERS", "true").lower() == "true"
        namespace = "RetailPlatform"
        dimensions = {"Pipeline": self.pipeline_name, "Environment": self.environment}
        metric_values = {
            "RecordsRead": self.records_read,
            "RecordsWritten": self.records_written,
            "RecordsRejected": self.records_rejected,
            "DurationSeconds": self.duration_seconds,
            "DQFailures": self.dq_failures,
        }
        if use_local:
            logger.info(
                "metric_emit",
                extra={
                    "pipeline": self.pipeline_name,
                    "run_id": self.run_id,
                    "status": self.reconciliation_status,
                    **{f"metric_{k}": v for k, v in metric_values.items()},
                },
            )
            return
        # Real AWS path (illustrative — requires boto3 credentials/network):
        import boto3  # local import: optional dependency at runtime

        cw = boto3.client("cloudwatch")
        cw.put_metric_data(
            Namespace=namespace,
            MetricData=[
                {
                    "MetricName": name,
                    "Dimensions": [{"Name": k, "Value": v} for k, v in dimensions.items()],
                    "Value": float(value),
                }
                for name, value in metric_values.items()
            ],
        )
