"""Local adapters standing in for AWS services (S3, SNS, Secrets Manager) so
the framework runs end-to-end without live AWS infrastructure. Each adapter
mirrors the AWS API shape it replaces closely enough that swapping to boto3
is a small, isolated change — never a rewrite of pipeline logic. Toggled by
`AWS_USE_LOCAL_ADAPTERS` (see .env.example, ADR-009).
"""
from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any, Optional

import pandas as pd

from src.common.logging_utils import get_logger

logger = get_logger(__name__)


def _use_local_adapters() -> bool:
    return os.getenv("AWS_USE_LOCAL_ADAPTERS", "true").lower() == "true"


class LocalS3Adapter:
    """Filesystem-backed stand-in for S3, rooted at LOCAL_S3_ROOT.

    `s3://bucket/prefix/...` URIs are mapped to `<LOCAL_S3_ROOT>/<bucket>/<prefix>/...`
    so lake-zone paths produced by ConfigLoader.s3_path() work unmodified.
    """

    def __init__(self, root: Optional[str] = None):
        self.root = Path(root or os.getenv("LOCAL_S3_ROOT", ".local_s3"))

    def _resolve(self, s3_uri: str) -> Path:
        assert s3_uri.startswith("s3://"), f"Expected s3:// URI, got {s3_uri}"
        return self.root / s3_uri[len("s3://") :]

    def write_dataframe(self, df: pd.DataFrame, s3_uri: str, fmt: str = "parquet") -> str:
        path = self._resolve(s3_uri)
        path.mkdir(parents=True, exist_ok=True)
        file_path = path / f"part-000.{fmt}"
        if fmt == "parquet":
            df.to_parquet(file_path, index=False)
        elif fmt == "csv":
            df.to_csv(file_path, index=False)
        elif fmt == "json":
            df.to_json(file_path, orient="records", lines=True)
        else:
            raise ValueError(f"Unsupported format: {fmt}")
        logger.info("s3_write", extra={"target": s3_uri, "records_written": len(df)})
        return str(file_path)

    def read_dataframe(self, s3_uri: str, fmt: str = "parquet") -> pd.DataFrame:
        path = self._resolve(s3_uri)
        frames = []
        pattern = f"*.{fmt}"
        for file_path in sorted(path.glob(pattern)) if path.exists() else []:
            if fmt == "parquet":
                frames.append(pd.read_parquet(file_path))
            elif fmt == "csv":
                frames.append(pd.read_csv(file_path))
            elif fmt == "json":
                frames.append(pd.read_json(file_path, orient="records", lines=True))
        if not frames:
            return pd.DataFrame()
        return pd.concat(frames, ignore_index=True)

    def exists(self, s3_uri: str) -> bool:
        return self._resolve(s3_uri).exists()

    def move(self, source_uri: str, dest_uri: str) -> None:
        """Represents S3 lifecycle moves (e.g., raw -> archive)."""
        src = self._resolve(source_uri)
        dst = self._resolve(dest_uri)
        dst.mkdir(parents=True, exist_ok=True)
        for f in src.glob("*"):
            f.rename(dst / f.name)


class SNSNotifier:
    """Failure/DQ/reconciliation alert dispatch (Section 20).

    Locally, alerts are logged as structured events; in AWS this publishes
    to the SNS topic ARN configured per environment.
    """

    def __init__(self, topic_arn: Optional[str] = None):
        self.topic_arn = topic_arn or os.getenv("SNS_ALERT_TOPIC_ARN", "local-mock-topic")

    def publish(self, subject: str, message: dict[str, Any]) -> None:
        if _use_local_adapters():
            logger.warning(
                "sns_alert",
                extra={"status": "ALERT", "error_category": subject, **{"message": json.dumps(message, default=str)}},
            )
            return
        import boto3  # optional dependency, only needed against live AWS

        client = boto3.client("sns")
        client.publish(TopicArn=self.topic_arn, Subject=subject, Message=json.dumps(message, default=str))


class SecretsManagerAdapter:
    """Resolves a secret by env-var-based ARN reference.

    Locally, `SQLSERVER_SECRET_ARN=...` in .env just names *which* mock
    credential env vars to read (e.g. SQLSERVER_MOCK_USER/PASSWORD). In AWS,
    the same secret_ref resolves via `secretsmanager.get_secret_value`.
    Never hardcode credentials — see Section 19 / ADR-009.
    """

    def get_secret(self, secret_ref_env_var: str) -> dict[str, str]:
        secret_arn = os.getenv(secret_ref_env_var)
        if not secret_arn:
            raise ValueError(f"No secret ARN configured for {secret_ref_env_var}")
        if _use_local_adapters():
            prefix = secret_ref_env_var.replace("_SECRET_ARN", "_MOCK")
            return {
                "username": os.getenv(f"{prefix}_USER", "mock_user"),
                "password": os.getenv(f"{prefix}_PASSWORD", "mock_password"),
                "key": os.getenv(f"{prefix}_KEY", "mock_key"),
            }
        import boto3  # optional dependency, only needed against live AWS

        client = boto3.client("secretsmanager")
        response = client.get_secret_value(SecretId=secret_arn)
        return json.loads(response["SecretString"])


def dataframe_to_records(df: pd.DataFrame) -> list[dict[str, Any]]:
    return df.to_dict(orient="records")
