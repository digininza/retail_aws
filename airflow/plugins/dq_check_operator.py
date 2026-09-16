"""Custom Airflow operator wrapping the DQ framework (Section 13).

Used by the `dq_precheck`/`dq_postcheck` tasks in the DAGs so those steps
are a single reusable, testable operator rather than duplicated inline
PythonOperator callables in every DAG.
"""
from __future__ import annotations

from typing import Any

from airflow.exceptions import AirflowException
from airflow.models import BaseOperator


class DataQualityCheckOperator(BaseOperator):
    """Runs `src.data_quality.framework.run_data_quality` against the S3
    location produced by the upstream task, raising AirflowException (and
    so failing the task, triggering Airflow's normal retry policy) on a
    CRITICAL rule failure — see src.common.exceptions.DataQualityError.
    """

    template_fields = ("s3_uri", "dq_profile_name")

    def __init__(self, *, s3_uri: str, dq_profile_name: str, environment: str, **kwargs: Any):
        super().__init__(**kwargs)
        self.s3_uri = s3_uri
        self.dq_profile_name = dq_profile_name
        self.environment = environment

    def execute(self, context: dict[str, Any]) -> dict[str, Any]:
        from src.common.config_loader import ConfigLoader
        from src.common.exceptions import DataQualityError
        from src.common.utilities import LocalS3Adapter
        from src.data_quality import framework as dq_framework

        cfg = ConfigLoader(environment=self.environment)
        df = LocalS3Adapter().read_dataframe(self.s3_uri, fmt="parquet")
        result = dq_framework.run_data_quality(df, cfg.dq_profile(self.dq_profile_name), self.dq_profile_name)

        context["ti"].xcom_push(key="records_read", value=len(df))
        context["ti"].xcom_push(key="dq_critical_failures", value=len(result.critical_failures))

        try:
            dq_framework.enforce(result)
        except DataQualityError as exc:
            raise AirflowException(str(exc)) from exc

        return {"passed": result.passed, "rule_count": len(result.rule_results)}
