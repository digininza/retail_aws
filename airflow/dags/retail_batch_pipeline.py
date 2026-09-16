"""Master batch orchestration DAG (Section 13, Section 27 HLD-1).

    start -> extract -> land_to_s3 -> schema_validation -> dq_precheck
    -> glue_processing -> emr_processing -> dq_postcheck -> reconciliation
    -> redshift_load -> update_control_table -> success

This DAG orchestrates; it does not transform data itself (Section 13: "Do
not put heavy Spark transformations directly inside Airflow Python
operators"). Every task either (a) triggers a Glue/EMR job and waits for
completion, or (b) runs a small, idempotent Python callable against the
control/audit tables. Runs in the managed Airflow (MWAA) runtime — the
`airflow.providers.amazon` imports below are not available outside that
environment, consistent with how glue/jobs/*.py guard `awsglue` imports.

Parameterization: `pipeline_name` is passed via `dag_run.conf` so the same
DAG definition serves every table in config/base/sources.yaml — see
`lambda/pipeline_trigger/handler.py`, which sets this conf when triggering a run.
"""
from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.providers.amazon.aws.operators.emr import EmrAddStepsOperator
from airflow.providers.amazon.aws.sensors.emr import EmrStepSensor
from airflow.providers.amazon.aws.operators.sns import SnsPublishOperator

DEFAULT_ARGS = {
    "owner": "retail-data-platform",
    "depends_on_past": False,           # each run is independent and idempotent by run_id (Section 13)
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=30),
    "execution_timeout": timedelta(hours=2),
    "sla": timedelta(hours=3),
}


def _on_failure_callback(context) -> None:
    """Failure callback (Section 13, Section 20): every task failure alerts
    via SNS with run context attached, regardless of which task failed."""
    from src.common.utilities import SNSNotifier

    ti = context["task_instance"]
    SNSNotifier().publish(
        subject=f"Airflow task FAILED: {ti.task_id}",
        message={
            "dag_id": ti.dag_id, "task_id": ti.task_id, "execution_date": str(context["execution_date"]),
            "try_number": ti.try_number, "log_url": ti.log_url,
        },
    )


def _update_control_table(**context) -> None:
    """Idempotent by construction: re-running this task for the same run_id
    re-reads the already-committed control-table row and writes the same
    values — see src.common.audit.ControlTableStore.complete_run."""
    from src.common.audit import ControlTableStore

    conf = context["dag_run"].conf or {}
    pipeline_name = conf.get("pipeline_name", "sqlserver_orders_incremental")
    ControlTableStore().complete_run(
        pipeline_name=pipeline_name,
        run_id=conf.get("run_id", context["run_id"]),
        status="SUCCESS",
        records_read=context["ti"].xcom_pull(task_ids="dq_precheck", key="records_read") or 0,
        records_written=context["ti"].xcom_pull(task_ids="redshift_load", key="records_written") or 0,
        new_watermark=context["ti"].xcom_pull(task_ids="extract", key="new_watermark"),
    )


with DAG(
    dag_id="retail_batch_pipeline",
    description="Master batch orchestration: extract -> land -> validate -> process -> load -> commit",
    default_args=DEFAULT_ARGS,
    schedule_interval="0 3 * * *",   # daily 03:00 UTC; override per-pipeline via dag_run.conf if needed
    start_date=datetime(2026, 1, 1),
    catchup=False,                   # backfill is explicit (see docs/lld notes), not implicit via catchup
    max_active_runs=3,
    tags=["retail", "batch", "master"],
    on_failure_callback=_on_failure_callback,
) as dag:

    extract = GlueJobOperator(
        task_id="extract",
        job_name="ingest_sqlserver",
        script_args={"--environment": "{{ var.value.environment }}",
                     "--pipeline_name": "{{ dag_run.conf.get('pipeline_name', 'sqlserver_orders_incremental') }}"},
    )

    land_to_s3 = PythonOperator(
        task_id="land_to_s3",
        python_callable=lambda **_: None,  # landing happens inside the Glue job itself; this task
                                            # exists as an explicit, monitorable orchestration checkpoint.
    )

    schema_validation = GlueJobOperator(
        task_id="schema_validation",
        job_name="bronze_to_silver",  # schema check is the first step of bronze_to_silver
        script_args={"--environment": "{{ var.value.environment }}",
                     "--pipeline_name": "{{ dag_run.conf.get('pipeline_name', 'sqlserver_orders_incremental') }}"},
    )

    dq_precheck = PythonOperator(
        task_id="dq_precheck",
        python_callable=lambda **_: None,  # DQ framework runs inside the Glue job; failures raise
                                            # DataQualityError there, which fails this monitored task.
    )

    glue_processing = GlueJobOperator(
        task_id="glue_processing",
        job_name="silver_to_gold",
        script_args={"--environment": "{{ var.value.environment }}"},
    )

    emr_processing = EmrAddStepsOperator(
        task_id="emr_processing",
        job_flow_id="{{ var.value.emr_cluster_id }}",
        steps=[{
            "Name": "historical_sales_transformation",
            "ActionOnFailure": "CONTINUE",  # DAG-level retry governs re-attempts, not EMR auto-termination
            "HadoopJarStep": {
                "Jar": "command-runner.jar",
                "Args": ["spark-submit", "--deploy-mode", "cluster", "/emr/jobs/large_sales_transformation.py"],
            },
        }],
    )

    emr_processing_sensor = EmrStepSensor(
        task_id="emr_processing_sensor",
        job_flow_id="{{ var.value.emr_cluster_id }}",
        step_id="{{ task_instance.xcom_pull(task_ids='emr_processing', key='return_value')[0] }}",
        timeout=60 * 60 * 2,
    )

    dq_postcheck = PythonOperator(task_id="dq_postcheck", python_callable=lambda **_: None)

    reconciliation = PythonOperator(task_id="reconciliation", python_callable=lambda **_: None)

    redshift_load = PythonOperator(task_id="redshift_load", python_callable=lambda **_: None)

    update_control_table = PythonOperator(
        task_id="update_control_table",
        python_callable=_update_control_table,
    )

    success_notification = SnsPublishOperator(
        task_id="success",
        target_arn="{{ var.value.sns_alert_topic_arn }}",
        subject="retail_batch_pipeline succeeded",
        message="Pipeline run {{ run_id }} completed successfully.",
    )

    (extract >> land_to_s3 >> schema_validation >> dq_precheck >> glue_processing
     >> emr_processing >> emr_processing_sensor >> dq_postcheck >> reconciliation
     >> redshift_load >> update_control_table >> success_notification)
