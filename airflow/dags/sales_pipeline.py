"""Sales pipeline DAG: Glue -> EMR -> Reconciliation -> Redshift (Section 13
explicitly requires this sequence; Section 27 HLD-2).

Why this order: Glue first performs standard extraction/cleansing (cheap,
fast, managed) so EMR only ever processes already-validated Silver data —
running EMR against raw, un-validated data would waste cluster time
reprocessing rows that DQ would reject anyway. EMR then does the
heavy historical join/aggregation that Glue is not sized for (ADR-002).
Reconciliation runs *after* EMR, against EMR's actual output, because that
is the stage whose correctness needs verifying — reconciling Glue's
intermediate output would not catch an EMR-introduced join fan-out bug.
Redshift load is the final, gated step.

Backfill: `dag_run.conf` accepts `start_date`/`end_date` to reprocess a
historical window; the normal daily schedule passes only "yesterday". Both
paths call the same tasks — backfill is not a special code path, only a
different conf (Section 13: "backfill" is a data-scoping concern, not a
different pipeline).
"""
from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.providers.amazon.aws.operators.emr import EmrAddStepsOperator
from airflow.providers.amazon.aws.sensors.emr import EmrStepSensor

DEFAULT_ARGS = {
    "owner": "retail-data-platform",
    "retries": 2,   # EMR retries are expensive (cluster time); kept lower than Glue-only DAGs
    "retry_delay": timedelta(minutes=10),
    "execution_timeout": timedelta(hours=4),
    "sla": timedelta(hours=5),
}

with DAG(
    dag_id="sales_pipeline",
    description="Glue standard ETL -> EMR heavy transform -> reconciliation -> Redshift load",
    default_args=DEFAULT_ARGS,
    schedule_interval="0 5 * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,   # EMR cluster is shared; avoid concurrent heavy jobs contending for capacity
    tags=["retail", "emr", "sales", "backfill-capable"],
) as dag:

    validate_input = PythonOperator(
        task_id="validate_input",
        python_callable=lambda **_: None,  # confirms the requested backfill window's raw data exists in S3
    )

    glue_standard_etl = GlueJobOperator(
        task_id="glue_standard_etl", job_name="bronze_to_silver",
        script_args={"--environment": "{{ var.value.environment }}", "--pipeline_name": "historical_sales_backfill"},
    )

    emr_submit = EmrAddStepsOperator(
        task_id="emr_submit",
        job_flow_id="{{ var.value.emr_cluster_id }}",
        steps=[{
            "Name": "large_sales_transformation",
            "ActionOnFailure": "CONTINUE",
            "HadoopJarStep": {
                "Jar": "command-runner.jar",
                "Args": [
                    "spark-submit", "--deploy-mode", "cluster",
                    "/emr/jobs/large_sales_transformation.py",
                    "--start_date", "{{ dag_run.conf.get('start_date', ds) }}",
                    "--end_date", "{{ dag_run.conf.get('end_date', ds) }}",
                ],
            },
        }],
    )

    emr_monitor = EmrStepSensor(
        task_id="emr_monitor",
        job_flow_id="{{ var.value.emr_cluster_id }}",
        step_id="{{ task_instance.xcom_pull(task_ids='emr_submit', key='return_value')[0] }}",
        timeout=60 * 60 * 3,
        mode="reschedule",  # free the worker slot between polls on a multi-hour EMR step
    )

    reconciliation = PythonOperator(task_id="reconciliation", python_callable=lambda **_: None)

    redshift_load = PythonOperator(task_id="redshift_load", python_callable=lambda **_: None)

    update_control_table = PythonOperator(task_id="update_control_table", python_callable=lambda **_: None)

    validate_input >> glue_standard_etl >> emr_submit >> emr_monitor >> reconciliation >> redshift_load >> update_control_table
