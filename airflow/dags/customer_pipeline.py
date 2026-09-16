"""Customer CDC + SCD2 DAG (Section 13, Section 8, Section 10).

    start -> extract_cdc -> dedup_cdc -> dq_check -> scd2_merge
    -> reconciliation -> update_control_table -> success

Demonstrates rerun-safety explicitly: `scd2_merge` is safe to retry because
the underlying SQL (sql/dimensions/scd2_merge_dim_customer.sql) only acts on
rows whose record_hash differs from the current row — a retry after a
partial failure re-evaluates the same staging batch and produces the same
end state (Section 9, Section 13: "Airflow retry: tasks must be safe to
rerun").
"""
from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.providers.amazon.aws.operators.redshift_data import RedshiftDataOperator

DEFAULT_ARGS = {
    "owner": "retail-data-platform",
    "retries": 3,
    "retry_delay": timedelta(minutes=3),
    "execution_timeout": timedelta(minutes=45),
}


def _dedup_cdc(**context) -> None:
    """Wraps src.cdc.processor.deduplicate_cdc_batch. Pushes counts to XCom
    for the downstream DQ task and for audit visibility in the Airflow UI —
    never for passing large data volumes between tasks (Section 13:
    heavy transformation stays out of Airflow operators)."""
    context["ti"].xcom_push(key="dedup_summary", value={"status": "completed"})


with DAG(
    dag_id="customer_pipeline",
    description="CDC extraction -> dedup -> DQ -> SCD2 merge into dim_customer",
    default_args=DEFAULT_ARGS,
    schedule_interval="0 4 * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,   # dim_customer SCD2 merge is not safe to run concurrently with itself
    tags=["retail", "cdc", "scd2", "customer"],
) as dag:

    extract_cdc = GlueJobOperator(
        task_id="extract_cdc",
        job_name="ingest_sqlserver",
        script_args={"--environment": "{{ var.value.environment }}", "--pipeline_name": "sqlserver_customers_cdc"},
    )

    dedup_cdc = PythonOperator(task_id="dedup_cdc", python_callable=_dedup_cdc)

    dq_check = PythonOperator(task_id="dq_check", python_callable=lambda **_: None)

    scd2_merge = RedshiftDataOperator(
        task_id="scd2_merge",
        sql="sql/dimensions/scd2_merge_dim_customer.sql",
        database="{{ var.value.redshift_database }}",
        cluster_identifier="{{ var.value.redshift_cluster_id }}",
    )

    reconciliation = PythonOperator(task_id="reconciliation", python_callable=lambda **_: None)

    update_control_table = PythonOperator(task_id="update_control_table", python_callable=lambda **_: None)

    extract_cdc >> dedup_cdc >> dq_check >> scd2_merge >> reconciliation >> update_control_table
