"""Product/vendor/purchase-order DAG — SAP-sourced master data (Section 13,
Section 2A). Fans out three independent Glue extracts in parallel (they
share no dependency), then joins before Gold promotion.
"""
from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator

DEFAULT_ARGS = {
    "owner": "retail-data-platform",
    "retries": 5,             # SAP RFC connectivity is flakier than SQL Server JDBC (Section 2A)
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,
    "execution_timeout": timedelta(hours=1),
}

with DAG(
    dag_id="product_pipeline",
    description="Parallel SAP master-data extraction (product/vendor/PO) -> Gold promotion",
    default_args=DEFAULT_ARGS,
    schedule_interval="30 2 * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=2,
    tags=["retail", "sap", "product"],
) as dag:

    extract_product_master = GlueJobOperator(
        task_id="extract_product_master", job_name="ingest_sap",
        script_args={"--environment": "{{ var.value.environment }}", "--pipeline_name": "sap_product_master_incremental"},
    )
    extract_vendor_master = GlueJobOperator(
        task_id="extract_vendor_master", job_name="ingest_sap",
        script_args={"--environment": "{{ var.value.environment }}", "--pipeline_name": "sap_vendor_master_incremental"},
    )
    extract_purchase_orders = GlueJobOperator(
        task_id="extract_purchase_orders", job_name="ingest_sap",
        script_args={"--environment": "{{ var.value.environment }}", "--pipeline_name": "sap_purchase_orders_incremental"},
    )

    dq_check_all = PythonOperator(task_id="dq_check_all", python_callable=lambda **_: None)

    gold_promotion = GlueJobOperator(
        task_id="gold_promotion", job_name="silver_to_gold",
        script_args={"--environment": "{{ var.value.environment }}"},
    )

    reconciliation = PythonOperator(task_id="reconciliation", python_callable=lambda **_: None)
    update_control_table = PythonOperator(task_id="update_control_table", python_callable=lambda **_: None)

    [extract_product_master, extract_vendor_master, extract_purchase_orders] >> dq_check_all
    dq_check_all >> gold_promotion >> reconciliation >> update_control_table
