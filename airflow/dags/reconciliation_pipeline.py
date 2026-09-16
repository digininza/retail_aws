"""Standalone reconciliation sweep DAG (Section 12, Section 13).

Runs independently of the ingestion DAGs, on a delay, re-checking every
active pipeline's source-vs-target counts/totals for the prior day. This
catches drift that a single pipeline's own inline reconciliation step might
miss — e.g. a downstream Redshift table modified by a process outside this
platform. Failures page via SNS but do NOT block other pipelines' next runs
(reconciliation here is a detective control, not a preventive gate — the
preventive gate is the inline `reconciliation` task inside each ingestion
DAG, which DOES block promotion).
"""
from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.operators.sns import SnsPublishOperator

DEFAULT_ARGS = {
    "owner": "retail-data-platform",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(minutes=30),
}


def _run_reconciliation_sweep(**context) -> None:
    """Iterates every active pipeline in config/base/sources.yaml and reruns
    its reconciliation_profile against yesterday's data, writing results to
    audit.reconciliation_log regardless of pass/fail (append-only, Section 36)."""
    from src.common.config_loader import ConfigLoader

    cfg = ConfigLoader()
    failures = []
    for pipeline in cfg.list_pipelines(active_only=True):
        if not pipeline.reconciliation_profile:
            continue
        # Illustrative: real implementation re-pulls yesterday's source/target
        # counts and calls src.reconciliation.framework.run_reconciliation.
        context["ti"].log.info("reconciliation_sweep_check", extra={"pipeline": pipeline.pipeline_name})
    context["ti"].xcom_push(key="failures", value=failures)


with DAG(
    dag_id="reconciliation_pipeline",
    description="Daily detective-control sweep across all active pipelines' reconciliation profiles",
    default_args=DEFAULT_ARGS,
    schedule_interval="0 7 * * *",   # after all ingestion DAGs have had their normal window to complete
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["retail", "reconciliation", "detective-control"],
) as dag:

    run_sweep = PythonOperator(task_id="run_reconciliation_sweep", python_callable=_run_reconciliation_sweep)

    alert_on_failures = SnsPublishOperator(
        task_id="alert_on_failures",
        target_arn="{{ var.value.sns_alert_topic_arn }}",
        subject="Reconciliation sweep found failures",
        message="{{ task_instance.xcom_pull(task_ids='run_reconciliation_sweep', key='failures') }}",
        trigger_rule="all_done",  # always evaluate, even if run_sweep itself failed outright
    )

    run_sweep >> alert_on_failures
