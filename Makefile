.PHONY: setup test test-unit test-integration test-dq test-recon \
        demo-incremental demo-cdc-scd2 demo-emr-backfill demo-streaming \
        lint clean

PYTHON ?= python3

setup:
	$(PYTHON) -m venv .venv
	. .venv/bin/activate && pip install -r requirements.txt

test:
	pytest tests/ -v

test-unit:
	pytest tests/unit -v

test-integration:
	pytest tests/integration -v

test-dq:
	pytest tests/data_quality -v

test-recon:
	pytest tests/reconciliation -v

# Scenario 1: SQL Server incremental -> S3 -> DQ -> reconciliation -> Redshift staging
demo-incremental:
	$(PYTHON) -m src.ingestion.sqlserver.run_demo

# CDC + SCD2 demo against dim_customer
demo-cdc-scd2:
	$(PYTHON) -m src.dimensional_model.run_scd2_demo

# Scenario 2: historical sales backfill via local PySpark (stands in for EMR)
demo-emr-backfill:
	$(PYTHON) emr/jobs/historical_backfill.py --local

# Scenario 3: POS/e-commerce streaming simulation via local Kinesis adapter.
# Sequential (not backgrounded) so the demo is deterministic: producers fully
# populate the local stream files before the consumer drains them.
demo-streaming:
	$(PYTHON) streaming/producers/pos_producer.py
	$(PYTHON) streaming/producers/ecom_producer.py
	$(PYTHON) streaming/consumers/event_consumer.py

lint:
	$(PYTHON) -m black --check src tests
	$(PYTHON) -m isort --check-only src tests

clean:
	find . -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true
	rm -rf .pytest_cache .mypy_cache .local_s3 .coverage htmlcov
