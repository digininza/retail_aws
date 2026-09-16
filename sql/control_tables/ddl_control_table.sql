-- Control table DDL (Section 7). Reference implementation targets a
-- relational control schema (Redshift `control` schema or an RDS Postgres
-- instance shared by Glue/Airflow) — see docs/lld/01_sqlserver_incremental_ingestion_lld.md
-- for the DynamoDB alternative-design discussion.
--
-- One row per pipeline. Never overwritten by a failed run's watermark
-- (Section 7's critical rule is enforced in application code — see
-- src/common/audit.py::ControlTableStore.complete_run — not by DDL alone).

CREATE SCHEMA IF NOT EXISTS control;

CREATE TABLE IF NOT EXISTS control.pipeline_control (
    pipeline_name               VARCHAR(200) PRIMARY KEY,
    source_system                VARCHAR(50)  NOT NULL,
    source_table                 VARCHAR(200) NOT NULL,
    load_type                    VARCHAR(20)  NOT NULL, -- FULL | INCREMENTAL | CDC | STREAMING
    watermark_column             VARCHAR(200),
    last_successful_watermark    VARCHAR(200),
    current_run_id               VARCHAR(64),
    last_run_status               VARCHAR(20),           -- RUNNING | SUCCESS | FAILED
    last_run_start                TIMESTAMP,
    last_run_end                  TIMESTAMP,
    records_read                  BIGINT,
    records_written               BIGINT,
    error_message                  VARCHAR(4000),
    updated_at                     TIMESTAMP DEFAULT GETDATE()
);

COMMENT ON TABLE control.pipeline_control IS
  'Section 7 control table. last_successful_watermark is the only field a
   downstream extract should read to plan its next incremental run.';
