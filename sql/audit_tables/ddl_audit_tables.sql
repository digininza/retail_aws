-- Audit + reconciliation history tables (Section 12, Section 36). Append-only,
-- separate from control.pipeline_control (which holds only *current* state).

CREATE SCHEMA IF NOT EXISTS audit;

CREATE TABLE IF NOT EXISTS audit.pipeline_run_log (
    run_id            VARCHAR(64) PRIMARY KEY,
    pipeline_name      VARCHAR(200) NOT NULL,
    environment         VARCHAR(20)  NOT NULL,
    start_time           TIMESTAMP    NOT NULL,
    end_time              TIMESTAMP,
    status                 VARCHAR(20)  NOT NULL,  -- RUNNING | SUCCESS | FAILED
    records_read           BIGINT,
    records_written        BIGINT,
    records_rejected       BIGINT,
    source                  VARCHAR(200),
    target                   VARCHAR(200),
    watermark_before         VARCHAR(200),
    watermark_after          VARCHAR(200),
    error_message             VARCHAR(4000)
);

CREATE TABLE IF NOT EXISTS audit.reconciliation_log (
    reconciliation_id  BIGINT IDENTITY(1,1) PRIMARY KEY,
    run_id               VARCHAR(64) NOT NULL REFERENCES audit.pipeline_run_log(run_id),
    pipeline_name         VARCHAR(200) NOT NULL,
    check_type              VARCHAR(30) NOT NULL, -- RECORD_COUNT | AGGREGATE | CONTROL_TOTAL | KEY_RECONCILIATION | PARTITION_DATE
    source_count            BIGINT,
    target_count            BIGINT,
    source_total             DECIMAL(18,2),
    target_total             DECIMAL(18,2),
    variance                  DECIMAL(18,2),
    variance_pct              DECIMAL(9,4),
    status                     VARCHAR(20) NOT NULL, -- PASSED | FAILED
    validation_timestamp        TIMESTAMP DEFAULT GETDATE()
);

CREATE TABLE IF NOT EXISTS audit.dq_failure_log (
    dq_failure_id   BIGINT IDENTITY(1,1) PRIMARY KEY,
    run_id            VARCHAR(64) NOT NULL REFERENCES audit.pipeline_run_log(run_id),
    pipeline_name      VARCHAR(200) NOT NULL,
    rule_name            VARCHAR(100) NOT NULL,
    severity              VARCHAR(20) NOT NULL, -- CRITICAL | ERROR | WARNING | INFO
    failed_record_count   BIGINT,
    error_reason            VARCHAR(1000),
    source_file               VARCHAR(500),
    ingestion_timestamp        TIMESTAMP DEFAULT GETDATE()
);

COMMENT ON TABLE audit.pipeline_run_log IS
  'Append-only run history (Section 36). Every field here should be
   populated on both SUCCESS and FAILED runs.';
