# Architecture

This document contains the full Mermaid diagram set (Section 40 of the build
prompt) plus narrative explaining each. See [README.md](README.md) for the
top-level summary diagram.

## 1. Overall Platform

```mermaid
flowchart TB
    subgraph Sources
        SAP[SAP ERP]
        SQLS[SQL Server]
        API[REST APIs]
        SUP[Supplier Files]
        POS[POS]
        ECOM[E-commerce]
    end
    subgraph Ingestion
        GLUE_I[Glue Jobs]
        LAMBDA[Lambda]
        KINESIS[Kinesis Streams]
    end
    subgraph Lake["S3 Data Lake"]
        RAW[raw/]
        BRONZE[bronze/]
        SILVER[silver/]
        GOLD[gold/]
        QUAR[quarantine/]
        ARCHIVE[archive/]
    end
    subgraph Processing
        GLUE_P[Glue - standard ETL]
        EMR[EMR - heavy Spark]
    end
    subgraph Warehouse["Amazon Redshift"]
        DIM[Dimensions]
        FACT[Facts]
        MART[Marts]
    end
    subgraph CrossCutting["Cross-Cutting"]
        AIRFLOW[Airflow]
        IAM[IAM]
        KMS[KMS]
        SM[Secrets Manager]
        CW[CloudWatch]
        SNS[SNS]
        CICD[CodePipeline/CodeBuild]
    end

    SAP --> GLUE_I
    SQLS --> GLUE_I
    API --> GLUE_I
    SUP --> GLUE_I
    POS --> KINESIS
    ECOM --> KINESIS
    KINESIS --> LAMBDA
    LAMBDA --> RAW
    GLUE_I --> RAW
    RAW --> BRONZE --> SILVER --> GOLD
    BRONZE -.fails DQ.-> QUAR
    RAW -.retention.-> ARCHIVE
    SILVER --> GLUE_P --> GOLD
    SILVER --> EMR --> GOLD
    GOLD --> DIM
    GOLD --> FACT
    DIM --> MART
    FACT --> MART

    AIRFLOW -.orchestrates.-> GLUE_I
    AIRFLOW -.orchestrates.-> GLUE_P
    AIRFLOW -.orchestrates.-> EMR
    AIRFLOW -.orchestrates.-> Warehouse
    CW -.monitors.-> Ingestion
    CW -.monitors.-> Processing
    CW --> SNS
```

## 2. Batch Ingestion (SAP / SQL Server / API / Files)

```mermaid
sequenceDiagram
    participant Src as Source System
    participant Ctl as Control Table
    participant Glue as Glue Job
    participant S3Raw as S3 raw/
    participant DQ as DQ Framework
    participant S3Silver as S3 silver/

    Glue->>Ctl: read last_successful_watermark
    Glue->>Src: extract WHERE modified_date > watermark
    Src-->>Glue: changed rows
    Glue->>S3Raw: write immutable raw batch
    Glue->>DQ: validate schema + rules
    DQ-->>Glue: pass / quarantine
    Glue->>S3Silver: write validated, deduplicated data
    Glue->>Ctl: update watermark (only on full success)
```

## 3. Incremental Ingestion (Watermark Lifecycle)

```mermaid
flowchart LR
    A[Read previous watermark] --> B[Extract changes > watermark]
    B --> C[Write to staging/raw]
    C --> D[DQ validation]
    D -->|pass| E[Reconciliation]
    D -->|fail CRITICAL| F[Quarantine + Alert, watermark unchanged]
    E -->|match| G[Commit to target]
    E -->|mismatch| F
    G --> H[Update watermark = max extracted timestamp]
    F --> I[Retain previous watermark]
```

## 4. CDC Processing

```mermaid
flowchart TB
    IN[Incoming CDC batch I/U/D] --> DEDUP[Dedup by business key + latest change_timestamp]
    DEDUP --> SPLIT{Operation}
    SPLIT -->|D| DEL[Apply deletes to target]
    SPLIT -->|U| UPD[Apply updates - matched merge]
    SPLIT -->|I| INS[Apply inserts - unmatched merge]
    DEL --> AUDIT[Write audit record]
    UPD --> AUDIT
    INS --> AUDIT
    AUDIT --> COMMIT[Commit only after validation]
```

## 5. SCD Type 2 (dim_customer)

```mermaid
flowchart TB
    NEW[Incoming customer record] --> LOOKUP{Business key exists in dim?}
    LOOKUP -->|No| INSERT[Insert new current row\nis_current=Y, effective_start=today, effective_end=NULL]
    LOOKUP -->|Yes| HASH{record_hash changed?}
    HASH -->|No| NOOP[No action]
    HASH -->|Yes| EXPIRE[Expire current row\nis_current=N, effective_end=today-1]
    EXPIRE --> INSERTNEW[Insert new current row\nnew surrogate key]
```

## 6. Streaming (POS / E-commerce)

```mermaid
flowchart LR
    POS[POS Terminal] -->|PutRecord| KDS[Kinesis Data Stream]
    ECOM[E-commerce App] -->|PutRecord| KDS
    KDS --> LAMBDA[Lambda / Streaming Consumer]
    LAMBDA -->|valid| S3RAW[S3 raw/events/]
    LAMBDA -->|invalid| DLQ[Quarantine / DLQ]
    S3RAW --> GLUE[Glue micro-batch processing]
    GLUE --> S3SILVER[S3 silver/events/]
    S3SILVER --> REDSHIFT[Redshift near-real-time mart]
```

## 7. Airflow Orchestration

```mermaid
flowchart TB
    START([start]) --> EXT[extract]
    EXT --> LAND[land_to_s3]
    LAND --> SCHEMA[schema_validation]
    SCHEMA --> DQPRE[dq_precheck]
    DQPRE --> GLUEJOB[glue_processing]
    GLUEJOB --> EMRJOB[emr_processing]
    EMRJOB --> DQPOST[dq_postcheck]
    DQPOST --> RECON[reconciliation]
    RECON --> RS[redshift_load]
    RS --> CTL[update_control_table]
    CTL --> DONE([success])
    SCHEMA -.breaking change.-> FAIL([fail + alert])
    DQPRE -.critical failure.-> FAIL
    RECON -.mismatch.-> FAIL
```

## 8. Failure / Retry Flow

```mermaid
flowchart TB
    TASK[Task executes] --> RESULT{Result}
    RESULT -->|Success| NEXT[Proceed to next task]
    RESULT -->|Retryable error| ATTEMPTS{Attempts < max_attempts?}
    ATTEMPTS -->|Yes| BACKOFF[Exponential backoff + jitter] --> TASK
    ATTEMPTS -->|No| ALERT1[SNS alert + mark FAILED]
    RESULT -->|Non-retryable error| ALERT2[SNS alert immediately + mark FAILED]
    ALERT1 --> PRESERVE[Preserve last committed watermark/state]
    ALERT2 --> PRESERVE
    PRESERVE --> RESTART[Manual/scheduled rerun starts from last committed state]
```

## 9. Data Quality Framework

```mermaid
flowchart LR
    IN[Incoming batch] --> RULES[Apply rule set from dq_rules.yaml]
    RULES --> SEV{Severity}
    SEV -->|CRITICAL fail| BLOCK[Block promotion, quarantine batch, alert]
    SEV -->|ERROR fail| PARTIAL[Quarantine failing rows, promote rest]
    SEV -->|WARNING/INFO fail| LOG[Log finding, promote all rows]
    SEV -->|Pass| PROMOTE[Promote to next zone]
```

## 10. Reconciliation

```mermaid
flowchart LR
    SRC[Source counts/sums] --> COMPARE{Compare to target}
    TGT[Target counts/sums] --> COMPARE
    COMPARE -->|within tolerance| PASS[Status = PASSED, write audit row]
    COMPARE -->|variance exceeds tolerance| FAIL[Status = FAILED, alert, block downstream]
```

## 11. CI/CD

```mermaid
flowchart LR
    DEV[Developer] --> GIT[Git Repository]
    GIT --> CB1[CodeBuild: validate + unit test + package]
    CB1 --> CP[CodePipeline]
    CP --> DDEV[Deploy Dev]
    DDEV --> SMOKE[Automated smoke test]
    SMOKE --> DQA[Deploy QA]
    DQA --> APPROVE{Manual Approval}
    APPROVE -->|approved| DPROD[Deploy Prod]
    DPROD --> POSTV[Post-deployment validation]
```

## 12. Security

```mermaid
flowchart TB
    subgraph Identity
        IAMROLES[IAM Roles per service]
    end
    GLUEROLE[Glue Execution Role] --> S3[S3 - least-privilege bucket policy]
    EMRROLE[EMR Service/Instance Roles] --> S3
    AFROLE[Airflow Role] --> GLUEROLE
    AFROLE --> EMRROLE
    LAMROLE[Lambda Execution Role] --> S3
    CICDROLE[CI/CD Deployment Role] --> GLUEROLE
    CICDROLE --> EMRROLE
    CICDROLE --> LAMROLE
    S3 --> KMSKEY[KMS CMK - encryption at rest]
    SRC[Source credentials] --> SECRETS[Secrets Manager]
    GLUEROLE -.reads.-> SECRETS
```

## 13. Dev/QA/Prod Promotion

```mermaid
flowchart LR
    subgraph Dev["Dev Account/Env"]
        DEVCODE[Latest code] --> DEVDATA[retail-dev-data]
    end
    subgraph QA["QA Account/Env"]
        QACODE[Promoted code] --> QADATA[retail-qa-data]
    end
    subgraph Prod["Prod Account/Env"]
        PRODCODE[Approved code] --> PRODDATA[retail-prod-data]
    end
    DEVCODE -->|CI/CD promotion + smoke test| QACODE
    QACODE -->|manual approval| PRODCODE
```

## Glue vs EMR vs Airflow — Explicit Responsibilities

| Concern | Glue | EMR | Airflow |
|---|---|---|---|
| Batch extraction from SAP/SQL Server/API | ✅ | — | triggers/monitors |
| Standard cleansing, schema validation | ✅ | — | triggers/monitors |
| Data Catalog / partition registration | ✅ | — | — |
| Complex multi-way joins, TB-scale aggregation | — | ✅ | triggers/monitors |
| Historical backfill | — | ✅ | triggers/monitors |
| Cross-job dependency management, scheduling, retries | — | — | ✅ |
| Executing Spark code | ✅ (Glue Spark) | ✅ (EMR Spark) | ❌ never |

See ADR-002 for the full Glue-vs-EMR decision rationale.
