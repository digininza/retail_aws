# IAM — Who Can Access What, and Why

All policy JSON in this directory is **illustrative** — placeholder account
IDs (`000000000000`), bucket names, and resource ARNs. Do not deploy as-is;
substitute real account/resource identifiers via the CI/CD pipeline's
parameter substitution step (`cicd/scripts/`).

## Service roles

| Role | Assumed by | Can do | Cannot do |
|---|---|---|---|
| `retail-{env}-glue-execution-role` | AWS Glue | Read source JDBC connections (via Secrets Manager), read/write S3 raw/bronze/silver/gold/quarantine, write to Glue Data Catalog, write CloudWatch Logs | Write to Redshift directly (loads go through a separate Redshift COPY role), modify IAM, access other environments' S3 buckets |
| `retail-{env}-emr-execution-role` (+ EC2 instance profile) | Amazon EMR | Read S3 silver/gold, write S3 gold, write CloudWatch Logs/metrics | Read raw/ (EMR only ever reads already-validated Silver data — see `sales_pipeline` DAG rationale), access Secrets Manager (EMR jobs don't connect to source systems directly) |
| `retail-{env}-mwaa-execution-role` | Amazon MWAA (Airflow) | Start/monitor Glue jobs and EMR steps, read/write its own DAG/log S3 bucket, publish to SNS, read Airflow Variables/Connections | Directly read/write the data lake buckets (orchestration only — no data access) |
| `retail-{env}-lambda-execution-role` | AWS Lambda | Read/write S3 raw/quarantine (scoped to specific prefixes per function), publish to SNS, read Secrets Manager (api_ingestion_handler only) | Start Glue/EMR jobs directly except `pipeline_trigger` (least privilege per function, not one shared role) |
| `retail-{env}-redshift-load-role` | Amazon Redshift (via `IAM_ROLE` in COPY/UNLOAD) | Read S3 gold/redshift_staging/ | Write to S3, read raw/bronze/silver |
| `retail-cicd-deployment-role` | AWS CodeBuild/CodePipeline | Deploy Glue job definitions, Lambda code, Airflow DAGs (S3 sync to the MWAA DAGs bucket), CloudFormation stack updates in Dev/QA; Prod deploys require the manual-approval-gated pipeline stage | Deploy directly to Prod without passing through QA + approval |

## Principles applied

1. **One role per service per environment** — a Dev Glue role cannot touch
   the Prod bucket, full stop; there is no shared "data-platform-role".
2. **Least privilege by prefix, not just by bucket** — e.g. Lambda's
   `file_arrival_handler` can only write to `quarantine/` and read
   `raw/`, not `gold/`.
3. **No standing human access to Prod data** — engineers assume a
   break-glass role with logging/alerting, not a permanent IAM user.
4. **Secrets Manager, never inline credentials** — every JDBC/API secret is
   referenced by ARN, resolved at runtime (see `.env.example`, ADR-009).

See `docs/hld/05_cicd_security_hld.md` for the full security HLD.
