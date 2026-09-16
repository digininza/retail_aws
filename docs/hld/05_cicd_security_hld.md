# HLD 05 — CI/CD & Security

## CI/CD pipeline

```
Developer -> Git -> CodeBuild (validate, unit test, package) -> CodePipeline
  -> Deploy Dev -> automated smoke test -> Deploy QA -> manual approval -> Deploy Prod
  -> post-deployment validation
```

What each stage does — see `cicd/buildspec.yml` and `cicd/pipeline.yaml` for
the executable definitions:

1. **Source** — GitHub via CodeStar connection.
2. **Validate** — config YAML parses for every environment; every Python
   file byte-compiles; `black`/`isort` checks.
3. **Unit test** — `pytest tests/unit tests/data_quality tests/reconciliation`.
4. **Package** — Glue/EMR scripts, Lambda handlers, Airflow DAGs/plugins,
   SQL, and config are assembled into `dist/`.
5. **Deploy Dev** — CloudFormation stack update + Glue job definitions +
   Lambda code + DAG sync, no approval gate.
6. **Smoke test** — triggers one low-risk pipeline end-to-end in Dev and
   checks the control table for a SUCCESS run (`cicd/scripts/smoke_test.sh`).
7. **Deploy QA** — same deployment actions, targeting the QA account.
8. **Manual approval** — a human explicitly approves promotion to Prod.
9. **Deploy Prod** — same deployment actions, targeting the Prod account,
   only reachable after step 8.
10. **Post-deployment validation** — confirms Glue/Lambda/Airflow artifacts
    are correctly deployed in Prod; does not run a full pipeline against
    live Prod data as part of the deploy itself.

**What actually executes deployment:** the YAML in `cicd/` describes
pipeline *structure* and *configuration*. AWS CodeBuild and CodePipeline are
the services that *execute* it — a YAML file has no agency on its own
(Section 26).

## What gets deployed, explicitly

| Artifact | Source | Destination |
|---|---|---|
| Glue job scripts | `glue/jobs/*.py` | S3 deployment-artifacts bucket, referenced by Glue job definitions |
| EMR job scripts | `emr/jobs/*.py` | S3 deployment-artifacts bucket, referenced by EMR steps |
| Lambda code | `lambda/*/handler.py` | Lambda function code (zipped) |
| Airflow DAGs/plugins | `airflow/dags/`, `airflow/plugins/` | MWAA's DAGs S3 bucket |
| Configuration | `config/base/`, `config/{env}/` | Bundled alongside job code; read at runtime, not baked into a container image |
| Infrastructure | `infrastructure/cloudformation/*.yaml` | Deployed via `aws cloudformation deploy` |
| SQL | `sql/**/*.sql` | Run by Airflow tasks / a Redshift Data API call, not deployed as standing objects except DDL |

## Environment isolation (ADR-009)

Dev, QA, and Prod are separate AWS accounts (illustrative account IDs
`000000000000` / `111111111111` / `222222222222`), each with its own S3
bucket, Glue Catalog, Redshift cluster, MWAA environment, and KMS key. There
is no cross-account IAM trust except the CI/CD deployment role's scoped
`sts:AssumeRole` into each environment's deploy-target role — no
application code or human user has standing cross-environment access.
Configuration for prod is never overwritten accidentally because
`config/prod/environment.yaml` is only ever touched by a PR that goes
through the same CI/CD approval gate as code changes — there is no
separate, less-controlled path for "just a config change."

## Security model (Section 19)

- **IAM**: one role per service per environment, least privilege by S3
  prefix (not just bucket) — see `infrastructure/iam/README.md` for the
  full role matrix and rationale.
- **KMS**: every S3 bucket and Redshift cluster encrypts at rest with a
  per-environment CMK; the bucket policy denies any `PutObject` that
  doesn't specify `aws:kms` encryption (`infrastructure/s3/bucket-policy.json`).
- **Secrets Manager**: every source credential (SQL Server, SAP, REST API)
  is referenced by ARN and resolved at runtime — never hardcoded, never
  committed (`.env.example`, `src/common/utilities.py::SecretsManagerAdapter`).
- **Encryption in transit**: the S3 bucket policy denies any non-TLS
  request (`aws:SecureTransport: false`).
- **Network**: Redshift and MWAA are not publicly accessible
  (`PubliclyAccessible: false`, `WebserverAccessMode: PRIVATE_ONLY`);
  access is via VPC-internal routes only (illustrative — actual subnet/SG
  topology is environment-specific and not modeled in this reference repo).

## Monitoring & alerting (Section 20)

CloudWatch alarms on Glue job failures, Lambda errors, DQ CRITICAL
failures, reconciliation failures, data freshness breaches, and pipeline
duration SLA breaches all publish to a per-environment SNS topic
(`infrastructure/monitoring/*.json`). Every structured log line
(`src.common.logging_utils`) carries `run_id`, `pipeline`, `environment`,
`status`, and record counts so a CloudWatch Logs Insights query can
reconstruct a full run's story without cross-referencing multiple systems.

## Related documents

- [../adr/ADR-009-environment-configuration.md](../adr/ADR-009-environment-configuration.md)
- [../adr/ADR-010-cicd-approach.md](../adr/ADR-010-cicd-approach.md)
- [../lld/09_cicd_deployment_lld.md](../lld/09_cicd_deployment_lld.md)
