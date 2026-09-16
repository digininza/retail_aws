# LLD 09 — CI/CD Deployment

## Objective

Deploy code, configuration, and infrastructure changes to Dev, then QA,
then Prod — automatically for Dev/QA, gated by a human approval for Prod —
with every promotion having passed the same validation and test suite
(Section 26).

## Assumptions

- All environments' AWS accounts already exist (account creation/VPC setup
  is out of scope for this pipeline; it deploys application-layer resources
  into existing account/network scaffolding).
- The CI/CD deployment role (`infrastructure/iam/cicd-deployment-role-policy.json`)
  has been provisioned with `sts:AssumeRole` trust into each environment's
  deploy-target role.

## Input / Output

**Input:** A merge to `main` (or a PR build, for validation-only stages).

**Output:** Updated Glue job definitions, Lambda function code, Airflow
DAGs, and CloudFormation stacks in the target environment(s), plus a
pass/fail signal surfaced back to the PR/commit.

## Components

| Stage | Tool | Definition |
|---|---|---|
| Build/validate/test/package | AWS CodeBuild | `cicd/buildspec.yml` |
| Pipeline orchestration | AWS CodePipeline | `cicd/pipeline.yaml` |
| Per-environment parameters | — | `cicd/deployment/{dev,qa,prod}.yaml` |
| Deployment execution | Shell + AWS CLI | `cicd/scripts/deploy.sh` |
| Post-Dev validation | Shell + AWS CLI | `cicd/scripts/smoke_test.sh` |

## Sequence flow

```
Source (GitHub) -> CodeBuild(validate, unit test, package) -> dist/ artifact
  -> Deploy Dev (CloudFormation + Glue defs + Lambda + DAG sync)
  -> Smoke test Dev (trigger 1 pipeline, poll control table for SUCCESS)
  -> Deploy QA (same actions, QA account)
  -> Manual approval (human, via CodePipeline console/SNS notification)
  -> Deploy Prod (same actions, Prod account)
  -> Post-deployment validation (artifact presence/version checks, no live data run)
```

## Pseudocode: `deploy.sh`

```
environment = $1
config = load(cicd/deployment/{environment}.yaml)
cloudformation deploy --template s3-data-lake.yaml --stack-name config.cloudformation_stack_name \
  --parameter-overrides Environment={environment}
s3 sync dist/glue/jobs  -> s3://retail-{environment}-deployment-artifacts/glue/jobs/
s3 sync dist/emr/jobs   -> s3://retail-{environment}-deployment-artifacts/emr/jobs/
s3 sync dist/airflow/dags -> s3://{config.mwaa_dags_bucket}/dags/
```

## Why Dev/QA auto-deploy but Prod doesn't

Dev and QA are internal-only, non-customer-facing, and fully recoverable —
a bad deploy costs a re-deploy, not incident response. Prod carries real
downstream consumers (Redshift-fed BI dashboards, potentially
finance-facing reconciliation reports); the cost asymmetry between "slightly
slower Prod releases" and "an unreviewed change reaches Prod data" justifies
the manual approval gate (Section 26 stage 8, ADR-010).

## Exception handling & retry

- A failed `pre_build`/`build` phase (lint, unit test) fails the CodeBuild
  action outright — CodePipeline does not proceed to any Deploy stage.
- A failed `Deploy Dev` CloudFormation action can be retried by re-running
  the pipeline execution; `deploy.sh`'s CloudFormation calls use
  `--no-fail-on-empty-changeset` so a rerun with no actual infra diff is a
  successful no-op, not a spurious failure.
- A failed smoke test blocks promotion to QA — the pipeline execution stops
  at that stage rather than proceeding with an unverified Dev deploy.

## Logging & audit

CodeBuild/CodePipeline execution history is itself the audit trail for
deployments (who/what/when); `smoke_test.sh`'s Glue run additionally
appears in `audit.pipeline_run_log` like any other pipeline execution,
tagged by its trigger context.

## Security

See `docs/hld/05_cicd_security_hld.md` for the full model — one deployment
role per environment via scoped `sts:AssumeRole`, no standing cross-account
access, Prod config changes go through the same gate as code changes.

## Performance / cost

Dev/QA use smaller Glue worker counts and single-node Redshift/EMR sizing
(`config/dev/environment.yaml`, `config/qa/environment.yaml`) specifically
so CI/CD-triggered Dev deploys and smoke tests are cheap to run on every
merge (Section 34).

## Edge cases

| Case | Handling |
|---|---|
| CloudFormation stack has no actual changes to deploy | `--no-fail-on-empty-changeset` prevents a spurious pipeline failure |
| Smoke test's Glue job legitimately takes longer than `TIMEOUT_SECONDS` | Treated as smoke test FAILURE (fail-safe: better to block promotion than assume success) |
| A hotfix needs to skip QA and go straight to Prod | Not supported by this pipeline design as documented — every change goes through the full Dev -> QA -> approval -> Prod sequence; an emergency path would be a deliberate, separately-documented break-glass process, not a normal pipeline feature |

## Test cases

CI/CD scripts are validated via `pre_build`'s own self-check
(`python -c "from src.common.config_loader import ConfigLoader; ..."` for
all three environments) plus `black`/`isort`/`py_compile` checks; there is
no separate automated test suite *for* the pipeline definition itself in
this reference project (a real deployment would add CDK/CFN unit tests, out
of scope here).
