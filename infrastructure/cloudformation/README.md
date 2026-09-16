# Infrastructure as Code

Illustrative CloudFormation for the platform's core resources. `s3-data-lake.yaml`
is written in full as a representative example; the remaining stacks
(Glue jobs, EMR cluster launch config, Redshift, MWAA, monitoring) are
represented as the JSON configuration documents under the sibling
`infrastructure/{glue,emr,redshift,airflow,monitoring}/` directories rather
than full CFN, to keep this reference project's infrastructure code
readable end-to-end instead of an exhaustive, mostly-boilerplate CFN dump.

## Stack composition (as it would be deployed)

```
retail-{env}-root-stack
├── s3-data-lake.yaml          (this directory — full example)
├── iam-roles.yaml              (see infrastructure/iam/*.json for the policy documents it would attach)
├── glue-jobs.yaml                (see infrastructure/glue/glue-job-definitions.json)
├── redshift-cluster.yaml           (see infrastructure/redshift/redshift-cluster-config.json)
├── mwaa-environment.yaml             (see infrastructure/airflow/mwaa-environment-config.json)
└── monitoring.yaml                     (see infrastructure/monitoring/*.json)
```

## Deployment

Stacks are deployed by CI/CD (`cicd/buildspec.yml`), never manually applied
against Prod. Dev/QA stacks may be applied via `aws cloudformation deploy`
directly by a platform engineer for iteration; Prod changes only ever flow
through the pipeline's approval-gated stage (Section 26, ADR-010).
